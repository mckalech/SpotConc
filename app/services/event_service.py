"""Service layer for fetching, caching and storing concert events."""

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from app.clients.ticketmaster_client import TicketmasterClient
from app.config import get_settings
from app.db.models import Artist, Event, EventCache
from app.db.session import get_session
from app.utils.logging import get_logger

logger = get_logger(__name__)


class EventService:
    """Orchestrates event discovery: cache check, API fetch, DB persistence."""

    def __init__(
        self,
        client: TicketmasterClient,
        session: Session,
    ):
        self._client = client
        self._session = session
        self._settings = get_settings()

    def find_events_for_all_artists(
        self,
        date_from: str,
        date_to: str,
        country: str = "GB",
    ) -> Dict[str, int]:
        """Search events for every artist in the database.

        Fetches from API concurrently via thread pool (rate-limited at client
        level), then stores results sequentially in the main session.

        Args:
            date_from: Start date (YYYY-MM-DD).
            date_to: End date (YYYY-MM-DD).
            country: Country code.

        Returns:
            Stats dict with counts.
        """
        artists = self._session.query(Artist).all()
        total = len(artists)
        logger.info(
            "Searching events for %d artists (country=%s, %s to %s)",
            total, country, date_from, date_to,
        )

        if total == 0:
            logger.warning("No artists in database. Run sync-spotify first.")
            return {"artists_processed": 0, "artists_with_events": 0,
                    "events_found": 0, "events_from_cache": 0, "artists_failed": 0}

        stats = {
            "artists_processed": 0,
            "artists_with_events": 0,
            "events_found": 0,
            "events_from_cache": 0,
            "artists_failed": 0,
        }

        max_workers = self._settings.max_concurrent_requests

        # Phase 1: Fetch raw events concurrently (no DB writes in threads)
        fetch_results: List[Tuple[Artist, Dict[str, Any]]] = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_artist = {
                executor.submit(
                    self._fetch_artist_events, artist, date_from, date_to, country
                ): artist
                for artist in artists
            }

            for future in as_completed(future_to_artist):
                artist = future_to_artist[future]
                stats["artists_processed"] += 1

                try:
                    result = future.result()
                    fetch_results.append((artist, result))
                except Exception as exc:
                    stats["artists_failed"] += 1
                    logger.warning(
                        "Failed to fetch events for '%s': %s",
                        artist.name,
                        str(exc)[:200],
                    )

                # Progress log every 25 artists
                if stats["artists_processed"] % 25 == 0 or stats["artists_processed"] == total:
                    logger.info(
                        "Fetch progress: %d/%d artists processed",
                        stats["artists_processed"],
                        total,
                    )

        # Phase 2: Store results sequentially in main session (thread-safe)
        logger.info("Storing events in database...")
        for artist, result in fetch_results:
            raw_events = result["raw_events"]
            from_cache = result["from_cache"]
            cache_key = result["cache_key"]

            events_stored = 0
            for raw_event in raw_events:
                parsed = TicketmasterClient.parse_event(raw_event)
                if not parsed["event_id"]:
                    continue
                self._upsert_event(artist, parsed)
                events_stored += 1

            # Update cache in DB
            if not from_cache:
                self._set_cache(cache_key, raw_events)

            if events_stored > 0:
                stats["artists_with_events"] += 1
            stats["events_found"] += events_stored
            if from_cache:
                stats["events_from_cache"] += events_stored

        self._session.commit()

        logger.info(
            "Event search complete: %d artists, %d with events, "
            "%d total events (%d from cache), %d failed",
            stats["artists_processed"],
            stats["artists_with_events"],
            stats["events_found"],
            stats["events_from_cache"],
            stats["artists_failed"],
        )
        return stats

    def _fetch_artist_events(
        self,
        artist: Artist,
        date_from: str,
        date_to: str,
        country: str,
    ) -> Dict[str, Any]:
        """Fetch events for a single artist. Thread-safe (no DB writes).

        Checks cache first (using a separate session for reads).

        Returns:
            Dict with raw_events list, from_cache flag, and cache_key.
        """
        cache_key = f"{artist.spotify_artist_id}:{country}:{date_from}:{date_to}"

        # Check cache in a thread-local session (read-only)
        cached = self._get_cache_threadsafe(cache_key)
        if cached is not None:
            logger.debug("Cache hit for '%s'", artist.name)
            return {"raw_events": cached, "from_cache": True, "cache_key": cache_key}

        # Fetch from API
        raw_events = self._client.search_events(
            keyword=artist.name,
            country_code=country,
            date_from=date_from,
            date_to=date_to,
        )

        return {"raw_events": raw_events, "from_cache": False, "cache_key": cache_key}

    def _get_cache_threadsafe(self, cache_key: str) -> Optional[List[Dict[str, Any]]]:
        """Read cache using a separate short-lived session (thread-safe)."""
        thread_session = get_session()
        try:
            entry = thread_session.get(EventCache, cache_key)
            if entry is None:
                return None

            ttl = timedelta(hours=entry.ttl_hours)
            if entry.fetched_at and (datetime.utcnow() - entry.fetched_at) > ttl:
                return None

            try:
                return json.loads(entry.response_json)
            except json.JSONDecodeError:
                return None
        finally:
            thread_session.close()

    def _upsert_event(self, artist: Artist, parsed: Dict[str, Any]) -> Event:
        """Insert or update an event record (main session, not thread-safe)."""
        event = self._session.get(Event, parsed["event_id"])
        if event is None:
            event = Event(
                event_id=parsed["event_id"],
                artist_id=artist.spotify_artist_id,
                event_name=parsed["event_name"],
                venue=parsed["venue"],
                city=parsed["city"],
                country=parsed["country"],
                date=parsed["date"],
                url=parsed["url"],
                confidence_score=0.0,
                match_status="PENDING",
            )
            self._session.add(event)
        else:
            event.event_name = parsed["event_name"]
            event.venue = parsed["venue"]
            event.city = parsed["city"]
            event.country = parsed["country"]
            event.date = parsed["date"]
            event.url = parsed["url"]
            # Reset matching so it gets re-evaluated
            event.confidence_score = 0.0
            event.match_status = "PENDING"
        return event

    def _set_cache(self, cache_key: str, raw_events: List[Dict[str, Any]]) -> None:
        """Store API response in cache (main session)."""
        entry = self._session.get(EventCache, cache_key)
        response_json = json.dumps(raw_events)

        if entry is None:
            entry = EventCache(
                cache_key=cache_key,
                response_json=response_json,
                fetched_at=datetime.utcnow(),
                ttl_hours=self._settings.cache_ttl_hours,
            )
            self._session.add(entry)
        else:
            entry.response_json = response_json
            entry.fetched_at = datetime.utcnow()
