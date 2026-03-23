"""Service for calculating match confidence between artists and events."""

from typing import Dict, List, Tuple

from sqlalchemy.orm import Session

from app.db.models import Artist, Event
from app.utils.logging import get_logger
from app.utils.normalization import normalize_artist_name

logger = get_logger(__name__)

# Score weights
SCORE_EXACT_MATCH = 0.6
SCORE_NORMALIZED_MATCH = 0.2
SCORE_NAME_IN_TITLE = 0.1
SCORE_VALID_CONTEXT = 0.1

# Status thresholds
THRESHOLD_ACCEPTED = 0.85
THRESHOLD_REVIEW = 0.65


class MatchingService:
    """Calculate match scores and assign statuses to events."""

    def __init__(self, session: Session):
        self._session = session

    def process_all(self) -> Dict[str, int]:
        """Score and classify all PENDING events.

        Returns:
            Stats dict with counts per status.
        """
        events = (
            self._session.query(Event)
            .filter(Event.match_status == "PENDING")
            .all()
        )
        logger.info("Matching %d pending events...", len(events))

        stats = {"ACCEPTED": 0, "REVIEW": 0, "REJECTED": 0}

        for event in events:
            artist = self._session.get(Artist, event.artist_id)
            if artist is None:
                logger.warning(
                    "Event '%s' references unknown artist_id '%s', rejecting",
                    event.event_name, event.artist_id,
                )
                event.confidence_score = 0.0
                event.match_status = "REJECTED"
                stats["REJECTED"] += 1
                continue

            score = self._calculate_score(artist, event)
            status = self._score_to_status(score)

            event.confidence_score = round(score, 4)
            event.match_status = status
            stats[status] += 1

        self._session.commit()

        logger.info(
            "Matching complete: %d ACCEPTED, %d REVIEW, %d REJECTED",
            stats["ACCEPTED"], stats["REVIEW"], stats["REJECTED"],
        )
        return stats

    @staticmethod
    def _calculate_score(artist: Artist, event: Event) -> float:
        """Calculate match confidence score (0.0 – 1.0).

        Rules:
            +0.6  exact name match (case-insensitive)
            +0.2  normalized name match
            +0.1  artist name found in event title
            +0.1  valid location and date present
        """
        score = 0.0

        artist_lower = artist.name.lower().strip()
        event_name_lower = event.event_name.lower().strip()
        artist_normalized = artist.normalized_name
        event_name_normalized = normalize_artist_name(event.event_name)

        # 1. Exact name match: artist name == event name, or event name starts
        #    with artist name (e.g. "Arctic Monkeys Live at O2")
        if artist_lower == event_name_lower or event_name_lower.startswith(artist_lower + " "):
            score += SCORE_EXACT_MATCH

        # 2. Normalized full match or event starts with normalized artist name
        if artist_normalized == event_name_normalized or event_name_normalized.startswith(artist_normalized + " "):
            score += SCORE_NORMALIZED_MATCH

        # 3. Artist name appears anywhere in event title (partial/substring)
        if artist_lower in event_name_lower or artist_normalized in event_name_normalized:
            score += SCORE_NAME_IN_TITLE

        # Valid context: has both date and city/venue
        has_date = bool(event.date and len(event.date) >= 10)
        has_location = bool(event.city or event.venue)
        if has_date and has_location:
            score += SCORE_VALID_CONTEXT

        return min(score, 1.0)

    @staticmethod
    def _score_to_status(score: float) -> str:
        """Map score to match status."""
        if score >= THRESHOLD_ACCEPTED:
            return "ACCEPTED"
        elif score >= THRESHOLD_REVIEW:
            return "REVIEW"
        else:
            return "REJECTED"
