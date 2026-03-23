"""Service for generating CSV and JSON reports from matched events."""

import json
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy.orm import Session

from app.config import DATA_DIR
from app.db.models import Artist, Event, Playlist, Track, playlist_tracks, track_artists
from app.utils.logging import get_logger

logger = get_logger(__name__)

OUTPUT_DIR = DATA_DIR / "output"


class ReportService:
    """Generate structured reports from accepted/review events."""

    def __init__(self, session: Session):
        self._session = session

    def generate(
        self,
        include_review: bool = False,
        output_dir: Optional[Path] = None,
    ) -> Dict[str, str]:
        """Generate CSV and JSON reports.

        Args:
            include_review: If True, include REVIEW events alongside ACCEPTED.
            output_dir: Override output directory (defaults to data/output/).

        Returns:
            Dict with paths to generated files.
        """
        out = output_dir or OUTPUT_DIR
        out.mkdir(parents=True, exist_ok=True)

        rows = self._build_report_rows(include_review)

        if not rows:
            logger.warning("No events to report")
            return {}

        df = pd.DataFrame(rows)

        # Sort by confidence_score desc, then artist_name
        df = df.sort_values(
            ["confidence_score", "artist_name"],
            ascending=[False, True],
        ).reset_index(drop=True)

        csv_path = out / "artists_with_events.csv"
        json_path = out / "artists_with_events.json"

        df.to_csv(csv_path, index=False, encoding="utf-8")
        logger.info("CSV report saved: %s (%d rows)", csv_path, len(df))

        # JSON: list of records
        records = df.to_dict(orient="records")
        json_path.write_text(
            json.dumps(records, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info("JSON report saved: %s (%d records)", json_path, len(records))

        return {
            "csv": str(csv_path),
            "json": str(json_path),
            "rows": len(df),
        }

    def _build_report_rows(self, include_review: bool) -> List[Dict]:
        """Query events with artist/playlist/track info and build flat rows."""
        statuses = ["ACCEPTED"]
        if include_review:
            statuses.append("REVIEW")

        events = (
            self._session.query(Event)
            .filter(Event.match_status.in_(statuses))
            .all()
        )

        rows = []
        for event in events:
            artist = self._session.get(Artist, event.artist_id)
            if artist is None:
                continue

            # Gather playlists and tracks this artist appears in
            playlists_found = self._get_artist_playlists(artist)
            tracks_found = self._get_artist_tracks(artist)

            rows.append({
                "artist_name": artist.name,
                "spotify_artist_id": artist.spotify_artist_id,
                "event_name": event.event_name,
                "event_date": event.date,
                "city": event.city,
                "venue": event.venue,
                "country_code": event.country,
                "ticket_url": event.url,
                "source": "ticketmaster",
                "playlists_found_in": "; ".join(playlists_found),
                "tracks_found_in": "; ".join(tracks_found),
                "confidence_score": event.confidence_score,
                "match_status": event.match_status,
            })

        return rows

    @staticmethod
    def _get_artist_playlists(artist: Artist) -> List[str]:
        """Get playlist names where this artist's tracks appear."""
        playlist_names = set()
        for track in artist.tracks:
            for playlist in track.playlists:
                playlist_names.add(playlist.name)
        return sorted(playlist_names)

    @staticmethod
    def _get_artist_tracks(artist: Artist) -> List[str]:
        """Get track names for this artist."""
        return sorted(set(track.name for track in artist.tracks))
