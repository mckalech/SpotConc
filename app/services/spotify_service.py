"""Service layer for syncing Spotify data into the local database."""

from typing import Any, Dict, List

from sqlalchemy.orm import Session

from app.clients.spotify_client import SpotifyClient
from app.db.models import Artist, Playlist, Track, playlist_tracks, track_artists
from app.utils.logging import get_logger
from app.utils.normalization import normalize_artist_name

logger = get_logger(__name__)


class SpotifyService:
    """Orchestrates fetching Spotify data and persisting it."""

    def __init__(self, client: SpotifyClient, session: Session):
        self._client = client
        self._session = session

    def sync_all(self, max_tracks: int = 0) -> Dict[str, int]:
        """Sync Liked Songs from Spotify: tracks -> artists -> DB.

        Args:
            max_tracks: Maximum number of tracks to fetch. 0 = all.

        Returns:
            Stats dict with counts of synced entities.
        """
        stats = {
            "tracks": 0,
            "artists": 0,
            "skipped_tracks": 0,
        }

        try:
            raw_tracks = self._client.get_saved_tracks(max_items=max_tracks)
        except Exception as exc:
            logger.error("Failed to fetch Liked Songs: %s", exc)
            return stats

        logger.info("Fetched %d Liked Songs items", len(raw_tracks))

        # Virtual playlist for Liked Songs
        playlist = self._upsert_playlist("liked_songs", "Liked Songs")

        for i, track_item in enumerate(raw_tracks, 1):
            track_data = track_item.get("track")

            # Skip null / unavailable tracks
            if not track_data or not track_data.get("id"):
                stats["skipped_tracks"] += 1
                continue

            track = self._upsert_track(track_data)
            stats["tracks"] += 1

            self._link_playlist_track(playlist, track)

            # Process artists on this track
            for artist_data in track_data.get("artists", []):
                if not artist_data.get("id"):
                    continue
                artist = self._upsert_artist(artist_data)
                stats["artists"] += 1
                self._link_track_artist(track, artist)

            # Commit every 200 tracks to avoid huge transactions
            if i % 200 == 0:
                self._session.commit()
                logger.info("Progress: %d/%d tracks processed", i, len(raw_tracks))

        self._session.commit()

        # Count unique artists
        unique_artists = self._session.query(Artist).count()

        logger.info(
            "Sync complete: %d tracks, %d unique artists (skipped %d tracks)",
            stats["tracks"],
            unique_artists,
            stats["skipped_tracks"],
        )
        return stats

    def _upsert_playlist(self, playlist_id: str, name: str) -> Playlist:
        """Insert or update a playlist record."""
        playlist = self._session.get(Playlist, playlist_id)
        if playlist is None:
            playlist = Playlist(spotify_playlist_id=playlist_id, name=name)
            self._session.add(playlist)
        else:
            playlist.name = name
        return playlist

    def _upsert_track(self, track_data: Dict[str, Any]) -> Track:
        """Insert or update a track record."""
        track_id = track_data["id"]
        track_name = track_data.get("name", "Unknown")

        track = self._session.get(Track, track_id)
        if track is None:
            track = Track(spotify_track_id=track_id, name=track_name)
            self._session.add(track)
        else:
            track.name = track_name
        return track

    def _upsert_artist(self, artist_data: Dict[str, Any]) -> Artist:
        """Insert or update an artist record with normalized name."""
        artist_id = artist_data["id"]
        artist_name = artist_data.get("name", "Unknown")

        artist = self._session.get(Artist, artist_id)
        if artist is None:
            artist = Artist(
                spotify_artist_id=artist_id,
                name=artist_name,
                normalized_name=normalize_artist_name(artist_name),
            )
            self._session.add(artist)
        else:
            artist.name = artist_name
            artist.normalized_name = normalize_artist_name(artist_name)
        return artist

    def _link_playlist_track(self, playlist: Playlist, track: Track) -> None:
        """Add track to playlist if not already linked."""
        if track not in playlist.tracks:
            playlist.tracks.append(track)

    def _link_track_artist(self, track: Track, artist: Artist) -> None:
        """Add artist to track if not already linked."""
        if artist not in track.artists:
            track.artists.append(artist)
