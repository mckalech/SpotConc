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

    def sync_all(self) -> Dict[str, int]:
        """Run full Spotify sync: playlists -> tracks -> artists.

        Returns:
            Stats dict with counts of synced entities.
        """
        stats = {
            "playlists": 0,
            "tracks": 0,
            "artists": 0,
            "skipped_tracks": 0,
            "skipped_playlists": 0,
        }

        try:
            playlists = self._client.get_current_user_playlists()
        except Exception as exc:
            logger.error("Failed to fetch playlists: %s", exc)
            return stats

        logger.info("Found %d playlists", len(playlists))

        for i, pl_data in enumerate(playlists, 1):
            playlist_id = pl_data.get("id")
            playlist_name = pl_data.get("name", "Unknown")

            if not playlist_id:
                logger.warning("Skipping playlist with no ID: %s", playlist_name)
                stats["skipped_playlists"] += 1
                continue

            logger.info(
                "Syncing playlist %d/%d: %s",
                i,
                len(playlists),
                playlist_name,
            )

            # Upsert playlist
            playlist = self._upsert_playlist(playlist_id, playlist_name)
            stats["playlists"] += 1

            # Fetch and process tracks
            try:
                raw_tracks = self._client.get_playlist_tracks(playlist_id)
            except Exception as exc:
                logger.warning(
                    "Failed to fetch tracks for playlist %s (%s): %s",
                    playlist_name,
                    playlist_id,
                    exc,
                )
                stats["skipped_playlists"] += 1
                continue

            for track_item in raw_tracks:
                track_data = track_item.get("track")

                # Skip null / unavailable tracks
                if not track_data or not track_data.get("id"):
                    stats["skipped_tracks"] += 1
                    continue

                track = self._upsert_track(track_data)
                stats["tracks"] += 1

                # Link track to playlist
                self._link_playlist_track(playlist, track)

                # Process artists on this track
                for artist_data in track_data.get("artists", []):
                    if not artist_data.get("id"):
                        continue
                    artist = self._upsert_artist(artist_data)
                    stats["artists"] += 1
                    self._link_track_artist(track, artist)

            # Commit after each playlist to avoid huge transactions
            self._session.commit()

        logger.info(
            "Sync complete: %d playlists, %d tracks, %d artists "
            "(skipped %d tracks, %d playlists)",
            stats["playlists"],
            stats["tracks"],
            stats["artists"],
            stats["skipped_tracks"],
            stats["skipped_playlists"],
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
