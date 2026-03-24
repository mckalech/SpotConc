"""Low-level HTTP client for Spotify Web API."""

import time
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import httpx

from app.auth.token_manager import TokenManager
from app.utils.logging import get_logger
from app.utils.retry import http_retry

logger = get_logger(__name__)

SPOTIFY_API_BASE = "https://api.spotify.com/v1"


class SpotifyClient:
    """Thin wrapper around Spotify Web API with automatic auth and pagination."""

    def __init__(self, token_manager: TokenManager):
        self._token_manager = token_manager
        self._client = httpx.Client(
            base_url=SPOTIFY_API_BASE,
            timeout=30.0,
        )

    def _headers(self) -> Dict[str, str]:
        token = self._token_manager.get_access_token()
        return {"Authorization": f"Bearer {token}"}

    @http_retry
    def _get(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make an authenticated GET request with retry.

        Args:
            url: Full URL or path relative to base URL.
            params: Optional query parameters.

        Returns:
            Parsed JSON response.
        """
        response = self._client.get(
            url,
            headers=self._headers(),
            params=params,
        )
        response.raise_for_status()
        return response.json()

    def _paginate(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        limit: int = 50,
        max_items: int = 0,
    ) -> List[Dict[str, Any]]:
        """Fetch all pages from a paginated Spotify endpoint.

        Args:
            url: Initial endpoint URL.
            params: Optional query parameters.
            limit: Items per page (max 50).
            max_items: Stop after collecting this many items. 0 = no limit.

        Returns:
            List of all items across all pages.
        """
        all_items = []
        request_params = dict(params or {})
        request_params["limit"] = limit
        request_params.setdefault("offset", 0)

        while True:
            try:
                data = self._get(url, params=request_params)
            except Exception as exc:
                if all_items:
                    logger.warning(
                        "Pagination interrupted after %d items: %s. Returning partial results.",
                        len(all_items),
                        str(exc)[:200],
                    )
                    return all_items
                raise

            items = data.get("items", [])
            all_items.extend(items)

            logger.info(
                "Fetched %d items (total so far: %d)",
                len(items),
                len(all_items),
            )

            # Stop if we've reached the requested limit
            if max_items > 0 and len(all_items) >= max_items:
                all_items = all_items[:max_items]
                break

            # Check if there are more pages
            next_url = data.get("next")
            if not next_url:
                break

            # Small delay between pages to avoid rate limiting
            time.sleep(0.1)

            # Extract path and params from next URL
            # (httpx base_url drops params from absolute URLs, so we parse manually)
            parsed = urlparse(next_url)
            url = parsed.path  # e.g. "/v1/me/tracks"
            # Strip the /v1 prefix since base_url already includes it
            if url.startswith("/v1"):
                url = url[3:]
            request_params = {k: v[0] for k, v in parse_qs(parsed.query).items()}

        return all_items

    def get_current_user_playlists(self) -> List[Dict[str, Any]]:
        """Fetch all playlists for the current user."""
        logger.info("Fetching user playlists...")
        return self._paginate("/me/playlists")

    def get_playlist_tracks(self, playlist_id: str) -> List[Dict[str, Any]]:
        """Fetch all tracks from a specific playlist.

        Args:
            playlist_id: Spotify playlist ID.

        Returns:
            List of playlist track objects (each containing a 'track' field).
        """
        logger.info("Fetching tracks for playlist %s...", playlist_id)
        return self._paginate(f"/playlists/{playlist_id}/tracks")

    def get_saved_tracks(self, max_items: int = 0) -> List[Dict[str, Any]]:
        """Fetch Liked Songs (saved tracks) for the current user.

        Args:
            max_items: Maximum number of tracks to fetch. 0 = all.

        Returns:
            List of saved track objects (each containing a 'track' field).
        """
        if max_items > 0:
            logger.info("Fetching up to %d Liked Songs...", max_items)
        else:
            logger.info("Fetching all Liked Songs...")
        return self._paginate("/me/tracks", max_items=max_items)

    def close(self):
        """Close the underlying HTTP client."""
        self._client.close()
