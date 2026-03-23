"""Manage Spotify OAuth tokens: exchange, refresh, persist to disk."""

import json
import time
from pathlib import Path
from typing import Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from app.config import get_settings
from app.utils.logging import get_logger

logger = get_logger(__name__)

SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"


class TokenData:
    """In-memory representation of OAuth token data."""

    def __init__(
        self,
        access_token: str,
        refresh_token: str,
        expires_at: float,
    ):
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.expires_at = expires_at

    @property
    def is_expired(self) -> bool:
        # Consider expired 60s before actual expiry to avoid edge cases
        return time.time() >= (self.expires_at - 60)

    def to_dict(self) -> dict:
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "expires_at": self.expires_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "TokenData":
        return cls(
            access_token=data["access_token"],
            refresh_token=data["refresh_token"],
            expires_at=data["expires_at"],
        )


class TokenManager:
    """Handle token exchange, refresh, and file-based persistence."""

    def __init__(self):
        self._settings = get_settings()
        self._token_path = Path(self._settings.token_path)
        self._token: Optional[TokenData] = None

    def get_access_token(self) -> str:
        """Return a valid access token, refreshing if needed.

        Raises:
            RuntimeError: If no token is available and authorization is required.
        """
        if self._token is None:
            self._token = self._load_from_disk()

        if self._token is None:
            raise RuntimeError(
                "No saved token found. Run 'sync-spotify' to authorize."
            )

        if self._token.is_expired:
            logger.info("Access token expired, refreshing...")
            self._refresh()

        return self._token.access_token

    def has_token(self) -> bool:
        """Check if a saved token exists (may be expired)."""
        if self._token is not None:
            return True
        return self._token_path.exists()

    @retry(
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TransportError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def exchange_code(self, code: str) -> None:
        """Exchange authorization code for access + refresh tokens.

        Args:
            code: The authorization code from Spotify callback.
        """
        logger.info("Exchanging authorization code for tokens...")

        response = httpx.post(
            SPOTIFY_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": self._settings.spotify_redirect_uri,
                "client_id": self._settings.spotify_client_id,
                "client_secret": self._settings.spotify_client_secret,
            },
            timeout=30.0,
        )
        response.raise_for_status()
        data = response.json()

        self._token = TokenData(
            access_token=data["access_token"],
            refresh_token=data["refresh_token"],
            expires_at=time.time() + data["expires_in"],
        )
        self._save_to_disk()
        logger.info("Tokens saved successfully")

    @retry(
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TransportError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _refresh(self) -> None:
        """Use refresh token to obtain a new access token."""
        if self._token is None or not self._token.refresh_token:
            raise RuntimeError("No refresh token available. Re-authorize.")

        response = httpx.post(
            SPOTIFY_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": self._token.refresh_token,
                "client_id": self._settings.spotify_client_id,
                "client_secret": self._settings.spotify_client_secret,
            },
            timeout=30.0,
        )
        response.raise_for_status()
        data = response.json()

        self._token.access_token = data["access_token"]
        self._token.expires_at = time.time() + data["expires_in"]

        # Spotify may return a new refresh token
        if "refresh_token" in data:
            self._token.refresh_token = data["refresh_token"]

        self._save_to_disk()
        logger.info("Token refreshed successfully")

    def _save_to_disk(self) -> None:
        """Persist token data to JSON file."""
        if self._token is None:
            return
        self._token_path.parent.mkdir(parents=True, exist_ok=True)
        self._token_path.write_text(
            json.dumps(self._token.to_dict(), indent=2),
            encoding="utf-8",
        )

    def _load_from_disk(self) -> Optional[TokenData]:
        """Load token data from JSON file, if it exists."""
        if not self._token_path.exists():
            return None
        try:
            data = json.loads(self._token_path.read_text(encoding="utf-8"))
            logger.info("Loaded saved token from %s", self._token_path)
            return TokenData.from_dict(data)
        except (json.JSONDecodeError, KeyError) as exc:
            logger.warning("Failed to load token file: %s", exc)
            return None
