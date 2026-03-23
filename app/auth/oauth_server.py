"""Lightweight local HTTP server to capture Spotify OAuth callback."""

import secrets
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse

from app.config import get_settings
from app.utils.logging import get_logger

logger = get_logger(__name__)

SPOTIFY_AUTH_URL = "https://accounts.spotify.com/authorize"
SCOPES = "playlist-read-private playlist-read-collaborative"


class _CallbackHandler(BaseHTTPRequestHandler):
    """HTTP handler that captures the authorization code from Spotify redirect."""

    auth_code: Optional[str] = None
    expected_state: Optional[str] = None
    error: Optional[str] = None

    def do_GET(self, *args, **kwargs):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        if "error" in params:
            _CallbackHandler.error = params["error"][0]
            self._respond(400, f"Authorization failed: {_CallbackHandler.error}")
            return

        # Verify state to prevent CSRF
        received_state = params.get("state", [None])[0]
        if received_state != _CallbackHandler.expected_state:
            _CallbackHandler.error = "State mismatch — possible CSRF attack"
            self._respond(400, _CallbackHandler.error)
            return

        code = params.get("code", [None])[0]
        if not code:
            _CallbackHandler.error = "No authorization code received"
            self._respond(400, _CallbackHandler.error)
            return

        _CallbackHandler.auth_code = code
        self._respond(
            200,
            "Authorization successful! You can close this tab and return to the terminal.",
        )

    def _respond(self, status: int, message: str):
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        html = (
            f"<html><body style='font-family:sans-serif;text-align:center;padding:50px'>"
            f"<h2>{message}</h2></body></html>"
        )
        self.wfile.write(html.encode("utf-8"))

    def log_message(self, format, *args):
        """Suppress default stderr logging from BaseHTTPRequestHandler."""
        pass


def request_authorization_code() -> str:
    """Open browser for Spotify login and return the authorization code.

    Spins up a temporary HTTP server on the redirect URI port,
    opens the browser to the Spotify auth page, waits for the callback,
    then shuts down the server and returns the code.

    Raises:
        RuntimeError: If authorization fails or is denied.
    """
    settings = get_settings()

    parsed_redirect = urlparse(settings.spotify_redirect_uri)
    host = parsed_redirect.hostname or "localhost"
    port = parsed_redirect.port or 8080

    # Generate random state for CSRF protection
    state = secrets.token_urlsafe(32)
    _CallbackHandler.expected_state = state
    _CallbackHandler.auth_code = None
    _CallbackHandler.error = None

    auth_params = urlencode(
        {
            "client_id": settings.spotify_client_id,
            "response_type": "code",
            "redirect_uri": settings.spotify_redirect_uri,
            "scope": SCOPES,
            "state": state,
        }
    )
    auth_url = f"{SPOTIFY_AUTH_URL}?{auth_params}"

    server = HTTPServer((host, port), _CallbackHandler)
    server.timeout = 120  # 2 minutes to complete auth

    logger.info("Opening browser for Spotify authorization...")
    logger.info("If the browser doesn't open, visit: %s", auth_url)
    webbrowser.open(auth_url)

    # Handle exactly one request (the callback)
    server.handle_request()
    server.server_close()

    if _CallbackHandler.error:
        raise RuntimeError(f"Spotify authorization failed: {_CallbackHandler.error}")

    if not _CallbackHandler.auth_code:
        raise RuntimeError("No authorization code received (timeout or unknown error)")

    logger.info("Authorization code received successfully")
    return _CallbackHandler.auth_code
