import os
from pathlib import Path
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


# Project root — directory containing the 'app' package
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Spotify
    spotify_client_id: str = ""
    spotify_client_secret: str = ""
    spotify_redirect_uri: str = "http://localhost:8080/callback"

    # Ticketmaster
    ticketmaster_api_key: str = ""

    # Database
    database_url: str = f"sqlite:///{DATA_DIR / 'db.sqlite'}"

    # Token storage
    token_path: str = str(DATA_DIR / "token.json")

    # Cache
    cache_ttl_hours: int = 24

    # Concurrency
    max_concurrent_requests: int = 5

    # Logging
    log_level: str = "INFO"


_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Return cached singleton Settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
