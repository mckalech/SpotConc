from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.config import get_settings
from app.db.models import Base


_engine = None
_session_factory = None


def _get_engine():
    """Create and cache the SQLAlchemy engine."""
    global _engine
    if _engine is None:
        settings = get_settings()
        db_url = settings.database_url

        # Ensure the directory for the sqlite file exists
        if db_url.startswith("sqlite:///"):
            db_path = Path(db_url.replace("sqlite:///", ""))
            db_path.parent.mkdir(parents=True, exist_ok=True)

        _engine = create_engine(
            db_url,
            echo=False,
            connect_args={"check_same_thread": False},  # sqlite-specific
        )
    return _engine


def get_session_factory() -> sessionmaker:
    """Return a cached sessionmaker bound to the engine."""
    global _session_factory
    if _session_factory is None:
        _session_factory = sessionmaker(bind=_get_engine())
    return _session_factory


def get_session() -> Session:
    """Create a new session instance."""
    factory = get_session_factory()
    return factory()


def init_db() -> None:
    """Create all tables if they don't exist."""
    engine = _get_engine()
    Base.metadata.create_all(engine)
