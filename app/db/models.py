from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Table,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


# ── Many-to-Many association tables ──────────────────────────────────────────

track_artists = Table(
    "track_artists",
    Base.metadata,
    Column(
        "spotify_track_id",
        String,
        ForeignKey("tracks.spotify_track_id"),
        primary_key=True,
    ),
    Column(
        "spotify_artist_id",
        String,
        ForeignKey("artists.spotify_artist_id"),
        primary_key=True,
    ),
)

playlist_tracks = Table(
    "playlist_tracks",
    Base.metadata,
    Column(
        "spotify_playlist_id",
        String,
        ForeignKey("playlists.spotify_playlist_id"),
        primary_key=True,
    ),
    Column(
        "spotify_track_id",
        String,
        ForeignKey("tracks.spotify_track_id"),
        primary_key=True,
    ),
)


# ── Core models ──────────────────────────────────────────────────────────────

class Artist(Base):
    __tablename__ = "artists"

    spotify_artist_id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    normalized_name = Column(String, nullable=False)

    tracks = relationship(
        "Track",
        secondary=track_artists,
        back_populates="artists",
    )
    events = relationship("Event", back_populates="artist")

    def __repr__(self) -> str:
        return f"<Artist {self.name!r} ({self.spotify_artist_id})>"


class Track(Base):
    __tablename__ = "tracks"

    spotify_track_id = Column(String, primary_key=True)
    name = Column(String, nullable=False)

    artists = relationship(
        "Artist",
        secondary=track_artists,
        back_populates="tracks",
    )
    playlists = relationship(
        "Playlist",
        secondary=playlist_tracks,
        back_populates="tracks",
    )

    def __repr__(self) -> str:
        return f"<Track {self.name!r} ({self.spotify_track_id})>"


class Playlist(Base):
    __tablename__ = "playlists"

    spotify_playlist_id = Column(String, primary_key=True)
    name = Column(String, nullable=False)

    tracks = relationship(
        "Track",
        secondary=playlist_tracks,
        back_populates="playlists",
    )

    def __repr__(self) -> str:
        return f"<Playlist {self.name!r} ({self.spotify_playlist_id})>"


class Event(Base):
    __tablename__ = "events"

    event_id = Column(String, primary_key=True)
    artist_id = Column(
        String,
        ForeignKey("artists.spotify_artist_id"),
        nullable=False,
    )
    event_name = Column(String, nullable=False)
    venue = Column(String)
    city = Column(String)
    country = Column(String)
    date = Column(String)  # ISO date string YYYY-MM-DD
    url = Column(Text)
    confidence_score = Column(Float, default=0.0)
    match_status = Column(String, default="PENDING")  # ACCEPTED | REVIEW | REJECTED

    artist = relationship("Artist", back_populates="events")

    def __repr__(self) -> str:
        return f"<Event {self.event_name!r} ({self.event_id})>"


class EventCache(Base):
    __tablename__ = "event_cache"

    cache_key = Column(String, primary_key=True)  # "{artist_id}:{country}:{date_from}:{date_to}"
    response_json = Column(Text, nullable=False)
    fetched_at = Column(DateTime, default=datetime.utcnow)
    ttl_hours = Column(Integer, default=24)

    def __repr__(self) -> str:
        return f"<EventCache {self.cache_key!r}>"
