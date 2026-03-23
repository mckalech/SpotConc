import logging
from datetime import datetime

import typer

from app.auth.oauth_server import request_authorization_code
from app.auth.token_manager import TokenManager
from app.clients.spotify_client import SpotifyClient
from app.clients.ticketmaster_client import TicketmasterClient
from app.db.session import get_session, init_db
from app.services.event_service import EventService
from app.services.matching_service import MatchingService
from app.services.report_service import ReportService
from app.services.spotify_service import SpotifyService
from app.utils.logging import setup_logging

logger = logging.getLogger(__name__)

app = typer.Typer(help="SpotConc — Spotify playlist concert finder")


@app.callback()
def main(verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging")):
    """Initialize logging and database on every invocation."""
    setup_logging(verbose=verbose)
    init_db()


def _ensure_spotify_auth(token_manager: TokenManager) -> None:
    """Run OAuth flow if no saved token exists."""
    if not token_manager.has_token():
        logger.info("No saved Spotify token found — starting authorization...")
        code = request_authorization_code()
        token_manager.exchange_code(code)


def _validate_date(value: str) -> str:
    """Validate date string is YYYY-MM-DD format."""
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise typer.BadParameter(f"Invalid date format: '{value}'. Use YYYY-MM-DD.")
    return value


# ── Individual commands ──────────────────────────────────────────────────────


@app.command()
def sync_spotify():
    """Fetch Liked Songs, tracks and artists from Spotify."""
    token_manager = TokenManager()
    _ensure_spotify_auth(token_manager)

    client = SpotifyClient(token_manager)
    session = get_session()

    try:
        service = SpotifyService(client, session)
        stats = service.sync_all()

        typer.echo(
            f"\nSync complete:\n"
            f"  Tracks:    {stats['tracks']}\n"
            f"  Artists:   {stats['artists']}\n"
            f"  Skipped:   {stats['skipped_tracks']} tracks"
        )
    finally:
        session.close()
        client.close()


@app.command()
def find_events(
    date_from: str = typer.Option(..., "--date-from", help="Start date (YYYY-MM-DD)"),
    date_to: str = typer.Option(..., "--date-to", help="End date (YYYY-MM-DD)"),
    country: str = typer.Option("GB", "--country", help="Country code (default: GB)"),
):
    """Search Ticketmaster for upcoming concerts and run matching."""
    date_from = _validate_date(date_from)
    date_to = _validate_date(date_to)

    session = get_session()
    tm_client = TicketmasterClient()

    try:
        # Fetch events
        event_service = EventService(tm_client, session)
        event_stats = event_service.find_events_for_all_artists(
            date_from=date_from,
            date_to=date_to,
            country=country,
        )

        # Run matching
        matching_service = MatchingService(session)
        match_stats = matching_service.process_all()

        typer.echo(
            f"\nEvent search complete:\n"
            f"  Artists processed:    {event_stats['artists_processed']}\n"
            f"  Artists with events:  {event_stats['artists_with_events']}\n"
            f"  Total events found:   {event_stats['events_found']}\n"
            f"  Events from cache:    {event_stats['events_from_cache']}\n"
            f"  Artists failed:       {event_stats['artists_failed']}\n"
            f"\nMatching results:\n"
            f"  ACCEPTED: {match_stats['ACCEPTED']}\n"
            f"  REVIEW:   {match_stats['REVIEW']}\n"
            f"  REJECTED: {match_stats['REJECTED']}"
        )
    finally:
        session.close()
        tm_client.close()


@app.command()
def report(
    include_review: bool = typer.Option(
        False, "--include-review", help="Include REVIEW events in report"
    ),
):
    """Generate CSV and JSON reports from matched events."""
    session = get_session()

    try:
        service = ReportService(session)
        result = service.generate(include_review=include_review)

        if not result:
            typer.echo("No events to report. Run find-events first.")
            return

        typer.echo(
            f"\nReport generated:\n"
            f"  Rows:  {result['rows']}\n"
            f"  CSV:   {result['csv']}\n"
            f"  JSON:  {result['json']}"
        )
    finally:
        session.close()


# ── Full pipeline ────────────────────────────────────────────────────────────


@app.command()
def run(
    date_from: str = typer.Option(..., "--date-from", help="Start date (YYYY-MM-DD)"),
    date_to: str = typer.Option(..., "--date-to", help="End date (YYYY-MM-DD)"),
    country: str = typer.Option("GB", "--country", help="Country code (default: GB)"),
    include_review: bool = typer.Option(
        False, "--include-review", help="Include REVIEW events in report"
    ),
):
    """Run the full pipeline: sync -> find events -> match -> report."""
    date_from = _validate_date(date_from)
    date_to = _validate_date(date_to)

    token_manager = TokenManager()
    _ensure_spotify_auth(token_manager)

    spotify_client = SpotifyClient(token_manager)
    tm_client = TicketmasterClient()
    session = get_session()

    try:
        # Step 1: Sync Spotify
        typer.echo("Step 1/4: Syncing Spotify data...")
        spotify_service = SpotifyService(spotify_client, session)
        sync_stats = spotify_service.sync_all()
        typer.echo(
            f"  -> {sync_stats['tracks']} tracks, "
            f"{sync_stats['artists']} artists"
        )

        # Step 2: Find events
        typer.echo("\nStep 2/4: Searching for events...")
        event_service = EventService(tm_client, session)
        event_stats = event_service.find_events_for_all_artists(
            date_from=date_from,
            date_to=date_to,
            country=country,
        )
        typer.echo(
            f"  -> {event_stats['events_found']} events found "
            f"for {event_stats['artists_with_events']} artists"
        )

        # Step 3: Match
        typer.echo("\nStep 3/4: Matching events...")
        matching_service = MatchingService(session)
        match_stats = matching_service.process_all()
        typer.echo(
            f"  -> {match_stats['ACCEPTED']} accepted, "
            f"{match_stats['REVIEW']} review, "
            f"{match_stats['REJECTED']} rejected"
        )

        # Step 4: Report
        typer.echo("\nStep 4/4: Generating report...")
        report_service = ReportService(session)
        result = report_service.generate(include_review=include_review)

        if result:
            typer.echo(
                f"  -> {result['rows']} rows\n"
                f"  -> CSV:  {result['csv']}\n"
                f"  -> JSON: {result['json']}"
            )
        else:
            typer.echo("  -> No matching events found")

        typer.echo("\nPipeline complete!")

    finally:
        session.close()
        spotify_client.close()
        tm_client.close()


if __name__ == "__main__":
    app()
