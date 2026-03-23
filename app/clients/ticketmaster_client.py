"""Low-level HTTP client for Ticketmaster Discovery API."""

from typing import Any, Dict, List, Optional

import httpx

from app.config import get_settings
from app.utils.logging import get_logger
from app.utils.rate_limit import RateLimiter
from app.utils.retry import http_retry

logger = get_logger(__name__)

TICKETMASTER_API_BASE = "https://app.ticketmaster.com/discovery/v2"


class TicketmasterClient:
    """Wrapper around Ticketmaster Discovery API with rate limiting and retry."""

    def __init__(self, rate_limiter: Optional[RateLimiter] = None):
        settings = get_settings()
        self._api_key = settings.ticketmaster_api_key
        self._rate_limiter = rate_limiter or RateLimiter(
            max_per_second=5.0,
            max_concurrent=settings.max_concurrent_requests,
        )
        self._client = httpx.Client(
            base_url=TICKETMASTER_API_BASE,
            timeout=30.0,
        )

    @http_retry
    def _get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make a rate-limited, retryable GET request.

        Args:
            endpoint: API endpoint path (e.g. "/events.json").
            params: Query parameters (apikey is auto-added).

        Returns:
            Parsed JSON response.
        """
        request_params = dict(params or {})
        request_params["apikey"] = self._api_key

        with self._rate_limiter:
            response = self._client.get(endpoint, params=request_params)
            response.raise_for_status()
            return response.json()

    def search_events(
        self,
        keyword: str,
        country_code: str = "GB",
        date_from: str = "",
        date_to: str = "",
        size: int = 50,
    ) -> List[Dict[str, Any]]:
        """Search for music events by artist keyword.

        Handles pagination to fetch all matching results.

        Args:
            keyword: Artist name to search for.
            country_code: ISO country code (e.g. "GB").
            date_from: Start date in YYYY-MM-DD format.
            date_to: End date in YYYY-MM-DD format.
            size: Results per page (max 200).

        Returns:
            List of event objects from Ticketmaster.
        """
        all_events = []
        page = 0

        # Ticketmaster expects datetime format: YYYY-MM-DDTHH:MM:SSZ
        start_date = f"{date_from}T00:00:00Z" if date_from else ""
        end_date = f"{date_to}T23:59:59Z" if date_to else ""

        while True:
            params = {
                "keyword": keyword,
                "countryCode": country_code,
                "classificationName": "music",
                "size": size,
                "page": page,
            }
            if start_date:
                params["startDateTime"] = start_date
            if end_date:
                params["endDateTime"] = end_date

            data = self._get("/events.json", params=params)

            # Ticketmaster wraps results in _embedded
            embedded = data.get("_embedded", {})
            events = embedded.get("events", [])
            all_events.extend(events)

            # Check pagination
            page_info = data.get("page", {})
            total_pages = page_info.get("totalPages", 1)
            current_page = page_info.get("number", 0)

            logger.debug(
                "Ticketmaster search '%s': page %d/%d, got %d events",
                keyword,
                current_page + 1,
                total_pages,
                len(events),
            )

            if current_page + 1 >= total_pages:
                break
            page += 1

        return all_events

    @staticmethod
    def parse_event(raw_event: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured fields from a raw Ticketmaster event.

        Args:
            raw_event: Raw event dict from API response.

        Returns:
            Dict with normalized event fields.
        """
        # Venue info
        venues = raw_event.get("_embedded", {}).get("venues", [])
        venue = venues[0] if venues else {}
        venue_name = venue.get("name", "")
        city_data = venue.get("city", {})
        city = city_data.get("name", "")
        country_data = venue.get("country", {})
        country_code = country_data.get("countryCode", "")

        # Date
        dates = raw_event.get("dates", {})
        start = dates.get("start", {})
        event_date = start.get("localDate", "")

        # URL
        url = raw_event.get("url", "")

        return {
            "event_id": raw_event.get("id", ""),
            "event_name": raw_event.get("name", ""),
            "venue": venue_name,
            "city": city,
            "country": country_code,
            "date": event_date,
            "url": url,
        }

    def close(self):
        """Close the underlying HTTP client."""
        self._client.close()
