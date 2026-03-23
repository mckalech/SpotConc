"""Retry configuration using tenacity for HTTP calls."""

import time

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from app.utils.logging import get_logger

logger = get_logger(__name__)


def _is_retryable(exc: BaseException) -> bool:
    """Return True only for retryable errors: 429, 5xx, or transport errors."""
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code == 429 or code >= 500
    return False


def _wait_with_retry_after(retry_state) -> float:
    """Use Retry-After header if available, otherwise exponential backoff."""
    exc = retry_state.outcome.exception()
    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 429:
        retry_after = exc.response.headers.get("Retry-After")
        if retry_after:
            wait_seconds = int(retry_after)
            logger.info("Rate limited. Waiting %d seconds (Retry-After header)...", wait_seconds)
            return float(wait_seconds)
    # Fallback to exponential backoff
    attempt = retry_state.attempt_number
    return min(2 ** attempt, 60)


def _log_retry(retry_state) -> None:
    """Log each retry attempt."""
    exc = retry_state.outcome.exception()
    attempt = retry_state.attempt_number
    logger.warning(
        "Retry attempt %d after error: %s",
        attempt,
        str(exc)[:200],
    )


# Reusable retry decorator for HTTP calls
# Only retries on 429, 5xx, and transport errors (timeouts, connection drops).
# 4xx errors like 403 are NOT retried.
# Uses Retry-After header for 429 responses.
http_retry = retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(8),
    wait=_wait_with_retry_after,
    before_sleep=_log_retry,
    reraise=True,
)
