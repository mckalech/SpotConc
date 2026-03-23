"""Retry configuration using tenacity for HTTP calls."""

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from app.utils.logging import get_logger

logger = get_logger(__name__)


def _is_retryable_status(exc: BaseException) -> bool:
    """Check if an HTTP error has a retryable status code (429, 5xx)."""
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code == 429 or exc.response.status_code >= 500
    return False


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
http_retry = retry(
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TransportError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    before_sleep=_log_retry,
    reraise=True,
)
