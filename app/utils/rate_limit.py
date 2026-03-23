"""Rate limiting utilities for API calls."""

import threading
import time
from typing import Optional

from app.utils.logging import get_logger

logger = get_logger(__name__)


class RateLimiter:
    """Thread-safe rate limiter using token bucket algorithm.

    Enforces a maximum number of requests per second across
    concurrent threads.

    Args:
        max_per_second: Maximum requests allowed per second.
        max_concurrent: Maximum concurrent in-flight requests.
    """

    def __init__(self, max_per_second: float = 5.0, max_concurrent: int = 5):
        self._min_interval = 1.0 / max_per_second
        self._semaphore = threading.Semaphore(max_concurrent)
        self._lock = threading.Lock()
        self._last_request_time: float = 0.0

    def acquire(self) -> None:
        """Block until a request slot is available."""
        self._semaphore.acquire()
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            if elapsed < self._min_interval:
                sleep_time = self._min_interval - elapsed
                time.sleep(sleep_time)
            self._last_request_time = time.monotonic()

    def release(self) -> None:
        """Release a request slot."""
        self._semaphore.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False
