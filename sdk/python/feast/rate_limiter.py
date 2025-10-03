import threading
import time
from abc import ABC, abstractmethod
from typing import Optional


class BaseRateLimiter(ABC):
    """Abstract base class for rate limiters used by Feast online stores.

    Implementations must provide a non-blocking acquire() that returns True if the
    caller is allowed to proceed, or False if the request is over the limit for the
    current window.
    """

    @abstractmethod
    def acquire(self) -> bool:  # pragma: no cover - interface
        """Attempt to acquire permission for one operation.

        Returns:
            bool: True if the caller can proceed, False otherwise.
        """
        pass

    def acquire_blocking(
        self, sleep_interval: float = 0.001, timeout: Optional[float] = None
    ) -> bool:
        """Blocking helper that will retry acquire() until successful or timeout.

        Args:
            sleep_interval: Seconds to sleep between attempts.
            timeout: Optional overall timeout in seconds. None means wait indefinitely.

        Returns:
            bool: True if acquired, False if timed out.
        """
        if timeout is None:
            while not self.acquire():  # pragma: no branch
                time.sleep(sleep_interval)
            return True
        else:
            deadline = time.time() + timeout
            while time.time() < deadline:
                if self.acquire():
                    return True
                time.sleep(sleep_interval)
            return False


class NoOpRateLimiter(BaseRateLimiter):
    """Rate limiter that never limits (used when limit is disabled)."""

    def acquire(self) -> bool:
        return True


class SlidingWindowRateLimiter(BaseRateLimiter):
    """Simple sliding window rate limiter.

    Allows up to `max_calls` within the last `period` seconds. This implementation
    uses a circular buffer of timestamps and is thread-safe.
    """

    def __init__(self, max_calls: int, period: float):
        if max_calls < 0:
            raise ValueError("max_calls must be >= 0")
        if period <= 0:
            raise ValueError("period must be > 0")
        self.max_calls = max_calls
        self.period = period
        # Preallocate list of timestamps; initialize to epoch 0 so they are always <= (now - period)
        self.timestamps = [0.0] * max_calls if max_calls > 0 else []
        self.index = 0
        self._lock = threading.Lock()

    def acquire(self) -> bool:
        # Fast path for disabled limiter (max_calls == 0)
        if self.max_calls == 0:
            return True
        now = time.time()
        window_start = now - self.period
        with self._lock:
            oldest = self.timestamps[self.index]
            if oldest <= window_start:
                # We can reuse this slot
                self.timestamps[self.index] = now
                self.index = (self.index + 1) % self.max_calls
                return True
            else:
                return False


def create_rate_limiter(
    max_calls: Optional[int], period: float = 1.0
) -> BaseRateLimiter:
    """Factory to create an appropriate rate limiter instance.

    Args:
        max_calls: Maximum number of calls per `period`. If 0, None, or <= 0, returns a NoOpRateLimiter.
        period: Length of the sliding time window in seconds.

    Returns:
        BaseRateLimiter: A configured rate limiter implementation.
    """
    if not max_calls or max_calls <= 0:
        return NoOpRateLimiter()
    return SlidingWindowRateLimiter(int(max_calls), period)


__all__ = [
    "BaseRateLimiter",
    "NoOpRateLimiter",
    "SlidingWindowRateLimiter",
    "create_rate_limiter",
]
