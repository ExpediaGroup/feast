import time
from threading import Lock
from typing import List


class SlidingWindowRateLimiter:
    def __init__(self, max_calls, period):
        self.max_calls = max_calls
        self.period = period
        self.timestamps: List[float] = [0.0] * max_calls
        self.index = 0
        self._lock = Lock()

    def acquire(self):
        if self.max_calls == 0:
            return True
        now = time.time()
        window_start = now - self.period
        with self._lock:
            if self.timestamps[self.index] <= window_start:
                self.timestamps[self.index] = now
                self.index = (self.index + 1) % self.max_calls
                return True
        return False

    def wait(self):
        """Block until a slot is available.

        This method repeatedly attempts to acquire a slot. If unavailable, it
        sleeps for a short duration derived from remaining window time.
        """
        if self.max_calls == 0:
            return
        backoff = 0.005  # initial minimal sleep
        while not self.acquire():
            # Compute estimated sleep until oldest timestamp exits window.
            # We use the current index position as the next candidate slot.
            now = time.time()
            with self._lock:
                oldest_ts = self.timestamps[self.index]
            remaining = oldest_ts + self.period - now
            if remaining <= 0:
                continue
            # Sleep the smaller of remaining and a capped value to re-check frequently.
            time.sleep(min(remaining, 0.05))
            # Optional exponential backoff (bounded) if still not free.
            backoff = min(backoff * 2, 0.05)
