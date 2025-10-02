import threading
import time

from feast.rate_limiter import (
    NoOpRateLimiter,
    SlidingWindowRateLimiter,
    create_rate_limiter,
)


def test_create_rate_limiter_noop():
    rl = create_rate_limiter(0)
    assert isinstance(rl, NoOpRateLimiter)
    for _ in range(10):
        assert rl.acquire() is True


def test_sliding_window_basic_limit():
    rl = SlidingWindowRateLimiter(max_calls=2, period=0.2)
    assert rl.acquire() is True
    assert rl.acquire() is True
    # Third immediate call should be rejected
    assert rl.acquire() is False
    time.sleep(0.21)  # allow window to slide fully
    assert rl.acquire() is True  # should succeed after window moves


def test_acquire_blocking_waits():
    rl = SlidingWindowRateLimiter(max_calls=1, period=0.15)
    assert rl.acquire() is True
    start = time.time()
    rl.acquire_blocking()  # should block until the window frees (~0.15s)
    elapsed = time.time() - start
    assert elapsed >= 0.13  # allow a little timing slack


def test_sliding_window_concurrency():
    rl = SlidingWindowRateLimiter(max_calls=3, period=1.0)
    results = []
    lock = threading.Lock()

    def worker():
        # Single attempt per thread
        allowed = rl.acquire()
        with lock:
            results.append(allowed)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # At most 3 should have succeeded
    assert sum(results) <= 3


def test_sliding_window_invalid_args():
    try:
        SlidingWindowRateLimiter(max_calls=-1, period=1)
        assert False, "Expected ValueError for negative max_calls"
    except ValueError:
        pass
    try:
        SlidingWindowRateLimiter(max_calls=1, period=0)
        assert False, "Expected ValueError for non-positive period"
    except ValueError:
        pass

