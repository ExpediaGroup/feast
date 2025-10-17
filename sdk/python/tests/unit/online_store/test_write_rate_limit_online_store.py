from types import SimpleNamespace

import feast.infra.passthrough_provider as pt
from feast.infra.passthrough_provider import PassthroughProvider
from feast.repo_config import RepoConfig


class FakeLimiter:
    """A fake SlidingWindowRateLimiter used to observe wait() calls without blocking."""

    def __init__(self, max_calls, period):
        self.max_calls = max_calls
        self.period = period
        self.wait_called = False

    def wait(self):
        self.wait_called = True


def _make_repo_with_store(write_rate_limit: int):
    # Create a RepoConfig and set its public online_config (alias for online_store)
    # to a simple object exposing write_rate_limit. This mirrors how real code
    # config objects are provided to providers.
    repo = RepoConfig(project="proj", registry="registry")
    repo.online_config = SimpleNamespace(write_rate_limit=write_rate_limit)
    return repo


def test_wait_called_when_limit_positive(monkeypatch):
    """When the resolved write rate limit is > 0, provider should call limiter.wait()."""
    repo = _make_repo_with_store(write_rate_limit=5)

    provider = PassthroughProvider(repo)

    # install fake limiter so we can observe wait() without sleeping
    monkeypatch.setattr(pt, "SlidingWindowRateLimiter", FakeLimiter)

    # inject a dummy online store that records that it was called
    called = {"flag": False}

    def _online_write_batch(config, table, data, progress):
        called["flag"] = True

    provider._online_store = SimpleNamespace(online_write_batch=_online_write_batch)

    table = SimpleNamespace(name="fv", tags=None)

    provider.online_write_batch(repo, table, data=[], progress=None)

    key = f"{repo.project}:{table.name}"
    assert key in provider._write_rate_limiters
    limiter = provider._write_rate_limiters[key]
    assert isinstance(limiter, FakeLimiter)
    assert limiter.wait_called is True
    assert called["flag"] is True


def test_no_wait_when_limit_zero(monkeypatch):
    """When the resolved write rate limit is 0, provider should NOT call limiter.wait()."""
    repo = _make_repo_with_store(write_rate_limit=0)
    provider = PassthroughProvider(repo)

    monkeypatch.setattr(pt, "SlidingWindowRateLimiter", FakeLimiter)

    called = {"flag": False}

    def _online_write_batch(config, table, data, progress):
        called["flag"] = True

    provider._online_store = SimpleNamespace(online_write_batch=_online_write_batch)

    table = SimpleNamespace(name="fv", tags=None)

    provider.online_write_batch(repo, table, data=[], progress=None)

    key = f"{repo.project}:{table.name}"
    assert key in provider._write_rate_limiters
    limiter = provider._write_rate_limiters[key]
    assert isinstance(limiter, FakeLimiter)
    # max_calls is zero, so wait should not have been called
    assert limiter.wait_called is False
    assert called["flag"] is True


def test_feature_view_tag_overrides_store_config(monkeypatch):
    """A feature view tag 'write_rate_limit' should take precedence over store-level config."""
    # store config has 1, feature view tag requests 3
    repo = _make_repo_with_store(write_rate_limit=1)
    provider = PassthroughProvider(repo)

    monkeypatch.setattr(pt, "SlidingWindowRateLimiter", FakeLimiter)

    provider._online_store = SimpleNamespace(online_write_batch=lambda *args, **kwargs: None)

    table = SimpleNamespace(name="fv", tags={"write_rate_limit": "3"})

    provider.online_write_batch(repo, table, data=[], progress=None)

    key = f"{repo.project}:{table.name}"
    limiter = provider._write_rate_limiters[key]
    assert limiter.max_calls == 3
