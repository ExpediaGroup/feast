from importlib.metadata import PackageNotFoundError

import pytest

from feast.feature_store import _print_materialization_log
from feast.utils import _utc_now
from feast.version import get_installed_version, get_version


def test_get_version_returns_real_version():
    """get_version() should find whichever of the "feast" / "eg-feast"
    distributions is actually installed, not silently fall back to "unknown"."""
    assert get_version() != "unknown"


def test_get_version_falls_back_from_feast_to_eg_feast(monkeypatch):
    """Regression test: this repo publishes its SDK distribution as "eg-feast"
    (see setup.py), not "feast". get_version() must fall back to "eg-feast" when
    "feast" isn't installed, instead of only checking "feast" and returning
    "unknown"."""

    def fake_version(name):
        if name == "feast":
            raise PackageNotFoundError()
        if name == "eg-feast":
            return "1.2.3"
        raise PackageNotFoundError()

    monkeypatch.setattr("feast.version.version", fake_version)

    assert get_version() == "1.2.3"


def test_get_version_falls_back_from_eg_feast_to_feast(monkeypatch):
    """The reverse case: upstream OSS installs where the distribution is named
    "feast" rather than "eg-feast" must still resolve correctly."""

    def fake_version(name):
        if name == "feast":
            return "0.9.0"
        raise PackageNotFoundError()

    monkeypatch.setattr("feast.version.version", fake_version)

    assert get_version() == "0.9.0"


def test_get_version_returns_unknown_when_neither_installed(monkeypatch):
    def fake_version(name):
        raise PackageNotFoundError()

    monkeypatch.setattr("feast.version.version", fake_version)

    assert get_version() == "unknown"


def test_get_installed_version_for_missing_package():
    assert get_installed_version("definitely-not-a-real-package-xyz") == "not installed"


def test_get_installed_version_for_installed_package():
    # "feast" or "eg-feast" (whichever is actually installed) should resolve.
    result = get_installed_version("pytest")
    assert result != "not installed"
    assert pytest.__version__ == result


def test_materialization_log_always_includes_version_banner(caplog):
    """The version banner is always-on - it must appear regardless of any
    opt-in config, unlike the row-content logging."""
    now = _utc_now()

    with caplog.at_level("INFO"):
        _print_materialization_log(
            start_date=now, end_date=now, num_feature_views=1, online_store="sqlite"
        )

    assert "Materialization starting" in caplog.text
    assert "feast=" in caplog.text
    assert "feature-store-materialization=" in caplog.text
