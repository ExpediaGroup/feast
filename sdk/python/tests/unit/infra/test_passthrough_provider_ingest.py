"""Unit tests for the parallel ingest path in ``PassthroughProvider.ingest_df``.

Covers worker-count selection (the FEAST_INGEST_WORKERS override versus the
legacy SPARK_DRIVER_CORES sizing) and the bounded wait on the worker pool. A
fake ``Pool`` stands in for ``multiprocessing.Pool`` so nothing is forked.
"""

import multiprocessing
import os
from types import SimpleNamespace

import pandas as pd
import pytest

import feast.infra.passthrough_provider as pt
from feast.errors import IngestWorkerHungError
from feast.infra.passthrough_provider import PassthroughProvider
from feast.repo_config import RepoConfig


class FakeTable:
    def __init__(self, num_rows: int):
        self.num_rows = num_rows

    def slice(self, offset, length):
        return ("chunk", offset, length)


class FakeAsyncResult:
    def __init__(self, pool, func, iterable):
        self._pool = pool
        self._func = func
        self._iterable = list(iterable)

    def get(self, timeout=None):
        self._pool.get_timeouts.append(timeout)
        if FakePool.hang:
            raise multiprocessing.TimeoutError()
        if FakePool.worker_error is not None:
            raise FakePool.worker_error
        for args in self._iterable:
            self._func(*args)


class FakePool:
    """Stands in for ``multiprocessing.Pool``; runs tasks inline on ``get()``."""

    instances: list = []
    hang = False
    worker_error = None

    def __init__(self, processes=None):
        self.processes = processes
        self.terminate_calls = 0
        self.get_timeouts = []
        FakePool.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        # Mirrors the real Pool: the context manager always terminates.
        self.terminate()
        return False

    def starmap_async(self, func, iterable):
        return FakeAsyncResult(self, func, iterable)

    def starmap(self, func, iterable):
        raise AssertionError("ingest_df must wait via starmap_async with a timeout")

    def terminate(self):
        self.terminate_calls += 1


@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch):
    for var in (
        "SPARK_DRIVER_CORES",
        "NUM_PROCESSES",
        pt.INGEST_WORKERS_ENV,
        pt.INGEST_POOL_TIMEOUT_ENV,
    ):
        monkeypatch.delenv(var, raising=False)
    FakePool.instances = []
    FakePool.hang = False
    FakePool.worker_error = None
    monkeypatch.setattr(pt, "Pool", FakePool)
    yield
    # ingest_df writes NUM_PROCESSES straight into os.environ.
    os.environ.pop("NUM_PROCESSES", None)


def _make_provider(monkeypatch, num_rows: int):
    provider = PassthroughProvider(
        RepoConfig(project="test_project", registry="test_registry")
    )
    table = FakeTable(num_rows)
    monkeypatch.setattr(
        provider,
        "_prep_table_and_join_keys_for_ingestion",
        lambda **kwargs: (table, ["driver_id"]),
    )
    calls = []
    monkeypatch.setattr(
        provider,
        "process",
        lambda chunk, fv, join_keys: calls.append((chunk, fv, join_keys)),
    )
    return provider, table, calls


FV = SimpleNamespace(name="driver_hourly_stats")


def test_no_env_writes_in_process(monkeypatch):
    provider, table, calls = _make_provider(monkeypatch, num_rows=4535)

    provider.ingest_df(FV, pd.DataFrame())

    assert FakePool.instances == []
    assert calls == [(table, FV, ["driver_id"])]
    assert os.environ["NUM_PROCESSES"] == "1"


def test_legacy_driver_cores_sizing_forks_cores_minus_one(monkeypatch):
    monkeypatch.setenv("SPARK_DRIVER_CORES", "12")
    provider, _, calls = _make_provider(monkeypatch, num_rows=4535)

    provider.ingest_df(FV, pd.DataFrame())

    (pool,) = FakePool.instances
    assert pool.processes == 11
    assert len(calls) == 11
    # every row lands in exactly one chunk
    assert sum(chunk[2] for chunk, _, _ in calls) == 4535
    assert os.environ["NUM_PROCESSES"] == "11"


def test_two_or_fewer_driver_cores_writes_in_process(monkeypatch):
    monkeypatch.setenv("SPARK_DRIVER_CORES", "2")
    provider, table, calls = _make_provider(monkeypatch, num_rows=4535)

    provider.ingest_df(FV, pd.DataFrame())

    assert FakePool.instances == []
    assert calls == [(table, FV, ["driver_id"])]


def test_worker_override_of_one_disables_forking(monkeypatch):
    monkeypatch.setenv("SPARK_DRIVER_CORES", "12")
    monkeypatch.setenv(pt.INGEST_WORKERS_ENV, "1")
    provider, table, calls = _make_provider(monkeypatch, num_rows=4535)

    provider.ingest_df(FV, pd.DataFrame())

    assert FakePool.instances == []
    assert calls == [(table, FV, ["driver_id"])]
    assert os.environ["NUM_PROCESSES"] == "1"


def test_worker_override_sets_pool_size(monkeypatch):
    monkeypatch.setenv("SPARK_DRIVER_CORES", "12")
    monkeypatch.setenv(pt.INGEST_WORKERS_ENV, "4")
    provider, _, calls = _make_provider(monkeypatch, num_rows=4535)

    provider.ingest_df(FV, pd.DataFrame())

    (pool,) = FakePool.instances
    assert pool.processes == 4
    assert len(calls) == 4
    assert os.environ["NUM_PROCESSES"] == "4"


def test_worker_count_never_exceeds_row_count(monkeypatch):
    monkeypatch.setenv(pt.INGEST_WORKERS_ENV, "4")
    provider, _, calls = _make_provider(monkeypatch, num_rows=3)

    provider.ingest_df(FV, pd.DataFrame())

    (pool,) = FakePool.instances
    assert pool.processes == 3
    assert len(calls) == 3


def test_invalid_worker_override_falls_back_to_legacy_sizing(monkeypatch):
    monkeypatch.setenv("SPARK_DRIVER_CORES", "4")
    monkeypatch.setenv(pt.INGEST_WORKERS_ENV, "lots")
    provider, _, calls = _make_provider(monkeypatch, num_rows=4535)

    provider.ingest_df(FV, pd.DataFrame())

    (pool,) = FakePool.instances
    assert pool.processes == 3
    assert len(calls) == 3


@pytest.mark.parametrize(
    "env_value, expected",
    [
        (None, pt.DEFAULT_INGEST_POOL_TIMEOUT_SECONDS),
        ("45", 45.0),
        ("0", None),
        ("-1", None),
        ("soon", pt.DEFAULT_INGEST_POOL_TIMEOUT_SECONDS),
        ("  ", pt.DEFAULT_INGEST_POOL_TIMEOUT_SECONDS),
    ],
)
def test_pool_timeout_env_parsing(monkeypatch, env_value, expected):
    if env_value is not None:
        monkeypatch.setenv(pt.INGEST_POOL_TIMEOUT_ENV, env_value)

    assert PassthroughProvider._ingest_pool_timeout_seconds() == expected


def test_pool_wait_uses_default_timeout(monkeypatch):
    monkeypatch.setenv("SPARK_DRIVER_CORES", "4")
    provider, _, _ = _make_provider(monkeypatch, num_rows=4535)

    provider.ingest_df(FV, pd.DataFrame())

    (pool,) = FakePool.instances
    assert pool.get_timeouts == [pt.DEFAULT_INGEST_POOL_TIMEOUT_SECONDS]


def test_hung_worker_raises_and_terminates_pool(monkeypatch):
    monkeypatch.setenv("SPARK_DRIVER_CORES", "12")
    monkeypatch.setenv(pt.INGEST_POOL_TIMEOUT_ENV, "0.25")
    FakePool.hang = True
    provider, _, calls = _make_provider(monkeypatch, num_rows=4535)

    with pytest.raises(IngestWorkerHungError) as excinfo:
        provider.ingest_df(FV, pd.DataFrame())

    (pool,) = FakePool.instances
    assert pool.get_timeouts == [0.25]
    # once explicitly on the deadline, once from the context manager exit
    assert pool.terminate_calls == 2
    assert calls == []
    message = str(excinfo.value)
    assert "driver_hourly_stats" in message
    assert "0.25s" in message
    assert "11 worker process" in message
    # raised from None: the bare multiprocessing.TimeoutError is not chained
    assert excinfo.value.__cause__ is None


def test_worker_exception_propagates_unchanged(monkeypatch):
    monkeypatch.setenv("SPARK_DRIVER_CORES", "4")
    FakePool.worker_error = RuntimeError("boom")
    provider, _, _ = _make_provider(monkeypatch, num_rows=4535)

    with pytest.raises(RuntimeError, match="boom"):
        provider.ingest_df(FV, pd.DataFrame())


def test_hung_worker_error_is_not_classified_as_transient():
    pytest.importorskip("pyspark")
    try:
        from feast.infra.contrib.spark_kafka_processor import _is_transient_error
    except ImportError as e:  # e.g. a local PySpark newer than the module supports
        pytest.skip(f"spark_kafka_processor not importable here: {e}")

    error = IngestWorkerHungError("driver_hourly_stats", 11, 600.0)

    assert not _is_transient_error(error)
