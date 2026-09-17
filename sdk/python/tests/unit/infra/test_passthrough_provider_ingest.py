"""Dispatch/configuration tests; test_ingest_worker exercises real processes."""

import os
import pickle
import threading
from types import SimpleNamespace

import pandas as pd
import pytest

import feast.infra.passthrough_provider as pt
from feast.errors import CassandraWriteTimeoutError, IngestWorkerHungError
from feast.infra.passthrough_provider import PassthroughProvider
from feast.repo_config import RepoConfig


class FakeTable:
    def __init__(self, num_rows):
        self.num_rows = num_rows

    def slice(self, offset, length):
        return (offset, length)


@pytest.fixture(autouse=True)
def isolated_env(monkeypatch):
    for key in (
        "SPARK_DRIVER_CORES",
        "NUM_PROCESSES",
        pt.INGEST_WORKERS_ENV,
        pt.INGEST_POOL_TIMEOUT_ENV,
    ):
        monkeypatch.delenv(key, raising=False)
    yield
    os.environ.pop("NUM_PROCESSES", None)


@pytest.fixture
def provider(monkeypatch):
    instance = PassthroughProvider(RepoConfig(project="test", registry="registry.db"))
    monkeypatch.setattr(
        instance,
        "_prep_table_and_join_keys_for_ingestion",
        lambda **kwargs: (FakeTable(len(kwargs["df"])), {"entity": 4}),
    )
    return instance


FV = SimpleNamespace(name="ingest_test")


def ingest(provider, count):
    provider.ingest_df(FV, pd.DataFrame({"entity": range(count)}))


@pytest.mark.parametrize("cores", [None, "2", "12", "invalid"])
def test_default_is_in_process_independent_of_driver_cores(
    provider, monkeypatch, cores
):
    if cores is not None:
        monkeypatch.setenv("SPARK_DRIVER_CORES", cores)
    calls = []
    monkeypatch.setattr(provider, "process", lambda *args: calls.append(args))
    ingest(provider, 10)
    assert len(calls) == 1
    assert calls[0][0].num_rows == 10
    assert os.environ["NUM_PROCESSES"] == "1"


@pytest.mark.parametrize("setting", ["0", "1", "", "  "])
def test_explicit_serial_mode(provider, monkeypatch, setting):
    monkeypatch.setenv(pt.INGEST_WORKERS_ENV, setting)
    calls = []
    monkeypatch.setattr(provider, "process", lambda *args: calls.append(args))
    ingest(provider, 10)
    assert len(calls) == 1


@pytest.mark.parametrize("setting", ["-1", "lots", "1.5"])
def test_invalid_workers_fail_before_writing(provider, monkeypatch, setting):
    monkeypatch.setenv(pt.INGEST_WORKERS_ENV, setting)
    with pytest.raises(ValueError, match="FEAST_INGEST_WORKERS"):
        ingest(provider, 10)


@pytest.mark.parametrize("rows,expected_workers", [(1, 1), (3, 3), (17, 4)])
def test_explicit_parallel_mode_preserves_isolation_for_small_batches(
    provider, monkeypatch, rows, expected_workers
):
    monkeypatch.setenv(pt.INGEST_WORKERS_ENV, "4")
    calls = []
    monkeypatch.setattr(pt, "run_ingest_workers", lambda *args: calls.append(args))
    # Cached locks and sessions must never be part of the worker payload.
    provider._online_store = SimpleNamespace(lock=threading.Lock())
    ingest(provider, rows)
    config, feature_view, chunks, workers, timeout = calls[0]
    assert config is provider.repo_config
    assert feature_view is FV
    assert workers == expected_workers
    assert len(chunks) == expected_workers
    assert sum(chunk[0][1] for chunk in chunks) == rows
    assert timeout == 600.0
    assert os.environ["NUM_PROCESSES"] == str(expected_workers)
    # This would fail if the bound provider, with its lock, leaked into args.
    pickle.dumps(calls[0])


def test_singleton_then_larger_batch_remain_in_parallel_mode(provider, monkeypatch):
    monkeypatch.setenv(pt.INGEST_WORKERS_ENV, "4")
    calls = []
    monkeypatch.setattr(pt, "run_ingest_workers", lambda *args: calls.append(args))

    def unexpected_parent_write(*args):
        raise AssertionError("parallel writes must not initialize parent clients")

    monkeypatch.setattr(provider, "process", unexpected_parent_write)
    ingest(provider, 1)
    ingest(provider, 3)
    assert [args[3] for args in calls] == [1, 3]
    assert provider._online_store is None
    assert provider._write_token_limiters == {}


def test_empty_batch_does_not_write_or_start_workers(provider, monkeypatch):
    monkeypatch.setenv(pt.INGEST_WORKERS_ENV, "4")

    def unexpected(*args):
        raise AssertionError("empty batch should be a no-op")

    monkeypatch.setattr(provider, "process", unexpected)
    monkeypatch.setattr(pt, "run_ingest_workers", unexpected)
    ingest(provider, 0)


@pytest.mark.parametrize(
    "setting,expected",
    [(None, 600.0), ("", 600.0), (" ", 600.0), ("0.25", 0.25), ("1200", 1200.0)],
)
def test_timeout_parsing(monkeypatch, setting, expected):
    if setting is not None:
        monkeypatch.setenv(pt.INGEST_POOL_TIMEOUT_ENV, setting)
    assert PassthroughProvider._ingest_pool_timeout_seconds() == expected


@pytest.mark.parametrize("setting", ["0", "-1", "nan", "inf", "-inf", "soon"])
def test_invalid_timeout_cannot_silently_disable_deadline(monkeypatch, setting):
    monkeypatch.setenv(pt.INGEST_POOL_TIMEOUT_ENV, setting)
    with pytest.raises(ValueError, match="FEAST_INGEST_POOL_TIMEOUT_SECONDS"):
        PassthroughProvider._ingest_pool_timeout_seconds()


def test_rate_limiter_uses_actual_concurrency(provider, monkeypatch):
    monkeypatch.setenv("SPARK_DRIVER_CORES", "12")
    monkeypatch.setenv("NUM_PROCESSES", "1")
    monkeypatch.setattr(provider, "_resolve_write_rate_limit", lambda *args: 100)
    provider.online_write_batch(provider.repo_config, FV, [], None)
    limiter = provider._write_token_limiters["test:ingest_test"]
    assert limiter.percent_usage == 0.9


@pytest.mark.parametrize(
    "error",
    [
        IngestWorkerHungError("overloaded_features", 4, 600),
        CassandraWriteTimeoutError("write deadline exceeded"),
    ],
)
def test_deadline_errors_survive_pickle_and_are_not_transient(error):
    from feast.infra.contrib.spark_kafka_processor import _is_transient_error

    restored = pickle.loads(pickle.dumps(error))
    assert type(restored) is type(error)
    assert str(restored) == str(error)
    assert not _is_transient_error(restored)


@pytest.mark.timeout(30)
def test_real_writes_survive_singleton_then_spawn_with_cached_parent_state(
    tmp_path, monkeypatch
):
    """Exercise actual Arrow/protobuf conversion, rate limiter and SQLite reads."""
    from datetime import datetime, timezone

    from feast import Entity, FeatureView, Field, FileSource
    from feast.protos.feast.types.EntityKey_pb2 import EntityKey
    from feast.protos.feast.types.Value_pb2 import Value
    from feast.types import Int64

    config = RepoConfig(
        project="ingest_regression",
        registry=str(tmp_path / "registry.db"),
        provider="local",
        online_store={"type": "sqlite", "path": str(tmp_path / "online.db")},
    )
    instance = PassthroughProvider(config)
    entity = Entity(name="driver", join_keys=["driver_id"])
    view = FeatureView(
        name="driver_values",
        entities=[entity],
        schema=[Field(name="value", dtype=Int64)],
        source=FileSource(path="unused.parquet", timestamp_field="event_timestamp"),
        tags={"write_rate_limit": "100"},
    )
    view.entity_columns = [Field(name="driver_id", dtype=Int64)]
    store = instance.online_store
    store.update(config, [], [view], [], [entity], False)
    rows = pd.DataFrame(
        {
            "driver_id": [1, 2, 3],
            "value": [10, 20, 30],
            "event_timestamp": [datetime.now(timezone.utc)] * 3,
        }
    )
    try:
        monkeypatch.setenv("SPARK_DRIVER_CORES", "12")
        instance.ingest_df(view, rows.iloc[:1])
        # The old bound-method dispatch cannot pickle this connection/limiter.
        assert instance._write_token_limiters
        assert store._conn is not None

        monkeypatch.setenv(pt.INGEST_WORKERS_ENV, "2")
        instance.ingest_df(view, rows.iloc[:1])
        instance.ingest_df(view, rows)

        keys = [
            EntityKey(join_keys=["driver_id"], entity_values=[Value(int64_val=i)])
            for i in [1, 2, 3]
        ]
        result = store.online_read(config, view, keys)
        assert [features["value"].int64_val for _, features in result] == [10, 20, 30]
    finally:
        store._conn.close()
