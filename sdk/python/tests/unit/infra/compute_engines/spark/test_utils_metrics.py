"""Unit tests for the Spark materialization-metrics capture path (EAPC-22385).

These exercise the executor-side capture in the stats-returning write UDFs
(``map_in_arrow_online_stats`` / ``map_in_pandas_online_stats``) without a live
Spark cluster: the UDFs are plain Python generators, so we feed them batches
directly. ``_convert_arrow_to_proto`` is monkeypatched to a no-op and a fake
online store stands in for the real one. Per-partition payloads are folded with
``merge_stats`` exactly like the driver does. Skipped when pyspark is
unavailable.
"""

import pickle
from datetime import datetime, timezone
from types import SimpleNamespace

import pyarrow as pa
import pytest

pytest.importorskip("pyspark")

from feast._materialization_metrics import (  # noqa: E402
    get_active_aggregator,
    merge_stats,
)
from feast.infra.compute_engines.spark import utils as spark_utils  # noqa: E402
from feast.infra.compute_engines.spark.utils import (  # noqa: E402
    map_in_arrow,
    map_in_arrow_online_stats,
    map_in_pandas_online_stats,
)


def _fake_feature_view():
    return SimpleNamespace(
        name="fv",
        features=[SimpleNamespace(name="conv_rate"), SimpleNamespace(name="acc_rate")],
        entity_columns=[],
        batch_source=SimpleNamespace(
            timestamp_field="event_timestamp", field_mapping=None
        ),
    )


def _fake_artifacts(online_store):
    repo_config = SimpleNamespace(
        project="proj",
        online_store=SimpleNamespace(type="cassandra"),
        # map_in_pandas(_online_stats) probes batch_engine.suppress_warnings.
        batch_engine=SimpleNamespace(),
    )
    fv = _fake_feature_view()
    return SimpleNamespace(unserialize=lambda: (fv, online_store, None, repo_config))


def _arrow_batch():
    ts = [
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 1, 3, tzinfo=timezone.utc),
    ]
    table = pa.table(
        {
            "driver_id": [1, 2],
            "event_timestamp": ts,
            "conv_rate": [0.1, None],
            "acc_rate": [0.5, 0.6],
        }
    )
    return table.to_batches()[0]


def _pandas_batch():
    return pa.Table.from_batches([_arrow_batch()]).to_pandas()


def _stats_from_arrow_output(outputs):
    """Unpickle the single stats record-batch the arrow UDF yields."""
    assert len(outputs) == 1
    payload = outputs[0].column("stats").to_pylist()[0]
    return pickle.loads(payload)


def _stats_from_pandas_output(outputs):
    """Unpickle the single stats DataFrame the pandas UDF yields."""
    assert len(outputs) == 1
    return pickle.loads(outputs[0]["stats"].iloc[0])


class TestMergeStatsDriverFold:
    """The driver folds per-partition payloads with merge_stats."""

    def test_partition_payloads_merge(self):
        a = {"rows_read_offline": 5, "rows_written_online": 5, "drop_reasons": {"x": 1}}
        b = {"rows_read_offline": 3, "rows_written_online": 3, "drop_reasons": {"x": 2}}
        merged = merge_stats(merge_stats({}, a), b)
        assert merged["rows_read_offline"] == 8
        assert merged["rows_written_online"] == 8
        assert merged["drop_reasons"] == {"x": 3}


class TestMapInArrowOnlineStats:
    def test_records_rows_and_freshness(self, monkeypatch):
        monkeypatch.setattr(spark_utils, "_convert_arrow_to_proto", lambda *a, **k: [])
        writes = []
        online_store = SimpleNamespace(
            online_write_batch=lambda **kw: writes.append(len(kw.get("data", [])))
        )

        outputs = list(
            map_in_arrow_online_stats(
                iter([_arrow_batch(), _arrow_batch()]),
                _fake_artifacts(online_store),
            )
        )
        stats = _stats_from_arrow_output(outputs)

        assert len(writes) == 2  # one write per batch
        assert stats["rows_read_offline"] == 4
        assert stats["rows_written_online"] == 4
        assert set(stats["fields_written"]) == {"conv_rate", "acc_rate"}
        assert stats["field_null_counts"]["conv_rate"] == 2  # one null per batch
        assert stats["max_event_timestamp"] == datetime(2026, 1, 3, tzinfo=timezone.utc)

    def test_store_drops_flow_through_contextvar(self, monkeypatch):
        monkeypatch.setattr(spark_utils, "_convert_arrow_to_proto", lambda *a, **k: [])

        # Simulate a store (like Cassandra TTL) recording a drop via the active
        # aggregator bound by collecting() on the executor.
        def _writer(**kw):
            agg = get_active_aggregator()
            assert agg is not None, "the aggregator must be bound during the write"
            agg.record_store_drop("ttl_expired", 1)

        online_store = SimpleNamespace(online_write_batch=_writer)

        outputs = list(
            map_in_arrow_online_stats(
                iter([_arrow_batch()]), _fake_artifacts(online_store)
            )
        )
        stats = _stats_from_arrow_output(outputs)

        assert stats["rows_read_offline"] == 2
        assert stats["rows_written_online"] == 1  # 2 written, 1 TTL-dropped
        assert stats["drop_reasons"] == {"ttl_expired": 1}
        assert (
            stats["rows_read_offline"] - stats["rows_written_online"]
            == stats["rows_dropped"]
        )
        # aggregator unbound again after the write
        assert get_active_aggregator() is None

    def test_plain_map_in_arrow_is_metrics_free(self, monkeypatch):
        monkeypatch.setattr(spark_utils, "_convert_arrow_to_proto", lambda *a, **k: [])
        calls = []
        online_store = SimpleNamespace(online_write_batch=lambda **kw: calls.append(1))
        # The non-metrics UDF: write happens, batches pass through, no stats.
        outputs = list(
            map_in_arrow(
                iter([_arrow_batch()]), _fake_artifacts(online_store), mode="online"
            )
        )
        assert calls == [1]
        assert len(outputs) == 1  # the data batch itself, passed through
        assert outputs[0].num_rows == 2


class TestMapInPandasOnlineStats:
    def test_records_rows_and_yields_stats(self, monkeypatch):
        monkeypatch.setattr(spark_utils, "_convert_arrow_to_proto", lambda *a, **k: [])
        writes = []

        def _writer(config, table, data, progress):  # positional, like the real call
            writes.append(1)

        online_store = SimpleNamespace(online_write_batch=_writer)

        outputs = list(
            map_in_pandas_online_stats(
                iter([_pandas_batch(), _pandas_batch()]),
                _fake_artifacts(online_store),
            )
        )
        stats = _stats_from_pandas_output(outputs)

        assert len(writes) == 2
        assert stats["rows_read_offline"] == 4
        assert stats["rows_written_online"] == 4
        assert set(stats["fields_written"]) == {"conv_rate", "acc_rate"}
        assert stats["max_event_timestamp"] == datetime(2026, 1, 3, tzinfo=timezone.utc)

    def test_empty_batch_ends_partition_like_map_in_pandas(self, monkeypatch):
        # Behavior parity: map_in_pandas returns on an empty batch (ending the
        # partition); the stats variant must do the same -- and still emit its
        # stats row so already-written batches are counted.
        monkeypatch.setattr(spark_utils, "_convert_arrow_to_proto", lambda *a, **k: [])
        writes = []

        def _writer(config, table, data, progress):
            writes.append(1)

        online_store = SimpleNamespace(online_write_batch=_writer)
        empty = _pandas_batch().iloc[0:0]

        outputs = list(
            map_in_pandas_online_stats(
                iter([_pandas_batch(), empty, _pandas_batch()]),
                _fake_artifacts(online_store),
            )
        )
        stats = _stats_from_pandas_output(outputs)

        # First batch written; empty batch ends the partition; third never seen.
        assert len(writes) == 1
        assert stats["rows_read_offline"] == 2
        assert stats["rows_written_online"] == 2

    def test_store_drops_flow_through_contextvar(self, monkeypatch):
        monkeypatch.setattr(spark_utils, "_convert_arrow_to_proto", lambda *a, **k: [])

        def _writer(config, table, data, progress):
            agg = get_active_aggregator()
            assert agg is not None
            agg.record_store_drop("ttl_expired", 2)

        online_store = SimpleNamespace(online_write_batch=_writer)

        outputs = list(
            map_in_pandas_online_stats(
                iter([_pandas_batch()]), _fake_artifacts(online_store)
            )
        )
        stats = _stats_from_pandas_output(outputs)
        assert stats["rows_written_online"] == 0  # 2 written, 2 dropped
        assert stats["drop_reasons"] == {"ttl_expired": 2}
        assert get_active_aggregator() is None
