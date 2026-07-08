"""Unit tests for the Spark materialization-metrics accumulator path (EAPC-22385).

These exercise the executor-side capture in ``map_in_arrow`` and the
``MaterializationStatsAccumulatorParam`` merge without a live Spark cluster:
``_convert_arrow_to_proto`` is monkeypatched to a no-op and a fake accumulator /
online store stand in for the real ones. Skipped when pyspark is unavailable.
"""

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
    MaterializationStatsAccumulatorParam,
    map_in_arrow,
)


class _FakeAccumulator:
    """Stands in for a Spark accumulator: merges added dicts via merge_stats."""

    def __init__(self):
        self.value = {}

    def add(self, term):
        self.value = merge_stats(self.value, term)


def _fake_feature_view():
    return SimpleNamespace(
        name="fv",
        features=[SimpleNamespace(name="conv_rate"), SimpleNamespace(name="acc_rate")],
        entity_columns=[],
        batch_source=SimpleNamespace(timestamp_field="event_timestamp"),
    )


def _fake_artifacts(online_store):
    repo_config = SimpleNamespace(
        project="proj", online_store=SimpleNamespace(type="cassandra")
    )
    fv = _fake_feature_view()
    return SimpleNamespace(
        unserialize=lambda: (fv, online_store, None, repo_config)
    )


def _batch():
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


class TestAccumulatorParam:
    def test_zero_is_empty_dict(self):
        param = MaterializationStatsAccumulatorParam()
        assert param.zero({}) == {}
        assert param.zero({"rows_read_offline": 5}) == {"rows_read_offline": 5}

    def test_add_in_place_merges_via_merge_stats(self):
        param = MaterializationStatsAccumulatorParam()
        a = {"rows_read_offline": 5, "rows_written_online": 5, "drop_reasons": {"x": 1}}
        b = {"rows_read_offline": 3, "rows_written_online": 3, "drop_reasons": {"x": 2}}
        merged = param.addInPlace(a, b)
        assert merged["rows_read_offline"] == 8
        assert merged["rows_written_online"] == 8
        assert merged["drop_reasons"] == {"x": 3}


class TestMapInArrowMetrics:
    def test_records_rows_and_freshness_into_accumulator(self, monkeypatch):
        monkeypatch.setattr(spark_utils, "_convert_arrow_to_proto", lambda *a, **k: [])
        writes = []
        online_store = SimpleNamespace(
            online_write_batch=lambda **kw: writes.append(len(kw.get("data", [])))
        )
        acc = _FakeAccumulator()

        # two batches of 2 rows each
        list(
            map_in_arrow(
                iter([_batch(), _batch()]),
                _fake_artifacts(online_store),
                mode="online",
                stats_accumulator=acc,
            )
        )

        assert acc.value["rows_read_offline"] == 4
        assert acc.value["rows_written_online"] == 4
        assert set(acc.value["fields_written"]) == {"conv_rate", "acc_rate"}
        assert acc.value["field_null_counts"]["conv_rate"] == 2  # one null per batch
        assert acc.value["max_event_timestamp"] == datetime(
            2026, 1, 3, tzinfo=timezone.utc
        )

    def test_store_drops_flow_through_contextvar(self, monkeypatch):
        monkeypatch.setattr(spark_utils, "_convert_arrow_to_proto", lambda *a, **k: [])

        # Simulate a store (like Cassandra TTL) recording a drop via the active
        # aggregator bound by map_in_arrow's collecting() on the executor.
        def _writer(**kw):
            agg = get_active_aggregator()
            assert agg is not None, "map_in_arrow must bind the aggregator during write"
            agg.record_store_drop("ttl_expired", 1)

        online_store = SimpleNamespace(online_write_batch=_writer)
        acc = _FakeAccumulator()

        list(
            map_in_arrow(
                iter([_batch()]),
                _fake_artifacts(online_store),
                mode="online",
                stats_accumulator=acc,
            )
        )

        assert acc.value["rows_read_offline"] == 2
        assert acc.value["rows_written_online"] == 1  # 2 written, 1 TTL-dropped
        assert acc.value["drop_reasons"] == {"ttl_expired": 1}
        assert (
            acc.value["rows_read_offline"] - acc.value["rows_written_online"]
            == acc.value["rows_dropped"]
        )
        # aggregator unbound again after the write
        assert get_active_aggregator() is None

    def test_no_accumulator_is_plain_write(self, monkeypatch):
        monkeypatch.setattr(spark_utils, "_convert_arrow_to_proto", lambda *a, **k: [])
        calls = []
        online_store = SimpleNamespace(
            online_write_batch=lambda **kw: calls.append(1)
        )
        # No stats_accumulator -> no metrics, write still happens, no crash.
        list(
            map_in_arrow(
                iter([_batch()]), _fake_artifacts(online_store), mode="online"
            )
        )
        assert calls == [1]
