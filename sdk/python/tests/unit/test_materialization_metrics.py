from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest

from feast._materialization_metrics import (
    MaterializationMetricsAggregator,
    collecting,
    get_active_aggregator,
    is_materialization_metrics_enabled,
)


def _aggregator():
    return MaterializationMetricsAggregator(
        project="proj",
        feature_view="fv",
        online_store_type="cassandra",
    )


class TestReconciliation:
    def test_clean_run_no_drops(self):
        agg = _aggregator()
        agg.record_read(100)
        agg.record_written(100)

        assert agg.rows_read_offline == 100
        assert agg.rows_written_online == 100
        assert agg.rows_dropped == 0
        assert agg.drop_reasons == {}

    def test_upstream_drops_reduce_written_via_output_node(self):
        # Upstream (filter/dedup) drops happen before the output node, so the
        # output node only ever sees the survivors. rows_written == rows reaching
        # the output node; the invariant still holds against rows_read.
        agg = _aggregator()
        agg.record_read(100)
        agg.record_upstream_drop("filter", 30)
        agg.record_upstream_drop("dedup", 10)
        agg.record_written(60)  # 100 - 30 - 10 survivors reached the output node

        assert agg.rows_written_online == 60
        assert agg.rows_dropped == 40
        assert agg.drop_reasons == {"filter": 30, "dedup": 10}
        # invariant
        assert agg.rows_read_offline - agg.rows_written_online == agg.rows_dropped

    def test_store_drops_decrement_written(self):
        # Store-level (Cassandra TTL) drops happen after the output node counted
        # the rows as "sent", so they must decrement rows_written.
        agg = _aggregator()
        agg.record_read(50)
        agg.record_written(50)  # all 50 sent to the store
        agg.record_store_drop("ttl_expired", 5)
        agg.record_store_drop("ttl_exceeds_max", 2)

        assert agg.rows_written_online == 43
        assert agg.rows_dropped == 7
        assert agg.drop_reasons == {"ttl_expired": 5, "ttl_exceeds_max": 2}
        assert agg.rows_read_offline - agg.rows_written_online == agg.rows_dropped

    def test_combined_upstream_and_store_drops(self):
        agg = _aggregator()
        agg.record_read(100)
        agg.record_upstream_drop("dedup", 20)
        agg.record_written(80)
        agg.record_store_drop("ttl_expired", 8)

        assert agg.rows_written_online == 72
        assert agg.rows_dropped == 28
        assert agg.rows_read_offline - agg.rows_written_online == agg.rows_dropped


class TestFreshness:
    def test_max_event_timestamp_and_lag(self):
        agg = _aggregator()
        newest = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        older = datetime(2026, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
        agg.observe_event_timestamp(older)
        agg.observe_event_timestamp(newest)
        agg.observe_event_timestamp(older)

        assert agg.max_event_timestamp == newest

        now = newest + timedelta(seconds=90)
        assert agg.lag_seconds(now=now) == pytest.approx(90.0)

    def test_lag_none_when_no_timestamp(self):
        agg = _aggregator()
        assert agg.max_event_timestamp is None
        assert agg.lag_seconds(now=datetime.now(timezone.utc)) is None


class TestArrowCapture:
    def test_observe_written_batch_records_fields_and_nulls_and_ts(self):
        ts = [
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 3, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        ]
        table = pa.table(
            {
                "driver_id": [1, 2, 3],
                "event_timestamp": ts,
                "conv_rate": [0.1, None, 0.3],
                "acc_rate": [None, None, 0.9],
            }
        )
        agg = _aggregator()
        agg.observe_written_batch(
            table,
            feature_fields=["conv_rate", "acc_rate"],
            timestamp_column="event_timestamp",
        )

        assert set(agg.fields_written) == {"conv_rate", "acc_rate"}
        assert agg.field_null_counts == {"conv_rate": 1, "acc_rate": 2}
        assert agg.max_event_timestamp == datetime(2026, 1, 3, tzinfo=timezone.utc)

    def test_observe_written_batch_ignores_absent_feature_field(self):
        table = pa.table({"driver_id": [1, 2], "conv_rate": [0.1, 0.2]})
        agg = _aggregator()
        agg.observe_written_batch(
            table,
            feature_fields=["conv_rate", "missing_feature"],
            timestamp_column="event_timestamp",  # column not present
        )
        # only present fields are reported; no crash on missing ts column
        assert agg.fields_written == ["conv_rate"]
        assert agg.field_null_counts == {"conv_rate": 0}
        assert agg.max_event_timestamp is None


class TestToDict:
    def test_to_dict_shape(self):
        agg = _aggregator()
        agg.record_read(10)
        agg.record_upstream_drop("filter", 1)
        agg.record_written(9)  # 1 dropped upstream, 9 reached the output node
        agg.record_store_drop("ttl_expired", 1)
        d = agg.to_dict()
        assert d["project"] == "proj"
        assert d["feature_view"] == "fv"
        assert d["online_store_type"] == "cassandra"
        assert d["rows_read_offline"] == 10
        assert d["rows_written_online"] == 8
        assert d["rows_dropped"] == 2
        assert d["drop_reasons"] == {"filter": 1, "ttl_expired": 1}
        # reconciliation invariant
        assert d["rows_read_offline"] - d["rows_written_online"] == d["rows_dropped"]


class TestEnvGate:
    def test_disabled_by_default(self, monkeypatch):
        monkeypatch.delenv("ENABLE_MATERIALIZATION_METRICS", raising=False)
        assert is_materialization_metrics_enabled() is False

    @pytest.mark.parametrize("val", ["1", "true", "True", "TRUE", "yes"])
    def test_enabled_truthy(self, monkeypatch, val):
        monkeypatch.setenv("ENABLE_MATERIALIZATION_METRICS", val)
        assert is_materialization_metrics_enabled() is True

    @pytest.mark.parametrize("val", ["0", "false", "no", "", "off"])
    def test_disabled_falsy(self, monkeypatch, val):
        monkeypatch.setenv("ENABLE_MATERIALIZATION_METRICS", val)
        assert is_materialization_metrics_enabled() is False


class TestContextVar:
    def test_collecting_sets_and_restores(self):
        assert get_active_aggregator() is None
        agg = _aggregator()
        with collecting(agg):
            assert get_active_aggregator() is agg
        assert get_active_aggregator() is None

    def test_collecting_none_is_noop(self):
        with collecting(None):
            assert get_active_aggregator() is None

    def test_nested_collecting_restores_outer(self):
        outer = _aggregator()
        inner = _aggregator()
        with collecting(outer):
            with collecting(inner):
                assert get_active_aggregator() is inner
            assert get_active_aggregator() is outer
        assert get_active_aggregator() is None
