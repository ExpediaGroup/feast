from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest

from feast._materialization_metrics import (
    MaterializationMetricsAggregator,
    collecting,
    drain_run_results,
    get_active_aggregator,
    is_materialization_metrics_enabled,
    merge_stats,
    record_run_result,
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


class TestVolume:
    def test_bytes_from_batch(self):
        table = pa.table({"driver_id": [1, 1, 2, 3], "conv_rate": [0.1, 0.2, 0.3, 0.4]})
        agg = _aggregator()
        agg.observe_written_batch(table, feature_fields=["conv_rate"])
        assert agg.bytes_written == table.nbytes
        d = agg.to_dict()
        assert d["bytes_written"] == table.nbytes


class TestMergeStats:
    """merge_stats backs the Spark AccumulatorParam: {} is identity, and merging
    per-partition to_dict() outputs must sum counts, add Counters, union fields,
    and take the later timestamp -- preserving the reconciliation invariant."""

    def test_empty_is_identity(self):
        agg = _aggregator()
        agg.record_read(10)
        agg.record_written(10)
        d = agg.to_dict()
        assert merge_stats({}, d) == d
        assert merge_stats(d, {}) == d

    def test_merge_two_partitions(self):
        p1 = MaterializationMetricsAggregator("proj", "fv", "cassandra")
        p1.record_read(60)
        p1.record_written(60)
        p1.record_store_drop("ttl_expired", 4)  # written -> 56
        p1.observe_event_timestamp(datetime(2026, 5, 1, tzinfo=timezone.utc))
        p1.field_null_counts.update({"conv_rate": 3})
        p1.fields_written.append("conv_rate")

        p2 = MaterializationMetricsAggregator("proj", "fv", "cassandra")
        p2.record_read(40)
        p2.record_written(40)
        p2.record_store_drop("ttl_expired", 1)  # written -> 39
        p2.observe_event_timestamp(datetime(2026, 6, 1, tzinfo=timezone.utc))
        p2.field_null_counts.update({"conv_rate": 2, "acc_rate": 5})
        p2.fields_written.extend(["conv_rate", "acc_rate"])

        merged = merge_stats(p1.to_dict(), p2.to_dict())

        assert merged["rows_read_offline"] == 100
        assert merged["rows_written_online"] == 95  # 56 + 39
        assert merged["drop_reasons"] == {"ttl_expired": 5}
        assert merged["rows_dropped"] == 5
        assert merged["fields_written"] == [
            "conv_rate",
            "acc_rate",
        ]  # order-stable union
        assert merged["field_null_counts"] == {"conv_rate": 5, "acc_rate": 5}
        assert merged["max_event_timestamp"] == datetime(
            2026, 6, 1, tzinfo=timezone.utc
        )
        # reconciliation invariant survives the merge
        assert (
            merged["rows_read_offline"] - merged["rows_written_online"]
            == merged["rows_dropped"]
        )

    def test_commutative(self):
        a = {"rows_read_offline": 5, "rows_written_online": 5, "drop_reasons": {"x": 1}}
        b = {"rows_read_offline": 3, "rows_written_online": 2, "drop_reasons": {"y": 2}}
        assert merge_stats(a, b) == merge_stats(b, a)

    def test_bytes_sum(self):
        # bytes accumulate across partitions.
        a = {"bytes_written": 100}
        b = {"bytes_written": 250}
        merged = merge_stats(a, b)
        assert merged["bytes_written"] == 350


class TestBridge:
    def test_record_and_drain(self):
        drain_run_results()  # clear any leftover state
        record_run_result({"feature_view": "fv1", "rows_written_online": 5})
        record_run_result({"feature_view": "fv2", "rows_written_online": 7})
        record_run_result(None)  # ignored
        record_run_result({})  # ignored
        results = drain_run_results()
        assert [r["feature_view"] for r in results] == ["fv1", "fv2"]
        # draining consumes: a second drain is empty
        assert drain_run_results() == []


class TestMergeFromDict:
    def test_fold_accumulator_value_into_driver_collector(self):
        # Driver-side collector starts empty; folds the merged executor stats in.
        driver = _aggregator()
        merged = {
            "rows_read_offline": 100,
            "rows_written_online": 95,
            "drop_reasons": {"ttl_expired": 5},
            "fields_written": ["conv_rate", "acc_rate"],
            "field_null_counts": {"conv_rate": 5},
            "max_event_timestamp": datetime(2026, 6, 1, tzinfo=timezone.utc),
        }
        driver.merge_from_dict(merged)

        assert driver.rows_read_offline == 100
        assert driver.rows_written_online == 95
        assert driver.rows_dropped == 5
        assert driver.drop_reasons == {"ttl_expired": 5}
        assert driver.fields_written == ["conv_rate", "acc_rate"]
        assert driver.field_null_counts == {"conv_rate": 5}
        assert driver.max_event_timestamp == datetime(2026, 6, 1, tzinfo=timezone.utc)
        # does NOT re-apply store-drop decrement (value is already net)
        assert (
            driver.rows_read_offline - driver.rows_written_online == driver.rows_dropped
        )

    def test_merge_from_empty_is_noop(self):
        driver = _aggregator()
        driver.record_read(7)
        driver.record_written(7)
        driver.merge_from_dict(None)
        driver.merge_from_dict({})
        assert driver.rows_read_offline == 7
        assert driver.rows_written_online == 7


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
