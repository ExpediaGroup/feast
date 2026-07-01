"""Write-time materialization-metrics hooks in the local compute-engine nodes."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa

from feast._materialization_metrics import (
    MaterializationMetricsAggregator,
    get_active_aggregator,
)
from feast.infra.compute_engines.dag.context import ColumnInfo, ExecutionContext
from feast.infra.compute_engines.local.arrow_table_value import ArrowTableValue
from feast.infra.compute_engines.local.backends.pandas_backend import PandasBackend
from feast.infra.compute_engines.local.nodes import (
    LocalDedupNode,
    LocalFilterNode,
    LocalOutputNode,
)

backend = PandasBackend()
now = pd.Timestamp.utcnow()

sample_df = pd.DataFrame(
    {
        "entity_id": [1, 1, 2, 2],
        "value": [10, 20, 30, 40],
        "event_timestamp": [
            now,
            now - timedelta(minutes=1),
            now,
            now - timedelta(minutes=5),
        ],
    }
)

entity_df = pd.DataFrame({"entity_id": [1, 2], "event_timestamp": [now, now]})


def _make_aggregator():
    return MaterializationMetricsAggregator(
        project="test_proj",
        feature_view="fv",
        online_store_type="cassandra",
    )


def create_context(node_outputs, metrics_collector=None, online_store=None):
    return ExecutionContext(
        project="test_proj",
        repo_config=MagicMock(),
        offline_store=MagicMock(),
        online_store=online_store or MagicMock(),
        entity_defs=MagicMock(),
        entity_df=entity_df,
        node_outputs=node_outputs,
        metrics_collector=metrics_collector,
    )


def _wire(node, source_name="source"):
    node.add_input(MagicMock())
    node.inputs[0].name = source_name
    return node


class TestFilterDropCount:
    def test_filter_records_dropped_rows(self):
        agg = _make_aggregator()
        context = create_context(
            node_outputs={"source": ArrowTableValue(pa.Table.from_pandas(sample_df))},
            metrics_collector=agg,
        )
        node = _wire(
            LocalFilterNode(
                name="filter",
                backend=backend,
                filter_expr="value > 15",
                column_info=ColumnInfo(
                    join_keys=["entity_id"],
                    feature_cols=["value"],
                    ts_col="event_timestamp",
                    created_ts_col=None,
                ),
            )
        )
        result = node.execute(context)
        assert result.data.num_rows == 3
        assert agg.drop_reasons == {"filter": 1}

    def test_filter_no_collector_is_noop(self):
        context = create_context(
            node_outputs={"source": ArrowTableValue(pa.Table.from_pandas(sample_df))},
            metrics_collector=None,
        )
        node = _wire(
            LocalFilterNode(
                name="filter",
                backend=backend,
                filter_expr="value > 15",
                column_info=ColumnInfo(
                    join_keys=["entity_id"],
                    feature_cols=["value"],
                    ts_col="event_timestamp",
                    created_ts_col=None,
                ),
            )
        )
        # Must not raise when there is no collector.
        assert node.execute(context).data.num_rows == 3


class TestDedupDropCount:
    def test_dedup_records_dropped_rows(self):
        df = pd.DataFrame(
            {
                "entity_id": [1, 1, 2, 2],
                "value": [100, 200, 300, 400],
                "event_timestamp": [
                    now - timedelta(seconds=1),
                    now,
                    now - timedelta(seconds=1),
                    now,
                ],
                "created_ts": [
                    now - timedelta(seconds=1),
                    now,
                    now,
                    now - timedelta(seconds=2),
                ],
            }
        )
        agg = _make_aggregator()
        context = create_context(
            node_outputs={"source": ArrowTableValue(pa.Table.from_pandas(df))},
            metrics_collector=agg,
        )
        node = _wire(
            LocalDedupNode(
                name="dedup",
                backend=backend,
                column_info=ColumnInfo(
                    join_keys=["entity_id"],
                    feature_cols=["value"],
                    ts_col="event_timestamp",
                    created_ts_col="created_ts",
                ),
            )
        )
        result = node.execute(context)
        assert result.data.num_rows == 2
        assert agg.drop_reasons == {"dedup": 2}


def _fake_feature_view(online=False, offline=False):
    return SimpleNamespace(
        name="fv",
        online=online,
        offline=offline,
        features=[SimpleNamespace(name="value")],
        entity_columns=[],
        batch_source=SimpleNamespace(timestamp_field="event_timestamp"),
    )


class TestOutputNodeCapture:
    def test_records_written_fields_nulls_and_freshness(self):
        ts = [
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 5, tzinfo=timezone.utc),
            datetime(2026, 1, 3, tzinfo=timezone.utc),
        ]
        table = pa.table(
            {
                "entity_id": [1, 2, 3],
                "value": [1.0, None, 3.0],
                "event_timestamp": ts,
            }
        )
        agg = _make_aggregator()
        context = create_context(
            node_outputs={"source": ArrowTableValue(table)},
            metrics_collector=agg,
        )
        # online=offline=False: capture happens, no store/offline write is attempted.
        node = _wire(LocalOutputNode("output", _fake_feature_view()))
        node.execute(context)

        assert agg.rows_written_online == 3
        assert agg.fields_written == ["value"]
        assert agg.field_null_counts == {"value": 1}
        assert agg.max_event_timestamp == datetime(2026, 1, 5, tzinfo=timezone.utc)


class TestOutputNodeStoreDropSeam:
    def test_store_can_record_drops_via_contextvar(self):
        """The output node must bind the aggregator as the active collector around
        the online write, so the store can record its own drops (Cassandra TTL)
        without a signature change."""
        table = pa.table(
            {
                "entity_id": [1, 2, 3, 4, 5],
                "value": [1.0, 2.0, 3.0, 4.0, 5.0],
                "event_timestamp": [
                    datetime(2026, 1, 1, tzinfo=timezone.utc) for _ in range(5)
                ],
            }
        )
        agg = _make_aggregator()

        seen = {}

        class FakeOnlineStore:
            def online_write_batch(self, config, table, data, progress):
                # Emulate the store reaching the active aggregator (as Cassandra does).
                active = get_active_aggregator()
                seen["active_is_agg"] = active is agg
                active.record_store_drop("ttl_expired", 2)

        context = create_context(
            node_outputs={"source": ArrowTableValue(table)},
            metrics_collector=agg,
            online_store=FakeOnlineStore(),
        )
        node = _wire(LocalOutputNode("output", _fake_feature_view(online=True)))

        with patch(
            "feast.infra.compute_engines.local.nodes._convert_arrow_to_proto",
            return_value=[],
        ):
            node.execute(context)

        assert seen["active_is_agg"] is True
        assert agg.rows_written_online == 3  # 5 sent - 2 dropped by the store
        assert agg.drop_reasons == {"ttl_expired": 2}
        # collector is unbound again once the write completes
        assert get_active_aggregator() is None
