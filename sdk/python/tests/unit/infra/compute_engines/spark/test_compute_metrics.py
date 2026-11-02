"""Write-time materialization metrics in the Spark engine's offline-store path."""

import pickle
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("pyspark")

from feast._materialization_metrics import (  # noqa: E402
    MaterializationMetricsAggregator,
    drain_run_results,
)
from feast.infra.common.materialization_job import (  # noqa: E402
    MaterializationJobStatus,
)
from feast.infra.compute_engines.spark.compute import SparkComputeEngine  # noqa: E402

START = datetime(2026, 1, 1, tzinfo=timezone.utc)
END = datetime(2026, 1, 2, tzinfo=timezone.utc)


def _materialize(enabled):
    repo_config = SimpleNamespace(
        metrics=SimpleNamespace(materialization=SimpleNamespace(enabled=enabled)),
        online_store=SimpleNamespace(type="cassandra"),
        batch_engine=SimpleNamespace(partitions=0),
    )
    offline_store = MagicMock()
    partition = MaterializationMetricsAggregator("proj", "fv", "cassandra")
    partition.record_read(4)
    partition.record_written(4)
    retrieval_job = offline_store.pull_latest_from_table_or_query.return_value
    spark_df = retrieval_job.to_spark_df.return_value
    spark_df.mapInPandas.return_value.collect.return_value = [
        {"stats": pickle.dumps(partition.to_dict())}
    ]
    engine = SparkComputeEngine(
        repo_config=repo_config,
        offline_store=offline_store,
        online_store=MagicMock(),
    )
    drain_run_results()
    with (
        patch(
            "feast.infra.compute_engines.spark.compute._get_column_names",
            return_value=(["driver_id"], ["conv_rate"], "event_timestamp", None),
        ),
        patch("feast.infra.compute_engines.spark.compute.SerializedArtifacts"),
    ):
        job = engine._materialize_from_offline_store(
            registry=MagicMock(),
            feature_view=SimpleNamespace(name="fv", entities=[], batch_source=None),
            start_date=START,
            end_date=END,
            project="proj",
        )
    assert job.status() == MaterializationJobStatus.SUCCEEDED
    return drain_run_results()


def test_offline_store_path_reports_window():
    [stats] = _materialize(enabled=True)
    assert stats["start_date"] == START
    assert stats["end_date"] == END
    assert stats["rows_written_online"] == 4


def test_offline_store_path_disabled_records_nothing():
    assert _materialize(enabled=False) == []
