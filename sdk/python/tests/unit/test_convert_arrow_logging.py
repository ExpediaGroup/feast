from datetime import datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa

from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.types import String, UnixTimestamp
from feast.utils import _convert_arrow_to_proto
from feast.value_type import ValueType


def _make_feature_view_and_table(num_rows: int):
    """Builds a minimal FeatureView + matching pyarrow.Table, mirroring the
    `make_fv` helper pattern used in test_feature_views.py."""
    entity = Entity(
        name="entity_id", join_keys=["entity_id"], value_type=ValueType.STRING
    )
    source = FileSource(path="dummy_path", timestamp_field="event_timestamp")
    feature_view = FeatureView(
        name="test_view",
        source=source,
        entities=[entity],
        schema=[
            Field(name="entity_id", dtype=String),
            Field(name="value", dtype=String),
            Field(name="event_timestamp", dtype=UnixTimestamp),
        ],
        ttl=timedelta(days=1),
    )

    base_time = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    df = pd.DataFrame(
        {
            "entity_id": [f"entity-{i}" for i in range(num_rows)],
            "value": [f"value-{i}" for i in range(num_rows)],
            "event_timestamp": [
                base_time + timedelta(seconds=i) for i in range(num_rows)
            ],
        }
    )
    table = pa.Table.from_pandas(df)
    join_keys = {"entity_id": ValueType.STRING}
    return feature_view, table, join_keys


def test_convert_arrow_to_proto_does_not_log_when_limit_is_none(caplog):
    feature_view, table, join_keys = _make_feature_view_and_table(3)

    with caplog.at_level("INFO"):
        _convert_arrow_to_proto(table, feature_view, join_keys, log_row_limit=None)

    assert "[materialize]" not in caplog.text


def test_convert_arrow_to_proto_logs_all_rows_within_limit(caplog):
    feature_view, table, join_keys = _make_feature_view_and_table(3)

    with caplog.at_level("INFO"):
        _convert_arrow_to_proto(table, feature_view, join_keys, log_row_limit=100)

    lines = [line for line in caplog.text.splitlines() if "[materialize]" in line]
    # 1 summary line + 3 per-row lines
    per_row_lines = [line for line in lines if "entity_keys=" in line]
    assert len(per_row_lines) == 3
    assert any("test_view" in line for line in lines)
    assert any("value-0" in line for line in per_row_lines)


def test_convert_arrow_to_proto_caps_logged_rows_at_limit(caplog):
    feature_view, table, join_keys = _make_feature_view_and_table(3)

    with caplog.at_level("INFO"):
        _convert_arrow_to_proto(table, feature_view, join_keys, log_row_limit=2)

    lines = [line for line in caplog.text.splitlines() if "[materialize]" in line]
    per_row_lines = [line for line in lines if "entity_keys=" in line]
    assert len(per_row_lines) == 2

    summary_lines = [line for line in lines if "entity_keys=" not in line]
    assert any(
        "writing 3 row" in line and "logging first 2" in line for line in summary_lines
    )


def test_convert_arrow_to_proto_returns_same_result_regardless_of_logging(caplog):
    """Logging must be a pure side effect - the returned rows should be
    identical whether or not log_row_limit is set."""
    feature_view, table, join_keys = _make_feature_view_and_table(3)

    result_without_logging = _convert_arrow_to_proto(table, feature_view, join_keys)
    result_with_logging = _convert_arrow_to_proto(
        table, feature_view, join_keys, log_row_limit=100
    )

    assert len(result_without_logging) == len(result_with_logging) == 3
