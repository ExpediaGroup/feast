"""The TTL is a bound INSERT parameter, so each table has one prepared statement.

A TTL formatted into the CQL text makes every distinct TTL a new statement to
prepare. Sorted feature views compute the TTL per row from the event
timestamp, so that meant a new prepared statement almost every second.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest

from feast import FeatureView
from feast.entity import Entity
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.online_stores.cassandra_online_store import (
    cassandra_online_store as cassandra_module,
)
from feast.infra.online_stores.cassandra_online_store.cassandra_online_store import (
    CassandraOnlineStore,
    CassandraOnlineStoreConfig,
)
from feast.protos.feast.core.SortedFeatureView_pb2 import SortOrder
from feast.protos.feast.types.EntityKey_pb2 import EntityKey
from feast.protos.feast.types.Value_pb2 import Value
from feast.repo_config import RepoConfig
from feast.sorted_feature_view import SortedFeatureView, SortKey
from feast.types import Int64
from feast.value_type import ValueType

NOW = datetime(2026, 9, 28, 12, 0, 0, tzinfo=timezone.utc)
NINETY_DAYS = int(timedelta(days=90).total_seconds())


class _InlineFuture:
    def add_callbacks(self, success, _failure):
        success(None)


class _RecordingBatch:
    """Stands in for BatchStatement and records what each row binds."""

    def __init__(self, added, **_kwargs):
        self._added = added

    def add(self, statement, parameters=None):
        self._added.append((statement, parameters))


def _repo_config(**online_options):
    return RepoConfig(
        registry="registry.db",
        project="ttl_test",
        provider="local",
        online_store=CassandraOnlineStoreConfig(
            hosts=["localhost"], keyspace="test_keyspace", **online_options
        ),
    )


@pytest.fixture
def store_and_session(monkeypatch):
    store = CassandraOnlineStore()
    session = Mock()
    session.prepare.side_effect = lambda query: ("prepared", query)
    session.execute_async.side_effect = lambda _batch: _InlineFuture()
    monkeypatch.setattr(store, "_get_session", lambda _config: session)
    monkeypatch.setattr(cassandra_module.utils, "_utc_now", lambda: NOW)
    added = []
    monkeypatch.setattr(
        cassandra_module,
        "BatchStatement",
        lambda **kwargs: _RecordingBatch(added, **kwargs),
    )
    return store, session, added


def _row(entity_id, event_ts, sort_key=1):
    return (
        EntityKey(join_keys=["id"], entity_values=[Value(int64_val=entity_id)]),
        {"feature1": Value(int64_val=42), "sort_key": Value(int64_val=sort_key)},
        event_ts,
        None,
    )


def _prepared_queries(session):
    return [call.args[0] for call in session.prepare.call_args_list]


def _assert_every_marker_bound(query, added):
    # The driver pads short value tuples with UNSET on protocol v4+, so a
    # missing TTL would not fail at bind time.
    assert all(len(params) == query.count("?") for _statement, params in added)


def test_sorted_feature_view_prepares_once_and_binds_ttl_per_row(
    store_and_session,
):
    store, session, added = store_and_session
    table = SortedFeatureView(
        name="test_sfv",
        source=FileSource(path="unused.parquet", timestamp_field="event_timestamp"),
        schema=[
            Field(name="feature1", dtype=Int64),
            Field(name="sort_key", dtype=Int64),
        ],
        entities=[Entity(name="id", join_keys=["id"], value_type=ValueType.INT64)],
        ttl=timedelta(days=90),
        sort_keys=[
            SortKey(
                name="sort_key",
                value_type=ValueType.INT64,
                default_sort_order=SortOrder.Enum.ASC,
            )
        ],
    )
    event_times = [
        NOW - timedelta(seconds=9),
        NOW - timedelta(seconds=10),
        NOW - timedelta(hours=1),
    ]
    data = [
        _row(1, event_times[0], sort_key=1),
        _row(1, event_times[1], sort_key=2),
        _row(2, event_times[2], sort_key=3),
        # Past the feature view TTL: skipped, never bound.
        _row(3, NOW - timedelta(days=91), sort_key=4),
    ]

    store.online_write_batch(_repo_config(), table, data, None)

    queries = _prepared_queries(session)
    assert len(queries) == 1
    assert queries[0].endswith("USING TTL ?;")
    assert len(store._prepared_statements) == 1

    # The TTL is the last bound value, after entity_key and event_ts.
    bound = sorted((params[-2], params[-1]) for _statement, params in added)
    assert bound == sorted(
        (event_ts, NINETY_DAYS - int((NOW - event_ts).total_seconds()))
        for event_ts in event_times
    )
    assert {statement for statement, _params in added} == {("prepared", queries[0])}
    _assert_every_marker_bound(queries[0], added)


@pytest.mark.parametrize(
    "key_ttl_seconds, expected_ttl",
    [(1_209_600, 1_209_600), (None, 0)],
    ids=["key_ttl_seconds", "no_ttl"],
)
def test_feature_view_binds_key_ttl_seconds(
    store_and_session, key_ttl_seconds, expected_ttl
):
    store, session, added = store_and_session
    table = FeatureView(
        name="test_fv",
        source=FileSource(path="unused.parquet", timestamp_field="event_timestamp"),
        schema=[
            Field(name="feature1", dtype=Int64),
            Field(name="sort_key", dtype=Int64),
        ],
    )
    data = [_row(1, NOW - timedelta(seconds=5)), _row(2, NOW - timedelta(hours=2))]

    store.online_write_batch(
        _repo_config(key_ttl_seconds=key_ttl_seconds), table, data, None
    )

    queries = _prepared_queries(session)
    assert len(queries) == 1
    assert queries[0].endswith("USING TTL ?;")
    # Two rows with two features each: four bound inserts, all with the same TTL.
    assert len(added) == 4
    assert {params[-1] for _statement, params in added} == {expected_ttl}
    assert {params[-2] for _statement, params in added} == {row[2] for row in data}
    _assert_every_marker_bound(queries[0], added)
