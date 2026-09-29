"""Cassandra write deadlines and real callback/backpressure behavior, without a DB."""

import os
import pickle
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest
from cassandra import ConsistencyLevel, WriteTimeout, WriteType
from cassandra.cluster import EXEC_PROFILE_DEFAULT, ExecutionProfile
from pydantic import ValidationError

from feast import FeatureView
from feast.entity import Entity
from feast.errors import CassandraWriteTimeoutError
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.online_stores.cassandra_online_store import (
    cassandra_online_store as cassandra_module,
)
from feast.infra.online_stores.cassandra_online_store.cassandra_online_store import (
    CassandraInvalidConfig,
    CassandraOnlineStore,
    CassandraOnlineStoreConfig,
    _PendingWrites,
)
from feast.protos.feast.core.SortedFeatureView_pb2 import SortOrder
from feast.protos.feast.types.EntityKey_pb2 import EntityKey
from feast.protos.feast.types.Value_pb2 import Value
from feast.repo_config import RepoConfig
from feast.sorted_feature_view import SortedFeatureView, SortKey
from feast.types import Int64
from feast.value_type import ValueType


class ControlledFuture:
    def __init__(self, *, inline=False, error=None):
        self.inline = inline
        self.error = error
        self.attached = threading.Event()

    def add_callbacks(self, success, failure):
        self.success = success
        self.failure = failure
        self.attached.set()
        if self.inline:
            self.finish()

    def finish(self):
        assert self.attached.wait(1)
        if self.error is None:
            self.success(None)
        else:
            self.failure(self.error)


def repo_config(**online_options):
    return RepoConfig(
        registry="registry.db",
        project="safety_test",
        provider="local",
        online_store=CassandraOnlineStoreConfig(
            hosts=["localhost"], keyspace="test_keyspace", **online_options
        ),
    )


@pytest.fixture(params=[False, True], ids=["feature_view", "sorted_feature_view"])
def write_case(request, monkeypatch):
    source = FileSource(path="unused.parquet", timestamp_field="event_timestamp")
    fields = [Field(name="feature1", dtype=Int64), Field(name="sort_key", dtype=Int64)]
    options = dict(name="test_fv", source=source, schema=fields)
    if request.param:
        table = SortedFeatureView(
            **options,
            entities=[Entity(name="id", join_keys=["id"], value_type=ValueType.INT64)],
            ttl=timedelta(days=1),
            sort_keys=[
                SortKey(
                    name="sort_key",
                    value_type=ValueType.INT64,
                    default_sort_order=SortOrder.Enum.ASC,
                )
            ],
        )
    else:
        table = FeatureView(**options)
    store = CassandraOnlineStore()
    session = Mock()
    monkeypatch.setattr(store, "_get_session", lambda _config: session)
    monkeypatch.setattr(
        store,
        "_get_cql_statement",
        lambda *_args, **_kwargs: (
            "INSERT INTO test_keyspace.test_fv (a, b, c, d) VALUES (%s, %s, %s, %s)"
            " USING TTL %s"
        ),
    )
    data = [
        (
            EntityKey(join_keys=["id"], entity_values=[Value(int64_val=1)]),
            {"feature1": Value(int64_val=42), "sort_key": Value(int64_val=1)},
            datetime.now(timezone.utc),
            None,
        )
    ]
    return store, session, table, data


def test_write_completes_with_inline_callbacks(write_case):
    store, session, table, data = write_case
    session.execute_async.side_effect = lambda _batch: ControlledFuture(inline=True)
    store.online_write_batch(repo_config(), table, data, None)
    assert session.execute_async.call_count == 1


@pytest.mark.parametrize("row_count", [1, 2], ids=["drain", "backpressure"])
def test_missing_callbacks_raise_without_submitting_untracked_writes(
    write_case, row_count
):
    store, session, table, data = write_case
    session.execute_async.side_effect = lambda _batch: ControlledFuture()
    # One mini-batch per row, including the sorted-feature-view path.
    data = data * row_count
    started = time.monotonic()
    with pytest.raises(CassandraWriteTimeoutError, match="test_fv"):
        store.online_write_batch(
            repo_config(
                write_concurrency=1, write_batch_size=2, write_timeout_seconds=0.03
            ),
            table,
            data,
            None,
        )
    assert time.monotonic() - started < 1
    assert session.execute_async.call_count == 1


def test_inline_failure_is_not_success_when_last_pending_write_finishes(write_case):
    store, session, table, data = write_case
    failure = ValueError("invalid write")
    session.execute_async.return_value = ControlledFuture(inline=True, error=failure)
    with pytest.raises(ValueError, match="invalid write") as exc:
        store.online_write_batch(repo_config(), table, data, None)
    assert exc.value is failure


def test_final_async_failure_wakes_waiter_and_is_not_reported_as_success(write_case):
    store, session, table, data = write_case
    future = ControlledFuture(error=ValueError("last write failed"))
    session.execute_async.return_value = future
    with ThreadPoolExecutor(max_workers=1) as executor:
        result = executor.submit(
            store.online_write_batch,
            repo_config(write_timeout_seconds=1.0),
            table,
            data,
            None,
        )
        assert future.attached.wait(1)
        future.finish()
        with pytest.raises(ValueError, match="last write failed"):
            result.result(timeout=1)


@pytest.mark.parametrize("write_concurrency", [None, 0, -1])
def test_nonpositive_concurrency_still_applies_backpressure(
    write_concurrency, write_case
):
    store, session, table, data = write_case
    session.execute_async.side_effect = lambda _batch: ControlledFuture()
    with pytest.raises(CassandraWriteTimeoutError):
        store.online_write_batch(
            repo_config(
                write_concurrency=write_concurrency,
                write_batch_size=2,
                write_timeout_seconds=0.03,
            ),
            table,
            data * 2,
            None,
        )
    assert session.execute_async.call_count == 1


def test_write_deadline_is_independent_of_request_timeout(write_case):
    store, session, table, data = write_case
    session.execute_async.return_value = ControlledFuture()
    started = time.monotonic()
    with pytest.raises(CassandraWriteTimeoutError, match="0.03s"):
        store.online_write_batch(
            repo_config(request_timeout=30.0, write_timeout_seconds=0.03),
            table,
            data,
            None,
        )
    assert time.monotonic() - started < 1


def test_synchronous_submission_failure_is_propagated_and_releases_token():
    session = Mock()
    session.execute_async.side_effect = RuntimeError("submission failed")
    pending = _PendingWrites(1, 1.0, "test_fv")
    with pytest.raises(RuntimeError, match="submission failed"):
        CassandraOnlineStore._apply_batch(Mock(), None, session, pending)
    assert not pending._pending
    with pytest.raises(RuntimeError, match="submission failed"):
        pending.wait()


def test_callback_registration_failure_is_propagated_and_releases_token():
    session = Mock()
    session.execute_async.return_value.add_callbacks.side_effect = RuntimeError(
        "callback registration failed"
    )
    pending = _PendingWrites(1, 1.0, "test_fv")
    with pytest.raises(RuntimeError, match="callback registration failed"):
        CassandraOnlineStore._apply_batch(Mock(), None, session, pending)
    assert not pending._pending


@pytest.mark.parametrize("failure", [None, ValueError("async failed")])
def test_backpressure_resumes_on_real_thread_callback_and_stops_on_error(failure):
    first = ControlledFuture(error=failure)
    second = ControlledFuture(inline=True)
    session = Mock()
    session.execute_async.side_effect = [first, second]
    pending = _PendingWrites(1, 1.0, "test_fv")
    CassandraOnlineStore._apply_batch(Mock(), None, session, pending)
    entered = threading.Event()

    def submit_second():
        entered.set()
        CassandraOnlineStore._apply_batch(Mock(), None, session, pending)

    with ThreadPoolExecutor(max_workers=1) as executor:
        submitted = executor.submit(submit_second)
        assert entered.wait(1)
        assert not submitted.done()
        assert session.execute_async.call_count == 1
        first.finish()
        if failure is None:
            submitted.result(timeout=1)
            pending.wait()
            assert session.execute_async.call_count == 2
        else:
            with pytest.raises(ValueError, match="async failed"):
                submitted.result(timeout=1)
            assert session.execute_async.call_count == 1


def test_first_async_failure_wins_even_when_all_callbacks_have_completed():
    first, second = ControlledFuture(), ControlledFuture()
    session = Mock()
    session.execute_async.side_effect = [first, second]
    pending = _PendingWrites(2, 1.0, "test_fv")
    for _ in range(2):
        CassandraOnlineStore._apply_batch(Mock(), None, session, pending)
    second.failure(ValueError("first observed error"))
    first.failure(RuntimeError("later error"))
    with pytest.raises(ValueError, match="first observed error"):
        pending.wait()


def test_duplicate_callback_does_not_release_another_write():
    first, second = ControlledFuture(), ControlledFuture()
    session = Mock()
    session.execute_async.side_effect = [first, second]
    pending = _PendingWrites(2, 0.03, "test_fv")
    for _ in range(2):
        CassandraOnlineStore._apply_batch(Mock(), None, session, pending)
    first.finish()
    first.finish()
    with pytest.raises(CassandraWriteTimeoutError, match="1 pending"):
        pending.wait()


def test_one_deadline_is_shared_across_submissions(monkeypatch):
    now = [100.0]
    monkeypatch.setattr(cassandra_module.time, "monotonic", lambda: now[0])
    pending = _PendingWrites(1, 120.0, "test_fv")
    session = Mock()
    session.execute_async.side_effect = lambda _batch: ControlledFuture(inline=True)
    CassandraOnlineStore._apply_batch(Mock(), None, session, pending)
    now[0] = 219.0
    CassandraOnlineStore._apply_batch(Mock(), None, session, pending)
    now[0] = 221.0
    with pytest.raises(CassandraWriteTimeoutError):
        CassandraOnlineStore._apply_batch(Mock(), None, session, pending)
    assert session.execute_async.call_count == 2


@pytest.mark.parametrize("inline", [True, False], ids=["callback", "submission"])
def test_driver_write_timeout_remains_pickle_safe(inline):
    failure = WriteTimeout(
        "timeout",
        consistency=ConsistencyLevel.ONE,
        required_responses=1,
        received_responses=0,
        write_type=WriteType.SIMPLE,
    )
    session = Mock()
    if inline:
        session.execute_async.return_value = ControlledFuture(
            inline=True, error=failure
        )
    else:
        session.execute_async.side_effect = failure
    with pytest.raises(Exception, match="WriteTimeout") as exc:
        CassandraOnlineStore._apply_batch(
            Mock(), None, session, _PendingWrites(1, 1.0, "test_fv")
        )
    restored = pickle.loads(pickle.dumps(exc.value))
    assert str(restored) == str(exc.value)


def test_deadline_error_remains_pickle_safe():
    error = CassandraWriteTimeoutError("test_fv: deadline expired")
    restored = pickle.loads(pickle.dumps(error))
    assert type(restored) is CassandraWriteTimeoutError
    assert str(restored) == str(error)


@pytest.mark.parametrize("field", ["request_timeout", "write_timeout_seconds"])
@pytest.mark.parametrize(
    "value", [0.0, -1.0, float("nan"), float("inf"), float("-inf")]
)
def test_timeouts_require_finite_positive_values(field, value):
    with pytest.raises(ValidationError):
        CassandraOnlineStoreConfig(**{field: value})


@pytest.mark.parametrize("request_timeout", [None, 30.0])
@pytest.mark.parametrize("load_balancing", [False, True])
def test_request_timeout_wiring_preserves_finite_driver_default(
    monkeypatch, request_timeout, load_balancing
):
    cluster = Mock()
    cluster.connect.return_value.is_shutdown = False
    cluster_factory = Mock(return_value=cluster)
    monkeypatch.setattr(cassandra_module, "Cluster", cluster_factory)
    options = {"request_timeout": request_timeout}
    if load_balancing:
        options["load_balancing"] = (
            CassandraOnlineStoreConfig.CassandraLoadBalancingPolicy(
                load_balancing_policy="DCAwareRoundRobinPolicy", local_dc="dc1"
            )
        )
    store = CassandraOnlineStore()
    config = repo_config(**options)
    assert store._get_session(config) is store._get_session(config)
    cluster.connect.assert_called_once()
    profiles = cluster_factory.call_args.kwargs.get("execution_profiles")
    if profiles:
        profile = profiles[EXEC_PROFILE_DEFAULT]
        assert profile.request_timeout == (
            request_timeout
            if request_timeout is not None
            else ExecutionProfile().request_timeout
        )
        assert profile.load_balancing_policy is not None
    else:
        assert request_timeout is None and not load_balancing


def test_store_refuses_inherited_live_session_without_touching_it(monkeypatch):
    store = CassandraOnlineStore()
    session = Mock()
    cluster = Mock()
    store._session = session
    store._cluster = cluster
    monkeypatch.setattr(store, "_session_pid", os.getpid() + 1)
    with pytest.raises(CassandraInvalidConfig, match="cannot be shared"):
        store._get_session(repo_config())
    store.__del__()
    assert not session.mock_calls
    assert not cluster.mock_calls


def test_prepared_statements_belong_to_each_store():
    first, second = CassandraOnlineStore(), CassandraOnlineStore()
    first._prepared_statements["insert"] = Mock()
    assert second._prepared_statements == {}


def test_reconnecting_clears_prepared_statement_cache(monkeypatch):
    store = CassandraOnlineStore()
    store._session = Mock(is_shutdown=True)
    store._prepared_statements["old"] = Mock()
    monkeypatch.setattr(cassandra_module, "Cluster", Mock())
    store._get_session(repo_config())
    assert store._prepared_statements == {}
