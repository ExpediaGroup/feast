import textwrap
import threading
import time
from datetime import datetime, timezone
from queue import Queue

import pytest

from feast import FeatureView
from feast.entity import Entity
from feast.field import Field
from feast.infra.offline_stores.dask import DaskOfflineStoreConfig
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.online_stores.cassandra_online_store import (
    cassandra_online_store as cassandra_online_store_module,
)
from feast.infra.online_stores.cassandra_online_store.cassandra_online_store import (
    CassandraOnlineStore,
    CassandraOnlineStoreConfig,
    CassandraWriteTimeoutError,
)
from feast.protos.feast.core.SortedFeatureView_pb2 import SortOrder
from feast.protos.feast.types.Value_pb2 import StringList
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
from feast.sorted_feature_view import SortedFeatureView, SortKey
from feast.types import (
    Array,
    Bool,
    Bytes,
    Float32,
    Float64,
    Int32,
    Int64,
    String,
    UnixTimestamp,
)
from feast.value_type import ValueType
from tests.integration.feature_repos.universal.online_store.cassandra import (
    CassandraOnlineStoreCreator,
)

REGISTRY = "s3://test_registry/registry.db"
PROJECT = "test_range_query"
PROVIDER = "aws"
REGION = "us-west-2"
SOURCE = FileSource(
    path="some path",
    timestamp_field="event_timestamp",
)


@pytest.fixture
def sorted_feature_view(file_source):
    return SortedFeatureView(
        name="test_sorted_feature_view",
        entities=[Entity(name="entity1", join_keys=["entity1_id"])],
        source=FileSource(name="my_file_source", path="test.parquet"),
        schema=[
            Field(name="feature1", dtype=Int64),
            Field(name="feature2", dtype=Array(String)),
            Field(name="sort_key1", dtype=Int64),
            Field(name="sort_key2", dtype=String),
        ],
        sort_keys=[
            SortKey(
                name="sort_key1",
                value_type=ValueType.INT64,
                default_sort_order=SortOrder.Enum.ASC,
            ),
            SortKey(
                name="sort_key2",
                value_type=ValueType.STRING,
                default_sort_order=SortOrder.Enum.DESC,
            ),
        ],
    )


@pytest.fixture
def sorted_feature_view_with_ts(file_source):
    return SortedFeatureView(
        name="test_sorted_feature_view",
        entities=[Entity(name="entity1", join_keys=["entity1_id"])],
        source=FileSource(
            name="my_file_source", path="test.parquet", timestamp_field="sort_key3"
        ),
        schema=[
            Field(name="feature1", dtype=Int64),
            Field(name="feature2", dtype=Array(String)),
            Field(name="sort_key1", dtype=Int64),
            Field(name="sort_key2", dtype=String),
        ],
        sort_keys=[
            SortKey(
                name="sort_key1",
                value_type=ValueType.INT64,
                default_sort_order=SortOrder.Enum.ASC,
            ),
            SortKey(
                name="sort_key2",
                value_type=ValueType.STRING,
                default_sort_order=SortOrder.Enum.DESC,
            ),
            SortKey(
                name="sort_key3",
                value_type=ValueType.UNIX_TIMESTAMP,
                default_sort_order=SortOrder.Enum.DESC,
            ),
        ],
    )


@pytest.fixture
def file_source():
    file_source = FileSource(name="my_file_source", path="test.parquet")
    return file_source


@pytest.fixture(scope="session")
def embedded_cassandra():
    online_store_creator = CassandraOnlineStoreCreator("cassandra")
    online_store_config = online_store_creator.create_online_store()

    yield online_store_config

    # Tearing down the Cassandra instance after all tests in the class
    online_store_creator.teardown()


@pytest.fixture(scope="session")
def cassandra_repo_config(embedded_cassandra):
    return RepoConfig(
        registry=REGISTRY,
        project=PROJECT,
        provider=PROVIDER,
        online_store=CassandraOnlineStoreConfig(
            type=embedded_cassandra["type"],
            hosts=embedded_cassandra["hosts"],
            port=embedded_cassandra["port"],
            keyspace=embedded_cassandra["keyspace"],
            write_concurrency=100,
        ),
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=2,
    ), embedded_cassandra["container"]


def test_fq_table_name_v1_within_limit(file_source):
    keyspace = "test_keyspace"
    project = "test_project"
    table = FeatureView(name="test_feature_view", source=file_source)

    expected_table_name = f'"{keyspace}"."{project}_{table.name}"'
    actual_table_name = CassandraOnlineStore._fq_table_name(keyspace, project, table, 1)

    assert expected_table_name == actual_table_name


def test_fq_table_name_v1_exceeds_limit(file_source):
    keyspace = "test_keyspace"
    project = "test_project"
    table = FeatureView(
        name="test_feature_view_with_a_very_long_name_exceeding_limit",
        source=file_source,
    )
    expected_table_name = f'"{keyspace}"."{project}_{table.name}"'
    actual_table_name = CassandraOnlineStore._fq_table_name(keyspace, project, table, 1)

    assert expected_table_name == actual_table_name


def test_fq_table_name_v2_within_limit(file_source):
    keyspace = "test_keyspace"
    project = "test_project"
    table = FeatureView(name="test_feature_view", source=file_source)

    expected_table_name = f'"{keyspace}"."{project}_{table.name}"'
    actual_table_name = CassandraOnlineStore._fq_table_name(keyspace, project, table, 2)

    assert expected_table_name == actual_table_name


def test_fq_table_name_v2_exceeds_limit(file_source):
    keyspace = "test_keyspace"
    project = "test_project"
    table = FeatureView(
        name="test_feature_view_with_a_very_long_name_exceeding_limit",
        source=file_source,
    )
    expected_table_name = (
        f'"{keyspace}"."test__29UZUpJQRijDZsYzl_test__5Ur8Mv5QutEG23Cp2C"'
    )
    actual_table_name = CassandraOnlineStore._fq_table_name(keyspace, project, table, 2)

    assert expected_table_name == actual_table_name


def test_fq_table_name_invalid_version(file_source):
    keyspace = "test_keyspace"
    project = "test_project"
    table = FeatureView(name="test_feature_view", source=file_source)

    with pytest.raises(ValueError) as excinfo:
        CassandraOnlineStore._fq_table_name(keyspace, project, table, 3)
    assert "Unknown table name format version: 3" in str(excinfo.value)


def test_build_sorted_table_cql(sorted_feature_view):
    project = "test_project"
    fqtable = "test_keyspace.test_project_test_sorted_feature_view"

    expected_cql = textwrap.dedent("""\
        CREATE TABLE IF NOT EXISTS test_keyspace.test_project_test_sorted_feature_view (
            entity_key TEXT,
            feature1 BLOB, feature2 BLOB, sort_key1 BIGINT, sort_key2 TEXT,
            event_ts TIMESTAMP,
            created_ts TIMESTAMP,
            PRIMARY KEY ((entity_key), sort_key1, sort_key2)
        ) WITH CLUSTERING ORDER BY (sort_key1 ASC, sort_key2 DESC)
        AND COMMENT='project=test_project, feature_view=test_sorted_feature_view';
    """).strip()

    cassandra_online_store = CassandraOnlineStore()
    actual_cql = cassandra_online_store._build_sorted_table_cql(
        project, sorted_feature_view, fqtable
    )

    assert actual_cql == expected_cql


def test_sorted_view_with_empty_schema_raises_error(file_source):
    with pytest.raises(ValueError) as excinfo:
        SortedFeatureView(
            name="empty_schema_view",
            entities=[Entity(name="entity1", join_keys=["entity1_id"])],
            source=file_source,
            schema=[],
            sort_keys=[
                SortKey(
                    name="nonexistent",
                    value_type=ValueType.INT64,
                    default_sort_order=SortOrder.Enum.ASC,
                )
            ],
        )
    assert "does not match any feature name" in str(excinfo.value)


def test_get_cql_type():
    store = CassandraOnlineStore()
    assert store._get_cql_type(Bytes) == "BLOB"
    assert store._get_cql_type(String) == "TEXT"
    assert store._get_cql_type(Int32) == "INT"
    assert store._get_cql_type(Int64) == "BIGINT"
    assert store._get_cql_type(Float32) == "FLOAT"
    assert store._get_cql_type(Float64) == "DOUBLE"
    assert store._get_cql_type(Bool) == "BOOLEAN"
    assert store._get_cql_type(UnixTimestamp) == "TIMESTAMP"
    assert store._get_cql_type(Array(Bytes)) == "LIST<BLOB>"
    assert store._get_cql_type(Array(String)) == "LIST<TEXT>"
    assert store._get_cql_type(Array(Int32)) == "LIST<INT>"
    assert store._get_cql_type(Array(Int64)) == "LIST<BIGINT>"
    assert store._get_cql_type(Array(Float32)) == "LIST<FLOAT>"
    assert store._get_cql_type(Array(Float64)) == "LIST<DOUBLE>"
    assert store._get_cql_type(Array(Bool)) == "LIST<BOOLEAN>"


def test_canonical_column_name():
    from feast.infra.online_stores.cassandra_online_store.cassandra_online_store import (
        _canonical_column_name,
    )

    assert _canonical_column_name("featureX") == "featurex"
    assert _canonical_column_name("FEATURE") == "feature"
    assert _canonical_column_name("already_lower") == "already_lower"
    assert _canonical_column_name("") == ""


def test_check_no_case_collisions_raises(file_source):
    from feast.infra.online_stores.cassandra_online_store.cassandra_online_store import (
        CassandraInvalidConfig,
    )

    fv = FeatureView(
        name="collision_view",
        source=file_source,
        schema=[
            Field(name="featureX", dtype=Int64),
            Field(name="featurex", dtype=Int64),
        ],
    )
    with pytest.raises(CassandraInvalidConfig, match="differ only in case"):
        CassandraOnlineStore._check_no_case_collisions(fv)


def test_check_no_case_collisions_passes(file_source):
    fv = FeatureView(
        name="good_view",
        source=file_source,
        schema=[
            Field(name="featureX", dtype=Int64),
            Field(name="featureY", dtype=Int64),
        ],
    )
    CassandraOnlineStore._check_no_case_collisions(fv)


def test_apply_batch_full_queue_times_out_without_submitting_write(mocker):
    """
    If the bounded queue is already saturated (the driver isn't draining
    in-flight writes), _apply_batch must time out enqueuing rather than
    submit the write. This is what prevents an orphaned, untracked write
    from being issued when the cluster/driver is unresponsive.
    """
    mocker.patch.object(
        cassandra_online_store_module, "PENDING_FUTURE_TIMEOUT_SECONDS", 0.05
    )
    full_queue: Queue = Queue(maxsize=1)
    full_queue.put(True)

    session = mocker.MagicMock()
    batch = mocker.MagicMock()

    with pytest.raises(Exception) as excinfo:
        CassandraOnlineStore._apply_batch(
            batch,
            None,
            session,
            full_queue,
            on_success=mocker.MagicMock(),
            on_failure=mocker.MagicMock(),
        )

    assert "Timed out after 0.05s enqueuing" in str(excinfo.value)
    session.execute_async.assert_not_called()


def test_apply_batch_happy_path_submits_and_tracks_write(mocker):
    """Regression guard: the reordered put()-before-execute_async still
    submits the write and attaches callbacks on the normal (non-saturated)
    path."""
    queue: Queue = Queue(maxsize=10)
    future = mocker.MagicMock()
    session = mocker.MagicMock()
    session.execute_async.return_value = future
    batch = mocker.MagicMock()
    progress = mocker.MagicMock()

    CassandraOnlineStore._apply_batch(
        batch,
        progress,
        session,
        queue,
        on_success=mocker.MagicMock(),
        on_failure=mocker.MagicMock(),
    )

    session.execute_async.assert_called_once_with(batch)
    future.add_callbacks.assert_called_once()
    progress.assert_called_once_with(1)
    # the reserved slot is only freed by the success/failure callback,
    # which the mock future never invokes here.
    assert queue.qsize() == 1


def test_apply_batch_releases_slot_if_execute_async_raises(mocker):
    """If execute_async raises synchronously (e.g. a connection error), the
    reserved queue slot must be released so it doesn't leak."""
    queue: Queue = Queue(maxsize=1)
    session = mocker.MagicMock()
    session.execute_async.side_effect = RuntimeError("boom")
    batch = mocker.MagicMock()

    with pytest.raises(RuntimeError, match="boom"):
        CassandraOnlineStore._apply_batch(
            batch,
            None,
            session,
            queue,
            on_success=mocker.MagicMock(),
            on_failure=mocker.MagicMock(),
        )

    assert queue.empty()


def test_online_write_batch_drain_deadline_raises(mocker, file_source):
    """
    If a write future's callback never fires (the driver/cluster is
    unresponsive), the final drain loop in online_write_batch must raise
    after PENDING_FUTURE_TIMEOUT_SECONDS instead of spinning forever.
    """
    mocker.patch.object(
        cassandra_online_store_module, "PENDING_FUTURE_TIMEOUT_SECONDS", 0.05
    )

    # A future whose callback is never invoked, so the queue never drains.
    future = mocker.MagicMock()
    future.add_callbacks = lambda ok, err: None

    session = mocker.MagicMock()
    session.execute_async.return_value = future
    session.prepare.return_value = mocker.MagicMock()

    store = CassandraOnlineStore()
    table = FeatureView(name="test_fv", source=file_source)

    entity_key = mocker.MagicMock()
    feature_val = mocker.MagicMock()
    data = [(entity_key, {"feature1": feature_val}, datetime.now(timezone.utc), None)]

    mocker.patch.object(store, "_get_session", return_value=session)
    mocker.patch.object(store, "_get_cql_statement", return_value=mocker.MagicMock())
    mocker.patch(
        "feast.infra.online_stores.cassandra_online_store.cassandra_online_store.serialize_entity_key",
        return_value=b"\x00",
    )

    config = RepoConfig(
        registry=REGISTRY,
        project=PROJECT,
        provider=PROVIDER,
        online_store=CassandraOnlineStoreConfig(
            hosts=["localhost"],
            keyspace="test_keyspace",
            write_concurrency=1,
        ),
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=2,
    )

    start = time.monotonic()
    with pytest.raises(Exception) as excinfo:
        store.online_write_batch(
            config=config,
            table=table,
            data=data,
            progress=None,
        )
    elapsed = time.monotonic() - start

    assert "Timed out after 0.05s waiting for" in str(excinfo.value)
    assert "pending Cassandra write future" in str(excinfo.value)
    # Should raise close to the deadline, not hang.
    assert elapsed < 5


def test_get_session_applies_request_timeout_without_load_balancing(mocker):
    """
    request_timeout must be applied via an ExecutionProfile even when
    `load_balancing` isn't configured -- otherwise it's silently dropped
    and never enforced by the driver.
    """
    captured_kwargs = {}

    def fake_cluster(hosts, **kwargs):
        captured_kwargs.update(kwargs)
        cluster = mocker.MagicMock()
        cluster.connect.return_value = mocker.MagicMock()
        return cluster

    mocker.patch(
        "feast.infra.online_stores.cassandra_online_store.cassandra_online_store.Cluster",
        side_effect=fake_cluster,
    )

    config = RepoConfig(
        registry=REGISTRY,
        project=PROJECT,
        provider=PROVIDER,
        online_store=CassandraOnlineStoreConfig(
            hosts=["localhost"],
            keyspace="test_keyspace",
            request_timeout=30,
        ),
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=2,
    )

    store = CassandraOnlineStore()
    store._get_session(config)

    assert "execution_profiles" in captured_kwargs
    profiles = captured_kwargs["execution_profiles"]
    default_profile = next(iter(profiles.values()))
    assert default_profile.request_timeout == 30


def _fake_cluster_capturing_kwargs(mocker):
    captured_kwargs = {}

    def fake_cluster(hosts, **kwargs):
        captured_kwargs.update(kwargs)
        cluster = mocker.MagicMock()
        cluster.connect.return_value = mocker.MagicMock()
        return cluster

    mocker.patch(
        "feast.infra.online_stores.cassandra_online_store.cassandra_online_store.Cluster",
        side_effect=fake_cluster,
    )
    return captured_kwargs


def test_get_session_no_execution_profile_when_neither_configured(mocker):
    """
    Regression guard: when neither `request_timeout` nor `load_balancing`
    is set, no ExecutionProfile should be built at all -- this is the
    no-op path the overwhelming majority of existing deployments take, and
    it must be left untouched by the request_timeout wiring fix.
    """
    captured_kwargs = _fake_cluster_capturing_kwargs(mocker)

    config = RepoConfig(
        registry=REGISTRY,
        project=PROJECT,
        provider=PROVIDER,
        online_store=CassandraOnlineStoreConfig(
            hosts=["localhost"], keyspace="test_keyspace"
        ),
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=2,
    )

    CassandraOnlineStore()._get_session(config)

    assert captured_kwargs.get("execution_profiles") is None


def test_get_session_load_balancing_without_request_timeout_sets_real_timeout(
    mocker,
):
    """
    This is the actual root cause of the incident this module's
    PENDING_FUTURE_TIMEOUT_SECONDS deadline guards against: when
    `load_balancing` is configured but `request_timeout` is not,
    ExecutionProfile(request_timeout=None) stores None verbatim -- which
    disables the driver's own per-request timer entirely, rather than
    falling back to the driver's 10s default. A write can then hang with
    no client-side deadline at all. Assert request_timeout is never passed
    through as None.
    """
    captured_kwargs = _fake_cluster_capturing_kwargs(mocker)

    config = RepoConfig(
        registry=REGISTRY,
        project=PROJECT,
        provider=PROVIDER,
        online_store=CassandraOnlineStoreConfig(
            hosts=["localhost"],
            keyspace="test_keyspace",
            load_balancing=CassandraOnlineStoreConfig.CassandraLoadBalancingPolicy(
                load_balancing_policy="DCAwareRoundRobinPolicy",
                local_dc="dc1",
            ),
            # request_timeout intentionally left unset
        ),
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=2,
    )

    CassandraOnlineStore()._get_session(config)

    profiles = captured_kwargs["execution_profiles"]
    default_profile = next(iter(profiles.values()))
    # The driver's own default (10.0s) applies here because we never pass
    # request_timeout=None into ExecutionProfile; this must NOT be None.
    assert default_profile.request_timeout is not None
    assert default_profile.request_timeout == 10.0


def test_get_session_request_timeout_only_preserves_load_balancing_policy(mocker):
    """
    The elif branch that applies request_timeout without an explicit
    `load_balancing` config must still pass an explicit load-balancing
    policy to the driver (via default_lbp_factory()), not leave it
    implicit -- otherwise the driver logs a forward-compatibility warning
    on every connect and, per the driver's own docs, will raise in a
    future major version.
    """
    captured_kwargs = _fake_cluster_capturing_kwargs(mocker)

    config = RepoConfig(
        registry=REGISTRY,
        project=PROJECT,
        provider=PROVIDER,
        online_store=CassandraOnlineStoreConfig(
            hosts=["localhost"],
            keyspace="test_keyspace",
            request_timeout=5,
        ),
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=2,
    )

    CassandraOnlineStore()._get_session(config)

    profiles = captured_kwargs["execution_profiles"]
    default_profile = next(iter(profiles.values()))
    assert default_profile.load_balancing_policy is not None


def test_online_write_batch_happy_path_with_real_callbacks(mocker, file_source):
    """
    End-to-end happy path through the *real* on_success/on_failure closures
    (not mocked away), proving the reserved queue slot is correctly released
    by a genuine success callback and the write completes without error.
    """
    future = mocker.MagicMock()
    future.add_callbacks = lambda ok, err: ok(mocker.MagicMock())

    session = mocker.MagicMock()
    session.execute_async.return_value = future
    session.prepare.return_value = mocker.MagicMock()

    store = CassandraOnlineStore()
    table = FeatureView(name="test_fv", source=file_source)
    entity_key = mocker.MagicMock()
    feature_val = mocker.MagicMock()
    data = [(entity_key, {"feature1": feature_val}, datetime.now(timezone.utc), None)]

    mocker.patch.object(store, "_get_session", return_value=session)
    mocker.patch.object(store, "_get_cql_statement", return_value=mocker.MagicMock())
    mocker.patch(
        "feast.infra.online_stores.cassandra_online_store.cassandra_online_store.serialize_entity_key",
        return_value=b"\x00",
    )

    progress = mocker.MagicMock()
    store.online_write_batch(
        config=RepoConfig(
            registry=REGISTRY,
            project=PROJECT,
            provider=PROVIDER,
            online_store=CassandraOnlineStoreConfig(
                hosts=["localhost"], keyspace="test_keyspace", write_concurrency=1
            ),
            offline_store=DaskOfflineStoreConfig(),
            entity_key_serialization_version=2,
        ),
        table=table,
        data=data,
        progress=progress,
    )

    session.execute_async.assert_called_once()
    progress.assert_called()


def test_online_write_batch_sorted_feature_view_drain_deadline_raises(
    mocker, sorted_feature_view
):
    """
    The SortedFeatureView code path calls _apply_batch through a different,
    per-entity-key-group loop than the plain FeatureView path. The deadline
    behavior must hold there too.
    """
    mocker.patch.object(
        cassandra_online_store_module, "PENDING_FUTURE_TIMEOUT_SECONDS", 0.05
    )

    future = mocker.MagicMock()
    future.add_callbacks = lambda ok, err: None  # callback never fires

    session = mocker.MagicMock()
    session.execute_async.return_value = future
    session.prepare.return_value = mocker.MagicMock()

    store = CassandraOnlineStore()
    entity_key = mocker.MagicMock()
    data = [
        (
            entity_key,
            {
                "feature1": ValueProto(int64_val=1),
                "feature2": ValueProto(string_list_val=StringList(val=["a"])),
                "sort_key1": ValueProto(int64_val=1),
                "sort_key2": ValueProto(string_val="z"),
            },
            datetime.now(timezone.utc),
            None,
        )
    ]

    mocker.patch.object(store, "_get_session", return_value=session)
    mocker.patch.object(store, "_get_cql_statement", return_value=mocker.MagicMock())
    mocker.patch(
        "feast.infra.online_stores.cassandra_online_store.cassandra_online_store.serialize_entity_key",
        return_value=b"\x00",
    )

    config = RepoConfig(
        registry=REGISTRY,
        project=PROJECT,
        provider=PROVIDER,
        online_store=CassandraOnlineStoreConfig(
            hosts=["localhost"], keyspace="test_keyspace", write_concurrency=1
        ),
        offline_store=DaskOfflineStoreConfig(),
        entity_key_serialization_version=2,
    )

    with pytest.raises(CassandraWriteTimeoutError) as excinfo:
        store.online_write_batch(
            config=config,
            table=sorted_feature_view,
            data=data,
            progress=None,
        )

    assert "Timed out after 0.05s waiting for" in str(excinfo.value)
    assert sorted_feature_view.name in str(excinfo.value)


def test_apply_batch_real_concurrent_release_across_threads(mocker):
    """
    Genuine cross-thread concurrency: a background thread plays the role of
    the driver's callback thread, releasing a queue slot shortly after the
    main thread blocks in put(). Proves the reservation and release aren't
    just correct in a single-threaded mock, but actually work under real
    contention.
    """
    queue: Queue = Queue(maxsize=1)
    queue.put(True)  # slot already occupied, as if one write is in flight

    def release_after_delay():
        time.sleep(0.05)
        queue.get_nowait()  # simulates the in-flight write's on_success firing

    releaser = threading.Thread(target=release_after_delay)
    releaser.start()

    future = mocker.MagicMock()
    session = mocker.MagicMock()
    session.execute_async.return_value = future
    batch = mocker.MagicMock()

    mocker.patch.object(
        cassandra_online_store_module, "PENDING_FUTURE_TIMEOUT_SECONDS", 2
    )

    start = time.monotonic()
    CassandraOnlineStore._apply_batch(
        batch,
        None,
        session,
        queue,
        on_success=mocker.MagicMock(),
        on_failure=mocker.MagicMock(),
        fqtable="test_keyspace.test_table",
    )
    elapsed = time.monotonic() - start
    releaser.join()

    # put() had to block until the background thread released the slot.
    assert elapsed >= 0.04
    session.execute_async.assert_called_once_with(batch)


def test_apply_batch_shares_one_deadline_across_multiple_calls(mocker):
    """
    _apply_batch must spend down a single shared deadline across
    successive calls, not get a fresh full timeout window every time --
    otherwise a multi-batch write's total wait time grows with the number
    of batches (K * timeout) instead of staying bounded by one timeout
    total, which is what "hard deadline" is supposed to mean.
    """
    queue: Queue = Queue(maxsize=1)
    queue.put(True)  # occupied

    session = mocker.MagicMock()
    session.execute_async.return_value = mocker.MagicMock()
    batch = mocker.MagicMock()

    shared_deadline = time.monotonic() + 0.2

    # First call: a background thread frees the slot quickly, so this call
    # only spends ~0.05s of the shared 0.2s budget.
    def release_after_delay():
        time.sleep(0.05)
        queue.get_nowait()

    releaser = threading.Thread(target=release_after_delay)
    releaser.start()

    CassandraOnlineStore._apply_batch(
        batch,
        None,
        session,
        queue,
        on_success=mocker.MagicMock(),
        on_failure=mocker.MagicMock(),
        fqtable="test_keyspace.test_table",
        deadline=shared_deadline,
    )
    releaser.join()
    # The first call's own reservation is never released by the MagicMock
    # future (its add_callbacks never invokes a real callback) -- release
    # it manually to simulate that first write having completed, then
    # re-occupy the slot for the second call's scenario.
    queue.get_nowait()

    # Second call: queue is full again (nothing frees it this time), using
    # the SAME shared_deadline. It must time out close to the remaining
    # ~0.15s budget, not get a fresh 0.2s window.
    queue.put(True)  # occupied again
    start = time.monotonic()
    with pytest.raises(CassandraWriteTimeoutError):
        CassandraOnlineStore._apply_batch(
            batch,
            None,
            session,
            queue,
            on_success=mocker.MagicMock(),
            on_failure=mocker.MagicMock(),
            fqtable="test_keyspace.test_table",
            deadline=shared_deadline,
        )
    elapsed = time.monotonic() - start

    # Bounded by the *remaining* budget (~0.15s), well under a fresh 0.2s
    # window, and nowhere near what two independent 0.2s timeouts would sum to.
    assert elapsed < 0.19
