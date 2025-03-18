import textwrap
from datetime import datetime

import pytest

from feast import FeatureView
from feast.entity import Entity
from feast.field import Field
from feast.infra.offline_stores.dask import DaskOfflineStoreConfig
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.online_stores.contrib.cassandra_online_store.cassandra_online_store import (
    CassandraOnlineStore,
    CassandraOnlineStoreConfig,
)
from feast.protos.feast.core.SortedFeatureView_pb2 import SortOrder
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
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


def test_online_write_batch_for_sorted_feature_view(cassandra_repo_config):
    repo_config, container = cassandra_repo_config[0], cassandra_repo_config[1]

    container.exec(
        'cqlsh -e "CREATE TABLE feast_keyspace.test_range_query_sortedfeatureview(entity_key TEXT,text TEXT, int int, event_ts TIMESTAMP,created_ts TIMESTAMP,PRIMARY KEY (entity_key));"'
    )

    (
        feature_view,
        data,
    ) = _create_n_test_sample_features(
        n=10,
    )

    CassandraOnlineStore().online_write_batch(
        config=repo_config,
        table=feature_view,
        data=data,
        progress=None,
    )
    assert "10" in container.exec(
        'cqlsh -e "select COUNT(*) from feast_keyspace.test_range_query_sortedfeatureview;"'
    ).output.decode("utf-8")


def _create_n_test_sample_features(n=10):
    fv = SortedFeatureView(
        name="sortedfeatureview",
        source=SOURCE,
        entities=[Entity(name="id")],
        sort_keys=[
            SortKey(
                name="event_timestamp",
                value_type=ValueType.UNIX_TIMESTAMP,
                default_sort_order=SortOrder.DESC,
            )
        ],
        schema=[
            Field(
                name="id",
                dtype=String,
            ),
            Field(
                name="text",
                dtype=String,
            ),
            Field(
                name="int",
                dtype=Int32,
            ),
        ],
    )
    return fv, [
        (
            EntityKeyProto(
                join_keys=["id"],
                entity_values=[ValueProto(string_val=str(i))],
            ),
            {
                "text": ValueProto(string_val="text"),
                "int": ValueProto(int32_val=n),
            },
            datetime.utcnow(),
            None,
        )
        for i in range(n)
    ]


def test_build_sorted_table_cql(sorted_feature_view):
    project = "test_project"
    fqtable = "test_keyspace.test_project_test_sorted_feature_view"

    expected_cql = textwrap.dedent("""\
        CREATE TABLE IF NOT EXISTS test_keyspace.test_project_test_sorted_feature_view (
            entity_key TEXT,
            feature1 BIGINT,feature2 LIST<TEXT>,sort_key1 BIGINT,sort_key2 TEXT,
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


def test_build_sorted_table_cql_with_timestamp_sort_key(sorted_feature_view_with_ts):
    project = "test_project"
    fqtable = "test_keyspace.test_project_test_sorted_feature_view"

    expected_cql = textwrap.dedent("""\
        CREATE TABLE IF NOT EXISTS test_keyspace.test_project_test_sorted_feature_view (
            entity_key TEXT,
            feature1 BIGINT,feature2 LIST<TEXT>,sort_key1 BIGINT,sort_key2 TEXT,
            event_ts TIMESTAMP,
            created_ts TIMESTAMP,
            PRIMARY KEY ((entity_key), sort_key1, sort_key2, event_ts)
        ) WITH CLUSTERING ORDER BY (sort_key1 ASC, sort_key2 DESC, event_ts DESC)
        AND COMMENT='project=test_project, feature_view=test_sorted_feature_view';
    """).strip()

    cassandra_online_store = CassandraOnlineStore()
    actual_cql = cassandra_online_store._build_sorted_table_cql(
        project, sorted_feature_view_with_ts, fqtable
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
