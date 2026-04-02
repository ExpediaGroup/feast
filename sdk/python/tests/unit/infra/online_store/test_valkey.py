import time
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest
from valkey import Valkey

from feast import Entity, FeatureView, Field, FileSource, RepoConfig, ValueType
from feast.infra.online_stores.eg_valkey import (
    EGValkeyOnlineStore,
    EGValkeyOnlineStoreConfig,
    _deserialize_vector_from_bytes,
    _get_valkey_vector_type,
    _get_vector_index_name,
    _serialize_vector_to_bytes,
)
from feast.infra.online_stores.helpers import _mmh3, _redis_key
from feast.protos.feast.core.SortedFeatureView_pb2 import SortOrder
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import (
    DoubleList,
    FloatList,
)
from feast.protos.feast.types.Value_pb2 import (
    Value as ValueProto,
)
from feast.sorted_feature_view import SortedFeatureView, SortKey
from feast.types import (
    Array,
    Float32,
    Float64,
    Int32,
    Int64,
    String,
    UnixTimestamp,
)
from tests.unit.infra.online_store.valkey_online_store_creator import (
    ValkeyOnlineStoreCreator,
)


@pytest.fixture
def valkey_online_store() -> EGValkeyOnlineStore:
    return EGValkeyOnlineStore()


@pytest.fixture(scope="session")
def valkey_online_store_config():
    creator = ValkeyOnlineStoreCreator("valkey_project")
    config = creator.create_online_store()
    yield config
    creator.teardown()


@pytest.fixture
def base_repo_config_kwargs():
    return dict(
        provider="local",
        project="test",
        entity_key_serialization_version=3,
        registry="dummy_registry.db",
    )


@pytest.fixture
def repo_config_without_docker_connection_string(base_repo_config_kwargs) -> RepoConfig:
    return RepoConfig(
        **base_repo_config_kwargs,
        online_store=EGValkeyOnlineStoreConfig(
            connection_string="valkey://localhost:6379",
        ),
    )


@pytest.fixture
def repo_config(valkey_online_store_config, base_repo_config_kwargs) -> RepoConfig:
    return RepoConfig(
        **base_repo_config_kwargs,
        online_store=EGValkeyOnlineStoreConfig(
            connection_string=valkey_online_store_config["connection_string"],
        ),
    )


@pytest.mark.docker
def test_valkey_online_write_batch_with_timestamp_as_sortkey(
    repo_config: RepoConfig,
    valkey_online_store: EGValkeyOnlineStore,
):
    (
        feature_view,
        data,
    ) = _create_sorted_feature_view_with_timestamp_as_sortkey(0)

    valkey_online_store.online_write_batch(
        config=repo_config,
        table=feature_view,
        data=data,
        progress=None,
    )

    connection_string = repo_config.online_store.connection_string
    connection_string_split = connection_string.split(":")
    conn_dict = {}
    conn_dict["host"] = connection_string_split[0]
    conn_dict["port"] = connection_string_split[1]

    r = Valkey(**conn_dict)

    pipe = r.pipeline(transaction=True)

    entity_key_driver_1 = EntityKeyProto(
        join_keys=["driver_id"],
        entity_values=[ValueProto(int32_val=1)],
    )

    redis_key_bin_driver_1 = _redis_key(
        repo_config.project,
        entity_key_driver_1,
        entity_key_serialization_version=repo_config.entity_key_serialization_version,
    )

    zset_key_driver_1 = valkey_online_store.zset_key_bytes(
        feature_view.name, redis_key_bin_driver_1
    )

    entity_key_driver_2 = EntityKeyProto(
        join_keys=["driver_id"],
        entity_values=[ValueProto(int32_val=2)],
    )
    redis_key_bin_driver_2 = _redis_key(
        repo_config.project,
        entity_key_driver_2,
        entity_key_serialization_version=repo_config.entity_key_serialization_version,
    )

    zset_key_driver_2 = valkey_online_store.zset_key_bytes(
        feature_view.name, redis_key_bin_driver_2
    )

    driver_1_zset_members = r.zrange(zset_key_driver_1, 0, -1, withscores=True)
    driver_2_zset_members = r.zrange(zset_key_driver_2, 0, -1, withscores=True)

    assert len(driver_1_zset_members) == 5
    assert len(driver_2_zset_members) == 5

    # Get last 3 trips for both drivers from the respective sorted sets
    last_3_trips_driver_1 = r.zrevrangebyscore(
        zset_key_driver_1, "+inf", "-inf", start=0, num=3
    )
    last_3_trips_driver_2 = r.zrevrangebyscore(
        zset_key_driver_2, "+inf", "-inf", start=0, num=3
    )

    # Look up features for last 3 trips for driver 1
    for id in last_3_trips_driver_1:
        hash_key = valkey_online_store.hash_key_bytes(redis_key_bin_driver_1, id)
        pipe.hgetall(hash_key)

    # Look up features for last 3 trips for driver 2
    for id in last_3_trips_driver_2:
        hash_key = valkey_online_store.hash_key_bytes(redis_key_bin_driver_2, id)
        pipe.hgetall(hash_key)

    features_list = pipe.execute()

    trip_id_feature_name = _mmh3(f"{feature_view.name}:trip_id")
    trip_id_drivers = []
    for feature_dict in features_list:
        val = ValueProto()
        val.ParseFromString(feature_dict[trip_id_feature_name])
        trip_id_drivers.append(val.int32_val)
    assert trip_id_drivers == [4, 3, 2, 9, 8, 7]


@pytest.mark.docker
def test_valkey_online_write_batch_deletes_existing_expired_members(
    repo_config: RepoConfig,
    valkey_online_store: EGValkeyOnlineStore,
):
    (
        feature_view,
        data,
    ) = _create_sorted_feature_view_with_timestamp_as_sortkey(1)

    del data[-5:]

    connection_string = repo_config.online_store.connection_string
    connection_string_split = connection_string.split(":")
    conn_dict = {}
    conn_dict["host"] = connection_string_split[0]
    conn_dict["port"] = connection_string_split[1]

    r = Valkey(**conn_dict)

    entity_key_driver_1 = EntityKeyProto(
        join_keys=["driver_id"],
        entity_values=[ValueProto(int32_val=1)],
    )

    redis_key_bin_driver_1 = _redis_key(
        repo_config.project,
        entity_key_driver_1,
        entity_key_serialization_version=repo_config.entity_key_serialization_version,
    )

    zset_key_driver_1 = valkey_online_store.zset_key_bytes(
        feature_view.name, redis_key_bin_driver_1
    )

    ttl_seconds = 3600
    now_ms = int(time.time() * 1000)

    expired_ts_ms = now_ms - (ttl_seconds) * 1000
    expired_member = f"member:{expired_ts_ms}".encode()
    r.zadd(zset_key_driver_1, {expired_member: expired_ts_ms})

    driver_1_zset_members_before_online_batch = r.zrange(
        zset_key_driver_1, 0, -1, withscores=True
    )
    assert len(driver_1_zset_members_before_online_batch) == 1

    valkey_online_store.online_write_batch(
        config=repo_config,
        table=feature_view,
        data=data,
        progress=None,
    )

    driver_1_zset_members = r.zrange(zset_key_driver_1, 0, -1, withscores=True)
    assert len(driver_1_zset_members) == 5


def test_multiple_sort_keys_not_supported(
    repo_config_without_docker_connection_string: RepoConfig,
    valkey_online_store: EGValkeyOnlineStore,
):
    (
        feature_view,
        data,
    ) = _create_sorted_feature_view_with_multiple_sortkeys()

    with pytest.raises(
        ValueError,
        match=r"Only one sort key is supported for Range query use cases in Valkey, but found 2 sort keys in the",
    ):
        valkey_online_store.online_write_batch(
            config=repo_config_without_docker_connection_string,
            table=feature_view,
            data=data,
            progress=None,
        )


def test_non_numeric_sort_key_not_supported(
    repo_config_without_docker_connection_string: RepoConfig,
    valkey_online_store: EGValkeyOnlineStore,
):
    (
        feature_view,
        data,
    ) = _create_sorted_feature_view_with_non_numeric_sortkey()

    with pytest.raises(
        TypeError, match=r"Unsupported sort key type STRING. Only timestamp"
    ):
        valkey_online_store.online_write_batch(
            config=repo_config_without_docker_connection_string,
            table=feature_view,
            data=data,
            progress=None,
        )


def _create_sorted_feature_view_with_timestamp_as_sortkey(i):
    fv = SortedFeatureView(
        name=f"driver_stats_{i}",
        source=FileSource(
            name="my_file_source",
            path="test.parquet",
            timestamp_field="event_timestamp",
        ),
        entities=[Entity(name="driver_id")],
        ttl=timedelta(minutes=20),
        sort_keys=[
            SortKey(
                name="event_timestamp",
                value_type=ValueType.UNIX_TIMESTAMP,
                default_sort_order=SortOrder.DESC,
            )
        ],
        schema=[
            Field(
                name="driver_id",
                dtype=Int32,
            ),
            Field(name="event_timestamp", dtype=UnixTimestamp),
            Field(
                name="trip_id",
                dtype=Int32,
            ),
            Field(
                name="rating",
                dtype=Float32,
            ),
        ],
    )

    return fv, _make_rows()


def _create_sorted_feature_view_with_multiple_sortkeys(n=10):
    fv = SortedFeatureView(
        name="driver_stats",
        source=FileSource(
            name="my_file_source",
            path="test.parquet",
            timestamp_field="event_timestamp",
        ),
        entities=[Entity(name="driver_id")],
        ttl=timedelta(seconds=100),
        sort_keys=[
            SortKey(
                name="rating",
                value_type=ValueType.FLOAT,
                default_sort_order=SortOrder.DESC,
            ),
            SortKey(
                name="trip_id",
                value_type=ValueType.INT32,
                default_sort_order=SortOrder.DESC,
            ),
        ],
        schema=[
            Field(
                name="driver_id",
                dtype=Int32,
            ),
            Field(name="event_timestamp", dtype=UnixTimestamp),
            Field(
                name="trip_id",
                dtype=Int32,
            ),
            Field(
                name="rating",
                dtype=Float32,
            ),
        ],
    )
    return fv, _make_rows()


def _create_sorted_feature_view_with_non_numeric_sortkey(n=10):
    fv = SortedFeatureView(
        name="driver_stats",
        source=FileSource(
            name="my_file_source",
            path="test.parquet",
            timestamp_field="event_timestamp",
        ),
        entities=[Entity(name="driver_id")],
        ttl=timedelta(seconds=100),
        sort_keys=[
            SortKey(
                name="rating",
                value_type=ValueType.STRING,
                default_sort_order=SortOrder.DESC,
            )
        ],
        schema=[
            Field(
                name="driver_id",
                dtype=Int32,
            ),
            Field(name="event_timestamp", dtype=UnixTimestamp),
            Field(
                name="trip_id",
                dtype=Int32,
            ),
            Field(
                name="rating",
                dtype=String,
            ),
        ],
    )
    return fv, _make_rows()


def _make_rows(n=10):
    """Generate 10 rows split between driver_id 1 (first 5) and 2 (rest),
    with rating = i + 0.5 and an event_timestamp spanning ~15 minutes."""
    now = int(time.time())  # constant for this call
    base_ts = now - 15 * 60
    return [
        (
            EntityKeyProto(
                join_keys=["driver_id"],
                entity_values=[
                    ValueProto(int32_val=1) if i <= 4 else ValueProto(int32_val=2)
                ],
            ),
            {
                "trip_id": ValueProto(int32_val=i),
                "rating": ValueProto(float_val=i + 0.5),
                "event_timestamp": ValueProto(unix_timestamp_val=base_ts + i * 60),
            },
            datetime.fromtimestamp(base_ts + i * 60, tz=timezone.utc),
            None,
        )
        for i in range(n)
    ]


def _make_redis_client(repo_config):
    connection_string = repo_config.online_store.connection_string
    host, port = connection_string.split(":")
    return Valkey(host=host, port=int(port), decode_responses=False)


@pytest.mark.docker
def test_ttl_cleanup_removes_expired_members_and_index(repo_config):
    # Ensure TTL cleanup removes expired members, hashes, and deletes empty ZSETs.
    redis_client = _make_redis_client(repo_config)
    store = EGValkeyOnlineStore()
    zset_key = b"test:ttl_cleanup:zset"
    ttl_seconds = 2
    now_ms = int(time.time() * 1000)
    expired_ts_ms = now_ms - (ttl_seconds + 1) * 1000
    active_ts_ms = now_ms
    expired_member = f"member:{expired_ts_ms}".encode()
    active_member = f"member:{active_ts_ms}".encode()

    redis_client.zadd(
        zset_key, {expired_member: expired_ts_ms, active_member: active_ts_ms}
    )

    cutoff = (int(time.time()) - ttl_seconds) * 1000

    with redis_client.pipeline(transaction=False) as pipe:
        store._run_cleanup_by_event_time(pipe, zset_key, ttl_seconds, cutoff)
        pipe.execute()

    remaining = redis_client.zrange(zset_key, 0, -1)
    assert active_member in remaining
    assert expired_member not in remaining

    time.sleep(3)
    assert not redis_client.exists(zset_key), "ZSET should be deleted when empty"


@pytest.mark.docker
def test_ttl_cleanup_no_expired_members(repo_config):
    # Ensure TTL cleanup is a no-op when there are no expired members.
    redis_client = _make_redis_client(repo_config)
    store = EGValkeyOnlineStore()
    zset_key = b"test:ttl_cleanup:zset"
    ttl_seconds = 5
    now_ms = int(time.time() * 1000)
    active_ts_ms = now_ms
    active_member = f"member:{active_ts_ms}".encode()

    redis_client.zadd(zset_key, {active_member: active_ts_ms})

    cutoff = (int(time.time()) - ttl_seconds) * 1000

    with redis_client.pipeline(transaction=False) as pipe:
        store._run_cleanup_by_event_time(pipe, zset_key, ttl_seconds, cutoff)
        pipe.execute()

    remaining = redis_client.zrange(zset_key, 0, -1)
    assert active_member in remaining


class TestVectorIndexName:
    """Tests for _get_vector_index_name helper function."""

    def test_get_vector_index_name(self):
        """Test index name generation follows expected format."""
        assert (
            _get_vector_index_name("my_project", "item_embeddings")
            == "my_project_item_embeddings_vidx"
        )

    def test_get_vector_index_name_with_special_chars(self):
        """Test index name with underscores in names."""
        assert (
            _get_vector_index_name("prod_project", "user_item_embeddings")
            == "prod_project_user_item_embeddings_vidx"
        )


class TestGetValkeyVectorType:
    """Tests for _get_valkey_vector_type helper function."""

    def test_get_valkey_vector_type_float32(self):
        """Test Float32 array maps to FLOAT32."""
        assert _get_valkey_vector_type(Array(Float32)) == "FLOAT32"

    def test_get_valkey_vector_type_float64(self):
        """Test Float64 array maps to FLOAT64."""
        assert _get_valkey_vector_type(Array(Float64)) == "FLOAT64"

    def test_get_valkey_vector_type_unsupported_defaults_to_float32(self):
        """Test unsupported types default to FLOAT32 with warning."""
        # Int32 array is not a valid vector type, should default to FLOAT32
        assert _get_valkey_vector_type(Array(Int32)) == "FLOAT32"


class TestSerializeVectorToBytes:
    """Tests for _serialize_vector_to_bytes helper function."""

    def test_serialize_vector_float32(self):
        """Test Float32 vector serialization to raw bytes."""
        val = ValueProto(float_list_val=FloatList(val=[0.1, 0.2, 0.3, 0.4]))
        field = Field(
            name="embedding",
            dtype=Array(Float32),
            vector_index=True,
            vector_length=4,
        )

        result = _serialize_vector_to_bytes(val, field)

        expected = np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32).tobytes()
        assert result == expected

    def test_serialize_vector_float64(self):
        """Test Float64 vector serialization to raw bytes."""
        val = ValueProto(double_list_val=DoubleList(val=[0.1, 0.2, 0.3, 0.4]))
        field = Field(
            name="embedding",
            dtype=Array(Float64),
            vector_index=True,
            vector_length=4,
        )

        result = _serialize_vector_to_bytes(val, field)

        expected = np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float64).tobytes()
        assert result == expected

    def test_serialize_vector_dimension_mismatch(self):
        """Test error when vector dimension doesn't match expected length."""
        val = ValueProto(float_list_val=FloatList(val=[0.1, 0.2, 0.3]))
        field = Field(
            name="embedding",
            dtype=Array(Float32),
            vector_index=True,
            vector_length=128,  # Expected 128, but vector has 3 elements
        )

        with pytest.raises(ValueError, match="dimension mismatch"):
            _serialize_vector_to_bytes(val, field)

    def test_serialize_vector_unsupported_type(self):
        """Test error when vector type is not float_list or double_list."""
        val = ValueProto(int32_val=123)  # Not a list type
        field = Field(
            name="embedding",
            dtype=Array(Float32),
            vector_index=True,
            vector_length=4,
        )

        with pytest.raises(ValueError, match="Unsupported vector type"):
            _serialize_vector_to_bytes(val, field)

    def test_serialize_vector_no_length_validation_when_zero(self):
        """Test that vector_length=0 skips dimension validation."""
        val = ValueProto(float_list_val=FloatList(val=[0.1, 0.2, 0.3]))
        field = Field(
            name="embedding",
            dtype=Array(Float32),
            vector_index=True,
            vector_length=0,  # No validation
        )

        # Should not raise
        result = _serialize_vector_to_bytes(val, field)
        assert len(result) == 3 * 4  # 3 floats * 4 bytes each


class TestDeserializeVectorFromBytes:
    """Tests for _deserialize_vector_from_bytes helper function."""

    def test_deserialize_vector_float32(self):
        """Test Float32 vector deserialization from raw bytes."""
        original = np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32)
        raw_bytes = original.tobytes()
        field = Field(
            name="embedding",
            dtype=Array(Float32),
            vector_index=True,
            vector_length=4,
        )

        result = _deserialize_vector_from_bytes(raw_bytes, field)

        assert result.HasField("float_list_val")
        np.testing.assert_array_almost_equal(
            result.float_list_val.val, original, decimal=5
        )

    def test_deserialize_vector_float64(self):
        """Test Float64 vector deserialization from raw bytes."""
        original = np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float64)
        raw_bytes = original.tobytes()
        field = Field(
            name="embedding",
            dtype=Array(Float64),
            vector_index=True,
            vector_length=4,
        )

        result = _deserialize_vector_from_bytes(raw_bytes, field)

        assert result.HasField("double_list_val")
        np.testing.assert_array_almost_equal(
            result.double_list_val.val, original, decimal=10
        )

    def test_roundtrip_float32(self):
        """Test serialize then deserialize preserves Float32 vector values."""
        original_values = [0.123, 0.456, 0.789, 1.0]
        val = ValueProto(float_list_val=FloatList(val=original_values))
        field = Field(
            name="embedding",
            dtype=Array(Float32),
            vector_index=True,
            vector_length=4,
        )

        raw_bytes = _serialize_vector_to_bytes(val, field)
        result = _deserialize_vector_from_bytes(raw_bytes, field)

        np.testing.assert_array_almost_equal(
            result.float_list_val.val, original_values, decimal=5
        )

    def test_roundtrip_float64(self):
        """Test serialize then deserialize preserves Float64 vector values."""
        original_values = [0.123456789, 0.987654321, 0.111111111, 0.999999999]
        val = ValueProto(double_list_val=DoubleList(val=original_values))
        field = Field(
            name="embedding",
            dtype=Array(Float64),
            vector_index=True,
            vector_length=4,
        )

        raw_bytes = _serialize_vector_to_bytes(val, field)
        result = _deserialize_vector_from_bytes(raw_bytes, field)

        np.testing.assert_array_almost_equal(
            result.double_list_val.val, original_values, decimal=10
        )


class TestVectorConfigOptions:
    """Tests for vector-related configuration options."""

    def test_default_vector_config_values(self):
        """Test that vector config has sensible defaults."""
        config = EGValkeyOnlineStoreConfig()

        assert config.vector_index_algorithm == "HNSW"
        assert config.vector_index_hnsw_m == 16
        assert config.vector_index_hnsw_ef_construction == 200
        assert config.vector_index_hnsw_ef_runtime == 10

    def test_vector_config_custom_values(self):
        """Test that vector config can be customized."""
        config = EGValkeyOnlineStoreConfig(
            vector_index_algorithm="FLAT",
            vector_index_hnsw_m=32,
            vector_index_hnsw_ef_construction=400,
            vector_index_hnsw_ef_runtime=20,
        )

        assert config.vector_index_algorithm == "FLAT"
        assert config.vector_index_hnsw_m == 32
        assert config.vector_index_hnsw_ef_construction == 400
        assert config.vector_index_hnsw_ef_runtime == 20


class TestGenerateHsetKeysForFeatures:
    """Tests for _generate_hset_keys_for_features helper method."""

    @pytest.fixture
    def feature_view_with_vector(self):
        """Create a FeatureView with mixed vector and non-vector fields."""
        return FeatureView(
            name="test_fv",
            source=FileSource(
                name="test_source",
                path="test.parquet",
                timestamp_field="event_timestamp",
            ),
            entities=[Entity(name="entity_id")],
            ttl=timedelta(days=1),
            schema=[
                Field(name="entity_id", dtype=Int64),
                Field(name="scalar_feature", dtype=Float32),
                Field(name="string_feature", dtype=String),
                Field(
                    name="embedding",
                    dtype=Array(Float32),
                    vector_index=True,
                    vector_length=4,
                ),
            ],
        )

    @pytest.fixture
    def feature_view_no_vector(self):
        """Create a FeatureView with only non-vector fields."""
        return FeatureView(
            name="test_fv_no_vec",
            source=FileSource(
                name="test_source",
                path="test.parquet",
                timestamp_field="event_timestamp",
            ),
            entities=[Entity(name="entity_id")],
            ttl=timedelta(days=1),
            schema=[
                Field(name="entity_id", dtype=Int64),
                Field(name="scalar_feature", dtype=Float32),
                Field(name="string_feature", dtype=String),
            ],
        )

    def test_vector_field_uses_original_name(self, feature_view_with_vector):
        """Test that vector fields use original name as hset key."""
        store = EGValkeyOnlineStore()

        requested_features, hset_keys, vector_fields = (
            store._generate_hset_keys_for_features(
                feature_view_with_vector, requested_features=["embedding"]
            )
        )

        # Vector field should use original name
        assert "embedding" in hset_keys
        assert "embedding" in vector_fields

    def test_non_vector_field_uses_mmh3_hash(self, feature_view_with_vector):
        """Test that non-vector fields use mmh3 hash as hset key."""
        store = EGValkeyOnlineStore()

        requested_features, hset_keys, vector_fields = (
            store._generate_hset_keys_for_features(
                feature_view_with_vector, requested_features=["scalar_feature"]
            )
        )

        # Non-vector field should use mmh3 hash
        expected_hash = _mmh3(f"{feature_view_with_vector.name}:scalar_feature")
        assert expected_hash in hset_keys
        assert "scalar_feature" not in vector_fields

    def test_timestamp_key_appended(self, feature_view_with_vector):
        """Test that timestamp key is always appended to hset keys."""
        store = EGValkeyOnlineStore()

        requested_features, hset_keys, vector_fields = (
            store._generate_hset_keys_for_features(
                feature_view_with_vector, requested_features=["embedding"]
            )
        )

        ts_key = f"_ts:{feature_view_with_vector.name}"
        assert ts_key in hset_keys
        assert ts_key in requested_features

    def test_mixed_fields_correct_keys(self, feature_view_with_vector):
        """Test that mixed vector and non-vector fields get correct keys."""
        store = EGValkeyOnlineStore()

        requested_features, hset_keys, vector_fields = (
            store._generate_hset_keys_for_features(
                feature_view_with_vector,
                requested_features=["embedding", "scalar_feature", "string_feature"],
            )
        )

        # Vector field uses original name
        assert "embedding" in hset_keys

        # Non-vector fields use mmh3 hash
        scalar_hash = _mmh3(f"{feature_view_with_vector.name}:scalar_feature")
        string_hash = _mmh3(f"{feature_view_with_vector.name}:string_feature")
        assert scalar_hash in hset_keys
        assert string_hash in hset_keys

        # Only embedding should be in vector_fields (now a dict)
        assert set(vector_fields.keys()) == {"embedding"}

    def test_no_requested_features_uses_all(self, feature_view_with_vector):
        """Test that None requested_features returns all feature view features."""
        store = EGValkeyOnlineStore()

        requested_features, hset_keys, vector_fields = (
            store._generate_hset_keys_for_features(
                feature_view_with_vector, requested_features=None
            )
        )

        # Should include all features from the feature view
        # Features are: scalar_feature, string_feature, embedding (excluding entity_id which is join key)
        assert len(requested_features) == 4  # 3 features + timestamp key

    def test_feature_view_without_vectors(self, feature_view_no_vector):
        """Test feature view with no vector fields returns empty vector_fields dict."""
        store = EGValkeyOnlineStore()

        requested_features, hset_keys, vector_fields = (
            store._generate_hset_keys_for_features(
                feature_view_no_vector,
                requested_features=["scalar_feature", "string_feature"],
            )
        )

        # No vector fields (empty dict)
        assert vector_fields == {}

        # All fields should use mmh3 hash
        for key in hset_keys:
            if not isinstance(key, str) or not key.startswith("_ts:"):
                assert isinstance(key, bytes)  # mmh3 returns bytes


class TestVectorFieldValidation:
    """Tests for vector field validation during index creation."""

    def test_vector_field_missing_vector_length_raises_error(
        self, valkey_online_store, repo_config_without_docker_connection_string
    ):
        """Test that vector field without vector_length raises ValueError."""
        from unittest.mock import MagicMock

        from valkey.exceptions import ResponseError

        # Create a FeatureView with vector field but no vector_length
        fv = FeatureView(
            name="test_missing_length",
            source=FileSource(
                name="test_source",
                path="test.parquet",
                timestamp_field="event_timestamp",
            ),
            entities=[Entity(name="item_id")],
            ttl=timedelta(days=1),
            schema=[
                Field(name="item_id", dtype=Int64),
                Field(
                    name="embedding",
                    dtype=Array(Float32),
                    vector_index=True,
                    # vector_length intentionally not set (defaults to 0)
                ),
            ],
        )

        # Get vector fields
        vector_fields = {f.name: f for f in fv.features if f.vector_index}

        # Mock client to avoid actual connection
        mock_client = MagicMock()
        # Simulate index doesn't exist (ResponseError is raised by valkey-py)
        mock_client.ft.return_value.info.side_effect = ResponseError("Unknown index")

        with pytest.raises(ValueError, match="vector_length"):
            valkey_online_store._create_vector_index_if_not_exists(
                mock_client,
                repo_config_without_docker_connection_string,
                fv,
                vector_fields,
            )

    def test_vector_field_with_negative_vector_length_raises_error(
        self, valkey_online_store, repo_config_without_docker_connection_string
    ):
        """Test that vector field with negative vector_length raises ValueError."""
        from unittest.mock import MagicMock

        from valkey.exceptions import ResponseError

        # Create a FeatureView with vector field with negative vector_length
        fv = FeatureView(
            name="test_negative_length",
            source=FileSource(
                name="test_source",
                path="test.parquet",
                timestamp_field="event_timestamp",
            ),
            entities=[Entity(name="item_id")],
            ttl=timedelta(days=1),
            schema=[
                Field(name="item_id", dtype=Int64),
                Field(
                    name="embedding",
                    dtype=Array(Float32),
                    vector_index=True,
                    vector_length=-1,
                ),
            ],
        )

        vector_fields = {f.name: f for f in fv.features if f.vector_index}

        mock_client = MagicMock()
        mock_client.ft.return_value.info.side_effect = ResponseError("Unknown index")

        with pytest.raises(ValueError, match="vector_length"):
            valkey_online_store._create_vector_index_if_not_exists(
                mock_client,
                repo_config_without_docker_connection_string,
                fv,
                vector_fields,
            )


# ============================================================================
# Vector Support Integration Tests (Docker Required)
# ============================================================================


def _create_feature_view_with_vector_field():
    """Create a FeatureView with a vector embedding field."""
    fv = FeatureView(
        name="item_embeddings",
        source=FileSource(
            name="item_source",
            path="test.parquet",
            timestamp_field="event_timestamp",
        ),
        entities=[Entity(name="item_id")],
        ttl=timedelta(days=1),
        schema=[
            Field(name="item_id", dtype=Int64),
            Field(name="item_name", dtype=String),
            Field(
                name="embedding",
                dtype=Array(Float32),
                vector_index=True,
                vector_length=4,
                vector_search_metric="COSINE",
            ),
        ],
    )
    return fv


def _make_vector_rows():
    """Generate rows with vector embeddings."""
    now = datetime.now(tz=timezone.utc)
    return [
        (
            EntityKeyProto(
                join_keys=["item_id"],
                entity_values=[ValueProto(int64_val=1)],
            ),
            {
                "item_name": ValueProto(string_val="item_1"),
                "embedding": ValueProto(
                    float_list_val=FloatList(val=[0.1, 0.2, 0.3, 0.4])
                ),
            },
            now,
            None,
        ),
        (
            EntityKeyProto(
                join_keys=["item_id"],
                entity_values=[ValueProto(int64_val=2)],
            ),
            {
                "item_name": ValueProto(string_val="item_2"),
                "embedding": ValueProto(
                    float_list_val=FloatList(val=[0.5, 0.6, 0.7, 0.8])
                ),
            },
            now,
            None,
        ),
    ]


@pytest.mark.docker
def test_valkey_online_write_batch_with_vector_field(
    repo_config: RepoConfig,
    valkey_online_store: EGValkeyOnlineStore,
):
    """Test writing a FeatureView with vector field stores data correctly."""
    feature_view = _create_feature_view_with_vector_field()
    data = _make_vector_rows()

    # Write data - note: index creation will fail without Search module,
    # but the write itself should work for storage verification
    try:
        valkey_online_store.online_write_batch(
            config=repo_config,
            table=feature_view,
            data=data,
            progress=None,
        )
    except Exception as e:
        # If Search module is not available, index creation will fail
        # This is expected with basic Valkey container
        if "Search" in str(e) or "unknown command" in str(e).lower():
            pytest.skip("Valkey Search module not available in test container")
        raise

    # Verify data was stored
    redis_client = _make_redis_client(repo_config)

    entity_key = EntityKeyProto(
        join_keys=["item_id"],
        entity_values=[ValueProto(int64_val=1)],
    )
    valkey_key_bin = _redis_key(
        repo_config.project,
        entity_key,
        entity_key_serialization_version=repo_config.entity_key_serialization_version,
    )

    stored_data = redis_client.hgetall(valkey_key_bin)

    # Verify vector field is stored with original name (not hashed)
    assert b"embedding" in stored_data

    # Verify non-vector field is stored with mmh3 hash
    item_name_key = _mmh3(f"{feature_view.name}:item_name")
    assert item_name_key in stored_data

    # Verify vector bytes can be deserialized
    embedding_bytes = stored_data[b"embedding"]
    vector = np.frombuffer(embedding_bytes, dtype=np.float32)
    np.testing.assert_array_almost_equal(vector, [0.1, 0.2, 0.3, 0.4], decimal=5)


@pytest.mark.docker
def test_valkey_online_read_with_vector_field(
    repo_config: RepoConfig,
    valkey_online_store: EGValkeyOnlineStore,
):
    """Test reading a FeatureView with vector field deserializes correctly."""
    feature_view = _create_feature_view_with_vector_field()
    data = _make_vector_rows()

    # Write data first
    try:
        valkey_online_store.online_write_batch(
            config=repo_config,
            table=feature_view,
            data=data,
            progress=None,
        )
    except Exception as e:
        if "Search" in str(e) or "unknown command" in str(e).lower():
            pytest.skip("Valkey Search module not available in test container")
        raise

    # Read data back
    entity_keys = [
        EntityKeyProto(
            join_keys=["item_id"],
            entity_values=[ValueProto(int64_val=1)],
        ),
        EntityKeyProto(
            join_keys=["item_id"],
            entity_values=[ValueProto(int64_val=2)],
        ),
    ]

    results = valkey_online_store.online_read(
        config=repo_config,
        table=feature_view,
        entity_keys=entity_keys,
    )

    # Verify results
    assert len(results) == 2

    # Check first entity
    ts1, features1 = results[0]
    assert ts1 is not None
    assert "embedding" in features1
    assert "item_name" in features1

    # Verify vector values
    embedding1 = features1["embedding"]
    assert embedding1.HasField("float_list_val")
    np.testing.assert_array_almost_equal(
        embedding1.float_list_val.val, [0.1, 0.2, 0.3, 0.4], decimal=5
    )

    # Check second entity
    ts2, features2 = results[1]
    embedding2 = features2["embedding"]
    np.testing.assert_array_almost_equal(
        embedding2.float_list_val.val, [0.5, 0.6, 0.7, 0.8], decimal=5
    )


@pytest.mark.docker
def test_valkey_online_read_with_requested_features_vector_only(
    repo_config: RepoConfig,
    valkey_online_store: EGValkeyOnlineStore,
):
    """Test reading only the vector field using requested_features parameter."""
    feature_view = _create_feature_view_with_vector_field()
    data = _make_vector_rows()

    # Write data first
    try:
        valkey_online_store.online_write_batch(
            config=repo_config,
            table=feature_view,
            data=data,
            progress=None,
        )
    except Exception as e:
        if "Search" in str(e) or "unknown command" in str(e).lower():
            pytest.skip("Valkey Search module not available in test container")
        raise

    entity_keys = [
        EntityKeyProto(
            join_keys=["item_id"],
            entity_values=[ValueProto(int64_val=1)],
        ),
    ]

    # Request only the vector field
    results = valkey_online_store.online_read(
        config=repo_config,
        table=feature_view,
        entity_keys=entity_keys,
        requested_features=["embedding"],
    )

    assert len(results) == 1
    ts, features = results[0]

    # Should only have the embedding feature
    assert "embedding" in features
    assert "item_name" not in features

    # Verify vector values
    embedding = features["embedding"]
    assert embedding.HasField("float_list_val")
    np.testing.assert_array_almost_equal(
        embedding.float_list_val.val, [0.1, 0.2, 0.3, 0.4], decimal=5
    )


@pytest.mark.docker
def test_valkey_online_read_with_requested_features_non_vector_only(
    repo_config: RepoConfig,
    valkey_online_store: EGValkeyOnlineStore,
):
    """Test reading only non-vector fields using requested_features parameter."""
    feature_view = _create_feature_view_with_vector_field()
    data = _make_vector_rows()

    # Write data first
    try:
        valkey_online_store.online_write_batch(
            config=repo_config,
            table=feature_view,
            data=data,
            progress=None,
        )
    except Exception as e:
        if "Search" in str(e) or "unknown command" in str(e).lower():
            pytest.skip("Valkey Search module not available in test container")
        raise

    entity_keys = [
        EntityKeyProto(
            join_keys=["item_id"],
            entity_values=[ValueProto(int64_val=1)],
        ),
    ]

    # Request only the non-vector field
    results = valkey_online_store.online_read(
        config=repo_config,
        table=feature_view,
        entity_keys=entity_keys,
        requested_features=["item_name"],
    )

    assert len(results) == 1
    ts, features = results[0]

    # Should only have the item_name feature
    assert "item_name" in features
    assert "embedding" not in features

    # Verify string value
    assert features["item_name"].string_val == "item_1"


@pytest.mark.docker
def test_valkey_online_read_with_requested_features_mixed(
    repo_config: RepoConfig,
    valkey_online_store: EGValkeyOnlineStore,
):
    """Test reading a mix of vector and non-vector fields using requested_features."""
    feature_view = _create_feature_view_with_vector_field()
    data = _make_vector_rows()

    # Write data first
    try:
        valkey_online_store.online_write_batch(
            config=repo_config,
            table=feature_view,
            data=data,
            progress=None,
        )
    except Exception as e:
        if "Search" in str(e) or "unknown command" in str(e).lower():
            pytest.skip("Valkey Search module not available in test container")
        raise

    entity_keys = [
        EntityKeyProto(
            join_keys=["item_id"],
            entity_values=[ValueProto(int64_val=2)],
        ),
    ]

    # Request both vector and non-vector fields
    results = valkey_online_store.online_read(
        config=repo_config,
        table=feature_view,
        entity_keys=entity_keys,
        requested_features=["embedding", "item_name"],
    )

    assert len(results) == 1
    ts, features = results[0]

    # Should have both features
    assert "embedding" in features
    assert "item_name" in features

    # Verify vector values
    embedding = features["embedding"]
    np.testing.assert_array_almost_equal(
        embedding.float_list_val.val, [0.5, 0.6, 0.7, 0.8], decimal=5
    )

    # Verify string value
    assert features["item_name"].string_val == "item_2"


class TestGetVectorFieldForSearch:
    """Tests for _get_vector_field_for_search helper method."""

    @pytest.fixture
    def feature_view_with_vector(self):
        """Create a FeatureView with vector field for testing."""
        return FeatureView(
            name="test_fv",
            source=FileSource(
                name="test_source",
                path="test.parquet",
                timestamp_field="event_timestamp",
            ),
            entities=[Entity(name="item_id", value_type=ValueType.INT64)],
            ttl=timedelta(days=1),
            schema=[
                Field(name="item_id", dtype=Int64),
                Field(name="scalar_feature", dtype=Float32),
                Field(
                    name="embedding",
                    dtype=Array(Float32),
                    vector_index=True,
                    vector_length=4,
                    vector_search_metric="COSINE",
                ),
            ],
        )

    @pytest.fixture
    def feature_view_no_vector(self):
        """Create a FeatureView without vector fields."""
        return FeatureView(
            name="test_fv_no_vector",
            source=FileSource(
                name="test_source",
                path="test.parquet",
                timestamp_field="event_timestamp",
            ),
            entities=[Entity(name="item_id", value_type=ValueType.INT64)],
            ttl=timedelta(days=1),
            schema=[
                Field(name="item_id", dtype=Int64),
                Field(name="scalar_feature", dtype=Float32),
            ],
        )

    @pytest.fixture
    def feature_view_multiple_vectors(self):
        """Create a FeatureView with multiple vector fields."""
        return FeatureView(
            name="test_fv_multi_vector",
            source=FileSource(
                name="test_source",
                path="test.parquet",
                timestamp_field="event_timestamp",
            ),
            entities=[Entity(name="item_id", value_type=ValueType.INT64)],
            ttl=timedelta(days=1),
            schema=[
                Field(name="item_id", dtype=Int64),
                Field(
                    name="embedding1",
                    dtype=Array(Float32),
                    vector_index=True,
                    vector_length=4,
                ),
                Field(
                    name="embedding2",
                    dtype=Array(Float32),
                    vector_index=True,
                    vector_length=8,
                ),
            ],
        )

    def test_returns_vector_field_from_requested_features(
        self, feature_view_with_vector
    ):
        """Test that vector field is returned when in requested_features."""
        store = EGValkeyOnlineStore()
        result = store._get_vector_field_for_search(
            feature_view_with_vector, requested_features=["embedding", "scalar_feature"]
        )
        assert result is not None
        assert result.name == "embedding"

    def test_returns_first_vector_field_when_not_in_requested(
        self, feature_view_with_vector
    ):
        """Test that first vector field is returned when not in requested_features."""
        store = EGValkeyOnlineStore()
        result = store._get_vector_field_for_search(
            feature_view_with_vector, requested_features=["scalar_feature"]
        )
        assert result is not None
        assert result.name == "embedding"

    def test_returns_none_for_no_vector_fields(self, feature_view_no_vector):
        """Test that None is returned when no vector fields exist."""
        store = EGValkeyOnlineStore()
        result = store._get_vector_field_for_search(
            feature_view_no_vector, requested_features=["scalar_feature"]
        )
        assert result is None

    def test_prefers_requested_vector_field(self, feature_view_multiple_vectors):
        """Test that vector field from requested_features is preferred."""
        store = EGValkeyOnlineStore()
        result = store._get_vector_field_for_search(
            feature_view_multiple_vectors, requested_features=["embedding2"]
        )
        assert result is not None
        assert result.name == "embedding2"

    def test_returns_first_when_no_requested_features(
        self, feature_view_multiple_vectors
    ):
        """Test that first vector field is returned when requested_features is None."""
        store = EGValkeyOnlineStore()
        result = store._get_vector_field_for_search(
            feature_view_multiple_vectors, requested_features=None
        )
        assert result is not None
        assert result.name == "embedding1"


class TestSerializeEmbeddingForSearch:
    """Tests for _serialize_embedding_for_search helper method."""

    @pytest.fixture
    def float32_vector_field(self):
        """Create a Float32 vector field."""
        return Field(
            name="embedding",
            dtype=Array(Float32),
            vector_index=True,
            vector_length=4,
        )

    @pytest.fixture
    def float64_vector_field(self):
        """Create a Float64 vector field."""
        return Field(
            name="embedding",
            dtype=Array(Float64),
            vector_index=True,
            vector_length=4,
        )

    def test_serializes_to_float32_bytes(self, float32_vector_field):
        """Test that embedding is serialized to float32 bytes."""
        store = EGValkeyOnlineStore()
        embedding = [0.1, 0.2, 0.3, 0.4]
        result = store._serialize_embedding_for_search(embedding, float32_vector_field)

        # Verify it's bytes
        assert isinstance(result, bytes)

        # Verify length (4 floats * 4 bytes each = 16 bytes)
        assert len(result) == 16

        # Verify values can be deserialized back
        arr = np.frombuffer(result, dtype=np.float32)
        np.testing.assert_array_almost_equal(arr, embedding, decimal=5)

    def test_serializes_to_float64_bytes(self, float64_vector_field):
        """Test that embedding is serialized to float64 bytes for Float64 fields."""
        store = EGValkeyOnlineStore()
        embedding = [0.1, 0.2, 0.3, 0.4]
        result = store._serialize_embedding_for_search(embedding, float64_vector_field)

        # Verify it's bytes
        assert isinstance(result, bytes)

        # Verify length (4 doubles * 8 bytes each = 32 bytes)
        assert len(result) == 32

        # Verify values can be deserialized back
        arr = np.frombuffer(result, dtype=np.float64)
        np.testing.assert_array_almost_equal(arr, embedding, decimal=10)

    def test_raises_error_on_dimension_mismatch(self, float32_vector_field):
        """Test that ValueError is raised when embedding dimension doesn't match field."""
        store = EGValkeyOnlineStore()
        # Field expects 4 dimensions, but we provide 3
        embedding = [0.1, 0.2, 0.3]
        with pytest.raises(ValueError, match="dimension .* does not match"):
            store._serialize_embedding_for_search(embedding, float32_vector_field)


class TestRetrieveOnlineDocumentsV2Validation:
    """Tests for retrieve_online_documents_v2 input validation."""

    @pytest.fixture
    def feature_view_with_vector(self):
        """Create a FeatureView with vector field for testing."""
        return FeatureView(
            name="test_fv",
            source=FileSource(
                name="test_source",
                path="test.parquet",
                timestamp_field="event_timestamp",
            ),
            entities=[Entity(name="item_id", value_type=ValueType.INT64)],
            ttl=timedelta(days=1),
            schema=[
                Field(name="item_id", dtype=Int64),
                Field(
                    name="embedding",
                    dtype=Array(Float32),
                    vector_index=True,
                    vector_length=4,
                    vector_search_metric="COSINE",
                ),
            ],
        )

    @pytest.fixture
    def feature_view_no_vector(self):
        """Create a FeatureView without vector fields."""
        return FeatureView(
            name="test_fv_no_vector",
            source=FileSource(
                name="test_source",
                path="test.parquet",
                timestamp_field="event_timestamp",
            ),
            entities=[Entity(name="item_id", value_type=ValueType.INT64)],
            ttl=timedelta(days=1),
            schema=[
                Field(name="item_id", dtype=Int64),
                Field(name="scalar_feature", dtype=Float32),
            ],
        )

    @pytest.fixture
    def repo_config(self):
        """Create a minimal RepoConfig for testing."""
        return RepoConfig(
            project="test_project",
            provider="local",
            registry="test_registry.db",
            online_store=EGValkeyOnlineStoreConfig(
                type="eg_valkey",
                connection_string="localhost:6379",
            ),
            entity_key_serialization_version=3,
        )

    def test_raises_error_when_embedding_is_none(
        self, repo_config, feature_view_with_vector
    ):
        """Test that ValueError is raised when embedding is None."""
        store = EGValkeyOnlineStore()
        with pytest.raises(ValueError, match="embedding must be provided"):
            store.retrieve_online_documents_v2(
                config=repo_config,
                table=feature_view_with_vector,
                requested_features=["embedding"],
                embedding=None,
                top_k=10,
            )

    def test_raises_error_when_query_string_provided(
        self, repo_config, feature_view_with_vector
    ):
        """Test that NotImplementedError is raised when query_string is provided."""
        store = EGValkeyOnlineStore()
        with pytest.raises(NotImplementedError, match="Keyword search"):
            store.retrieve_online_documents_v2(
                config=repo_config,
                table=feature_view_with_vector,
                requested_features=["embedding"],
                embedding=[0.1, 0.2, 0.3, 0.4],
                top_k=10,
                query_string="test query",
            )

    def test_raises_error_when_no_vector_field(
        self, repo_config, feature_view_no_vector
    ):
        """Test that ValueError is raised when FeatureView has no vector fields."""
        store = EGValkeyOnlineStore()
        with pytest.raises(ValueError, match="No vector field found"):
            store.retrieve_online_documents_v2(
                config=repo_config,
                table=feature_view_no_vector,
                requested_features=["scalar_feature"],
                embedding=[0.1, 0.2, 0.3, 0.4],
                top_k=10,
            )

    def test_raises_error_when_dimension_mismatch(
        self, repo_config, feature_view_with_vector
    ):
        """Test that ValueError is raised when embedding dimension doesn't match field."""
        store = EGValkeyOnlineStore()
        # feature_view_with_vector has vector_length=4, so 3-dim embedding should fail
        with pytest.raises(ValueError, match="Embedding dimension .* does not match"):
            store.retrieve_online_documents_v2(
                config=repo_config,
                table=feature_view_with_vector,
                requested_features=["embedding"],
                embedding=[0.1, 0.2, 0.3],  # Wrong dimension (3 instead of 4)
                top_k=10,
            )

    def test_raises_error_when_index_does_not_exist(
        self, repo_config, feature_view_with_vector
    ):
        """Test that ValueError is raised when vector index doesn't exist."""
        from unittest.mock import MagicMock, patch

        from valkey.exceptions import ResponseError

        store = EGValkeyOnlineStore()

        # Mock the client to simulate "no such index" error
        mock_client = MagicMock()
        mock_client.ft.return_value.search.side_effect = ResponseError("no such index")

        with patch.object(store, "_get_client", return_value=mock_client):
            with pytest.raises(ValueError, match="does not exist.*materialize"):
                store.retrieve_online_documents_v2(
                    config=repo_config,
                    table=feature_view_with_vector,
                    requested_features=["embedding"],
                    embedding=[0.1, 0.2, 0.3, 0.4],
                    top_k=10,
                )
