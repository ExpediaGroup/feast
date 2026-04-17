import base64

import pytest

from feast.infra.online_stores.elasticsearch_online_store.elasticsearch import (
    _encode_feature_value,
)
from feast.protos.feast.types.Value_pb2 import (
    FloatList,
    Int64List,
)
from feast.protos.feast.types.Value_pb2 import (
    Value as ValueProto,
)


class TestEncodeFeatureValue:
    def test_vector_field_includes_vector_value(self):
        """When is_vector=True and value is a float list, vector_value should be present."""
        value = ValueProto(float_list_val=FloatList(val=[0.1, 0.2, 0.3]))
        result = _encode_feature_value(value, is_vector=True)

        assert "vector_value" in result
        assert result["vector_value"] == pytest.approx([0.1, 0.2, 0.3])

    def test_non_vector_list_excludes_vector_value(self):
        """When is_vector=False and value is a float list, vector_value should NOT be present."""
        value = ValueProto(float_list_val=FloatList(val=[0.1, 0.2, 0.3]))
        result = _encode_feature_value(value, is_vector=False)

        assert "vector_value" not in result

    def test_non_vector_int_list_excludes_vector_value(self):
        """An int64 list with is_vector=False should not produce vector_value."""
        value = ValueProto(int64_list_val=Int64List(val=[1, 2, 3]))
        result = _encode_feature_value(value, is_vector=False)

        assert "vector_value" not in result

    def test_string_value_has_value_text(self):
        """A string ValueProto should produce value_text, not vector_value."""
        value = ValueProto(string_val="hello")
        result = _encode_feature_value(value, is_vector=False)

        assert result["value_text"] == "hello"
        assert "vector_value" not in result

    def test_feature_value_always_present(self):
        """feature_value (base64 binary) should always be present regardless of is_vector."""
        vector_value = ValueProto(float_list_val=FloatList(val=[1.0, 2.0]))
        string_value = ValueProto(string_val="test")
        int_value = ValueProto(int64_val=42)

        for val in [vector_value, string_value, int_value]:
            for is_vector in [True, False]:
                result = _encode_feature_value(val, is_vector=is_vector)
                assert "feature_value" in result
                # Verify it's valid base64 that deserializes back
                decoded = base64.b64decode(result["feature_value"])
                roundtrip = ValueProto()
                roundtrip.ParseFromString(decoded)

    def test_default_is_vector_false(self):
        """Calling without is_vector should default to False (no vector_value)."""
        value = ValueProto(float_list_val=FloatList(val=[0.1, 0.2]))
        result = _encode_feature_value(value)

        assert "vector_value" not in result


class TestElasticSearchOnlineStoreConfig:
    def test_defaults(self):
        """Test default config values."""
        from feast.infra.online_stores.elasticsearch_online_store.elasticsearch import (
            ElasticSearchOnlineStoreConfig,
        )

        config = ElasticSearchOnlineStoreConfig()
        assert config.vector_index_type is None
        assert config.hnsw_m is None
        assert config.hnsw_ef_construction is None
        assert config.rescore_oversample is None
        assert config.use_native_knn is False
        assert config.knn_num_candidates_multiplier is None

    def test_valid_index_type(self):
        """Test valid vector_index_type values."""
        from feast.infra.online_stores.elasticsearch_online_store.elasticsearch import (
            ElasticSearchOnlineStoreConfig,
        )

        for index_type in ["int8_hnsw", "int4_hnsw", "bbq_hnsw", "hnsw", "flat", "bbq_flat"]:
            config = ElasticSearchOnlineStoreConfig(vector_index_type=index_type)
            assert config.vector_index_type == index_type

    def test_invalid_index_type(self):
        """Test invalid vector_index_type raises ValueError."""
        from feast.infra.online_stores.elasticsearch_online_store.elasticsearch import (
            ElasticSearchOnlineStoreConfig,
        )

        with pytest.raises(ValueError, match="vector_index_type must be one of"):
            ElasticSearchOnlineStoreConfig(vector_index_type="invalid_type")

    def test_rescore_range_validation(self):
        """Test rescore_oversample range validation."""
        from feast.infra.online_stores.elasticsearch_online_store.elasticsearch import (
            ElasticSearchOnlineStoreConfig,
        )

        # Valid values
        ElasticSearchOnlineStoreConfig(
            vector_index_type="int8_hnsw", rescore_oversample=2.0
        )
        ElasticSearchOnlineStoreConfig(
            vector_index_type="int8_hnsw", rescore_oversample=0
        )
        ElasticSearchOnlineStoreConfig(
            vector_index_type="int8_hnsw", rescore_oversample=1.0
        )
        # No upper bound: ES source code enforces >= 1.0 only
        ElasticSearchOnlineStoreConfig(
            vector_index_type="int8_hnsw", rescore_oversample=50.0
        )

        # Invalid: between 0 and 1.0
        with pytest.raises(ValueError, match="must be 0 or >= 1.0"):
            ElasticSearchOnlineStoreConfig(
                vector_index_type="int8_hnsw", rescore_oversample=0.5
            )

    def test_rescore_requires_quantized_type(self):
        """Test rescore_oversample only works with quantized types."""
        from feast.infra.online_stores.elasticsearch_online_store.elasticsearch import (
            ElasticSearchOnlineStoreConfig,
        )

        # Valid: quantized type
        ElasticSearchOnlineStoreConfig(
            vector_index_type="int8_hnsw", rescore_oversample=2.0
        )

        # Invalid: non-quantized type
        with pytest.raises(ValueError, match="can only be used with quantized"):
            ElasticSearchOnlineStoreConfig(
                vector_index_type="hnsw", rescore_oversample=2.0
            )

    def test_hnsw_params_require_hnsw_type(self):
        """Test HNSW params only work with HNSW types."""
        from feast.infra.online_stores.elasticsearch_online_store.elasticsearch import (
            ElasticSearchOnlineStoreConfig,
        )

        # Valid: HNSW type
        ElasticSearchOnlineStoreConfig(vector_index_type="int8_hnsw", hnsw_m=32)

        # Invalid: flat type
        with pytest.raises(ValueError, match="only apply to HNSW index types"):
            ElasticSearchOnlineStoreConfig(vector_index_type="int8_flat", hnsw_m=32)

    def test_hnsw_m_range(self):
        """Test hnsw_m range validation."""
        from feast.infra.online_stores.elasticsearch_online_store.elasticsearch import (
            ElasticSearchOnlineStoreConfig,
        )

        # Valid: ES enforces its own upper limits, Feast only rejects < 1
        ElasticSearchOnlineStoreConfig(vector_index_type="int8_hnsw", hnsw_m=1)
        ElasticSearchOnlineStoreConfig(vector_index_type="int8_hnsw", hnsw_m=100)
        ElasticSearchOnlineStoreConfig(vector_index_type="int8_hnsw", hnsw_m=200)

        # Invalid: zero or negative
        with pytest.raises(ValueError, match="must be >= 1"):
            ElasticSearchOnlineStoreConfig(vector_index_type="int8_hnsw", hnsw_m=0)

    def test_knn_multiplier_validation(self):
        """Test knn_num_candidates_multiplier validation."""
        from feast.infra.online_stores.elasticsearch_online_store.elasticsearch import (
            ElasticSearchOnlineStoreConfig,
        )

        # Valid
        ElasticSearchOnlineStoreConfig(knn_num_candidates_multiplier=1.0)
        ElasticSearchOnlineStoreConfig(knn_num_candidates_multiplier=10.0)

        # Invalid: too low
        with pytest.raises(ValueError, match="must be >= 1.0"):
            ElasticSearchOnlineStoreConfig(knn_num_candidates_multiplier=0.5)


class TestCreateIndexWithQuantization:
    def test_index_mapping_with_int8_quantization(self):
        """Test index mapping includes quantization settings."""
        from unittest.mock import MagicMock

        from feast import FeatureView, Field, RepoConfig
        from feast.infra.online_stores.elasticsearch_online_store.elasticsearch import (
            ElasticSearchOnlineStore,
            ElasticSearchOnlineStoreConfig,
        )
        from feast.types import Array, Float32

        config = RepoConfig(
            project="test",
            registry="registry.db",
            provider="local",
            online_store=ElasticSearchOnlineStoreConfig(
                vector_enabled=True,
                similarity="cosine",
                vector_index_type="int8_hnsw",
                hnsw_m=32,
                hnsw_ef_construction=200,
            ),
        )

        fv = MagicMock(spec=FeatureView)
        fv.name = "test_fv"
        fv.schema = [
            Field(
                name="vector",
                dtype=Array(Float32),
                vector_index=True,
                vector_length=128,
                vector_search_metric="cosine",
            )
        ]

        store = ElasticSearchOnlineStore()
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = False
        store._client = mock_client

        store.create_index(config, fv)

        # Verify create was called
        assert mock_client.indices.create.called
        call_args = mock_client.indices.create.call_args
        mapping = call_args.kwargs["mappings"]

        # Check quantization settings in dynamic template
        template = mapping["dynamic_templates"][0]["feature_objects"]["mapping"]
        vector_props = template["properties"]["vector_value"]

        assert vector_props["type"] == "dense_vector"
        assert vector_props["dims"] == 128
        assert "index_options" in vector_props
        assert vector_props["index_options"]["type"] == "int8_hnsw"
        assert vector_props["index_options"]["m"] == 32
        assert vector_props["index_options"]["ef_construction"] == 200

    def test_int4_requires_even_dimensions(self):
        """Test int4 quantization validates even dimensions."""
        from unittest.mock import MagicMock

        from feast import FeatureView, Field, RepoConfig
        from feast.infra.online_stores.elasticsearch_online_store.elasticsearch import (
            ElasticSearchOnlineStore,
            ElasticSearchOnlineStoreConfig,
        )
        from feast.types import Array, Float32

        config = RepoConfig(
            project="test",
            registry="registry.db",
            provider="local",
            online_store=ElasticSearchOnlineStoreConfig(
                vector_enabled=True, vector_index_type="int4_hnsw"
            ),
        )

        fv = MagicMock(spec=FeatureView)
        fv.name = "test_fv"
        fv.schema = [
            Field(
                name="vector",
                dtype=Array(Float32),
                vector_index=True,
                vector_length=127,  # Odd number
            )
        ]

        store = ElasticSearchOnlineStore()
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = False
        store._client = mock_client

        with pytest.raises(ValueError, match="requires even number of dimensions"):
            store.create_index(config, fv)

    def test_bbq_requires_min_dimensions(self):
        """Test bbq quantization validates minimum dimensions."""
        from unittest.mock import MagicMock

        from feast import FeatureView, Field, RepoConfig
        from feast.infra.online_stores.elasticsearch_online_store.elasticsearch import (
            ElasticSearchOnlineStore,
            ElasticSearchOnlineStoreConfig,
        )
        from feast.types import Array, Float32

        config = RepoConfig(
            project="test",
            registry="registry.db",
            provider="local",
            online_store=ElasticSearchOnlineStoreConfig(
                vector_enabled=True, vector_index_type="bbq_hnsw"
            ),
        )

        fv = MagicMock(spec=FeatureView)
        fv.name = "test_fv"
        fv.schema = [
            Field(
                name="vector",
                dtype=Array(Float32),
                vector_index=True,
                vector_length=32,  # Less than 64
            )
        ]

        store = ElasticSearchOnlineStore()
        mock_client = MagicMock()
        mock_client.indices.exists.return_value = False
        store._client = mock_client

        with pytest.raises(ValueError, match="requires >= 64 dimensions"):
            store.create_index(config, fv)
