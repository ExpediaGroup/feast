import base64
import json
import math
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from feast import Entity, FeatureView, RepoConfig
from feast.field import Field
from feast.infra.online_stores._signal_scores import decode_signal_scores
from feast.infra.online_stores.elasticsearch_online_store.elasticsearch import (
    ElasticSearchOnlineStore,
    ElasticSearchOnlineStoreConfig,
    _encode_feature_value,
    _to_value_proto,
)
from feast.protos.feast.types.Value_pb2 import (
    FloatList,
    Int64List,
)
from feast.protos.feast.types.Value_pb2 import (
    Value as ValueProto,
)
from feast.types import Array, Float32, Int64, String
from feast.value_type import ValueType


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


def _make_feature_view(
    name="test_fv",
    vector_fields=None,
    extra_fields=None,
):
    """Helper to build a FeatureView with optional vector fields."""
    from feast import FileSource

    schema = [Field(name="item_id", dtype=Int64)]
    if vector_fields is None:
        vector_fields = [("embedding", 4)]
    for fname, dim in vector_fields:
        schema.append(
            Field(
                name=fname,
                dtype=Array(Float32),
                vector_index=True,
                vector_length=dim,
                vector_search_metric="COSINE",
            )
        )
    for fname, dtype in extra_fields or []:
        schema.append(Field(name=fname, dtype=dtype))

    return FeatureView(
        name=name,
        source=FileSource(
            name="test_source",
            path="test.parquet",
            timestamp_field="event_timestamp",
        ),
        entities=[Entity(name="item_id", value_type=ValueType.INT64)],
        ttl=timedelta(days=1),
        schema=schema,
    )


_repo_config_counter = 0


def _make_repo_config(vector_enabled=True, **overrides):
    """Helper to build a RepoConfig with ES online store."""
    global _repo_config_counter
    _repo_config_counter += 1
    es_config = ElasticSearchOnlineStoreConfig(
        type="elasticsearch",
        host="localhost",
        port=9200,
        vector_enabled=vector_enabled,
        **overrides,
    )
    return RepoConfig(
        project="test_project",
        provider="local",
        registry=f"/tmp/test_registry_{_repo_config_counter}.db",
        online_store=es_config,
        entity_key_serialization_version=3,
    )


class TestRetrieveOnlineDocumentsV3Validation:
    """Tests for retrieve_online_documents_v3 input validation."""

    @pytest.fixture
    def store(self):
        return ElasticSearchOnlineStore()

    @pytest.fixture
    def config(self):
        return _make_repo_config()

    @pytest.fixture
    def fv_single_vector(self):
        return _make_feature_view(
            vector_fields=[("embedding", 4)],
            extra_fields=[("title", String)],
        )

    @pytest.fixture
    def fv_multi_vector(self):
        return _make_feature_view(
            vector_fields=[("title_vec", 4), ("body_vec", 4)],
        )

    @pytest.fixture
    def fv_no_vector(self):
        return _make_feature_view(vector_fields=[])

    def test_empty_embeddings_raises(self, store, config, fv_single_vector):
        with pytest.raises(ValueError, match="at least one embedding"):
            store.retrieve_online_documents_v3(
                config=config,
                table=fv_single_vector,
                requested_features=["title"],
                embeddings={},
                top_k=5,
            )

    def test_vector_not_enabled_raises(self, store, fv_single_vector):
        config = _make_repo_config(vector_enabled=False)
        with pytest.raises(ValueError, match="not enabled"):
            store.retrieve_online_documents_v3(
                config=config,
                table=fv_single_vector,
                requested_features=["title"],
                embeddings={"embedding": [0.1, 0.2, 0.3, 0.4]},
                top_k=5,
            )

    def test_unknown_fusion_strategy_raises(self, store, config, fv_single_vector):
        with pytest.raises(ValueError, match="Unknown fusion_strategy"):
            store.retrieve_online_documents_v3(
                config=config,
                table=fv_single_vector,
                requested_features=["title"],
                embeddings={"embedding": [0.1, 0.2, 0.3, 0.4]},
                top_k=5,
                fusion_strategy="INVALID",
            )

    def test_unknown_embedding_key_raises(self, store, config, fv_single_vector):
        with pytest.raises(ValueError, match="does not match any vector-indexed"):
            store.retrieve_online_documents_v3(
                config=config,
                table=fv_single_vector,
                requested_features=["title"],
                embeddings={"nonexistent_field": [0.1, 0.2, 0.3, 0.4]},
                top_k=5,
            )

    def test_no_vector_fields_raises(self, store, config, fv_no_vector):
        with pytest.raises(ValueError, match="no vector-indexed fields"):
            store.retrieve_online_documents_v3(
                config=config,
                table=fv_no_vector,
                requested_features=["item_id"],
                embeddings={"some_field": [0.1, 0.2, 0.3, 0.4]},
                top_k=5,
            )

    def test_weighted_linear_missing_weights_raises(
        self, store, config, fv_multi_vector
    ):
        with pytest.raises(ValueError, match="missing weights for signals"):
            store.retrieve_online_documents_v3(
                config=config,
                table=fv_multi_vector,
                requested_features=["item_id"],
                embeddings={
                    "title_vec": [0.1, 0.2, 0.3, 0.4],
                    "body_vec": [0.5, 0.6, 0.7, 0.8],
                },
                top_k=5,
                query_string="test",
                fusion_strategy="WEIGHTED_LINEAR",
                signal_weights={"title_vec": 0.5},
            )

    def test_weighted_linear_partial_weights_raises(
        self, store, config, fv_multi_vector
    ):
        """Missing bm25 weight when query_string is present."""
        with pytest.raises(ValueError, match=r"missing weights for signals.*\bbm25\b"):
            store.retrieve_online_documents_v3(
                config=config,
                table=fv_multi_vector,
                requested_features=["item_id"],
                embeddings={
                    "title_vec": [0.1, 0.2, 0.3, 0.4],
                    "body_vec": [0.5, 0.6, 0.7, 0.8],
                },
                top_k=5,
                query_string="test",
                fusion_strategy="WEIGHTED_LINEAR",
                signal_weights={"title_vec": 0.5, "body_vec": 0.3},
            )

    def test_vector_only_nullifies_query_string(self, store, config, fv_single_vector):
        """VECTOR_ONLY should drop query_string before building retrievers."""
        mock_client = MagicMock()
        mock_client.search.return_value = {"hits": {"hits": []}}

        with patch.object(store, "_get_client", return_value=mock_client):
            store.retrieve_online_documents_v3(
                config=config,
                table=fv_single_vector,
                requested_features=["title"],
                embeddings={"embedding": [0.1, 0.2, 0.3, 0.4]},
                top_k=5,
                query_string="should be dropped",
                fusion_strategy="VECTOR_ONLY",
            )

        call_body = mock_client.search.call_args[1]["body"]
        retriever = call_body["retriever"]
        assert "knn" in retriever, "VECTOR_ONLY should produce a knn retriever"
        assert "standard" not in json.dumps(retriever)
        assert "rrf" not in retriever

    def test_empty_query_string_treated_as_none(self, store, config, fv_single_vector):
        """Whitespace-only query_string should not create a BM25 retriever."""
        mock_client = MagicMock()
        mock_client.search.return_value = {"hits": {"hits": []}}

        with patch.object(store, "_get_client", return_value=mock_client):
            store.retrieve_online_documents_v3(
                config=config,
                table=fv_single_vector,
                requested_features=["title"],
                embeddings={"embedding": [0.1, 0.2, 0.3, 0.4]},
                top_k=5,
                query_string="   ",
            )

        call_body = mock_client.search.call_args[1]["body"]
        retriever = call_body["retriever"]
        assert "knn" in retriever
        assert "standard" not in json.dumps(retriever)

    @pytest.mark.parametrize(
        "strategy", ["auto", "Auto", "AUTO", "rrf", "Rrf", "vector_only"]
    )
    def test_strategy_case_insensitive(self, store, config, fv_single_vector, strategy):
        mock_client = MagicMock()
        mock_client.search.return_value = {"hits": {"hits": []}}

        with patch.object(store, "_get_client", return_value=mock_client):
            store.retrieve_online_documents_v3(
                config=config,
                table=fv_single_vector,
                requested_features=["title"],
                embeddings={"embedding": [0.1, 0.2, 0.3, 0.4]},
                top_k=5,
                fusion_strategy=strategy,
            )
        mock_client.search.assert_called_once()

    @pytest.mark.parametrize("flag", [True, False])
    def test_include_signal_scores_accepted_but_ignored(
        self, store, config, fv_single_vector, flag
    ):
        """include_signal_scores is a reserved param; should not raise for True or False."""
        mock_client = MagicMock()
        mock_client.search.return_value = {"hits": {"hits": []}}

        with patch.object(store, "_get_client", return_value=mock_client):
            store.retrieve_online_documents_v3(
                config=config,
                table=fv_single_vector,
                requested_features=["title"],
                embeddings={"embedding": [0.1, 0.2, 0.3, 0.4]},
                top_k=5,
                include_signal_scores=flag,
            )


class TestRetrieveOnlineDocumentsV3QueryBuilding:
    """Tests for the ES query body construction."""

    @pytest.fixture
    def store(self):
        return ElasticSearchOnlineStore()

    @pytest.fixture
    def config(self):
        return _make_repo_config()

    @pytest.fixture
    def fv_single(self):
        return _make_feature_view(
            vector_fields=[("embedding", 4)],
            extra_fields=[("title", String)],
        )

    @pytest.fixture
    def fv_multi(self):
        return _make_feature_view(
            vector_fields=[("title_vec", 4), ("body_vec", 4)],
        )

    def _call_and_capture_body(self, store, config, table, **kwargs):
        mock_client = MagicMock()
        mock_client.search.return_value = {"hits": {"hits": []}}
        with patch.object(store, "_get_client", return_value=mock_client):
            store.retrieve_online_documents_v3(config=config, table=table, **kwargs)
        return mock_client.search.call_args[1]["body"]

    def test_single_vector_uses_knn_retriever(self, store, config, fv_single):
        body = self._call_and_capture_body(
            store,
            config,
            fv_single,
            requested_features=["title"],
            embeddings={"embedding": [0.1, 0.2, 0.3, 0.4]},
            top_k=5,
        )
        retriever = body["retriever"]
        assert "knn" in retriever
        assert retriever["knn"]["field"] == "embedding.vector_value"
        assert retriever["knn"]["query_vector"] == [0.1, 0.2, 0.3, 0.4]
        assert retriever["knn"]["k"] == 5
        assert body["size"] == 5

    def test_single_vector_knn_k_equals_top_k(self, store, config, fv_single):
        body = self._call_and_capture_body(
            store,
            config,
            fv_single,
            requested_features=["title"],
            embeddings={"embedding": [0.1, 0.2, 0.3, 0.4]},
            top_k=10,
        )
        assert body["retriever"]["knn"]["k"] == 10

    def test_multi_vector_uses_rrf_by_default(self, store, config, fv_multi):
        body = self._call_and_capture_body(
            store,
            config,
            fv_multi,
            requested_features=["item_id"],
            embeddings={
                "title_vec": [0.1, 0.2, 0.3, 0.4],
                "body_vec": [0.5, 0.6, 0.7, 0.8],
            },
            top_k=5,
        )
        retriever = body["retriever"]
        assert "rrf" in retriever
        assert len(retriever["rrf"]["retrievers"]) == 2

    def test_multi_vector_rrf_has_rank_constant(self, store, config, fv_multi):
        body = self._call_and_capture_body(
            store,
            config,
            fv_multi,
            requested_features=["item_id"],
            embeddings={
                "title_vec": [0.1, 0.2, 0.3, 0.4],
                "body_vec": [0.5, 0.6, 0.7, 0.8],
            },
            top_k=5,
            rrf_k=42,
        )
        assert body["retriever"]["rrf"]["rank_constant"] == 42

    def test_query_string_adds_bm25_retriever(self, store, config, fv_single):
        body = self._call_and_capture_body(
            store,
            config,
            fv_single,
            requested_features=["title"],
            embeddings={"embedding": [0.1, 0.2, 0.3, 0.4]},
            top_k=5,
            query_string="search term",
        )
        retriever = body["retriever"]
        assert "rrf" in retriever
        retrievers = retriever["rrf"]["retrievers"]
        assert len(retrievers) == 2
        retriever_types = [list(r.keys())[0] for r in retrievers]
        assert "knn" in retriever_types
        assert "standard" in retriever_types

    def test_single_vector_plus_bm25_uses_rrf(self, store, config, fv_single):
        """Single vector + query_string should produce RRF with knn + standard retrievers."""
        body = self._call_and_capture_body(
            store,
            config,
            fv_single,
            requested_features=["title"],
            embeddings={"embedding": [0.1, 0.2, 0.3, 0.4]},
            top_k=5,
            query_string="search term",
            fusion_strategy="RRF",
        )
        retriever = body["retriever"]
        assert "rrf" in retriever
        retrievers = retriever["rrf"]["retrievers"]
        assert len(retrievers) == 2
        types = {list(r.keys())[0] for r in retrievers}
        assert types == {"knn", "standard"}
        for r in retrievers:
            if "knn" in r:
                assert r["knn"]["field"] == "embedding.vector_value"
            if "standard" in r:
                assert r["standard"]["query"]["query_string"]["query"] == "search term"

    def test_weighted_linear_uses_linear_retriever(self, store, config, fv_multi):
        body = self._call_and_capture_body(
            store,
            config,
            fv_multi,
            requested_features=["item_id"],
            embeddings={
                "title_vec": [0.1, 0.2, 0.3, 0.4],
                "body_vec": [0.5, 0.6, 0.7, 0.8],
            },
            top_k=5,
            fusion_strategy="WEIGHTED_LINEAR",
            signal_weights={"title_vec": 0.7, "body_vec": 0.3},
        )
        retriever = body["retriever"]
        assert "linear" in retriever
        weighted = retriever["linear"]["retrievers"]
        assert len(weighted) == 2
        weights = [w["weight"] for w in weighted]
        assert 0.7 in weights
        assert 0.3 in weights

    def test_multi_signal_inner_k_larger_than_top_k(self, store, config, fv_multi):
        body = self._call_and_capture_body(
            store,
            config,
            fv_multi,
            requested_features=["item_id"],
            embeddings={
                "title_vec": [0.1, 0.2, 0.3, 0.4],
                "body_vec": [0.5, 0.6, 0.7, 0.8],
            },
            top_k=5,
        )
        for r in body["retriever"]["rrf"]["retrievers"]:
            if "knn" in r:
                assert r["knn"]["k"] >= 100
                assert r["knn"]["k"] <= 1000

    def test_num_candidates_uses_math_ceil(self, store, config, fv_single):
        """Verify math.ceil is applied by using a multiplier that produces a fraction."""
        object.__setattr__(config.online_store, "knn_num_candidates_multiplier", 1.5)
        body = self._call_and_capture_body(
            store,
            config,
            fv_single,
            requested_features=["title"],
            embeddings={"embedding": [0.1, 0.2, 0.3, 0.4]},
            top_k=3,
        )
        k = body["retriever"]["knn"]["k"]
        num_candidates = body["retriever"]["knn"]["num_candidates"]
        # 3 * 1.5 = 4.5 → ceil → 5, proving ceil is used (floor would give 4)
        assert num_candidates == math.ceil(k * 1.5)
        assert num_candidates == 5
        assert num_candidates != int(k * 1.5)

    def test_rrf_single_signal_executes_as_single(self, store, config, fv_single):
        """RRF with only one signal should still succeed (logged warning, not error)."""
        body = self._call_and_capture_body(
            store,
            config,
            fv_single,
            requested_features=["title"],
            embeddings={"embedding": [0.1, 0.2, 0.3, 0.4]},
            top_k=5,
            fusion_strategy="RRF",
        )
        retriever = body["retriever"]
        assert "knn" in retriever, "Single signal RRF degrades to single retriever"
        assert "rrf" not in retriever

    def test_auto_single_signal_uses_direct_knn(self, store, config, fv_single):
        """AUTO with one vector and no query_string should produce a bare knn retriever."""
        body = self._call_and_capture_body(
            store,
            config,
            fv_single,
            requested_features=["title"],
            embeddings={"embedding": [0.1, 0.2, 0.3, 0.4]},
            top_k=5,
            fusion_strategy="AUTO",
        )
        retriever = body["retriever"]
        assert "knn" in retriever, "Single signal AUTO should use direct knn"
        assert "rrf" not in retriever
        assert "linear" not in retriever

    def test_source_fields_include_metadata(self, store, config, fv_single):
        body = self._call_and_capture_body(
            store,
            config,
            fv_single,
            requested_features=["title"],
            embeddings={"embedding": [0.1, 0.2, 0.3, 0.4]},
            top_k=5,
        )
        source = body["_source"]
        assert "entity_key" in source
        assert "timestamp" in source
        assert "title" in source


class TestRetrieveOnlineDocumentsV3ResponseParsing:
    """Tests for parsing ES response into V3 result tuples."""

    @pytest.fixture
    def store(self):
        return ElasticSearchOnlineStore()

    @pytest.fixture
    def config(self):
        return _make_repo_config()

    @pytest.fixture
    def fv(self):
        return _make_feature_view(
            vector_fields=[("embedding", 4)],
            extra_fields=[("title", String)],
        )

    def _mock_es_response(self, hits):
        return {"hits": {"hits": hits}}

    def _make_hit(self, score, timestamp, features=None):
        from feast.infra.key_encoding_utils import serialize_entity_key
        from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto

        ek = EntityKeyProto(
            join_keys=["item_id"],
            entity_values=[ValueProto(int64_val=1)],
        )
        ek_bytes = serialize_entity_key(ek, entity_key_serialization_version=3)
        ek_b64 = base64.b64encode(ek_bytes).decode("utf-8")
        source = {
            "entity_key": ek_b64,
            "timestamp": timestamp,
        }
        if features:
            source.update(features)
        return {"_source": source, "_score": score}

    def test_single_result_has_final_score(self, store, config, fv):
        hit = self._make_hit(0.95, "2024-01-01T00:00:00")
        mock_client = MagicMock()
        mock_client.search.return_value = self._mock_es_response([hit])

        with patch.object(store, "_get_client", return_value=mock_client):
            results = store.retrieve_online_documents_v3(
                config=config,
                table=fv,
                requested_features=["title"],
                embeddings={"embedding": [0.1, 0.2, 0.3, 0.4]},
                top_k=5,
            )

        assert len(results) == 1
        ts, ek, feat_dict = results[0]
        assert feat_dict["final_score"].float_val == pytest.approx(0.95)

    def test_single_result_has_signal_scores(self, store, config, fv):
        hit = self._make_hit(0.95, "2024-01-01T00:00:00")
        mock_client = MagicMock()
        mock_client.search.return_value = self._mock_es_response([hit])

        with patch.object(store, "_get_client", return_value=mock_client):
            results = store.retrieve_online_documents_v3(
                config=config,
                table=fv,
                requested_features=["title"],
                embeddings={"embedding": [0.1, 0.2, 0.3, 0.4]},
                top_k=5,
            )

        feat_dict = results[0][2]
        scores = decode_signal_scores(feat_dict["signal_scores"])
        assert "vec_embedding" in scores
        assert scores["vec_embedding"] == pytest.approx(0.95)

    def test_signal_scores_is_compact_sorted_json(self, store, config, fv):
        """signal_scores should be compact JSON with sorted keys."""
        hit = self._make_hit(0.95, "2024-01-01T00:00:00")
        mock_client = MagicMock()
        mock_client.search.return_value = self._mock_es_response([hit])

        with patch.object(store, "_get_client", return_value=mock_client):
            results = store.retrieve_online_documents_v3(
                config=config,
                table=fv,
                requested_features=["title"],
                embeddings={"embedding": [0.1, 0.2, 0.3, 0.4]},
                top_k=5,
            )

        raw = results[0][2]["signal_scores"].string_val
        assert " " not in raw
        parsed = json.loads(raw)
        assert list(parsed.keys()) == sorted(parsed.keys())

    def test_multi_signal_signal_scores_are_empty(self, store, config):
        fv = _make_feature_view(
            vector_fields=[("title_vec", 4), ("body_vec", 4)],
        )
        hit = self._make_hit(0.8, "2024-01-01T00:00:00")
        mock_client = MagicMock()
        mock_client.search.return_value = self._mock_es_response([hit])

        with patch.object(store, "_get_client", return_value=mock_client):
            results = store.retrieve_online_documents_v3(
                config=config,
                table=fv,
                requested_features=["item_id"],
                embeddings={
                    "title_vec": [0.1, 0.2, 0.3, 0.4],
                    "body_vec": [0.5, 0.6, 0.7, 0.8],
                },
                top_k=5,
            )

        feat_dict = results[0][2]
        scores = decode_signal_scores(feat_dict["signal_scores"])
        assert scores == {}

    def test_empty_results(self, store, config, fv):
        mock_client = MagicMock()
        mock_client.search.return_value = self._mock_es_response([])

        with patch.object(store, "_get_client", return_value=mock_client):
            results = store.retrieve_online_documents_v3(
                config=config,
                table=fv,
                requested_features=["title"],
                embeddings={"embedding": [0.1, 0.2, 0.3, 0.4]},
                top_k=5,
            )

        assert results == []

    def test_timestamp_parsed(self, store, config, fv):
        hit = self._make_hit(0.9, "2024-06-15T12:30:00")
        mock_client = MagicMock()
        mock_client.search.return_value = self._mock_es_response([hit])

        with patch.object(store, "_get_client", return_value=mock_client):
            results = store.retrieve_online_documents_v3(
                config=config,
                table=fv,
                requested_features=["title"],
                embeddings={"embedding": [0.1, 0.2, 0.3, 0.4]},
                top_k=5,
            )

        ts = results[0][0]
        assert isinstance(ts, datetime)
        assert ts.year == 2024
        assert ts.month == 6

    def test_top_k_limits_results(self, store, config, fv):
        """Verify that at most top_k results are returned even if ES returns more."""
        hits = [self._make_hit(0.9 - i * 0.1, "2024-01-01T00:00:00") for i in range(5)]
        mock_client = MagicMock()
        mock_client.search.return_value = self._mock_es_response(hits)

        with patch.object(store, "_get_client", return_value=mock_client):
            results = store.retrieve_online_documents_v3(
                config=config,
                table=fv,
                requested_features=["title"],
                embeddings={"embedding": [0.1, 0.2, 0.3, 0.4]},
                top_k=3,
            )

        assert len(results) <= 3
        body = mock_client.search.call_args[1]["body"]
        assert body["size"] == 3

    def test_feature_values_included(self, store, config, fv):
        encoded_val = base64.b64encode(
            ValueProto(string_val="hello world").SerializeToString()
        ).decode("utf-8")
        hit = self._make_hit(
            0.9,
            "2024-01-01T00:00:00",
            features={"title": {"feature_value": encoded_val}},
        )
        mock_client = MagicMock()
        mock_client.search.return_value = self._mock_es_response([hit])

        with patch.object(store, "_get_client", return_value=mock_client):
            results = store.retrieve_online_documents_v3(
                config=config,
                table=fv,
                requested_features=["title"],
                embeddings={"embedding": [0.1, 0.2, 0.3, 0.4]},
                top_k=5,
            )

        feat_dict = results[0][2]
        assert "title" in feat_dict
        assert feat_dict["title"].string_val == "hello world"


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

        for index_type in [
            "int8_hnsw",
            "int4_hnsw",
            "bbq_hnsw",
            "hnsw",
            "flat",
            "bbq_flat",
        ]:
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

        # Valid values: (1.0, 10.0) exclusive
        ElasticSearchOnlineStoreConfig(
            vector_index_type="int8_hnsw", rescore_oversample=2.0
        )
        ElasticSearchOnlineStoreConfig(
            vector_index_type="int8_hnsw", rescore_oversample=5.5
        )
        ElasticSearchOnlineStoreConfig(
            vector_index_type="int8_hnsw", rescore_oversample=9.9
        )
        # None disables rescore
        ElasticSearchOnlineStoreConfig(
            vector_index_type="int8_hnsw", rescore_oversample=None
        )

        # Invalid: at or below 1.0
        with pytest.raises(
            ValueError, match="must be in the range \\(1.0, 10.0\\) exclusive"
        ):
            ElasticSearchOnlineStoreConfig(
                vector_index_type="int8_hnsw", rescore_oversample=1.0
            )
        with pytest.raises(
            ValueError, match="must be in the range \\(1.0, 10.0\\) exclusive"
        ):
            ElasticSearchOnlineStoreConfig(
                vector_index_type="int8_hnsw", rescore_oversample=0.5
            )

        # Invalid: at or above 10.0
        with pytest.raises(
            ValueError, match="must be in the range \\(1.0, 10.0\\) exclusive"
        ):
            ElasticSearchOnlineStoreConfig(
                vector_index_type="int8_hnsw", rescore_oversample=10.0
            )
        with pytest.raises(
            ValueError, match="must be in the range \\(1.0, 10.0\\) exclusive"
        ):
            ElasticSearchOnlineStoreConfig(
                vector_index_type="int8_hnsw", rescore_oversample=50.0
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

        # Invalid: vector_index_type is None
        with pytest.raises(ValueError, match="can only be used with quantized"):
            ElasticSearchOnlineStoreConfig(
                vector_index_type=None, rescore_oversample=2.0
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


class TestToValueProto:
    def test_bool_not_treated_as_int(self):
        """bool is a subclass of int in Python; ensure True -> bool_val, not int64_val."""
        result = _to_value_proto(True)
        assert result.bool_val is True
        assert result.int64_val == 0

        result = _to_value_proto(False)
        assert result.bool_val is False

    def test_int(self):
        result = _to_value_proto(42)
        assert result.int64_val == 42
        assert result.bool_val is False

    def test_float(self):
        result = _to_value_proto(3.14)
        assert result.float_val == pytest.approx(3.14)

    def test_string(self):
        result = _to_value_proto("hello")
        assert result.string_val == "hello"

    def test_float_list(self):
        result = _to_value_proto([1.0, 2.0, 3.0])
        assert list(result.float_list_val.val) == pytest.approx([1.0, 2.0, 3.0])

    def test_int_list(self):
        result = _to_value_proto([1, 2, 3])
        assert list(result.int64_list_val.val) == [1, 2, 3]

    def test_mixed_list_raises(self):
        with pytest.raises(ValueError, match="mixed or unsupported"):
            _to_value_proto([1, "two", 3.0])

    def test_passthrough_value_proto(self):
        original = ValueProto(string_val="already a proto")
        result = _to_value_proto(original)
        assert result is original

    def test_unsupported_type_raises(self):
        with pytest.raises(ValueError, match="Unsupported type"):
            _to_value_proto(object())
