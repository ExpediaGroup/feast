from __future__ import absolute_import

import base64
import json
import logging
import math
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

from elasticsearch import Elasticsearch, helpers
from pydantic import model_validator

from feast import Entity, FeatureView, RepoConfig
from feast.infra.key_encoding_utils import (
    deserialize_entity_key,
    get_list_val_str,
    serialize_entity_key,
)
from feast.infra.online_stores._signal_scores import encode_signal_scores
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.online_stores.vector_store import VectorStoreConfig
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from feast.utils import (
    _build_retrieve_online_document_record,
    _get_feature_view_vector_field_metadata,
    to_naive_utc,
)

logger = logging.getLogger(__name__)


class ElasticSearchOnlineStoreConfig(FeastConfigBaseModel, VectorStoreConfig):
    """
    Configuration for the ElasticSearch online store.
    NOTE: The class *must* end with the `OnlineStoreConfig` suffix.
    """

    type: str = "elasticsearch"

    host: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    port: Optional[int] = None
    index: Optional[str] = None
    scheme: Optional[str] = "http"
    vector_field_path: Optional[str] = "embedding.vector_value"

    # The number of rows to write in a single batch
    write_batch_size: Optional[int] = 40

    # Quantization / index_options configuration
    vector_index_type: Optional[str] = None
    # One of: "hnsw", "int8_hnsw", "int4_hnsw", "bbq_hnsw",
    #         "flat", "int8_flat", "int4_flat", "bbq_flat"
    # None = use ES default (hnsw for <8.x, int8_hnsw for 9.0+)

    # HNSW tuning parameters (only apply to HNSW index types)
    hnsw_m: Optional[int] = None  # Neighbor connections (ES default: 16)
    hnsw_ef_construction: Optional[int] = (
        None  # Build-time candidates (ES default: 100)
    )

    # Rescore configuration for quantized indices only (int4/int8/bbq)
    rescore_oversample: Optional[float] = (
        None  # Must be (1.0, 10.0) exclusive; None to disable
    )

    # Query method toggle
    use_native_knn: bool = False  # False = script_score (backward compatible)
    # True = native knn query (faster, approximate)

    # KNN query tuning
    knn_num_candidates_multiplier: Optional[float] = (
        None  # Default: 2.0; num_candidates = top_k * multiplier (must be >= 1.0)
    )

    @model_validator(mode="after")
    def validate_quantization_config(self):
        """Validate quantization configuration constraints."""
        # Validate vector_index_type is a known value
        valid_index_types = {
            "hnsw",
            "int8_hnsw",
            "int4_hnsw",
            "bbq_hnsw",
            "flat",
            "int8_flat",
            "int4_flat",
            "bbq_flat",
        }
        if (
            self.vector_index_type is not None
            and self.vector_index_type not in valid_index_types
        ):
            raise ValueError(
                f"vector_index_type must be one of {valid_index_types}, got {self.vector_index_type}"
            )

        # Validate rescore_oversample range and constraints
        # ES requires: (1.0, 10.0) exclusive, per https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/dense-vector
        if self.rescore_oversample is not None:
            if self.rescore_oversample <= 1.0 or self.rescore_oversample >= 10.0:
                raise ValueError(
                    f"rescore_oversample must be in the range (1.0, 10.0) exclusive, "
                    f"got {self.rescore_oversample}"
                )

            # Validate rescore_oversample only applies to quantized indices
            quantized_types = {
                "int8_hnsw",
                "int4_hnsw",
                "bbq_hnsw",
                "int8_flat",
                "int4_flat",
                "bbq_flat",
            }
            if (
                self.vector_index_type is None
                or self.vector_index_type not in quantized_types
            ):
                raise ValueError(
                    f"rescore_oversample can only be used with quantized index types {quantized_types}, "
                    f"got vector_index_type={self.vector_index_type}"
                )

        # Validate HNSW parameters only apply to HNSW index types
        hnsw_types = {"hnsw", "int8_hnsw", "int4_hnsw", "bbq_hnsw"}
        if (self.hnsw_m is not None or self.hnsw_ef_construction is not None) and (
            self.vector_index_type is not None
            and self.vector_index_type not in hnsw_types
        ):
            raise ValueError(
                f"hnsw_m and hnsw_ef_construction only apply to HNSW index types {hnsw_types}, "
                f"got vector_index_type='{self.vector_index_type}'"
            )

        # Validate HNSW parameter ranges (basic sanity only; ES enforces its own limits)
        if self.hnsw_m is not None and self.hnsw_m < 1:
            raise ValueError(f"hnsw_m must be >= 1, got {self.hnsw_m}")

        if self.hnsw_ef_construction is not None and self.hnsw_ef_construction < 1:
            raise ValueError(
                f"hnsw_ef_construction must be >= 1, got {self.hnsw_ef_construction}"
            )

        # Validate knn_num_candidates_multiplier range (must be >= 1.0)
        if self.knn_num_candidates_multiplier is not None:
            if self.knn_num_candidates_multiplier < 1.0:
                raise ValueError(
                    f"knn_num_candidates_multiplier must be >= 1.0, got {self.knn_num_candidates_multiplier}"
                )

        return self


class ElasticSearchOnlineStore(OnlineStore):
    _client: Optional[Elasticsearch] = None

    def _get_client(self, config: RepoConfig) -> Elasticsearch:
        online_store_config = config.online_store
        assert isinstance(online_store_config, ElasticSearchOnlineStoreConfig)

        user = online_store_config.user if online_store_config.user is not None else ""
        password = (
            online_store_config.password
            if online_store_config.password is not None
            else ""
        )

        if self._client:
            return self._client
        else:
            self._client = Elasticsearch(
                hosts=[
                    {
                        "host": online_store_config.host or "localhost",
                        "port": online_store_config.port or 9200,
                        "scheme": online_store_config.scheme or "http",
                    }
                ],
                basic_auth=(user, password),
            )
            return self._client

    def _bulk_batch_actions(self, table: FeatureView, batch: List[Dict[str, Any]]):
        for row in batch:
            yield {
                "_index": table.name,
                "_id": f"{row['entity_key']}_{row['timestamp']}",
                "_source": row,
            }

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        vector_field = _get_feature_view_vector_field_metadata(table)
        vector_field_name = vector_field.name if vector_field else None
        insert_values = []
        grouped_docs: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "features": {},
                "vector_value": None,
                "timestamp": None,
                "created_ts": None,
            }
        )
        for entity_key, values, timestamp, created_ts in data:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            encoded_entity_key = base64.b64encode(entity_key_bin).decode("utf-8")
            timestamp = to_naive_utc(timestamp)
            if created_ts is not None:
                created_ts = to_naive_utc(created_ts)

            doc_key = f"{encoded_entity_key}_{timestamp}"

            for feature_name, value in values.items():
                doc = _encode_feature_value(
                    value, is_vector=(feature_name == vector_field_name)
                )
                grouped_docs[doc_key]["features"][feature_name] = doc
                grouped_docs[doc_key]["timestamp"] = timestamp
                grouped_docs[doc_key]["created_ts"] = created_ts
                grouped_docs[doc_key]["entity_key"] = encoded_entity_key

        insert_values = [
            {
                "entity_key": document["entity_key"],
                "timestamp": document["timestamp"],
                "created_ts": document["created_ts"],
                **(document["features"] or {}),
            }
            for document in grouped_docs.values()
        ]

        batch_size = config.online_store.write_batch_size
        for i in range(0, len(insert_values), batch_size):
            batch = insert_values[i : i + batch_size]
            actions = self._bulk_batch_actions(table, batch)
            helpers.bulk(self._get_client(config), actions, refresh="wait_for")
            if progress:
                progress(len(batch))

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        encoded_entity_keys = [
            base64.b64encode(
                serialize_entity_key(ek, config.entity_key_serialization_version)
            ).decode("utf-8")
            for ek in entity_keys
        ]

        # Determine which fields to retrieve
        includes = ["timestamp", "created_ts", "entity_key"]
        if requested_features:
            includes.extend(requested_features)
        else:
            includes.append("*")

        body = {
            "size": len(encoded_entity_keys),
            "_source": {"includes": includes, "excludes": ["*.vector_value"]},
            "query": {
                "bool": {"filter": [{"terms": {"entity_key": encoded_entity_keys}}]}
            },
        }

        response = self._get_client(config).search(index=table.name, body=body)

        # Build a lookup dict keyed by entity_key to preserve input order
        entity_key_to_result: Dict[
            str, Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]
        ] = {}

        for hit in response["hits"]["hits"]:
            source = hit["_source"]
            entity_key_val = source.get("entity_key")
            timestamp = source.get("timestamp")
            timestamp = datetime.fromisoformat(timestamp)

            features: Dict[str, ValueProto] = {}

            fields_to_extract = (
                requested_features if requested_features else source.keys()
            )

            for feature_name in fields_to_extract:
                if feature_name in ("timestamp", "created_ts", "entity_key"):
                    continue
                feature_obj = source.get(feature_name)
                if not feature_obj or "feature_value" not in feature_obj:
                    continue
                try:
                    features[feature_name] = _to_value_proto(feature_obj)
                except Exception as e:
                    raise ValueError(
                        f"Failed to parse feature '{feature_name}' from hit: {e}"
                    )

            entity_key_to_result[entity_key_val] = (
                timestamp,
                features if features else None,
            )

        # Return results in the same order as input entity_keys
        results: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        for encoded_key in encoded_entity_keys:
            results.append(entity_key_to_result.get(encoded_key, (None, None)))

        return results

    def create_index(self, config: RepoConfig, table: FeatureView):
        """
        Create an index in ElasticSearch for the given table.
        TODO: This method can be exposed to users to customize the indexing functionality.
        Args:
            config: Feast repo configuration object.
            table: FeatureView table for which the index needs to be created.
        """
        vector_field_length = (
            getattr(_get_feature_view_vector_field_metadata(table), "vector_length", 0)
            or 512
        )

        # Validate vector_field_length is positive
        if vector_field_length <= 0:
            raise ValueError(
                f"vector_field_length must be > 0, got {vector_field_length} for table '{table.name}'"
            )

        vector_mapping = _build_vector_mapping(config, vector_field_length, table.name)

        index_mapping = {
            "dynamic_templates": [
                {
                    "feature_objects": {
                        "match_mapping_type": "object",
                        "match": "*",
                        "mapping": {
                            "type": "object",
                            "properties": {
                                "feature_value": {"type": "binary"},
                                "value_text": {"type": "text"},
                                "vector_value": vector_mapping,
                            },
                        },
                    }
                }
            ],
            "properties": {
                "entity_key": {"type": "keyword"},
                "timestamp": {"type": "date"},
                "created_ts": {"type": "date"},
            },
        }

        client = self._get_client(config)
        if not client.indices.exists(index=table.name):
            client.indices.create(index=table.name, mappings=index_mapping)
        else:
            logger.info(f"Index '{table.name}' already exists; skipping creation. ")

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        client = self._get_client(config)

        # Cache existing indices to reduce API calls
        all_table_names = [t.name for t in tables_to_delete] + [
            t.name for t in tables_to_keep
        ]
        existing_indices: Set[str] = set()
        for table_name in all_table_names:
            if client.indices.exists(index=table_name):
                existing_indices.add(table_name)

        # Delete data from indices that should be removed
        for table in tables_to_delete:
            if table.name in existing_indices:
                client.delete_by_query(
                    index=table.name, body={"query": {"match_all": {}}}
                )

        # Create indices for tables that should be kept
        for table in tables_to_keep:
            if table.name not in existing_indices:
                self.create_index(config, table)

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        project = config.project
        client = self._get_client(config)
        try:
            # Cache existing indices to reduce API calls
            existing_indices: Set[str] = set()
            for table in tables:
                if client.indices.exists(index=table.name):
                    existing_indices.add(table.name)

            # Delete all existing indices
            for table_name in existing_indices:
                client.indices.delete(index=table_name)
        except Exception as e:
            logging.exception(f"Error deleting index in project {project}: {e}")
            raise

    def retrieve_online_documents(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: List[str],
        embedding: List[float],
        top_k: int,
        *args,
        **kwargs,
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[ValueProto],
            Optional[ValueProto],
            Optional[ValueProto],
        ]
    ]:
        result: List[
            Tuple[
                Optional[datetime],
                Optional[EntityKeyProto],
                Optional[ValueProto],
                Optional[ValueProto],
                Optional[ValueProto],
            ]
        ] = []
        vector_field = _get_feature_view_vector_field_metadata(table)
        vector_field_path = (
            f"{vector_field.name}.vector_value"
            if vector_field
            else config.online_store.vector_field_path or "embedding.vector_value"
        )

        # Build query based on use_native_knn config
        body: Dict[str, Any] = {"size": top_k, "_source": True}

        if config.online_store.use_native_knn:
            # Native knn query (fast, approximate)
            # Uses the similarity metric configured in the index mapping
            multiplier = config.online_store.knn_num_candidates_multiplier or 2.0
            num_candidates: int = max(top_k, math.ceil(top_k * multiplier))

            knn_query: Dict[str, Any] = {
                "field": vector_field_path,
                "query_vector": embedding,
                "k": top_k,
                "num_candidates": num_candidates,
            }

            if config.online_store.rescore_oversample is not None:
                knn_query["rescore_vector"] = {
                    "oversample": config.online_store.rescore_oversample
                }

            body["knn"] = knn_query
        else:
            # Legacy script_score query (slow, exact, backward compatible)
            body["query"] = {
                "script_score": {
                    "query": {
                        "bool": {"filter": [{"exists": {"field": vector_field_path}}]}
                    },
                    "script": {
                        "source": f"cosineSimilarity(params.query_vector, '{vector_field_path}') + 1.0",
                        "params": {"query_vector": embedding},
                    },
                }
            }
        response = self._get_client(config).search(index=table.name, body=body)
        rows = response["hits"]["hits"][0:top_k]
        for row in rows:
            entity_key = row["_source"]["entity_key"]
            source = row["_source"]
            distance = row["_score"]

            timestamp_str = source.get("timestamp")
            timestamp = datetime.fromisoformat(timestamp_str)

            for feature_name in requested_features:
                feature_data = source.get(feature_name, {})
                feature_value = feature_data.get("feature_value")
                vector_value = feature_data.get("vector_value")
                result.append(
                    _build_retrieve_online_document_record(
                        base64.b64decode(entity_key),
                        base64.b64decode(feature_value),
                        str(vector_value),
                        distance,
                        timestamp,
                        config.entity_key_serialization_version,
                    )
                )
        return result

    def retrieve_online_documents_v2(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: List[str],
        embedding: Optional[List[float]],
        top_k: int,
        distance_metric: Optional[str] = None,
        query_string: Optional[str] = None,
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[Dict[str, ValueProto]],
        ]
    ]:
        """
        Retrieve documents using vector similarity or keyword search from Elasticsearch.
        """
        result: List[
            Tuple[
                Optional[datetime],
                Optional[EntityKeyProto],
                Optional[Dict[str, ValueProto]],
            ]
        ] = []
        if embedding is None and query_string is None:
            raise ValueError("Either embedding or query_string must be provided")

        if embedding is not None and not config.online_store.vector_enabled:
            raise ValueError("Vector search is not enabled in the online store config")

        es_index = table.name
        body: Dict[str, Any] = {
            "size": top_k,
        }
        composite_key_name = _get_composite_key_name(table)

        source_fields = requested_features.copy()
        source_fields += ["entity_key", "timestamp"]
        source_fields += composite_key_name
        body["_source"] = source_fields

        if embedding:
            vector_field = _get_feature_view_vector_field_metadata(table)
            vector_field_path = (
                f"{vector_field.name}.vector_value"
                if vector_field
                else config.online_store.vector_field_path or "embedding.vector_value"
            )
            similarity = (
                distance_metric
                or (
                    vector_field.vector_search_metric
                    if vector_field and vector_field.vector_search_metric
                    else None
                )
                or config.online_store.similarity
            ).lower()

            # Determine query method: native knn or script_score
            use_native_knn = config.online_store.use_native_knn

            if use_native_knn:
                # Native knn query (fast, approximate)
                # Uses the similarity metric configured in the index mapping
                # Validate that the requested similarity is supported
                if similarity not in (
                    "cosine",
                    "dot_product",
                    "l2",
                    "l2_norm",
                    "euclidean",
                ):
                    raise ValueError(
                        f"Unsupported similarity for native knn: {similarity}"
                    )

                # Calculate num_candidates for approximate nearest neighbor search
                multiplier = config.online_store.knn_num_candidates_multiplier or 2.0
                num_candidates: int = max(top_k, math.ceil(top_k * multiplier))

                knn_clause: Dict[str, Any] = {
                    "field": vector_field_path,
                    "query_vector": embedding,
                    "k": top_k,
                    "num_candidates": num_candidates,
                }

                if config.online_store.rescore_oversample is not None:
                    knn_clause["rescore_vector"] = {
                        "oversample": config.online_store.rescore_oversample
                    }
            else:
                # Legacy script_score query (slow, exact, backward compatible)
                if similarity == "cosine":
                    script = f"cosineSimilarity(params.query_vector, '{vector_field_path}') + 1.0"
                elif similarity == "dot_product":
                    script = f"dotProduct(params.query_vector, '{vector_field_path}')"
                elif similarity in ("l2", "l2_norm", "euclidean"):
                    script = (
                        f"1 / (1 + l2norm(params.query_vector, '{vector_field_path}'))"
                    )
                else:
                    raise ValueError(
                        f"Unsupported similarity/distance_metric: {similarity}"
                    )

        # Build query based on search type and query method
        # Hybrid search (embedding + keyword)
        if embedding and query_string:
            if use_native_knn:
                # Native knn with query filter
                body["knn"] = knn_clause
                body["query"] = {"query_string": {"query": f'"{query_string}"'}}
            else:
                # Legacy script_score with keyword filter
                body["query"] = {
                    "script_score": {
                        "query": {
                            "bool": {
                                "must": [
                                    {"query_string": {"query": f'"{query_string}"'}},
                                    {"exists": {"field": vector_field_path}},
                                ]
                            }
                        },
                        "script": {
                            "source": script,
                            "params": {"query_vector": embedding},
                        },
                    }
                }
        # Vector search only
        elif embedding:
            if use_native_knn:
                # Native knn query
                body["knn"] = knn_clause
            else:
                # Legacy script_score
                body["query"] = {
                    "script_score": {
                        "query": {
                            "bool": {
                                "filter": [{"exists": {"field": vector_field_path}}]
                            }
                        },
                        "script": {
                            "source": script,
                            "params": {"query_vector": embedding},
                        },
                    }
                }
        # Keyword search only
        elif query_string:
            body["query"] = {"query_string": {"query": f'"{query_string}"'}}

        response = self._get_client(config).search(index=es_index, body=body)

        rows = response["hits"]["hits"][0:top_k]
        for row in rows:
            entity_key = row["_source"]["entity_key"]
            entity_key_proto = deserialize_entity_key(
                base64.b64decode(entity_key),
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            timestamp = row["_source"]["timestamp"]
            timestamp = datetime.fromisoformat(timestamp)

            # Create feature dict with all requested features
            feature_dict = {"distance": _to_value_proto(float(row["_score"]))}
            if query_string is not None:
                feature_dict["text_rank"] = _to_value_proto(float(row["_score"]))
            join_key_values = _extract_join_keys(entity_key_proto)
            feature_dict.update(join_key_values)

            for feature in requested_features:
                if feature in ("distance", "text_rank"):
                    continue
                value = row["_source"].get(feature, None)
                if value is not None:
                    feature_dict[feature] = _to_value_proto(value)

            result.append((timestamp, entity_key_proto, feature_dict))
        return result

    def retrieve_online_documents_v3(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: List[str],
        embeddings: Dict[str, List[float]],
        top_k: int,
        query_string: Optional[str] = None,
        fusion_strategy: str = "AUTO",
        signal_weights: Optional[Dict[str, float]] = None,
        rrf_k: int = 60,
        distance_metric: Optional[str] = None,
        include_signal_scores: bool = False,
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[Dict[str, ValueProto]],
        ]
    ]:
        """
        V3 document retrieval on Elasticsearch backend.

        Uses the ES retriever API (ES 8.14+) for all query types: single-signal
        kNN, multi-signal RRF, and weighted linear fusion.

        Reserved output fields (always present in each result's feature_dict):
        - ``final_score``: ES _score (higher = better). For single-signal this
          is the raw kNN score; for fusion it is the rank-based composite score.
        - ``signal_scores``: JSON-encoded Dict[str, float] with per-signal
          scores when available, empty dict for fused results (ES does not
          expose per-retriever scores after fusion).

        Reserved parameters (accepted but currently unused):
        - ``distance_metric``: V3-ES always uses the metric configured in the
          index mapping; this param is reserved for future per-query override.
        - ``include_signal_scores``: No-op today. ``signal_scores`` is always
          populated best-effort (see Reserved output fields). Reserved for a
          future ES-explain path that will expose per-signal scores after
          fusion at extra latency cost.
        """
        del distance_metric
        del include_signal_scores

        valid_strategies = {"AUTO", "RRF", "WEIGHTED_LINEAR", "VECTOR_ONLY"}
        effective_strategy = fusion_strategy.upper()
        if effective_strategy not in valid_strategies:
            raise ValueError(
                f"Unknown fusion_strategy '{fusion_strategy}'. "
                f"Valid options: {sorted(valid_strategies)}"
            )

        if not embeddings:
            raise ValueError(
                "V3 requires at least one embedding. "
                "Pass embeddings={field_name: vector}."
            )

        if not config.online_store.vector_enabled:
            raise ValueError("Vector search is not enabled in the online store config.")

        if effective_strategy == "VECTOR_ONLY":
            query_string = None

        # Normalize empty/whitespace query_string to None
        if query_string is not None and not query_string.strip():
            query_string = None

        # Validate embedding keys against FeatureView schema
        vector_fields = {f.name: f for f in table.features if f.vector_index}
        for key in embeddings:
            if key not in vector_fields:
                available = sorted(vector_fields.keys())
                if not available:
                    raise ValueError(
                        f"FeatureView '{table.name}' has no vector-indexed fields. "
                        f"Cannot perform vector search."
                    )
                raise ValueError(
                    f"Embedding key '{key}' does not match any vector-indexed field "
                    f"in FeatureView '{table.name}'. "
                    f"Available vector fields: {available}"
                )

        # Build retrievers: one kNN per embedding, optional BM25 for query_string
        retrievers_with_names: List[Tuple[str, Dict[str, Any]]] = []
        for field_name, vec in embeddings.items():
            knn_retriever: Dict[str, Any] = {
                "knn": {
                    "field": f"{field_name}.vector_value",
                    "query_vector": vec,
                }
            }
            retrievers_with_names.append((field_name, knn_retriever))

        has_text_signal = query_string is not None
        if has_text_signal:
            text_retriever: Dict[str, Any] = {
                "standard": {"query": {"query_string": {"query": query_string}}}
            }
            retrievers_with_names.append(("bm25", text_retriever))

        is_single_signal = len(retrievers_with_names) == 1

        if is_single_signal and effective_strategy in ("RRF", "WEIGHTED_LINEAR"):
            logger.warning(
                "Only one signal present — fusion_strategy '%s' has no effect. "
                "The query will execute as a single-signal retrieval.",
                effective_strategy,
            )

        # Set inner k based on signal count
        multiplier = (
            getattr(config.online_store, "knn_num_candidates_multiplier", 2.0) or 2.0
        )
        if is_single_signal:
            inner_k = top_k
        else:
            inner_k = min(max(top_k * 10, 100), 1000)
        num_candidates = max(inner_k, math.ceil(inner_k * multiplier))

        for _, retriever in retrievers_with_names:
            if "knn" in retriever:
                retriever["knn"]["k"] = inner_k
                retriever["knn"]["num_candidates"] = num_candidates

        # Resolve execution mode
        if is_single_signal:
            execution_mode = "single"
        elif effective_strategy == "WEIGHTED_LINEAR":
            execution_mode = "linear"
        else:
            execution_mode = "rrf"

        # Validate WEIGHTED_LINEAR signal coverage
        if execution_mode == "linear":
            expected_signals = {name for name, _ in retrievers_with_names}
            provided = set(signal_weights.keys()) if signal_weights else set()
            missing = expected_signals - provided
            if missing:
                raise ValueError(
                    f"WEIGHTED_LINEAR fusion missing weights for signals: "
                    f"{sorted(missing)}. Provide a weight for each signal: "
                    f"embedding field names and/or 'bm25'."
                )

        # Compose query body
        retrievers = [r for _, r in retrievers_with_names]
        composite_key_name = _get_composite_key_name(table)
        source_fields = requested_features.copy()
        source_fields += ["entity_key", "timestamp"]
        source_fields += composite_key_name

        if execution_mode == "single":
            top_retriever = retrievers[0]
        elif execution_mode == "rrf":
            top_retriever = {"rrf": {"retrievers": retrievers, "rank_constant": rrf_k}}
        else:
            assert signal_weights is not None
            weighted = []
            for signal_name, retriever in retrievers_with_names:
                weight = signal_weights[signal_name]
                weighted.append({"retriever": retriever, "weight": weight})
            top_retriever = {"linear": {"retrievers": weighted}}

        body: Dict[str, Any] = {
            "retriever": top_retriever,
            "size": top_k,
            "_source": source_fields,
        }

        response = self._get_client(config).search(index=table.name, body=body)

        # Parse results
        result: List[
            Tuple[
                Optional[datetime],
                Optional[EntityKeyProto],
                Optional[Dict[str, ValueProto]],
            ]
        ] = []

        rows = response["hits"]["hits"][:top_k]
        for row in rows:
            entity_key = row["_source"]["entity_key"]
            entity_key_proto = deserialize_entity_key(
                base64.b64decode(entity_key),
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            timestamp = datetime.fromisoformat(row["_source"]["timestamp"])

            feature_dict: Dict[str, ValueProto] = {}
            feature_dict["final_score"] = _to_value_proto(float(row["_score"]))

            signal_scores: Dict[str, float] = {}
            if is_single_signal:
                embed_key = next(iter(embeddings.keys()))
                signal_scores[f"vec_{embed_key}"] = float(row["_score"])

            feature_dict["signal_scores"] = encode_signal_scores(signal_scores)

            join_key_values = _extract_join_keys(entity_key_proto)
            feature_dict.update(join_key_values)

            for feature in requested_features:
                if feature in ("final_score", "signal_scores"):
                    continue
                value = row["_source"].get(feature, None)
                if value is not None:
                    feature_dict[feature] = _to_value_proto(value)

            result.append((timestamp, entity_key_proto, feature_dict))

        return result


def _build_vector_mapping(
    config: RepoConfig, vector_field_length: int, table_name: str
) -> Dict[str, Any]:
    """
    Build the dense_vector mapping for an Elasticsearch index, including
    quantization index_options when configured.
    """
    # Validate dimension-based quantization constraints
    if config.online_store.vector_index_type:
        index_type = config.online_store.vector_index_type
        if "int4" in index_type and vector_field_length % 2 != 0:
            raise ValueError(
                f"int4 quantization ('{index_type}') requires even number of dimensions, "
                f"got {vector_field_length} for table '{table_name}'. "
                f"See https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/dense-vector"
            )
        if "bbq" in index_type and vector_field_length < 64:
            raise ValueError(
                f"bbq quantization ('{index_type}') requires >= 64 dimensions, "
                f"got {vector_field_length} for table '{table_name}'. "
                f"See https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/dense-vector"
            )

    vector_mapping: Dict[str, Any] = {
        "type": "dense_vector",
        "dims": vector_field_length,
        "index": True,
        "similarity": config.online_store.similarity,
    }

    if config.online_store.vector_index_type:
        index_options: Dict[str, Any] = {"type": config.online_store.vector_index_type}
        if config.online_store.hnsw_m is not None:
            index_options["m"] = config.online_store.hnsw_m
        if config.online_store.hnsw_ef_construction is not None:
            index_options["ef_construction"] = config.online_store.hnsw_ef_construction
        vector_mapping["index_options"] = index_options

    return vector_mapping


def _to_value_proto(value: Any) -> ValueProto:
    """
    Convert a value to a ValueProto object.
    """
    val_proto = ValueProto()
    if isinstance(value, ValueProto):
        return value
    # Check bool before int/float since bool is a subclass of int in Python
    if isinstance(value, bool):
        val_proto.bool_val = value
    elif isinstance(value, float):
        val_proto.float_val = value
    elif isinstance(value, int):
        val_proto.int64_val = value
    elif isinstance(value, str):
        val_proto.string_val = value
    elif isinstance(value, list):
        if not value:
            val_proto.float_list_val.val.extend(value)
        elif all(isinstance(v, float) for v in value):
            val_proto.float_list_val.val.extend(value)
        elif all(isinstance(v, int) for v in value):
            val_proto.int64_list_val.val.extend(value)
        else:
            raise ValueError(f"List contains mixed or unsupported types: {value}")
    elif isinstance(value, dict):
        if "feature_value" in value:
            try:
                raw_bytes = base64.b64decode(value["feature_value"])
                val_proto.ParseFromString(raw_bytes)
            except Exception as e:
                raise ValueError(f"Failed to decode feature_value from dict: {e}")
        else:
            raise ValueError(f"Dict missing 'feature_value' key: {value}")
    else:
        raise ValueError(
            f"Unsupported type for ValueProto: {type(value).__name__} (value: {value})"
        )
    return val_proto


def _encode_feature_value(value: ValueProto, is_vector: bool = False) -> Dict[str, Any]:
    """
    Encode a ValueProto into a dictionary for Elasticsearch storage.
    """
    encoded_value = base64.b64encode(value.SerializeToString()).decode("utf-8")
    result = {"feature_value": encoded_value}

    if is_vector:
        vector_val = get_list_val_str(value)
        if vector_val:
            result["vector_value"] = json.loads(vector_val)
        else:
            logger.warning(
                "Feature is marked as vector but value does not contain a valid vector."
            )
    if value.HasField("string_val"):
        result["value_text"] = value.string_val
    elif value.HasField("bytes_val"):
        result["value_text"] = value.bytes_val.decode("utf-8")
    elif value.HasField("int64_val"):
        result["value_text"] = str(value.int64_val)
    elif value.HasField("double_val"):
        result["value_text"] = str(value.double_val)
    return result


def _get_composite_key_name(table: FeatureView) -> List[str]:
    return [field.name for field in table.entity_columns]


def _extract_join_keys(entity_key_proto) -> Dict[str, ValueProto]:
    join_keys = entity_key_proto.join_keys
    entity_values = entity_key_proto.entity_values
    return {
        join_keys[i]: _to_value_proto(entity_values[i])
        for i in range(len(join_keys))
        if i < len(entity_values)
    }
