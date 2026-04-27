# Copyright 2021 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import logging
import math
import os
import time
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any,
    ByteString,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import numpy as np
from google.protobuf.timestamp_pb2 import Timestamp
from pydantic import StrictStr
from valkey.exceptions import ResponseError, ValkeyError

from feast import Entity, FeatureView, RepoConfig, utils
from feast.field import Field
from feast.infra.key_encoding_utils import (
    deserialize_entity_key,
    serialize_entity_key,
)
from feast.infra.online_stores._signal_scores import encode_signal_scores
from feast.infra.online_stores.helpers import _mmh3, _redis_key, _redis_key_prefix
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import FloatList
from feast.protos.feast.types.Value_pb2 import (
    Value as ValueProto,
)
from feast.repo_config import FeastConfigBaseModel
from feast.sorted_feature_view import SortedFeatureView
from feast.types import Array, Float64
from feast.value_type import ValueType

try:
    from valkey import Valkey
    from valkey import asyncio as valkey_asyncio
    from valkey.cluster import ClusterNode, ValkeyCluster
    from valkey.commands.search.field import TagField, VectorField
    from valkey.commands.search.indexDefinition import IndexDefinition, IndexType
    from valkey.commands.search.query import Query
    from valkey.sentinel import Sentinel
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("eg-valkey", str(e))

logger = logging.getLogger(__name__)


def _get_vector_index_name(
    project: str, feature_view_name: str, feature_name: str
) -> str:
    """Generate Valkey Search index name for a vector field."""
    return f"{project}_{feature_view_name}_{feature_name}_vidx"


def _get_valkey_vector_type(feast_dtype) -> str:
    """
    Map Feast dtype to Valkey vector TYPE parameter.

    Valkey Search only supports FLOAT32 vectors. Float64 arrays will be
    converted to float32 during serialization.

    Args:
        feast_dtype: Feast data type (e.g., Array(Float32))

    Returns:
        Valkey vector type string: always "FLOAT32"
    """
    if feast_dtype == Array(Float64):
        logger.warning(
            "Valkey Search only supports FLOAT32 vectors. "
            "Float64 data will be converted to float32 (possible precision loss)."
        )
    return "FLOAT32"


def _serialize_vector_to_bytes(val: ValueProto, field: Field) -> bytes:
    """
    Serialize a vector ValueProto to raw float32 bytes for Valkey storage.

    Vector fields must be stored as raw bytes (not protobuf serialized) to be
    compatible with Valkey Search FT.SEARCH queries. Valkey only supports
    FLOAT32, so float64 data is converted to float32.

    Args:
        val: The ValueProto containing the vector data
        field: The Field metadata for dtype and dimension information

    Returns:
        Raw float32 bytes in the format expected by Valkey vector search

    Raises:
        ValueError: If vector type is unsupported or dimension mismatches
    """
    if val.HasField("float_list_val"):
        vector = np.array(val.float_list_val.val, dtype=np.float32)
    elif val.HasField("double_list_val"):
        # Convert float64 to float32 (Valkey only supports float32)
        vector = np.array(val.double_list_val.val, dtype=np.float32)
    else:
        raise ValueError(
            f"Unsupported vector type for field {field.name}. "
            f"Expected float_list_val or double_list_val."
        )

    # Validate dimension matches expected
    if field.vector_length > 0 and len(vector) != field.vector_length:
        raise ValueError(
            f"Vector dimension mismatch for field {field.name}: "
            f"expected {field.vector_length}, got {len(vector)}"
        )

    return vector.tobytes()


def _deserialize_vector_from_bytes(raw_bytes: bytes, field: Field) -> ValueProto:
    """
    Deserialize raw vector bytes back to ValueProto.

    Valkey stores all vectors as float32, so we always deserialize as float32
    regardless of the original field dtype.

    Args:
        raw_bytes: Raw float32 bytes from Valkey
        field: Field metadata (unused, kept for API consistency)

    Returns:
        ValueProto with float_list_val (always float32)
    """
    vector = np.frombuffer(raw_bytes, dtype=np.float32)
    return ValueProto(float_list_val=FloatList(val=vector.tolist()))


class EGValkeyType(str, Enum):
    valkey = "valkey"
    valkey_cluster = "valkey_cluster"
    valkey_sentinel = "valkey_sentinel"


class EGValkeyOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for Valkey store"""

    type: Literal["eg-valkey"] = "eg-valkey"
    """Online store type selector"""

    valkey_type: EGValkeyType = EGValkeyType.valkey
    """Valkey type: valkey or valkey_cluster or valkey_sentinel"""

    sentinel_master: StrictStr = "mymaster"
    """Sentinel's master name"""

    connection_string: StrictStr = "localhost:6379"
    """Connection string containing the host, port, and configuration parameters for Valkey
     format: host:port,parameter1,parameter2 eg. valkey:6379,db=0 """

    replica_address: Optional[StrictStr] = None
    """
    (Optional) Address of the replica node used for read operations.
    If not provided, the master node will be used for both read and write operations.
    Used by GO Feature server. An example use case is the reader endpoint provided by cloud vendors.
    Example: "host1:port1,host2:port2"
    """

    key_ttl_seconds: Optional[int] = None
    """(Optional) valkey key bin ttl (in seconds) for expiring entities. Value None means No TTL. Value 0 means expire in 0 seconds."""

    full_scan_for_deletion: Optional[bool] = True
    """(Optional) whether to scan for deletion of features"""

    read_batch_size: Optional[int] = 100
    """(Optional) number of keys to read in a single batch for online read requests. Anything < 1 means no batching."""

    max_pipeline_commands: Optional[int] = 500
    """(Optional) The maximum number of Valkey commands to queue in a pipeline before sending them to Valkey in a single batch."""

    # Vector search configuration
    vector_index_algorithm: Literal["FLAT", "HNSW"] = "HNSW"
    """Algorithm for vector indexing. FLAT for exact search (<100K vectors), HNSW for approximate search (large datasets)."""

    vector_index_hnsw_m: Optional[int] = 16
    """HNSW: Max number of outgoing edges per node."""

    vector_index_hnsw_ef_construction: Optional[int] = 200
    """HNSW: Size of dynamic candidate list during index construction."""

    vector_index_hnsw_ef_runtime: Optional[int] = 10
    """HNSW: Size of dynamic candidate list during search."""


class EGValkeyOnlineStore(OnlineStore):
    """
    Valkey implementation of the online store interface. Implementation is similar to Redis online store.

    See https://github.com/feast-dev/feast/blob/master/docs/specs/online_store_format.md#redis-online-store-format
    for more details about the data model for this implementation.

    Attributes:
        _client: Valkey connection.
    """

    _client: Optional[Union[Valkey, ValkeyCluster]] = None
    _client_async: Optional[
        Union[valkey_asyncio.Valkey, valkey_asyncio.ValkeyCluster]
    ] = None

    def delete_entity_values(self, config: RepoConfig, join_keys: List[str]):
        client = self._get_client(config.online_store)
        deleted_count = 0
        prefix = _redis_key_prefix(join_keys)

        with client.pipeline(transaction=False) as pipe:
            for _k in client.scan_iter(
                b"".join([prefix, b"*", config.project.encode("utf8")])
            ):
                pipe.delete(_k)
                deleted_count += 1
            pipe.execute()

        logger.debug(f"Deleted {deleted_count} rows for entity {', '.join(join_keys)}")

    def delete_table(self, config: RepoConfig, table: FeatureView):
        """
        Delete all rows in Valkey for a specific feature view

        Args:
            config: Feast config
            table: Feature view to delete
        """
        client = self._get_client(config.online_store)
        deleted_count = 0
        prefix = _redis_key_prefix(table.join_keys)

        # Build list of hash keys to delete
        # Vector fields use original name, non-vector fields use mmh3 hash
        valkey_hash_keys = [
            f.name.encode("utf8") if f.vector_index else _mmh3(f"{table.name}:{f.name}")
            for f in table.features
        ]
        valkey_hash_keys.append(bytes(f"_ts:{table.name}", "utf8"))

        with client.pipeline(transaction=False) as pipe:
            for _k in client.scan_iter(
                b"".join([prefix, b"*", config.project.encode("utf8")])
            ):
                _tables = {
                    _hk[4:] for _hk in client.hgetall(_k) if _hk.startswith(b"_ts:")
                }
                if bytes(table.name, "utf8") not in _tables:
                    continue
                if len(_tables) == 1:
                    pipe.delete(_k)
                else:
                    pipe.hdel(_k, *valkey_hash_keys)
                deleted_count += 1
            pipe.execute()

        logger.debug(f"Deleted {deleted_count} rows for feature view {table.name}")

        # Drop vector index if it exists
        self._drop_vector_index_if_exists(client, config.project, table)

    def _drop_vector_index_if_exists(
        self,
        client: Union[Valkey, ValkeyCluster],
        project: str,
        table: FeatureView,
    ) -> None:
        """Drop Valkey Search vector indexes for all vector fields in a feature view."""
        vector_fields = [f for f in table.features if f.vector_index]

        # Drop index for each vector field
        for field in vector_fields:
            index_name = _get_vector_index_name(project, table.name, field.name)
            try:
                client.ft(index_name).dropindex(delete_documents=False)
                logger.info(f"Dropped vector index {index_name}")
            except ResponseError as e:
                # Index doesn't exist - this is fine
                if "unknown index" in str(e).lower():
                    logger.debug(
                        f"Vector index {index_name} does not exist, skipping drop"
                    )
                else:
                    raise

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        """
        Delete data from feature views that are no longer in use.

        Args:
            config: Feast config
            tables_to_delete: Feature views to delete
            tables_to_keep: Feature views to keep
            entities_to_delete: Entities to delete
            entities_to_keep: Entities to keep
            partial: Whether to do a partial update
        """
        online_store_config = config.online_store

        assert isinstance(online_store_config, EGValkeyOnlineStoreConfig)

        if online_store_config.full_scan_for_deletion:
            for table in tables_to_delete:
                self.delete_table(config, table)

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        """
        We delete the keys in valkey for tables/views being removed.
        """
        client = self._get_client(config.online_store)

        # Drop vector indexes for each table
        for table in tables:
            self._drop_vector_index_if_exists(client, config.project, table)

        # Delete entity values
        join_keys_to_delete = set(tuple(table.join_keys) for table in tables)
        for join_keys in join_keys_to_delete:
            self.delete_entity_values(config, list(join_keys))

    @staticmethod
    def _parse_connection_string(connection_string: str):
        """
        Reads Valkey connections string using format
        for ValkeyCluster:
            valkey1:6379,valkey2:6379,skip_full_coverage_check=true,ssl=true,password=...
        for Valkey Standalone:
            valkey_master:6379,db=0,ssl=true,password=...
        """
        startup_nodes = [
            dict(zip(["host", "port"], c.split(":")))
            for c in connection_string.split(",")
            if "=" not in c
        ]
        params = {}
        for c in connection_string.split(","):
            if "=" in c:
                kv = c.split("=", 1)
                try:
                    kv[1] = json.loads(kv[1])
                except json.JSONDecodeError:
                    ...

                it = iter(kv)
                params.update(dict(zip(it, it)))
        return startup_nodes, params

    def _get_client(self, online_store_config: EGValkeyOnlineStoreConfig):
        """
        Creates the Valkey client ValkeyCluster or Valkey depending on configuration
        """
        if not self._client:
            startup_nodes, kwargs = self._parse_connection_string(
                online_store_config.connection_string
            )
            if online_store_config.valkey_type == EGValkeyType.valkey_cluster:
                logger.info(f"Using Valkey Cluster: {startup_nodes}")
                kwargs["startup_nodes"] = [
                    ClusterNode(**node) for node in startup_nodes
                ]
                self._client = ValkeyCluster(**kwargs)  # type: ignore
            elif online_store_config.valkey_type == EGValkeyType.valkey_sentinel:
                sentinel_hosts = []

                for item in startup_nodes:
                    sentinel_hosts.append((item["host"], int(item["port"])))

                logger.info(f"Using Valkey Sentinel: {sentinel_hosts}")
                sentinel = Sentinel(sentinel_hosts, **kwargs)
                master = sentinel.master_for(online_store_config.sentinel_master)
                self._client = master
            else:
                logger.info(f"Using Valkey: {startup_nodes[0]}")
                kwargs["host"] = startup_nodes[0]["host"]
                kwargs["port"] = startup_nodes[0]["port"]
                self._client = Valkey(**kwargs)
        return self._client

    async def _get_client_async(self, online_store_config: EGValkeyOnlineStoreConfig):
        if not self._client_async:
            startup_nodes, kwargs = self._parse_connection_string(
                online_store_config.connection_string
            )
            if online_store_config.valkey_type == EGValkeyType.valkey_cluster:
                kwargs["startup_nodes"] = [
                    valkey_asyncio.cluster.ClusterNode(**node) for node in startup_nodes
                ]
                self._client_async = valkey_asyncio.ValkeyCluster(**kwargs)  # type: ignore
            elif online_store_config.valkey_type == EGValkeyType.valkey_sentinel:
                sentinel_hosts = []
                for item in startup_nodes:
                    sentinel_hosts.append((item["host"], int(item["port"])))

                sentinel = valkey_asyncio.Sentinel(sentinel_hosts, **kwargs)
                master = sentinel.master_for(online_store_config.sentinel_master)
                self._client_async = master
            else:
                kwargs["host"] = startup_nodes[0]["host"]
                kwargs["port"] = startup_nodes[0]["port"]
                self._client_async = valkey_asyncio.Valkey(**kwargs)
        return self._client_async

    def _create_vector_index_if_not_exists(
        self,
        client: Union[Valkey, ValkeyCluster],
        config: RepoConfig,
        table: FeatureView,
        vector_fields: Dict[str, Field],
    ) -> None:
        """
        Create Valkey Search index for each vector field if not already exists.

        Uses FT.CREATE with VECTOR field type and appropriate algorithm parameters.
        Creates one index per vector field for future multi-vector support.

        Args:
            client: Valkey client
            config: Feast repo configuration
            table: Feature view with vector fields
            vector_fields: Dictionary of vector field name to Field object
        """
        online_store_config = config.online_store
        assert isinstance(online_store_config, EGValkeyOnlineStoreConfig)

        # Define index on HASH keys with specific prefix (shared across all indexes)
        key_prefix = _redis_key_prefix(table.join_keys)
        definition = IndexDefinition(
            prefix=[key_prefix],
            index_type=IndexType.HASH,
        )

        # Create one index per vector field
        for field_name, field in vector_fields.items():
            index_name = _get_vector_index_name(config.project, table.name, field_name)

            # Check if index exists
            try:
                client.ft(index_name).info()
                logger.debug(f"Vector index {index_name} already exists")
                continue
            except ResponseError:
                pass  # Index doesn't exist, create it

            # Validate required properties
            if field.vector_length <= 0:
                raise ValueError(
                    f"Field {field_name} has vector_index=True but vector_length is not set. "
                    f"vector_length must be > 0 for vector indexing."
                )

            # Determine vector type from Feast dtype
            vector_type = _get_valkey_vector_type(field.dtype)

            # Build algorithm attributes
            attributes = {
                "TYPE": vector_type,  # Always FLOAT32 (Valkey limitation)
                "DIM": field.vector_length,
                "DISTANCE_METRIC": field.vector_search_metric or "COSINE",
            }

            # Add algorithm-specific parameters
            algorithm = online_store_config.vector_index_algorithm
            if algorithm == "HNSW":
                attributes["M"] = online_store_config.vector_index_hnsw_m
                attributes["EF_CONSTRUCTION"] = (
                    online_store_config.vector_index_hnsw_ef_construction
                )
                attributes["EF_RUNTIME"] = (
                    online_store_config.vector_index_hnsw_ef_runtime
                )

            # Create the index with vector field and project tag for filtering
            # __project__ TAG field enables filtering by project in hybrid queries
            try:
                client.ft(index_name).create_index(
                    fields=[
                        VectorField(field_name, algorithm, attributes),
                        TagField("__project__"),
                    ],
                    definition=definition,
                )
                logger.info(f"Created vector index {index_name} for field {field_name}")
            except ResponseError as e:
                if "already exists" in str(e).lower():
                    logger.debug(f"Vector index {index_name} already exists")
                    continue
                logger.error(
                    f"Failed to create vector index {index_name}: {e}. "
                    f"Ensure Valkey Search module is loaded."
                )
                raise

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        online_store_config = config.online_store
        assert isinstance(online_store_config, EGValkeyOnlineStoreConfig)

        client = self._get_client(online_store_config)
        project = config.project

        feature_view = table.name
        ts_key = f"_ts:{feature_view}"
        keys = []

        # Track all ZSET keys touched in this batch for TTL cleanup & trimming
        zsets_to_cleanup: set[Tuple[bytes, bytes]] = (
            set()
        )  # (zset_key, entity_key_bytes)
        # pipelining optimization: send multiple commands to valkey server without waiting for every reply
        with client.pipeline(transaction=False) as pipe:
            if isinstance(table, SortedFeatureView):
                logger.info(f"Using SortedFeatureView: {table.name}")

                if len(table.sort_keys) != 1:
                    raise ValueError(
                        f"Only one sort key is supported for Range query use cases in Valkey, "
                        f"but found {len(table.sort_keys)} sort keys in the feature view {table.name}."
                    )

                sort_key_type = table.sort_keys[0].value_type
                is_sort_key_timestamp = sort_key_type == ValueType.UNIX_TIMESTAMP

                if sort_key_type not in (ValueType.UNIX_TIMESTAMP,):
                    raise TypeError(
                        f"Unsupported sort key type {sort_key_type.name}. Only timestamp type is supported as a sort key."
                    )

                sort_key_name = table.sort_keys[0].name

                num_cmds = 0
                max_pipeline_commands_per_process = (
                    EGValkeyOnlineStore._get_max_pipeline_commands_per_process(
                        online_store_config.max_pipeline_commands
                    )
                )

                ttl_feature_view = table.ttl

                for entity_key, values, timestamp, _ in data:
                    ttl = None
                    if ttl_feature_view:
                        ttl = EGValkeyOnlineStore._get_ttl(ttl_feature_view, timestamp)
                    # Negative TTL means already expired, skip this row
                    if ttl and ttl < 0:
                        continue

                    entity_key_bytes = _redis_key(
                        project,
                        entity_key,
                        entity_key_serialization_version=config.entity_key_serialization_version,
                    )
                    sort_key_val = values[sort_key_name]
                    sort_key_bytes = EGValkeyOnlineStore.sort_key_bytes(
                        sort_key_name,
                        sort_key_val,
                        v=config.entity_key_serialization_version,
                    )

                    event_time_seconds = int(utils.make_tzaware(timestamp).timestamp())
                    ts = Timestamp()
                    ts.seconds = event_time_seconds
                    entity_hset = dict()
                    entity_hset[ts_key] = ts.SerializeToString()

                    for feature_name, val in values.items():
                        f_key = _mmh3(f"{feature_view}:{feature_name}")
                        entity_hset[f_key] = val.SerializeToString()

                    zset_key = EGValkeyOnlineStore.zset_key_bytes(
                        table.name, entity_key_bytes
                    )
                    hash_key = EGValkeyOnlineStore.hash_key_bytes(
                        entity_key_bytes, sort_key_bytes
                    )
                    zset_score = EGValkeyOnlineStore.zset_score(sort_key_val)
                    zset_member = sort_key_bytes
                    zsets_to_cleanup.add((zset_key, entity_key_bytes))

                    pipe.hset(hash_key, mapping=entity_hset)
                    pipe.zadd(zset_key, {zset_member: zset_score})
                    num_cmds += 2

                    if ttl:
                        pipe.expire(name=hash_key, time=ttl)
                        num_cmds += 1

                    if num_cmds >= max_pipeline_commands_per_process:
                        # TODO: May be add retries with backoff
                        try:
                            results = pipe.execute()  # flush
                        except ValkeyError:
                            logger.exception(
                                "Error executing Valkey pipeline batch for feature view %s",
                                feature_view,
                            )
                            raise
                        num_cmds = 0
                if num_cmds:
                    # flush any remaining data in the last batch
                    try:
                        results = pipe.execute()
                    except ValkeyError:
                        logger.exception(
                            "Error executing Valkey pipeline batch for feature view %s",
                            feature_view,
                        )
                        raise

                ttl_feature_view_seconds = (
                    int(ttl_feature_view.total_seconds()) if ttl_feature_view else None
                )

                run_cleanup_by_event_time = (
                    ttl_feature_view_seconds is not None
                ) and is_sort_key_timestamp

                # AFTER batch flush: run TTL cleanup
                if run_cleanup_by_event_time and ttl_feature_view_seconds:
                    cleanup_cmds = 0
                    cutoff = (int(time.time()) - ttl_feature_view_seconds) * 1000
                    for zset_key, entity_key_bytes in zsets_to_cleanup:
                        self._run_cleanup_by_event_time(
                            pipe, zset_key, ttl_feature_view_seconds, cutoff
                        )
                        cleanup_cmds += 2
                        if cleanup_cmds >= max_pipeline_commands_per_process:
                            try:
                                pipe.execute()
                            except ValkeyError:
                                logger.exception(
                                    "Error executing Valkey cleanup pipeline for feature view %s",
                                    feature_view,
                                )
                                raise
                            cleanup_cmds = 0
                    if cleanup_cmds:
                        try:
                            pipe.execute()
                        except ValkeyError:
                            logger.exception(
                                "Error executing Valkey cleanup pipeline for feature view %s",
                                feature_view,
                            )
                            raise
            else:
                # Identify vector fields (only for regular FeatureViews, not SortedFeatureView)
                vector_fields = {f.name: f for f in table.features if f.vector_index}

                # Create vector index if needed (only on first write with vector fields)
                if vector_fields:
                    self._create_vector_index_if_not_exists(
                        client, config, table, vector_fields
                    )

                # check if a previous record under the key bin exists
                # TODO: investigate if check and set is a better approach rather than pulling all entity ts and then setting
                # it may be significantly slower but avoids potential (rare) race conditions
                for entity_key, _, _, _ in data:
                    valkey_key_bin = _redis_key(
                        project,
                        entity_key,
                        entity_key_serialization_version=config.entity_key_serialization_version,
                    )
                    keys.append(valkey_key_bin)
                    pipe.hmget(valkey_key_bin, ts_key)
                prev_event_timestamps = pipe.execute()
                # flattening the list of lists. `hmget` does the lookup assuming a list of keys in the key bin
                prev_event_timestamps = [i[0] for i in prev_event_timestamps]

                for valkey_key_bin, prev_event_time, (
                    entity_key,
                    values,
                    timestamp,
                    _,
                ) in zip(keys, prev_event_timestamps, data):
                    event_time_seconds = int(utils.make_tzaware(timestamp).timestamp())

                    # ignore if event_timestamp is before the event features that are currently in the feature store
                    if prev_event_time:
                        prev_ts = Timestamp()
                        prev_ts.ParseFromString(prev_event_time)
                        if prev_ts.seconds and event_time_seconds <= prev_ts.seconds:
                            # TODO: somehow signal that it's not overwriting the current record?
                            if progress:
                                progress(1)
                            continue

                    ts = Timestamp()
                    ts.seconds = event_time_seconds
                    entity_hset = dict()
                    entity_hset[ts_key] = ts.SerializeToString()
                    # Store project and entity key for vector search
                    entity_hset["__project__"] = project.encode()
                    entity_hset["__entity_key__"] = serialize_entity_key(
                        entity_key,
                        entity_key_serialization_version=config.entity_key_serialization_version,
                    )

                    for feature_name, val in values.items():
                        if feature_name in vector_fields:
                            # Vector field: store with ORIGINAL name and RAW bytes
                            vector_bytes = _serialize_vector_to_bytes(
                                val, vector_fields[feature_name]
                            )
                            entity_hset[feature_name] = vector_bytes
                        else:
                            # Non-vector field: store with mmh3 hash and protobuf serialization
                            f_key = _mmh3(f"{feature_view}:{feature_name}")
                            entity_hset[f_key] = val.SerializeToString()

                    pipe.hset(valkey_key_bin, mapping=entity_hset)

                    ttl = online_store_config.key_ttl_seconds
                    if ttl:
                        pipe.expire(name=valkey_key_bin, time=ttl)
                results = pipe.execute()
            if progress:
                progress(len(results))

    @staticmethod
    def zset_score(sort_key_value: ValueProto):
        """
        # Get sorted set score from sorted set value
        """
        feast_value_type = sort_key_value.WhichOneof("val")
        if feast_value_type == "unix_timestamp_val":
            feature_value = (
                sort_key_value.unix_timestamp_val * 1000
            )  # Convert to milliseconds
        else:
            feature_value = getattr(sort_key_value, str(feast_value_type))
        return feature_value

    @staticmethod
    def hash_key_bytes(entity_key_bytes: bytes, sort_key_bytes: bytes) -> bytes:
        """
        hash key format: <ek_bytes><sort_key_bytes>
        """
        return b"".join([entity_key_bytes, sort_key_bytes])

    @staticmethod
    def zset_key_bytes(feature_view: str, entity_key_bytes: bytes) -> bytes:
        """
        sorted set key format: <feature_view><ek_bytes>
        """
        return b"".join([feature_view.encode("utf-8"), entity_key_bytes])

    @staticmethod
    def sort_key_bytes(sort_key_name: str, sort_val: ValueProto, v: int = 3) -> bytes:
        """
        # Serialize sort key using the same method used for entity key
        """
        sk = EntityKeyProto(join_keys=[sort_key_name], entity_values=[sort_val])
        return serialize_entity_key(sk, entity_key_serialization_version=v)

    @staticmethod
    def _get_ttl(
        ttl_feature_view: timedelta,
        timestamp: datetime,
    ) -> int:
        if ttl_feature_view > timedelta():
            ttl_offset = ttl_feature_view
        else:
            return 0
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        ttl_remaining = timestamp - utils._utc_now() + ttl_offset
        return math.ceil(ttl_remaining.total_seconds())

    @staticmethod
    def _get_max_pipeline_commands_per_process(
        max_pipeline_commands: int | None,
    ) -> int:
        assert max_pipeline_commands is not None
        num_processes = int(os.environ.get("NUM_PROCESSES", 1))
        max_pipeline_commands_per_process = max(
            1, math.ceil(max_pipeline_commands / num_processes)
        )
        return max_pipeline_commands_per_process

    def _run_cleanup_by_event_time(
        self, pipe, zset_key: bytes, ttl_seconds: int, cutoff
    ):
        pipe.zremrangebyscore(zset_key, "-inf", cutoff)
        pipe.expire(zset_key, ttl_seconds)

    def _generate_valkey_keys_for_entities(
        self, config: RepoConfig, entity_keys: List[EntityKeyProto]
    ) -> List[bytes]:
        keys = []
        for entity_key in entity_keys:
            valkey_key_bin = _redis_key(
                config.project,
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            keys.append(valkey_key_bin)
        return keys

    def _generate_hset_keys_for_features(
        self,
        feature_view: FeatureView,
        requested_features: Optional[List[str]] = None,
    ) -> Tuple[List[str], List[str], Dict[str, Field]]:
        """
        Generate HSET keys for feature retrieval.

        Returns:
            Tuple of (feature_names, hset_keys, vector_fields dict)
        """
        if not requested_features:
            requested_features = [f.name for f in feature_view.features]

        vector_fields = {f.name: f for f in feature_view.features if f.vector_index}

        hset_keys = []
        for feature_name in requested_features:
            if feature_name in vector_fields:
                # Vector field: use original name
                hset_keys.append(feature_name)
            else:
                # Non-vector: use mmh3 hash
                hset_keys.append(_mmh3(f"{feature_view.name}:{feature_name}"))

        ts_key = f"_ts:{feature_view.name}"
        hset_keys.append(ts_key)
        requested_features = list(requested_features) + [ts_key]

        return requested_features, hset_keys, vector_fields

    def _convert_valkey_values_to_protobuf(
        self,
        valkey_values: List[List[ByteString]],
        feature_view: FeatureView,
        requested_features: List[str],
        vector_fields: Dict[str, Field],
    ):
        """
        Convert Valkey values back to protobuf, handling vector fields.

        Args:
            valkey_values: Raw values from Valkey
            feature_view: Feature view object (not just name)
            requested_features: List of feature names
            vector_fields: Dict of field name to Field for vector fields
        """
        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        for values in valkey_values:
            features = self._get_features_for_entity(
                values, feature_view, requested_features, vector_fields
            )
            result.append(features)
        return result

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        online_store_config = config.online_store
        assert isinstance(online_store_config, EGValkeyOnlineStoreConfig)

        client = self._get_client(online_store_config)
        feature_view = table

        requested_features, hset_keys, vector_fields = (
            self._generate_hset_keys_for_features(feature_view, requested_features)
        )
        keys = self._generate_valkey_keys_for_entities(config, entity_keys)

        with client.pipeline(transaction=False) as pipe:
            for valkey_key_bin in keys:
                pipe.hmget(valkey_key_bin, hset_keys)

            valkey_values = pipe.execute()

        return self._convert_valkey_values_to_protobuf(
            valkey_values, feature_view, requested_features, vector_fields
        )

    async def online_read_async(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        online_store_config = config.online_store
        assert isinstance(online_store_config, EGValkeyOnlineStoreConfig)

        client = await self._get_client_async(online_store_config)
        feature_view = table

        requested_features, hset_keys, vector_fields = (
            self._generate_hset_keys_for_features(feature_view, requested_features)
        )
        keys = self._generate_valkey_keys_for_entities(config, entity_keys)

        async with client.pipeline(transaction=False) as pipe:
            for valkey_key_bin in keys:
                pipe.hmget(valkey_key_bin, hset_keys)
            valkey_values = await pipe.execute()

        return self._convert_valkey_values_to_protobuf(
            valkey_values, feature_view, requested_features, vector_fields
        )

    def _get_features_for_entity(
        self,
        values: List[ByteString],
        feature_view: FeatureView,
        requested_features: List[str],
        vector_fields: Dict[str, Field],
    ) -> Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]:
        """
        Parse features for a single entity, handling vector deserialization.

        Args:
            values: Raw bytes from Valkey
            feature_view: Feature view object
            requested_features: List of feature names (includes _ts key)
            vector_fields: Dict of field name to Field for vector fields (O(1) lookup)
        """
        res_val = dict(zip(requested_features, values))

        res_ts = Timestamp()
        ts_val = res_val.pop(f"_ts:{feature_view.name}")
        if ts_val:
            res_ts.ParseFromString(bytes(ts_val))

        res = {}
        for feature_name, val_bin in res_val.items():
            if not val_bin:
                res[feature_name] = ValueProto()
                continue

            if feature_name in vector_fields:
                # Vector field: deserialize from raw bytes
                field = vector_fields[feature_name]
                val = _deserialize_vector_from_bytes(bytes(val_bin), field)
            else:
                # Regular field: parse protobuf
                val = ValueProto()
                val.ParseFromString(bytes(val_bin))

            res[feature_name] = val

        if not res:
            return None, None
        else:
            timestamp = datetime.fromtimestamp(res_ts.seconds, tz=timezone.utc)
            return timestamp, res

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
        Retrieve documents using vector similarity search from Valkey.

        Args:
            config: Feast configuration object
            table: FeatureView to search
            requested_features: List of feature names to return
            embedding: Query embedding vector
            top_k: Number of results to return
            distance_metric: Optional override for distance metric (COSINE, L2, IP)
            query_string: Not supported in V1 (reserved for future BM25 search)

        Returns:
            List of tuples containing (timestamp, entity_key, features_dict)
        """
        if embedding is None:
            raise ValueError("embedding must be provided for vector search")

        if query_string is not None:
            raise NotImplementedError(
                "Keyword search (query_string) is not yet supported for Valkey. "
                "Only vector similarity search is available."
            )

        online_store_config = config.online_store
        assert isinstance(online_store_config, EGValkeyOnlineStoreConfig)

        client = self._get_client(online_store_config)
        project = config.project

        # Find the vector field to search against
        vector_field = self._get_vector_field_for_search(table, requested_features)
        if vector_field is None:
            raise ValueError(
                f"No vector field found in FeatureView {table.name}. "
                "Ensure the FeatureView has a field with vector_index=True."
            )

        # Determine distance metric
        metric = distance_metric or vector_field.vector_search_metric or "COSINE"

        # Serialize query embedding to bytes
        embedding_bytes = self._serialize_embedding_for_search(embedding, vector_field)

        # Build and execute FT.SEARCH query
        index_name = _get_vector_index_name(project, table.name, vector_field.name)
        search_results = self._execute_vector_search(
            client=client,
            index_name=index_name,
            project=project,
            vector_field_name=vector_field.name,
            embedding_bytes=embedding_bytes,
            top_k=top_k,
            metric=metric,
        )

        if not search_results:
            return []

        # Fetch features for each result using pipeline HMGET
        return self._fetch_features_for_search_results(
            client=client,
            config=config,
            table=table,
            requested_features=requested_features,
            search_results=search_results,
        )

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
        include_signal_scores: bool = True,
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[Dict[str, ValueProto]],
        ]
    ]:
        """
        V3 document retrieval on Valkey backend.

        Valkey supports a subset of V3 features:
        - Single embedding only (multi-embedding raises ValueError)
        - AUTO and VECTOR_ONLY fusion strategies (others raise ValueError)
        - query_string is silently dropped with a warning (Valkey cannot
          use text as a ranking signal)

        Returns the same tuple shape as V2 with final_score and signal_scores
        added to the feature dict. final_score is the raw Valkey distance
        (lower = better for COSINE/L2, higher = better for IP). See the V3
        design doc for cross-backend score semantics.

        Reserved parameters (accepted but currently unused):
        - ``include_signal_scores``: Reserved for future use.
        """
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

        if len(embeddings) > 1:
            raise ValueError(
                "Multi-vector fusion requires the Elasticsearch backend. "
                "Valkey supports single-vector search only. "
                "Use a single embedding or switch to the Elasticsearch online store."
            )

        if effective_strategy in ("RRF", "WEIGHTED_LINEAR"):
            raise ValueError(
                f"Fusion strategy '{effective_strategy}' is not supported on Valkey. "
                "Use fusion_strategy='AUTO' or 'VECTOR_ONLY', "
                "or switch to the Elasticsearch backend for fusion support."
            )

        if query_string is not None and effective_strategy != "VECTOR_ONLY":
            logger.warning(
                "query_string is being dropped — Valkey backend does not support "
                "text search as a ranking signal. To use text as a ranking signal, "
                "switch to the Elasticsearch backend."
            )

        embed_key, embed_vector = next(iter(embeddings.items()))

        v2_results = self.retrieve_online_documents_v2(
            config=config,
            table=table,
            requested_features=requested_features,
            embedding=embed_vector,
            top_k=top_k,
            distance_metric=distance_metric,
            query_string=None,  # Valkey does not support query_string
        )

        v3_results: List[
            Tuple[
                Optional[datetime],
                Optional[EntityKeyProto],
                Optional[Dict[str, ValueProto]],
            ]
        ] = []
        for timestamp, entity_key_proto, feature_dict in v2_results:
            if feature_dict is None:
                v3_results.append((timestamp, entity_key_proto, None))
                continue

            distance_val = feature_dict.pop("distance", None)
            if distance_val is not None and distance_val.HasField("double_val"):
                feature_dict["final_score"] = distance_val
                signal_scores = {f"vec_{embed_key}": distance_val.double_val}
            else:
                signal_scores = {}

            feature_dict["signal_scores"] = encode_signal_scores(signal_scores)
            v3_results.append((timestamp, entity_key_proto, feature_dict))

        return v3_results

    def _get_vector_field_for_search(
        self,
        table: FeatureView,
        requested_features: Optional[List[str]],
    ) -> Optional[Field]:
        """Find the vector field to use for search."""
        vector_fields = [f for f in table.features if f.vector_index]

        if not vector_fields:
            return None

        # If requested_features specified, prefer a vector field from that list
        if requested_features:
            # Convert to set for O(1) lookup instead of O(n) list search
            requested_set = set(requested_features)
            for f in vector_fields:
                if f.name in requested_set:
                    return f

        # Default to first vector field
        return vector_fields[0]

    def _serialize_embedding_for_search(
        self,
        embedding: List[float],
        vector_field: Field,
    ) -> bytes:
        """Serialize query embedding to bytes matching the field's dtype."""
        # Validate embedding dimension matches field configuration
        if len(embedding) != vector_field.vector_length:
            raise ValueError(
                f"Embedding dimension {len(embedding)} does not match "
                f"vector field '{vector_field.name}' dimension {vector_field.vector_length}"
            )

        if vector_field.dtype == Array(Float64):
            return np.array(embedding, dtype=np.float64).tobytes()
        else:
            # Default to float32
            return np.array(embedding, dtype=np.float32).tobytes()

    def _execute_vector_search(
        self,
        client: Union[Valkey, ValkeyCluster],
        index_name: str,
        project: str,
        vector_field_name: str,
        embedding_bytes: bytes,
        top_k: int,
        metric: str,
    ) -> List[Tuple[bytes, float]]:
        """
        Execute FT.SEARCH with KNN query.

        Returns:
            List of (doc_key, distance) tuples
        """
        # Escape special characters in project name for tag filter.
        # In Valkey Search tag queries, characters like - . @ need backslash escaping.
        escaped_project = project
        for ch in r'\-.@+~<>{}[]^":|!*()':
            escaped_project = escaped_project.replace(ch, f"\\{ch}")

        query_str = (
            f"(@__project__:{{{escaped_project}}})"
            f"=>[KNN {top_k} @{vector_field_name} $vec AS __distance__]"
        )

        # KNN results are already sorted by distance (ascending) by the engine.
        # No explicit SORTBY is needed — Valkey Search does not support SORTBY
        # with KNN queries.
        query = (
            Query(query_str).return_fields("__distance__").paging(0, top_k).dialect(2)
        )

        try:
            results = client.ft(index_name).search(
                query,
                query_params={"vec": embedding_bytes},
            )
        except ResponseError as e:
            if "no such index" in str(e).lower():
                raise ValueError(
                    f"Vector index '{index_name}' does not exist. "
                    "Ensure data has been materialized with 'feast materialize'."
                )
            raise

        # Parse results: extract doc keys and distances
        search_results = []
        for doc in results.docs:
            doc_key = doc.id.encode() if isinstance(doc.id, str) else doc.id
            # Default to inf (worst distance) if __distance__ is missing
            # 0.0 would incorrectly indicate a perfect match
            distance = float(getattr(doc, "__distance__", float("inf")))
            search_results.append((doc_key, distance))

        return search_results

    def _fetch_features_for_search_results(
        self,
        client: Union[Valkey, ValkeyCluster],
        config: RepoConfig,
        table: FeatureView,
        requested_features: List[str],
        search_results: List[Tuple[bytes, float]],
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[Dict[str, ValueProto]],
        ]
    ]:
        """
        Fetch features for search results using pipeline HMGET.

        This is the second step of two-step retrieval:
        1. FT.SEARCH returns doc keys and distances
        2. HMGET fetches the actual feature values
        """
        # Pre-compute mappings once (avoid repeated dict/hash operations in loops)
        vector_fields_dict = {f.name: f for f in table.features if f.vector_index}

        # Build feature_name -> hset_key mapping and hset_keys list in single pass
        feature_to_hset_key: Dict[str, Any] = {}
        hset_keys = []
        for feature_name in requested_features:
            if feature_name in vector_fields_dict:
                hset_key = feature_name
            else:
                hset_key = _mmh3(f"{table.name}:{feature_name}")
            feature_to_hset_key[feature_name] = hset_key
            hset_keys.append(hset_key)

        # Add timestamp and entity key
        ts_key = f"_ts:{table.name}"
        hset_keys.append(ts_key)
        hset_keys.append("__entity_key__")

        # Extract doc_keys and distances in single pass
        doc_keys = []
        distances = {}
        for doc_key, dist in search_results:
            doc_keys.append(doc_key)
            distances[doc_key] = dist

        # Pipeline HMGET for all results (single round-trip to Valkey)
        with client.pipeline(transaction=False) as pipe:
            for doc_key in doc_keys:
                key_str = doc_key.decode() if isinstance(doc_key, bytes) else doc_key
                pipe.hmget(key_str, hset_keys)
            fetched_values = pipe.execute()

        # Pre-fetch serialization version once
        entity_key_serialization_version = config.entity_key_serialization_version

        # Build result list
        results: List[
            Tuple[
                Optional[datetime],
                Optional[EntityKeyProto],
                Optional[Dict[str, ValueProto]],
            ]
        ] = []

        for doc_key, values in zip(doc_keys, fetched_values):
            # Parse values into dict
            val_dict = dict(zip(hset_keys, values))

            # Parse timestamp
            timestamp = None
            ts_val = val_dict.get(ts_key)
            if ts_val:
                ts_proto = Timestamp()
                ts_proto.ParseFromString(bytes(ts_val))
                timestamp = datetime.fromtimestamp(ts_proto.seconds, tz=timezone.utc)

            # Parse entity key
            entity_key_proto = None
            entity_key_bytes = val_dict.get("__entity_key__")
            if entity_key_bytes:
                entity_key_proto = deserialize_entity_key(
                    bytes(entity_key_bytes),
                    entity_key_serialization_version=entity_key_serialization_version,
                )

            # Build feature dict with pre-allocated capacity hint
            feature_dict: Dict[str, ValueProto] = {}

            # Add distance as a feature
            distance_proto = ValueProto()
            distance_proto.double_val = distances[doc_key]
            feature_dict["distance"] = distance_proto

            # Parse requested features using pre-computed mappings
            for feature_name in requested_features:
                hset_key = feature_to_hset_key[feature_name]
                val_bin = val_dict.get(hset_key)

                if not val_bin:
                    feature_dict[feature_name] = ValueProto()
                    continue

                if feature_name in vector_fields_dict:
                    # Vector field: deserialize from raw bytes
                    feature_dict[feature_name] = _deserialize_vector_from_bytes(
                        bytes(val_bin), vector_fields_dict[feature_name]
                    )
                else:
                    # Regular field: parse protobuf
                    val = ValueProto()
                    val.ParseFromString(bytes(val_bin))
                    feature_dict[feature_name] = val

            results.append((timestamp, entity_key_proto, feature_dict))

        return results
