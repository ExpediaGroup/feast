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
from datetime import datetime, timezone
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

from google.protobuf.timestamp_pb2 import Timestamp
from pydantic import StrictStr, StrictInt

from feast import Entity, FeatureView, RepoConfig, utils
from feast.infra.online_stores.helpers import _mmh3, _redis_key, _redis_key_prefix
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from feast.rate_limiter import create_rate_limiter, BaseRateLimiter

try:
    from valkey import Valkey
    from valkey import asyncio as valkey_asyncio
    from valkey.cluster import ClusterNode, ValkeyCluster
    from valkey.sentinel import Sentinel
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("eg-valkey", str(e))

logger = logging.getLogger(__name__)


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

    write_rate_limit: Optional[StrictInt] = 0
    """
    (Optional) Maximum number of write batches per second. Value 0 (default) means no rate limiting.
    Each pipeline execution that writes data counts as one batch.
    """


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

        valkey_hash_keys = [_mmh3(f"{table.name}:{f.name}") for f in table.features]
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

        # Initialize generic rate limiter (NoOp if disabled)
        rate_limiter: BaseRateLimiter = create_rate_limiter(
            online_store_config.write_rate_limit, 1
        )

        feature_view = table.name
        ts_key = f"_ts:{feature_view}"
        keys: List[bytes] = []
        # pipelining optimization: send multiple commands to valkey server without waiting for every reply
        with client.pipeline(transaction=False) as pipe:
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
            staged = 0
            for valkey_key_bin, prev_event_time, (_, values, timestamp, _) in zip(
                keys, prev_event_timestamps, data
            ):
                event_time_seconds = int(utils.make_tzaware(timestamp).timestamp())

                # Skip stale
                if prev_event_time:
                    prev_ts = Timestamp()
                    prev_ts.ParseFromString(prev_event_time)
                    if prev_ts.seconds and event_time_seconds <= prev_ts.seconds:
                        if progress:
                            progress(1)
                        # TODO: somehow signal that it's not overwriting the current record?
                        continue

                ts = Timestamp(); ts.seconds = event_time_seconds
                entity_hset: Dict[Any, Any] = {ts_key: ts.SerializeToString()}
                for feature_name, val in values.items():
                    f_key = _mmh3(f"{feature_view}:{feature_name}")
                    entity_hset[f_key] = val.SerializeToString()
                pipe.hset(valkey_key_bin, mapping=entity_hset)
                ttl = online_store_config.key_ttl_seconds

                if ttl:

                    pipe.expire(name=valkey_key_bin, time=ttl)
                staged += 1

            if staged:
                if not rate_limiter.acquire():
                    rate_limiter.acquire_blocking()
                results = pipe.execute()
                if progress:
                    progress(len(results))

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
    ) -> Tuple[List[str], List[str]]:
        if not requested_features:
            requested_features = [f.name for f in feature_view.features]

        hset_keys = [_mmh3(f"{feature_view.name}:{k}") for k in requested_features]

        ts_key = f"_ts:{feature_view.name}"
        hset_keys.append(ts_key)
        requested_features.append(ts_key)

        return requested_features, hset_keys

    def _convert_valkey_values_to_protobuf(
        self,
        valkey_values: List[List[ByteString]],
        feature_view: str,
        requested_features: List[str],
    ):
        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        for values in valkey_values:
            features = self._get_features_for_entity(
                values, feature_view, requested_features
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

        requested_features, hset_keys = self._generate_hset_keys_for_features(
            feature_view, requested_features
        )
        keys = self._generate_valkey_keys_for_entities(config, entity_keys)

        with client.pipeline(transaction=False) as pipe:
            for valkey_key_bin in keys:
                pipe.hmget(valkey_key_bin, hset_keys)

            valkey_values = pipe.execute()

        return self._convert_valkey_values_to_protobuf(
            valkey_values, feature_view.name, requested_features
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

        requested_features, hset_keys = self._generate_hset_keys_for_features(
            feature_view, requested_features
        )
        keys = self._generate_valkey_keys_for_entities(config, entity_keys)

        async with client.pipeline(transaction=False) as pipe:
            for valkey_key_bin in keys:
                pipe.hmget(valkey_key_bin, hset_keys)
            valkey_values = await pipe.execute()

        return self._convert_valkey_values_to_protobuf(
            valkey_values, feature_view.name, requested_features
        )

    def _get_features_for_entity(
        self,
        values: List[ByteString],
        feature_view: str,
        requested_features: List[str],
    ) -> Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]:
        res_val = dict(zip(requested_features, values))

        res_ts = Timestamp()
        ts_val = res_val.pop(f"_ts:{feature_view}")
        if ts_val:
            res_ts.ParseFromString(bytes(ts_val))

        res = {}
        for feature_name, val_bin in res_val.items():
            val = ValueProto()
            if val_bin:
                val.ParseFromString(bytes(val_bin))
            res[feature_name] = val

        if not res:
            return None, None
        else:
            timestamp = datetime.fromtimestamp(res_ts.seconds, tz=timezone.utc)
            return timestamp, res
