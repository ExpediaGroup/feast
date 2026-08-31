# Copyright 2026 The Feast Authors
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

"""
Helpers that map Feast objects to OpenLineage identities and facet payloads.

Naming contract (metadata-bus-user-guide PR #21, EAPC-22333):
  * FeatureView namespace: ``mlp://mlpfs-{env}`` (shared across a project)
  * FeatureView name:      ``{project}.{feature_view_name}``
  * batch source URI:      ``egdl://data-{env}.{region}/{db}.{table}`` (lake FQN)
  * stream source URI:     the EGSP consumer URN, or ``kafka://{servers}/{topic}``

These helpers return *pure data* (strings, lists of tuples) and the Feast-custom
facet objects. The standard OpenLineage facets (documentation, ownership, tags,
schema, dataSource, datasetType) are assembled in the emitter, which owns the
OpenLineage-client dependency.
"""

import os
from typing import TYPE_CHECKING, Any, List, Optional, Tuple

if TYPE_CHECKING:
    from feast import FeatureView
    from feast.data_source import DataSource

# Feast DataSource class name -> ``datasetType`` sub-type. The sub-type is the
# discriminator the sync app uses to route resolution (egdl:// vs egsp://).
_SOURCE_SUBTYPE = {
    "SparkSource": "BATCH_SPARK",
    "FileSource": "BATCH_FILE",
    "BigQuerySource": "BATCH_BIGQUERY",
    "SnowflakeSource": "BATCH_SNOWFLAKE",
    "RedshiftSource": "BATCH_REDSHIFT",
    "TrinoSource": "BATCH_TRINO",
    "AthenaSource": "BATCH_ATHENA",
    "PostgreSQLSource": "BATCH_POSTGRES",
    "KafkaSource": "STREAM_KAFKA",
    "KinesisSource": "STREAM_KINESIS",
    "PushSource": "PUSH",
}


def dataset_name(project: str, asset_name: str) -> str:
    """Project-qualified OpenLineage dataset name, e.g. ``proj.hotel_price_features``."""
    return f"{project}.{asset_name}"


def source_display_name(data_source: "DataSource") -> str:
    """Bare source name used in the ``dataSource``/``feast_*Source`` facet ``name``."""
    return data_source.name or f"unnamed_{type(data_source).__name__}"


def source_subtype(data_source: "DataSource") -> str:
    """``datasetType`` sub-type for a data source, by class name."""
    cls = type(data_source).__name__
    if cls in _SOURCE_SUBTYPE:
        return _SOURCE_SUBTYPE[cls]
    # Fall back to a BATCH_<CLASS> shape so an unmapped source is still routable.
    base = cls[:-6] if cls.endswith("Source") else cls
    return f"BATCH_{base.upper()}"


def is_stream_backed(feature_view: "FeatureView") -> bool:
    """A FeatureView is stream-backed when it carries a stream source."""
    return getattr(feature_view, "stream_source", None) is not None


# MLP data-plane region for the data-lake FQN. No region is injected into the
# feast apply path (REGISTRY_ENV carries only the env tier, and the registry host
# has no region segment); every ML Platform feature repo hardcodes us-east-1, so
# reuse that as the default -- overridable via AWS_REGION rather than new config.
_DEFAULT_DATA_LAKE_REGION = "us-east-1"


def _data_lake_region() -> str:
    return (
        os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or _DEFAULT_DATA_LAKE_REGION
    )


def batch_source_uri(data_source: "DataSource", env: Optional[str] = None) -> str:
    """
    Best-effort physical URI for a batch source.

    Preference order:
      1. ``egdl://data-{env}.{region}/{db}.{table}`` -- the data-lake FQN the sync
         app resolves against. ``{db}.{table}`` already lives verbatim in the
         SparkSource ``table`` attribute at apply time (customers build it as
         ``f"{OFFLINE_STORE_DATABASE_NAME}.{table}"``); ``env`` is the resolved
         REGISTRY_ENV and ``region`` the MLP default (see ``_data_lake_region``).
      2. the source ``path`` (e.g. ``s3://...``).
      3. ``table://{table}`` / ``query://{hash}`` / ``feast://{name}`` fallbacks
         for sources with neither a table nor an env (e.g. ``query=`` sources).
    """
    table = getattr(data_source, "table", None)
    if table and env:
        return f"egdl://data-{env}.{_data_lake_region()}/{table}"

    path = getattr(data_source, "path", None)
    if path:
        return str(path)
    if table:
        return f"table://{table}"
    query = getattr(data_source, "query", None)
    if query:
        return f"query://{hash(query)}"
    return f"feast://{data_source.name or 'unnamed'}"


def stream_source_uri(data_source: "DataSource") -> str:
    """
    Best-effort URI for a stream source.

    At EG the Kafka ``topic`` is an ``urn:egsp:consumer:...`` URN, which the sync
    app resolves to the ``egsp://`` topic node; pass it through verbatim. For a
    plain topic name, fall back to ``kafka://{servers}/{topic}``.
    """
    options = getattr(data_source, "kafka_options", None)
    topic = getattr(options, "topic", None) if options else None
    if topic:
        if topic.startswith("urn:"):
            return topic
        servers = getattr(options, "kafka_bootstrap_servers", None) or "kafka"
        return f"kafka://{servers}/{topic}"
    return f"feast://{data_source.name or 'unnamed'}"


# ------------------------------------------------------------ standard-facet data


def schema_fields(feature_view: "FeatureView") -> List[Tuple[str, str, str]]:
    """
    ``(name, type, description)`` per column: join-key columns first, then
    features. Types are the Feast dtype string (OM-type mapping is a
    consumer/follow-up concern -- see EAPC-22333 open items).
    """
    fields: List[Tuple[str, str, str]] = []
    seen = set()

    entity_columns = getattr(feature_view, "entity_columns", None) or []
    for col in entity_columns:
        fields.append((col.name, str(col.dtype), col.description or ""))
        seen.add(col.name)
    # entity_columns holds the join-key *columns* (keyed by join_key). It is only
    # populated after schema inference; ONLY when it is still empty do we fall back
    # to the bare entity *names* so join keys still surface as columns pre-inference.
    # Once it is populated, the fallback must not run: an Entity whose name differs
    # from its join_key (e.g. name ``driver`` / join_key ``driver_id``) would emit
    # a phantom, type-less column for the *name* on top of the real join-key column.
    if not entity_columns:
        for entity_name in getattr(feature_view, "entities", None) or []:
            if entity_name and entity_name not in seen:
                fields.append((entity_name, "", ""))
                seen.add(entity_name)

    for feat in getattr(feature_view, "features", None) or []:
        if feat.name not in seen:
            fields.append((feat.name, str(feat.dtype), feat.description or ""))
            seen.add(feat.name)
    return fields


def join_keys(feature_view: "FeatureView") -> List[str]:
    """
    Physical join-key column names for the view -- what ``feast_featureView.entities``
    carries, so it lines up with ``schema.fields`` for join-key column tagging.

    Prefer ``entity_columns`` (the resolved join-key ``Field``s, keyed by join_key,
    populated after inference); fall back to the bare entity *names* only
    pre-inference, when the join keys are not yet resolvable. An Entity's ``name``
    and ``join_key`` can differ (e.g. name ``driver`` / join_key ``driver_id``); the
    join_key is the real source column, so it -- not the name -- is what a consumer
    must match against ``schema.fields`` to tag the join-key column.
    """
    entity_columns = getattr(feature_view, "entity_columns", None) or []
    if entity_columns:
        return [col.name for col in entity_columns]
    return [e for e in (getattr(feature_view, "entities", None) or []) if e]


# EG mandatory FeatureView tags (application, team, owner, product, costCenter --
# feature-store-feast-sdk validation) that are dropped from the tags facet, either
# because they are re-homed onto another facet or because a governed tag carries
# the information instead (EGDL feedback, Deepak Jain, EAPC-22333):
#   * application -> re-keyed to the governed ``eg-application-name`` tag (below)
#   * product     -> dropped outright. The ML Platform ``product`` tag is an MLP
#                    product name, *not* a data-mesh data-product name, so it is
#                    not a valid ``eg-data-product`` value; the governed tag is
#                    resolved independently of it (see ``tag_pairs``)
#   * team, owner -> already carried by the standard ``ownership`` facet
_TAGS_REHOMED = frozenset({"application", "product", "team", "owner"})

# The governed data-product tag. Resolved -- and therefore emitted -- exactly once
# by ``tag_pairs`` rather than passed straight through, so a present-but-blank tag
# still falls back to the configured default.
_TAG_DATA_PRODUCT = "eg-data-product"


def tag_pairs(
    feature_view: "FeatureView", default_data_product: Optional[str] = None
) -> List[Tuple[str, str]]:
    """
    ``(key, value)`` tag pairs for the standard ``tags`` facet.

    Emits the view's own tags -- minus the EG mandatory tags dropped in favour of
    another facet or a governed tag (see ``_TAGS_REHOMED``) so the tags facet does
    not duplicate ownership / application / product information -- then
    ``ttl_seconds`` (as a string) and the two governed tags below.

    ``application`` -> ``eg-application-name``:
        At apply time the FeatureView carries the app name under the ML Platform
        ``application`` tag; ``eg-application-name`` itself only exists on the
        offline table tags, which the emitter never sees. Re-key onto the governed
        tag the sync app recognizes.

    ``eg-data-product``:
        Taken from the view's own ``eg-data-product`` tag, falling back to
        ``default_data_product`` when the view carries no such tag or its value is
        blank. Feast does **not** map the product to a data-mesh domain itself --
        it forwards the product identity, and the Metadata Bus relay resolves
        ``eg-data-product`` against ``eg-internal/data-mesh-definitions`` to set
        the FeatureView's domain and owners (the same mechanism the model side /
        MRS uses, EAPC-22420): "``eg-data-product`` is a tag when provided is used
        to resolve Domain and Owner of the asset in OM" (Deepak Jain), and "the tag
        value must match the ``metadata.name`` field in your data product YAML"
        (FAQ: OpenMetadata).

        The ML Platform ``product`` tag is deliberately **not** used as a source
        here. Its values are MLP product names, which are not registered data
        products -- emitting one (e.g. ``unified-feature-store``) resolves to
        nothing and strands the view. So a view is tagged either with a real
        ``eg-data-product`` or with the platform default
        (``config.DEFAULT_DATA_PRODUCT``, ``mlp-feature-registry``), per
        data-mesh-definitions#480; teams override it by tagging their objects
        with their own registered product.

    Per the metadata-bus rule, each governed tag is omitted entirely -- never a
    placeholder -- when no value exists.
    """
    view_tags = getattr(feature_view, "tags", None) or {}

    pairs: List[Tuple[str, str]] = [
        (str(k), str(v))
        for k, v in view_tags.items()
        if k not in _TAGS_REHOMED and k != _TAG_DATA_PRODUCT
    ]

    ttl = getattr(feature_view, "ttl", None)
    if ttl:
        pairs.append(("ttl_seconds", str(int(ttl.total_seconds()))))

    app = (
        view_tags.get("application")
        or view_tags.get("eg-application-name")
        or os.environ.get("EG_APPLICATION_NAME")
    )
    if app and "eg-application-name" not in view_tags:
        pairs.append(("eg-application-name", str(app)))

    data_product = (
        str(view_tags.get(_TAG_DATA_PRODUCT) or "").strip() or default_data_product
    )
    if data_product:
        pairs.append((_TAG_DATA_PRODUCT, str(data_product)))
    return pairs


def owners(feature_view: "FeatureView") -> List[str]:
    """Owning teams/people for the ``ownership`` facet (empty when unset)."""
    owner = getattr(feature_view, "owner", None)
    return [owner] if owner else []


def dataset_type(feature_view: "FeatureView") -> Tuple[str, str]:
    """``(datasetType, subType)`` discriminator for the primary source."""
    primary = (
        feature_view.stream_source
        if is_stream_backed(feature_view)
        else feature_view.batch_source
    )
    kind = "STREAM" if is_stream_backed(feature_view) else "TABLE"
    subtype = source_subtype(primary) if primary is not None else f"{kind}_UNKNOWN"
    return kind, subtype


# ------------------------------------------------------------ custom facets


def build_feature_view_facet(feature_view: "FeatureView") -> Any:
    """Build the trimmed ``feast_featureView`` dataset facet."""
    from feast.openlineage.facets import FeastFeatureViewFacet

    mode = getattr(feature_view, "mode", None)
    primary = (
        feature_view.stream_source
        if is_stream_backed(feature_view)
        else feature_view.batch_source
    )
    ts_field = getattr(primary, "timestamp_field", None) if primary else None

    return FeastFeatureViewFacet(
        entities=join_keys(feature_view),
        online_enabled=bool(getattr(feature_view, "online", True)),
        offline_enabled=bool(getattr(feature_view, "offline", False)),
        mode=str(mode) if mode else None,
        timestamp_field=ts_field or None,
    )


def build_stream_source_facet(stream_source: "DataSource") -> Any:
    """Build the ``feast_streamSource`` dataset facet from a KafkaSource."""
    from feast.openlineage.facets import FeastStreamSourceFacet

    options = getattr(stream_source, "kafka_options", None)
    topic = getattr(options, "topic", None) if options else None
    servers = getattr(options, "kafka_bootstrap_servers", None) if options else None
    message_format = getattr(options, "message_format", None) if options else None
    watermark = getattr(options, "watermark_delay_threshold", None) if options else None

    return FeastStreamSourceFacet(
        name=source_display_name(stream_source),
        topic=topic or None,
        kafka_bootstrap_servers=servers or None,
        message_format=type(message_format).__name__ if message_format else None,
        timestamp_field=getattr(stream_source, "timestamp_field", None) or None,
        field_mapping=dict(getattr(stream_source, "field_mapping", {}) or {}),
        watermark_delay_seconds=int(watermark.total_seconds()) if watermark else None,
    )


def build_batch_source_facet(
    batch_source: "DataSource", env: Optional[str] = None
) -> Any:
    """Build the ``feast_batchSource`` dataset facet for a stream-backed view."""
    from feast.openlineage.facets import FeastBatchSourceFacet

    return FeastBatchSourceFacet(
        name=source_display_name(batch_source),
        uri=batch_source_uri(batch_source, env),
        source_type=source_subtype(batch_source),
        timestamp_field=getattr(batch_source, "timestamp_field", None) or None,
        created_timestamp_field=getattr(batch_source, "created_timestamp_column", None)
        or None,
        field_mapping=dict(getattr(batch_source, "field_mapping", {}) or {}),
    )
