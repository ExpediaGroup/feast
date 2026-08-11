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
Custom OpenLineage facets for the ML Platform Feature Store (Feast).

Scope (EAPC-22333, metadata-bus-user-guide PR #21)
--------------------------------------------------
``feast apply`` emits exactly **one** ``DatasetEvent`` per FeatureView. Standard
concerns (description, ownership, tags, schema, data source, dataset type) ride
on the corresponding **standard** OpenLineage facets, built directly from the
OpenLineage client in the emitter. The custom facets defined here carry only the
Feast-specific residue that has no standard-facet home:

* ``feast_featureView`` -- join-key entities, online/offline flags, mode,
  timestamp field.
* ``feast_streamSource`` / ``feast_batchSource`` -- the two upstreams of a
  *stream-backed* FeatureView. A ``KafkaSource`` always wraps a required
  ``batch_source`` (SDK-enforced), so a single standard ``dataSource`` facet
  cannot represent both. The stream rides on the standard ``dataSource`` facet
  (discriminated by ``datasetType: STREAM``); the mandatory batch source rides
  on ``feast_batchSource``; ``feast_streamSource`` carries the Kafka-only detail
  (topic URN, message format, watermark) that has no standard home.

EG divergence from upstream feast-dev/feast
--------------------------------------------
Upstream points every custom facet's ``_schemaURL`` at
``https://feast.dev/spec/facets/1-0-0/<Name>.json`` -- URLs that are not hosted
and therefore 404. The Metadata Bus / OpenMetadata relay expects a resolvable
schema URL, so this fork deliberately does **not** override ``_get_schema()``.
Inheriting the OpenLineage base facet means custom dataset facets resolve to
``.../OpenLineage.json#/$defs/DatasetFacet`` -- matching the interim convention
already used by the model side (MRS, EAPC-22420) and ``data-api-sdk``. A governed
EG-hosted facet-schema location is the intended long-term home.
"""

from typing import Dict, List, Optional

import attr

try:
    from openlineage.client.generated.base import DatasetFacet
    from openlineage.client.utils import RedactMixin  # noqa: F401

    OPENLINEAGE_AVAILABLE = True
except ImportError:
    # Provide a stub class when OpenLineage is not installed so imports do not
    # fail; the facets are only ever instantiated when OpenLineage is available.
    OPENLINEAGE_AVAILABLE = False

    @attr.define
    class DatasetFacet:  # type: ignore[no-redef]
        _producer: str = attr.field(default="")
        _schemaURL: str = attr.field(default="")
        _deleted: bool = attr.field(default=None)

        def __attrs_post_init__(self):
            pass


@attr.define(kw_only=True)
class FeastFeatureViewFacet(DatasetFacet):
    """
    Feast-specific FeatureView metadata with no standard-facet home.

    Carried on the FeatureView ``DatasetEvent`` alongside the standard
    ``documentation`` / ``ownership`` / ``tags`` / ``schema`` / ``dataSource`` /
    ``datasetType`` facets. Everything with a standard home lives there; this
    facet is deliberately trimmed to the residue.

    Attributes:
        entities: Join-key entity names this view is keyed on. Doubles as the
            signal for which ``schema.fields`` entries are join-key columns.
        online_enabled: Whether online retrieval is enabled.
        offline_enabled: Whether offline retrieval is enabled.
        mode: Transformation mode (PYTHON, PANDAS, SPARK, ...); the stream
            transformation engine on stream feature views.
        timestamp_field: Event-timestamp column used for point-in-time
            correctness.
    """

    entities: List[str] = attr.field(factory=list)
    online_enabled: bool = attr.field(default=True)
    offline_enabled: bool = attr.field(default=False)
    mode: Optional[str] = attr.field(default=None)
    timestamp_field: Optional[str] = attr.field(default=None)


@attr.define(kw_only=True)
class FeastStreamSourceFacet(DatasetFacet):
    """
    Kafka-only metadata for a stream-backed FeatureView.

    The stream itself rides on the standard ``dataSource`` facet (with
    ``datasetType: STREAM``); this facet carries the Feast/Kafka detail that has
    no standard-facet home. Consumers resolve ``topic`` (an EGSP consumer URN at
    EG) to the ``egsp://`` topic node registered by ``egsp-stream-registry``.

    Attributes:
        name: Stream source name.
        topic: Kafka topic (an ``urn:egsp:consumer:...`` URN at EG).
        kafka_bootstrap_servers: Bootstrap servers, when set.
        message_format: Serialization format of the messages (e.g.
            ``ConfluentAvroFormat``).
        timestamp_field: Event-timestamp field on the stream.
        field_mapping: Mapping from (possibly nested) source fields to feature
            columns -- source-column-to-feature column lineage.
        watermark_delay_seconds: Watermark delay threshold in seconds, when set.
    """

    name: str = attr.field()
    topic: Optional[str] = attr.field(default=None)
    kafka_bootstrap_servers: Optional[str] = attr.field(default=None)
    message_format: Optional[str] = attr.field(default=None)
    timestamp_field: Optional[str] = attr.field(default=None)
    field_mapping: Dict[str, str] = attr.field(factory=dict)
    watermark_delay_seconds: Optional[int] = attr.field(default=None)


@attr.define(kw_only=True)
class FeastBatchSourceFacet(DatasetFacet):
    """
    The mandatory batch (historical-retrieval) source of a stream-backed
    FeatureView.

    A ``KafkaSource`` always wraps a required ``batch_source``; since the stream
    occupies the standard ``dataSource`` facet, the batch source is carried here
    so its historical-retrieval upstream is not lost. Consumers resolve ``uri``
    to the real ``egdl://`` lake table.

    Attributes:
        name: Batch source name.
        uri: Physical/logical URI (``egdl://...`` FQN when resolvable).
        source_type: ``datasetType`` sub-type of the batch source (e.g.
            ``BATCH_SPARK``).
        timestamp_field: Event-timestamp field on the batch source.
        created_timestamp_field: Created-timestamp field, when set.
        field_mapping: Source-field-to-feature-column mapping.
    """

    name: str = attr.field()
    uri: Optional[str] = attr.field(default=None)
    source_type: str = attr.field(default="")
    timestamp_field: Optional[str] = attr.field(default=None)
    created_timestamp_field: Optional[str] = attr.field(default=None)
    field_mapping: Dict[str, str] = attr.field(factory=dict)
