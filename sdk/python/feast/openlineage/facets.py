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

These facets extend the standard OpenLineage facets to capture Feast-specific
metadata about feature views, feature services, data sources, and entities.

EG divergence from upstream feast-dev/feast
--------------------------------------------
Upstream points every custom facet's ``_schemaURL`` at
``https://feast.dev/spec/facets/1-0-0/<Name>.json`` -- URLs that are not hosted
and therefore 404. The Metadata Bus / OpenMetadata relay expects a resolvable
schema URL, so this fork deliberately does **not** override ``_get_schema()``.
Inheriting the OpenLineage base facet means:

* dataset facets resolve to ``.../OpenLineage.json#/$defs/DatasetFacet``
* job facets resolve to ``.../OpenLineage.json#/$defs/JobFacet``

which matches the interim convention already used by the model side (MRS,
EAPC-22420) and ``data-api-sdk``. A governed EG-hosted facet-schema location is
the intended long-term home, to be settled with EGDL alongside the MRS facets.

Only the facets used by the ``feast apply`` emit path are defined here. Upstream's
materialization / retrieval run facets are intentionally omitted (out of scope --
see EAPC-22333).
"""

from typing import Dict, List, Optional

import attr

try:
    from openlineage.client.generated.base import DatasetFacet, JobFacet
    from openlineage.client.utils import RedactMixin  # noqa: F401

    OPENLINEAGE_AVAILABLE = True
except ImportError:
    # Provide stub classes when OpenLineage is not installed so imports do not
    # fail; the facets are only ever instantiated when OpenLineage is available.
    OPENLINEAGE_AVAILABLE = False

    @attr.define
    class JobFacet:  # type: ignore[no-redef]
        _producer: str = attr.field(default="")
        _schemaURL: str = attr.field(default="")

        def __attrs_post_init__(self):
            pass

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
    Custom facet for Feast Feature View metadata.

    Carried on the ``DatasetEvent`` that registers the feature-view node.

    Attributes:
        name: Feature view name (bare Feast name, not project-qualified)
        ttl_seconds: Time-to-live in seconds (0 means no TTL)
        entities: List of entity names associated with the feature view
        features: List of feature names in the feature view
        online_enabled: Whether online retrieval is enabled
        offline_enabled: Whether offline retrieval is enabled
        mode: Transformation mode (PYTHON, PANDAS, RAY, SPARK, SQL, etc.)
        description: Human-readable description
        owner: Owner of the feature view
        tags: Key-value tags
    """

    name: str = attr.field()
    ttl_seconds: int = attr.field(default=0)
    entities: List[str] = attr.field(factory=list)
    features: List[str] = attr.field(factory=list)
    online_enabled: bool = attr.field(default=True)
    offline_enabled: bool = attr.field(default=False)
    mode: Optional[str] = attr.field(default=None)
    description: str = attr.field(default="")
    owner: str = attr.field(default="")
    tags: Dict[str, str] = attr.field(factory=dict)


@attr.define(kw_only=True)
class FeastFeatureServiceFacet(DatasetFacet):
    """
    Custom facet for Feast Feature Service metadata.

    Carried on the ``DatasetEvent`` that registers the feature-service node.

    Attributes:
        name: Feature service name (bare Feast name)
        feature_views: List of feature view names included in the service
        feature_count: Total number of features in the service
        description: Human-readable description
        owner: Owner of the feature service
        tags: Key-value tags
        logging_enabled: Whether feature logging is enabled
    """

    name: str = attr.field()
    feature_views: List[str] = attr.field(factory=list)
    feature_count: int = attr.field(default=0)
    description: str = attr.field(default="")
    owner: str = attr.field(default="")
    tags: Dict[str, str] = attr.field(factory=dict)
    logging_enabled: bool = attr.field(default=False)


@attr.define(kw_only=True)
class FeastDataSourceFacet(DatasetFacet):
    """
    Custom facet for Feast Data Source (batch source) metadata.

    Carried on the ``DatasetEvent`` that registers the batch-source node,
    alongside the standard ``dataSource`` facet that carries the physical URI.

    Attributes:
        name: Data source name
        source_type: Type of data source (file, bigquery, snowflake, etc.)
        timestamp_field: Name of the timestamp field
        created_timestamp_field: Name of the created timestamp field
        field_mapping: Mapping from source fields to feature names
        description: Human-readable description
        tags: Key-value tags
    """

    name: str = attr.field()
    source_type: str = attr.field()
    timestamp_field: Optional[str] = attr.field(default=None)
    created_timestamp_field: Optional[str] = attr.field(default=None)
    field_mapping: Dict[str, str] = attr.field(factory=dict)
    description: str = attr.field(default="")
    tags: Dict[str, str] = attr.field(factory=dict)


@attr.define(kw_only=True)
class FeastEntityFacet(DatasetFacet):
    """
    Custom facet for Feast Entity metadata.

    Carried on the ``DatasetEvent`` that registers the entity node.

    Attributes:
        name: Entity name
        join_keys: List of join key column names
        value_type: Data type of the entity
        description: Human-readable description
        owner: Owner of the entity
        tags: Key-value tags
    """

    name: str = attr.field()
    join_keys: List[str] = attr.field(factory=list)
    value_type: str = attr.field(default="STRING")
    description: str = attr.field(default="")
    owner: str = attr.field(default="")
    tags: Dict[str, str] = attr.field(factory=dict)


@attr.define(kw_only=True)
class FeastProjectFacet(JobFacet):
    """
    Custom facet for Feast Project metadata.

    Carried as a **job** facet on the feature-view edge ``RunEvent``.

    Attributes:
        project_name: Name of the Feast project
        provider: Infrastructure provider (local, gcp, aws, etc.)
        online_store_type: Type of online store
        offline_store_type: Type of offline store
        registry_type: Type of registry (file, sql, http, etc.)
    """

    project_name: str = attr.field()
    provider: str = attr.field(default="local")
    online_store_type: str = attr.field(default="")
    offline_store_type: str = attr.field(default="")
    registry_type: str = attr.field(default="file")
