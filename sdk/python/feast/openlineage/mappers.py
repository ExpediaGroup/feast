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

Naming contract (metadata-bus-user-guide PR #20/#21):
  * namespace: ``mlp://mlpfs-{env}`` (shared across a project's assets)
  * dataset name: ``{project}.{asset_name}`` (project is the leading segment)
"""

from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from feast import Entity, FeatureService, FeatureView
    from feast.data_source import DataSource


def dataset_name(project: str, asset_name: str) -> str:
    """Project-qualified OpenLineage dataset name, e.g. ``proj.hotel_price_features``."""
    return f"{project}.{asset_name}"


def data_source_uri(data_source: "DataSource") -> str:
    """
    Best-effort physical URI for a data source.

    Mirrors upstream resolution: path -> table:// -> query:// -> feast://.
    This is what lets OpenMetadata connect the batch-source node to the
    underlying data-lake asset.
    """
    path = getattr(data_source, "path", None)
    if path:
        return str(path)
    table = getattr(data_source, "table", None)
    if table:
        return f"table://{table}"
    query = getattr(data_source, "query", None)
    if query:
        return f"query://{hash(query)}"
    return f"feast://{data_source.name or 'unnamed'}"


def build_feature_view_facet(feature_view: "FeatureView") -> Any:
    """Build the ``feast_featureView`` dataset facet."""
    from feast.openlineage.facets import FeastFeatureViewFacet

    ttl_seconds = 0
    ttl = getattr(feature_view, "ttl", None)
    if ttl:
        ttl_seconds = int(ttl.total_seconds())

    mode = getattr(feature_view, "mode", None)

    return FeastFeatureViewFacet(
        name=feature_view.name,
        ttl_seconds=ttl_seconds,
        entities=list(feature_view.entities) if feature_view.entities else [],
        features=[f.name for f in feature_view.features]
        if feature_view.features
        else [],
        online_enabled=bool(getattr(feature_view, "online", True)),
        offline_enabled=bool(getattr(feature_view, "offline", False)),
        mode=str(mode) if mode else None,
        description=feature_view.description or "",
        owner=feature_view.owner or "",
        tags=dict(feature_view.tags) if feature_view.tags else {},
    )


def build_feature_service_facet(
    feature_service: "FeatureService",
    feature_views_by_name: Dict[str, "FeatureView"],
) -> Any:
    """Build the ``feast_featureService`` dataset facet."""
    from feast.openlineage.facets import FeastFeatureServiceFacet

    fv_names = [proj.name for proj in feature_service.feature_view_projections]

    # An empty projection feature list means "all features from that view".
    total_features = 0
    for proj in feature_service.feature_view_projections:
        if proj.features:
            total_features += len(proj.features)
        elif proj.name in feature_views_by_name:
            fv = feature_views_by_name[proj.name]
            if getattr(fv, "features", None):
                total_features += len(fv.features)

    return FeastFeatureServiceFacet(
        name=feature_service.name,
        feature_views=fv_names,
        feature_count=total_features,
        description=feature_service.description or "",
        owner=feature_service.owner or "",
        tags=dict(feature_service.tags) if feature_service.tags else {},
        logging_enabled=getattr(feature_service, "logging", None) is not None,
    )


def build_data_source_facet(data_source: "DataSource") -> Any:
    """Build the ``feast_dataSource`` dataset facet."""
    from feast.openlineage.facets import FeastDataSourceFacet

    return FeastDataSourceFacet(
        name=data_source.name or f"unnamed_{type(data_source).__name__}",
        source_type=type(data_source).__name__,
        timestamp_field=getattr(data_source, "timestamp_field", None) or None,
        created_timestamp_field=getattr(data_source, "created_timestamp_column", None)
        or None,
        field_mapping=dict(getattr(data_source, "field_mapping", {}) or {}),
        description=getattr(data_source, "description", "") or "",
        tags=dict(getattr(data_source, "tags", {}) or {}),
    )


def build_entity_facet(entity: "Entity") -> Any:
    """Build the ``feast_entity`` dataset facet."""
    from feast.openlineage.facets import FeastEntityFacet

    return FeastEntityFacet(
        name=entity.name,
        join_keys=[entity.join_key] if getattr(entity, "join_key", None) else [],
        value_type=str(entity.value_type)
        if getattr(entity, "value_type", None)
        else "STRING",
        description=entity.description or "",
        owner=getattr(entity, "owner", "") or "",
        tags=dict(entity.tags) if entity.tags else {},
    )


def build_project_facet(
    project: str,
    provider: str = "local",
    online_store_type: str = "",
    offline_store_type: str = "",
    registry_type: str = "file",
) -> Any:
    """Build the ``feast_project`` job facet."""
    from feast.openlineage.facets import FeastProjectFacet

    return FeastProjectFacet(
        project_name=project,
        provider=provider,
        online_store_type=online_store_type,
        offline_store_type=offline_store_type,
        registry_type=registry_type,
    )


def source_display_name(data_source: "DataSource") -> str:
    """Bare source name used in both the dataset name and facet ``name`` field."""
    return data_source.name or f"unnamed_{type(data_source).__name__}"


def timestamp_field(data_source: "DataSource") -> Optional[str]:
    return getattr(data_source, "timestamp_field", None) or None
