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
Emit OpenLineage events to the Metadata Bus on ``feast apply``.

Event model (metadata-bus-user-guide PR #21, EAPC-22333)
--------------------------------------------------------
Scope is deliberately narrow: **one ``DatasetEvent`` per FeatureView**, and
nothing else. No standalone source / entity / feature-service nodes, and no
``RunEvent`` edges -- the consumer (the EGDL OpenLineage -> OpenMetadata sync
app) draws every lineage edge by resolving the facets on the FeatureView node:

* ``dataSource`` + ``datasetType`` -> the primary upstream (a lake ``egdl://``
  table for a batch view, an ``egsp://`` topic for a stream view).
* ``feast_batchSource`` -> the mandatory second upstream of a stream view.
* ``feast_featureView.entities`` cross-referenced with ``schema.fields`` -> the
  join-key columns.

Standard concerns ride on standard OpenLineage facets (``documentation``,
``ownership``, ``tags``, ``schema``); only the Feast-specific residue lives in
the ``feast_*`` custom facets. This diverges from upstream feast-dev/feast, which
emits ``RunEvent``s and embeds descriptive facets on datasets inside the run.
"""

import logging
from typing import Any, List, Optional

from feast.openlineage import mappers
from feast.openlineage.client import FeastOpenLineageClient
from feast.openlineage.config import OpenLineageConfig

try:
    from openlineage.client.facet_v2 import (
        dataset_type_dataset,
        datasource_dataset,
        documentation_dataset,
        lifecycle_state_change_dataset,
        ownership_dataset,
        schema_dataset,
        tags_dataset,
    )

    OPENLINEAGE_AVAILABLE = True
except ImportError:
    OPENLINEAGE_AVAILABLE = False

logger = logging.getLogger(__name__)

# Standard OwnershipDatasetFacet owner type for a Feast owner string.
_OWNER_TYPE = "maintainer"


class FeastOpenLineageEmitter:
    """High-level Feast-apply -> Metadata Bus OpenLineage emitter."""

    def __init__(self, config: Optional[OpenLineageConfig] = None):
        self._config = config or OpenLineageConfig()
        self._client = FeastOpenLineageClient(self._config)

    @property
    def is_enabled(self) -> bool:
        return (
            self._client.is_enabled
            and self._config.emit_on_apply
            and self._config.namespace is not None
        )

    def emit_apply(self, objects: List[Any], project: str) -> None:
        """Emit one ``DatasetEvent`` per FeatureView in ``objects``."""
        if not self.is_enabled:
            if self._client.is_enabled and self._config.namespace is None:
                logger.warning(
                    "OpenLineage is enabled but the deployment environment could not "
                    "be resolved (set REGISTRY_ENV, CONTROL_PLANE_ENVIRONMENT, or "
                    "openlineage.environment in feature_store.yaml). Skipping emission."
                )
            return

        from feast import FeatureView

        namespace = self._config.namespace
        assert namespace is not None  # guarded by is_enabled

        feature_views = [obj for obj in objects if isinstance(obj, FeatureView)]
        for fv in feature_views:
            self._register_feature_view(namespace, project, fv)

    # ------------------------------------------------------------------ facets

    def _lifecycle_create_facet(self):
        return lifecycle_state_change_dataset.LifecycleStateChangeDatasetFacet(
            lifecycleStateChange="CREATE"
        )

    def _standard_facets(self, fv: Any) -> dict:
        """Standard OpenLineage facets built from FeatureView metadata."""
        facets: dict = {}

        description = getattr(fv, "description", None)
        if description:
            facets["documentation"] = documentation_dataset.DocumentationDatasetFacet(
                description=description
            )

        owner_names = mappers.owners(fv)
        if owner_names:
            facets["ownership"] = ownership_dataset.OwnershipDatasetFacet(
                owners=[
                    ownership_dataset.Owner(name=name, type=_OWNER_TYPE)
                    for name in owner_names
                ]
            )

        tag_pairs = mappers.tag_pairs(
            fv, default_data_product=self._config.default_data_product
        )
        if tag_pairs:
            facets["tags"] = tags_dataset.TagsDatasetFacet(
                tags=[
                    tags_dataset.TagsDatasetFacetFields(key=key, value=value)
                    for key, value in tag_pairs
                ]
            )

        fields = mappers.schema_fields(fv)
        if fields:
            facets["schema"] = schema_dataset.SchemaDatasetFacet(
                fields=[
                    schema_dataset.SchemaDatasetFacetFields(
                        name=name,
                        type=ftype or None,
                        # Always emit a description string ("" when the feature has
                        # none) rather than dropping the key -- EGDL asked for an
                        # empty string over an absent field (Deepak Jain, EAPC-22333).
                        description=fdesc or "",
                        # The OL client defaults the nested-struct `fields` to `[]`
                        # via attr factory=list, which then serializes as an empty
                        # array on every column. Feast columns are flat, so pin it to
                        # None to drop the key entirely (EGDL request, EAPC-22333).
                        fields=None,
                    )
                    for name, ftype, fdesc in fields
                ]
            )
        return facets

    def _source_facets(self, fv: Any) -> dict:
        """``dataSource`` + ``datasetType`` (+ stream/batch custom facets)."""
        env = self._config.resolve_environment()

        ds_type, ds_subtype = mappers.dataset_type(fv)
        facets: dict = {
            "datasetType": dataset_type_dataset.DatasetTypeDatasetFacet(
                datasetType=ds_type, subType=ds_subtype
            ),
        }

        if mappers.is_stream_backed(fv):
            stream = fv.stream_source
            facets["dataSource"] = datasource_dataset.DatasourceDatasetFacet(
                name=mappers.source_display_name(stream),
                uri=mappers.stream_source_uri(stream),
            )
            facets["feast_streamSource"] = mappers.build_stream_source_facet(stream)
            # KafkaSource mandates a batch_source; keep its historical-retrieval
            # upstream from being lost (the standard dataSource slot is taken).
            batch = getattr(fv, "batch_source", None)
            if batch is not None and getattr(batch, "name", None):
                facets["feast_batchSource"] = mappers.build_batch_source_facet(
                    batch, env=env
                )
        else:
            batch = fv.batch_source
            facets["dataSource"] = datasource_dataset.DatasourceDatasetFacet(
                name=mappers.source_display_name(batch),
                uri=mappers.batch_source_uri(batch, env=env),
            )
        return facets

    def _register_feature_view(self, namespace: str, project: str, fv: Any) -> None:
        facets = {
            "lifecycleStateChange": self._lifecycle_create_facet(),
            "feast_featureView": mappers.build_feature_view_facet(fv),
        }
        facets.update(self._standard_facets(fv))
        facets.update(self._source_facets(fv))
        self._client.emit_dataset_event(
            namespace=namespace,
            name=mappers.dataset_name(project, fv.name),
            facets=facets,
        )

    def close(self) -> None:
        self._client.close()
