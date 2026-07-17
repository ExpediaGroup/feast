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
OpenMetadata requires both endpoints of a lineage edge to exist as catalog
entities *before* the edge can be drawn. Feast satisfies this in two layers:

1. **Nodes** -- one ``DatasetEvent`` per batch source, entity, feature view and
   feature service, each carrying ``lifecycleStateChange: CREATE`` plus its
   descriptive ``feast_*`` dataset facet.
2. **Edges** -- ``RunEvent`` (``COMPLETE``) per feature view
   (``feature_view_{project}.{name}``: batch source + entities -> view) and per
   feature service (``feature_service_{project}.{name}``: views -> service),
   referencing inputs/outputs by **identity only**.

This diverges from upstream feast-dev/feast, which emits ``RunEvent``s only and
embeds the descriptive facets on the datasets *inside* the run.
"""

import logging
import uuid
from typing import Any, Dict, List, Optional

from feast.openlineage import mappers
from feast.openlineage.client import FeastOpenLineageClient
from feast.openlineage.config import OpenLineageConfig

try:
    from openlineage.client.event_v2 import (
        InputDataset,
        OutputDataset,
        RunState,
    )
    from openlineage.client.facet_v2 import (
        datasource_dataset,
        job_type_job,
        lifecycle_state_change_dataset,
    )

    OPENLINEAGE_AVAILABLE = True
except ImportError:
    OPENLINEAGE_AVAILABLE = False

logger = logging.getLogger(__name__)

_DUMMY_ENTITY = "__dummy"


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

    def _lifecycle_create_facet(self):
        return lifecycle_state_change_dataset.LifecycleStateChangeDatasetFacet(
            lifecycleStateChange="CREATE"
        )

    def emit_apply(
        self,
        objects: List[Any],
        project: str,
        project_metadata: Optional[Dict[str, str]] = None,
    ) -> None:
        """Register nodes then draw edges for the applied objects."""
        if not self.is_enabled:
            if self._client.is_enabled and self._config.namespace is None:
                logger.warning(
                    "OpenLineage is enabled but the deployment environment could not "
                    "be resolved (set REGISTRY_ENV, CONTROL_PLANE_ENVIRONMENT, or "
                    "openlineage.environment in feature_store.yaml). Skipping emission."
                )
            return

        from feast import Entity, FeatureService, FeatureView

        namespace = self._config.namespace
        assert namespace is not None  # guarded by is_enabled

        feature_views: List[Any] = []
        feature_services: List[Any] = []
        entities: List[Any] = []
        data_sources: Dict[str, Any] = {}

        for obj in objects:
            if isinstance(obj, FeatureView):
                feature_views.append(obj)
            elif isinstance(obj, FeatureService):
                feature_services.append(obj)
            elif isinstance(obj, Entity):
                if obj.name and obj.name != _DUMMY_ENTITY:
                    entities.append(obj)
            else:
                # Explicit DataSource objects (duck-typed: have a `name`).
                from feast.data_source import DataSource

                if isinstance(obj, DataSource) and obj.name:
                    data_sources[obj.name] = obj

        # Collect batch/stream sources referenced by the feature views.
        for fv in feature_views:
            for attr in ("batch_source", "stream_source"):
                src = getattr(fv, attr, None)
                if src is not None and getattr(src, "name", None):
                    data_sources.setdefault(src.name, src)

        feature_views_by_name = {fv.name: fv for fv in feature_views}

        # --- 1. Register nodes (DatasetEvent per asset) -------------------
        for src in data_sources.values():
            self._register_data_source(namespace, project, src)
        for entity in entities:
            self._register_entity(namespace, project, entity)
        for fv in feature_views:
            self._register_feature_view(namespace, project, fv)
        for fs in feature_services:
            self._register_feature_service(
                namespace, project, fs, feature_views_by_name
            )

        # --- 2. Draw edges (RunEvent COMPLETE) ----------------------------
        for fv in feature_views:
            self._emit_feature_view_edge(namespace, project, fv, project_metadata)
        for fs in feature_services:
            self._emit_feature_service_edge(namespace, project, fs)

    # ------------------------------------------------------------------ nodes

    def _register_data_source(self, namespace: str, project: str, src: Any) -> None:
        name = mappers.source_display_name(src)
        facets = {
            "lifecycleStateChange": self._lifecycle_create_facet(),
            "dataSource": datasource_dataset.DatasourceDatasetFacet(
                name=name,
                uri=mappers.data_source_uri(src),
            ),
            "feast_dataSource": mappers.build_data_source_facet(src),
        }
        self._client.emit_dataset_event(
            namespace=namespace,
            name=mappers.dataset_name(project, name),
            facets=facets,
        )

    def _register_entity(self, namespace: str, project: str, entity: Any) -> None:
        facets = {
            "lifecycleStateChange": self._lifecycle_create_facet(),
            "feast_entity": mappers.build_entity_facet(entity),
        }
        self._client.emit_dataset_event(
            namespace=namespace,
            name=mappers.dataset_name(project, entity.name),
            facets=facets,
        )

    def _register_feature_view(self, namespace: str, project: str, fv: Any) -> None:
        facets = {
            "lifecycleStateChange": self._lifecycle_create_facet(),
            "feast_featureView": mappers.build_feature_view_facet(fv),
        }
        self._client.emit_dataset_event(
            namespace=namespace,
            name=mappers.dataset_name(project, fv.name),
            facets=facets,
        )

    def _register_feature_service(
        self,
        namespace: str,
        project: str,
        fs: Any,
        feature_views_by_name: Dict[str, Any],
    ) -> None:
        facets = {
            "lifecycleStateChange": self._lifecycle_create_facet(),
            "feast_featureService": mappers.build_feature_service_facet(
                fs, feature_views_by_name
            ),
        }
        self._client.emit_dataset_event(
            namespace=namespace,
            name=mappers.dataset_name(project, fs.name),
            facets=facets,
        )

    # ------------------------------------------------------------------ edges

    def _job_type_facet(self, job_type: str = "APPLICATION"):
        return job_type_job.JobTypeJobFacet(
            processingType="BATCH",
            integration="FEAST",
            jobType=job_type,
        )

    def _emit_feature_view_edge(
        self,
        namespace: str,
        project: str,
        fv: Any,
        project_metadata: Optional[Dict[str, str]],
    ) -> None:
        inputs: List[Any] = []
        for attr in ("batch_source", "stream_source"):
            src = getattr(fv, attr, None)
            if src is not None and getattr(src, "name", None):
                inputs.append(
                    InputDataset(
                        namespace=namespace,
                        name=mappers.dataset_name(
                            project, mappers.source_display_name(src)
                        ),
                    )
                )
        for entity_name in fv.entities or []:
            if entity_name and entity_name != _DUMMY_ENTITY:
                inputs.append(
                    InputDataset(
                        namespace=namespace,
                        name=mappers.dataset_name(project, entity_name),
                    )
                )

        outputs = [
            OutputDataset(
                namespace=namespace,
                name=mappers.dataset_name(project, fv.name),
            )
        ]

        meta = project_metadata or {}
        job_facets = {
            "jobType": self._job_type_facet(),
            "feast_project": mappers.build_project_facet(
                project=project,
                provider=meta.get("provider", "local"),
                online_store_type=meta.get("online_store_type", ""),
                offline_store_type=meta.get("offline_store_type", ""),
                registry_type=meta.get("registry_type", "file"),
            ),
        }

        self._client.emit_run_event(
            namespace=namespace,
            job_name=f"feature_view_{mappers.dataset_name(project, fv.name)}",
            run_id=str(uuid.uuid4()),
            event_type=RunState.COMPLETE,
            inputs=inputs,
            outputs=outputs,
            job_facets=job_facets,
        )

    def _emit_feature_service_edge(self, namespace: str, project: str, fs: Any) -> None:
        inputs = [
            InputDataset(
                namespace=namespace,
                name=mappers.dataset_name(project, proj.name),
            )
            for proj in fs.feature_view_projections
        ]
        outputs = [
            OutputDataset(
                namespace=namespace,
                name=mappers.dataset_name(project, fs.name),
            )
        ]
        self._client.emit_run_event(
            namespace=namespace,
            job_name=f"feature_service_{mappers.dataset_name(project, fs.name)}",
            run_id=str(uuid.uuid4()),
            event_type=RunState.COMPLETE,
            inputs=inputs,
            outputs=outputs,
            job_facets={"jobType": self._job_type_facet()},
        )

    def close(self) -> None:
        self._client.close()
