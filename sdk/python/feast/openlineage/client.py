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
Thin wrapper around the OpenLineage client for Feast -> Metadata Bus emission.

Scope is intentionally narrow: emit ``DatasetEvent`` (node registration) and
``RunEvent`` (lineage edge) events. The upstream feast-dev consumer/processor
path is not ported.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from feast.openlineage.config import OpenLineageConfig

try:
    from openlineage.client import OpenLineageClient
    from openlineage.client.event_v2 import (
        DatasetEvent,
        Job,
        Run,
        RunEvent,
        RunState,
        StaticDataset,
        set_producer,
    )

    OPENLINEAGE_AVAILABLE = True
except ImportError:
    OPENLINEAGE_AVAILABLE = False
    OpenLineageClient = None  # type: ignore[misc,assignment]
    RunState = None  # type: ignore[misc,assignment]

logger = logging.getLogger(__name__)


class FeastOpenLineageClient:
    """OpenLineage client wrapper for Feast Feature Store apply-time emission."""

    def __init__(self, config: Optional[OpenLineageConfig] = None):
        self._config = config or OpenLineageConfig()

        if not OPENLINEAGE_AVAILABLE:
            logger.warning(
                "OpenLineage is not installed. Lineage events will not be emitted. "
                "Install with: pip install 'feast[openlineage]'"
            )
            self._client = None
            return

        if not self._config.enabled:
            logger.info("OpenLineage integration is disabled")
            self._client = None
            return

        # Producer applies to both the event-level `producer` and every facet
        # `_producer`; the Metadata Bus expects the EG Feast fork URI here.
        set_producer(self._config.producer)

        try:
            transport_config = self._config.get_transport_config()
            if transport_config is None:
                self._client = OpenLineageClient()
            else:
                self._client = OpenLineageClient(config={"transport": transport_config})
            logger.info(
                "OpenLineage client initialized with %s transport",
                self._config.transport_type or "default",
            )
        except Exception as e:
            logger.error("Failed to initialize OpenLineage client: %s", e)
            self._client = None

    @property
    def is_enabled(self) -> bool:
        return self._client is not None and self._config.enabled

    @property
    def config(self) -> OpenLineageConfig:
        return self._config

    def emit(self, event: Any) -> bool:
        """Emit a single OpenLineage event. Non-fatal on error."""
        if not self.is_enabled or self._client is None:
            logger.debug("OpenLineage disabled, skipping event emission")
            return False
        try:
            self._client.emit(event)
            return True
        except Exception as e:
            logger.error("Failed to emit OpenLineage event: %s", e)
            return False

    def emit_dataset_event(
        self,
        namespace: str,
        name: str,
        facets: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Emit a ``DatasetEvent`` that registers a catalog node."""
        if not self.is_enabled:
            return False
        try:
            event = DatasetEvent(
                eventTime=datetime.now(timezone.utc).isoformat(),
                dataset=StaticDataset(
                    namespace=namespace,
                    name=name,
                    facets=facets or {},
                ),
            )
            return self.emit(event)
        except Exception as e:
            logger.error("Failed to create DatasetEvent for %s: %s", name, e)
            return False

    def emit_run_event(
        self,
        namespace: str,
        job_name: str,
        run_id: str,
        event_type: "RunState",
        inputs: Optional[List[Any]] = None,
        outputs: Optional[List[Any]] = None,
        job_facets: Optional[Dict[str, Any]] = None,
        run_facets: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Emit a ``RunEvent`` that draws a lineage edge between existing nodes."""
        if not self.is_enabled:
            return False
        try:
            event = RunEvent(
                eventTime=datetime.now(timezone.utc).isoformat(),
                eventType=event_type,
                run=Run(runId=run_id, facets=run_facets or {}),
                job=Job(namespace=namespace, name=job_name, facets=job_facets or {}),
                inputs=inputs or [],
                outputs=outputs or [],
            )
            return self.emit(event)
        except Exception as e:
            logger.error("Failed to create RunEvent for %s: %s", job_name, e)
            return False

    def close(self, timeout: float = 5.0) -> bool:
        if self._client is not None:
            try:
                return self._client.close(timeout)
            except Exception as e:
                logger.error("Error closing OpenLineage client: %s", e)
                return False
        return True
