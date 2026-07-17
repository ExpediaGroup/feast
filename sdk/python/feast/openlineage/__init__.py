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
OpenLineage integration for the ML Platform Feature Store (EG Feast fork).

Emits OpenLineage ``DatasetEvent`` (node registration) and ``RunEvent`` (lineage
edge) events to the Metadata Bus when ``feast apply`` runs, so feature-store
assets become discoverable in OpenMetadata. Enabled via the ``openlineage`` block
in ``feature_store.yaml``. See ``README.md`` in this package for configuration.
"""

from feast.openlineage.client import FeastOpenLineageClient
from feast.openlineage.config import OpenLineageConfig
from feast.openlineage.emitter import FeastOpenLineageEmitter
from feast.openlineage.facets import (
    FeastDataSourceFacet,
    FeastEntityFacet,
    FeastFeatureServiceFacet,
    FeastFeatureViewFacet,
    FeastProjectFacet,
)

__all__ = [
    "FeastOpenLineageClient",
    "FeastOpenLineageEmitter",
    "OpenLineageConfig",
    "FeastFeatureViewFacet",
    "FeastFeatureServiceFacet",
    "FeastDataSourceFacet",
    "FeastEntityFacet",
    "FeastProjectFacet",
]
