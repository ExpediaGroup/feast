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
Configuration for the Feast -> Metadata Bus OpenLineage integration.

This is the runtime (dataclass) config consumed by the emitter/client. The
pydantic mirror read from ``feature_store.yaml`` lives in
``feast.repo_config.OpenLineageConfig`` and is converted here via
``to_openlineage_config()``.
"""

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# The EG Feast fork is the producer of every feast-apply event, per the
# Metadata Bus feature-store schema (EAPC-22333 / metadata-bus-user-guide PR #21).
DEFAULT_PRODUCER = "https://github.com/ExpediaGroup/feast"

# Environment variables that identify the deployment environment, in resolution
# order. Both build the Feast registry host in their respective apply paths, so
# keying the namespace on the same variable keeps emitted lineage consistent
# with the registry the assets are written to:
#   * REGISTRY_ENV               -> GitHub Actions register-features path (test/corp/dw/prod)
#   * CONTROL_PLANE_ENVIRONMENT  -> Flyte feature-registration task (corp/dw)
ENV_RESOLUTION_ORDER = ("REGISTRY_ENV", "CONTROL_PLANE_ENVIRONMENT")

# Metadata Bus proxy endpoints (documented for convenience; transport is
# configured explicitly via feature_store.yaml):
#   test: https://metadata-bus-proxy.rcp.us-east-1.data.test.exp-aws.net
#   prod: https://metadata-bus-proxy.rcp.us-east-1.data.dw.exp-aws.net
# with endpoint "api/v1/lineage".
DEFAULT_TRANSPORT_ENDPOINT = "api/v1/lineage"


@dataclass
class OpenLineageConfig:
    """Runtime configuration for OpenLineage emission on ``feast apply``."""

    enabled: bool = False
    transport_type: Optional[str] = None
    transport_url: Optional[str] = None
    transport_endpoint: str = DEFAULT_TRANSPORT_ENDPOINT
    api_key: Optional[str] = None
    # Explicit override for the namespace env segment. When unset, resolved from
    # REGISTRY_ENV then CONTROL_PLANE_ENVIRONMENT (see ENV_RESOLUTION_ORDER).
    environment: Optional[str] = None
    producer: str = DEFAULT_PRODUCER
    emit_on_apply: bool = True
    additional_config: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "OpenLineageConfig":
        """Build a config from FEAST_OPENLINEAGE_* environment variables."""
        return cls(
            enabled=os.getenv("FEAST_OPENLINEAGE_ENABLED", "false").lower() == "true",
            transport_type=os.getenv("FEAST_OPENLINEAGE_TRANSPORT_TYPE"),
            transport_url=os.getenv("FEAST_OPENLINEAGE_URL"),
            transport_endpoint=os.getenv(
                "FEAST_OPENLINEAGE_ENDPOINT", DEFAULT_TRANSPORT_ENDPOINT
            ),
            api_key=os.getenv("FEAST_OPENLINEAGE_API_KEY"),
            environment=os.getenv("FEAST_OPENLINEAGE_ENV"),
            producer=os.getenv("FEAST_OPENLINEAGE_PRODUCER", DEFAULT_PRODUCER),
            emit_on_apply=os.getenv("FEAST_OPENLINEAGE_EMIT_ON_APPLY", "true").lower()
            == "true",
        )

    def resolve_environment(self) -> Optional[str]:
        """
        Resolve the namespace environment segment.

        Order: explicit ``environment`` override -> REGISTRY_ENV -> CONTROL_PLANE_ENVIRONMENT.
        Returns ``None`` if nothing is set (caller should skip emission).
        """
        if self.environment:
            return self.environment
        for var in ENV_RESOLUTION_ORDER:
            value = os.environ.get(var)
            if value:
                return value.strip()
        return None

    @property
    def namespace(self) -> Optional[str]:
        """The feature-store namespace ``mlp://mlpfs-{env}``, or None if unresolved."""
        env = self.resolve_environment()
        if not env:
            return None
        return f"mlp://mlpfs-{env}"

    def get_transport_config(self) -> Optional[Dict[str, Any]]:
        """
        Build the OpenLineage transport config dict.

        Returns ``None`` when ``transport_type`` is unset (the OpenLineage SDK
        falls back to its own defaults / OPENLINEAGE_* env vars).
        """
        if not self.transport_type:
            return None

        config: Dict[str, Any] = {"type": self.transport_type}

        if self.transport_type == "http":
            if not self.transport_url:
                raise ValueError(
                    "transport_url is required for http transport "
                    "(e.g. https://metadata-bus-proxy.rcp.us-east-1.data.test.exp-aws.net)"
                )
            config["url"] = self.transport_url
            config["endpoint"] = self.transport_endpoint
            if self.api_key:
                config["auth"] = {"type": "api_key", "apiKey": self.api_key}
        elif self.transport_type == "kafka":
            config["topic"] = self.additional_config.get("topic", "lineage-events")
        elif self.transport_type == "file":
            config["log_file_path"] = self.additional_config.get(
                "log_file_path", "openlineage_events.json"
            )

        # Allow arbitrary extra keys (merged last so they can override defaults).
        for key, value in self.additional_config.items():
            config.setdefault(key, value)

        return config
