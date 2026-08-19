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
"""
Tests for the GetMaterializationIntervalHistory gRPC RPC: the server-side
handler (registry_server.py), invoked directly against a real SqlRegistry
without spinning up an actual gRPC transport, and the RemoteRegistry client
method with a mocked stub.
"""

import tempfile
from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.registry.remote import RemoteRegistry
from feast.infra.registry.sql import SqlRegistry, SqlRegistryConfig
from feast.protos.feast.registry.RegistryServer_pb2 import (
    GetMaterializationIntervalHistoryRequest,
    GetMaterializationIntervalHistoryResponse,
)
from feast.registry_server import RegistryServer
from feast.types import Float32
from feast.utils import _utc_now


@pytest.fixture
def sql_registry_with_history():
    fd, registry_path = tempfile.mkstemp()
    registry_config = SqlRegistryConfig(
        registry_type="sql",
        path=f"sqlite:///{registry_path}",
        purge_feast_metadata=False,
        materialization_intervals_max_len=3,
    )
    registry = SqlRegistry(registry_config, "test_project", None)

    entity = Entity(name="driver", description="Driver entity")
    registry.apply_entity(entity, "test_project")
    source = FileSource(path="some path", timestamp_field="event_timestamp")
    feature_view = FeatureView(
        name="driver_stats",
        entities=[entity],
        schema=[Field(name="conv_rate", dtype=Float32)],
        source=source,
        online=True,
    )
    registry.apply_feature_view(feature_view, "test_project")

    now = _utc_now()
    for i in range(5):
        registry.apply_materialization(
            feature_view,
            "test_project",
            now - timedelta(days=5 - i, hours=1),
            now - timedelta(days=5 - i),
        )

    yield registry
    registry.teardown()


class TestGetMaterializationIntervalHistoryServerHandler:
    def test_handler_returns_full_history_with_pagination_metadata(
        self, sql_registry_with_history
    ):
        server = RegistryServer(sql_registry_with_history)
        request = GetMaterializationIntervalHistoryRequest(
            feature_view_name="driver_stats", project="test_project"
        )
        request.pagination.page = 1
        request.pagination.limit = 10

        response = server.GetMaterializationIntervalHistory(request, None)

        assert len(response.entries) == 5
        assert response.pagination.total_count == 5
        assert all(
            entry.feature_view_name == "driver_stats" for entry in response.entries
        )

    def test_handler_paginates(self, sql_registry_with_history):
        server = RegistryServer(sql_registry_with_history)
        request = GetMaterializationIntervalHistoryRequest(
            feature_view_name="driver_stats", project="test_project"
        )
        request.pagination.page = 1
        request.pagination.limit = 2

        response = server.GetMaterializationIntervalHistory(request, None)

        assert len(response.entries) == 2
        assert response.pagination.total_count == 5
        assert response.pagination.total_pages == 3
        assert response.pagination.has_next is True

    def test_handler_raises_for_unknown_feature_view(self, sql_registry_with_history):
        from feast.errors import FeatureViewNotFoundException

        server = RegistryServer(sql_registry_with_history)
        request = GetMaterializationIntervalHistoryRequest(
            feature_view_name="does_not_exist", project="test_project"
        )
        # The permission check resolves the feature view first (same as
        # every other feature-view-scoped RPC), so an unknown feature view
        # surfaces as a not-found error rather than an empty result.
        with pytest.raises(FeatureViewNotFoundException):
            server.GetMaterializationIntervalHistory(request, None)


class TestRemoteRegistryGetMaterializationIntervalHistory:
    def test_client_builds_request_and_parses_response(self):
        remote_registry = RemoteRegistry.__new__(RemoteRegistry)
        fake_entry = GetMaterializationIntervalHistoryResponse().entries.add()
        fake_entry.feature_view_name = "driver_stats"
        fake_entry.project = "test_project"

        mock_stub = MagicMock()
        mock_stub.GetMaterializationIntervalHistory.return_value = (
            GetMaterializationIntervalHistoryResponse(entries=[fake_entry])
        )
        remote_registry.stub = mock_stub

        result = remote_registry.get_materialization_interval_history(
            feature_view_name="driver_stats", project="test_project"
        )

        assert len(result) == 1
        assert result[0].feature_view_name == "driver_stats"
        called_request = mock_stub.GetMaterializationIntervalHistory.call_args[0][0]
        assert called_request.feature_view_name == "driver_stats"
        assert called_request.project == "test_project"
