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
"""In-process ListAuditLogs RPC tests: RegistryServer handler and RemoteRegistry."""

import tempfile
from unittest.mock import MagicMock

import pytest

from feast.entity import Entity
from feast.errors import FeastBadRequest
from feast.infra.registry.remote import RemoteRegistry
from feast.infra.registry.sql import SqlRegistry, SqlRegistryConfig
from feast.project import Project
from feast.protos.feast.registry.RegistryServer_pb2 import (
    ListAuditLogsRequest,
    ListAuditLogsResponse,
    ObjectAuditLogEntry,
    PaginationMetadata,
)
from feast.registry_server import RegistryServer
from feast.value_type import ValueType


@pytest.fixture
def sql_registry_audit():
    fd, registry_path = tempfile.mkstemp()
    registry_config = SqlRegistryConfig(
        registry_type="sql",
        path=f"sqlite:///{registry_path}",
        purge_feast_metadata=False,
        enable_object_audit_log=True,
    )
    registry = SqlRegistry(registry_config, "test_project", None)
    registry.apply_project(Project(name="test_project"))
    registry.apply_entity(
        Entity(
            name="test_entity",
            join_keys=["id"],
            value_type=ValueType.STRING,
        ),
        "test_project",
    )
    yield registry
    registry.teardown()


class TestListAuditLogsServerHandler:
    def test_handler_returns_project_rows_and_pagination(self, sql_registry_audit):
        server = RegistryServer(sql_registry_audit)
        request = ListAuditLogsRequest(project="test_project")
        request.pagination.page = 1
        request.pagination.limit = 10

        response = server.ListAuditLogs(request, None)

        assert response.pagination.total_count >= 1
        assert response.pagination.page == 1
        assert response.pagination.limit == 10
        assert all(entry.project == "test_project" for entry in response.audit_logs)
        assert any(entry.object_name == "test_entity" for entry in response.audit_logs)

    def test_handler_paginates(self, sql_registry_audit):
        sql_registry_audit.apply_entity(
            Entity(name="second", join_keys=["id"], value_type=ValueType.STRING),
            "test_project",
        )
        server = RegistryServer(sql_registry_audit)
        request = ListAuditLogsRequest(project="test_project")
        request.pagination.page = 1
        request.pagination.limit = 1

        response = server.ListAuditLogs(request, None)

        assert len(response.audit_logs) == 1
        assert response.pagination.total_count >= 2
        assert response.pagination.has_next is True

    def test_handler_rejects_empty_project(self, sql_registry_audit):
        server = RegistryServer(sql_registry_audit)
        with pytest.raises(FeastBadRequest, match="project is required"):
            server.ListAuditLogs(ListAuditLogsRequest(project=""), None)


class TestRemoteRegistryListAuditLogs:
    def test_client_builds_request_and_parses_response(self):
        remote_registry = RemoteRegistry.__new__(RemoteRegistry)
        entry = ObjectAuditLogEntry(
            id=1,
            project="test_project",
            object_name="test_entity",
            operation="create",
        )
        pagination = PaginationMetadata(page=1, limit=50, total_count=1, total_pages=1)
        mock_stub = MagicMock()
        mock_stub.ListAuditLogs.return_value = ListAuditLogsResponse(
            audit_logs=[entry], pagination=pagination
        )
        remote_registry.stub = mock_stub

        entries, meta = remote_registry.list_object_audit_logs(
            project="test_project", page=1, limit=50, include_proto=False
        )

        request = mock_stub.ListAuditLogs.call_args.args[0]
        assert request.project == "test_project"
        assert request.pagination.page == 1
        assert request.pagination.limit == 50
        assert request.include_proto is False
        assert len(entries) == 1
        assert entries[0].object_name == "test_entity"
        assert meta.total_count == 1
