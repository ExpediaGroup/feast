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
"""ReportFeatureServer / ListFeatureServers: server handlers and both clients."""

import json
import tempfile
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import httpx
import pytest

from feast.errors import (
    FeatureServerReportInvalid,
    FeatureServerReportsDisabled,
    ProjectObjectNotFoundException,
)
from feast.infra.registry import feature_server_reports as fsr
from feast.infra.registry.base_registry import BaseRegistry
from feast.infra.registry.feature_server_reports import (
    FeatureServerRecord,
    FeatureServerReport,
    FeatureServerUrl,
)
from feast.infra.registry.http import HttpRegistry
from feast.infra.registry.remote import RemoteRegistry
from feast.infra.registry.sql import SqlRegistry, SqlRegistryConfig
from feast.permissions.security_manager import (
    SecurityManager,
    no_security_manager,
    set_security_manager,
)
from feast.permissions.user import User
from feast.project import Project
from feast.protos.feast.registry.RegistryServer_pb2 import (
    ListFeatureServersRequest,
    ListFeatureServersResponse,
    ReportFeatureServerRequest,
)
from feast.registry_server import RegistryServer

NOW = datetime(2026, 11, 4, 12, 0, tzinfo=timezone.utc)


def _report(host: str = "app") -> FeatureServerReport:
    return FeatureServerReport(
        contract=1,
        urls=[
            FeatureServerUrl(url=f"grpcs://{host}.example.com:443", scope="external")
        ],
        version="v1",
        registry="grpc",
        labels={"app": host},
    )


@pytest.fixture
def sql_registry_reports():
    fd, registry_path = tempfile.mkstemp()
    registry = SqlRegistry(
        SqlRegistryConfig(
            registry_type="sql",
            path=f"sqlite:///{registry_path}",
            purge_feast_metadata=False,
            enable_feature_server_reports=True,
        ),
        "test_project",
        None,
    )
    registry.apply_project(Project(name="test_project"))
    yield registry
    registry.teardown()


@pytest.fixture
def no_sm():
    no_security_manager()
    yield
    no_security_manager()


class TestReportFeatureServerHandler:
    def test_unverified_without_current_user(self, sql_registry_reports, no_sm):
        server = RegistryServer(sql_registry_reports)
        request = ReportFeatureServerRequest(
            project="test_project", report=fsr.report_to_proto(_report())
        )
        server.ReportFeatureServer(request, None)

        records = sql_registry_reports.list_feature_servers("test_project")
        assert len(records) == 1
        assert records[0].verified is False
        assert records[0].identity is None

    def test_verified_with_current_user(self, sql_registry_reports):
        sm = SecurityManager(
            project="test_project", registry=sql_registry_reports, write_auth_only=True
        )
        set_security_manager(sm)
        token = sm.set_current_user(User(username="svc-app", roles=[]))
        try:
            server = RegistryServer(sql_registry_reports)
            server.ReportFeatureServer(
                ReportFeatureServerRequest(
                    project="test_project", report=fsr.report_to_proto(_report())
                ),
                None,
            )
        finally:
            sm.reset_request_context(token, None)
            no_security_manager()
        record = sql_registry_reports.list_feature_servers("test_project")[0]
        assert record.verified is True
        assert record.identity == "svc-app"

    def test_security_manager_without_user_is_unverified(self, sql_registry_reports):
        set_security_manager(
            SecurityManager(
                project="test_project",
                registry=sql_registry_reports,
                write_auth_only=True,
            )
        )
        try:
            server = RegistryServer(sql_registry_reports)
            server.ReportFeatureServer(
                ReportFeatureServerRequest(
                    project="test_project", report=fsr.report_to_proto(_report())
                ),
                None,
            )
        finally:
            no_security_manager()
        assert (
            sql_registry_reports.list_feature_servers("test_project")[0].verified
            is False
        )

    def test_errors_propagate(self, sql_registry_reports, no_sm):
        server = RegistryServer(sql_registry_reports)
        with pytest.raises(ProjectObjectNotFoundException):
            server.ReportFeatureServer(
                ReportFeatureServerRequest(
                    project="ghost", report=fsr.report_to_proto(_report())
                ),
                None,
            )
        # A request without a report has contract 0.
        with pytest.raises(FeatureServerReportInvalid):
            server.ReportFeatureServer(
                ReportFeatureServerRequest(project="test_project"), None
            )
        sql_registry_reports.registry_config.enable_feature_server_reports = False
        with pytest.raises(FeatureServerReportsDisabled):
            server.ReportFeatureServer(
                ReportFeatureServerRequest(
                    project="test_project", report=fsr.report_to_proto(_report())
                ),
                None,
            )


class TestListFeatureServersHandler:
    def test_lists_records_as_protos(self, sql_registry_reports, no_sm):
        sql_registry_reports.report_feature_server("test_project", _report("b"))
        sql_registry_reports.report_feature_server("test_project", _report("a"))
        server = RegistryServer(sql_registry_reports)
        response = server.ListFeatureServers(
            ListFeatureServersRequest(project="test_project"), None
        )
        assert len(response.feature_servers) == 2
        keys = [r.key for r in response.feature_servers]
        assert keys == sorted(keys)
        assert {r.labels["app"] for r in response.feature_servers} == {"a", "b"}
        assert all(r.versions["v1"].seconds > 0 for r in response.feature_servers)

    def test_unknown_project_lists_nothing(self, sql_registry_reports, no_sm):
        server = RegistryServer(sql_registry_reports)
        response = server.ListFeatureServers(
            ListFeatureServersRequest(project="ghost"), None
        )
        assert list(response.feature_servers) == []


class TestBaseRegistryDefaults:
    def test_not_implemented(self):
        class Minimal(BaseRegistry):
            pass

        Minimal.__abstractmethods__ = frozenset()
        registry = Minimal()
        with pytest.raises(NotImplementedError):
            registry.report_feature_server("p", _report())
        with pytest.raises(NotImplementedError):
            registry.list_feature_servers("p")


class TestRemoteRegistryFeatureServers:
    def test_report_builds_request(self):
        remote_registry = RemoteRegistry.__new__(RemoteRegistry)
        remote_registry.stub = MagicMock()
        assert remote_registry.report_feature_server("test_project", _report()) is True
        request = remote_registry.stub.ReportFeatureServer.call_args.args[0]
        assert request.project == "test_project"
        assert fsr.report_from_proto(request.report) == _report()

    def test_list_parses_response(self):
        record = fsr.merge_report(None, _report(), NOW, verified=True, identity="x")
        remote_registry = RemoteRegistry.__new__(RemoteRegistry)
        remote_registry.stub = MagicMock()
        remote_registry.stub.ListFeatureServers.return_value = (
            ListFeatureServersResponse(feature_servers=[fsr.record_to_proto(record)])
        )
        records = remote_registry.list_feature_servers("test_project")
        request = remote_registry.stub.ListFeatureServers.call_args.args[0]
        assert request.project == "test_project"
        assert records == [record]


@pytest.fixture
def http_registry_factory():
    def make(handler):
        with patch.object(HttpRegistry, "__init__", lambda self, *a, **kw: None):
            registry = HttpRegistry.__new__(HttpRegistry)
        registry.base_url = "http://test-registry"
        registry.project = "test_project"
        registry.http_client = httpx.Client(transport=httpx.MockTransport(handler))
        return registry

    return make


class TestHttpRegistryFeatureServers:
    def test_report_posts_json_and_accepts_204(self, http_registry_factory):
        seen = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["method"] = request.method
            seen["url"] = str(request.url)
            seen["content_type"] = request.headers["content-type"]
            seen["body"] = json.loads(request.content)
            return httpx.Response(204)

        registry = http_registry_factory(handler)
        assert registry.report_feature_server("test_project", _report()) is True
        assert seen["method"] == "POST"
        assert (
            seen["url"] == "http://test-registry/projects/test_project/feature_servers"
        )
        assert seen["content_type"] == "application/json"
        assert FeatureServerReport.model_validate(seen["body"]) == _report()
        assert seen["body"]["contract"] == 1

    @pytest.mark.parametrize("status", [404, 413, 422, 503])
    def test_report_error_status_raises(self, http_registry_factory, status):
        registry = http_registry_factory(
            lambda request: httpx.Response(status, json={"detail": "x"})
        )
        with pytest.raises(httpx.HTTPStatusError) as exc:
            registry.report_feature_server("test_project", _report())
        assert exc.value.response.status_code == status

    def test_list_parses_iso_records(self, http_registry_factory):
        record = fsr.merge_report(None, _report("b"), NOW)
        other = fsr.merge_report(None, _report("a"), NOW, verified=True, identity="i")
        body = [json.loads(r.model_dump_json()) for r in (record, other)]

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.method == "GET"
            assert (
                str(request.url)
                == "http://test-registry/projects/test_project/feature_servers"
            )
            return httpx.Response(200, json=body)

        registry = http_registry_factory(handler)
        records = registry.list_feature_servers("test_project")
        assert records == sorted([record, other], key=lambda r: r.key)
        assert all(isinstance(r, FeatureServerRecord) for r in records)
        assert records[0].last_seen.tzinfo is not None

    def test_list_error_status_raises(self, http_registry_factory):
        registry = http_registry_factory(lambda request: httpx.Response(503))
        with pytest.raises(httpx.HTTPStatusError):
            registry.list_feature_servers("test_project")
