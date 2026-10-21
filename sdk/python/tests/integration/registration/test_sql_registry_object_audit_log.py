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
import gzip
import random
import string
from contextlib import contextmanager

import pytest
from pytest_lazyfixture import lazy_fixture
from sqlalchemy import func, inspect, select, text
from sqlalchemy.exc import DatabaseError, IntegrityError

from feast.entity import Entity
from feast.errors import EntityNotFoundException
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.registry.sql import (
    FeastMetadataKeys,
    ObjectAuditLogObjectType,
    ObjectAuditLogOperation,
    SqlRegistry,
    SqlRegistryConfig,
    feast_metadata,
    object_audit_log,
)
from feast.project import Project
from feast.protos.feast.core.Entity_pb2 import Entity as EntityProto
from feast.protos.feast.core.Project_pb2 import Project as ProjectProto
from feast.utils import _utc_now
from feast.value_type import ValueType
from tests.integration.registration.test_universal_registry import (
    _create_mysql_database,
    _create_pg_database,
)

_OBJECT_AUDIT_LOG_COLUMNS = {
    "id",
    "project_id",
    "project_uuid",
    "object_type",
    "object_name",
    "operation",
    "actor",
    "before_proto",
    "after_proto",
    "request_id",
    "recorded_at",
}
_OBJECT_AUDIT_LOG_NULLABLE = {
    "actor",
    "before_proto",
    "after_proto",
    "request_id",
}
_AUDIT_PROJECT_UUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
_PROJECT = "test_project"

sql_audit_fixtures = [
    pytest.param(
        lazy_fixture("pg_registry_audit"),
        marks=pytest.mark.xdist_group(name="pg_registry"),
        id="pg",
    ),
    pytest.param(
        lazy_fixture("mysql_registry_audit"),
        marks=pytest.mark.xdist_group(name="mysql_registry"),
        id="mysql",
    ),
]
sql_unaudited_fixtures = [
    pytest.param(
        lazy_fixture("pg_registry_unaudited"),
        marks=pytest.mark.xdist_group(name="pg_registry"),
        id="pg",
    ),
    pytest.param(
        lazy_fixture("mysql_registry_unaudited"),
        marks=pytest.mark.xdist_group(name="mysql_registry"),
        id="mysql",
    ),
]


def _sql_registry(path: str, *, enable_object_audit_log: bool) -> SqlRegistry:
    return SqlRegistry(
        SqlRegistryConfig(
            registry_type="sql",
            path=path,
            cache_ttl_seconds=2,
            cache_mode="sync",
            sqlalchemy_config_kwargs={"echo": False, "pool_pre_ping": True},
            thread_pool_executor_worker_count=0,
            purge_feast_metadata=False,
            enable_object_audit_log=enable_object_audit_log,
        ),
        _PROJECT,
        None,
    )


@pytest.fixture(scope="function")
def pg_registry_audit(postgres_server):
    yield from _pg_registry(postgres_server, enable_object_audit_log=True)


@pytest.fixture(scope="function")
def pg_registry_unaudited(postgres_server):
    yield from _pg_registry(postgres_server, enable_object_audit_log=False)


def _pg_registry(postgres_server, *, enable_object_audit_log: bool):
    db_name = "".join(random.choices(string.ascii_lowercase, k=10))
    _create_pg_database(postgres_server, db_name)
    container_port = postgres_server.get_exposed_port(5432)
    container_host = postgres_server.get_container_host_ip()
    registry = _sql_registry(
        f"postgresql+psycopg://{postgres_server.username}:{postgres_server.password}"
        f"@{container_host}:{container_port}/{db_name}",
        enable_object_audit_log=enable_object_audit_log,
    )
    try:
        yield registry
    finally:
        registry.teardown()


@pytest.fixture(scope="function")
def mysql_registry_audit(mysql_server):
    yield from _mysql_registry(mysql_server, enable_object_audit_log=True)


@pytest.fixture(scope="function")
def mysql_registry_unaudited(mysql_server):
    yield from _mysql_registry(mysql_server, enable_object_audit_log=False)


def _mysql_registry(mysql_server, *, enable_object_audit_log: bool):
    db_name = "".join(random.choices(string.ascii_lowercase, k=10))
    _create_mysql_database(mysql_server, db_name)
    # Non-SUPER users cannot CREATE TRIGGER while binlog is on unless
    # log_bin_trust_function_creators is set (fail-closed tests).
    mysql_server.exec(
        f"mysql -uroot -p{mysql_server.root_password} "
        "-e 'SET GLOBAL log_bin_trust_function_creators=1'"
    )
    connection_url = (
        "/".join(mysql_server.get_connection_url().split("/")[:-1]) + f"/{db_name}"
    )
    registry = _sql_registry(
        connection_url, enable_object_audit_log=enable_object_audit_log
    )
    try:
        yield registry
    finally:
        registry.teardown()


def _entity(name: str = "test_entity", description: str = "Test entity") -> Entity:
    return Entity(
        name=name,
        description=description,
        join_keys=["id"],
        value_type=ValueType.STRING,
    )


def _audit_rows(
    registry: SqlRegistry,
    object_name: str,
    object_type: str = ObjectAuditLogObjectType.ENTITIES.value,
):
    with registry.write_engine.begin() as conn:
        return conn.execute(
            select(object_audit_log)
            .where(
                object_audit_log.c.object_name == object_name,
                object_audit_log.c.object_type == object_type,
            )
            .order_by(object_audit_log.c.id)
        ).all()


def _from_gzip_entity_proto(blob) -> EntityProto:
    return EntityProto.FromString(gzip.decompress(bytes(blob)))


@contextmanager
def _audit_inserts_fail(registry: SqlRegistry):
    """Fail the next object_audit_log INSERT inside the mutation transaction."""
    dialect = registry.write_engine.dialect.name
    with registry.write_engine.begin() as conn:
        if dialect == "postgresql":
            conn.execute(
                text(
                    "CREATE FUNCTION fail_object_audit_log_insert() RETURNS trigger AS $$"
                    "BEGIN RAISE EXCEPTION 'audit insert failed' USING ERRCODE = '23514'; END;"
                    "$$ LANGUAGE plpgsql"
                )
            )
            conn.execute(
                text(
                    "CREATE TRIGGER fail_object_audit_log_insert "
                    "BEFORE INSERT ON object_audit_log "
                    "FOR EACH ROW EXECUTE PROCEDURE fail_object_audit_log_insert()"
                )
            )
        elif dialect in ("mysql", "mariadb"):
            conn.execute(
                text(
                    "CREATE TRIGGER fail_object_audit_log_insert "
                    "BEFORE INSERT ON object_audit_log "
                    "FOR EACH ROW "
                    "SIGNAL SQLSTATE '23000' SET MESSAGE_TEXT = 'audit insert failed'"
                )
            )
        else:
            raise RuntimeError(f"unsupported dialect {dialect}")
    try:
        yield
    finally:
        with registry.write_engine.begin() as conn:
            if dialect == "postgresql":
                conn.execute(
                    text(
                        "DROP TRIGGER IF EXISTS fail_object_audit_log_insert "
                        "ON object_audit_log"
                    )
                )
                conn.execute(
                    text("DROP FUNCTION IF EXISTS fail_object_audit_log_insert()")
                )
            else:
                conn.execute(
                    text("DROP TRIGGER IF EXISTS fail_object_audit_log_insert")
                )


@pytest.mark.integration
@pytest.mark.parametrize("sql_registry", sql_unaudited_fixtures)
class TestObjectAuditLogSchemaSqlBackends:
    def test_create_all_creates_object_audit_log_columns_and_index(self, sql_registry):
        inspector = inspect(sql_registry.write_engine)
        assert inspector.has_table("object_audit_log")
        columns = {
            col["name"]: col for col in inspector.get_columns("object_audit_log")
        }
        assert set(columns) == _OBJECT_AUDIT_LOG_COLUMNS
        for name, col in columns.items():
            assert col["nullable"] is (name in _OBJECT_AUDIT_LOG_NULLABLE)
        index_names = {idx["name"] for idx in inspector.get_indexes("object_audit_log")}
        assert "idx_object_audit_log_project_object_recorded" in index_names
        assert "idx_object_audit_log_recorded_at" in index_names
        if sql_registry.write_engine.dialect.name == "postgresql":
            check_names = {
                ck["name"] for ck in inspector.get_check_constraints("object_audit_log")
            }
            assert "ck_object_audit_log_operation" in check_names

    @pytest.mark.parametrize(
        "operation",
        [
            ObjectAuditLogOperation.CREATE,
            ObjectAuditLogOperation.UPDATE,
            ObjectAuditLogOperation.DELETE,
        ],
    )
    def test_operation_check_allows_enum_values(self, sql_registry, operation):
        with sql_registry.write_engine.begin() as conn:
            conn.execute(
                object_audit_log.insert().values(
                    project_id="p",
                    project_uuid=_AUDIT_PROJECT_UUID,
                    object_type=ObjectAuditLogObjectType.ENTITIES.value,
                    object_name="e",
                    operation=operation.value,
                    recorded_at=0,
                )
            )

    @pytest.mark.parametrize("operation", ["CREATE", "upsert", "delete ", ""])
    def test_operation_check_rejects_unknown_values(self, sql_registry, operation):
        dialect = sql_registry.write_engine.dialect.name
        if dialect in ("mysql", "mariadb") and operation.lower() in {
            op.value for op in ObjectAuditLogOperation
        }:
            pytest.skip("MySQL CHECK collation treats CREATE as create")
        with sql_registry.write_engine.begin() as conn:
            with pytest.raises((IntegrityError, DatabaseError)):
                conn.execute(
                    object_audit_log.insert().values(
                        project_id="p",
                        project_uuid=_AUDIT_PROJECT_UUID,
                        object_type=ObjectAuditLogObjectType.ENTITIES.value,
                        object_name="e",
                        operation=operation,
                        recorded_at=0,
                    )
                )

    def test_delete_project_leaves_object_audit_log_rows(self, sql_registry):
        sql_registry.apply_project(Project(name="reused"))
        with sql_registry.write_engine.begin() as conn:
            conn.execute(
                object_audit_log.insert().values(
                    project_id="reused",
                    project_uuid=_AUDIT_PROJECT_UUID,
                    object_type=ObjectAuditLogObjectType.ENTITIES.value,
                    object_name="e",
                    operation=ObjectAuditLogOperation.CREATE.value,
                    recorded_at=0,
                )
            )
        sql_registry.delete_project("reused")
        with sql_registry.write_engine.begin() as conn:
            planted = conn.execute(
                select(func.count())
                .select_from(object_audit_log)
                .where(object_audit_log.c.object_name == "e")
            ).scalar_one()
        assert planted == 1

    def test_flag_off_skips_inserts(self, sql_registry):
        sql_registry.apply_entity(_entity(), _PROJECT)
        sql_registry.delete_entity("test_entity", _PROJECT)
        with sql_registry.write_engine.begin() as conn:
            count = conn.execute(
                select(func.count()).select_from(object_audit_log)
            ).scalar_one()
        assert count == 0


@pytest.mark.integration
@pytest.mark.parametrize("sql_registry_audit", sql_audit_fixtures)
class TestObjectAuditLogWritesSqlBackends:
    def test_apply_entity_inserts_create_row_with_after_proto(self, sql_registry_audit):
        sql_registry_audit.apply_entity(_entity(), _PROJECT)
        rows = _audit_rows(sql_registry_audit, "test_entity")
        assert len(rows) == 1
        row = rows[0]
        assert row.operation == ObjectAuditLogOperation.CREATE.value
        assert row.object_type == ObjectAuditLogObjectType.ENTITIES.value
        assert row.project_id == _PROJECT
        assert row.project_uuid
        assert row.before_proto is None
        proto = _from_gzip_entity_proto(row.after_proto)
        assert proto.spec.name == "test_entity"
        now = int(_utc_now().timestamp())
        assert abs(row.recorded_at - now) < 60
        assert row.recorded_at < 10**11

    def test_apply_entity_change_inserts_update_with_both_blobs(
        self, sql_registry_audit
    ):
        sql_registry_audit.apply_entity(_entity(description="first"), _PROJECT)
        sql_registry_audit.apply_entity(_entity(description="second"), _PROJECT)
        rows = _audit_rows(sql_registry_audit, "test_entity")
        assert [row.operation for row in rows] == [
            ObjectAuditLogOperation.CREATE.value,
            ObjectAuditLogOperation.UPDATE.value,
        ]
        before = _from_gzip_entity_proto(rows[1].before_proto)
        after = _from_gzip_entity_proto(rows[1].after_proto)
        assert before.spec.description == "first"
        assert after.spec.description == "second"

    def test_apply_entity_noop_does_not_insert(self, sql_registry_audit):
        sql_registry_audit.apply_entity(_entity(), _PROJECT)
        sql_registry_audit.apply_entity(_entity(), _PROJECT)
        rows = _audit_rows(sql_registry_audit, "test_entity")
        assert len(rows) == 1
        assert rows[0].operation == ObjectAuditLogOperation.CREATE.value

    def test_delete_entity_inserts_delete_row_with_before_proto(
        self, sql_registry_audit
    ):
        sql_registry_audit.apply_entity(_entity(), _PROJECT)
        sql_registry_audit.delete_entity("test_entity", _PROJECT)
        with pytest.raises(EntityNotFoundException):
            sql_registry_audit.get_entity("test_entity", _PROJECT)
        rows = _audit_rows(sql_registry_audit, "test_entity")
        assert [row.operation for row in rows] == [
            ObjectAuditLogOperation.CREATE.value,
            ObjectAuditLogOperation.DELETE.value,
        ]
        assert rows[1].after_proto is None
        proto = _from_gzip_entity_proto(rows[1].before_proto)
        assert proto.spec.name == "test_entity"

    def test_failed_audit_insert_rolls_back_apply(self, sql_registry_audit):
        sql_registry_audit.apply_project(Project(name=_PROJECT))
        with _audit_inserts_fail(sql_registry_audit):
            with pytest.raises((IntegrityError, DatabaseError)):
                sql_registry_audit.apply_entity(_entity(), _PROJECT)
        with pytest.raises(EntityNotFoundException):
            sql_registry_audit.get_entity("test_entity", _PROJECT)
        assert _audit_rows(sql_registry_audit, "test_entity") == []

    def test_failed_audit_insert_rolls_back_delete(self, sql_registry_audit):
        sql_registry_audit.apply_entity(_entity(), _PROJECT)
        with _audit_inserts_fail(sql_registry_audit):
            with pytest.raises((IntegrityError, DatabaseError)):
                sql_registry_audit.delete_entity("test_entity", _PROJECT)
        assert sql_registry_audit.get_entity("test_entity", _PROJECT).name == (
            "test_entity"
        )
        rows = _audit_rows(sql_registry_audit, "test_entity")
        assert [row.operation for row in rows] == [ObjectAuditLogOperation.CREATE.value]

    def test_delete_project_keeps_prior_rows_and_adds_project_delete(
        self, sql_registry_audit
    ):
        sql_registry_audit.apply_entity(_entity(), _PROJECT)
        sql_registry_audit.apply_data_source(
            FileSource(
                name="test_source",
                path="some path",
                timestamp_field="event_timestamp",
            ),
            _PROJECT,
        )
        sql_registry_audit.delete_project(_PROJECT)
        entity_rows = _audit_rows(sql_registry_audit, "test_entity")
        assert [row.operation for row in entity_rows] == [
            ObjectAuditLogOperation.CREATE.value,
            ObjectAuditLogOperation.DELETE.value,
        ]
        assert entity_rows[1].after_proto is None
        proto = _from_gzip_entity_proto(entity_rows[1].before_proto)
        assert proto.spec.name == "test_entity"
        source_rows = _audit_rows(
            sql_registry_audit,
            "test_source",
            ObjectAuditLogObjectType.DATA_SOURCES.value,
        )
        assert [row.operation for row in source_rows] == [
            ObjectAuditLogOperation.CREATE.value,
            ObjectAuditLogOperation.DELETE.value,
        ]
        project_rows = _audit_rows(
            sql_registry_audit,
            _PROJECT,
            ObjectAuditLogObjectType.PROJECTS.value,
        )
        assert project_rows[-1].operation == ObjectAuditLogOperation.DELETE.value
        assert project_rows[-1].after_proto is None
        project_proto = ProjectProto.FromString(
            gzip.decompress(bytes(project_rows[-1].before_proto))
        )
        assert project_proto.spec.name == _PROJECT
        assert entity_rows[1].project_uuid == project_rows[-1].project_uuid
        assert entity_rows[1].id < project_rows[-1].id

    def test_failed_child_audit_rolls_back_delete_project(self, sql_registry_audit):
        sql_registry_audit.apply_entity(_entity(), _PROJECT)
        with _audit_inserts_fail(sql_registry_audit):
            with pytest.raises((IntegrityError, DatabaseError)):
                sql_registry_audit.delete_project(_PROJECT)
        assert sql_registry_audit.get_entity("test_entity", _PROJECT).name == (
            "test_entity"
        )
        rows = _audit_rows(sql_registry_audit, "test_entity")
        assert [row.operation for row in rows] == [ObjectAuditLogOperation.CREATE.value]

    def test_delete_entity_mints_project_uuid_when_missing(self, sql_registry_audit):
        sql_registry_audit.apply_entity(_entity(), _PROJECT)
        with sql_registry_audit.write_engine.begin() as conn:
            conn.execute(
                feast_metadata.delete().where(
                    feast_metadata.c.project_id == _PROJECT,
                    feast_metadata.c.metadata_key
                    == FeastMetadataKeys.PROJECT_UUID.value,
                )
            )
        sql_registry_audit.delete_entity("test_entity", _PROJECT)
        delete = _audit_rows(sql_registry_audit, "test_entity")[-1]
        assert delete.operation == ObjectAuditLogOperation.DELETE.value
        assert delete.project_uuid
