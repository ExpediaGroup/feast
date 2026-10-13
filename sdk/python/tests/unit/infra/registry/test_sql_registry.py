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
import tempfile
import threading
from datetime import timedelta
from unittest.mock import patch

import pytest
from pydantic import ValidationError
from sqlalchemy import func, inspect, select
from sqlalchemy.exc import IntegrityError

from feast.entity import Entity
from feast.errors import (
    DataSourceObjectNotFoundException,
    EntityNotFoundException,
    FeatureViewNotFoundException,
    PermissionNotFoundException,
    ProjectNotFoundException,
)
from feast.feature_view import MATERIALIZATION_INTERVALS_MAX_LEN, FeatureView
from feast.field import Field
from feast.infra.infra_object import Infra
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.online_stores.sqlite import SqliteTable
from feast.infra.registry.sql import (
    FeastMetadataKeys,
    ObjectAuditLogObjectType,
    ObjectAuditLogOperation,
    SqlRegistry,
    SqlRegistryConfig,
    feast_metadata,
    managed_infra,
    object_audit_log,
    projects,
)
from feast.infra.registry.sql_fallback import (
    SqlFallbackRegistry,
    SqlFallbackRegistryConfig,
)
from feast.permissions.action import AuthzedAction
from feast.permissions.permission import Permission
from feast.permissions.policy import RoleBasedPolicy
from feast.permissions.security_manager import (
    SecurityManager,
    no_security_manager,
    set_security_manager,
)
from feast.permissions.user import User
from feast.project import Project
from feast.protos.feast.core.Entity_pb2 import Entity as EntityProto
from feast.protos.feast.core.Project_pb2 import Project as ProjectProto
from feast.protos.feast.core.SortedFeatureView_pb2 import SortOrder
from feast.saved_dataset import ValidationReference
from feast.sort_key import SortKey
from feast.sorted_feature_view import SortedFeatureView
from feast.types import Float32, Int64
from feast.utils import _utc_now
from feast.value_type import ValueType


@pytest.fixture
def sqlite_registry():
    """Create a temporary SQLite registry for testing."""
    fd, registry_path = tempfile.mkstemp()
    registry_config = SqlRegistryConfig(
        registry_type="sql",
        path=f"sqlite:///{registry_path}",
        purge_feast_metadata=False,
    )

    registry = SqlRegistry(registry_config, "test_project", None)
    yield registry
    registry.teardown()


@pytest.fixture
def sqlite_registry_audit():
    """SQL registry with object_audit_log inserts enabled."""
    fd, registry_path = tempfile.mkstemp()
    registry_config = SqlRegistryConfig(
        registry_type="sql",
        path=f"sqlite:///{registry_path}",
        purge_feast_metadata=False,
        enable_object_audit_log=True,
    )
    registry = SqlRegistry(registry_config, "test_project", None)
    yield registry
    registry.teardown()


@pytest.fixture
def sqlite_registry_custom_max_intervals():
    """Create a temporary SQLite registry with a custom materialization
    intervals cap, to verify apply_materialization honors it end-to-end."""
    fd, registry_path = tempfile.mkstemp()
    custom_max_intervals = 3
    assert custom_max_intervals != MATERIALIZATION_INTERVALS_MAX_LEN
    registry_config = SqlRegistryConfig(
        registry_type="sql",
        path=f"sqlite:///{registry_path}",
        purge_feast_metadata=False,
        materialization_intervals_max_len=custom_max_intervals,
    )

    registry = SqlRegistry(registry_config, "test_project", None)
    yield registry, custom_max_intervals
    registry.teardown()


@pytest.fixture
def sqlite_fallback_registry():
    """Create a temporary SQLite-backed SqlFallbackRegistry (the caching
    registry eg-feature-store-registry and mlpfs-remote-registry run)."""
    fd, registry_path = tempfile.mkstemp()
    registry_config = SqlFallbackRegistryConfig(
        registry_type="sql-fallback",
        path=f"sqlite:///{registry_path}",
        purge_feast_metadata=False,
        cache_ttl_seconds=600,
    )

    registry = SqlFallbackRegistry(registry_config, "test_project", None)
    yield registry
    registry.teardown()


@pytest.fixture
def sqlite_fallback_registry_audit():
    """SqlFallbackRegistry with object_audit_log inserts enabled."""
    fd, registry_path = tempfile.mkstemp()
    registry_config = SqlFallbackRegistryConfig(
        registry_type="sql-fallback",
        path=f"sqlite:///{registry_path}",
        purge_feast_metadata=False,
        cache_ttl_seconds=600,
        enable_object_audit_log=True,
    )
    registry = SqlFallbackRegistry(registry_config, "test_project", None)
    yield registry
    registry.teardown()


def _sorted_feature_view(name: str, entity: Entity) -> SortedFeatureView:
    return SortedFeatureView(
        name=name,
        source=FileSource(path="some path", timestamp_field="event_timestamp"),
        entities=[entity],
        schema=[
            Field(name="sort_key1", dtype=Int64),
            Field(name="conv_rate", dtype=Float32),
        ],
        sort_keys=[
            SortKey(
                name="sort_key1",
                value_type=ValueType.INT64,
                default_sort_order=SortOrder.DESC,
            )
        ],
        online=True,
    )


class TestSqlRegistryConfig:
    def test_enable_object_audit_log_rejects_purge_feast_metadata(self):
        with pytest.raises(ValidationError, match="enable_object_audit_log"):
            SqlRegistryConfig(
                registry_type="sql",
                path="sqlite:///:memory:",
                purge_feast_metadata=True,
                enable_object_audit_log=True,
            )

    def test_enable_object_audit_log_without_purge_is_allowed(self):
        config = SqlRegistryConfig(
            registry_type="sql",
            path="sqlite:///:memory:",
            purge_feast_metadata=False,
            enable_object_audit_log=True,
        )
        assert config.enable_object_audit_log is True

    def test_sql_fallback_config_inherits_the_same_rejection(self):
        with pytest.raises(ValidationError, match="enable_object_audit_log"):
            SqlFallbackRegistryConfig(
                registry_type="sql-fallback",
                path="sqlite:///:memory:",
                purge_feast_metadata=True,
                enable_object_audit_log=True,
            )

    def test_assignment_rejects_enabling_audit_when_purge_is_on(self):
        config = SqlRegistryConfig(
            registry_type="sql",
            path="sqlite:///:memory:",
            purge_feast_metadata=True,
        )
        with pytest.raises(ValidationError, match="enable_object_audit_log"):
            config.enable_object_audit_log = True

    def test_assignment_rejects_enabling_purge_when_audit_is_on(self):
        config = SqlRegistryConfig(
            registry_type="sql",
            path="sqlite:///:memory:",
            enable_object_audit_log=True,
        )
        with pytest.raises(ValidationError, match="enable_object_audit_log"):
            config.purge_feast_metadata = True

    def test_sql_registry_init_rejects_model_construct_both_true(self):
        config = SqlRegistryConfig.model_construct(
            path="sqlite:///:memory:",
            purge_feast_metadata=True,
            enable_object_audit_log=True,
        )
        with pytest.raises(ValueError, match="enable_object_audit_log"):
            SqlRegistry(config, "test_project", None)


class TestSqlRegistry:
    """Test class for SqlRegistry"""

    def test_apply_and_retrieve_entity(self, sqlite_registry):
        """Test applying and retrieving an entity from the SQL registry."""
        entity = Entity(
            name="test_entity",
            description="Test entity for testing",
            tags={"test": "transaction"},
        )
        sqlite_registry.apply_entity(entity, "test_project")

        retrieved_entity = sqlite_registry.get_entity("test_entity", "test_project")
        assert retrieved_entity.name == "test_entity"
        assert retrieved_entity.description == "Test entity for testing"

    def test_delete_entity(self, sqlite_registry):
        """Test deleting an entity from the SQL registry."""
        entity = Entity(name="test_entity", description="Test entity")
        sqlite_registry.apply_entity(entity, "test_project")

        sqlite_registry.delete_entity("test_entity", "test_project")

        with pytest.raises(EntityNotFoundException):
            sqlite_registry.get_entity("test_entity", "test_project")

    def test_get_project_metadata_model_returns_initialized_metadata(
        self, sqlite_registry
    ):
        """Test that get_project_metadata_model returns metadata after applying an entity."""
        entity = Entity(name="test_entity", description="Test entity")
        sqlite_registry.apply_entity(entity, "test_project")

        project_metadata = sqlite_registry.get_project_metadata_model("test_project")

        assert project_metadata.project_name == "test_project"
        assert project_metadata.project_uuid is not None
        assert project_metadata.last_updated_timestamp is not None

    def test_get_project_metadata_model_nonexistent_project(self, sqlite_registry):
        """Test that get_project_metadata_model handles non-existent projects gracefully."""
        project_metadata = sqlite_registry.get_project_metadata_model(
            "nonexistent_project"
        )

        assert project_metadata.project_name == "nonexistent_project"
        assert project_metadata is not None

    def test_get_all_project_metadata_multiple_projects(self, sqlite_registry):
        """Test that get_all_project_metadata returns metadata for all projects."""
        entity1 = Entity(name="entity1", description="Entity 1")
        entity2 = Entity(name="entity2", description="Entity 2")
        sqlite_registry.apply_entity(entity1, "project_1")
        sqlite_registry.apply_entity(entity2, "project_2")

        all_metadata = sqlite_registry.get_all_project_metadata()

        project_names = [m.project_name for m in all_metadata]
        assert "project_1" in project_names
        assert "project_2" in project_names
        for metadata in all_metadata:
            assert metadata.project_uuid is not None

    def test_apply_materialization_honors_configured_max_intervals(
        self, sqlite_registry_custom_max_intervals
    ):
        """Test that apply_materialization caps materialization_intervals at the
        registry's configured materialization_intervals_max_len, not the module
        default -- exercising the full apply_materialization -> get_feature_view
        round trip through a real SQL-backed registry."""
        registry, custom_max_intervals = sqlite_registry_custom_max_intervals

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
        num_intervals = custom_max_intervals + 5
        for i in range(num_intervals):
            start_date = now - timedelta(days=num_intervals - i, hours=1)
            end_date = now - timedelta(days=num_intervals - i)
            registry.apply_materialization(
                feature_view, "test_project", start_date, end_date
            )

        stored_feature_view = registry.get_feature_view("driver_stats", "test_project")
        assert (
            len(stored_feature_view.materialization_intervals) == custom_max_intervals
        )


class TestSqlRegistryMaterializationIntervalHistory:
    """Test class for SqlRegistry's materialization_interval_history table."""

    def _apply_feature_view(self, registry, name, entity):
        source = FileSource(path="some path", timestamp_field="event_timestamp")
        feature_view = FeatureView(
            name=name,
            entities=[entity],
            schema=[Field(name="conv_rate", dtype=Float32)],
            source=source,
            online=True,
        )
        registry.apply_feature_view(feature_view, "test_project")
        return feature_view

    def test_apply_materialization_archives_new_and_dropped_intervals(
        self, sqlite_registry_custom_max_intervals
    ):
        registry, custom_max_intervals = sqlite_registry_custom_max_intervals
        entity = Entity(name="driver", description="Driver entity")
        registry.apply_entity(entity, "test_project")
        feature_view = self._apply_feature_view(registry, "driver_stats", entity)

        now = _utc_now()
        num_intervals = custom_max_intervals + 5
        for i in range(num_intervals):
            registry.apply_materialization(
                feature_view,
                "test_project",
                now - timedelta(days=num_intervals - i, hours=1),
                now - timedelta(days=num_intervals - i),
            )

        stored_feature_view = registry.get_feature_view("driver_stats", "test_project")
        assert (
            len(stored_feature_view.materialization_intervals) == custom_max_intervals
        )

        history = registry.get_materialization_interval_history(
            "driver_stats", "test_project"
        )
        # Every interval ever added is in history, even the ones the cap
        # already dropped from the feature view's own list.
        assert len(history) == num_intervals
        # Ordered by start_time ascending.
        start_times = [entry.start_time.ToSeconds() for entry in history]
        assert start_times == sorted(start_times)

    def test_apply_materialization_is_idempotent_in_history(
        self, sqlite_registry_custom_max_intervals
    ):
        registry, custom_max_intervals = sqlite_registry_custom_max_intervals
        entity = Entity(name="driver", description="Driver entity")
        registry.apply_entity(entity, "test_project")
        feature_view = self._apply_feature_view(registry, "driver_stats", entity)

        now = _utc_now()
        intervals = [
            (now - timedelta(days=i, hours=1), now - timedelta(days=i))
            for i in range(custom_max_intervals + 2)
        ]
        for start, end in intervals:
            registry.apply_materialization(feature_view, "test_project", start, end)
        history_first_pass = registry.get_materialization_interval_history(
            "driver_stats", "test_project"
        )
        assert len(history_first_pass) == len(intervals)

        # Re-apply the exact same intervals again (simulating a re-run or a
        # feature view re-apply that re-hydrates the same stored intervals) --
        # history must not grow, since these were already recorded.
        for start, end in intervals:
            registry.apply_materialization(feature_view, "test_project", start, end)
        history_second_pass = registry.get_materialization_interval_history(
            "driver_stats", "test_project"
        )
        assert len(history_second_pass) == len(intervals)

    def test_apply_feature_view_backfills_history_for_pre_existing_over_cap_list(
        self, sqlite_registry_custom_max_intervals
    ):
        """A feature view that already has more intervals than the cap (e.g.
        seeded outside of add_materialization_interval, or persisted before
        a cap was ever enforced) gets its full pre-existing interval list
        archived to history the next time it's re-applied, not just the
        ones the cap drops."""
        registry, custom_max_intervals = sqlite_registry_custom_max_intervals
        entity = Entity(name="driver", description="Driver entity")
        registry.apply_entity(entity, "test_project")

        source = FileSource(path="some path", timestamp_field="event_timestamp")
        now = _utc_now()
        num_intervals = custom_max_intervals + 5
        seeded_intervals = [
            (
                now - timedelta(days=num_intervals - i, hours=1),
                now - timedelta(days=num_intervals - i),
            )
            for i in range(num_intervals)
        ]
        feature_view = FeatureView(
            name="driver_stats",
            entities=[entity],
            schema=[Field(name="conv_rate", dtype=Float32)],
            source=source,
            online=True,
        )
        for interval in seeded_intervals:
            feature_view.materialization_intervals.append(interval)
        registry.apply_feature_view(feature_view, "test_project")

        # Before any re-apply, nothing has been archived yet (this is the
        # first time this feature view has ever been seen).
        assert (
            registry.get_materialization_interval_history(
                "driver_stats", "test_project"
            )
            == []
        )

        # Re-applying (e.g. a fresh `feast apply`) triggers the
        # hydration/cap path, which now must archive the *entire*
        # previously-stored list before truncating it.
        fresh_feature_view = FeatureView(
            name="driver_stats",
            entities=[entity],
            schema=[Field(name="conv_rate", dtype=Float32)],
            source=source,
            online=True,
        )
        registry.apply_feature_view(fresh_feature_view, "test_project")

        stored_feature_view = registry.get_feature_view("driver_stats", "test_project")
        assert (
            len(stored_feature_view.materialization_intervals) == custom_max_intervals
        )
        history = registry.get_materialization_interval_history(
            "driver_stats", "test_project"
        )
        assert len(history) == num_intervals

    def test_get_materialization_interval_history_filters_by_feature_view(
        self, sqlite_registry_custom_max_intervals
    ):
        registry, _ = sqlite_registry_custom_max_intervals
        entity = Entity(name="driver", description="Driver entity")
        registry.apply_entity(entity, "test_project")
        fv1 = self._apply_feature_view(registry, "fv1", entity)
        fv2 = self._apply_feature_view(registry, "fv2", entity)

        now = _utc_now()
        registry.apply_materialization(
            fv1, "test_project", now - timedelta(hours=1), now
        )
        registry.apply_materialization(
            fv2, "test_project", now - timedelta(hours=1), now
        )

        history_fv1 = registry.get_materialization_interval_history(
            "fv1", "test_project"
        )
        assert len(history_fv1) == 1
        assert history_fv1[0].feature_view_name == "fv1"

    def test_concurrent_archiving_of_same_interval_does_not_duplicate(
        self, sqlite_registry
    ):
        """Two callers racing to archive the exact same interval (e.g. a
        retry racing the original apply_materialization call) must not
        produce duplicate history rows -- enforced by a unique index at the
        DB level, not a check-then-insert race in application code."""
        entity = Entity(name="driver", description="Driver entity")
        sqlite_registry.apply_entity(entity, "test_project")
        feature_view = self._apply_feature_view(sqlite_registry, "driver_stats", entity)

        now = _utc_now()
        start, end = now - timedelta(hours=1), now
        barrier = threading.Barrier(2)
        errors = []

        def _race():
            try:
                barrier.wait(timeout=5)
                sqlite_registry._record_materialization_interval_history(
                    feature_view.name, "test_project", [(start, end)]
                )
            except Exception as exception:  # pragma: no cover - assertion path
                errors.append(exception)

        threads = [threading.Thread(target=_race) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, f"unexpected exceptions from concurrent archiving: {errors}"
        history = sqlite_registry.get_materialization_interval_history(
            "driver_stats", "test_project"
        )
        assert len(history) == 1


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

_AUDIT_PROJECT_UUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
_OBJECT_AUDIT_LOG_NULLABLE = {
    "actor",
    "before_proto",
    "after_proto",
    "request_id",
}


def _object_audit_log_row_count(registry: SqlRegistry) -> int:
    with registry.write_engine.begin() as conn:
        return conn.execute(
            select(func.count()).select_from(object_audit_log)
        ).scalar_one()


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


def _project_last_updated(
    registry: SqlRegistry, project: str = "test_project"
) -> int:
    with registry.write_engine.begin() as conn:
        return conn.execute(
            select(projects.c.last_updated_timestamp).where(
                projects.c.project_id == project
            )
        ).scalar_one()


def _delete_project_uuid(registry: SqlRegistry, project: str = "test_project") -> None:
    with registry.write_engine.begin() as conn:
        conn.execute(
            feast_metadata.delete().where(
                feast_metadata.c.project_id == project,
                feast_metadata.c.metadata_key == FeastMetadataKeys.PROJECT_UUID.value,
            )
        )


def _entity(name: str = "test_entity", description: str = "Test entity") -> Entity:
    return Entity(
        name=name,
        description=description,
        join_keys=["id"],
        value_type=ValueType.STRING,
    )


class TestObjectAuditLogSchema:
    """Ticket 3: create_all, CHECK constraint, and delete_project leaving audit rows."""

    def test_create_all_creates_object_audit_log_columns_and_index(
        self, sqlite_registry
    ):
        inspector = inspect(sqlite_registry.write_engine)
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
    def test_operation_check_allows_enum_values(self, sqlite_registry, operation):
        with sqlite_registry.write_engine.begin() as conn:
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
    def test_operation_check_rejects_unknown_values(self, sqlite_registry, operation):
        with sqlite_registry.write_engine.begin() as conn:
            with pytest.raises(IntegrityError):
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

    def test_delete_project_leaves_object_audit_log_rows(self, sqlite_registry):
        sqlite_registry.apply_project(Project(name="reused"))
        with sqlite_registry.write_engine.begin() as conn:
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

        sqlite_registry.delete_project("reused")
        with sqlite_registry.write_engine.begin() as conn:
            planted = conn.execute(
                select(func.count())
                .select_from(object_audit_log)
                .where(object_audit_log.c.object_name == "e")
            ).scalar_one()
        assert planted == 1

    def test_delete_project_raises_when_missing(self, sqlite_registry):
        with pytest.raises(ProjectNotFoundException):
            sqlite_registry.delete_project("does_not_exist")

    def test_apply_and_delete_entity_leave_object_gone(self, sqlite_registry):
        entity = Entity(name="test_entity", description="Test entity")
        sqlite_registry.apply_entity(entity, "test_project")
        sqlite_registry.delete_entity("test_entity", "test_project")

        with pytest.raises(EntityNotFoundException):
            sqlite_registry.get_entity("test_entity", "test_project")


class TestObjectAuditLogWrites:
    """Ticket 4: apply/delete insert forensic rows in the same transaction."""

    def test_flag_off_skips_inserts(self, sqlite_registry):
        sqlite_registry.apply_entity(_entity(), "test_project")
        sqlite_registry.delete_entity("test_entity", "test_project")
        assert _object_audit_log_row_count(sqlite_registry) == 0

    def test_live_purge_flag_rejects_audited_write(self, sqlite_registry_audit):
        sqlite_registry_audit.apply_entity(_entity(), "test_project")
        sqlite_registry_audit.purge_feast_metadata = True
        with pytest.raises(ValueError, match="enable_object_audit_log"):
            sqlite_registry_audit.apply_entity(_entity(name="other"), "test_project")
        with pytest.raises(ValueError, match="enable_object_audit_log"):
            sqlite_registry_audit.delete_entity("test_entity", "test_project")
        sqlite_registry_audit.purge_feast_metadata = False
        assert sqlite_registry_audit.get_entity("test_entity", "test_project").name == (
            "test_entity"
        )

    def test_apply_entity_inserts_create_row_with_after_proto(
        self, sqlite_registry_audit
    ):
        sqlite_registry_audit.apply_entity(_entity(), "test_project")
        rows = _audit_rows(sqlite_registry_audit, "test_entity")
        assert len(rows) == 1
        row = rows[0]
        assert row.operation == ObjectAuditLogOperation.CREATE.value
        assert row.object_type == ObjectAuditLogObjectType.ENTITIES.value
        assert row.project_id == "test_project"
        assert row.project_uuid
        assert row.before_proto is None
        proto = _from_gzip_entity_proto(row.after_proto)
        assert proto.spec.name == "test_entity"
        now = int(_utc_now().timestamp())
        assert abs(row.recorded_at - now) < 60
        assert row.recorded_at < 10**11

    def test_apply_entity_change_inserts_update_with_both_blobs(
        self, sqlite_registry_audit
    ):
        sqlite_registry_audit.apply_entity(_entity(description="first"), "test_project")
        sqlite_registry_audit.apply_entity(
            _entity(description="second"), "test_project"
        )
        rows = _audit_rows(sqlite_registry_audit, "test_entity")
        assert [row.operation for row in rows] == [
            ObjectAuditLogOperation.CREATE.value,
            ObjectAuditLogOperation.UPDATE.value,
        ]
        update = rows[1]
        before = _from_gzip_entity_proto(update.before_proto)
        after = _from_gzip_entity_proto(update.after_proto)
        assert before.spec.description == "first"
        assert after.spec.description == "second"

    def test_apply_entity_noop_does_not_insert(self, sqlite_registry_audit):
        sqlite_registry_audit.apply_entity(_entity(), "test_project")
        sqlite_registry_audit.apply_entity(_entity(), "test_project")
        rows = _audit_rows(sqlite_registry_audit, "test_entity")
        assert len(rows) == 1
        assert rows[0].operation == ObjectAuditLogOperation.CREATE.value

    def test_update_infra_noop_skips_update_and_audit(self, sqlite_registry_audit):
        sqlite_registry_audit.update_infra(Infra(), "test_project")
        with sqlite_registry_audit.write_engine.begin() as conn:
            ts = conn.execute(
                select(managed_infra.c.last_updated_timestamp)
            ).scalar_one()
        sqlite_registry_audit.update_infra(Infra(), "test_project")
        with sqlite_registry_audit.write_engine.begin() as conn:
            ts_after = conn.execute(
                select(managed_infra.c.last_updated_timestamp)
            ).scalar_one()
        assert ts_after == ts
        rows = _audit_rows(
            sqlite_registry_audit,
            "infra_obj",
            ObjectAuditLogObjectType.MANAGED_INFRA.value,
        )
        assert [row.operation for row in rows] == [ObjectAuditLogOperation.CREATE.value]

    def test_update_infra_change_inserts_update(self, sqlite_registry_audit, tmp_path):
        sqlite_registry_audit.update_infra(Infra(), "test_project")
        changed = Infra()
        changed.infra_objects.append(
            SqliteTable(path=str(tmp_path / "online.db"), name="my_table")
        )
        sqlite_registry_audit.update_infra(changed, "test_project")
        rows = _audit_rows(
            sqlite_registry_audit,
            "infra_obj",
            ObjectAuditLogObjectType.MANAGED_INFRA.value,
        )
        assert [row.operation for row in rows] == [
            ObjectAuditLogOperation.CREATE.value,
            ObjectAuditLogOperation.UPDATE.value,
        ]
        assert rows[1].after_proto != rows[1].before_proto

    def test_apply_validation_reference_noop_skips_audit(self, sqlite_registry_audit):
        ref = ValidationReference(
            name="vr",
            dataset_name="ds",
            profiler=object(),
        )
        sqlite_registry_audit.apply_validation_reference(ref, "test_project")
        sqlite_registry_audit.apply_validation_reference(
            ValidationReference(name="vr", dataset_name="ds", profiler=object()),
            "test_project",
        )
        rows = _audit_rows(
            sqlite_registry_audit,
            "vr",
            ObjectAuditLogObjectType.VALIDATION_REFERENCES.value,
        )
        assert [row.operation for row in rows] == [ObjectAuditLogOperation.CREATE.value]

    def test_apply_materialization_does_not_write_object_audit_log(
        self, sqlite_registry_audit
    ):
        entity = _entity(name="driver")
        sqlite_registry_audit.apply_entity(entity, "test_project")
        feature_view = FeatureView(
            name="driver_stats",
            entities=[entity],
            schema=[Field(name="conv_rate", dtype=Float32)],
            source=FileSource(path="some path", timestamp_field="event_timestamp"),
            online=True,
        )
        sqlite_registry_audit.apply_feature_view(feature_view, "test_project")
        create_rows = _audit_rows(
            sqlite_registry_audit,
            "driver_stats",
            object_type=ObjectAuditLogObjectType.FEATURE_VIEWS.value,
        )
        now = _utc_now()
        sqlite_registry_audit.apply_materialization(
            feature_view,
            "test_project",
            now - timedelta(hours=1),
            now,
        )
        after_rows = _audit_rows(
            sqlite_registry_audit,
            "driver_stats",
            object_type=ObjectAuditLogObjectType.FEATURE_VIEWS.value,
        )
        assert after_rows == create_rows
        assert [row.operation for row in after_rows] == [
            ObjectAuditLogOperation.CREATE.value
        ]
        history = sqlite_registry_audit.get_materialization_interval_history(
            "driver_stats", "test_project"
        )
        assert len(history) == 1

    def test_delete_entity_inserts_delete_row_with_before_proto(
        self, sqlite_registry_audit
    ):
        sqlite_registry_audit.apply_entity(_entity(), "test_project")
        sqlite_registry_audit.delete_entity("test_entity", "test_project")
        with pytest.raises(EntityNotFoundException):
            sqlite_registry_audit.get_entity("test_entity", "test_project")
        rows = _audit_rows(sqlite_registry_audit, "test_entity")
        assert [row.operation for row in rows] == [
            ObjectAuditLogOperation.CREATE.value,
            ObjectAuditLogOperation.DELETE.value,
        ]
        delete = rows[1]
        assert delete.after_proto is None
        proto = _from_gzip_entity_proto(delete.before_proto)
        assert proto.spec.name == "test_entity"

    def test_delete_entity_succeeds_when_project_row_missing(self, sqlite_registry):
        sqlite_registry.apply_entity(_entity(), "test_project")
        with sqlite_registry.write_engine.begin() as conn:
            conn.execute(
                projects.delete().where(projects.c.project_id == "test_project")
            )
        sqlite_registry.delete_entity("test_entity", "test_project")
        with pytest.raises(EntityNotFoundException):
            sqlite_registry.get_entity("test_entity", "test_project")

    def test_delete_data_source_uses_same_connection(self, sqlite_registry_audit):
        source = FileSource(
            name="test_source",
            path="some path",
            timestamp_field="event_timestamp",
        )
        sqlite_registry_audit.apply_data_source(source, "test_project")
        sqlite_registry_audit.delete_data_source("test_source", "test_project")
        rows = _audit_rows(
            sqlite_registry_audit,
            "test_source",
            ObjectAuditLogObjectType.DATA_SOURCES.value,
        )
        assert [row.operation for row in rows] == [
            ObjectAuditLogOperation.CREATE.value,
            ObjectAuditLogOperation.DELETE.value,
        ]

    def test_failed_audit_insert_rolls_back_apply(self, sqlite_registry_audit):
        sqlite_registry_audit.apply_project(Project(name="test_project"))
        with patch.object(
            sqlite_registry_audit,
            "_record_object_audit",
            side_effect=RuntimeError("audit failed"),
        ):
            with pytest.raises(RuntimeError, match="audit failed"):
                sqlite_registry_audit.apply_entity(_entity(), "test_project")
        with pytest.raises(EntityNotFoundException):
            sqlite_registry_audit.get_entity("test_entity", "test_project")
        assert _audit_rows(sqlite_registry_audit, "test_entity") == []

    def test_failed_audit_insert_rolls_back_delete(self, sqlite_registry_audit):
        sqlite_registry_audit.apply_entity(_entity(), "test_project")
        with patch.object(
            sqlite_registry_audit,
            "_record_object_audit",
            side_effect=RuntimeError("audit failed"),
        ):
            with pytest.raises(RuntimeError, match="audit failed"):
                sqlite_registry_audit.delete_entity("test_entity", "test_project")
        assert sqlite_registry_audit.get_entity("test_entity", "test_project").name == (
            "test_entity"
        )
        rows = _audit_rows(sqlite_registry_audit, "test_entity")
        assert [row.operation for row in rows] == [ObjectAuditLogOperation.CREATE.value]

    def test_actor_and_request_id_from_security_manager(self, sqlite_registry_audit):
        sm = SecurityManager(
            project="test_project",
            registry=sqlite_registry_audit,
            write_auth_only=True,
        )
        set_security_manager(sm)
        sm.set_current_user(User("alice", []))
        sm.set_request_id("bbbbbbbb-cccc-dddd-eeee-ffffffffffff")
        try:
            sqlite_registry_audit.apply_entity(_entity(), "test_project")
        finally:
            no_security_manager()
        row = _audit_rows(sqlite_registry_audit, "test_entity")[0]
        assert row.actor == "alice"
        assert row.request_id == "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"

    def test_delete_project_keeps_prior_rows_and_adds_project_delete(
        self, sqlite_registry_audit
    ):
        sqlite_registry_audit.apply_entity(_entity(), "test_project")
        sqlite_registry_audit.apply_data_source(
            FileSource(
                name="test_source",
                path="some path",
                timestamp_field="event_timestamp",
            ),
            "test_project",
        )
        sqlite_registry_audit.delete_project("test_project")
        entity_rows = _audit_rows(sqlite_registry_audit, "test_entity")
        assert [row.operation for row in entity_rows] == [
            ObjectAuditLogOperation.CREATE.value,
            ObjectAuditLogOperation.DELETE.value,
        ]
        assert entity_rows[1].after_proto is None
        proto = _from_gzip_entity_proto(entity_rows[1].before_proto)
        assert proto.spec.name == "test_entity"
        source_rows = _audit_rows(
            sqlite_registry_audit,
            "test_source",
            object_type=ObjectAuditLogObjectType.DATA_SOURCES.value,
        )
        assert [row.operation for row in source_rows] == [
            ObjectAuditLogOperation.CREATE.value,
            ObjectAuditLogOperation.DELETE.value,
        ]
        assert source_rows[1].after_proto is None
        project_rows = _audit_rows(
            sqlite_registry_audit,
            "test_project",
            object_type=ObjectAuditLogObjectType.PROJECTS.value,
        )
        assert project_rows[-1].operation == ObjectAuditLogOperation.DELETE.value
        assert project_rows[-1].after_proto is None
        project_proto = ProjectProto.FromString(
            gzip.decompress(bytes(project_rows[-1].before_proto))
        )
        assert project_proto.spec.name == "test_project"
        assert entity_rows[1].project_uuid == project_rows[-1].project_uuid
        assert entity_rows[1].id < project_rows[-1].id

    def test_failed_child_audit_rolls_back_delete_project(self, sqlite_registry_audit):
        sqlite_registry_audit.apply_entity(_entity(), "test_project")
        with patch.object(
            sqlite_registry_audit,
            "_record_delete_project_child_audits",
            side_effect=RuntimeError("audit failed"),
        ):
            with pytest.raises(RuntimeError, match="audit failed"):
                sqlite_registry_audit.delete_project("test_project")
        assert sqlite_registry_audit.get_entity("test_entity", "test_project").name == (
            "test_entity"
        )
        rows = _audit_rows(sqlite_registry_audit, "test_entity")
        assert [row.operation for row in rows] == [ObjectAuditLogOperation.CREATE.value]

    def test_delete_project_looks_up_actor_once_for_all_children(
        self, sqlite_registry_audit
    ):
        sqlite_registry_audit.apply_entity(_entity(), "test_project")
        sqlite_registry_audit.apply_data_source(
            FileSource(
                name="test_source",
                path="some path",
                timestamp_field="event_timestamp",
            ),
            "test_project",
        )
        with patch.object(
            SqlRegistry,
            "_audit_actor_and_request_id",
            wraps=SqlRegistry._audit_actor_and_request_id,
        ) as lookup:
            sqlite_registry_audit.delete_project("test_project")
        assert lookup.call_count == 2

    def test_delete_project_fetches_project_uuid_once(self, sqlite_registry_audit):
        sqlite_registry_audit.apply_entity(_entity(), "test_project")
        with patch.object(
            sqlite_registry_audit,
            "_maybe_init_project_metadata",
            wraps=sqlite_registry_audit._maybe_init_project_metadata,
        ) as maybe_init:
            sqlite_registry_audit.delete_project("test_project")
        assert maybe_init.call_count == 1
        assert maybe_init.call_args.args[1] is not None

    def test_project_uuid_fetched_once_per_audited_write(self, sqlite_registry_audit):
        sqlite_registry_audit.apply_project(Project(name="test_project"))
        with patch.object(
            sqlite_registry_audit,
            "_maybe_init_project_metadata",
            wraps=sqlite_registry_audit._maybe_init_project_metadata,
        ) as maybe_init:
            sqlite_registry_audit.apply_entity(_entity(), "test_project")
            assert maybe_init.call_count == 1
            sqlite_registry_audit.delete_entity("test_entity", "test_project")
            assert maybe_init.call_count == 2

    def test_apply_reuses_project_uuid_from_maybe_init_in_same_txn(
        self, sqlite_registry_audit
    ):
        sqlite_registry_audit.apply_project(Project(name="test_project"))
        with patch.object(
            sqlite_registry_audit,
            "_maybe_init_project_metadata",
            wraps=sqlite_registry_audit._maybe_init_project_metadata,
        ) as maybe_init:
            sqlite_registry_audit.apply_entity(_entity(), "test_project")
        maybe_init.assert_called_once()
        assert maybe_init.call_args.args[1] is not None

    def test_noop_apply_does_not_fetch_project_uuid(self, sqlite_registry_audit):
        sqlite_registry_audit.apply_entity(_entity(), "test_project")
        with patch.object(
            sqlite_registry_audit,
            "_project_uuid_if_auditing",
            wraps=sqlite_registry_audit._project_uuid_if_auditing,
        ) as fetch_uuid:
            sqlite_registry_audit.apply_entity(_entity(), "test_project")
        assert fetch_uuid.call_count == 0

    def test_delete_entity_mints_project_uuid_when_missing(self, sqlite_registry_audit):
        sqlite_registry_audit.apply_entity(_entity(), "test_project")
        _delete_project_uuid(sqlite_registry_audit)
        sqlite_registry_audit.delete_entity("test_entity", "test_project")
        delete = _audit_rows(sqlite_registry_audit, "test_entity")[-1]
        assert delete.operation == ObjectAuditLogOperation.DELETE.value
        assert delete.project_uuid
        with sqlite_registry_audit.write_engine.begin() as conn:
            row = conn.execute(
                select(feast_metadata.c.metadata_value).where(
                    feast_metadata.c.project_id == "test_project",
                    feast_metadata.c.metadata_key
                    == FeastMetadataKeys.PROJECT_UUID.value,
                )
            ).first()
        assert row is not None
        assert str(row._mapping["metadata_value"]) == delete.project_uuid

    def test_delete_project_mints_project_uuid_when_missing(
        self, sqlite_registry_audit
    ):
        sqlite_registry_audit.apply_entity(_entity(), "test_project")
        _delete_project_uuid(sqlite_registry_audit)
        sqlite_registry_audit.delete_project("test_project")
        entity_delete = _audit_rows(sqlite_registry_audit, "test_entity")[-1]
        project_delete = _audit_rows(
            sqlite_registry_audit,
            "test_project",
            ObjectAuditLogObjectType.PROJECTS.value,
        )[-1]
        assert entity_delete.operation == ObjectAuditLogOperation.DELETE.value
        assert project_delete.operation == ObjectAuditLogOperation.DELETE.value
        assert entity_delete.project_uuid
        assert entity_delete.project_uuid == project_delete.project_uuid


class TestSqlRegistrySortedFeatureView:
    """SortedFeatureView lookups and materialization through SqlRegistry. A
    SortedFeatureView is a FeatureView subclass persisted in its own table, so
    type dispatch and table fallbacks both have to account for it explicitly."""

    def test_get_any_feature_view_returns_sorted_feature_view(self, sqlite_registry):
        entity = Entity(name="driver", join_keys=["driver_id"])
        sqlite_registry.apply_entity(entity, "test_project")
        sqlite_registry.apply_feature_view(
            _sorted_feature_view("driver_sorted", entity), "test_project"
        )

        feature_view = sqlite_registry.get_any_feature_view(
            "driver_sorted", "test_project"
        )

        assert isinstance(feature_view, SortedFeatureView)
        assert [sort_key.name for sort_key in feature_view.sort_keys] == ["sort_key1"]

    def test_get_any_feature_view_returns_feature_view(self, sqlite_registry):
        entity = Entity(name="driver", join_keys=["driver_id"])
        sqlite_registry.apply_entity(entity, "test_project")
        sqlite_registry.apply_feature_view(
            FeatureView(
                name="driver_stats",
                entities=[entity],
                schema=[Field(name="conv_rate", dtype=Float32)],
                source=FileSource(path="some path", timestamp_field="event_timestamp"),
            ),
            "test_project",
        )

        feature_view = sqlite_registry.get_any_feature_view(
            "driver_stats", "test_project"
        )

        assert type(feature_view) is FeatureView

    def test_get_any_feature_view_unknown_name_raises(self, sqlite_registry):
        with pytest.raises(FeatureViewNotFoundException):
            sqlite_registry.get_any_feature_view("does_not_exist", "test_project")

    def test_delete_feature_view_updates_project_timestamp_once(self, sqlite_registry):
        entity = Entity(name="driver", join_keys=["driver_id"])
        sqlite_registry.apply_entity(entity, "test_project")
        sqlite_registry.apply_feature_view(
            _sorted_feature_view("driver_sorted", entity), "test_project"
        )
        with patch.object(
            sqlite_registry,
            "_update_project_timestamp",
            wraps=sqlite_registry._update_project_timestamp,
        ) as update_ts:
            sqlite_registry.delete_feature_view("driver_sorted", "test_project")
        assert update_ts.call_count == 1
        with pytest.raises(FeatureViewNotFoundException):
            sqlite_registry.get_any_feature_view("driver_sorted", "test_project")

    def test_delete_missing_feature_view_does_not_update_project_timestamp(
        self, sqlite_registry
    ):
        sqlite_registry.apply_entity(
            Entity(name="driver", join_keys=["driver_id"]), "test_project"
        )
        before = _project_last_updated(sqlite_registry)
        with patch.object(
            sqlite_registry,
            "_update_project_timestamp",
            wraps=sqlite_registry._update_project_timestamp,
        ) as update_ts:
            with pytest.raises(FeatureViewNotFoundException):
                sqlite_registry.delete_feature_view("does_not_exist", "test_project")
        assert update_ts.call_count == 0
        assert _project_last_updated(sqlite_registry) == before

    def test_apply_materialization_on_sorted_feature_view_preserves_sort_keys(
        self, sqlite_registry_custom_max_intervals
    ):
        registry, custom_max_intervals = sqlite_registry_custom_max_intervals
        entity = Entity(name="driver", join_keys=["driver_id"])
        registry.apply_entity(entity, "test_project")
        sorted_feature_view = _sorted_feature_view("driver_sorted", entity)
        registry.apply_feature_view(sorted_feature_view, "test_project")

        now = _utc_now()
        num_intervals = custom_max_intervals + 2
        for i in range(num_intervals):
            registry.apply_materialization(
                sorted_feature_view,
                "test_project",
                now - timedelta(days=num_intervals - i, hours=1),
                now - timedelta(days=num_intervals - i),
            )

        stored = registry.get_sorted_feature_view("driver_sorted", "test_project")
        assert isinstance(stored, SortedFeatureView)
        assert [sort_key.name for sort_key in stored.sort_keys] == ["sort_key1"]
        assert stored.sort_keys[0].default_sort_order == SortOrder.DESC
        assert len(stored.materialization_intervals) == custom_max_intervals
        # Still only in the sorted_feature_views table, not rewritten as a plain FeatureView.
        assert registry.list_feature_views("test_project") == []

        history = registry.get_materialization_interval_history(
            "driver_sorted", "test_project"
        )
        assert len(history) == num_intervals


class TestSqlFallbackRegistrySortedFeatureViewCache:
    """get_any_feature_view must cache a SortedFeatureView in the sorted cache,
    not the plain FeatureView cache."""

    def test_get_any_feature_view_caches_sorted_feature_view_in_sorted_cache(
        self, sqlite_fallback_registry
    ):
        entity = Entity(name="driver", join_keys=["driver_id"])
        sqlite_fallback_registry.apply_entity(entity, "test_project")
        sqlite_fallback_registry.apply_feature_view(
            _sorted_feature_view("driver_sorted", entity), "test_project"
        )

        feature_view = sqlite_fallback_registry.get_any_feature_view(
            "driver_sorted", "test_project"
        )

        assert isinstance(feature_view, SortedFeatureView)
        cached_sorted = sqlite_fallback_registry.cached_sorted_feature_views.get(
            "test_project", "driver_sorted"
        )
        assert isinstance(cached_sorted, SortedFeatureView)
        assert (
            sqlite_fallback_registry.cached_feature_views.get(
                "test_project", "driver_sorted"
            )
            is None
        )

    def test_cached_get_feature_view_does_not_return_sorted_feature_view(
        self, sqlite_fallback_registry
    ):
        entity = Entity(name="driver", join_keys=["driver_id"])
        sqlite_fallback_registry.apply_entity(entity, "test_project")
        sqlite_fallback_registry.apply_feature_view(
            _sorted_feature_view("driver_sorted", entity), "test_project"
        )
        sqlite_fallback_registry.get_any_feature_view("driver_sorted", "test_project")

        with pytest.raises(FeatureViewNotFoundException):
            sqlite_fallback_registry.get_feature_view(
                "driver_sorted", "test_project", allow_cache=True
            )

    def test_cached_get_any_feature_view_returns_sorted_feature_view(
        self, sqlite_fallback_registry
    ):
        entity = Entity(name="driver", join_keys=["driver_id"])
        sqlite_fallback_registry.apply_entity(entity, "test_project")
        sqlite_fallback_registry.apply_feature_view(
            _sorted_feature_view("driver_sorted", entity), "test_project"
        )
        sqlite_fallback_registry.get_any_feature_view("driver_sorted", "test_project")

        feature_view = sqlite_fallback_registry.get_any_feature_view(
            "driver_sorted", "test_project", allow_cache=True
        )

        assert isinstance(feature_view, SortedFeatureView)


class TestSqlFallbackRegistryObjectAuditLog:
    """SqlFallbackRegistry _delete_object forwards proto_field_name, writes
    audit rows, and evicts the per-table cache."""

    def test_delete_entity_writes_audit_and_evicts_cache(
        self, sqlite_fallback_registry_audit
    ):
        registry = sqlite_fallback_registry_audit
        registry.apply_entity(_entity(), "test_project")
        assert registry.get_entity("test_entity", "test_project").name == "test_entity"
        registry.delete_entity("test_entity", "test_project")
        rows = _audit_rows(registry, "test_entity")
        assert [row.operation for row in rows] == [
            ObjectAuditLogOperation.CREATE.value,
            ObjectAuditLogOperation.DELETE.value,
        ]
        assert rows[1].after_proto is None
        proto = _from_gzip_entity_proto(rows[1].before_proto)
        assert proto.spec.name == "test_entity"
        assert registry.cached_entities.get("test_project", "test_entity") is None
        with pytest.raises(EntityNotFoundException):
            registry.get_entity("test_entity", "test_project", allow_cache=True)

    def test_delete_data_source_writes_audit_and_evicts_cache(
        self, sqlite_fallback_registry_audit
    ):
        registry = sqlite_fallback_registry_audit
        source = FileSource(
            name="test_source",
            path="some path",
            timestamp_field="event_timestamp",
        )
        registry.apply_data_source(source, "test_project")
        assert registry.get_data_source("test_source", "test_project").name == (
            "test_source"
        )
        registry.delete_data_source("test_source", "test_project")
        rows = _audit_rows(
            registry,
            "test_source",
            ObjectAuditLogObjectType.DATA_SOURCES.value,
        )
        assert [row.operation for row in rows] == [
            ObjectAuditLogOperation.CREATE.value,
            ObjectAuditLogOperation.DELETE.value,
        ]
        assert rows[1].after_proto is None
        assert rows[1].before_proto is not None
        assert registry.cached_data_sources.get("test_project", "test_source") is None
        with pytest.raises(DataSourceObjectNotFoundException):
            registry.get_data_source("test_source", "test_project", allow_cache=True)

    def test_delete_permission_writes_audit_and_evicts_cache(
        self, sqlite_fallback_registry_audit
    ):
        registry = sqlite_fallback_registry_audit
        permission = Permission(
            name="read_permission",
            actions=AuthzedAction.DESCRIBE,
            policy=RoleBasedPolicy(roles=["reader"]),
            types=FeatureView,
        )
        registry.apply_permission(permission, "test_project")
        assert registry.get_permission("read_permission", "test_project").name == (
            "read_permission"
        )
        registry.delete_permission("read_permission", "test_project")
        rows = _audit_rows(
            registry,
            "read_permission",
            ObjectAuditLogObjectType.PERMISSIONS.value,
        )
        assert [row.operation for row in rows] == [
            ObjectAuditLogOperation.CREATE.value,
            ObjectAuditLogOperation.DELETE.value,
        ]
        assert rows[1].after_proto is None
        assert rows[1].before_proto is not None
        assert (
            registry.cached_permissions.get("test_project", "read_permission") is None
        )
        with pytest.raises(PermissionNotFoundException):
            registry.get_permission("read_permission", "test_project", allow_cache=True)
