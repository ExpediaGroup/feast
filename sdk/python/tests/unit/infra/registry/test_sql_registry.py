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

import tempfile
import threading
from datetime import timedelta

import pytest

from feast.entity import Entity
from feast.feature_view import MATERIALIZATION_INTERVALS_MAX_LEN, FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.registry.sql import SqlRegistry, SqlRegistryConfig
from feast.types import Float32
from feast.utils import _utc_now


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

        with pytest.raises(Exception):
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
