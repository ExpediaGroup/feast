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
Tests for the file-based Registry's materialization-interval-history support
-- the same durable, uncapped history as SqlRegistry's separate table, but
stored as a top-level repeated field on the Registry proto blob (see
feast.core.Registry.materialization_interval_history).
"""

import os
import tempfile
from datetime import timedelta

import pytest

from feast.entity import Entity
from feast.feature_view import MATERIALIZATION_INTERVALS_MAX_LEN, FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.registry.registry import Registry
from feast.repo_config import RegistryConfig
from feast.types import Float32
from feast.utils import _utc_now


@pytest.fixture
def file_registry_custom_max_intervals():
    """Create a temporary file-based registry with a custom materialization
    intervals cap, to verify apply_materialization/apply_feature_view honor
    it end-to-end, same as the SQL registry's equivalent fixture."""
    tmpdir = tempfile.mkdtemp()
    registry_path = os.path.join(tmpdir, "registry.db")
    custom_max_intervals = 3
    assert custom_max_intervals != MATERIALIZATION_INTERVALS_MAX_LEN
    registry_config = RegistryConfig(
        registry_type="file",
        path=registry_path,
        materialization_intervals_max_len=custom_max_intervals,
    )
    registry = Registry("test_project", registry_config, None)
    yield registry, custom_max_intervals
    registry.teardown()


def _apply_feature_view(registry, name, entity):
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


class TestFileRegistryMaterializationIntervalHistory:
    def test_apply_materialization_honors_configured_max_intervals(
        self, file_registry_custom_max_intervals
    ):
        registry, custom_max_intervals = file_registry_custom_max_intervals
        entity = Entity(name="driver", description="Driver entity")
        registry.apply_entity(entity, "test_project")
        feature_view = _apply_feature_view(registry, "driver_stats", entity)

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

    def test_apply_materialization_archives_new_and_dropped_intervals(
        self, file_registry_custom_max_intervals
    ):
        registry, custom_max_intervals = file_registry_custom_max_intervals
        entity = Entity(name="driver", description="Driver entity")
        registry.apply_entity(entity, "test_project")
        feature_view = _apply_feature_view(registry, "driver_stats", entity)

        now = _utc_now()
        num_intervals = custom_max_intervals + 5
        for i in range(num_intervals):
            registry.apply_materialization(
                feature_view,
                "test_project",
                now - timedelta(days=num_intervals - i, hours=1),
                now - timedelta(days=num_intervals - i),
            )

        history = registry.get_materialization_interval_history(
            "driver_stats", "test_project"
        )
        assert len(history) == num_intervals
        start_times = [entry.start_time.ToSeconds() for entry in history]
        assert start_times == sorted(start_times)

    def test_apply_materialization_is_idempotent_in_history(
        self, file_registry_custom_max_intervals
    ):
        registry, custom_max_intervals = file_registry_custom_max_intervals
        entity = Entity(name="driver", description="Driver entity")
        registry.apply_entity(entity, "test_project")
        feature_view = _apply_feature_view(registry, "driver_stats", entity)

        now = _utc_now()
        intervals = [
            (now - timedelta(days=i, hours=1), now - timedelta(days=i))
            for i in range(custom_max_intervals + 2)
        ]
        for start, end in intervals:
            registry.apply_materialization(feature_view, "test_project", start, end)
        for start, end in intervals:
            registry.apply_materialization(feature_view, "test_project", start, end)

        history = registry.get_materialization_interval_history(
            "driver_stats", "test_project"
        )
        assert len(history) == len(intervals)

    def test_apply_feature_view_backfills_history_for_pre_existing_over_cap_list(
        self, file_registry_custom_max_intervals
    ):
        """A feature view that already has more intervals than the cap (e.g.
        seeded outside of add_materialization_interval, or persisted before
        a cap was ever enforced) gets its full pre-existing interval list
        archived to history the next time it's re-applied."""
        registry, custom_max_intervals = file_registry_custom_max_intervals
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

        assert (
            registry.get_materialization_interval_history(
                "driver_stats", "test_project"
            )
            == []
        )

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
        self, file_registry_custom_max_intervals
    ):
        registry, _ = file_registry_custom_max_intervals
        entity = Entity(name="driver", description="Driver entity")
        registry.apply_entity(entity, "test_project")
        fv1 = _apply_feature_view(registry, "fv1", entity)
        fv2 = _apply_feature_view(registry, "fv2", entity)

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
