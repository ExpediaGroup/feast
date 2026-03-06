"""
Tests for is_update_compatible_with across all Feast object types.
"""

import pytest

from feast.data_source import DataSource
from feast.entity import Entity
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.permissions.permission import Permission
from feast.project import Project
from feast.types import Float32, Int64, String
from feast.value_type import ValueType


# ---------------------------------------------------------------------------
# Entity
# ---------------------------------------------------------------------------


def test_entity_compatible_same():
    e1 = Entity(name="user", join_keys=["user_id"], value_type=ValueType.STRING)
    e2 = Entity(name="user", join_keys=["user_id"], value_type=ValueType.STRING)
    ok, reasons = e1.is_update_compatible_with(e2)
    assert ok
    assert reasons == []


def test_entity_incompatible_join_key_change():
    e1 = Entity(name="user", join_keys=["user_id"], value_type=ValueType.STRING)
    e2 = Entity(name="user", join_keys=["account_id"], value_type=ValueType.STRING)
    ok, reasons = e1.is_update_compatible_with(e2)
    assert not ok
    assert any("join_key cannot change" in r for r in reasons)


def test_entity_incompatible_value_type_change():
    e1 = Entity(name="user", join_keys=["user_id"], value_type=ValueType.STRING)
    e2 = Entity(name="user", join_keys=["user_id"], value_type=ValueType.INT64)
    ok, reasons = e1.is_update_compatible_with(e2)
    assert not ok
    assert any("value_type cannot change" in r for r in reasons)


def test_entity_compatible_unknown_to_typed():
    """Changing from UNKNOWN to a concrete type is allowed."""
    e1 = Entity(name="user", join_keys=["user_id"], value_type=ValueType.UNKNOWN)
    e2 = Entity(name="user", join_keys=["user_id"], value_type=ValueType.STRING)
    ok, reasons = e1.is_update_compatible_with(e2)
    assert ok
    assert reasons == []


# ---------------------------------------------------------------------------
# FeatureService
# ---------------------------------------------------------------------------


def test_feature_service_always_compatible():
    source = FileSource(path="dummy")
    fv = FeatureView(
        name="fv",
        source=source,
        entities=[],
        schema=[Field(name="f1", dtype=Float32)],
    )
    fs1 = FeatureService(name="fs", features=[fv])
    fs2 = FeatureService(name="fs", features=[fv])
    ok, reasons = fs1.is_update_compatible_with(fs2)
    assert ok
    assert reasons == []


# ---------------------------------------------------------------------------
# OnDemandFeatureView
# ---------------------------------------------------------------------------


def test_odfv_compatible_same():
    source = FileSource(path="dummy")
    fv = FeatureView(
        name="fv",
        source=source,
        entities=[],
        schema=[Field(name="f1", dtype=Float32)],
    )
    odfv1 = OnDemandFeatureView(
        name="odfv",
        sources=[fv],
        schema=[Field(name="out1", dtype=Float32)],
        mode="python",
        udf=lambda x: x,
    )
    odfv2 = OnDemandFeatureView(
        name="odfv",
        sources=[fv],
        schema=[Field(name="out1", dtype=Float32)],
        mode="python",
        udf=lambda x: x,
    )
    ok, reasons = odfv1.is_update_compatible_with(odfv2)
    assert ok
    assert reasons == []


# ---------------------------------------------------------------------------
# DataSource
# ---------------------------------------------------------------------------


def test_data_source_compatible_same():
    ds1 = FileSource(name="src", path="path.parquet", timestamp_field="ts")
    ds2 = FileSource(name="src", path="path.parquet", timestamp_field="ts")
    ok, reasons = ds1.is_update_compatible_with(ds2)
    assert ok
    assert reasons == []


def test_data_source_incompatible_timestamp_field_change():
    ds1 = FileSource(name="src", path="path.parquet", timestamp_field="ts")
    ds2 = FileSource(name="src", path="path.parquet", timestamp_field="event_time")
    ok, reasons = ds1.is_update_compatible_with(ds2)
    assert not ok
    assert any("timestamp_field cannot change" in r for r in reasons)


def test_data_source_compatible_when_no_timestamp():
    """If original has no timestamp_field, change is allowed."""
    ds1 = FileSource(name="src", path="path.parquet")
    ds2 = FileSource(name="src", path="path.parquet", timestamp_field="ts")
    ok, reasons = ds1.is_update_compatible_with(ds2)
    assert ok
    assert reasons == []


# ---------------------------------------------------------------------------
# Permission
# ---------------------------------------------------------------------------


def test_permission_always_compatible():
    p1 = Permission(name="perm")
    p2 = Permission(name="perm")
    ok, reasons = p1.is_update_compatible_with(p2)
    assert ok
    assert reasons == []


# ---------------------------------------------------------------------------
# Project
# ---------------------------------------------------------------------------


def test_project_always_compatible():
    p1 = Project(name="proj", description="old")
    p2 = Project(name="proj", description="new")
    ok, reasons = p1.is_update_compatible_with(p2)
    assert ok
    assert reasons == []
