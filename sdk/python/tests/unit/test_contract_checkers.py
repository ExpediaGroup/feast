import pytest

from feast import Entity, FeatureView, Field, FileSource, SortedFeatureView, ValueType
from feast.contract_checkers import (
    FeatureViewContractChecker,
    SortedFeatureViewContractChecker,
)
from feast.sort_key import SortKey
from feast.types import Float64, Int64, String


def make_fv(name, fields, entities, ttl):
    ds = FileSource(path="file:///dummy.parquet", event_timestamp_column="ts")
    return FeatureView(
        name=name,
        entities=entities,
        ttl=ttl,
        schema=[Field(name=fname, dtype=ftype) for fname, ftype in fields],
        source=ds,
    )


def make_sfv(name, fields, entities, ttl, sort_keys):
    ds = FileSource(path="file:///dummy.parquet", event_timestamp_column="ts")
    return SortedFeatureView(
        name=name,
        entities=entities,
        ttl=ttl,
        schema=[Field(name=fname, dtype=ftype) for fname, ftype in fields],
        source=ds,
        sort_keys=sort_keys,
    )


@pytest.fixture
def entity_e1():
    return Entity(name="e1", join_keys=["e1"], value_type=ValueType.INT64)


# Tests for FeatureViewContractChecker
def test_fv_no_changes_passes(entity_e1):
    fv = make_fv("fv1", [("f1", Float64)], [entity_e1], None)
    ok, reasons = FeatureViewContractChecker.check(fv, fv)
    assert ok and reasons == []


def test_fv_remove_non_entity_field_warns(caplog, entity_e1):
    old = make_fv("fv1", [("f1", Float64), ("f2", String)], [entity_e1], None)
    new = make_fv("fv1", [("f1", Float64)], [entity_e1], None)
    caplog.set_level("WARNING")
    ok, reasons = FeatureViewContractChecker.check(new, old)
    assert ok and reasons == []
    assert "Feature 'f2' removed from FeatureView 'fv1'." in caplog.text


def test_fv_remove_entity_field_fails(entity_e1):
    old = make_fv("fv1", [("e1", Int64), ("f2", String)], [entity_e1], None)
    new = make_fv("fv1", [("f2", String)], [entity_e1], None)
    ok, reasons = FeatureViewContractChecker.check(new, old)
    assert not ok
    assert any("entity key and cannot be removed" in r for r in reasons)


def test_fv_change_dtype_fails(entity_e1):
    old = make_fv("fv1", [("f1", Float64)], [entity_e1], None)
    new = make_fv("fv1", [("f1", String)], [entity_e1], None)
    ok, reasons = FeatureViewContractChecker.check(new, old)
    assert not ok
    assert any("type changed" in r for r in reasons)


# Tests for SortedFeatureViewContractChecker
def test_sfv_remove_non_key_warns(caplog, entity_e1):
    key = SortKey(name="k1", value_type=ValueType.INT64, default_sort_order=0)
    old = make_sfv(
        "sfv1", [("k1", Int64), ("f2", String)], [entity_e1], None, sort_keys=[key]
    )
    new = make_sfv("sfv1", [("k1", Int64)], [entity_e1], None, sort_keys=[key])
    caplog.set_level("WARNING")
    ok, reasons = SortedFeatureViewContractChecker.check(new, old)
    assert ok and reasons == []
    assert "Feature 'f2' removed from SortedFeatureView 'sfv1'." in caplog.text


def test_sfv_remove_entity_key_fails(entity_e1):
    key = SortKey(name="k1", value_type=ValueType.INT64, default_sort_order=0)
    old = make_sfv(
        "sfv1",
        [("e1", Int64), ("f2", String), ("k1", Int64)],
        [entity_e1],
        None,
        sort_keys=[key],
    )
    new = make_sfv(
        "sfv1", [("f2", String), ("k1", Int64)], [entity_e1], None, sort_keys=[key]
    )
    ok, reasons = SortedFeatureViewContractChecker.check(new, old)
    assert not ok
    assert any("entity key and cannot be removed" in r for r in reasons)


def test_sfv_change_sort_keys_fails(entity_e1):
    key1 = SortKey(name="k1", value_type=ValueType.INT64, default_sort_order=0)
    key2 = SortKey(name="k2", value_type=ValueType.INT64, default_sort_order=0)
    old = make_sfv("sfv1", [("k1", Int64)], [entity_e1], None, sort_keys=[key1])
    new = make_sfv("sfv1", [("k2", Int64)], [entity_e1], None, sort_keys=[key2])
    ok, reasons = SortedFeatureViewContractChecker.check(new, old)
    assert not ok
    assert any("sort key change detected" in r for r in reasons)
