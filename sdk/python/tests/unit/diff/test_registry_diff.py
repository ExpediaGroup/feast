import copy
from datetime import timedelta

import pandas as pd
import pytest

from feast import Field, PushSource
from feast.diff.registry_diff import (
    diff_registry_objects,
    tag_objects_for_keep_delete_update_add,
)
from feast.entity import Entity
from feast.feast_object import ALL_RESOURCE_TYPES
from feast.feature_view import FeatureView
from feast.on_demand_feature_view import on_demand_feature_view
from feast.permissions.action import AuthzedAction
from feast.permissions.permission import Permission
from feast.permissions.policy import RoleBasedPolicy
from feast.protos.feast.core.SortedFeatureView_pb2 import SortOrder
from feast.sort_key import SortKey
from feast.sorted_feature_view import SortedFeatureView
from feast.types import Int64, String
from feast.value_type import ValueType
from tests.utils.data_source_test_creator import prep_file_source


@pytest.fixture
def sorted_feature_view_fixture(simple_dataset_1):
    with prep_file_source(df=simple_dataset_1, timestamp_field="ts_1") as file_source:
        entity = Entity(name="id", join_keys=["id"])
        schema = [Field(name="sort_key_1", dtype=Int64)]
        sort_key1 = SortKey(
            name="sort_key_1",
            value_type=ValueType.INT64,
            default_sort_order=SortOrder.ASC,
            tags={"k": "v"},
            description="initial",
        )
        sfv = SortedFeatureView(
            name="sorted_fv",
            entities=[entity],
            schema=schema,
            source=file_source,
            sort_keys=[sort_key1],
        )
        yield sfv


@pytest.fixture
def modified_sorted_feature_view(sorted_feature_view_fixture):
    # Create a modified copy with a different sort key default order.
    sfv2 = copy.copy(sorted_feature_view_fixture)
    sort_key_modified = SortKey(
        name="sort_key_1",
        value_type=sorted_feature_view_fixture.sort_keys[0].value_type,
        default_sort_order=SortOrder.DESC,
        tags=sorted_feature_view_fixture.sort_keys[0].tags,
        description=sorted_feature_view_fixture.sort_keys[0].description,
    )
    sfv2.sort_keys = [sort_key_modified]
    return sfv2


def test_sorted_feature_view_diff_non_sort_key_ttl(sorted_feature_view_fixture):
    # Create a modified copy with an updated TTL.
    modified = copy.copy(sorted_feature_view_fixture)
    modified.ttl = timedelta(days=1)
    diff = diff_registry_objects(
        sorted_feature_view_fixture, modified, "sorted feature view"
    )
    non_sort_diffs = [
        d for d in diff.feast_object_property_diffs if d.property_name == "ttl"
    ]
    assert len(non_sort_diffs) >= 1, "Expected a diff in the ttl field"


def test_sorted_feature_view_no_diff(sorted_feature_view_fixture):
    # When comparing the same object, no diffs should be detected.
    diff = diff_registry_objects(
        sorted_feature_view_fixture, sorted_feature_view_fixture, "sorted feature view"
    )
    assert len(diff.feast_object_property_diffs) == 0


def test_sorted_feature_view_diff(
    modified_sorted_feature_view, sorted_feature_view_fixture
):
    # When the sort key default sort order changes, a diff should be detected.
    diff = diff_registry_objects(
        sorted_feature_view_fixture, modified_sorted_feature_view, "sorted feature view"
    )
    assert len(diff.feast_object_property_diffs) >= 1
    sort_key_diffs = [
        p for p in diff.feast_object_property_diffs if "sort_keys" in p.property_name
    ]
    assert len(sort_key_diffs) > 0
    assert any("default_sort_order" in p.property_name for p in sort_key_diffs)


def test_tag_objects_for_keep_delete_update_add(simple_dataset_1):
    with prep_file_source(df=simple_dataset_1, timestamp_field="ts_1") as file_source:
        entity = Entity(name="id", join_keys=["id"])
        to_delete = FeatureView(
            name="to_delete",
            entities=[entity],
            source=file_source,
        )
        unchanged_fv = FeatureView(
            name="fv1",
            entities=[entity],
            source=file_source,
        )
        pre_changed = FeatureView(
            name="fv2",
            entities=[entity],
            source=file_source,
            tags={"when": "before"},
        )
        post_changed = FeatureView(
            name="fv2",
            entities=[entity],
            source=file_source,
            tags={"when": "after"},
        )
        to_add = FeatureView(
            name="to_add",
            entities=[entity],
            source=file_source,
        )

        keep, delete, update, add = tag_objects_for_keep_delete_update_add(
            [unchanged_fv, pre_changed, to_delete], [unchanged_fv, post_changed, to_add]
        )

        assert len(list(keep)) == 2
        assert unchanged_fv in keep
        assert pre_changed in keep
        assert post_changed not in keep
        assert len(list(delete)) == 1
        assert to_delete in delete
        assert len(list(update)) == 2
        assert unchanged_fv in update
        assert post_changed in update
        assert pre_changed not in update
        assert len(list(add)) == 1
        assert to_add in add


def test_diff_registry_objects_feature_views(simple_dataset_1):
    with prep_file_source(df=simple_dataset_1, timestamp_field="ts_1") as file_source:
        entity = Entity(name="id", join_keys=["id"])
        pre_changed = FeatureView(
            name="fv2",
            entities=[entity],
            source=file_source,
            tags={"when": "before"},
        )
        post_changed = FeatureView(
            name="fv2",
            entities=[entity],
            source=file_source,
            tags={"when": "after"},
        )

        feast_object_diffs = diff_registry_objects(
            pre_changed, pre_changed, "feature view"
        )
        assert len(feast_object_diffs.feast_object_property_diffs) == 0

        feast_object_diffs = diff_registry_objects(
            pre_changed, post_changed, "feature view"
        )
        assert len(feast_object_diffs.feast_object_property_diffs) == 1

        assert feast_object_diffs.feast_object_property_diffs[0].property_name == "tags"
        assert feast_object_diffs.feast_object_property_diffs[0].val_existing == {
            "when": "before"
        }
        assert feast_object_diffs.feast_object_property_diffs[0].val_declared == {
            "when": "after"
        }


def test_diff_odfv(simple_dataset_1):
    with prep_file_source(df=simple_dataset_1, timestamp_field="ts_1") as file_source:
        entity = Entity(name="id", join_keys=["id"])
        fv = FeatureView(
            name="fv2",
            entities=[entity],
            source=file_source,
            tags={"when": "before"},
        )

        @on_demand_feature_view(
            sources=[fv],
            schema=[Field(name="first_char", dtype=String)],
        )
        def pre_changed(inputs: pd.DataFrame) -> pd.DataFrame:
            df = pd.DataFrame()
            df["first_char"] = inputs["string_col"].str[:1].astype("string")
            return df

        @on_demand_feature_view(
            sources=[fv],
            schema=[Field(name="first_char", dtype=String)],
        )
        def post_changed(inputs: pd.DataFrame) -> pd.DataFrame:
            df = pd.DataFrame()
            df["first_char"] = inputs["string_col"].str[:1].astype("string") + "hi"
            return df

        feast_object_diffs = diff_registry_objects(
            pre_changed, pre_changed, "on demand feature view"
        )
        assert len(feast_object_diffs.feast_object_property_diffs) == 0

        feast_object_diffs = diff_registry_objects(
            pre_changed, post_changed, "on demand feature view"
        )

        # Note that user_defined_function.body is excluded because it always changes (dill is non-deterministic), even
        # if no code is changed
        assert len(feast_object_diffs.feast_object_property_diffs) == 3
        assert feast_object_diffs.feast_object_property_diffs[0].property_name == "name"
        # Note we should only now be looking at changes for the feature_transformation field
        assert (
            feast_object_diffs.feast_object_property_diffs[1].property_name
            == "feature_transformation.name"
        )
        assert (
            feast_object_diffs.feast_object_property_diffs[2].property_name
            == "feature_transformation.body_text"
        )


def test_diff_registry_objects_batch_to_push_source(simple_dataset_1):
    with prep_file_source(df=simple_dataset_1, timestamp_field="ts_1") as file_source:
        entity = Entity(name="id", join_keys=["id"])
        pre_changed = FeatureView(
            name="fv2",
            entities=[entity],
            source=file_source,
        )
        post_changed = FeatureView(
            name="fv2",
            entities=[entity],
            source=PushSource(name="push_source", batch_source=file_source),
        )

        feast_object_diffs = diff_registry_objects(
            pre_changed, post_changed, "feature view"
        )
        assert len(feast_object_diffs.feast_object_property_diffs) == 1
        assert (
            feast_object_diffs.feast_object_property_diffs[0].property_name
            == "stream_source"
        )


def test_diff_registry_objects_permissions():
    pre_changed = Permission(
        name="reader",
        types=ALL_RESOURCE_TYPES,
        policy=RoleBasedPolicy(roles=["reader"]),
        actions=[AuthzedAction.DESCRIBE],
    )
    post_changed = Permission(
        name="reader",
        types=ALL_RESOURCE_TYPES,
        policy=RoleBasedPolicy(roles=["reader"]),
        actions=[AuthzedAction.CREATE],
    )

    feast_object_diffs = diff_registry_objects(pre_changed, post_changed, "permission")
    assert len(feast_object_diffs.feast_object_property_diffs) == 1
    assert feast_object_diffs.feast_object_property_diffs[0].property_name == "actions"
