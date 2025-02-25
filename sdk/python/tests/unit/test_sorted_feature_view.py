import copy
from datetime import timedelta

import pytest

from feast import FileSource
from feast.entity import Entity
from feast.field import Field
from feast.protos.feast.core.SortedFeatureView_pb2 import SortOrder
from feast.sort_key import SortKey
from feast.sorted_feature_view import SortedFeatureView
from feast.types import Float32
from feast.utils import _utc_now, make_tzaware
from feast.value_type import ValueType


def test_sorted_feature_view_to_proto_and_from_proto():
    """
    Test round-trip conversion:
      - Create a SortedFeatureView with a sort key.
      - Convert it to its proto representation.
      - Convert back from proto.
      - Verify that key attributes (name, description, tags, owner, sort keys, etc.) are preserved.
    """
    source = FileSource(path="some path")
    entity = Entity(name="entity1", join_keys=["entity1_id"])

    sort_key = SortKey(
        name="sort_key1", value_type=ValueType.INT64, default_sort_order=SortOrder.ASC
    )

    sfv = SortedFeatureView(
        name="sorted_feature_view_test",
        source=source,
        entities=[entity],
        ttl=timedelta(days=1),
        sort_keys=[sort_key],
        description="test sorted feature view",
        tags={"test": "true"},
        owner="test_owner",
    )

    proto = sfv.to_proto()
    sfv_from_proto = SortedFeatureView.from_proto(proto)

    assert sfv.name == sfv_from_proto.name
    assert sfv.description == sfv_from_proto.description
    assert sfv.tags == sfv_from_proto.tags
    assert sfv.owner == sfv_from_proto.owner

    assert len(sfv.sort_keys) == len(sfv_from_proto.sort_keys)
    for original_sk, proto_sk in zip(sfv.sort_keys, sfv_from_proto.sort_keys):
        assert original_sk.name == proto_sk.name
        assert original_sk.default_sort_order == proto_sk.default_sort_order
        assert original_sk.value_type == proto_sk.value_type


def test_sorted_feature_view_ensure_valid():
    """
    Test that a SortedFeatureView without any sort keys fails validation.
    """
    source = FileSource(path="some path")
    entity = Entity(name="entity1", join_keys=["entity1_id"])

    sfv = SortedFeatureView(
        name="invalid_sorted_feature_view",
        source=source,
        entities=[entity],
        sort_keys=[],
    )

    with pytest.raises(ValueError) as excinfo:
        sfv.ensure_valid()
    assert "must have at least one sort key defined" in str(excinfo.value)


def test_sorted_feature_view_ensure_valid_sort_key_in_entity_columns():
    """
    Test that a SortedFeatureView fails validation if any sort key's name is part of the entity columns.
    """
    source = FileSource(path="some path")
    entity = Entity(name="entity1", join_keys=["entity1_id"])
    entity_field = Field(name="entity1", dtype=Float32)

    sort_key = SortKey(
        name="entity1",
        value_type=ValueType.STRING,
        default_sort_order=SortOrder.ASC,  # Assuming ASC is valid.
    )

    # Create a SortedFeatureView with a sort key that conflicts.
    sfv = SortedFeatureView(
        name="invalid_sorted_feature_view",
        source=source,
        entities=[entity],
        sort_keys=[sort_key],
    )

    sfv.entity_columns = [entity_field]

    with pytest.raises(ValueError) as excinfo:
        sfv.ensure_valid()
    assert "Sort key entity1 cannot be part of entity columns" in str(excinfo.value)


def test_sorted_feature_view_copy():
    """
    Test that __copy__ produces a valid and independent copy of a SortedFeatureView.
    """
    source = FileSource(path="some path")
    entity = Entity(name="entity1", join_keys=["entity1_id"])

    sort_key = SortKey(
        name="dummy_sort_key",
        value_type=ValueType.STRING,
        default_sort_order=SortOrder.ASC,
    )

    sfv = SortedFeatureView(
        name="sorted_feature_view_test",
        source=source,
        entities=[entity],
        ttl=timedelta(days=1),
        sort_keys=[sort_key],
        description="Test sorted feature view",
        tags={"test": "true"},
        owner="test_owner",
    )

    sfv_copy = copy.copy(sfv)
    # Check that the copied object's attributes match.
    assert sfv.name == sfv_copy.name
    assert sfv.sort_keys == sfv_copy.sort_keys
    assert sfv.features == sfv_copy.features
    # Check that modifying the copy does not affect the original.
    sfv_copy.tags["new_key"] = "new_value"
    assert "new_key" not in sfv.tags


def test_sorted_feature_view_materialization_intervals_update():
    """
    Test that the update_materialization_intervals method correctly updates intervals.
    """
    source = FileSource(path="dummy/path")
    entity = Entity(name="entity1", join_keys=["entity1_id"])

    sfv = SortedFeatureView(
        name="sorted_feature_view_test",
        source=source,
        entities=[entity],
        ttl=timedelta(days=1),
        sort_keys=[
            SortKey(
                name="dummy",
                value_type=ValueType.STRING,
                default_sort_order=SortOrder.ASC,
            )
        ],
    )

    assert len(sfv.materialization_intervals) == 0

    # Add one interval.
    current_time = _utc_now()
    start_date = make_tzaware(current_time - timedelta(days=1))
    end_date = make_tzaware(current_time)
    sfv.materialization_intervals.append((start_date, end_date))

    new_sfv = SortedFeatureView(
        name="sorted_feature_view_updated",
        source=source,
        entities=[entity],
        ttl=timedelta(days=1),
        sort_keys=[
            SortKey(
                name="dummy",
                value_type=ValueType.STRING,
                default_sort_order=SortOrder.ASC,
            )
        ],
    )
    new_sfv.update_materialization_intervals(sfv.materialization_intervals)
    assert len(new_sfv.materialization_intervals) == 1
    assert new_sfv.materialization_intervals[0] == (start_date, end_date)
