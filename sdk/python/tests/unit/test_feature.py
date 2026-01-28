import pytest
from pydantic_core import ValidationError

from feast.field import Feature, Field
from feast.protos.feast.types import Value_pb2 as ValueProto
from feast.types import Array, Bool, Float32, Int32, Int64, String
from feast.value_type import ValueType


def test_feature_serialization_with_description():
    expected_description = "Average daily trips"
    feature = Feature(
        name="avg_daily_trips", dtype=ValueType.FLOAT, description=expected_description
    )
    serialized_feature = feature.to_proto()
    assert serialized_feature.description == expected_description


def test_field_serialization_with_description():
    expected_description = "Average daily trips"
    field = Field(
        name="avg_daily_trips", dtype=Float32, description=expected_description
    )
    feature = Feature(
        name="avg_daily_trips", dtype=ValueType.FLOAT, description=expected_description
    )
    serialized_field = field.to_proto()
    field_from_feature = Field.from_feature(feature)
    assert serialized_field.description == expected_description
    assert field_from_feature.description == expected_description
    field = Field.from_proto(serialized_field)
    assert field.description == expected_description


def test_field_with_default_value_to_proto():
    default_val = ValueProto.Value(int32_val=42)
    field = Field(name="age", dtype=Int32, default_value=default_val)
    proto = field.to_proto()
    assert proto.name == "age"
    assert proto.HasField("default_value")
    assert proto.default_value.int32_val == 42


def test_field_without_default_value_to_proto():
    field = Field(name="age", dtype=Int32)
    proto = field.to_proto()
    assert proto.name == "age"
    assert not proto.HasField("default_value")


def test_field_from_proto_with_default_value():
    from feast.protos.feast.core.Feature_pb2 import FeatureSpecV2

    default_val = ValueProto.Value(string_val="unknown")
    proto = FeatureSpecV2(
        name="country",
        value_type=2,  # STRING
        default_value=default_val,
    )
    field = Field.from_proto(proto)
    assert field.name == "country"
    assert field.default_value is not None
    assert field.default_value.string_val == "unknown"


def test_field_from_proto_without_default_value():
    from feast.protos.feast.core.Feature_pb2 import FeatureSpecV2

    proto = FeatureSpecV2(name="country", value_type=2)
    field = Field.from_proto(proto)
    assert field.name == "country"
    assert field.default_value is None


def test_field_roundtrip_with_default_value():
    default_val = ValueProto.Value(int64_val=9999)
    original_field = Field(name="user_id", dtype=Int64, default_value=default_val)
    proto = original_field.to_proto()
    restored_field = Field.from_proto(proto)
    assert restored_field.name == original_field.name
    assert restored_field.dtype == original_field.dtype
    assert restored_field.default_value.int64_val == 9999


def test_field_default_value_type_validation():
    with pytest.raises(ValidationError, match="does not match field dtype"):
        default_val = ValueProto.Value(string_val="not_an_int")
        Field(name="age", dtype=Int32, default_value=default_val)


def test_field_with_list_default_value():
    default_val = ValueProto.Value(int32_list_val=ValueProto.Int32List(val=[1, 2, 3]))
    field = Field(name="scores", dtype=Array(Int32), default_value=default_val)
    assert list(field.default_value.int32_list_val.val) == [1, 2, 3]


def test_feature_with_default_value_to_proto():
    default_val = ValueProto.Value(int32_val=0)
    feature = Feature(name="count", dtype=ValueType.INT32, default_value=default_val)
    proto = feature.to_proto()
    assert proto.name == "count"
    assert proto.HasField("default_value")
    assert proto.default_value.int32_val == 0


def test_feature_without_default_value_to_proto():
    feature = Feature(name="count", dtype=ValueType.INT32)
    proto = feature.to_proto()
    assert proto.name == "count"
    assert not proto.HasField("default_value")


def test_feature_from_proto_with_default_value():
    from feast.protos.feast.core.Feature_pb2 import FeatureSpecV2

    default_val = ValueProto.Value(bool_val=False)
    proto = FeatureSpecV2(
        name="is_active",
        value_type=7,  # BOOL
        default_value=default_val,
    )
    feature = Feature.from_proto(proto)
    assert feature.name == "is_active"
    assert feature.default_value is not None
    assert feature.default_value.bool_val is False


def test_feature_from_proto_without_default_value():
    from feast.protos.feast.core.Feature_pb2 import FeatureSpecV2

    proto = FeatureSpecV2(name="is_active", value_type=7)
    feature = Feature.from_proto(proto)
    assert feature.name == "is_active"
    assert feature.default_value is None


def test_feature_roundtrip_with_default_value():
    default_val = ValueProto.Value(string_val="US")
    original_feature = Feature(
        name="country", dtype=ValueType.STRING, default_value=default_val
    )
    proto = original_feature.to_proto()
    restored_feature = Feature.from_proto(proto)
    assert restored_feature.name == original_feature.name
    assert restored_feature.dtype == original_feature.dtype
    assert restored_feature.default_value.string_val == "US"


def test_backward_compatibility_field_from_feature():
    default_val = ValueProto.Value(int32_val=18)
    feature = Feature(name="age", dtype=ValueType.INT32, default_value=default_val)
    field = Field.from_feature(feature)
    assert field.name == "age"
    assert field.default_value is not None
    assert field.default_value.int32_val == 18


def test_field_default_value_edge_cases():
    # Zero value
    field1 = Field(
        name="count", dtype=Int32, default_value=ValueProto.Value(int32_val=0)
    )
    assert field1.default_value.int32_val == 0
    # Empty string
    field2 = Field(
        name="name", dtype=String, default_value=ValueProto.Value(string_val="")
    )
    assert field2.default_value.string_val == ""
    # False boolean
    field3 = Field(
        name="flag", dtype=Bool, default_value=ValueProto.Value(bool_val=False)
    )
    assert field3.default_value.bool_val is False
    # Negative number
    field4 = Field(
        name="error", dtype=Int64, default_value=ValueProto.Value(int64_val=-1)
    )
    assert field4.default_value.int64_val == -1
