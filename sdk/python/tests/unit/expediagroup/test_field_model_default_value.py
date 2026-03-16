"""
Unit tests for FieldModel default_value JSON serialization.

Tests cover:
- Serialization of proto Value to JSON dict
- Deserialization from JSON dict to proto Value
- Roundtrip (serialize -> deserialize)
- Field bridge methods (to_field/from_field)
- Full roundtrip (Field -> FieldModel -> JSON -> FieldModel -> Field)
"""

from feast.expediagroup.pydantic_models.field_model import FieldModel
from feast.field import Field
from feast.protos.feast.types.Value_pb2 import Value
from feast.types import Bool, Float64, Int64, String


def test_field_model_serialize_int64_default():
    """FieldModel with Int64 default_value serializes to dict with int64Val."""
    fm = FieldModel(name="age", dtype=Int64, default_value=Value(int64_val=42))
    d = fm.model_dump(mode='json')

    assert d["default_value"] is not None
    # Proto JSON format represents int64 as string to preserve precision
    assert d["default_value"] == {"int64Val": "42"}


def test_field_model_serialize_string_default():
    """FieldModel with String default_value serializes to dict with stringVal."""
    fm = FieldModel(name="country", dtype=String, default_value=Value(string_val="US"))
    d = fm.model_dump(mode='json')

    assert d["default_value"] is not None
    assert d["default_value"] == {"stringVal": "US"}


def test_field_model_serialize_double_default():
    """FieldModel with Float64 default_value serializes to dict with doubleVal."""
    fm = FieldModel(name="rating", dtype=Float64, default_value=Value(double_val=2.718))
    d = fm.model_dump(mode='json')

    assert d["default_value"] is not None
    assert d["default_value"] == {"doubleVal": 2.718}


def test_field_model_serialize_bool_default():
    """FieldModel with Bool default_value serializes to dict with boolVal."""
    fm = FieldModel(name="is_active", dtype=Bool, default_value=Value(bool_val=True))
    d = fm.model_dump(mode='json')

    assert d["default_value"] is not None
    assert d["default_value"] == {"boolVal": True}


def test_field_model_serialize_none_default():
    """FieldModel without default_value serializes default_value as None."""
    fm = FieldModel(name="optional_field", dtype=String)
    d = fm.model_dump(mode='json')

    assert d["default_value"] is None


def test_field_model_deserialize_from_dict():
    """FieldModel.model_validate() with dict default_value creates proper proto Value."""
    data = {
        "name": "country",
        "dtype": 2,  # String type enum value
        "default_value": {"stringVal": "CA"},
    }
    fm = FieldModel.model_validate(data)

    assert fm.default_value is not None
    assert isinstance(fm.default_value, Value)
    assert fm.default_value.string_val == "CA"


def test_field_model_deserialize_from_proto():
    """FieldModel.model_validate() with proto Value passes through unchanged."""
    proto_value = Value(int64_val=100)
    data = {
        "name": "count",
        "dtype": 4,  # Int64 type enum value
        "default_value": proto_value,
    }
    fm = FieldModel.model_validate(data)

    assert fm.default_value is not None
    assert isinstance(fm.default_value, Value)
    assert fm.default_value.int64_val == 100
    # Verify it's the same object (not a copy)
    assert fm.default_value is proto_value


def test_field_model_roundtrip_json():
    """Serialize to dict, deserialize back, verify proto values match."""
    # Create original with double value
    fm1 = FieldModel(
        name="score", dtype=Float64, default_value=Value(double_val=3.14159)
    )

    # Serialize to dict
    d = fm1.model_dump(mode='json')

    # Deserialize from dict
    fm2 = FieldModel.model_validate(d)

    # Verify proto values match
    assert fm2.default_value is not None
    assert fm2.default_value.double_val == 3.14159


def test_field_model_to_field_preserves_default():
    """FieldModel.to_field() returns Field with matching default_value."""
    proto_value = Value(string_val="default")
    fm = FieldModel(
        name="status",
        dtype=String,
        description="Status field",
        default_value=proto_value,
    )

    field = fm.to_field()

    assert isinstance(field, Field)
    assert field.name == "status"
    assert field.dtype == String
    assert field.default_value is not None
    assert field.default_value.string_val == "default"
    # Verify it's the same proto object
    assert field.default_value is proto_value


def test_field_model_from_field_preserves_default():
    """FieldModel.from_field(Field(...)) captures default_value."""
    proto_value = Value(bool_val=False)
    field = Field(
        name="enabled", dtype=Bool, description="Enable flag", default_value=proto_value
    )

    fm = FieldModel.from_field(field)

    assert fm.name == "enabled"
    assert fm.dtype == Bool
    assert fm.default_value is not None
    assert fm.default_value.bool_val is False
    # Verify it's the same proto object
    assert fm.default_value is proto_value


def test_field_model_full_roundtrip():
    """Field -> FieldModel -> model_dump() -> model_validate() -> to_field() -> compare."""
    # Start with a Field
    original_field = Field(
        name="price",
        dtype=Float64,
        description="Item price",
        tags={"unit": "USD"},
        default_value=Value(double_val=9.99),
    )

    # Convert to FieldModel
    fm1 = FieldModel.from_field(original_field)

    # Serialize to JSON dict
    json_dict = fm1.model_dump(mode='json')

    # Deserialize from JSON dict
    fm2 = FieldModel.model_validate(json_dict)

    # Convert back to Field
    result_field = fm2.to_field()

    # Compare all attributes
    assert result_field.name == original_field.name
    assert result_field.dtype == original_field.dtype
    assert result_field.description == original_field.description
    assert result_field.tags == original_field.tags
    assert result_field.default_value is not None
    assert result_field.default_value.double_val == 9.99
