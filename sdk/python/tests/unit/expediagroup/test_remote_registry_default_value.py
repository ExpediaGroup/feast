"""
Tests for Remote Registry default_value proto roundtrip.

Verifies that FeatureView.to_proto() / from_proto() preserves Field.default_value,
which is the core serialization path used by Remote Registry (feast serve_registry).

Copyright 2026 Expedia Group
"""

import pytest

from feast.field import Field
from feast.feature_view import FeatureView
from feast.sorted_feature_view import SortedFeatureView
from feast.data_source import RequestSource
from feast.protos.feast.types.Value_pb2 import Value
from feast.types import Int64, String, Float64, Bool


def test_feature_view_proto_roundtrip_with_defaults():
    """
    Verify FeatureView with Field default_value survives proto serialization/deserialization.

    This simulates the Remote Registry gRPC path:
    Server: FeatureView -> to_proto() -> bytes over wire
    Client: bytes -> from_proto() -> FeatureView
    """
    # Create FeatureView with fields that have default values
    fields = [
        Field(name="country_code", dtype=String, default_value=Value(string_val="US")),
        Field(name="latitude", dtype=Float64, default_value=Value(double_val=0.0)),
        Field(name="is_active", dtype=Bool, default_value=Value(bool_val=True)),
    ]

    # Use RequestSource as a minimal source for testing
    source = RequestSource(
        name="test_source",
        schema=fields,
    )

    fv = FeatureView(
        name="test_fv_with_defaults",
        source=source,
        schema=fields,
    )

    # Serialize to proto
    fv_proto = fv.to_proto()

    # Verify proto has default_value set (Phase 1 implementation)
    assert len(fv_proto.spec.features) == 3
    assert fv_proto.spec.features[0].default_value.string_val == "US"
    assert fv_proto.spec.features[1].default_value.double_val == 0.0
    assert fv_proto.spec.features[2].default_value.bool_val is True

    # Simulate wire transmission by serializing to bytes
    proto_bytes = fv_proto.SerializeToString()

    # Deserialize from proto (simulates client receiving from Remote Registry)
    from feast.protos.feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto
    fv_proto_received = FeatureViewProto()
    fv_proto_received.ParseFromString(proto_bytes)

    # Convert back to FeatureView object
    fv_reconstructed = FeatureView.from_proto(fv_proto_received)

    # Verify default_value preserved (order may change)
    assert len(fv_reconstructed.schema) == 3
    fields_by_name = {f.name: f for f in fv_reconstructed.schema}
    assert fields_by_name["country_code"].default_value.string_val == "US"
    assert fields_by_name["latitude"].default_value.double_val == 0.0
    assert fields_by_name["is_active"].default_value.bool_val is True


def test_feature_view_proto_roundtrip_without_defaults():
    """
    Verify FeatureView without default_value works correctly (backwards compatibility).
    """
    fields = [
        Field(name="country_code", dtype=String),
        Field(name="latitude", dtype=Float64),
    ]

    source = RequestSource(
        name="test_source",
        schema=fields,
    )

    fv = FeatureView(
        name="test_fv_without_defaults",
        source=source,
        schema=fields,
    )

    # Round-trip through proto
    fv_proto = fv.to_proto()
    proto_bytes = fv_proto.SerializeToString()

    from feast.protos.feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto
    fv_proto_received = FeatureViewProto()
    fv_proto_received.ParseFromString(proto_bytes)

    fv_reconstructed = FeatureView.from_proto(fv_proto_received)

    # Verify default_value is None (not set)
    assert fv_reconstructed.schema[0].default_value is None
    assert fv_reconstructed.schema[1].default_value is None


def test_feature_view_proto_roundtrip_mixed_defaults():
    """
    Verify FeatureView with some fields having defaults and some not.
    """
    fields = [
        Field(name="country_code", dtype=String, default_value=Value(string_val="US")),
        Field(name="latitude", dtype=Float64),  # No default
        Field(name="property_id", dtype=Int64),  # No default
        Field(name="is_active", dtype=Bool, default_value=Value(bool_val=False)),
    ]

    source = RequestSource(
        name="test_source",
        schema=fields,
    )

    fv = FeatureView(
        name="test_fv_mixed",
        source=source,
        schema=fields,
    )

    # Round-trip through proto
    fv_proto = fv.to_proto()
    proto_bytes = fv_proto.SerializeToString()

    from feast.protos.feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto
    fv_proto_received = FeatureViewProto()
    fv_proto_received.ParseFromString(proto_bytes)

    fv_reconstructed = FeatureView.from_proto(fv_proto_received)

    # Verify only fields with defaults have default_value set (order may change)
    fields_by_name = {f.name: f for f in fv_reconstructed.schema}
    assert fields_by_name["country_code"].default_value.string_val == "US"
    assert fields_by_name["latitude"].default_value is None
    assert fields_by_name["property_id"].default_value is None
    assert fields_by_name["is_active"].default_value.bool_val is False


def test_sorted_feature_view_proto_roundtrip_with_defaults():
    """
    Verify SortedFeatureView with default_value survives proto roundtrip.
    Confirms that both sort_keys AND default_value are preserved.
    """
    from feast.data_source import RequestSource
    from feast.sort_key import SortKey
    from feast.value_type import ValueType

    fields = [
        Field(name="property_id", dtype=Int64),
        Field(name="score", dtype=Float64, default_value=Value(double_val=0.0)),
        Field(name="country_code", dtype=String, default_value=Value(string_val="US")),
    ]

    source = RequestSource(
        name="test_source",
        schema=fields,
    )

    sfv = SortedFeatureView(
        name="test_sorted_fv",
        source=source,
        schema=fields,
        sort_keys=[SortKey(name="score", value_type=ValueType.DOUBLE)],  # Sort by score field
    )

    # Round-trip through proto
    sfv_proto = sfv.to_proto()
    proto_bytes = sfv_proto.SerializeToString()

    from feast.protos.feast.core.SortedFeatureView_pb2 import SortedFeatureView as SortedFeatureViewProto
    sfv_proto_received = SortedFeatureViewProto()
    sfv_proto_received.ParseFromString(proto_bytes)

    sfv_reconstructed = SortedFeatureView.from_proto(sfv_proto_received)

    # Verify sort_keys preserved
    assert len(sfv_reconstructed.sort_keys) == 1
    assert sfv_reconstructed.sort_keys[0].name == "score"

    # Verify default_value preserved (order may change)
    fields_by_name = {f.name: f for f in sfv_reconstructed.schema}
    assert fields_by_name["property_id"].default_value is None
    assert fields_by_name["score"].default_value.double_val == 0.0
    assert fields_by_name["country_code"].default_value.string_val == "US"


def test_feature_view_proto_bytes_identity():
    """
    Verify proto wire format contains default_value.

    This tests the actual gRPC wire format by inspecting the proto
    before converting back to Python objects.
    """
    fields = [
        Field(name="age", dtype=Int64, default_value=Value(int64_val=18)),
    ]

    source = RequestSource(
        name="test_source",
        schema=fields,
    )

    fv = FeatureView(
        name="test_fv",
        source=source,
        schema=fields,
    )

    # Serialize to proto
    fv_proto = fv.to_proto()

    # Serialize to bytes (what goes over gRPC wire)
    proto_bytes = fv_proto.SerializeToString()

    # Deserialize back to proto (NOT to FeatureView yet)
    from feast.protos.feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto
    fv_proto_from_wire = FeatureViewProto()
    fv_proto_from_wire.ParseFromString(proto_bytes)

    # Inspect proto directly - verify default_value is in the wire format
    assert fv_proto_from_wire.spec.features[0].HasField("default_value")
    assert fv_proto_from_wire.spec.features[0].default_value.int64_val == 18
