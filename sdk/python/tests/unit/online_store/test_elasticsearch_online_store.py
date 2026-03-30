import base64

import pytest

from feast.protos.feast.types.Value_pb2 import (
    FloatList,
    Int64List,
    Value as ValueProto,
)

from feast.infra.online_stores.elasticsearch_online_store.elasticsearch import (
    _encode_feature_value,
)


class TestEncodeFeatureValue:
    def test_vector_field_includes_vector_value(self):
        """When is_vector=True and value is a float list, vector_value should be present."""
        value = ValueProto(float_list_val=FloatList(val=[0.1, 0.2, 0.3]))
        result = _encode_feature_value(value, is_vector=True)

        assert "vector_value" in result
        assert result["vector_value"] == pytest.approx([0.1, 0.2, 0.3])

    def test_non_vector_list_excludes_vector_value(self):
        """When is_vector=False and value is a float list, vector_value should NOT be present."""
        value = ValueProto(float_list_val=FloatList(val=[0.1, 0.2, 0.3]))
        result = _encode_feature_value(value, is_vector=False)

        assert "vector_value" not in result

    def test_non_vector_int_list_excludes_vector_value(self):
        """An int64 list with is_vector=False should not produce vector_value."""
        value = ValueProto(int64_list_val=Int64List(val=[1, 2, 3]))
        result = _encode_feature_value(value, is_vector=False)

        assert "vector_value" not in result

    def test_string_value_has_value_text(self):
        """A string ValueProto should produce value_text, not vector_value."""
        value = ValueProto(string_val="hello")
        result = _encode_feature_value(value, is_vector=False)

        assert result["value_text"] == "hello"
        assert "vector_value" not in result

    def test_feature_value_always_present(self):
        """feature_value (base64 binary) should always be present regardless of is_vector."""
        vector_value = ValueProto(float_list_val=FloatList(val=[1.0, 2.0]))
        string_value = ValueProto(string_val="test")
        int_value = ValueProto(int64_val=42)

        for val in [vector_value, string_value, int_value]:
            for is_vector in [True, False]:
                result = _encode_feature_value(val, is_vector=is_vector)
                assert "feature_value" in result
                # Verify it's valid base64 that deserializes back
                decoded = base64.b64decode(result["feature_value"])
                roundtrip = ValueProto()
                roundtrip.ParseFromString(decoded)

    def test_default_is_vector_false(self):
        """Calling without is_vector should default to False (no vector_value)."""
        value = ValueProto(float_list_val=FloatList(val=[0.1, 0.2]))
        result = _encode_feature_value(value)

        assert "vector_value" not in result
