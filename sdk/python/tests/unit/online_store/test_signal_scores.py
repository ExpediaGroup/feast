import json

import pytest

from feast.infra.online_stores._signal_scores import (
    decode_signal_scores,
    encode_signal_scores,
)
from feast.protos.feast.types.Value_pb2 import Value as ValueProto


class TestEncodeSignalScores:
    def test_single_score(self):
        result = encode_signal_scores({"vec_embedding": 0.95})
        assert result.HasField("string_val")
        parsed = json.loads(result.string_val)
        assert parsed == {"vec_embedding": 0.95}

    def test_multiple_scores(self):
        scores = {"vec_title": 0.8, "vec_body": 0.6, "bm25": 12.5}
        result = encode_signal_scores(scores)
        parsed = json.loads(result.string_val)
        assert parsed == scores

    def test_empty_dict(self):
        result = encode_signal_scores({})
        assert result.string_val == "{}"

    def test_sort_keys_deterministic(self):
        result_a = encode_signal_scores({"z_field": 1.0, "a_field": 2.0})
        result_b = encode_signal_scores({"a_field": 2.0, "z_field": 1.0})
        assert result_a.string_val == result_b.string_val
        parsed = json.loads(result_a.string_val)
        keys = list(parsed.keys())
        assert keys == sorted(keys)

    def test_compact_json_no_spaces(self):
        result = encode_signal_scores({"a": 1.0, "b": 2.0})
        assert " " not in result.string_val

    def test_returns_value_proto(self):
        result = encode_signal_scores({"x": 1.0})
        assert isinstance(result, ValueProto)


class TestDecodeSignalScores:
    def test_roundtrip(self):
        original = {"vec_embedding": 0.95, "bm25": 12.5}
        encoded = encode_signal_scores(original)
        decoded = decode_signal_scores(encoded)
        assert decoded == original

    def test_roundtrip_empty(self):
        encoded = encode_signal_scores({})
        decoded = decode_signal_scores(encoded)
        assert decoded == {}

    def test_empty_string_val(self):
        val = ValueProto()
        val.string_val = ""
        assert decode_signal_scores(val) == {}

    def test_no_string_field(self):
        val = ValueProto()
        val.int64_val = 42
        assert decode_signal_scores(val) == {}

    def test_default_value_proto(self):
        val = ValueProto()
        assert decode_signal_scores(val) == {}

    def test_malformed_json_raises(self):
        val = ValueProto()
        val.string_val = "not-json"
        with pytest.raises(json.JSONDecodeError):
            decode_signal_scores(val)
