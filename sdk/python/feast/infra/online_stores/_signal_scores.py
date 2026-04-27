import json
from typing import Dict

from feast.protos.feast.types.Value_pb2 import Value as ValueProto


def encode_signal_scores(scores: Dict[str, float]) -> ValueProto:
    """Encode a signal_scores dict as a JSON string in ValueProto."""
    val = ValueProto()
    val.string_val = json.dumps(scores, separators=(",", ":"), sort_keys=True)
    return val


def decode_signal_scores(value: ValueProto) -> Dict[str, float]:
    """Decode a signal_scores ValueProto back to a dict."""
    if not value.HasField("string_val") or not value.string_val:
        return {}
    return json.loads(value.string_val)
