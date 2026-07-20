import numpy as np
import pandas as pd
import pytest

from feast.type_map import (
    MS_TIMESTAMP_THRESHOLD,
    _python_datetime_to_int_ms_timestamp,
    _python_datetime_to_int_timestamp,
    feast_value_type_to_python_type,
    python_values_to_proto_values,
)
from feast.value_type import ValueType


def test_null_unix_timestamp():
    """Test that null UnixTimestamps get converted from proto correctly."""

    data = np.array(["NaT"], dtype="datetime64")
    protos = python_values_to_proto_values(data, ValueType.UNIX_TIMESTAMP)
    converted = feast_value_type_to_python_type(protos[0])

    assert converted is None


def test_null_unix_timestamp_list():
    """Test that UnixTimestamp lists with a null get converted from proto
    correctly."""

    data = np.array([["NaT"]], dtype="datetime64")
    protos = python_values_to_proto_values(data, ValueType.UNIX_TIMESTAMP_LIST)
    converted = feast_value_type_to_python_type(protos[0])

    assert converted[0] is None


def test_python_datetime_to_int_ms_timestamp_raw_int_below_threshold_is_treated_as_seconds():
    """A raw (non-datetime) integer below MS_TIMESTAMP_THRESHOLD is assumed to be
    seconds and converted to milliseconds - e.g. a sort key column that arrived
    as a Spark LongType rather than TimestampType (source Avro schema missing a
    timestamp-millis/timestamp-micros logicalType)."""

    seconds_value = 1717244257  # 2024-06-01 12:17:37 UTC, well below the threshold
    assert seconds_value < MS_TIMESTAMP_THRESHOLD

    result = _python_datetime_to_int_ms_timestamp([seconds_value])

    assert result == [seconds_value * 1000]


def test_python_datetime_to_int_ms_timestamp_raw_int_above_threshold_passes_through():
    """A raw integer already above MS_TIMESTAMP_THRESHOLD is assumed to already be
    milliseconds and is passed through unchanged."""

    ms_value = 1717244257886  # 2024-06-01 12:17:37.886 UTC
    assert ms_value > MS_TIMESTAMP_THRESHOLD

    result = _python_datetime_to_int_ms_timestamp([ms_value])

    assert result == [ms_value]


def test_ms_timestamp_ndarray_fast_path_preserves_millis():
    """A 1-D datetime64 ndarray should take the vectorized fast path (guarded by
    np.issubdtype, not isinstance - values.dtype is a numpy.dtype instance and
    is never an instance of the np.datetime64 scalar type) and retain
    millisecond precision."""

    arr = np.array(["2024-06-01T12:17:37.886"], dtype="datetime64[ns]")

    result = _python_datetime_to_int_ms_timestamp(arr)

    # The fast path returns an ndarray; the scalar fallback loop returns a
    # plain list. Asserting the type pins down that the fast path actually ran.
    assert isinstance(result, np.ndarray)
    assert result.tolist() == [1717244257886]


def test_ms_timestamp_ndarray_matches_scalar_loop():
    """The ndarray fast path and the scalar fallback loop must agree."""

    arr = np.array(["2024-06-01T12:17:37.886"], dtype="datetime64[ns]")

    fast_path_result = _python_datetime_to_int_ms_timestamp(arr)
    scalar_loop_result = _python_datetime_to_int_ms_timestamp(list(arr))

    assert list(fast_path_result) == list(scalar_loop_result)


def test_seconds_timestamp_ndarray_fast_path():
    """Same fast-path bug (isinstance vs np.issubdtype) also existed in the
    pre-existing seconds-precision function; verify it takes the vectorized
    path and truncates to whole seconds."""

    arr = np.array(["2024-06-01T12:17:37.886"], dtype="datetime64[ns]")

    result = _python_datetime_to_int_timestamp(arr)

    assert isinstance(result, np.ndarray)
    assert result.tolist() == [1717244257]


def test_ndarray_non_1d_raises():
    """The ndim check is only reachable once the dtype fast-path guard
    actually matches datetime64 ndarrays."""

    arr = np.array([["2024-06-01"]], dtype="datetime64[ns]")

    with pytest.raises(ValueError, match="Only 1 dimensional arrays are supported."):
        _python_datetime_to_int_ms_timestamp(arr)

    with pytest.raises(ValueError, match="Only 1 dimensional arrays are supported."):
        _python_datetime_to_int_timestamp(arr)


@pytest.mark.parametrize(
    "values",
    (
        np.array([True]),
        np.array([False]),
        np.array([0]),
        np.array([1]),
        [True],
        [False],
        [0],
        [1],
    ),
)
def test_python_values_to_proto_values_bool(values):
    protos = python_values_to_proto_values(values, ValueType.BOOL)
    converted = feast_value_type_to_python_type(protos[0])

    assert converted is bool(values[0])


@pytest.mark.parametrize(
    "values, value_type, expected",
    (
        (np.array([b"[1,2,3]"]), ValueType.INT64_LIST, [1, 2, 3]),
        (np.array([b"[1,2,3]"]), ValueType.INT32_LIST, [1, 2, 3]),
        (np.array([b"[1.5,2.5,3.5]"]), ValueType.FLOAT_LIST, [1.5, 2.5, 3.5]),
        (np.array([b"[1.5,2.5,3.5]"]), ValueType.DOUBLE_LIST, [1.5, 2.5, 3.5]),
        (np.array([b'["a","b","c"]']), ValueType.STRING_LIST, ["a", "b", "c"]),
        (np.array([b"[true,false]"]), ValueType.BOOL_LIST, [True, False]),
        (np.array([b"[1,0]"]), ValueType.BOOL_LIST, [True, False]),
        (np.array([None]), ValueType.INT32_LIST, None),
        (np.array([None]), ValueType.INT64_LIST, None),
        (np.array([None]), ValueType.FLOAT_LIST, None),
        (np.array([None]), ValueType.DOUBLE_LIST, None),
        (np.array([None]), ValueType.BOOL_LIST, None),
        (np.array([None]), ValueType.BYTES_LIST, None),
        (np.array([None]), ValueType.STRING_LIST, None),
        (np.array([None]), ValueType.UNIX_TIMESTAMP_LIST, None),
        ([b"[1,2,3]"], ValueType.INT64_LIST, [1, 2, 3]),
        ([b"[1,2,3]"], ValueType.INT32_LIST, [1, 2, 3]),
        ([b"[1.5,2.5,3.5]"], ValueType.FLOAT_LIST, [1.5, 2.5, 3.5]),
        ([b"[1.5,2.5,3.5]"], ValueType.DOUBLE_LIST, [1.5, 2.5, 3.5]),
        ([b'["a","b","c"]'], ValueType.STRING_LIST, ["a", "b", "c"]),
        ([b"[true,false]"], ValueType.BOOL_LIST, [True, False]),
        ([b"[1,0]"], ValueType.BOOL_LIST, [True, False]),
        ([None], ValueType.STRING_LIST, None),
    ),
)
def test_python_values_to_proto_values_bytes_to_list(values, value_type, expected):
    protos = python_values_to_proto_values(values, value_type)
    converted = feast_value_type_to_python_type(protos[0])
    assert converted == expected


def test_python_values_to_proto_values_bytes_to_list_not_supported():
    with pytest.raises(TypeError):
        _ = python_values_to_proto_values([b"[]"], ValueType.BYTES_LIST)


def test_python_values_to_proto_values_int_list_with_null_not_supported():
    df = pd.DataFrame({"column": [1, 2, None]})
    arr = df["column"].to_numpy()
    with pytest.raises(TypeError):
        _ = python_values_to_proto_values(arr, ValueType.INT32_LIST)
