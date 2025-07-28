package onlinestore

import (
	"fmt"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"google.golang.org/protobuf/proto"
)

func UnmarshalStoredProto(value interface{}) (*types.Value, serving.FieldStatus, error) {
	if value == nil {
		return &types.Value{Val: &types.Value_NullVal{NullVal: types.Null_NULL}},
			serving.FieldStatus_NULL_VALUE,
			nil
	}

	switch v := value.(type) {
	case []byte:
		if len(v) == 0 {
			return &types.Value{Val: &types.Value_NullVal{NullVal: types.Null_NULL}},
				serving.FieldStatus_NULL_VALUE,
				nil
		}
		var message types.Value
		if err := proto.Unmarshal(v, &message); err == nil {
			return &message, serving.FieldStatus_PRESENT, nil
		}
		return &types.Value{Val: &types.Value_BytesVal{BytesVal: v}},
			serving.FieldStatus_PRESENT,
			nil
	case string:
		return &types.Value{Val: &types.Value_StringVal{StringVal: v}},
			serving.FieldStatus_PRESENT,
			nil
	case int32:
		return &types.Value{Val: &types.Value_Int32Val{Int32Val: v}},
			serving.FieldStatus_PRESENT,
			nil
	case int64:
		return &types.Value{Val: &types.Value_Int64Val{Int64Val: v}},
			serving.FieldStatus_PRESENT,
			nil
	case int:
		return &types.Value{Val: &types.Value_Int64Val{Int64Val: int64(v)}},
			serving.FieldStatus_PRESENT,
			nil
	case float32:
		return &types.Value{Val: &types.Value_FloatVal{FloatVal: v}},
			serving.FieldStatus_PRESENT,
			nil
	case float64:
		return &types.Value{Val: &types.Value_DoubleVal{DoubleVal: v}},
			serving.FieldStatus_PRESENT,
			nil
	case bool:
		return &types.Value{Val: &types.Value_BoolVal{BoolVal: v}},
			serving.FieldStatus_PRESENT,
			nil
	case []string:
		return &types.Value{Val: &types.Value_StringListVal{
				StringListVal: &types.StringList{Val: v}}},
			serving.FieldStatus_PRESENT,
			nil
	case []int32:
		return &types.Value{Val: &types.Value_Int32ListVal{
				Int32ListVal: &types.Int32List{Val: v}}},
			serving.FieldStatus_PRESENT,
			nil
	case []int64:
		return &types.Value{Val: &types.Value_Int64ListVal{
				Int64ListVal: &types.Int64List{Val: v}}},
			serving.FieldStatus_PRESENT,
			nil
	case []float32:
		return &types.Value{Val: &types.Value_FloatListVal{
				FloatListVal: &types.FloatList{Val: v}}},
			serving.FieldStatus_PRESENT,
			nil
	case []float64:
		return &types.Value{Val: &types.Value_DoubleListVal{
				DoubleListVal: &types.DoubleList{Val: v}}},
			serving.FieldStatus_PRESENT,
			nil
	case []bool:
		return &types.Value{Val: &types.Value_BoolListVal{
				BoolListVal: &types.BoolList{Val: v}}},
			serving.FieldStatus_PRESENT,
			nil
	case [][]byte:
		return &types.Value{Val: &types.Value_BytesListVal{
				BytesListVal: &types.BytesList{Val: v}}},
			serving.FieldStatus_PRESENT,
			nil
	default:
		return nil, serving.FieldStatus_NOT_FOUND,
			fmt.Errorf("unexpected type for feature value: %T", value)
	}
}
