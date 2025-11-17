package utils

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"time"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/rs/zerolog/log"
	"github.com/spaolacci/murmur3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// BuildZsetKey Helper function to build ZSET key = <feature_view><entity_key_bytes>
func BuildZsetKey(featureView string, entityKeyBin []byte) string {
	return featureView + string(entityKeyBin)
}

// BuildHashKey Helper function to build HASH key = <entity_key_bytes><sort_key_bytes>
func BuildHashKey(entityKeyBin, sortKeyBin []byte) string {
	k := make([]byte, 0, len(entityKeyBin)+len(sortKeyBin))
	k = append(k, entityKeyBin...)
	k = append(k, sortKeyBin...)
	return string(k)
}

// Mmh3FieldHash mmh3 field hash that matches Python _mmh3
func Mmh3FieldHash(fv, fn string) string {
	h := murmur3.New32()
	_, _ = h.Write([]byte(fmt.Sprintf("%s:%s", fv, fn)))
	sum := h.Sum32()

	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, sum)
	return string(buf)
}

// DecodeTimestamp Helper function to decode timestamp protobuf from Redis HSET value
func DecodeTimestamp(raw interface{}) timestamppb.Timestamp {
	var ts timestamppb.Timestamp
	if raw == nil {
		return ts
	}

	var b []byte
	switch v := raw.(type) {
	case string:
		b = []byte(v)
	case []byte:
		b = v
	default:
		log.Warn().
			Str("type", fmt.Sprintf("%T", raw)).
			Msg("OnlineReadRange: unexpected timestamp type, using zero timestamp")
		return ts
	}

	if err := proto.Unmarshal(b, &ts); err != nil {
		log.Warn().
			Err(err).
			Msg("OnlineReadRange: failed to unmarshal event timestamp, using zero timestamp")
	}
	return ts
}

// GetScoreRange builds Redis ZRANGEBYSCORE min/max bounds.
// Redis format:
//
//	"-inf", "+inf", "123", "(123"
func GetScoreRange(filters []*model.SortKeyFilter) (string, string) {
	if len(filters) == 0 {
		return "-inf", "+inf"
	}

	var (
		minStr, maxStr string
		minSet, maxSet bool
	)

	for _, f := range filters {
		if f == nil {
			continue
		}

		if f.Equals != nil {
			s, _ := fmtInterface(f.Equals)
			return s, s
		}

		if f.RangeStart != nil {
			s, _ := fmtInterface(f.RangeStart)
			if !f.StartInclusive {
				s = "(" + s
			}
			minStr = s
			minSet = true
		}

		if f.RangeEnd != nil {
			s, _ := fmtInterface(f.RangeEnd)
			if !f.EndInclusive {
				s = "(" + s
			}
			maxStr = s
			maxSet = true
		}
	}

	if !minSet {
		minStr = "-inf"
	}
	if !maxSet {
		maxStr = "+inf"
	}

	return minStr, maxStr
}

// fmtInterface converts Feast SortKeyFilter values (which may be native Go values
// or pointer forms) into the string Redis expects for ZRANGEBYSCORE.
// Handles: int, int32, int64, float32, float64, time.Time,
// and pointer versions of each.
func fmtInterface(v interface{}) (string, error) {
	if v == nil {
		return "", fmt.Errorf("nil value")
	}

	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return "", fmt.Errorf("nil pointer")
		}
		return fmtInterface(rv.Elem().Interface()) // Recurse
	}

	switch x := v.(type) {
	case int:
		return fmt.Sprintf("%d", x), nil
	case int32:
		return fmt.Sprintf("%d", x), nil
	case int64:
		return fmt.Sprintf("%d", x), nil

	case float32:
		return fmt.Sprintf("%g", x), nil
	case float64:
		return fmt.Sprintf("%g", x), nil

	case time.Time:
		// Feast timestamps become time.Time for sort keys
		return fmt.Sprintf("%d", x.Unix()), nil

	case string:
		return x, nil
	}

	// fallback
	return fmt.Sprintf("%v", v), nil
}

// computeEffectiveReverse combines the SortedFeatureView / SortKeyFilter order
// with the user-provided reverse_sort_order flag.
func ComputeEffectiveReverse(filters []*model.SortKeyFilter, userReverse bool) bool {
	effective := userReverse

	if len(filters) == 0 {
		return effective
	}
	skf := filters[0]
	if skf == nil || skf.Order == nil {
		return effective
	}

	switch skf.Order.Order {
	case core.SortOrder_DESC:
		return !effective
	case core.SortOrder_ASC, core.SortOrder_INVALID:
		return effective
	default:
		return effective
	}
}
