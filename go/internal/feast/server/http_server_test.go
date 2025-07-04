//go:build !integration

package server

import (
	"encoding/json"
	"github.com/feast-dev/feast/go/internal/feast/onlineserving"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	prototypes "github.com/feast-dev/feast/go/protos/feast/types"
	"google.golang.org/protobuf/types/known/timestamppb"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestUnmarshalJSON(t *testing.T) {
	u := repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[1, 2, 3]")))
	assert.Equal(t, []int64{1, 2, 3}, u.int64Val)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[1.2, 2.3, 3.4]")))
	assert.Equal(t, []float64{1.2, 2.3, 3.4}, u.doubleVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[\"foo\", \"bar\"]")))
	assert.Equal(t, []string{"foo", "bar"}, u.stringVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[true, false, true]")))
	assert.Equal(t, []bool{true, false, true}, u.boolVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[[1, 2, 3], [4, 5, 6]]")))
	assert.Equal(t, [][]int64{{1, 2, 3}, {4, 5, 6}}, u.int64ListVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[[1.2, 2.3, 3.4], [10.2, 20.3, 30.4]]")))
	assert.Equal(t, [][]float64{{1.2, 2.3, 3.4}, {10.2, 20.3, 30.4}}, u.doubleListVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[[\"foo\", \"bar\"], [\"foo2\", \"bar2\"]]")))
	assert.Equal(t, [][]string{{"foo", "bar"}, {"foo2", "bar2"}}, u.stringListVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[[true, false, true], [false, true, false]]")))
	assert.Equal(t, [][]bool{{true, false, true}, {false, true, false}}, u.boolListVal)
}
func TestMarshalInt32JSON(t *testing.T) {
	var arrowArray arrow.Array
	memoryPool := memory.NewGoAllocator()
	builder := array.NewInt32Builder(memoryPool)
	defer builder.Release()
	builder.AppendValues([]int32{1, 2, 3, 4}, nil)
	arrowArray = builder.NewArray()
	defer arrowArray.Release()
	expectedJSON := `[1,2,3,4]`

	jsonData, err := json.Marshal(arrowArray)
	assert.NoError(t, err, "Error marshaling Arrow array")

	assert.Equal(t, expectedJSON, string(jsonData), "JSON output does not match expected")
	assert.IsType(t, &array.Int32{}, arrowArray, "arrowArray is not of type *array.Int32")
}

func TestMarshalInt64JSON(t *testing.T) {
	var arrowArray arrow.Array
	memoryPool := memory.NewGoAllocator()
	builder := array.NewInt64Builder(memoryPool)
	defer builder.Release()
	builder.AppendValues([]int64{-9223372036854775808, 9223372036854775807}, nil)
	arrowArray = builder.NewArray()
	defer arrowArray.Release()
	expectedJSON := `[-9223372036854775808,9223372036854775807]`

	jsonData, err := json.Marshal(arrowArray)
	assert.NoError(t, err, "Error marshaling Arrow array")

	assert.Equal(t, expectedJSON, string(jsonData), "JSON output does not match expected")
	assert.IsType(t, &array.Int64{}, arrowArray, "arrowArray is not of type *array.Int64")
}

func TestUnmarshalRangeRequestJSON(t *testing.T) {
	jsonData := `{      
  "features": [            
    "batch_mat_sorted_fv:feature_1",
    "batch_mat_sorted_fv:feature_2",
    "batch_mat_sorted_fv:feature_3"
  ],      
  "entities": {    
    "entity_key": [  
      "entity_key_4"
    ]
  },          
  "sort_key_filters": [
    {                    
      "sort_key_name": "feature_5",
      "equals": 126.8
    },
    {                    
      "sort_key_name": "event_timestamp",
      "range": {
        "range_start": 1740000000,
      	"start_inclusive": true,
      	"end_inclusive": false
	  }
    }
  ],
  "reverse_sort_order": false,
  "limit": 10,
  "full_feature_names": true
}`
	var request getOnlineFeaturesRangeRequest
	decoder := json.NewDecoder(strings.NewReader(jsonData))
	err := decoder.Decode(&request)
	assert.NoError(t, err, "Error unmarshalling JSON")

	sortKeyFiltersProto, err := getSortKeyFiltersProto(request.SortKeyFilters)
	assert.NoError(t, err, "Error converting to proto")

	assert.Equal(t, 2, len(sortKeyFiltersProto))
	assert.Equal(t, "feature_5", sortKeyFiltersProto[0].GetSortKeyName())
	assert.Equal(t, 126.8, sortKeyFiltersProto[0].GetEquals().GetDoubleVal())
	assert.Nil(t, sortKeyFiltersProto[0].GetRange())
	assert.Equal(t, "event_timestamp", sortKeyFiltersProto[1].GetSortKeyName())
	assert.Equal(t, int64(1740000000), sortKeyFiltersProto[1].GetRange().RangeStart.GetInt64Val())
	assert.Equal(t, true, sortKeyFiltersProto[1].GetRange().StartInclusive)
	assert.Equal(t, false, sortKeyFiltersProto[1].GetRange().EndInclusive)
	assert.Nil(t, sortKeyFiltersProto[1].GetEquals())

	assert.Equal(t, int32(10), request.Limit)
}

func TestUnmarshalRangeRequestJSON_InvalidSortKeyFilter(t *testing.T) {
	jsonData := `{      
  "features": [            
	"batch_mat_sorted_fv:feature_1",
	"batch_mat_sorted_fv:feature_2",
	"batch_mat_sorted_fv:feature_3"
  ],      
  "entities": {    
	"entity_key": [  
	  "entity_key_4"
	]
  },          
  "sort_key_filters": [
	{                    
	  "sort_key_name": {}
	}
  ],
  "reverse_sort_order": false,
  "limit": 10,
  "full_feature_names": true
}`
	var request getOnlineFeaturesRangeRequest
	decoder := json.NewDecoder(strings.NewReader(jsonData))
	err := decoder.Decode(&request)
	assert.Error(t, err, "Should return an error unmarshalling JSON")
	assert.Equal(t, "json: cannot unmarshal object into Go struct field sortKeyFilter.sort_key_filters.sort_key_name of type string", err.Error())
}

func TestSortKeyFilterToProto_WithBothEqualsAndRange(t *testing.T) {
	allFilters := []sortKeyFilter{
		{
			SortKeyName: "invalid",
			Equals:      []byte("1"),
			Range: rangeQuery{
				RangeStart:     []byte("1"),
				RangeEnd:       []byte("2"),
				StartInclusive: false,
				EndInclusive:   false,
			},
		},
	}
	_, err := getSortKeyFiltersProto(allFilters)
	assert.Error(t, err, "Should return an error for invalid sort key filter")
	assert.Equal(t, "SortKeyFilter must have either equals or range, but not both", err.Error())
}

func TestSortKeyFilter_ToProto_WithNoEqualsOrRange(t *testing.T) {
	noFilters := []sortKeyFilter{{SortKeyName: "invalid"}}
	_, err := getSortKeyFiltersProto(noFilters)
	assert.Error(t, err, "Should return an error for invalid sort key filter")
	assert.Equal(t, "SortKeyFilter must have either equals or range", err.Error())
}

func TestSortKeyFilter_ToProto_WithInvalidEquals(t *testing.T) {
	invalidEquals := []sortKeyFilter{
		{
			SortKeyName: "invalid",
			Equals:      []byte("[1]"),
		},
	}
	_, err := getSortKeyFiltersProto(invalidEquals)
	assert.Error(t, err, "Should return an error for invalid sort key filter equals")
	assert.Equal(t, "error parsing equals filter: could not parse JSON value: [1]", err.Error())
}

func TestSortKeyFilter_ToProto_WithInvalidRangeStart(t *testing.T) {
	invalidRange := []sortKeyFilter{
		{
			SortKeyName: "invalid",
			Range: rangeQuery{
				RangeStart: []byte("[1]"),
			},
		},
	}
	_, err := getSortKeyFiltersProto(invalidRange)
	assert.Error(t, err, "Should return an error for invalid sort key filter range")
	assert.Equal(t, "error parsing range_start: could not parse JSON value: [1]", err.Error())
}

func TestSortKeyFilter_ToProto_WithInvalidRangeEnd(t *testing.T) {
	invalidRange := []sortKeyFilter{
		{
			SortKeyName: "invalid",
			Range: rangeQuery{
				RangeEnd: []byte("[1]"),
			},
		},
	}
	_, err := getSortKeyFiltersProto(invalidRange)
	assert.Error(t, err, "Should return an error for invalid sort key filter range")
	assert.Equal(t, "error parsing range_end: could not parse JSON value: [1]", err.Error())
}

func TestProcessFeatureVectors_NotFoundReturnsNull(t *testing.T) {
	memoryPool := memory.NewGoAllocator()

	entitiesProto := map[string]*prototypes.RepeatedValue{
		"entity_key": {
			Val: []*prototypes.Value{
				{Val: &prototypes.Value_StringVal{StringVal: "entity_1"}},
				{Val: &prototypes.Value_StringVal{StringVal: "entity_2"}},
			},
		},
	}

	featureBuilder := array.NewListBuilder(memoryPool, arrow.PrimitiveTypes.Int32)
	defer featureBuilder.Release()

	valueBuilder := featureBuilder.ValueBuilder().(*array.Int32Builder)

	// Entity 1: NOT_FOUND
	featureBuilder.Append(true)
	valueBuilder.Append(0)
	featureBuilder.Append(true)
	valueBuilder.Append(42)

	featureVector := &onlineserving.RangeFeatureVector{
		Name:        "feature_2",
		RangeValues: featureBuilder.NewArray(),
		RangeStatuses: [][]serving.FieldStatus{
			{serving.FieldStatus_NOT_FOUND},
			{serving.FieldStatus_PRESENT},
		},
		RangeTimestamps: [][]*timestamppb.Timestamp{
			{{Seconds: 0}},
			{{Seconds: 1234567890}},
		},
	}
	defer featureVector.RangeValues.Release()

	featureNames, results := processFeatureVectors(
		[]*onlineserving.RangeFeatureVector{featureVector},
		false,
		entitiesProto,
	)

	assert.Equal(t, []string{"feature_2"}, featureNames)
	values := results[0]["values"].([]interface{})
	entity1Values := values[0].([]interface{})
	assert.Equal(t, 1, len(entity1Values))
	assert.Nil(t, entity1Values[0])
	entity2Values := values[1].([]interface{})
	assert.Equal(t, int32(42), entity2Values[0])
}

func TestProcessFeatureVectors_TimestampHandling(t *testing.T) {
	memoryPool := memory.NewGoAllocator()

	entitiesProto := map[string]*prototypes.RepeatedValue{
		"entity_key": {
			Val: []*prototypes.Value{
				{Val: &prototypes.Value_StringVal{StringVal: "entity_1"}},
				{Val: &prototypes.Value_StringVal{StringVal: "entity_2"}},
			},
		},
	}

	featureBuilder := array.NewListBuilder(memoryPool, arrow.BinaryTypes.String)
	defer featureBuilder.Release()

	valueBuilder := featureBuilder.ValueBuilder().(*array.StringBuilder)

	// Entity 1: NOT_FOUND
	featureBuilder.Append(true)
	valueBuilder.Append("")
	featureBuilder.Append(true)
	valueBuilder.Append("value")

	featureVector := &onlineserving.RangeFeatureVector{
		Name:        "feature_3",
		RangeValues: featureBuilder.NewArray(),
		RangeStatuses: [][]serving.FieldStatus{
			{serving.FieldStatus_NOT_FOUND},
			{serving.FieldStatus_PRESENT},
		},
		RangeTimestamps: [][]*timestamppb.Timestamp{
			{{Seconds: 0, Nanos: 0}},
			{{Seconds: 0, Nanos: 0}},
		},
	}
	defer featureVector.RangeValues.Release()

	featureNames, results := processFeatureVectors(
		[]*onlineserving.RangeFeatureVector{featureVector},
		true,
		entitiesProto,
	)

	assert.Equal(t, []string{"feature_3"}, featureNames)
	timestamps := results[0]["event_timestamps"].([][]interface{})
	assert.Nil(t, timestamps[0][0])
	assert.Equal(t, "1970-01-01T00:00:00Z", timestamps[1][0])
}

func TestProcessFeatureVectors_NullValueReturnsNull(t *testing.T) {
	memoryPool := memory.NewGoAllocator()

	entitiesProto := map[string]*prototypes.RepeatedValue{
		"entity_key": {
			Val: []*prototypes.Value{
				{Val: &prototypes.Value_StringVal{StringVal: "entity_1"}},
			},
		},
	}

	featureBuilder := array.NewListBuilder(memoryPool, arrow.PrimitiveTypes.Float32)
	defer featureBuilder.Release()

	valueBuilder := featureBuilder.ValueBuilder().(*array.Float32Builder)

	featureBuilder.Append(true)
	valueBuilder.Append(0.0)

	featureVector := &onlineserving.RangeFeatureVector{
		Name:        "feature_4",
		RangeValues: featureBuilder.NewArray(),
		RangeStatuses: [][]serving.FieldStatus{
			{serving.FieldStatus_NULL_VALUE},
		},
		RangeTimestamps: [][]*timestamppb.Timestamp{
			{{Seconds: 1234567890}},
		},
	}
	defer featureVector.RangeValues.Release()

	featureNames, results := processFeatureVectors(
		[]*onlineserving.RangeFeatureVector{featureVector},
		true,
		entitiesProto,
	)

	assert.Equal(t, []string{"feature_4"}, featureNames)
	values := results[0]["values"].([]interface{})
	entity1Values := values[0].([]interface{})
	assert.Equal(t, 1, len(entity1Values))
	assert.Nil(t, entity1Values[0])
	timestamps := results[0]["event_timestamps"].([][]interface{})
	assert.Nil(t, timestamps[0][0])
}
