//go:build integration

package scylladb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/feast-dev/feast/go/internal/feast/server"
	"github.com/feast-dev/feast/go/internal/test"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ALL_SORTED_FEATURE_NAMES  = "int_val,long_val,float_val,double_val,byte_val,string_val,timestamp_val,boolean_val,array_int_val,array_long_val,array_float_val,array_double_val,array_byte_val,array_string_val,array_timestamp_val,array_boolean_val,null_int_val,null_long_val,null_float_val,null_double_val,null_byte_val,null_string_val,null_timestamp_val,null_boolean_val,null_array_int_val,null_array_long_val,null_array_float_val,null_array_double_val,null_array_byte_val,null_array_string_val,null_array_timestamp_val,null_array_boolean_val,event_timestamp"
	ALL_REGULAR_FEATURE_NAMES = "int_val,long_val,float_val,double_val,byte_val,string_val,timestamp_val,boolean_val,array_int_val,array_long_val,array_float_val,array_double_val,array_byte_val,array_string_val,array_timestamp_val,array_boolean_val,null_int_val,null_long_val,null_float_val,null_double_val,null_byte_val,null_string_val,null_timestamp_val,null_boolean_val,null_array_int_val,null_array_long_val,null_array_float_val,null_array_double_val,null_array_byte_val,null_array_string_val,null_array_timestamp_val,null_array_boolean_val"
)

var client serving.ServingServiceClient
var ctx context.Context

func TestMain(m *testing.M) {
	dir, err := filepath.Abs("./")
	if err != nil {
		fmt.Printf("Failed to get absolute path: %v\n", err)
		os.Exit(1)
	}
	err = test.SetupInitializedRepo(dir)
	if err != nil {
		fmt.Printf("Failed to set up test environment: %v\n", err)
		os.Exit(1)
	}

	ctx = context.Background()
	var closer func()

	client, closer = server.GetClient(ctx, dir, "")

	// Run the tests
	exitCode := m.Run()

	// Clean up the test environment
	test.CleanUpInitializedRepo(dir)
	closer()

	// Exit with the appropriate code
	if exitCode != 0 {
		fmt.Printf("CassandraOnlineStore Int Tests failed with exit code %d\n", exitCode)
	}
	os.Exit(exitCode)
}

func TestGetOnlineFeaturesRange(t *testing.T) {
	entities := make(map[string]*types.RepeatedValue)

	entities["index_id"] = &types.RepeatedValue{
		Val: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 1}},
			{Val: &types.Value_Int64Val{Int64Val: 2}},
			{Val: &types.Value_Int64Val{Int64Val: 3}},
		},
	}

	featureNames := getAllSortedFeatureNames()

	var featureNamesWithFeatureView []string

	for _, featureName := range featureNames {
		featureNamesWithFeatureView = append(featureNamesWithFeatureView, "all_dtypes_sorted:"+featureName)
	}

	request := &serving.GetOnlineFeaturesRangeRequest{
		Kind: &serving.GetOnlineFeaturesRangeRequest_Features{
			Features: &serving.FeatureList{
				Val: featureNamesWithFeatureView,
			},
		},
		Entities: entities,
		SortKeyFilters: []*serving.SortKeyFilter{
			{
				SortKeyName: "event_timestamp",
				Query: &serving.SortKeyFilter_Range{
					Range: &serving.SortKeyFilter_RangeQuery{
						RangeStart: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 0}},
					},
				},
			},
		},
		Limit:           10,
		IncludeMetadata: true,
	}
	response, err := client.GetOnlineFeaturesRange(ctx, request)
	assert.NoError(t, err)
	assertResponseData(t, response, featureNames, 3, true)
}

func TestGetOnlineFeaturesRange_withOnlyEqualsFilter(t *testing.T) {
	entities := make(map[string]*types.RepeatedValue)

	entities["index_id"] = &types.RepeatedValue{
		Val: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 2}},
		},
	}

	featureNames := getAllSortedFeatureNames()

	var featureNamesWithFeatureView []string

	for _, featureName := range featureNames {
		featureNamesWithFeatureView = append(featureNamesWithFeatureView, "all_dtypes_sorted:"+featureName)
	}

	request := &serving.GetOnlineFeaturesRangeRequest{
		Kind: &serving.GetOnlineFeaturesRangeRequest_Features{
			Features: &serving.FeatureList{
				Val: featureNamesWithFeatureView,
			},
		},
		Entities: entities,
		SortKeyFilters: []*serving.SortKeyFilter{
			{
				SortKeyName: "event_timestamp",
				Query: &serving.SortKeyFilter_Equals{
					Equals: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 1744769171919}},
				},
			},
		},
		Limit:           10,
		IncludeMetadata: true,
	}
	response, err := client.GetOnlineFeaturesRange(ctx, request)
	assert.NoError(t, err)
	assert.NotNil(t, response)
	assert.Equal(t, 1, len(response.Entities))
	for i, featureResult := range response.Results {
		assert.Equal(t, 1, len(featureResult.Values))
		assert.Equal(t, 1, len(featureResult.Statuses))
		assert.Equal(t, 1, len(featureResult.EventTimestamps))
		for j, value := range featureResult.Values {
			assert.NotNil(t, value)
			assert.Equal(t, 1, len(value.Val))
			featureName := featureNames[i]
			if strings.Contains(featureName, "null") {
				// For null features, we expect the value to contain 1 entry with a nil value
				assert.Nil(t, value.Val[0].Val, "Feature %s should have a nil value", featureName)
				assert.Equal(t, serving.FieldStatus_NULL_VALUE, featureResult.Statuses[j].Status[0], "Feature %s should have a NULL_VALUE status but was %s", featureName, featureResult.Statuses[j].Status[0])
			} else {
				assert.NotNil(t, value.Val[0].Val, "Feature %s should have a non-nil value", featureName)
				assert.Equal(t, serving.FieldStatus_PRESENT, featureResult.Statuses[j].Status[0], "Feature %s should have a PRESENT status but was %s", featureName, featureResult.Statuses[j].Status[0])
			}
		}
	}
}

func TestGetOnlineFeaturesRange_forNonExistentEntityKey(t *testing.T) {
	entities := make(map[string]*types.RepeatedValue)

	entities["index_id"] = &types.RepeatedValue{
		Val: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: -1}},
		},
	}

	featureNames := getAllRegularFeatureNames()

	var featureNamesWithFeatureView []string

	for _, featureName := range featureNames {
		featureNamesWithFeatureView = append(featureNamesWithFeatureView, "all_dtypes_sorted:"+featureName)
	}

	request := &serving.GetOnlineFeaturesRangeRequest{
		Kind: &serving.GetOnlineFeaturesRangeRequest_Features{
			Features: &serving.FeatureList{
				Val: featureNamesWithFeatureView,
			},
		},
		Entities: entities,
		SortKeyFilters: []*serving.SortKeyFilter{
			{
				SortKeyName: "event_timestamp",
				Query: &serving.SortKeyFilter_Range{
					Range: &serving.SortKeyFilter_RangeQuery{
						RangeStart: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 0}},
					},
				},
			},
		},
		Limit:           10,
		IncludeMetadata: true,
	}
	response, err := client.GetOnlineFeaturesRange(ctx, request)
	assert.NoError(t, err)
	assert.NotNil(t, response)
	assert.Equal(t, 1, len(response.Entities))
	for _, featureResult := range response.Results {
		assert.Equal(t, 1, len(featureResult.Values))
		assert.Equal(t, 1, len(featureResult.Statuses))
		assert.Equal(t, 1, len(featureResult.EventTimestamps))
		for j, value := range featureResult.Values {
			assert.NotNil(t, value)
			assert.Equal(t, 1, len(value.Val))
			assert.Nil(t, value.Val[0].Val)
			assert.Equal(t, serving.FieldStatus_NOT_FOUND, featureResult.Statuses[j].Status[0])
		}
	}
}

func TestGetOnlineFeaturesRange_includesDuplicatedRequestedFeatures(t *testing.T) {
	entities := make(map[string]*types.RepeatedValue)

	entities["index_id"] = &types.RepeatedValue{
		Val: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 1}},
			{Val: &types.Value_Int64Val{Int64Val: 2}},
			{Val: &types.Value_Int64Val{Int64Val: 3}},
		},
	}

	featureNames := []string{"int_val", "int_val"}

	var featureNamesWithFeatureView []string

	for _, featureName := range featureNames {
		featureNamesWithFeatureView = append(featureNamesWithFeatureView, "all_dtypes_sorted:"+featureName)
	}

	request := &serving.GetOnlineFeaturesRangeRequest{
		Kind: &serving.GetOnlineFeaturesRangeRequest_Features{
			Features: &serving.FeatureList{
				Val: featureNamesWithFeatureView,
			},
		},
		Entities: entities,
		SortKeyFilters: []*serving.SortKeyFilter{
			{
				SortKeyName: "event_timestamp",
				Query: &serving.SortKeyFilter_Range{
					Range: &serving.SortKeyFilter_RangeQuery{
						RangeStart: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 0}},
					},
				},
			},
		},
		Limit: 10,
	}
	response, err := client.GetOnlineFeaturesRange(ctx, request)
	assert.NoError(t, err)
	assertResponseData(t, response, featureNames, 3, false)
}

func TestGetOnlineFeaturesRange_withEmptySortKeyFilter(t *testing.T) {
	entities := make(map[string]*types.RepeatedValue)

	entities["index_id"] = &types.RepeatedValue{
		Val: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 1}},
			{Val: &types.Value_Int64Val{Int64Val: 2}},
			{Val: &types.Value_Int64Val{Int64Val: 3}},
		},
	}

	featureNames := getAllRegularFeatureNames()

	var featureNamesWithFeatureView []string

	for _, featureName := range featureNames {
		featureNamesWithFeatureView = append(featureNamesWithFeatureView, "all_dtypes_sorted:"+featureName)
	}

	request := &serving.GetOnlineFeaturesRangeRequest{
		Kind: &serving.GetOnlineFeaturesRangeRequest_Features{
			Features: &serving.FeatureList{
				Val: featureNamesWithFeatureView,
			},
		},
		Entities:       entities,
		SortKeyFilters: []*serving.SortKeyFilter{},
		Limit:          10,
	}
	response, err := client.GetOnlineFeaturesRange(ctx, request)
	assert.NoError(t, err)
	assertResponseData(t, response, featureNames, 3, false)
}

func TestGetOnlineFeaturesRange_withFeatureService(t *testing.T) {
	entities := make(map[string]*types.RepeatedValue)

	entities["index_id"] = &types.RepeatedValue{
		Val: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 1}},
			{Val: &types.Value_Int64Val{Int64Val: 2}},
			{Val: &types.Value_Int64Val{Int64Val: 3}},
		},
	}

	request := &serving.GetOnlineFeaturesRangeRequest{
		Kind: &serving.GetOnlineFeaturesRangeRequest_FeatureService{
			FeatureService: "test_sorted_service",
		},
		Entities: entities,
		SortKeyFilters: []*serving.SortKeyFilter{
			{
				SortKeyName: "event_timestamp",
				Query: &serving.SortKeyFilter_Range{
					Range: &serving.SortKeyFilter_RangeQuery{
						RangeStart: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 0}},
					},
				},
			},
		},
		Limit: 10,
	}
	response, err := client.GetOnlineFeaturesRange(ctx, request)
	assert.NoError(t, err)

	featureNames := getAllSortedFeatureNames()
	assertResponseData(t, response, featureNames, 3, false)
}

func TestGetOnlineFeaturesRange_withFeatureViewThrowsError(t *testing.T) {
	entities := make(map[string]*types.RepeatedValue)

	entities["index_id"] = &types.RepeatedValue{
		Val: []*types.Value{
			{Val: &types.Value_Int64Val{Int64Val: 1}},
			{Val: &types.Value_Int64Val{Int64Val: 2}},
			{Val: &types.Value_Int64Val{Int64Val: 3}},
		},
	}

	featureNames := getAllRegularFeatureNames()

	var featureNamesWithFeatureView []string

	for _, featureName := range featureNames {
		featureNamesWithFeatureView = append(featureNamesWithFeatureView, "all_dtypes:"+featureName)
	}

	request := &serving.GetOnlineFeaturesRangeRequest{
		Kind: &serving.GetOnlineFeaturesRangeRequest_Features{
			Features: &serving.FeatureList{
				Val: featureNamesWithFeatureView,
			},
		},
		Entities: entities,
		SortKeyFilters: []*serving.SortKeyFilter{
			{
				SortKeyName: "event_timestamp",
				Query: &serving.SortKeyFilter_Range{
					Range: &serving.SortKeyFilter_RangeQuery{
						RangeStart: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 0}},
					},
				},
			},
		},
		Limit: 10,
	}
	_, err := client.GetOnlineFeaturesRange(ctx, request)
	require.Error(t, err, "Expected an error due to regular feature view requested for range query")
	assert.Contains(t, err.Error(), "sorted feature view all_dtypes doesn't exist",
		"Expected error message for non-existent sorted feature view")
}

// TestGetOnlineFeaturesRange_SubSecondSortKeyValuesRemainDistinct is a
// regression test for the EAPC-22316 follow-up: InterfaceToProtoValue in
// go/types/typeconversion.go was truncating UNIX_TIMESTAMP sort key values
// read back from Cassandra to whole seconds via time.Time.Unix(), causing
// rows for the same entity that differ by less than a second to appear
// identical when the sort key is requested as a feature value. The fixture
// (sub_second_sort_key_view, seeded from sub_second_data.parquet) has three
// rows for one entity: two only 18ms apart within the same second, and one
// two seconds later - mirroring the real-world repro that caught this bug.
func TestGetOnlineFeaturesRange_SubSecondSortKeyValuesRemainDistinct(t *testing.T) {
	entities := make(map[string]*types.RepeatedValue)
	entities["sub_second_entity_id"] = &types.RepeatedValue{
		Val: []*types.Value{
			{Val: &types.Value_StringVal{StringVal: "entity-1"}},
		},
	}

	request := &serving.GetOnlineFeaturesRangeRequest{
		Kind: &serving.GetOnlineFeaturesRangeRequest_Features{
			Features: &serving.FeatureList{
				Val: []string{
					"sub_second_sort_key_view:event_timestamp",
					"sub_second_sort_key_view:value",
				},
			},
		},
		Entities: entities,
		SortKeyFilters: []*serving.SortKeyFilter{
			{
				SortKeyName: "event_timestamp",
				Query: &serving.SortKeyFilter_Range{
					Range: &serving.SortKeyFilter_RangeQuery{
						RangeStart: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 0}},
					},
				},
			},
		},
		Limit:           10,
		IncludeMetadata: true,
	}

	response, err := client.GetOnlineFeaturesRange(ctx, request)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Len(t, response.Results, 2, "expected event_timestamp and value results")

	eventTimestampIdx := -1
	for i, name := range response.Metadata.FeatureNames.Val {
		if name == "event_timestamp" {
			eventTimestampIdx = i
		}
	}
	require.NotEqual(t, -1, eventTimestampIdx, "expected to find event_timestamp in the response")

	eventTimestampVector := response.Results[eventTimestampIdx]
	require.Len(t, eventTimestampVector.Values, 1, "expected results for the single requested entity")

	timestamps := eventTimestampVector.Values[0].Val
	require.Len(t, timestamps, 3, "expected all 3 sub-second-apart rows to be returned")

	seen := make(map[int64]bool)
	for _, v := range timestamps {
		ts := v.GetUnixTimestampVal()
		assert.False(t, seen[ts], "sort key value %d was returned more than once; sub-second rows collapsed", ts)
		seen[ts] = true
	}
	assert.Len(t, seen, 3, "expected 3 distinct millisecond-precision sort key values")

	expected := map[int64]bool{
		1717244257886: true, // 2024-06-01 12:17:37.886
		1717244257904: true, // 2024-06-01 12:17:37.904
		1717244259035: true, // 2024-06-01 12:17:39.035
	}
	for ts := range seen {
		assert.True(t, expected[ts], "unexpected sort key value %d", ts)
	}

	// EventTimestamps come from the _ts:<fv> hash field (a timestamppb.Timestamp).
	// grpc_server calls GetTimestampSeconds, so values must be whole-second Unix timestamps.
	require.Len(t, eventTimestampVector.EventTimestamps, 1, "expected EventTimestamps for 1 entity")
	require.Len(t, eventTimestampVector.EventTimestamps[0].Val, 3, "expected EventTimestamp for each of the 3 rows")
	for _, tsVal := range eventTimestampVector.EventTimestamps[0].Val {
		secs := tsVal.GetUnixTimestampVal()
		assert.True(t, secs > 1_000_000_000 && secs < 2_000_000_000,
			"EventTimestamp must be seconds-precision (~2001–2033), got %d", secs)
	}
}

// TestGetOnlineFeaturesRange_CustomNamedSortKeyValuesRemainDistinct guards
// against the EAPC-22316 fix being tied to the "event_timestamp" column name
// rather than working for any UNIX_TIMESTAMP column declared as a sort key.
// The fixture (sub_second_custom_sortkey_view, seeded from
// sub_second_custom_sortkey_data.parquet) declares "viewed_at" - a name
// distinct from the source's own "event_timestamp" watermark field - as the
// sort key, mirroring the real customer schema. Same three-row shape: two
// rows 18ms apart within the same second, one two seconds later.
func TestGetOnlineFeaturesRange_CustomNamedSortKeyValuesRemainDistinct(t *testing.T) {
	entities := make(map[string]*types.RepeatedValue)
	entities["custom_sortkey_entity_id"] = &types.RepeatedValue{
		Val: []*types.Value{
			{Val: &types.Value_StringVal{StringVal: "entity-1"}},
		},
	}

	request := &serving.GetOnlineFeaturesRangeRequest{
		Kind: &serving.GetOnlineFeaturesRangeRequest_Features{
			Features: &serving.FeatureList{
				Val: []string{
					"sub_second_custom_sortkey_view:viewed_at",
					"sub_second_custom_sortkey_view:value",
				},
			},
		},
		Entities: entities,
		SortKeyFilters: []*serving.SortKeyFilter{
			{
				SortKeyName: "viewed_at",
				Query: &serving.SortKeyFilter_Range{
					Range: &serving.SortKeyFilter_RangeQuery{
						RangeStart: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 0}},
					},
				},
			},
		},
		Limit:           10,
		IncludeMetadata: true,
	}

	response, err := client.GetOnlineFeaturesRange(ctx, request)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Len(t, response.Results, 2, "expected viewed_at and value results")

	viewedAtIdx := -1
	for i, name := range response.Metadata.FeatureNames.Val {
		if name == "viewed_at" {
			viewedAtIdx = i
		}
	}
	require.NotEqual(t, -1, viewedAtIdx, "expected to find viewed_at in the response")

	viewedAtVector := response.Results[viewedAtIdx]
	require.Len(t, viewedAtVector.Values, 1, "expected results for the single requested entity")

	timestamps := viewedAtVector.Values[0].Val
	require.Len(t, timestamps, 3, "expected all 3 sub-second-apart rows to be returned")

	seen := make(map[int64]bool)
	for _, v := range timestamps {
		ts := v.GetUnixTimestampVal()
		assert.False(t, seen[ts], "sort key value %d was returned more than once; sub-second rows collapsed", ts)
		seen[ts] = true
	}
	assert.Len(t, seen, 3, "expected 3 distinct millisecond-precision sort key values")

	expected := map[int64]bool{
		1717244257886: true, // 2024-06-01 12:17:37.886
		1717244257904: true, // 2024-06-01 12:17:37.904
		1717244259035: true, // 2024-06-01 12:17:39.035
	}
	for ts := range seen {
		assert.True(t, expected[ts], "unexpected sort key value %d", ts)
	}

	// EventTimestamps come from the _ts:<fv> hash field (a timestamppb.Timestamp).
	// grpc_server calls GetTimestampSeconds, so values must be whole-second Unix timestamps.
	require.Len(t, viewedAtVector.EventTimestamps, 1, "expected EventTimestamps for 1 entity")
	require.Len(t, viewedAtVector.EventTimestamps[0].Val, 3, "expected EventTimestamp for each of the 3 rows")
	for _, tsVal := range viewedAtVector.EventTimestamps[0].Val {
		secs := tsVal.GetUnixTimestampVal()
		assert.True(t, secs > 1_000_000_000 && secs < 2_000_000_000,
			"EventTimestamp must be seconds-precision (~2001–2033), got %d", secs)
	}
}

func assertResponseData(t *testing.T, response *serving.GetOnlineFeaturesRangeResponse, featureNames []string, entitiesRequested int, includeMetadata bool) {
	assert.NotNil(t, response)
	assert.Equal(t, 1, len(response.Entities), "Should have 1 list of entity")
	indexIdEntity, exists := response.Entities["index_id"]
	assert.True(t, exists, "Should have index_id entity")
	assert.NotNil(t, indexIdEntity)
	assert.Equal(t, entitiesRequested, len(indexIdEntity.Val), "Entity should have %d values", entitiesRequested)
	assert.Equal(t, len(featureNames), len(response.Results), "Should have expected number of features")

	for i, featureResult := range response.Results {
		assert.Equal(t, entitiesRequested, len(featureResult.Values))
		if includeMetadata {
			assert.Equal(t, entitiesRequested, len(featureResult.Statuses))
			assert.Equal(t, entitiesRequested, len(featureResult.EventTimestamps), "Feature %s should have %d event timestamps", featureNames[i], entitiesRequested)
		}
		for j, value := range featureResult.Values {
			featureName := featureNames[i]
			if strings.Contains(featureName, "null") {
				// For null features, we expect the value to contain 1 entry with a nil value
				assert.NotNil(t, value)
				assert.Equal(t, 10, len(value.Val), "Feature %s should have one value, got %d %s", featureName, len(value.Val), value.Val)
				assert.Nil(t, value.Val[0].Val, "Feature %s should have a nil value", featureName)
			} else {
				assert.NotNil(t, value)
				assert.Equal(t, 10, len(value.Val), "Feature %s should have 10 values, got %d", featureName, len(value.Val))
			}

			if includeMetadata {
				for k := range value.Val {
					if strings.Contains(featureName, "null") {
						assert.Equal(t, serving.FieldStatus_NULL_VALUE, featureResult.Statuses[j].Status[k], "Feature %s should have NULL status", featureName)
					} else {
						assert.Equal(t, serving.FieldStatus_PRESENT, featureResult.Statuses[j].Status[k], "Feature %s should have PRESENT status", featureName)
					}
				}
			}
		}
	}
}

func getAllSortedFeatureNames() []string {
	return strings.Split(ALL_SORTED_FEATURE_NAMES, ",")
}

func getAllRegularFeatureNames() []string {
	return strings.Split(ALL_REGULAR_FEATURE_NAMES, ",")
}

