package metrics

import (
	"os"
	"testing"

	"github.com/feast-dev/feast/go/internal/feast/onlineserving"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/stretchr/testify/assert"
)

type metricCall struct {
	name  string
	value int64
	tags  []string
}

type fakeStatsdClient struct {
	calls []metricCall
}

func (f *fakeStatsdClient) Count(name string, value int64, tags []string, rate float64) error {
	f.calls = append(f.calls, metricCall{name: name, value: value, tags: tags})
	return nil
}

func newTestAggregator(client StatsdClient) *LookupMetricsAggregator {
	return NewLookupMetricsAggregator("test_project", "redis", "test_service", "test", client)
}

func TestAggregator_AllNotFound(t *testing.T) {
	fake := &fakeStatsdClient{}
	agg := newTestAggregator(fake)

	agg.Record("user_fv__age", serving.FieldStatus_NOT_FOUND)
	agg.Record("user_fv__age", serving.FieldStatus_NOT_FOUND)
	agg.Record("user_fv__age", serving.FieldStatus_NOT_FOUND)
	agg.Emit()

	assert.Len(t, fake.calls, 1)
	assert.Equal(t, "feast.feature_server.feature_lookup_not_found", fake.calls[0].name)
	assert.Equal(t, int64(3), fake.calls[0].value)
	assert.Contains(t, fake.calls[0].tags, "feature:user_fv__age")
}

func TestAggregator_AllNullOrExpired(t *testing.T) {
	fake := &fakeStatsdClient{}
	agg := newTestAggregator(fake)

	agg.Record("order_fv__amt", serving.FieldStatus_NULL_VALUE)
	agg.Record("order_fv__amt", serving.FieldStatus_NULL_VALUE)
	agg.Record("order_fv__amt", serving.FieldStatus_OUTSIDE_MAX_AGE)
	agg.Emit()

	assert.Len(t, fake.calls, 1)
	assert.Equal(t, "feast.feature_server.feature_lookup_null_or_expired", fake.calls[0].name)
	assert.Equal(t, int64(3), fake.calls[0].value)
	assert.Contains(t, fake.calls[0].tags, "feature:order_fv__amt")
}

func TestAggregator_MixedStatuses(t *testing.T) {
	fake := &fakeStatsdClient{}
	agg := newTestAggregator(fake)

	agg.Record("fv_a__f1", serving.FieldStatus_PRESENT)
	agg.Record("fv_a__f1", serving.FieldStatus_NOT_FOUND)
	agg.Record("fv_b__f2", serving.FieldStatus_NULL_VALUE)
	agg.Record("fv_b__f2", serving.FieldStatus_PRESENT)
	agg.Record("fv_b__f2", serving.FieldStatus_OUTSIDE_MAX_AGE)
	agg.Emit()

	assert.Len(t, fake.calls, 2)

	callsByName := map[string]metricCall{}
	for _, c := range fake.calls {
		callsByName[c.name+":"+findTag(c.tags, "feature:")] = c
	}

	nf := callsByName["feast.feature_server.feature_lookup_not_found:fv_a__f1"]
	assert.Equal(t, int64(1), nf.value)

	ne := callsByName["feast.feature_server.feature_lookup_null_or_expired:fv_b__f2"]
	assert.Equal(t, int64(2), ne.value)
}

func TestAggregator_AllPresent(t *testing.T) {
	fake := &fakeStatsdClient{}
	agg := newTestAggregator(fake)

	agg.Record("fv__f1", serving.FieldStatus_PRESENT)
	agg.Record("fv__f1", serving.FieldStatus_PRESENT)
	agg.Emit()

	assert.Len(t, fake.calls, 0)
}

func TestAggregator_NilSafe(t *testing.T) {
	var agg *LookupMetricsAggregator
	agg.Record("fv__f1", serving.FieldStatus_NOT_FOUND)
	agg.RecordFromFeatureVectors(nil)
	agg.RecordFromRangeFeatureVectors(nil)
	agg.Emit()
}

func TestAggregator_NilClient(t *testing.T) {
	agg := NewLookupMetricsAggregator("p", "r", "s", "e", nil)
	assert.Nil(t, agg)
}

func TestAggregator_Tags(t *testing.T) {
	fake := &fakeStatsdClient{}
	agg := NewLookupMetricsAggregator("mlpfs", "eg-valkey", "ranking-fs", "dw", fake)

	agg.Record("hotel_fv__price", serving.FieldStatus_NOT_FOUND)
	agg.Emit()

	assert.Len(t, fake.calls, 1)
	tags := fake.calls[0].tags
	assert.Contains(t, tags, "project:mlpfs")
	assert.Contains(t, tags, "online_store_type:eg-valkey")
	assert.Contains(t, tags, "service:ranking-fs")
	assert.Contains(t, tags, "env:dw")
	assert.Contains(t, tags, "feature:hotel_fv__price")
	assert.Contains(t, tags, "feature_view:hotel_fv")
}

func TestRecordFromFeatureVectors(t *testing.T) {
	fake := &fakeStatsdClient{}
	agg := newTestAggregator(fake)

	vectors := []*onlineserving.FeatureVector{
		{
			Name:     "fv_a__f1",
			Statuses: []serving.FieldStatus{serving.FieldStatus_PRESENT, serving.FieldStatus_NOT_FOUND},
		},
		{
			Name:     "fv_a__f2",
			Statuses: []serving.FieldStatus{serving.FieldStatus_NOT_FOUND, serving.FieldStatus_NOT_FOUND},
		},
	}

	agg.RecordFromFeatureVectors(vectors)
	agg.Emit()

	assert.Len(t, fake.calls, 2)

	callsByFeature := map[string]int64{}
	for _, c := range fake.calls {
		callsByFeature[findTag(c.tags, "feature:")] = c.value
	}
	assert.Equal(t, int64(1), callsByFeature["fv_a__f1"])
	assert.Equal(t, int64(2), callsByFeature["fv_a__f2"])
}

func TestRecordFromRangeFeatureVectors(t *testing.T) {
	fake := &fakeStatsdClient{}
	agg := newTestAggregator(fake)

	vectors := []*onlineserving.RangeFeatureVector{
		{
			Name: "sfv__f1",
			RangeStatuses: [][]serving.FieldStatus{
				{serving.FieldStatus_PRESENT, serving.FieldStatus_NOT_FOUND},
				{serving.FieldStatus_NOT_FOUND},
			},
		},
	}

	agg.RecordFromRangeFeatureVectors(vectors)
	agg.Emit()

	assert.Len(t, fake.calls, 1)
	assert.Equal(t, int64(2), fake.calls[0].value)
	assert.Equal(t, "feast.feature_server.feature_lookup_not_found", fake.calls[0].name)
}

func TestIsMissingKeyMetricsEnabled(t *testing.T) {
	os.Unsetenv("ENABLE_MISSING_KEY_METRICS")
	assert.False(t, IsMissingKeyMetricsEnabled())

	os.Setenv("ENABLE_MISSING_KEY_METRICS", "true")
	assert.True(t, IsMissingKeyMetricsEnabled())

	os.Setenv("ENABLE_MISSING_KEY_METRICS", "TRUE")
	assert.True(t, IsMissingKeyMetricsEnabled())

	os.Setenv("ENABLE_MISSING_KEY_METRICS", "false")
	assert.False(t, IsMissingKeyMetricsEnabled())

	os.Unsetenv("ENABLE_MISSING_KEY_METRICS")
}

func TestGetOnlineStoreType(t *testing.T) {
	// Test with mock config that has "type" key
	assert.Equal(t, "unknown_service", GetServiceName())
	assert.Equal(t, "unknown_env", GetEnvironment())
}

func TestExtractFeatureView(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"standard format", "hotel_fv__price", "hotel_fv"},
		{"with underscore in feature", "hotel_fv__review_score_avg", "hotel_fv"},
		{"multiple feature views", "user_fv__age", "user_fv"},
		{"long feature view name", "ranking_signals_fv__score", "ranking_signals_fv"},
		{"no double underscore", "age", "unknown"},
		{"colon separator", "hotel_fv:price", "unknown"},
		{"empty string", "", "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractFeatureView(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// findTag extracts the value portion of a tag matching the given prefix.
func findTag(tags []string, prefix string) string {
	for _, tag := range tags {
		if len(tag) > len(prefix) && tag[:len(prefix)] == prefix {
			return tag[len(prefix):]
		}
	}
	return ""
}
