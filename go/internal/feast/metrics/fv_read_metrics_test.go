package metrics

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFVMetrics_EmitLatencyDistribution(t *testing.T) {
	fake := &fakeStatsdClient{}
	m := NewFeatureViewReadMetrics("proj", "redis", fake)

	m.Emit([]string{"hotel_fv"}, 42.5, false)

	assert.Len(t, fake.distCalls, 1)
	assert.Equal(t, FVReadLatencyMetric, fake.distCalls[0].name)
	assert.Equal(t, 42.5, fake.distCalls[0].value)
	assert.Contains(t, fake.distCalls[0].tags, "project:proj")
	assert.Contains(t, fake.distCalls[0].tags, "online_store_type:redis")
	assert.Contains(t, fake.distCalls[0].tags, "feature_view:hotel_fv")
}

func TestFVMetrics_EmitRequestsCount(t *testing.T) {
	fake := &fakeStatsdClient{}
	m := NewFeatureViewReadMetrics("proj", "redis", fake)

	m.Emit([]string{"hotel_fv"}, 10.0, false)

	requestCalls := filterCalls(fake.calls, FVReadRequestsMetric)
	assert.Len(t, requestCalls, 1)
	assert.Equal(t, int64(1), requestCalls[0].value)
	assert.Contains(t, requestCalls[0].tags, "feature_view:hotel_fv")
}

func TestFVMetrics_EmitErrors(t *testing.T) {
	fake := &fakeStatsdClient{}
	m := NewFeatureViewReadMetrics("proj", "redis", fake)

	m.Emit([]string{"hotel_fv"}, 100.0, true)

	errorCalls := filterCalls(fake.calls, FVReadErrorsMetric)
	assert.Len(t, errorCalls, 1)
	assert.Equal(t, int64(1), errorCalls[0].value)
	assert.Contains(t, errorCalls[0].tags, "feature_view:hotel_fv")
}

func TestFVMetrics_NoErrorsWhenSuccess(t *testing.T) {
	fake := &fakeStatsdClient{}
	m := NewFeatureViewReadMetrics("proj", "redis", fake)

	m.Emit([]string{"hotel_fv"}, 50.0, false)

	errorCalls := filterCalls(fake.calls, FVReadErrorsMetric)
	assert.Len(t, errorCalls, 0)
}

func TestFVMetrics_MultipleFeatureViews(t *testing.T) {
	fake := &fakeStatsdClient{}
	m := NewFeatureViewReadMetrics("proj", "valkey", fake)

	m.Emit([]string{"hotel_fv", "user_fv", "booking_fv"}, 75.0, false)

	// 3 latency distributions
	assert.Len(t, fake.distCalls, 3)

	// 3 request counts
	requestCalls := filterCalls(fake.calls, FVReadRequestsMetric)
	assert.Len(t, requestCalls, 3)

	// All have same latency
	for _, dc := range fake.distCalls {
		assert.Equal(t, 75.0, dc.value)
	}

	// Check each FV is tagged
	fvTags := make(map[string]bool)
	for _, dc := range fake.distCalls {
		fvTags[findTag(dc.tags, "feature_view:")] = true
	}
	assert.True(t, fvTags["hotel_fv"])
	assert.True(t, fvTags["user_fv"])
	assert.True(t, fvTags["booking_fv"])
}

func TestFVMetrics_NilClient(t *testing.T) {
	m := NewFeatureViewReadMetrics("proj", "redis", nil)
	assert.Nil(t, m)
}

func TestFVMetrics_NilSafe(t *testing.T) {
	var m *FeatureViewReadMetrics
	m.Emit([]string{"fv"}, 10.0, false) // should not panic
}

func TestFVMetrics_Sampling(t *testing.T) {
	os.Setenv("FEAST_METRICS_SAMPLE_RATE", "0.5")
	defer os.Unsetenv("FEAST_METRICS_SAMPLE_RATE")

	fake := &fakeStatsdClient{}
	m := NewFeatureViewReadMetrics("proj", "redis", fake)

	assert.Equal(t, 0.5, m.sampleRate)

	// Emit many times — should skip roughly half
	emitted := 0
	for i := 0; i < 100; i++ {
		fake.distCalls = nil
		m.Emit([]string{"fv"}, 10.0, false)
		if len(fake.distCalls) > 0 {
			emitted++
		}
	}

	// With 50% sampling, should emit roughly 50 times (allow wide margin)
	assert.Greater(t, emitted, 20)
	assert.Less(t, emitted, 80)
}

func TestIsFVMetricsEnabled(t *testing.T) {
	os.Unsetenv("ENABLE_FV_LEVEL_METRICS")
	os.Unsetenv("ENABLE_MISSING_KEY_METRICS")
	assert.False(t, IsFVMetricsEnabled())

	os.Setenv("ENABLE_FV_LEVEL_METRICS", "true")
	assert.True(t, IsFVMetricsEnabled())
	os.Unsetenv("ENABLE_FV_LEVEL_METRICS")

	os.Setenv("ENABLE_MISSING_KEY_METRICS", "true")
	assert.True(t, IsFVMetricsEnabled())
	os.Unsetenv("ENABLE_MISSING_KEY_METRICS")
}
