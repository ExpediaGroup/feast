//go:build !integration

package server

import (
	"bytes"
	"encoding/json"
	"os"
	"sort"
	"testing"

	"github.com/feast-dev/feast/go/internal/feast/metrics"
	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/onlineserving"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/internal/feast/server/debuglogging"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeStatsdClient struct{}

func (f *fakeStatsdClient) Count(string, int64, []string, float64) error          { return nil }
func (f *fakeStatsdClient) Distribution(string, float64, []string, float64) error { return nil }

func TestNewLookupAggregator_NilWhenMissingKeyMetricsDisabled(t *testing.T) {
	os.Unsetenv("ENABLE_MISSING_KEY_METRICS")
	mc := &MetricsContext{
		MissingKeyMetricsEnabled: metrics.IsMissingKeyMetricsEnabled(),
		Project:                  "test",
		OnlineStore:              "redis",
		Client:                   &fakeStatsdClient{},
		SampleRate:               1.0,
	}
	assert.Nil(t, mc.NewLookupAggregator())
}

func TestNewLookupAggregator_NonNilWhenMissingKeyMetricsEnabled(t *testing.T) {
	os.Setenv("ENABLE_MISSING_KEY_METRICS", "true")
	defer os.Unsetenv("ENABLE_MISSING_KEY_METRICS")
	mc := &MetricsContext{
		MissingKeyMetricsEnabled: metrics.IsMissingKeyMetricsEnabled(),
		Project:                  "test",
		OnlineStore:              "redis",
		Client:                   &fakeStatsdClient{},
		SampleRate:               1.0,
	}
	assert.NotNil(t, mc.NewLookupAggregator())
}

func TestNewMetricsContext_FVReadMetricsNilWhenFVMetricsDisabled(t *testing.T) {
	os.Unsetenv("ENABLE_FV_LEVEL_METRICS")
	os.Setenv("ENABLE_MISSING_KEY_METRICS", "true")
	defer os.Unsetenv("ENABLE_MISSING_KEY_METRICS")

	config := &registry.RepoConfig{Project: "test"}
	mc := NewMetricsContext(&fakeStatsdClient{}, config)

	assert.NotNil(t, mc)
	assert.Nil(t, mc.FVReadMetrics)
	assert.True(t, mc.MissingKeyMetricsEnabled)
}

func TestNewMetricsContext_LookupMetricsDisabledWhenMissingKeyMetricsDisabled(t *testing.T) {
	os.Setenv("ENABLE_FV_LEVEL_METRICS", "true")
	defer os.Unsetenv("ENABLE_FV_LEVEL_METRICS")
	os.Unsetenv("ENABLE_MISSING_KEY_METRICS")

	config := &registry.RepoConfig{Project: "test"}
	mc := NewMetricsContext(&fakeStatsdClient{}, config)

	assert.NotNil(t, mc)
	assert.NotNil(t, mc.FVReadMetrics)
	assert.False(t, mc.MissingKeyMetricsEnabled)
	assert.Nil(t, mc.NewLookupAggregator())
}

func sortedStrings(s []string) []string {
	out := make([]string, len(s))
	copy(out, s)
	sort.Strings(out)
	return out
}

func TestExtractFVNamesFromRequest_FeatureRefs(t *testing.T) {
	names := extractFVNamesFromRequest([]string{"hotel_fv:price", "hotel_fv:rating", "user_fv:age"}, nil)
	assert.Equal(t, []string{"hotel_fv", "user_fv"}, sortedStrings(names))
}

func TestExtractFVNamesFromRequest_NoColon(t *testing.T) {
	// refs without ":" are ignored (not a valid feature ref)
	names := extractFVNamesFromRequest([]string{"hotel_fv_price"}, nil)
	assert.Empty(t, names)
}

func TestExtractFVNamesFromRequest_FeatureService(t *testing.T) {
	fs := &model.FeatureService{
		Projections: []*model.FeatureViewProjection{
			{Name: "hotel_fv", NameAlias: ""},
			{Name: "user_fv", NameAlias: ""},
		},
	}
	names := extractFVNamesFromRequest(nil, fs)
	assert.Equal(t, []string{"hotel_fv", "user_fv"}, sortedStrings(names))
}

func TestExtractFVNamesFromRequest_Deduplication(t *testing.T) {
	names := extractFVNamesFromRequest([]string{"hotel_fv:price", "hotel_fv:rating"}, nil)
	assert.Equal(t, []string{"hotel_fv"}, names)
}

func TestExtractFVNamesFromRequest_Empty(t *testing.T) {
	names := extractFVNamesFromRequest(nil, nil)
	assert.Empty(t, names)
}

func TestEmitDebugRequestLog_NotEmittedWhenShouldEmitFalse(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)

	EmitDebugRequestLog(logger, debuglogging.Config{Enabled: false, SampleRate: 0}, false,
		"p13n", []string{"customer_profile"}, "http", "/get-online-features",
		1, 0, nil, "cassandra", 4.2, nil)

	assert.Empty(t, buf.Bytes())
}

func TestEmitDebugRequestLog_EmittedWhenRequestFlagged(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	vectors := []*onlineserving.FeatureVector{
		{Name: "f1", Statuses: []serving.FieldStatus{serving.FieldStatus_NULL_VALUE}},
	}

	EmitDebugRequestLog(logger, debuglogging.Config{Enabled: false, SampleRate: 0}, true,
		"p13n", []string{"customer_profile"}, "http", "/get-online-features",
		1, 0, vectors, "cassandra", 4.2, nil)

	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &decoded))
	assert.Equal(t, "feature_view_request_debug_log", decoded["event"])
	assert.Equal(t, float64(1), decoded["null_field_count"])
	assert.Equal(t, "cassandra", decoded["online_store_type"])
}

func TestEmitDebugRequestLog_ExcludesEntityColumnsFromReturnedCount(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	// Two returned columns: one entity/join-key column plus one feature column.
	vectors := []*onlineserving.FeatureVector{
		{Name: "driver_id"},
		{Name: "conv_rate"},
	}

	// entityKeyCount = 1, so features_returned_count must be 2 - 1 = 1,
	// matching features_requested (1) rather than the raw column count (2).
	EmitDebugRequestLog(logger, debuglogging.Config{Enabled: false, SampleRate: 0}, true,
		"p13n", []string{"driver_hourly_stats"}, "http", "/get-online-features",
		1, 1, vectors, "cassandra", 4.2, nil)

	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &decoded))
	assert.Equal(t, float64(1), decoded["features_requested"])
	assert.Equal(t, float64(1), decoded["features_returned_count"])
}

func TestEmitDebugRequestLog_ReturnedCountClampsAtZeroOnError(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)

	// Error path: featureVectors is nil (len 0) but entityKeyCount is 2;
	// features_returned_count must clamp to 0, not go negative.
	EmitDebugRequestLog(logger, debuglogging.Config{Enabled: false, SampleRate: 0}, true,
		"p13n", []string{"driver_hourly_stats"}, "http", "/get-online-features",
		1, 2, nil, "cassandra", 4.2, assert.AnError)

	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &decoded))
	assert.Equal(t, float64(0), decoded["features_returned_count"])
}

func TestEmitDebugRequestLogRange_EmittedWhenRequestFlagged(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	vectors := []*onlineserving.RangeFeatureVector{
		{Name: "f1", RangeStatuses: [][]serving.FieldStatus{{serving.FieldStatus_OUTSIDE_MAX_AGE}}},
	}

	EmitDebugRequestLogRange(logger, debuglogging.Config{Enabled: false, SampleRate: 0}, true,
		"p13n", []string{"customer_profile"}, "http", "/get-online-features-range",
		1, vectors, "cassandra", 4.2, nil)

	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &decoded))
	assert.Equal(t, "feature_view_request_debug_log", decoded["event"])
	assert.Equal(t, float64(1), decoded["null_field_count"])
}
