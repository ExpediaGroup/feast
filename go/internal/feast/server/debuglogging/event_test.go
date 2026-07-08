package debuglogging

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmit_WritesExpectedFields(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)

	Emit(logger, RequestEvent{
		Project:               "p13n",
		FeatureViews:          []string{"customer_profile"},
		RequestPath:           "/get-online-features",
		Transport:             "http",
		FeaturesRequested:     5,
		FeaturesReturnedCount: 5,
		NullFieldCount:        0,
		StoreRTTMs:            4.2,
		OnlineStoreType:       "cassandra",
		ErrorType:             nil,
		EntityKeyHashes:       []string{"sha256:abc"},
	})

	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &decoded))

	assert.Equal(t, "feature_view_request_debug_log", decoded["event"])
	assert.Equal(t, "p13n", decoded["project"])
	assert.Equal(t, []interface{}{"customer_profile"}, decoded["feature_view"])
	assert.Equal(t, "/get-online-features", decoded["request_path"])
	assert.Equal(t, "http", decoded["transport"])
	assert.Equal(t, float64(5), decoded["features_requested"])
	assert.Equal(t, float64(5), decoded["features_returned_count"])
	assert.Equal(t, float64(0), decoded["null_field_count"])
	assert.Equal(t, 4.2, decoded["store_rtt_ms"])
	assert.Equal(t, "cassandra", decoded["online_store_type"])
	assert.Nil(t, decoded["error_type"])
	assert.Equal(t, []interface{}{"sha256:abc"}, decoded["entity_key_hash"])
}

func TestEmit_WritesErrorTypeWhenPresent(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	errType := "NotFound"

	Emit(logger, RequestEvent{ErrorType: &errType})

	var decoded map[string]interface{}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &decoded))

	assert.Equal(t, "NotFound", decoded["error_type"])
}
