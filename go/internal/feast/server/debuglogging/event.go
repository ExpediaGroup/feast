package debuglogging

import "github.com/rs/zerolog"

// RequestEvent captures every field for a single structured debug-log line,
// per the schema in docs/superpowers/specs/2026-07-06-feature-view-request-debug-logging-design.md.
type RequestEvent struct {
	Project               string
	FeatureViews          []string
	RequestPath           string
	Transport             string
	FeaturesRequested     int
	FeaturesReturnedCount int
	NullFieldCount        int
	StoreRTTMs            float64
	OnlineStoreType       string
	ErrorType             *string
}

// Emit writes one structured JSON log line for a debug-flagged/sampled
// request. Uses Info level: the Go server's default level is Info (it has
// no SetGlobalLevel call anywhere), so this is already visible without any
// level-bypass mechanism. The "event" field lets Splunk/log queries filter
// this line out from ordinary request logs.
func Emit(logger zerolog.Logger, event RequestEvent) {
	e := logger.Info().
		Str("event", "feature_view_request_debug_log").
		Str("project", event.Project).
		Strs("feature_view", event.FeatureViews).
		Str("request_path", event.RequestPath).
		Str("transport", event.Transport).
		Int("features_requested", event.FeaturesRequested).
		Int("features_returned_count", event.FeaturesReturnedCount).
		Int("null_field_count", event.NullFieldCount).
		Float64("store_rtt_ms", event.StoreRTTMs).
		Str("online_store_type", event.OnlineStoreType)

	if event.ErrorType != nil {
		e = e.Str("error_type", *event.ErrorType)
	} else {
		e = e.Interface("error_type", nil)
	}

	e.Msg("feature view request debug log")
}
