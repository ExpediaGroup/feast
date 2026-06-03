package metrics

import (
	"math/rand"
	"os"
	"strconv"
)

const (
	FVReadLatencyMetric  = "mlpfs.featureserver.fv_read_latency_ms"
	FVReadRequestsMetric = "mlpfs.featureserver.fv_read_requests"
	FVReadErrorsMetric   = "mlpfs.featureserver.fv_read_errors"
)

type FeatureViewReadMetrics struct {
	project     string
	onlineStore string
	client      StatsdClient
	sampleRate  float64
}

func NewFeatureViewReadMetrics(project, onlineStore string, client StatsdClient) *FeatureViewReadMetrics {
	if client == nil {
		return nil
	}

	sampleRate := 1.0
	if rateStr := os.Getenv("FEAST_METRICS_SAMPLE_RATE"); rateStr != "" {
		if rate, err := strconv.ParseFloat(rateStr, 64); err == nil {
			if rate > 0 && rate <= 1.0 {
				sampleRate = rate
			}
		}
	}

	return &FeatureViewReadMetrics{
		project:     project,
		onlineStore: onlineStore,
		client:      client,
		sampleRate:  sampleRate,
	}
}

// Emit emits latency, request count, and optionally errors for each feature view.
func (m *FeatureViewReadMetrics) Emit(featureViewNames []string, latencyMs float64, hasError bool) {
	if m == nil || m.client == nil {
		return
	}

	if m.sampleRate < 1.0 && rand.Float64() > m.sampleRate {
		return
	}

	baseTags := []string{
		"project:" + m.project,
		"online_store_type:" + m.onlineStore,
	}

	for _, fvName := range featureViewNames {
		tags := make([]string, len(baseTags)+1)
		copy(tags, baseTags)
		tags[len(baseTags)] = "feature_view:" + fvName

		m.client.Distribution(FVReadLatencyMetric, latencyMs, tags, 1.0)
		m.client.Count(FVReadRequestsMetric, 1, tags, 1.0)

		if hasError {
			m.client.Count(FVReadErrorsMetric, 1, tags, 1.0)
		}
	}
}
