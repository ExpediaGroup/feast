package metrics

import (
	"math/rand"
	"os"
	"strconv"
	"strings"

	"github.com/feast-dev/feast/go/internal/feast/onlineserving"
	"github.com/feast-dev/feast/go/protos/feast/serving"
)

// extractFeatureView extracts the feature view name from a full feature name.
// Feature names follow the format: feature_view__feature_name
// Example: "hotel_fv__price" -> "hotel_fv"
func extractFeatureView(featureName string) string {
	parts := strings.SplitN(featureName, "__", 2)
	if len(parts) == 2 {
		return parts[0]
	}
	return "unknown"
}

type LookupMetricsAggregator struct {
	notFound      map[string]int64
	nullOrExpired map[string]int64
	project       string
	onlineStore   string
	client        StatsdClient
	sampleRate    float64
}

func NewLookupMetricsAggregator(
	project, onlineStore string,
	client StatsdClient,
) *LookupMetricsAggregator {
	if client == nil {
		return nil
	}

	// Read sampling rate from environment (default: 1.0 = no sampling)
	sampleRate := 1.0
	if rateStr := os.Getenv("FEAST_METRICS_SAMPLE_RATE"); rateStr != "" {
		if rate, err := strconv.ParseFloat(rateStr, 64); err == nil {
			if rate > 0 && rate <= 1.0 {
				sampleRate = rate
			}
		}
	}

	return &LookupMetricsAggregator{
		notFound:      make(map[string]int64),
		nullOrExpired: make(map[string]int64),
		project:       project,
		onlineStore:   onlineStore,
		client:        client,
		sampleRate:    sampleRate,
	}
}

func (m *LookupMetricsAggregator) Record(featureID string, status serving.FieldStatus) {
	if m == nil {
		return
	}
	switch status {
	case serving.FieldStatus_NOT_FOUND:
		m.notFound[featureID]++
	case serving.FieldStatus_NULL_VALUE, serving.FieldStatus_OUTSIDE_MAX_AGE:
		m.nullOrExpired[featureID]++
	}
}

func (m *LookupMetricsAggregator) RecordFromFeatureVectors(vectors []*onlineserving.FeatureVector) {
	if m == nil {
		return
	}
	for _, vector := range vectors {
		for _, status := range vector.Statuses {
			m.Record(vector.Name, status)
		}
	}
}

func (m *LookupMetricsAggregator) RecordFromRangeFeatureVectors(vectors []*onlineserving.RangeFeatureVector) {
	if m == nil {
		return
	}
	for _, vector := range vectors {
		for _, entityStatuses := range vector.RangeStatuses {
			for _, status := range entityStatuses {
				m.Record(vector.Name, status)
			}
		}
	}
}

func (m *LookupMetricsAggregator) Emit() {
	if m == nil || m.client == nil {
		return
	}

	// Probabilistic sampling: skip this request's metrics based on sample_rate
	if m.sampleRate < 1.0 && rand.Float64() > m.sampleRate {
		return
	}

	// Calculate multiplier to preserve statistical accuracy
	// If sampleRate=0.1, we only emit 10% of the time, so multiply counts by 10
	multiplier := 1.0 / m.sampleRate

	baseTags := []string{
		"project:" + m.project,
		"online_store_type:" + m.onlineStore,
	}

	for featureID, count := range m.notFound {
		if count == 0 {
			continue
		}
		// Adjust count to preserve accuracy when sampling
		adjustedCount := int64(float64(count) * multiplier)
		tags := make([]string, len(baseTags)+2)
		copy(tags, baseTags)
		tags[len(baseTags)] = "feature:" + featureID
		tags[len(baseTags)+1] = "feature_view:" + extractFeatureView(featureID)
		m.client.Count("mlpfs.featureserver.feature_lookup_not_found", adjustedCount, tags, 1.0)
	}

	for featureID, count := range m.nullOrExpired {
		if count == 0 {
			continue
		}
		adjustedCount := int64(float64(count) * multiplier)
		tags := make([]string, len(baseTags)+2)
		copy(tags, baseTags)
		tags[len(baseTags)] = "feature:" + featureID
		tags[len(baseTags)+1] = "feature_view:" + extractFeatureView(featureID)
		m.client.Count("mlpfs.featureserver.feature_lookup_null_or_expired", adjustedCount, tags, 1.0)
	}
}
