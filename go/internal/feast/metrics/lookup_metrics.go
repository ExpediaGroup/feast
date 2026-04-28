package metrics

import (
	"github.com/feast-dev/feast/go/internal/feast/onlineserving"
	"github.com/feast-dev/feast/go/protos/feast/serving"
)

type LookupMetricsAggregator struct {
	notFound      map[string]int64
	nullOrExpired map[string]int64
	project       string
	onlineStore   string
	service       string
	env           string
	client        StatsdClient
}

func NewLookupMetricsAggregator(
	project, onlineStore, service, env string,
	client StatsdClient,
) *LookupMetricsAggregator {
	if client == nil {
		return nil
	}
	return &LookupMetricsAggregator{
		notFound:      make(map[string]int64),
		nullOrExpired: make(map[string]int64),
		project:       project,
		onlineStore:   onlineStore,
		service:       service,
		env:           env,
		client:        client,
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

	baseTags := []string{
		"project:" + m.project,
		"online_store_type:" + m.onlineStore,
		"service:" + m.service,
		"env:" + m.env,
	}

	for featureID, count := range m.notFound {
		if count == 0 {
			continue
		}
		tags := make([]string, len(baseTags)+1)
		copy(tags, baseTags)
		tags[len(baseTags)] = "feature:" + featureID
		m.client.Count("feast.feature_server.feature_lookup_not_found", count, tags, 1.0)
	}

	for featureID, count := range m.nullOrExpired {
		if count == 0 {
			continue
		}
		tags := make([]string, len(baseTags)+1)
		copy(tags, baseTags)
		tags[len(baseTags)] = "feature:" + featureID
		m.client.Count("feast.feature_server.feature_lookup_null_or_expired", count, tags, 1.0)
	}
}
