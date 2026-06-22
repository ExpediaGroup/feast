package metrics

const (
	FVReadLatencyMetric  = "mlpfs.featureserver.fv_read_latency_ms"
	FVReadRequestsMetric = "mlpfs.featureserver.fv_read_requests"
	FVReadErrorsMetric   = "mlpfs.featureserver.fv_read_errors"
)

// FeatureViewReadMetrics is a singleton that emits per-feature-view read metrics.
// Create once at server startup via NewFeatureViewReadMetrics and reuse across requests.
type FeatureViewReadMetrics struct {
	baseTags   []string
	client     StatsdClient
	sampleRate float64
}

// NewFeatureViewReadMetrics creates a reusable metrics emitter. The sampleRate
// is parsed once at startup (see ParseSampleRate) rather than reading the
// environment on every request.
func NewFeatureViewReadMetrics(project, onlineStore string, client StatsdClient, sampleRate float64) *FeatureViewReadMetrics {
	if client == nil {
		return nil
	}

	return &FeatureViewReadMetrics{
		baseTags: []string{
			"project:" + project,
			"online_store_type:" + onlineStore,
		},
		client:     client,
		sampleRate: sampleRate,
	}
}

// Emit emits latency, request count, and optionally errors for each feature view.
func (m *FeatureViewReadMetrics) Emit(featureViewNames []string, latencyMs float64, hasError bool) {
	if m == nil || m.client == nil {
		return
	}

	for _, fvName := range featureViewNames {
		tags := make([]string, len(m.baseTags)+1)
		copy(tags, m.baseTags)
		tags[len(m.baseTags)] = "feature_view:" + fvName

		m.client.Distribution(FVReadLatencyMetric, latencyMs, tags, m.sampleRate)
		m.client.Count(FVReadRequestsMetric, 1, tags, m.sampleRate)

		if hasError {
			m.client.Count(FVReadErrorsMetric, 1, tags, m.sampleRate)
		}
	}
}
