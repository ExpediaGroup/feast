package server

import (
	"net/http"
	"os"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"github.com/feast-dev/feast/go/internal/feast/metrics"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

func LogWithSpanContext(span *tracer.Span) zerolog.Logger {
	spanContext := span.Context()

	var logger = zerolog.New(os.Stderr).With().
		Int64("trace_id", int64(spanContext.TraceIDLower())).
		Int64("span_id", int64(spanContext.SpanID())).
		Timestamp().
		Logger()

	return logger
}

// MetricsContext holds pre-initialized metrics objects for the server lifetime.
// Created once at server startup to avoid per-request allocations and env reads.
type MetricsContext struct {
	FVReadMetrics *metrics.FeatureViewReadMetrics
	SampleRate    float64
	Project       string
	OnlineStore   string
	Client        metrics.StatsdClient
}

func NewMetricsContext(client metrics.StatsdClient, config *registry.RepoConfig) *MetricsContext {
	if client == nil || config == nil {
		return nil
	}
	sampleRate := metrics.ParseSampleRate()
	project := config.Project
	onlineStore := metrics.GetOnlineStoreType(config)

	return &MetricsContext{
		FVReadMetrics: metrics.NewFeatureViewReadMetrics(project, onlineStore, client, sampleRate),
		SampleRate:    sampleRate,
		Project:       project,
		OnlineStore:   onlineStore,
		Client:        client,
	}
}

func (mc *MetricsContext) NewLookupAggregator() *metrics.LookupMetricsAggregator {
	if mc == nil {
		return nil
	}
	return metrics.NewLookupMetricsAggregator(mc.Project, mc.OnlineStore, mc.Client, mc.SampleRate)
}

func CommonHttpHandlers(s *HttpServer, healthCheckHandler http.HandlerFunc) []Handler {
	return []Handler{
		{
			Path:        "/get-online-features",
			HandlerFunc: recoverMiddleware(http.HandlerFunc(s.getOnlineFeatures)),
		},
		{
			Path:        "/get-online-features-range",
			HandlerFunc: recoverMiddleware(http.HandlerFunc(s.getOnlineFeaturesRange)),
		},
		{
			Path:        "/version",
			HandlerFunc: recoverMiddleware(http.HandlerFunc(s.getVersion)),
		},
		{
			Path:        "/metrics",
			HandlerFunc: promhttp.Handler(),
		},
		{
			Path:        "/health",
			HandlerFunc: healthCheckHandler,
		},
	}
}
