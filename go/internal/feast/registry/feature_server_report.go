package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	registryPb "github.com/feast-dev/feast/go/protos/feast/registry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Registry type names carried in FeatureServerReport.registry.
const (
	FeatureServerReportRegistryHttp = "http"
	FeatureServerReportRegistryGrpc = "grpc"
)

// FeatureServerReportTransport sends a feature server report to the registry
// the server already uses, for the project the store was created with.
// Implemented by HttpRegistryStore (REST route) and GrpcRegistryStore
// (ReportFeatureServer RPC). Other stores have no transport.
type FeatureServerReportTransport interface {
	// ReportFeatureServer sends one report. A nil error means the registry
	// stored it or found nothing changed. Any other error is a
	// *FeatureServerReportError.
	ReportFeatureServer(ctx context.Context, report *registryPb.FeatureServerReport) error
	// FeatureServerReportRegistry is the value for the report's registry field: "http" or "grpc".
	FeatureServerReportRegistry() string
}

// FeatureServerReportError is a report the registry did not accept.
// Retryable is true for outcomes worth one later retry (no response, 5xx,
// UNAVAILABLE, DEADLINE_EXCEEDED) and false for rejections (4xx,
// INVALID_ARGUMENT, NOT_FOUND, RESOURCE_EXHAUSTED, UNAUTHENTICATED, ...).
type FeatureServerReportError struct {
	Retryable bool
	// Outcome is a short label for logs: an HTTP status code, a gRPC code
	// name, or "no_response".
	Outcome string
	Detail  string
	Err     error
}

func (e *FeatureServerReportError) Error() string {
	msg := "feature server report not accepted: " + e.Outcome
	if e.Detail != "" {
		msg += ": " + e.Detail
	}
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

func (e *FeatureServerReportError) Unwrap() error { return e.Err }

// FeatureServerReportTransport returns the transport for this registry's
// store, or false when the store is neither an HTTP nor a gRPC registry
// (file, S3, ...), in which case nothing is reported.
func (r *Registry) FeatureServerReportTransport() (FeatureServerReportTransport, bool) {
	t, ok := r.registryStore.(FeatureServerReportTransport)
	return t, ok
}

// featureServerReportJSON is the REST body; field names match the registry's
// FeatureServerReport model.
type featureServerReportJSON struct {
	Contract int32                  `json:"contract"`
	Urls     []featureServerUrlJSON `json:"urls"`
	Version  string                 `json:"version"`
	Registry string                 `json:"registry"`
	Labels   map[string]string      `json:"labels"`
}

type featureServerUrlJSON struct {
	Url   string `json:"url"`
	Scope string `json:"scope"`
}

func featureServerReportToJSON(report *registryPb.FeatureServerReport) ([]byte, error) {
	body := featureServerReportJSON{
		Contract: report.GetContract(),
		Urls:     make([]featureServerUrlJSON, 0, len(report.GetUrls())),
		Version:  report.GetVersion(),
		Registry: report.GetRegistry(),
		Labels:   report.GetLabels(),
	}
	if body.Labels == nil {
		body.Labels = map[string]string{}
	}
	for _, u := range report.GetUrls() {
		body.Urls = append(body.Urls, featureServerUrlJSON{Url: u.GetUrl(), Scope: u.GetScope()})
	}
	return json.Marshal(body)
}

// maxReportErrorBody bounds how much of a rejection body is kept for the log line.
const maxReportErrorBody = 512

func (r *HttpRegistryStore) FeatureServerReportRegistry() string {
	return FeatureServerReportRegistryHttp
}

// ReportFeatureServer POSTs the report as JSON to
// <registry path>/projects/{project}/feature_servers.
func (r *HttpRegistryStore) ReportFeatureServer(ctx context.Context, report *registryPb.FeatureServerReport) error {
	body, err := featureServerReportToJSON(report)
	if err != nil {
		return &FeatureServerReportError{Retryable: false, Outcome: "encode_failed", Err: err}
	}
	endpoint := fmt.Sprintf("%s/projects/%s/feature_servers", strings.TrimRight(r.endpoint, "/"), url.PathEscape(r.project))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return &FeatureServerReportError{Retryable: false, Outcome: "bad_request", Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Client-Id", r.clientId)

	resp, err := r.client.Do(req)
	if err != nil {
		// Connection refused, DNS failure, timeout: no response.
		return &FeatureServerReportError{Retryable: true, Outcome: "no_response", Err: err}
	}
	defer resp.Body.Close()
	detail, _ := io.ReadAll(io.LimitReader(resp.Body, maxReportErrorBody))
	_, _ = io.Copy(io.Discard, resp.Body)

	return classifyHttpReportStatus(resp.StatusCode, strings.TrimSpace(string(detail)))
}

func classifyHttpReportStatus(code int, detail string) error {
	switch {
	case code >= 200 && code < 300:
		return nil
	case code >= 500:
		return &FeatureServerReportError{Retryable: true, Outcome: fmt.Sprintf("http_%d", code), Detail: detail}
	default:
		// 4xx and anything unexpected (1xx, 3xx): a rejection; retrying the
		// same body would get the same answer.
		return &FeatureServerReportError{Retryable: false, Outcome: fmt.Sprintf("http_%d", code), Detail: detail}
	}
}

func (g *GrpcRegistryStore) FeatureServerReportRegistry() string {
	return FeatureServerReportRegistryGrpc
}

// ReportFeatureServer calls RegistryServer.ReportFeatureServer on the
// connection the store already holds.
func (g *GrpcRegistryStore) ReportFeatureServer(ctx context.Context, report *registryPb.FeatureServerReport) error {
	_, err := g.client.ReportFeatureServer(ctx, &registryPb.ReportFeatureServerRequest{
		Project: g.project,
		Report:  report,
	})
	return classifyGrpcReportError(err)
}

func classifyGrpcReportError(err error) error {
	if err == nil {
		return nil
	}
	st := status.Convert(err)
	code := st.Code()
	if errors.Is(err, context.DeadlineExceeded) {
		code = codes.DeadlineExceeded
	}
	retryable := code == codes.Unavailable || code == codes.DeadlineExceeded
	return &FeatureServerReportError{Retryable: retryable, Outcome: code.String(), Detail: st.Message()}
}
