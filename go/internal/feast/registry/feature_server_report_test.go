//go:build !integration

package registry

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	registryPb "github.com/feast-dev/feast/go/protos/feast/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func sampleReport(registryType string) *registryPb.FeatureServerReport {
	return &registryPb.FeatureServerReport{
		Contract: 1,
		Urls: []*registryPb.FeatureServerUrl{
			{Url: "grpcs://app-mlpfs.rcp.us-east-1.example.net:443", Scope: "external"},
			{Url: "grpc://app-mlpfs.ns.svc.cluster.local:6566", Scope: "internal"},
		},
		Version:  "mlpfs-1a2b3c4",
		Registry: registryType,
		Labels:   map[string]string{"app": "app", "instance": "mlpfs"},
	}
}

func asReportError(t *testing.T, err error) *FeatureServerReportError {
	t.Helper()
	var reportErr *FeatureServerReportError
	require.True(t, errors.As(err, &reportErr), "expected *FeatureServerReportError, got %T: %v", err, err)
	return reportErr
}

// ---- HTTP transport ----

func newReportHttpStore(endpoint string) *HttpRegistryStore {
	return &HttpRegistryStore{
		project:  "my_project",
		endpoint: endpoint,
		clientId: "my-client",
		client:   http.Client{Timeout: 5 * time.Second},
	}
}

func TestHttpReportFeatureServer_RequestShape(t *testing.T) {
	var gotMethod, gotPath, gotContentType, gotClientId string
	var gotBody map[string]interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		gotContentType, gotClientId = r.Header.Get("Content-Type"), r.Header.Get("Client-Id")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	// A trailing slash on the registry path must not produce "//projects".
	store := newReportHttpStore(server.URL + "/")
	err := store.ReportFeatureServer(context.Background(), sampleReport("http"))

	require.NoError(t, err)
	assert.Equal(t, http.MethodPost, gotMethod)
	assert.Equal(t, "/projects/my_project/feature_servers", gotPath)
	assert.Equal(t, "application/json", gotContentType)
	assert.Equal(t, "my-client", gotClientId)
	assert.Equal(t, map[string]interface{}{
		"contract": float64(1),
		"urls": []interface{}{
			map[string]interface{}{"url": "grpcs://app-mlpfs.rcp.us-east-1.example.net:443", "scope": "external"},
			map[string]interface{}{"url": "grpc://app-mlpfs.ns.svc.cluster.local:6566", "scope": "internal"},
		},
		"version":  "mlpfs-1a2b3c4",
		"registry": "http",
		"labels":   map[string]interface{}{"app": "app", "instance": "mlpfs"},
	}, gotBody)
}

func TestHttpReportFeatureServer_EmptyReportSendsEmptyCollections(t *testing.T) {
	var raw []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	err := newReportHttpStore(server.URL).ReportFeatureServer(context.Background(),
		&registryPb.FeatureServerReport{Contract: 1, Registry: "http"})
	require.NoError(t, err)
	assert.JSONEq(t, `{"contract":1,"urls":[],"version":"","registry":"http","labels":{}}`, string(raw))
}

func TestHttpReportFeatureServer_StatusClassification(t *testing.T) {
	cases := []struct {
		status    int
		wantErr   bool
		retryable bool
	}{
		{http.StatusNoContent, false, false},
		{http.StatusOK, false, false},
		{http.StatusBadRequest, true, false},
		{http.StatusUnauthorized, true, false},
		{http.StatusNotFound, true, false},
		{http.StatusRequestEntityTooLarge, true, false},
		{http.StatusUnprocessableEntity, true, false},
		{http.StatusTooManyRequests, true, false},
		{http.StatusInternalServerError, true, true},
		{http.StatusBadGateway, true, true},
		{http.StatusServiceUnavailable, true, true},
		{http.StatusGatewayTimeout, true, true},
	}
	for _, tc := range cases {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(`{"detail":"reason here"}`))
			}))
			defer server.Close()

			err := newReportHttpStore(server.URL).ReportFeatureServer(context.Background(), sampleReport("http"))
			if !tc.wantErr {
				assert.NoError(t, err)
				return
			}
			reportErr := asReportError(t, err)
			assert.Equal(t, tc.retryable, reportErr.Retryable)
			assert.Contains(t, reportErr.Outcome, "http_")
			assert.Contains(t, reportErr.Detail, "reason here")
		})
	}
}

func TestHttpReportFeatureServer_NoResponseIsRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	endpoint := server.URL
	server.Close() // nothing listens any more

	reportErr := asReportError(t, newReportHttpStore(endpoint).ReportFeatureServer(context.Background(), sampleReport("http")))
	assert.True(t, reportErr.Retryable)
	assert.Equal(t, "no_response", reportErr.Outcome)
}

func TestHttpReportFeatureServer_TimeoutIsRetryable(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer server.Close()
	defer close(release)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	reportErr := asReportError(t, newReportHttpStore(server.URL).ReportFeatureServer(ctx, sampleReport("http")))
	assert.True(t, reportErr.Retryable)
	assert.Equal(t, "no_response", reportErr.Outcome)
	assert.True(t, errors.Is(reportErr, context.DeadlineExceeded))
}

// ---- gRPC transport ----

// reportRegistryServer is a RegistryServer fake that records report requests
// and answers with a configurable error.
type reportRegistryServer struct {
	registryPb.UnimplementedRegistryServerServer
	mu       sync.Mutex
	requests []*registryPb.ReportFeatureServerRequest
	err      error
	delay    time.Duration
}

func (s *reportRegistryServer) ReportFeatureServer(ctx context.Context, req *registryPb.ReportFeatureServerRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	s.requests = append(s.requests, req)
	err, delay := s.err, s.delay
	s.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func newReportGrpcStore(t *testing.T, fake registryPb.RegistryServerServer) *GrpcRegistryStore {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	registryPb.RegisterRegistryServerServer(srv, fake)
	go func() { _ = srv.Serve(listener) }()

	conn, err := grpc.NewClient(
		"passthrough:///bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Close()
		srv.Stop()
		listener.Close()
	})
	return &GrpcRegistryStore{project: "my_project", client: registryPb.NewRegistryServerClient(conn), conn: conn}
}

func TestGrpcReportFeatureServer_OK(t *testing.T) {
	fake := &reportRegistryServer{}
	store := newReportGrpcStore(t, fake)
	report := sampleReport("grpc")

	require.NoError(t, store.ReportFeatureServer(context.Background(), report))

	require.Len(t, fake.requests, 1)
	assert.Equal(t, "my_project", fake.requests[0].GetProject())
	assert.True(t, proto.Equal(report, fake.requests[0].GetReport()))
}

func TestGrpcReportFeatureServer_Classification(t *testing.T) {
	cases := []struct {
		code      codes.Code
		retryable bool
	}{
		{codes.InvalidArgument, false},
		{codes.NotFound, false},
		{codes.ResourceExhausted, false},
		{codes.Unauthenticated, false},
		{codes.PermissionDenied, false},
		{codes.Unimplemented, false}, // registry without the RPC yet
		{codes.Internal, false},
		{codes.Unavailable, true},
		{codes.DeadlineExceeded, true},
	}
	for _, tc := range cases {
		t.Run(tc.code.String(), func(t *testing.T) {
			store := newReportGrpcStore(t, &reportRegistryServer{err: status.Error(tc.code, "reason here")})
			reportErr := asReportError(t, store.ReportFeatureServer(context.Background(), sampleReport("grpc")))
			assert.Equal(t, tc.retryable, reportErr.Retryable)
			assert.Equal(t, tc.code.String(), reportErr.Outcome)
			assert.Equal(t, "reason here", reportErr.Detail)
		})
	}
}

func TestGrpcReportFeatureServer_UnimplementedServerIsNotRetried(t *testing.T) {
	// A registry built before the RPC existed answers UNIMPLEMENTED.
	store := newReportGrpcStore(t, &registryPb.UnimplementedRegistryServerServer{})
	reportErr := asReportError(t, store.ReportFeatureServer(context.Background(), sampleReport("grpc")))
	assert.False(t, reportErr.Retryable)
	assert.Equal(t, codes.Unimplemented.String(), reportErr.Outcome)
}

func TestGrpcReportFeatureServer_ClientDeadlineIsRetryable(t *testing.T) {
	store := newReportGrpcStore(t, &reportRegistryServer{delay: time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	reportErr := asReportError(t, store.ReportFeatureServer(ctx, sampleReport("grpc")))
	assert.True(t, reportErr.Retryable)
	assert.Equal(t, codes.DeadlineExceeded.String(), reportErr.Outcome)
}

func TestClassifyGrpcReportError_PlainContextDeadline(t *testing.T) {
	reportErr := asReportError(t, classifyGrpcReportError(context.DeadlineExceeded))
	assert.True(t, reportErr.Retryable)
	assert.Equal(t, codes.DeadlineExceeded.String(), reportErr.Outcome)
}

// ---- transport selection ----

func TestRegistryFeatureServerReportTransport(t *testing.T) {
	httpTransport, ok := (&Registry{registryStore: &HttpRegistryStore{}}).FeatureServerReportTransport()
	require.True(t, ok)
	assert.Equal(t, "http", httpTransport.FeatureServerReportRegistry())

	grpcTransport, ok := (&Registry{registryStore: &GrpcRegistryStore{}}).FeatureServerReportTransport()
	require.True(t, ok)
	assert.Equal(t, "grpc", grpcTransport.FeatureServerReportRegistry())

	_, ok = (&Registry{registryStore: &FileRegistryStore{}}).FeatureServerReportTransport()
	assert.False(t, ok)
	_, ok = (&Registry{registryStore: &S3RegistryStore{}}).FeatureServerReportTransport()
	assert.False(t, ok)
}
