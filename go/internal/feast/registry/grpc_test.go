//go:build !integration

package registry

import (
	"context"
	"net"
	"testing"

	"github.com/feast-dev/feast/go/protos/feast/core"
	registryPb "github.com/feast-dev/feast/go/protos/feast/registry"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

// testRegistryServer is a minimal RegistryServer implementation for testing.
type testRegistryServer struct {
	registryPb.UnimplementedRegistryServerServer
}

func (s *testRegistryServer) GetEntity(_ context.Context, req *registryPb.GetEntityRequest) (*core.Entity, error) {
	return &core.Entity{Spec: &core.EntitySpecV2{Name: req.Name}}, nil
}

func (s *testRegistryServer) GetFeatureView(_ context.Context, req *registryPb.GetFeatureViewRequest) (*core.FeatureView, error) {
	return &core.FeatureView{Spec: &core.FeatureViewSpec{Name: req.Name}}, nil
}

func (s *testRegistryServer) GetSortedFeatureView(_ context.Context, req *registryPb.GetSortedFeatureViewRequest) (*core.SortedFeatureView, error) {
	return &core.SortedFeatureView{Spec: &core.SortedFeatureViewSpec{Name: req.Name}}, nil
}

func (s *testRegistryServer) GetOnDemandFeatureView(_ context.Context, req *registryPb.GetOnDemandFeatureViewRequest) (*core.OnDemandFeatureView, error) {
	return &core.OnDemandFeatureView{Spec: &core.OnDemandFeatureViewSpec{Name: req.Name}}, nil
}

func (s *testRegistryServer) GetFeatureService(_ context.Context, req *registryPb.GetFeatureServiceRequest) (*core.FeatureService, error) {
	return &core.FeatureService{Spec: &core.FeatureServiceSpec{Name: req.Name}}, nil
}

func (s *testRegistryServer) Proto(_ context.Context, _ *emptypb.Empty) (*core.Registry, error) {
	return &core.Registry{}, nil
}

// newTestGrpcStore spins up an in-process gRPC server using bufconn and returns
// a GrpcRegistryStore backed by it, along with a cleanup function.
func newTestGrpcStore(t *testing.T, project string) (*GrpcRegistryStore, func()) {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	registryPb.RegisterRegistryServerServer(srv, &testRegistryServer{})
	go func() {
		if err := srv.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			t.Errorf("bufconn server error: %v", err)
		}
	}()

	conn, err := grpc.NewClient(
		"passthrough:///bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatalf("failed to create grpc client: %v", err)
	}

	store := &GrpcRegistryStore{
		project: project,
		client:  registryPb.NewRegistryServerClient(conn),
		conn:    conn,
	}

	cleanup := func() {
		conn.Close()
		srv.Stop()
		listener.Close()
	}
	return store, cleanup
}

func TestGrpcGetEntity(t *testing.T) {
	store, cleanup := newTestGrpcStore(t, "test_project")
	defer cleanup()

	result, err := store.getEntity("test_entity", true)

	assert.Nil(t, err)
	assert.Equal(t, "test_entity", result.Spec.Name)
}

func TestGrpcGetFeatureView(t *testing.T) {
	store, cleanup := newTestGrpcStore(t, "test_project")
	defer cleanup()

	result, err := store.getFeatureView("test_feature_view", true)

	assert.Nil(t, err)
	assert.Equal(t, "test_feature_view", result.Spec.Name)
}

func TestGrpcGetSortedFeatureView(t *testing.T) {
	store, cleanup := newTestGrpcStore(t, "test_project")
	defer cleanup()

	result, err := store.getSortedFeatureView("test_sorted_view", true)

	assert.Nil(t, err)
	assert.Equal(t, "test_sorted_view", result.Spec.Name)
}

func TestGrpcGetOnDemandFeatureView(t *testing.T) {
	store, cleanup := newTestGrpcStore(t, "test_project")
	defer cleanup()

	result, err := store.getOnDemandFeatureView("test_odfv", true)

	assert.Nil(t, err)
	assert.Equal(t, "test_odfv", result.Spec.Name)
}

func TestGrpcGetFeatureService(t *testing.T) {
	store, cleanup := newTestGrpcStore(t, "test_project")
	defer cleanup()

	result, err := store.getFeatureService("test_feature_service", true)

	assert.Nil(t, err)
	assert.Equal(t, "test_feature_service", result.Spec.Name)
}

func TestGrpcGetRegistryProto(t *testing.T) {
	store, cleanup := newTestGrpcStore(t, "test_project")
	defer cleanup()

	registry, err := store.GetRegistryProto()

	assert.Nil(t, err)
	assert.IsType(t, &core.Registry{}, registry)
}

func TestGrpcUpdateRegistryProtoNotImplemented(t *testing.T) {
	store, cleanup := newTestGrpcStore(t, "test_project")
	defer cleanup()

	err := store.UpdateRegistryProto(&core.Registry{})

	assert.NotNil(t, err)
	assert.IsType(t, &NotImplementedError{}, err)
	assert.Equal(t, "UpdateRegistryProto", err.(*NotImplementedError).FunctionName)
}

func TestGrpcHasFallback(t *testing.T) {
	store, cleanup := newTestGrpcStore(t, "test_project")
	defer cleanup()

	assert.True(t, store.HasFallback())
}

func TestGrpcTeardown(t *testing.T) {
	store, cleanup := newTestGrpcStore(t, "test_project")
	defer cleanup()

	err := store.Teardown()

	assert.Nil(t, err)
	assert.Equal(t, connectivity.Shutdown, store.conn.GetState())
}

func TestParseGrpcTarget(t *testing.T) {
	cases := []struct {
		input    string
		target   string
		useTLS   bool
	}{
		{"grpc://host:50051", "host:50051", false},
		{"grpcs://host:50051", "host:50051", true},
		{"host:50051", "host:50051", false},
	}
	for _, c := range cases {
		target, useTLS := parseGrpcTarget(c.input)
		assert.Equal(t, c.target, target, "input: %s", c.input)
		assert.Equal(t, c.useTLS, useTLS, "input: %s", c.input)
	}
}
