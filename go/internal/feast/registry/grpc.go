package registry

import (
	"context"
	"fmt"
	"time"

	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/registry"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type GrpcRegistryStore struct {
	project  string
	endpoint string
	clientId string
	conn     *grpc.ClientConn
	client   registry.RegistryServerClient
	timeout  time.Duration
}

func NewGrpcRegistryStore(config *RegistryConfig, project string) (*GrpcRegistryStore, error) {
	log.Info().Msgf("Using gRPC Registry: %s", config.Path)

	conn, err := grpc.NewClient(
		config.Path,
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC registry: %w", err)
	}

	client := registry.NewRegistryServerClient(conn)

	grs := &GrpcRegistryStore{
		project:  project,
		endpoint: config.Path,
		clientId: config.ClientId,
		conn:     conn,
		client:   client,
		timeout:  5 * time.Second,
	}

	if err := grs.TestConnectivity(); err != nil {
		conn.Close()
		return nil, err
	}

	return grs, nil
}

func (grs *GrpcRegistryStore) TestConnectivity() error {
	// May consider removing this, however added for feature parity with http_server, however no obvious healthcheck exists
	ctx, cancel := context.WithTimeout(context.Background(), grs.timeout)
	defer cancel()

	ctx = grs.withMetadata(ctx)

	req := &registry.ListEntitiesRequest{
		Project:    grs.project,
		AllowCache: false,
	}

	_, err := grs.client.ListEntities(ctx, req)
	if err != nil {
		return fmt.Errorf("gRPC registry connectivity check failed: %w", err)
	}

	return nil
}

func (grs *GrpcRegistryStore) withMetadata(ctx context.Context) context.Context {
	if grs.clientId != "" {
		md := metadata.Pairs("client-id", grs.clientId)
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	return ctx
}

func (grs *GrpcRegistryStore) GetEntity(name string, allowCache bool) (*core.Entity, error) {
	ctx, cancel := context.WithTimeout(context.Background(), grs.timeout)
	defer cancel()

	ctx = grs.withMetadata(ctx)

	req := &registry.GetEntityRequest{
		Name:       name,
		Project:    grs.project,
		AllowCache: allowCache,
	}

	entity, err := grs.client.GetEntity(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get entity %s: %w", name, err)
	}

	return entity, nil
}

func (grs *GrpcRegistryStore) GetFeatureView(name string, allowCache bool) (*core.FeatureView, error) {
	ctx, cancel := context.WithTimeout(context.Background(), grs.timeout)
	defer cancel()

	ctx = grs.withMetadata(ctx)

	req := &registry.GetFeatureViewRequest{
		Name:       name,
		Project:    grs.project,
		AllowCache: allowCache,
	}

	fv, err := grs.client.GetFeatureView(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get feature view %s: %w", name, err)
	}

	return fv, nil
}

func (grs *GrpcRegistryStore) GetSortedFeatureView(name string, allowCache bool) (*core.SortedFeatureView, error) {
	ctx, cancel := context.WithTimeout(context.Background(), grs.timeout)
	defer cancel()

	ctx = grs.withMetadata(ctx)

	req := &registry.GetSortedFeatureViewRequest{
		Name:       name,
		Project:    grs.project,
		AllowCache: allowCache,
	}

	sfv, err := grs.client.GetSortedFeatureView(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get sorted feature view %s: %w", name, err)
	}

	return sfv, nil
}

func (grs *GrpcRegistryStore) GetFeatureService(name string, allowCache bool) (*core.FeatureService, error) {
	ctx, cancel := context.WithTimeout(context.Background(), grs.timeout)
	defer cancel()

	ctx = grs.withMetadata(ctx)

	req := &registry.GetFeatureServiceRequest{
		Name:       name,
		Project:    grs.project,
		AllowCache: allowCache,
	}

	fs, err := grs.client.GetFeatureService(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get feature service %s: %w", name, err)
	}

	return fs, nil
}

func (grs *GrpcRegistryStore) GetOnDemandFeatureView(name string, allowCache bool) (*core.OnDemandFeatureView, error) {
	ctx, cancel := context.WithTimeout(context.Background(), grs.timeout)
	defer cancel()

	ctx = grs.withMetadata(ctx)

	req := &registry.GetOnDemandFeatureViewRequest{
		Name:       name,
		Project:    grs.project,
		AllowCache: allowCache,
	}

	odfv, err := grs.client.GetOnDemandFeatureView(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get on-demand feature view %s: %w", name, err)
	}

	return odfv, nil
}

func (grs *GrpcRegistryStore) GetRegistryProto() (*core.Registry, error) {
	// gRPC stores use fallback mode, so this returns empty registry
	registry := core.Registry{}
	return &registry, nil
}

func (grs *GrpcRegistryStore) UpdateRegistryProto(rp *core.Registry) error {
	return &NotImplementedError{FunctionName: "UpdateRegistryProto"}
}

func (grs *GrpcRegistryStore) Teardown() error {
	if grs.conn != nil {
		return grs.conn.Close()
	}
	return nil
}

func (grs *GrpcRegistryStore) HasFallback() bool {
	return true
}
