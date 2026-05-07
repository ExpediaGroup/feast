package registry

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/feast-dev/feast/go/protos/feast/core"
	registryPb "github.com/feast-dev/feast/go/protos/feast/registry"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type GrpcRegistryStore struct {
	project string
	client  registryPb.RegistryServerClient
	conn    *grpc.ClientConn
}

// NewGrpcRegistryStore creates a gRPC-backed registry store.
//
// TLS is enabled when any of the following are true (mirrors Python RemoteRegistryConfig):
//   - config.Path uses the "grpcs://" scheme
//   - config.IsTls is true
//   - config.Cert is set (path to a PEM certificate file)
//
// Without TLS, the connection is insecure. config.Path may be bare "host:port",
// "grpc://host:port", or "grpcs://host:port".
func NewGrpcRegistryStore(config *RegistryConfig, project string) (*GrpcRegistryStore, error) {
	target, schemeIsTLS := parseGrpcTarget(config.Path)
	useTLS := schemeIsTLS || config.IsTls || config.Cert != ""

	var dialOpts []grpc.DialOption
	if useTLS {
		tlsCreds, err := buildTLSCredentials(config.Cert)
		if err != nil {
			return nil, err
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(tlsCreds))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.NewClient(target, dialOpts...)
	if err != nil {
		return nil, err
	}

	log.Info().Msgf("Using gRPC Feature Registry: %s", config.Path)
	return &GrpcRegistryStore{
		project: project,
		client:  registryPb.NewRegistryServerClient(conn),
		conn:    conn,
	}, nil
}

// buildTLSCredentials returns TLS transport credentials. If certPath is non-empty
// the PEM file is loaded as a custom root CA (for self-signed certs), otherwise
// the system certificate pool is used.
func buildTLSCredentials(certPath string) (credentials.TransportCredentials, error) {
	if certPath == "" {
		return credentials.NewTLS(&tls.Config{}), nil
	}
	pemBytes, err := os.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("grpc registry: reading cert file %q: %w", certPath, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("grpc registry: no valid PEM certificates found in %q", certPath)
	}
	return credentials.NewTLS(&tls.Config{RootCAs: pool}), nil
}

// parseGrpcTarget strips grpc:// or grpcs:// scheme prefixes and returns
// the target address along with whether TLS should be used.
func parseGrpcTarget(path string) (target string, useTLS bool) {
	if strings.HasPrefix(path, "https://") {
		return strings.TrimPrefix(path, "https://"), true
	}
	if strings.HasPrefix(path, "http://") {
		return strings.TrimPrefix(path, "http://"), false
	}
	return path, false
}

func (g *GrpcRegistryStore) getEntity(name string, allowCache bool) (*core.Entity, error) {
	return g.client.GetEntity(context.Background(), &registryPb.GetEntityRequest{
		Name:       name,
		Project:    g.project,
		AllowCache: allowCache,
	})
}

func (g *GrpcRegistryStore) getFeatureView(name string, allowCache bool) (*core.FeatureView, error) {
	return g.client.GetFeatureView(context.Background(), &registryPb.GetFeatureViewRequest{
		Name:       name,
		Project:    g.project,
		AllowCache: allowCache,
	})
}

func (g *GrpcRegistryStore) getSortedFeatureView(name string, allowCache bool) (*core.SortedFeatureView, error) {
	return g.client.GetSortedFeatureView(context.Background(), &registryPb.GetSortedFeatureViewRequest{
		Name:       name,
		Project:    g.project,
		AllowCache: allowCache,
	})
}

func (g *GrpcRegistryStore) getOnDemandFeatureView(name string, allowCache bool) (*core.OnDemandFeatureView, error) {
	return g.client.GetOnDemandFeatureView(context.Background(), &registryPb.GetOnDemandFeatureViewRequest{
		Name:       name,
		Project:    g.project,
		AllowCache: allowCache,
	})
}

func (g *GrpcRegistryStore) getFeatureService(name string, allowCache bool) (*core.FeatureService, error) {
	return g.client.GetFeatureService(context.Background(), &registryPb.GetFeatureServiceRequest{
		Name:       name,
		Project:    g.project,
		AllowCache: allowCache,
	})
}

func (g *GrpcRegistryStore) GetRegistryProto() (*core.Registry, error) {
	return g.client.Proto(context.Background(), &emptypb.Empty{})
}

func (g *GrpcRegistryStore) UpdateRegistryProto(rp *core.Registry) error {
	return &NotImplementedError{FunctionName: "UpdateRegistryProto"}
}

func (g *GrpcRegistryStore) Teardown() error {
	return g.conn.Close()
}

func (g *GrpcRegistryStore) HasFallback() bool {
	return true
}
