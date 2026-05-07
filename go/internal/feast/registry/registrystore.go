package registry

import (
	"github.com/feast-dev/feast/go/protos/feast/core"
)

// A RegistryStore is a storage backend for the Feast registry.
type RegistryStore interface {
	GetRegistryProto() (*core.Registry, error)
	UpdateRegistryProto(*core.Registry) error
	Teardown() error
	HasFallback() bool
}

// FallbackRegistryStore is implemented by stores that support per-item fetching
// (HasFallback() == true). This avoids store-specific type casts in registry.go.
type FallbackRegistryStore interface {
	getEntity(name string, allowCache bool) (*core.Entity, error)
	getFeatureView(name string, allowCache bool) (*core.FeatureView, error)
	getSortedFeatureView(name string, allowCache bool) (*core.SortedFeatureView, error)
	getOnDemandFeatureView(name string, allowCache bool) (*core.OnDemandFeatureView, error)
	getFeatureService(name string, allowCache bool) (*core.FeatureService, error)
}
