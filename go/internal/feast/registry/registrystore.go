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

type RemoteRegistryStore interface {
	RegistryStore // Add base interface for composition.

	getEntity(name string, allowCache bool) (*core.Entity, error)
	getFeatureView(name string, allowCache bool) (*core.FeatureView, error)
	getSortedFeatureView(name string, allowCache bool) (*core.SortedFeatureView, error)
	getFeatureService(name string, allowCache bool) (*core.FeatureService, error)
	getOnDemandFeatureView(name string, allowCache bool) (*core.OnDemandFeatureView, error)
}
