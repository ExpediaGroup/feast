package registry

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/ghodss/yaml"
)

const (
	defaultCacheTtlSeconds = int64(600)
	defaultClientID        = "Unknown"
)

type RepoConfig struct {
	// Feast project name
	Project string `json:"project"`
	// Feast provider name
	Provider string `json:"provider"`
	// Path to the registry. Custom registry loaders are not yet supported
	// Registry string `json:"registry"`
	Registry interface{} `json:"registry"`
	// Online store config
	OnlineStore map[string]interface{} `json:"online_store"`
	// Offline store config
	OfflineStore map[string]interface{} `json:"offline_store"`
	// Feature server config (currently unrelated to Go server)
	FeatureServer map[string]interface{} `json:"feature_server"`
	// Feature flags for experimental features
	Flags map[string]interface{} `json:"flags"`
	// RepoPath
	RepoPath string `json:"repo_path"`
	// EntityKeySerializationVersion
	EntityKeySerializationVersion int64 `json:"entity_key_serialization_version"`
	// If false, use gopy bindings to calculate ODFV transformations.
	// "True" value required for Go feature server to serve ODFVs with stability and at scale.
	GoTransformationsServer bool `json:"go_transformations_server"`
	// Transformation server base endpoint.
	GoTransformationsEndpoint string `json:"go_transformations_endpoint"`
}

type RegistryConfig struct {
	RegistryStoreType string `json:"registry_store_type"`
	Path              string `json:"path"`
	ClientId          string `json:"client_id" default:"Unknown"`
	CacheTtlSeconds   int64  `json:"cache_ttl_seconds" default:"600"`
}

// NewRepoConfigFromJSON converts a JSON string into a RepoConfig struct and also sets the repo path.
func NewRepoConfigFromJSON(repoPath, configJSON string) (*RepoConfig, error) {
	config := RepoConfig{}
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		return nil, err
	}
	repoPath, err := filepath.Abs(repoPath)
	if err != nil {
		return nil, err
	}
	config.RepoPath = repoPath
	return &config, nil
}

// NewRepoConfigFromFile reads the `feature_store.yaml` file in the repo path and converts it
// into a RepoConfig struct.
func NewRepoConfigFromFile(repoPath string) (*RepoConfig, error) {
	data, err := ioutil.ReadFile(filepath.Join(repoPath, "feature_store.yaml"))
	if err != nil {
		return nil, err
	}
	repoPath, err = filepath.Abs(repoPath)
	if err != nil {
		return nil, err
	}

	config := RepoConfig{}
	if err = yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}
	config.RepoPath = repoPath
	return &config, nil
}

func (r *RepoConfig) GetRegistryConfig() (*RegistryConfig, error) {
	if registryConfigMap, ok := r.Registry.(map[string]interface{}); ok {
		registryConfig := RegistryConfig{CacheTtlSeconds: defaultCacheTtlSeconds, ClientId: defaultClientID}
		for k, v := range registryConfigMap {
			switch k {
			case "path":
				if value, ok := v.(string); ok {
					registryConfig.Path = value
				}
			case "registry_store_type":
				if value, ok := v.(string); ok {
					registryConfig.RegistryStoreType = value
				}
			case "client_id":
				if value, ok := v.(string); ok {
					registryConfig.ClientId = value
				}
			case "cache_ttl_seconds":
				// cache_ttl_seconds defaulted to type float64. Ex: "cache_ttl_seconds": 60 in registryConfigMap
				switch value := v.(type) {
				case float64:
					registryConfig.CacheTtlSeconds = int64(value)
				case int:
					registryConfig.CacheTtlSeconds = int64(value)
				case int32:
					registryConfig.CacheTtlSeconds = int64(value)
				case int64:
					registryConfig.CacheTtlSeconds = value
				default:
					return nil, fmt.Errorf("unexpected type %T for CacheTtlSeconds", v)
				}
			}
		}
		return &registryConfig, nil
	} else {
		return &RegistryConfig{Path: r.Registry.(string), ClientId: defaultClientID, CacheTtlSeconds: defaultCacheTtlSeconds}, nil
	}
}
