package feast

import (
	"context"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/feast-dev/feast/go/internal/feast/onlinestore"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	types "github.com/feast-dev/feast/go/protos/feast/types"
)

// Return absolute path to the test_repo registry regardless of the working directory
func getRegistryPath() map[string]interface{} {
	// Get the file path of this source file, regardless of the working directory
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		panic("couldn't find file path of the test file")
	}
	registry := map[string]interface{}{
		"path": filepath.Join(filename, "..", "..", "..", "feature_repo/data/registry.db"),
	}
	return registry
}

func TestNewFeatureStore(t *testing.T) {
	t.Skip("@todo(achals): feature_repo isn't checked in yet")
	config := registry.RepoConfig{
		Project:  "feature_repo",
		Registry: getRegistryPath(),
		Provider: "local",
		OnlineStore: map[string]interface{}{
			"type": "redis",
		},
	}
	fs, err := NewFeatureStore(&config, nil)
	assert.Nil(t, err)
	assert.IsType(t, &onlinestore.RedisOnlineStore{}, fs.onlineStore)
}

func TestGetOnlineFeaturesRedis(t *testing.T) {
	t.Skip("@todo(achals): feature_repo isn't checked in yet")
	config := registry.RepoConfig{
		Project:  "feature_repo",
		Registry: getRegistryPath(),
		Provider: "local",
		OnlineStore: map[string]interface{}{
			"type":              "redis",
			"connection_string": "localhost:6379",
		},
	}

	featureNames := []string{"driver_hourly_stats:conv_rate",
		"driver_hourly_stats:acc_rate",
		"driver_hourly_stats:avg_daily_trips",
	}
	entities := map[string]*types.RepeatedValue{"driver_id": {Val: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1001}},
		{Val: &types.Value_Int64Val{Int64Val: 1002}},
		{Val: &types.Value_Int64Val{Int64Val: 1003}}}},
	}

	fs, err := NewFeatureStore(&config, nil)
	assert.Nil(t, err)
	ctx := context.Background()
	response, err := fs.GetOnlineFeatures(
		ctx, featureNames, nil, entities, map[string]*types.RepeatedValue{}, true)
	assert.Nil(t, err)
	assert.Len(t, response, 4) // 3 Features + 1 entity = 4 columns (feature vectors) in response
}

func getRepoConfig() (config registry.RepoConfig) {
	return registry.RepoConfig{
		Project:  "feature_repo",
		Registry: getRegistryPath(),
		Provider: "local",
		OnlineStore: map[string]interface{}{
			"type":              "redis",
			"connection_string": "localhost:6379",
		},
	}
}
func TestGetEntityKeyTypeMapsReturnsExpectedResult(t *testing.T) {

	config := getRepoConfig()
	fs, _ := NewFeatureStore(&config, nil)
	entity1 := &core.Entity{
		Spec: &core.EntitySpecV2{
			Name:      "entity1",
			JoinKey:   "joinKey1",
			ValueType: types.ValueType_INT64,
		},
	}
	entity2 := &core.Entity{
		Spec: &core.EntitySpecV2{
			Name:      "entity2",
			JoinKey:   "joinKey2",
			ValueType: types.ValueType_INT32,
		},
	}
	cachedEntities := make(map[string]map[string]*core.Entity)
	cachedEntities["feature_repo"] = make(map[string]*core.Entity)
	cachedEntities["feature_repo"]["entity1"] = entity1
	cachedEntities["feature_repo"]["entity2"] = entity2

	fs.registry.CachedEntities = cachedEntities

	entityKeyTypeMap, err := fs.GetEntityKeyTypeMaps()

	assert.Nil(t, err)
	assert.Equal(t, 2, len(entityKeyTypeMap))
	assert.Equal(t, types.ValueType_INT64, entityKeyTypeMap["joinKey1"])
	assert.Equal(t, types.ValueType_INT32, entityKeyTypeMap["joinKey2"])
}

func TestGetEntityKeyTypeMapsReturnsErrorWhenNoEntities(t *testing.T) {

	config := getRepoConfig()
	fs, _ := NewFeatureStore(&config, nil)

	cachedEntities := make(map[string]map[string]*core.Entity)
	fs.registry.CachedEntities = cachedEntities

	entityKeyTypeMap, err := fs.GetEntityKeyTypeMaps()

	assert.NotNil(t, err)
	assert.Equal(t, 0, len(entityKeyTypeMap))
}
func TestGetRequestSourcesWithValidFeatures(t *testing.T) {
	config := getRepoConfig()
	fs, _ := NewFeatureStore(&config, nil)
	fVList := []string{"odfv1", "fv1"}

	odfv := &core.OnDemandFeatureView{
		Spec: &core.OnDemandFeatureViewSpec{
			Name:    "odfv1",
			Project: "feature_repo",
			Sources: map[string]*core.OnDemandSource{
				"odfv1": {
					Source: &core.OnDemandSource_RequestDataSource{
						RequestDataSource: &core.DataSource{
							Name: "request_source_1",
							Type: core.DataSource_REQUEST_SOURCE,
							Options: &core.DataSource_RequestDataOptions_{
								RequestDataOptions: &core.DataSource_RequestDataOptions{
									DeprecatedSchema: map[string]types.ValueType_Enum{
										"feature1": types.ValueType_INT64,
									},
									Schema: []*core.FeatureSpecV2{
										{
											Name:      "feat1",
											ValueType: types.ValueType_INT64,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	cachedOnDemandFVs := make(map[string]map[string]*core.OnDemandFeatureView)
	cachedOnDemandFVs["feature_repo"] = make(map[string]*core.OnDemandFeatureView)
	cachedOnDemandFVs["feature_repo"]["odfv1"] = odfv
	fs.registry.CachedOnDemandFeatureViews = cachedOnDemandFVs
	requestSources, err := fs.GetRequestSources(fVList)

	assert.Nil(t, err)
	assert.Equal(t, 1, len(requestSources))
	assert.Equal(t, types.ValueType_INT64.Enum(), requestSources["feat1"].Enum())
}

func TestGetRequestSourcesWithInvalidFeatures(t *testing.T) {

	config := getRepoConfig()
	fs, _ := NewFeatureStore(&config, nil)
	fVList := []string{"invalidFV", "fv1"}

	odfv := &core.OnDemandFeatureView{
		Spec: &core.OnDemandFeatureViewSpec{
			Name:    "odfv1",
			Project: "feature_repo",
			Sources: map[string]*core.OnDemandSource{
				"odfv1": {
					Source: &core.OnDemandSource_RequestDataSource{
						RequestDataSource: &core.DataSource{
							Name: "request_source_1",
							Type: core.DataSource_REQUEST_SOURCE,
							Options: &core.DataSource_RequestDataOptions_{
								RequestDataOptions: &core.DataSource_RequestDataOptions{
									DeprecatedSchema: map[string]types.ValueType_Enum{
										"feature1": types.ValueType_INT64,
									},
									Schema: []*core.FeatureSpecV2{
										{
											Name:      "feature1",
											ValueType: types.ValueType_INT64,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	cachedOnDemandFVs := make(map[string]map[string]*core.OnDemandFeatureView)
	cachedOnDemandFVs["feature_repo"] = make(map[string]*core.OnDemandFeatureView)
	cachedOnDemandFVs["feature_repo"]["odfv1"] = odfv
	fs.registry.CachedOnDemandFeatureViews = cachedOnDemandFVs

	requestSources, err := fs.GetRequestSources(fVList)

	assert.NotNil(t, err)
	assert.Equal(t, 0, len(requestSources))
}

func TestGetRequestSourcesWithNoFeatures(t *testing.T) {

	config := getRepoConfig()
	fs, _ := NewFeatureStore(&config, nil)
	var fvList []string

	requestSources, err := fs.GetRequestSources(fvList)

	assert.NotNil(t, err)
	assert.Equal(t, 0, len(requestSources))
}
