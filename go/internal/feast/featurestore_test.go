package feast

import (
	"context"
	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/feast-dev/feast/go/internal/feast/onlinestore"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	types "github.com/feast-dev/feast/go/protos/feast/types"
)

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
func TestGetRequestSources(t *testing.T) {
	config := GetRepoConfig()
	fs, _ := NewFeatureStore(&config, nil)

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

	cached_odfv := &model.OnDemandFeatureView{
		Base: model.NewBaseFeatureView("odfv1", []*core.FeatureSpecV2{
			{
				Name:      "feat1",
				ValueType: types.ValueType_INT64,
			},
		}),
		SourceFeatureViewProjections: make(map[string]*model.FeatureViewProjection),
		SourceRequestDataSources: map[string]*core.DataSource_RequestDataOptions{
			"request_source_1": {
				Schema: []*core.FeatureSpecV2{
					{
						Name:      "feat1",
						ValueType: types.ValueType_INT64,
					},
				},
			},
		},
	}
	fVList := make([]*model.OnDemandFeatureView, 0)
	fVList = append(fVList, cached_odfv)
	cachedOnDemandFVs := make(map[string]map[string]*core.OnDemandFeatureView)
	cachedOnDemandFVs["feature_repo"] = make(map[string]*core.OnDemandFeatureView)
	cachedOnDemandFVs["feature_repo"]["odfv1"] = odfv
	fs.registry.CachedOnDemandFeatureViews = cachedOnDemandFVs
	requestSources, err := fs.GetRequestSources(fVList)

	assert.Nil(t, err)
	assert.Equal(t, 1, len(requestSources))
	assert.Equal(t, types.ValueType_INT64.Enum(), requestSources["feat1"].Enum())
}
