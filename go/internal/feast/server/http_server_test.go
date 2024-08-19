package server

import (
	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestUnmarshalJSON(t *testing.T) {
	u := repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[1, 2, 3]")))
	assert.Equal(t, []int64{1, 2, 3}, u.int64Val)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[1.2, 2.3, 3.4]")))
	assert.Equal(t, []float64{1.2, 2.3, 3.4}, u.doubleVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[\"foo\", \"bar\"]")))
	assert.Equal(t, []string{"foo", "bar"}, u.stringVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[true, false, true]")))
	assert.Equal(t, []bool{true, false, true}, u.boolVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[[1, 2, 3], [4, 5, 6]]")))
	assert.Equal(t, [][]int64{{1, 2, 3}, {4, 5, 6}}, u.int64ListVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[[1.2, 2.3, 3.4], [10.2, 20.3, 30.4]]")))
	assert.Equal(t, [][]float64{{1.2, 2.3, 3.4}, {10.2, 20.3, 30.4}}, u.doubleListVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[[\"foo\", \"bar\"], [\"foo2\", \"bar2\"]]")))
	assert.Equal(t, [][]string{{"foo", "bar"}, {"foo2", "bar2"}}, u.stringListVal)

	u = repeatedValue{}
	assert.Nil(t, u.UnmarshalJSON([]byte("[[true, false, true], [false, true, false]]")))
	assert.Equal(t, [][]bool{{true, false, true}, {false, true, false}}, u.boolListVal)
}

func TestGetOnlineFeaturesWithValidRequest(t *testing.T) {
	s := NewHttpServer(nil, nil)

	config := feast.GetRepoConfig()
	s.fs, _ = feast.NewFeatureStore(&config, nil)
	jsonRequest := `{
		"features": ["fv1:feat1", "fv1:feat2"],
	"entities": {
	"join_key_1": [1, 2],
	"join_key_2": ["value1", "value2"]
	}
	}`

	fv := &core.FeatureView{
		Spec: &core.FeatureViewSpec{
			Name: "fv1",
			Features: []*core.FeatureSpecV2{
				{
					Name:      "feat1",
					ValueType: types.ValueType_INT64,
				},
				{
					Name:      "feat2",
					ValueType: types.ValueType_STRING,
				},
			},
			Ttl: &durationpb.Duration{
				Seconds: 3600, // 1 hour
				Nanos:   0,
			},
			Entities: []string{"entity1", "entity2"},
		},
	}

	entity1 := &core.Entity{
		Spec: &core.EntitySpecV2{
			Name:        "entity1",
			Project:     "feature_repo",
			ValueType:   types.ValueType_INT32,
			Description: "This is a sample entity",
			JoinKey:     "join_key_1",
			Tags: map[string]string{
				"tag1": "value1",
				"tag2": "value2",
			},
			Owner: "sample_owner",
		},
		Meta: &core.EntityMeta{
			CreatedTimestamp:     timestamppb.Now(),
			LastUpdatedTimestamp: timestamppb.Now(),
		},
	}

	entity2 := &core.Entity{
		Spec: &core.EntitySpecV2{
			Name:        "entity2",
			Project:     "feature_repo",
			ValueType:   types.ValueType_STRING,
			Description: "This is a sample entity",
			JoinKey:     "join_key_2",
			Tags: map[string]string{
				"tag1": "value1",
				"tag2": "value2",
			},
			Owner: "sample_owner",
		},
		Meta: &core.EntityMeta{
			CreatedTimestamp:     timestamppb.Now(),
			LastUpdatedTimestamp: timestamppb.Now(),
		},
	}

	cachedFVs := make(map[string]map[string]*core.FeatureView)
	cachedFVs["feature_repo"] = make(map[string]*core.FeatureView)
	cachedFVs["feature_repo"]["fv1"] = fv

	cachedEntities := make(map[string]map[string]*core.Entity)
	cachedEntities["feature_repo"] = make(map[string]*core.Entity)
	cachedEntities["feature_repo"]["entity1"] = entity1
	cachedEntities["feature_repo"]["entity2"] = entity2

	s.fs.Registry().CachedFeatureViews = cachedFVs
	s.fs.Registry().CachedEntities = cachedEntities

	req, _ := http.NewRequest("POST", "/get-online-features", strings.NewReader(jsonRequest))
	rr := httptest.NewRecorder()

	req.Header.Set("Content-Type", "application/json")
	s.getOnlineFeatures(rr, req)

	// Retrieve connection error string (as currently there's no mock for OnlineStore)
	bodyBytes, _ := io.ReadAll(rr.Body)
	bodyString := string(bodyBytes)

	// Error is only due to connection error resulting from not mocking
	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	strings.Contains(bodyString, "connection refused")
}
func TestGetOnlineFeaturesWithEmptyFeatures(t *testing.T) {
	s := NewHttpServer(nil, nil)

	config := feast.GetRepoConfig()
	s.fs, _ = feast.NewFeatureStore(&config, nil)

	jsonRequest := `{
	"entities": {
	"entity1": [1, 2, 3],
	"entity2": ["value1", "value2"]
	}
	}`

	fv := &core.FeatureView{
		Spec: &core.FeatureViewSpec{
			Name: "fv1",
			Features: []*core.FeatureSpecV2{
				{
					Name:      "feat1",
					ValueType: types.ValueType_INT64,
				},
				{
					Name:      "feat2",
					ValueType: types.ValueType_STRING,
				},
			},
			Ttl: &durationpb.Duration{
				Seconds: 3600, // 1 hour
				Nanos:   0,
			},
			Entities: []string{"entity1", "entity2"},
		},
	}

	entity1 := &core.Entity{
		Spec: &core.EntitySpecV2{
			Name:        "entity1",
			Project:     "feature_repo",
			ValueType:   types.ValueType_INT32,
			Description: "This is a sample entity",
			JoinKey:     "join_key_1",
			Tags: map[string]string{
				"tag1": "value1",
				"tag2": "value2",
			},
			Owner: "sample_owner",
		},
		Meta: &core.EntityMeta{
			CreatedTimestamp:     timestamppb.Now(),
			LastUpdatedTimestamp: timestamppb.Now(),
		},
	}

	entity2 := &core.Entity{
		Spec: &core.EntitySpecV2{
			Name:        "entity2",
			Project:     "feature_repo",
			ValueType:   types.ValueType_STRING,
			Description: "This is a sample entity",
			JoinKey:     "join_key_2",
			Tags: map[string]string{
				"tag1": "value1",
				"tag2": "value2",
			},
			Owner: "sample_owner",
		},
		Meta: &core.EntityMeta{
			CreatedTimestamp:     timestamppb.Now(),
			LastUpdatedTimestamp: timestamppb.Now(),
		},
	}

	cachedFVs := make(map[string]map[string]*core.FeatureView)
	cachedFVs["feature_repo"] = make(map[string]*core.FeatureView)
	cachedFVs["feature_repo"]["fv1"] = fv

	cachedEntities := make(map[string]map[string]*core.Entity)
	cachedEntities["feature_repo"] = make(map[string]*core.Entity)
	cachedEntities["feature_repo"]["entity1"] = entity1
	cachedEntities["feature_repo"]["entity2"] = entity2

	s.fs.Registry().CachedFeatureViews = cachedFVs
	s.fs.Registry().CachedEntities = cachedEntities

	req, _ := http.NewRequest("POST", "/get-online-features", strings.NewReader(jsonRequest))
	rr := httptest.NewRecorder()

	s.getOnlineFeatures(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestGetOnlineFeaturesWithEmptyEntities(t *testing.T) {
	s := NewHttpServer(nil, nil)

	config := feast.GetRepoConfig()
	s.fs, _ = feast.NewFeatureStore(&config, nil)

	jsonRequest := `{
		"features": ["fv1:feat1", "fv1:feat2"]
	}`

	fv := &core.FeatureView{
		Spec: &core.FeatureViewSpec{
			Name: "fv1",
			Features: []*core.FeatureSpecV2{
				{
					Name:      "feat1",
					ValueType: types.ValueType_INT64,
				},
				{
					Name:      "feat2",
					ValueType: types.ValueType_STRING,
				},
			},
			Ttl: &durationpb.Duration{
				Seconds: 3600, // 1 hour
				Nanos:   0,
			},
			Entities: []string{"entity1", "entity2"},
		},
	}

	entity1 := &core.Entity{
		Spec: &core.EntitySpecV2{
			Name:        "entity1",
			Project:     "feature_repo",
			ValueType:   types.ValueType_INT32,
			Description: "This is a sample entity",
			JoinKey:     "join_key_1",
			Tags: map[string]string{
				"tag1": "value1",
				"tag2": "value2",
			},
			Owner: "sample_owner",
		},
		Meta: &core.EntityMeta{
			CreatedTimestamp:     timestamppb.Now(),
			LastUpdatedTimestamp: timestamppb.Now(),
		},
	}

	entity2 := &core.Entity{
		Spec: &core.EntitySpecV2{
			Name:        "entity2",
			Project:     "feature_repo",
			ValueType:   types.ValueType_STRING,
			Description: "This is a sample entity",
			JoinKey:     "join_key_2",
			Tags: map[string]string{
				"tag1": "value1",
				"tag2": "value2",
			},
			Owner: "sample_owner",
		},
		Meta: &core.EntityMeta{
			CreatedTimestamp:     timestamppb.Now(),
			LastUpdatedTimestamp: timestamppb.Now(),
		},
	}

	cachedFVs := make(map[string]map[string]*core.FeatureView)
	cachedFVs["feature_repo"] = make(map[string]*core.FeatureView)
	cachedFVs["feature_repo"]["fv1"] = fv

	cachedEntities := make(map[string]map[string]*core.Entity)
	cachedEntities["feature_repo"] = make(map[string]*core.Entity)
	cachedEntities["feature_repo"]["entity1"] = entity1
	cachedEntities["feature_repo"]["entity2"] = entity2

	s.fs.Registry().CachedFeatureViews = cachedFVs
	s.fs.Registry().CachedEntities = cachedEntities

	req, _ := http.NewRequest("POST", "/get-online-features", strings.NewReader(jsonRequest))
	rr := httptest.NewRecorder()

	s.getOnlineFeatures(rr, req)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
}

func TestGetOnlineFeaturesWithValidFeatureService(t *testing.T) {
	s := NewHttpServer(nil, nil)

	config := feast.GetRepoConfig()
	s.fs, _ = feast.NewFeatureStore(&config, nil)

	jsonRequest := `{
		"feature_service": "fs1",
		"entities": {
			"join_key_1": [1, 2],
			"join_key_2": ["value1", "value2"]
		}
	}`

	fv := &core.FeatureView{
		Spec: &core.FeatureViewSpec{
			Name: "fv1",
			Features: []*core.FeatureSpecV2{
				{
					Name:      "feat1",
					ValueType: types.ValueType_INT64,
				},
				{
					Name:      "feat2",
					ValueType: types.ValueType_STRING,
				},
			},
			Ttl: &durationpb.Duration{
				Seconds: 3600, // 1 hour
				Nanos:   0,
			},
			Entities: []string{"entity1", "entity2"},
		},
	}

	entity1 := &core.Entity{
		Spec: &core.EntitySpecV2{
			Name:        "entity1",
			Project:     "feature_repo",
			ValueType:   types.ValueType_INT32,
			Description: "This is a sample entity",
			JoinKey:     "join_key_1",
			Tags: map[string]string{
				"tag1": "value1",
				"tag2": "value2",
			},
			Owner: "sample_owner",
		},
		Meta: &core.EntityMeta{
			CreatedTimestamp:     timestamppb.Now(),
			LastUpdatedTimestamp: timestamppb.Now(),
		},
	}

	entity2 := &core.Entity{
		Spec: &core.EntitySpecV2{
			Name:        "entity2",
			Project:     "feature_repo",
			ValueType:   types.ValueType_STRING,
			Description: "This is a sample entity",
			JoinKey:     "join_key_2",
			Tags: map[string]string{
				"tag1": "value1",
				"tag2": "value2",
			},
			Owner: "sample_owner",
		},
		Meta: &core.EntityMeta{
			CreatedTimestamp:     timestamppb.Now(),
			LastUpdatedTimestamp: timestamppb.Now(),
		},
	}

	featureService := &core.FeatureService{
		Spec: &core.FeatureServiceSpec{
			Name:        "fs1",
			Project:     "feature_repo",
			Description: "This is a sample feature service",
			Owner:       "sample_owner",
			Features: []*core.FeatureViewProjection{
				{
					FeatureViewName: "fv1",
					FeatureColumns: []*core.FeatureSpecV2{
						{
							Name:      "feat1",
							ValueType: types.ValueType_INT64,
						},
						{
							Name:      "feat2",
							ValueType: types.ValueType_STRING,
						},
					},
					//JoinKeyMap: map[string]string{
					//	"join_key_1": "",
					//	"join_key_2": "",
					//},
				},
			},
		},
		Meta: &core.FeatureServiceMeta{
			CreatedTimestamp:     timestamppb.Now(),
			LastUpdatedTimestamp: timestamppb.Now(),
		},
	}

	cachedFVs := make(map[string]map[string]*core.FeatureView)
	cachedFVs["feature_repo"] = make(map[string]*core.FeatureView)
	cachedFVs["feature_repo"]["fv1"] = fv

	cachedEntities := make(map[string]map[string]*core.Entity)
	cachedEntities["feature_repo"] = make(map[string]*core.Entity)
	cachedEntities["feature_repo"]["entity1"] = entity1
	cachedEntities["feature_repo"]["entity2"] = entity2

	cachedFSs := make(map[string]map[string]*core.FeatureService)
	cachedFSs["feature_repo"] = make(map[string]*core.FeatureService)
	cachedFSs["feature_repo"]["fs1"] = featureService

	s.fs.Registry().CachedFeatureViews = cachedFVs
	s.fs.Registry().CachedFeatureServices = cachedFSs
	s.fs.Registry().CachedEntities = cachedEntities

	req, _ := http.NewRequest("POST", "/get-online-features", strings.NewReader(jsonRequest))
	rr := httptest.NewRecorder()

	s.getOnlineFeatures(rr, req)

	// Retrieve connection error string (as currently there's no mock for OnlineStore)
	bodyBytes, _ := io.ReadAll(rr.Body)
	bodyString := string(bodyBytes)

	// Error is only due to connection error resulting from not mocking
	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	strings.Contains(bodyString, "connection refused")

}

func TestGetOnlineFeaturesWithInvalidFeatureService(t *testing.T) {
	s := NewHttpServer(nil, nil)

	config := feast.GetRepoConfig()
	s.fs, _ = feast.NewFeatureStore(&config, nil)

	jsonRequest := `{
		"feature_service": "invalid_fs",
		"entities": {
			"join_key_1": [1, 2],
			"join_key_2": ["value1", "value2"]
	}
	}`

	fv := &core.FeatureView{
		Spec: &core.FeatureViewSpec{
			Name: "fv1",
			Features: []*core.FeatureSpecV2{
				{
					Name:      "feat1",
					ValueType: types.ValueType_INT64,
				},
				{
					Name:      "feat2",
					ValueType: types.ValueType_STRING,
				},
			},
			Ttl: &durationpb.Duration{
				Seconds: 3600, // 1 hour
				Nanos:   0,
			},
			Entities: []string{"entity1", "entity2"},
		},
	}

	entity1 := &core.Entity{
		Spec: &core.EntitySpecV2{
			Name:        "entity1",
			Project:     "feature_repo",
			ValueType:   types.ValueType_INT32,
			Description: "This is a sample entity",
			JoinKey:     "join_key_1",
			Tags: map[string]string{
				"tag1": "value1",
				"tag2": "value2",
			},
			Owner: "sample_owner",
		},
		Meta: &core.EntityMeta{
			CreatedTimestamp:     timestamppb.Now(),
			LastUpdatedTimestamp: timestamppb.Now(),
		},
	}

	entity2 := &core.Entity{
		Spec: &core.EntitySpecV2{
			Name:        "entity2",
			Project:     "feature_repo",
			ValueType:   types.ValueType_STRING,
			Description: "This is a sample entity",
			JoinKey:     "join_key_2",
			Tags: map[string]string{
				"tag1": "value1",
				"tag2": "value2",
			},
			Owner: "sample_owner",
		},
		Meta: &core.EntityMeta{
			CreatedTimestamp:     timestamppb.Now(),
			LastUpdatedTimestamp: timestamppb.Now(),
		},
	}

	featureService := &core.FeatureService{
		Spec: &core.FeatureServiceSpec{
			Name:        "fs1",
			Project:     "feature_repo",
			Description: "This is a sample feature service",
			Owner:       "sample_owner",
			Features: []*core.FeatureViewProjection{
				{
					FeatureViewName: "fv1",
					FeatureColumns: []*core.FeatureSpecV2{
						{
							Name:      "feat1",
							ValueType: types.ValueType_INT64,
						},
						{
							Name:      "feat2",
							ValueType: types.ValueType_STRING,
						},
					},
					//JoinKeyMap: map[string]string{
					//	"join_key_1": "",
					//	"join_key_2": "",
					//},
				},
			},
		},
		Meta: &core.FeatureServiceMeta{
			CreatedTimestamp:     timestamppb.Now(),
			LastUpdatedTimestamp: timestamppb.Now(),
		},
	}

	cachedFVs := make(map[string]map[string]*core.FeatureView)
	cachedFVs["feature_repo"] = make(map[string]*core.FeatureView)
	cachedFVs["feature_repo"]["fv1"] = fv

	cachedEntities := make(map[string]map[string]*core.Entity)
	cachedEntities["feature_repo"] = make(map[string]*core.Entity)
	cachedEntities["feature_repo"]["entity1"] = entity1
	cachedEntities["feature_repo"]["entity2"] = entity2

	cachedFSs := make(map[string]map[string]*core.FeatureService)
	cachedFSs["feature_repo"] = make(map[string]*core.FeatureService)
	cachedFSs["feature_repo"]["fs1"] = featureService

	s.fs.Registry().CachedFeatureViews = cachedFVs
	s.fs.Registry().CachedFeatureServices = cachedFSs
	s.fs.Registry().CachedEntities = cachedEntities

	req, _ := http.NewRequest("POST", "/get-online-features", strings.NewReader(jsonRequest))
	rr := httptest.NewRecorder()

	s.getOnlineFeatures(rr, req)

	// Retrieve error string
	bodyBytes, _ := io.ReadAll(rr.Body)
	bodyString := string(bodyBytes)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	assert.Equal(t, strings.Contains(bodyString, "no cached feature service"), true)
}

func TestGetOnlineFeaturesWithInvalidEntities(t *testing.T) {
	s := NewHttpServer(nil, nil)

	config := feast.GetRepoConfig()
	s.fs, _ = feast.NewFeatureStore(&config, nil)

	jsonRequest := `{
		"features": ["fv1:feat1", "fv1:feat2"],
		"entities": {
			"join_key_invalid": [1, 2],
			"join_key_2": ["value1", "value2"]
		}
	}`

	fv := &core.FeatureView{
		Spec: &core.FeatureViewSpec{
			Name: "fv1",
			Features: []*core.FeatureSpecV2{
				{
					Name:      "feat1",
					ValueType: types.ValueType_INT64,
				},
				{
					Name:      "feat2",
					ValueType: types.ValueType_STRING,
				},
			},
			Ttl: &durationpb.Duration{
				Seconds: 3600, // 1 hour
				Nanos:   0,
			},
			Entities: []string{"entity1", "entity2"},
		},
	}

	entity1 := &core.Entity{
		Spec: &core.EntitySpecV2{
			Name:        "entity1",
			Project:     "feature_repo",
			ValueType:   types.ValueType_INT32,
			Description: "This is a sample entity",
			JoinKey:     "join_key_1",
			Tags: map[string]string{
				"tag1": "value1",
				"tag2": "value2",
			},
			Owner: "sample_owner",
		},
		Meta: &core.EntityMeta{
			CreatedTimestamp:     timestamppb.Now(),
			LastUpdatedTimestamp: timestamppb.Now(),
		},
	}

	entity2 := &core.Entity{
		Spec: &core.EntitySpecV2{
			Name:        "entity2",
			Project:     "feature_repo",
			ValueType:   types.ValueType_STRING,
			Description: "This is a sample entity",
			JoinKey:     "join_key_2",
			Tags: map[string]string{
				"tag1": "value1",
				"tag2": "value2",
			},
			Owner: "sample_owner",
		},
		Meta: &core.EntityMeta{
			CreatedTimestamp:     timestamppb.Now(),
			LastUpdatedTimestamp: timestamppb.Now(),
		},
	}

	cachedFVs := make(map[string]map[string]*core.FeatureView)
	cachedFVs["feature_repo"] = make(map[string]*core.FeatureView)
	cachedFVs["feature_repo"]["fv1"] = fv

	cachedEntities := make(map[string]map[string]*core.Entity)
	cachedEntities["feature_repo"] = make(map[string]*core.Entity)
	cachedEntities["feature_repo"]["entity1"] = entity1
	cachedEntities["feature_repo"]["entity2"] = entity2

	s.fs.Registry().CachedFeatureViews = cachedFVs
	s.fs.Registry().CachedEntities = cachedEntities

	req, _ := http.NewRequest("POST", "/get-online-features", strings.NewReader(jsonRequest))
	rr := httptest.NewRecorder()

	s.getOnlineFeatures(rr, req)

	// Retrieve error string
	bodyBytes, _ := io.ReadAll(rr.Body)
	bodyString := string(bodyBytes)

	assert.Equal(t, http.StatusNotFound, rr.Code)
	assert.Equal(t, strings.Contains(bodyString, "Entity with key join_key_invalid not found"), true)
}

func TestGetOnlineFeaturesWithEntities(t *testing.T) {
	s := NewHttpServer(nil, nil)

	config := feast.GetRepoConfig()
	s.fs, _ = feast.NewFeatureStore(&config, nil)

	jsonRequest := `{
		"features": ["odfv1:feat1"],
		"entities": {
			"join_key_1": [1, 2],
			"rq_field": [1, 2]
		}
	}`

	fv := &core.FeatureView{
		Spec: &core.FeatureViewSpec{
			Name: "fv1",
			Features: []*core.FeatureSpecV2{
				{
					Name:      "feat1",
					ValueType: types.ValueType_INT64,
				},
				{
					Name:      "feat2",
					ValueType: types.ValueType_STRING,
				},
			},
			Ttl: &durationpb.Duration{
				Seconds: 3600, // 1 hour
				Nanos:   0,
			},
			Entities: []string{"entity1"},
		},
	}

	odfv := &core.OnDemandFeatureView{
		Spec: &core.OnDemandFeatureViewSpec{
			Name:    "odfv1",
			Project: "feature_repo",
			Features: []*core.FeatureSpecV2{
				{
					Name:      "feat1",
					ValueType: types.ValueType_INT64,
				},
				{
					Name:      "feat2",
					ValueType: types.ValueType_INT64,
				},
			},
			Sources: map[string]*core.OnDemandSource{
				"rqsource1": {
					Source: &core.OnDemandSource_RequestDataSource{
						RequestDataSource: &core.DataSource{
							Name: "rqsource1",
							Type: core.DataSource_REQUEST_SOURCE,
							Options: &core.DataSource_RequestDataOptions_{
								RequestDataOptions: &core.DataSource_RequestDataOptions{
									Schema: []*core.FeatureSpecV2{
										{
											Name:      "rq_field",
											ValueType: types.ValueType_INT64,
										},
									},
								},
							},
						},
					},
				},
				"fv1": {
					Source: &core.OnDemandSource_FeatureView{
						FeatureView: &core.FeatureView{
							Spec: &core.FeatureViewSpec{
								Name: "fv1",
								Features: []*core.FeatureSpecV2{
									{
										Name:      "feat1",
										ValueType: types.ValueType_INT64,
									},
									{
										Name:      "feat2",
										ValueType: types.ValueType_STRING,
									},
								},
								Ttl: &durationpb.Duration{
									Seconds: 3600, // 1 hour
									Nanos:   0,
								},
								Entities: []string{"entity1"},
							},
						},
					},
				},
			},
			FeatureTransformation: &core.FeatureTransformationV2{
				Transformation: &core.FeatureTransformationV2_UserDefinedFunction{
					UserDefinedFunction: &core.UserDefinedFunctionV2{
						Name:     "Sample User Defined Function V2",
						Body:     []byte("function body"),
						BodyText: "function body text",
					},
				},
			},
		},
	}

	entity1 := &core.Entity{
		Spec: &core.EntitySpecV2{
			Name:        "entity1",
			Project:     "feature_repo",
			ValueType:   types.ValueType_INT32,
			Description: "This is a sample entity",
			JoinKey:     "join_key_1",
			Tags: map[string]string{
				"tag1": "value1",
				"tag2": "value2",
			},
			Owner: "sample_owner",
		},
		Meta: &core.EntityMeta{
			CreatedTimestamp:     timestamppb.Now(),
			LastUpdatedTimestamp: timestamppb.Now(),
		},
	}

	cachedODFVs := make(map[string]map[string]*core.OnDemandFeatureView)
	cachedODFVs["feature_repo"] = make(map[string]*core.OnDemandFeatureView)
	cachedODFVs["feature_repo"]["odfv1"] = odfv

	cachedFVs := make(map[string]map[string]*core.FeatureView)
	cachedFVs["feature_repo"] = make(map[string]*core.FeatureView)
	cachedFVs["feature_repo"]["odfv1"] = fv

	cachedEntities := make(map[string]map[string]*core.Entity)
	cachedEntities["feature_repo"] = make(map[string]*core.Entity)
	cachedEntities["feature_repo"]["entity1"] = entity1

	s.fs.Registry().CachedOnDemandFeatureViews = cachedODFVs
	s.fs.Registry().CachedFeatureViews = cachedFVs
	s.fs.Registry().CachedEntities = cachedEntities

	req, _ := http.NewRequest("POST", "/get-online-features", strings.NewReader(jsonRequest))
	rr := httptest.NewRecorder()

	s.getOnlineFeatures(rr, req)

	// Retrieve connection error string (as currently there's no mock for OnlineStore)
	bodyBytes, _ := io.ReadAll(rr.Body)
	bodyString := string(bodyBytes)

	// Error is only due to connection error resulting from not mocking
	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	assert.Equal(t, strings.Contains(bodyString, "connection refused"), true)
}

func TestGetOnlineFeaturesWithRequestContextOnly(t *testing.T) {

	s := NewHttpServer(nil, nil)

	config := feast.GetRepoConfig()
	s.fs, _ = feast.NewFeatureStore(&config, nil)

	jsonRequest := `{
		"features": ["odfv1:feat1"],
		"entities": {
			"join_key_1": [1, 2]
			},
		"request_context": {
			"rq_field": [1, 2]
		}
	}`

	fv := &core.FeatureView{
		Spec: &core.FeatureViewSpec{
			Name: "fv1",
			Features: []*core.FeatureSpecV2{
				{
					Name:      "feat1",
					ValueType: types.ValueType_INT64,
				},
				{
					Name:      "feat2",
					ValueType: types.ValueType_STRING,
				},
			},
			Ttl: &durationpb.Duration{
				Seconds: 3600, // 1 hour
				Nanos:   0,
			},
			Entities: []string{"entity1"},
		},
	}

	odfv := &core.OnDemandFeatureView{
		Spec: &core.OnDemandFeatureViewSpec{
			Name:    "odfv1",
			Project: "feature_repo",
			Features: []*core.FeatureSpecV2{
				{
					Name:      "feat1",
					ValueType: types.ValueType_INT64,
				},
				{
					Name:      "feat2",
					ValueType: types.ValueType_INT64,
				},
			},
			Sources: map[string]*core.OnDemandSource{
				"rqsource1": {
					Source: &core.OnDemandSource_RequestDataSource{
						RequestDataSource: &core.DataSource{
							Name: "rqsource1",
							Type: core.DataSource_REQUEST_SOURCE,
							Options: &core.DataSource_RequestDataOptions_{
								RequestDataOptions: &core.DataSource_RequestDataOptions{
									Schema: []*core.FeatureSpecV2{
										{
											Name:      "rq_field",
											ValueType: types.ValueType_INT64,
										},
									},
								},
							},
						},
					},
				},
				"fv1": {
					Source: &core.OnDemandSource_FeatureView{
						FeatureView: &core.FeatureView{
							Spec: &core.FeatureViewSpec{
								Name: "fv1",
								Features: []*core.FeatureSpecV2{
									{
										Name:      "feat1",
										ValueType: types.ValueType_INT64,
									},
									{
										Name:      "feat2",
										ValueType: types.ValueType_STRING,
									},
								},
								Ttl: &durationpb.Duration{
									Seconds: 3600, // 1 hour
									Nanos:   0,
								},
								Entities: []string{"entity1"},
							},
						},
					},
				},
			},
			FeatureTransformation: &core.FeatureTransformationV2{
				Transformation: &core.FeatureTransformationV2_UserDefinedFunction{
					UserDefinedFunction: &core.UserDefinedFunctionV2{
						Name:     "Sample User Defined Function V2",
						Body:     []byte("function body"),
						BodyText: "function body text",
					},
				},
			},
		},
	}

	entity1 := &core.Entity{
		Spec: &core.EntitySpecV2{
			Name:        "entity1",
			Project:     "feature_repo",
			ValueType:   types.ValueType_INT32,
			Description: "This is a sample entity",
			JoinKey:     "join_key_1",
			Tags: map[string]string{
				"tag1": "value1",
				"tag2": "value2",
			},
			Owner: "sample_owner",
		},
		Meta: &core.EntityMeta{
			CreatedTimestamp:     timestamppb.Now(),
			LastUpdatedTimestamp: timestamppb.Now(),
		},
	}

	cachedODFVs := make(map[string]map[string]*core.OnDemandFeatureView)
	cachedODFVs["feature_repo"] = make(map[string]*core.OnDemandFeatureView)
	cachedODFVs["feature_repo"]["odfv1"] = odfv

	cachedFVs := make(map[string]map[string]*core.FeatureView)
	cachedFVs["feature_repo"] = make(map[string]*core.FeatureView)
	cachedFVs["feature_repo"]["odfv1"] = fv

	cachedEntities := make(map[string]map[string]*core.Entity)
	cachedEntities["feature_repo"] = make(map[string]*core.Entity)
	cachedEntities["feature_repo"]["entity1"] = entity1

	s.fs.Registry().CachedOnDemandFeatureViews = cachedODFVs
	s.fs.Registry().CachedFeatureViews = cachedFVs
	s.fs.Registry().CachedEntities = cachedEntities

	req, _ := http.NewRequest("POST", "/get-online-features", strings.NewReader(jsonRequest))
	rr := httptest.NewRecorder()

	s.getOnlineFeatures(rr, req)

	// Retrieve connection error string (as currently there's no mock for OnlineStore)
	bodyBytes, _ := io.ReadAll(rr.Body)
	bodyString := string(bodyBytes)

	// Error is only due to connection error resulting from not mocking
	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	assert.Equal(t, strings.Contains(bodyString, "connection refused"), true)

}

func TestGetOnlineFeaturesWithInvalidRequestContext(t *testing.T) {

	s := NewHttpServer(nil, nil)

	config := feast.GetRepoConfig()
	s.fs, _ = feast.NewFeatureStore(&config, nil)

	jsonRequest := `{
		"features": ["odfv1:feat1"],
		"entities": {
			"join_key_1": [1, 2]
			},
		"request_context": {
			"rq_field_1": [1, 2]
		}
	}`

	fv := &core.FeatureView{
		Spec: &core.FeatureViewSpec{
			Name: "fv1",
			Features: []*core.FeatureSpecV2{
				{
					Name:      "feat1",
					ValueType: types.ValueType_INT64,
				},
				{
					Name:      "feat2",
					ValueType: types.ValueType_STRING,
				},
			},
			Ttl: &durationpb.Duration{
				Seconds: 3600, // 1 hour
				Nanos:   0,
			},
			Entities: []string{"entity1"},
		},
	}

	odfv := &core.OnDemandFeatureView{
		Spec: &core.OnDemandFeatureViewSpec{
			Name:    "odfv1",
			Project: "feature_repo",
			Features: []*core.FeatureSpecV2{
				{
					Name:      "feat1",
					ValueType: types.ValueType_INT64,
				},
				{
					Name:      "feat2",
					ValueType: types.ValueType_INT64,
				},
			},
			Sources: map[string]*core.OnDemandSource{
				"rqsource1": {
					Source: &core.OnDemandSource_RequestDataSource{
						RequestDataSource: &core.DataSource{
							Name: "rqsource1",
							Type: core.DataSource_REQUEST_SOURCE,
							Options: &core.DataSource_RequestDataOptions_{
								RequestDataOptions: &core.DataSource_RequestDataOptions{
									Schema: []*core.FeatureSpecV2{
										{
											Name:      "rq_field",
											ValueType: types.ValueType_INT64,
										},
									},
								},
							},
						},
					},
				},
				"fv1": {
					Source: &core.OnDemandSource_FeatureView{
						FeatureView: &core.FeatureView{
							Spec: &core.FeatureViewSpec{
								Name: "fv1",
								Features: []*core.FeatureSpecV2{
									{
										Name:      "feat1",
										ValueType: types.ValueType_INT64,
									},
									{
										Name:      "feat2",
										ValueType: types.ValueType_STRING,
									},
								},
								Ttl: &durationpb.Duration{
									Seconds: 3600, // 1 hour
									Nanos:   0,
								},
								Entities: []string{"entity1"},
							},
						},
					},
				},
			},
			FeatureTransformation: &core.FeatureTransformationV2{
				Transformation: &core.FeatureTransformationV2_UserDefinedFunction{
					UserDefinedFunction: &core.UserDefinedFunctionV2{
						Name:     "Sample User Defined Function V2",
						Body:     []byte("function body"),
						BodyText: "function body text",
					},
				},
			},
		},
	}

	entity1 := &core.Entity{
		Spec: &core.EntitySpecV2{
			Name:        "entity1",
			Project:     "feature_repo",
			ValueType:   types.ValueType_INT32,
			Description: "This is a sample entity",
			JoinKey:     "join_key_1",
			Tags: map[string]string{
				"tag1": "value1",
				"tag2": "value2",
			},
			Owner: "sample_owner",
		},
		Meta: &core.EntityMeta{
			CreatedTimestamp:     timestamppb.Now(),
			LastUpdatedTimestamp: timestamppb.Now(),
		},
	}

	cachedODFVs := make(map[string]map[string]*core.OnDemandFeatureView)
	cachedODFVs["feature_repo"] = make(map[string]*core.OnDemandFeatureView)
	cachedODFVs["feature_repo"]["odfv1"] = odfv

	cachedFVs := make(map[string]map[string]*core.FeatureView)
	cachedFVs["feature_repo"] = make(map[string]*core.FeatureView)
	cachedFVs["feature_repo"]["odfv1"] = fv

	cachedEntities := make(map[string]map[string]*core.Entity)
	cachedEntities["feature_repo"] = make(map[string]*core.Entity)
	cachedEntities["feature_repo"]["entity1"] = entity1

	s.fs.Registry().CachedOnDemandFeatureViews = cachedODFVs
	s.fs.Registry().CachedFeatureViews = cachedFVs
	s.fs.Registry().CachedEntities = cachedEntities

	req, _ := http.NewRequest("POST", "/get-online-features", strings.NewReader(jsonRequest))
	rr := httptest.NewRecorder()

	s.getOnlineFeatures(rr, req)

	bodyBytes, _ := io.ReadAll(rr.Body)
	bodyString := string(bodyBytes)

	assert.Equal(t, http.StatusNotFound, rr.Code)
	assert.Equal(t, strings.Contains(bodyString, "Request Source rq_field_1 not found"), true)

}
