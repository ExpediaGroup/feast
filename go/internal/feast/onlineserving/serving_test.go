//go:build !integration

package onlineserving

import (
	"fmt"
	"github.com/apache/arrow/go/v17/arrow"
	types2 "github.com/feast-dev/feast/go/types"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/feast-dev/feast/go/internal/feast/onlinestore"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/internal/test"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/types"
)

func TestGroupingFeatureRefsByFeatureView(t *testing.T) {
	viewA := &model.FeatureView{
		Base: &model.BaseFeatureView{
			Name: "viewA",
			Projection: &model.FeatureViewProjection{
				NameAlias: "aliasViewA",
			},
		},
		EntityNames: []string{"driver", "customer"},
	}
	viewB := &model.FeatureView{
		Base:        &model.BaseFeatureView{Name: "viewB"},
		EntityNames: []string{"driver", "customer"},
	}
	viewC := &model.FeatureView{
		Base:        &model.BaseFeatureView{Name: "viewC"},
		EntityNames: []string{"driver"},
	}
	viewD := &model.FeatureView{
		Base:        &model.BaseFeatureView{Name: "viewD"},
		EntityNames: []string{"customer"},
	}
	refGroups, _ := GroupFeatureRefsByFeatureView(
		[]*FeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureC", "featureD"}},
			{View: viewC, FeatureRefs: []string{"featureE"}},
			{View: viewD, FeatureRefs: []string{"featureF"}},
		},
		map[string]*types.RepeatedValue{
			"driver_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
			}},
			"customer_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 2}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 4}},
			}},
		},
		map[string]string{
			"driver":   "driver_id",
			"customer": "customer_id",
		},
		true,
	)

	assert.Len(t, refGroups, 4)

}

func TestGroupingFeatureRefs(t *testing.T) {
	viewA := &model.FeatureView{
		Base: &model.BaseFeatureView{
			Name: "viewA",
			Projection: &model.FeatureViewProjection{
				NameAlias: "aliasViewA",
			},
		},
		EntityNames: []string{"driver", "customer"},
	}
	viewB := &model.FeatureView{
		Base:        &model.BaseFeatureView{Name: "viewB"},
		EntityNames: []string{"driver", "customer"},
	}
	viewC := &model.FeatureView{
		Base:        &model.BaseFeatureView{Name: "viewC"},
		EntityNames: []string{"driver"},
	}
	viewD := &model.FeatureView{
		Base:        &model.BaseFeatureView{Name: "viewD"},
		EntityNames: []string{"customer"},
	}
	refGroups, _ := GroupFeatureRefs(
		[]*FeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureC", "featureD"}},
			{View: viewC, FeatureRefs: []string{"featureE"}},
			{View: viewD, FeatureRefs: []string{"featureF"}},
		},
		map[string]*types.RepeatedValue{
			"driver_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
			}},
			"customer_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 2}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 4}},
			}},
		},
		map[string]string{
			"driver":   "driver_id",
			"customer": "customer_id",
		},
		true,
	)

	assert.Len(t, refGroups, 3)

	// Group 1
	assert.Equal(t, []string{"featureA", "featureB", "featureC", "featureD"},
		refGroups["customer_id,driver_id"].FeatureNames)
	assert.Equal(t, []string{"viewA", "viewA", "viewB", "viewB"},
		refGroups["customer_id,driver_id"].FeatureViewNames)
	assert.Equal(t, []string{
		"aliasViewA__featureA", "aliasViewA__featureB",
		"viewB__featureC", "viewB__featureD"},
		refGroups["customer_id,driver_id"].AliasedFeatureNames)
	for _, group := range [][]int{{0}, {1}, {2, 3}, {4}} {
		assert.Contains(t, refGroups["customer_id,driver_id"].Indices, group)
	}

	// Group2
	assert.Equal(t, []string{"featureE"},
		refGroups["driver_id"].FeatureNames)
	for _, group := range [][]int{{0, 1}, {2, 3, 4}} {
		assert.Contains(t, refGroups["driver_id"].Indices, group)
	}

	// Group3
	assert.Equal(t, []string{"featureF"},
		refGroups["customer_id"].FeatureNames)

	for _, group := range [][]int{{0}, {1}, {2, 3}, {4}} {
		assert.Contains(t, refGroups["customer_id"].Indices, group)
	}

}

func TestGroupingFeatureRefsWithJoinKeyAliases(t *testing.T) {
	viewA := &model.FeatureView{
		Base: &model.BaseFeatureView{
			Name: "viewA",
			Projection: &model.FeatureViewProjection{
				Name:       "viewA",
				JoinKeyMap: map[string]string{"location_id": "destination_id"},
			},
		},
		EntityNames: []string{"location"},
	}
	viewB := &model.FeatureView{
		Base:        &model.BaseFeatureView{Name: "viewB"},
		EntityNames: []string{"location"},
	}

	refGroups, _ := GroupFeatureRefs(
		[]*FeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureC", "featureD"}},
		},
		map[string]*types.RepeatedValue{
			"location_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
			}},
			"destination_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 2}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 4}},
			}},
		},
		map[string]string{
			"location": "location_id",
		},
		true,
	)

	assert.Len(t, refGroups, 2)

	assert.Equal(t, []string{"featureA", "featureB"},
		refGroups["location_id[destination_id]"].FeatureNames)
	for _, group := range [][]int{{0}, {1}, {2, 3}, {4}} {
		assert.Contains(t, refGroups["location_id[destination_id]"].Indices, group)
	}

	assert.Equal(t, []string{"featureC", "featureD"},
		refGroups["location_id"].FeatureNames)
	for _, group := range [][]int{{0, 1}, {2, 3, 4}} {
		assert.Contains(t, refGroups["location_id"].Indices, group)
	}

}

func TestGroupingFeatureRefsWithMissingKey(t *testing.T) {
	viewA := &model.FeatureView{
		Base: &model.BaseFeatureView{
			Name: "viewA",
			Projection: &model.FeatureViewProjection{
				Name:       "viewA",
				JoinKeyMap: map[string]string{"location_id": "destination_id"},
			},
		},
		EntityNames: []string{"location"},
	}

	_, err := GroupFeatureRefs(
		[]*FeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
		},
		map[string]*types.RepeatedValue{
			"location_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 0}},
			}},
		},
		map[string]string{
			"location": "location_id",
		},
		true,
	)
	assert.Errorf(t, err, "key destination_id is missing in provided entity rows for view viewA")
}

func TestGroupingFeatureRefsV2WithEntityDataModel(t *testing.T) {
	viewA := &model.FeatureView{
		Base: &model.BaseFeatureView{
			Name: "viewA",
			Projection: &model.FeatureViewProjection{
				NameAlias: "aliasViewA",
			},
		},
		EntityNames: []string{"driver", "customer"},
	}
	viewB := &model.FeatureView{
		Base:        &model.BaseFeatureView{Name: "viewB"},
		EntityNames: []string{"driver", "customer"},
	}
	viewC := &model.FeatureView{
		Base:        &model.BaseFeatureView{Name: "viewC"},
		EntityNames: []string{"driver"},
	}
	viewD := &model.FeatureView{
		Base:        &model.BaseFeatureView{Name: "viewD"},
		EntityNames: []string{"customer"},
	}
	refGroups, _ := GroupFeatureRefsV2(
		[]*FeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureC", "featureD"}},
			{View: viewC, FeatureRefs: []string{"featureE"}},
			{View: viewD, FeatureRefs: []string{"featureF"}},
		},
		map[string]*types.RepeatedValue{
			"driver_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
			}},
			"customer_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 2}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 4}},
			}},
		},
		map[string]string{
			"driver":   "driver_id",
			"customer": "customer_id",
		},
		true,
		onlinestore.EntityLevel,
	)

	expectedKeys := make([]*GroupedFeaturesPerEntitySet, 3)

	expectedKeys[0] = &GroupedFeaturesPerEntitySet{
		FeatureNames:        []string{"featureA", "featureB", "featureC", "featureD"},
		FeatureViewNames:    []string{"viewA", "viewA", "viewB", "viewB"},
		AliasedFeatureNames: []string{"aliasViewA__featureA", "aliasViewA__featureB", "viewB__featureC", "viewB__featureD"},
		Indices:             [][]int{{0}, {1}, {2, 3}, {4}},
		EntityKeys: []*types.EntityKey{
			{
				JoinKeys:     []string{"customer_id", "driver_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 1}}, {Val: &types.Value_Int32Val{Int32Val: 0}}},
			},
			{
				JoinKeys:     []string{"customer_id", "driver_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 2}}, {Val: &types.Value_Int32Val{Int32Val: 0}}},
			},
			{
				JoinKeys:     []string{"customer_id", "driver_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 3}}, {Val: &types.Value_Int32Val{Int32Val: 1}}},
			},
			{
				JoinKeys:     []string{"customer_id", "driver_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 4}}, {Val: &types.Value_Int32Val{Int32Val: 1}}},
			},
		},
	}
	expectedKeys[1] = &GroupedFeaturesPerEntitySet{
		FeatureNames:        []string{"featureE"},
		FeatureViewNames:    []string{"viewC"},
		AliasedFeatureNames: []string{"viewC__featureE"},
		Indices:             [][]int{{0, 1}, {2, 3, 4}},
		EntityKeys: []*types.EntityKey{
			{
				JoinKeys:     []string{"driver_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 0}}},
			},
			{
				JoinKeys:     []string{"driver_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 1}}},
			},
		},
	}

	expectedKeys[2] = &GroupedFeaturesPerEntitySet{
		FeatureNames:        []string{"featureF"},
		FeatureViewNames:    []string{"viewD"},
		AliasedFeatureNames: []string{"viewD__featureF"},
		Indices:             [][]int{{0}, {1}, {2, 3}, {4}},
		EntityKeys: []*types.EntityKey{
			{
				JoinKeys:     []string{"customer_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 1}}},
			},
			{
				JoinKeys:     []string{"customer_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 2}}},
			},
			{
				JoinKeys:     []string{"customer_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 3}}},
			},
			{
				JoinKeys:     []string{"customer_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 4}}},
			},
		},
	}

	assert.Len(t, refGroups, 3)

	assert.True(t, compareGroupedFeatures(refGroups, expectedKeys))
}

func TestGroupingFeatureRefsV2WithFeatureViewDataModel(t *testing.T) {
	viewA := &model.FeatureView{
		Base: &model.BaseFeatureView{
			Name: "viewA",
			Projection: &model.FeatureViewProjection{
				NameAlias: "aliasViewA",
			},
		},
		EntityNames: []string{"driver", "customer"},
	}
	viewB := &model.FeatureView{
		Base:        &model.BaseFeatureView{Name: "viewB"},
		EntityNames: []string{"driver", "customer"},
	}
	viewC := &model.FeatureView{
		Base:        &model.BaseFeatureView{Name: "viewC"},
		EntityNames: []string{"driver"},
	}
	viewD := &model.FeatureView{
		Base:        &model.BaseFeatureView{Name: "viewD"},
		EntityNames: []string{"customer"},
	}
	refGroups, _ := GroupFeatureRefsV2(
		[]*FeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureC", "featureD"}},
			{View: viewC, FeatureRefs: []string{"featureE"}},
			{View: viewD, FeatureRefs: []string{"featureF"}},
		},
		map[string]*types.RepeatedValue{
			"driver_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
			}},
			"customer_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 2}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 4}},
			}},
		},
		map[string]string{
			"driver":   "driver_id",
			"customer": "customer_id",
		},
		true,
		onlinestore.FeatureViewLevel,
	)

	expectedKeys := make([]*GroupedFeaturesPerEntitySet, 4)

	expectedKeys[0] = &GroupedFeaturesPerEntitySet{
		FeatureNames:        []string{"featureA", "featureB"},
		FeatureViewNames:    []string{"viewA", "viewA"},
		AliasedFeatureNames: []string{"aliasViewA__featureA", "aliasViewA__featureB"},
		Indices:             [][]int{{0}, {1}, {2, 3}, {4}},
		EntityKeys: []*types.EntityKey{
			{
				JoinKeys:     []string{"customer_id", "driver_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 1}}, {Val: &types.Value_Int32Val{Int32Val: 0}}},
			},
			{
				JoinKeys:     []string{"customer_id", "driver_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 2}}, {Val: &types.Value_Int32Val{Int32Val: 0}}},
			},
			{
				JoinKeys:     []string{"customer_id", "driver_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 3}}, {Val: &types.Value_Int32Val{Int32Val: 1}}},
			},
			{
				JoinKeys:     []string{"customer_id", "driver_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 4}}, {Val: &types.Value_Int32Val{Int32Val: 1}}},
			},
		},
	}
	expectedKeys[1] = &GroupedFeaturesPerEntitySet{
		FeatureNames:        []string{"featureC", "featureD"},
		FeatureViewNames:    []string{"viewB", "viewB"},
		AliasedFeatureNames: []string{"viewB__featureC", "viewB__featureD"},
		Indices:             [][]int{{0}, {1}, {2, 3}, {4}},
		EntityKeys: []*types.EntityKey{
			{
				JoinKeys:     []string{"customer_id", "driver_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 1}}, {Val: &types.Value_Int32Val{Int32Val: 0}}},
			},
			{
				JoinKeys:     []string{"customer_id", "driver_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 2}}, {Val: &types.Value_Int32Val{Int32Val: 0}}},
			},
			{
				JoinKeys:     []string{"customer_id", "driver_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 3}}, {Val: &types.Value_Int32Val{Int32Val: 1}}},
			},
			{
				JoinKeys:     []string{"customer_id", "driver_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 4}}, {Val: &types.Value_Int32Val{Int32Val: 1}}},
			},
		},
	}
	expectedKeys[2] = &GroupedFeaturesPerEntitySet{
		FeatureNames:        []string{"featureE"},
		FeatureViewNames:    []string{"viewC"},
		AliasedFeatureNames: []string{"viewC__featureE"},
		Indices:             [][]int{{0, 1}, {2, 3, 4}},
		EntityKeys: []*types.EntityKey{
			{
				JoinKeys:     []string{"driver_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 0}}},
			},
			{
				JoinKeys:     []string{"driver_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 1}}},
			},
		},
	}

	expectedKeys[3] = &GroupedFeaturesPerEntitySet{
		FeatureNames:        []string{"featureF"},
		FeatureViewNames:    []string{"viewD"},
		AliasedFeatureNames: []string{"viewD__featureF"},
		Indices:             [][]int{{0}, {1}, {2, 3}, {4}},
		EntityKeys: []*types.EntityKey{
			{
				JoinKeys:     []string{"customer_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 1}}},
			},
			{
				JoinKeys:     []string{"customer_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 2}}},
			},
			{
				JoinKeys:     []string{"customer_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 3}}},
			},
			{
				JoinKeys:     []string{"customer_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 4}}},
			},
		},
	}

	assert.Len(t, refGroups, 4)

	assert.True(t, compareGroupedFeatures(refGroups, expectedKeys))
}

func compareGroupedFeatures(actual []*GroupedFeaturesPerEntitySet, expected []*GroupedFeaturesPerEntitySet) bool {
	if len(actual) != len(expected) {
		return false
	}

	for _, expectedGroup := range expected {
		found := false
		for _, actualGroup := range actual {
			if slices.Equal(expectedGroup.FeatureNames, actualGroup.FeatureNames) &&
				slices.Equal(expectedGroup.FeatureViewNames, actualGroup.FeatureViewNames) &&
				slices.Equal(expectedGroup.AliasedFeatureNames, actualGroup.AliasedFeatureNames) &&
				reflect.DeepEqual(expectedGroup.Indices, actualGroup.Indices) {
				if !compareEntityKeys(actualGroup.EntityKeys, expectedGroup.EntityKeys) {
					return false
				}
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func compareEntityKeys(actual []*types.EntityKey, expected []*types.EntityKey) bool {
	if len(actual) != len(expected) {
		return false
	}
	for index, expectedKey := range expected {
		if !proto.Equal(actual[index], expectedKey) {
			return false
		}
	}
	return true
}

func TestGroupingFeatureRefsV2WithMissingKeyForFeatureViewDataModel(t *testing.T) {
	viewA := &model.FeatureView{
		Base: &model.BaseFeatureView{
			Name: "viewA",
			Projection: &model.FeatureViewProjection{
				Name:       "viewA",
				JoinKeyMap: map[string]string{"location_id": "destination_id"},
			},
		},
		EntityNames: []string{"location"},
	}

	_, err := GroupFeatureRefsV2(
		[]*FeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
		},
		map[string]*types.RepeatedValue{
			"location_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 0}},
			}},
		},
		map[string]string{
			"location": "location_id",
		},
		true,
		onlinestore.FeatureViewLevel,
	)
	assert.Errorf(t, err, "key destination_id is missing in provided entity rows for view viewA")
}

func TestGroupingFeatureRefsV2WithMissingKeyForEntityDataModel(t *testing.T) {
	viewA := &model.FeatureView{
		Base: &model.BaseFeatureView{
			Name: "viewA",
			Projection: &model.FeatureViewProjection{
				Name:       "viewA",
				JoinKeyMap: map[string]string{"location_id": "destination_id"},
			},
		},
		EntityNames: []string{"location"},
	}

	_, err := GroupFeatureRefsV2(
		[]*FeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
		},
		map[string]*types.RepeatedValue{
			"location_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 0}},
			}},
		},
		map[string]string{
			"location": "location_id",
		},
		true,
		onlinestore.EntityLevel,
	)
	assert.Errorf(t, err, "key destination_id is missing in provided entity rows for view viewA")
}

func createRegistry(project string) (*registry.Registry, error) {
	// Return absolute path to the test_repo registry regardless of the working directory
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		panic("couldn't find file path of the test file")
	}
	path := filepath.Join(filename, "..", "..", "..", "feature_repo/data/registry.db")
	r, err := registry.NewRegistry(&registry.RegistryConfig{Path: path}, path, project)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func TestUnpackFeatureService(t *testing.T) {
	projectName := "test_project"
	testRegistry, err := createRegistry(projectName)
	assert.NoError(t, err)

	featASpec := test.CreateFeature("featA", types.ValueType_INT32)
	featBSpec := test.CreateFeature("featB", types.ValueType_INT32)
	featCSpec := test.CreateFeature("featC", types.ValueType_INT32)
	featDSpec := test.CreateFeature("featD", types.ValueType_INT32)
	featESpec := test.CreateFeature("featE", types.ValueType_FLOAT)
	onDemandFeature1 := test.CreateFeature("featF", types.ValueType_FLOAT)
	onDemandFeature2 := test.CreateFeature("featG", types.ValueType_FLOAT)
	featSSpec := test.CreateFeature("featS", types.ValueType_FLOAT)
	sortKeyA := test.CreateSortKeyProto("featS", core.SortOrder_DESC, types.ValueType_FLOAT)

	entities := []*core.Entity{test.CreateEntityProto("entity", types.ValueType_INT32, "entity")}
	viewA := test.CreateFeatureViewProto("viewA", entities, featASpec, featBSpec)
	viewB := test.CreateFeatureViewProto("viewB", entities, featCSpec, featDSpec)
	viewC := test.CreateFeatureViewProto("viewC", entities, featESpec)
	viewS := test.CreateSortedFeatureViewProto("viewS", entities, []*core.SortKey{sortKeyA}, featSSpec)
	onDemandView := test.CreateOnDemandFeatureViewProto(
		"odfv",
		map[string][]*core.FeatureSpecV2{"viewB": {featCSpec}, "viewC": {featESpec}},
		onDemandFeature1, onDemandFeature2)

	fs := test.CreateFeatureService("service", map[string][]*core.FeatureSpecV2{
		"viewA": {featASpec, featBSpec},
		"viewB": {featCSpec},
		"odfv":  {onDemandFeature2},
	})
	testRegistry.SetModels([]*core.FeatureService{}, []*core.Entity{}, []*core.FeatureView{viewA, viewB, viewC}, []*core.SortedFeatureView{viewS}, []*core.OnDemandFeatureView{onDemandView})

	fvs, odfvs, err := GetFeatureViewsToUseByService(fs, testRegistry, projectName)

	assertCorrectUnpacking(t, fvs, odfvs, err)
}

func assertCorrectUnpacking(t *testing.T, fvs []*FeatureViewAndRefs, odfvs []*model.OnDemandFeatureView, err error) {
	assert.Nil(t, err)
	assert.Len(t, fvs, 3)
	assert.Len(t, odfvs, 1)

	fvsByName := make(map[string]*FeatureViewAndRefs)
	for _, fv := range fvs {
		fvsByName[fv.View.Base.Name] = fv
	}

	// feature views and features as declared in service
	assert.Equal(t, []string{"featA", "featB"}, fvsByName["viewA"].FeatureRefs)
	assert.Equal(t, []string{"featC"}, fvsByName["viewB"].FeatureRefs)

	// dependency of the on demand feature view
	assert.Equal(t, []string{"featE"}, fvsByName["viewC"].FeatureRefs)

	// only requested features projected
	assert.Len(t, odfvs[0].Base.Projection.Features, 1)
	assert.Equal(t, "featG", odfvs[0].Base.Projection.Features[0].Name)
}

func TestUnpackFeatureViewsByReferences(t *testing.T) {
	projectName := "test_project"
	testRegistry, err := createRegistry(projectName)
	assert.NoError(t, err)

	featASpec := test.CreateFeature("featA", types.ValueType_INT32)
	featBSpec := test.CreateFeature("featB", types.ValueType_INT32)
	featCSpec := test.CreateFeature("featC", types.ValueType_INT32)
	featDSpec := test.CreateFeature("featD", types.ValueType_INT32)
	featESpec := test.CreateFeature("featE", types.ValueType_FLOAT)
	onDemandFeature1 := test.CreateFeature("featF", types.ValueType_FLOAT)
	onDemandFeature2 := test.CreateFeature("featG", types.ValueType_FLOAT)
	featSSpec := test.CreateFeature("featS", types.ValueType_FLOAT)
	sortKeyA := test.CreateSortKeyProto("featS", core.SortOrder_DESC, types.ValueType_FLOAT)

	entities := []*core.Entity{test.CreateEntityProto("entity", types.ValueType_INT32, "entity")}
	viewA := test.CreateFeatureViewProto("viewA", entities, featASpec, featBSpec)
	viewB := test.CreateFeatureViewProto("viewB", entities, featCSpec, featDSpec)
	viewC := test.CreateFeatureViewProto("viewC", entities, featESpec)
	viewS := test.CreateSortedFeatureViewProto("viewS", entities, []*core.SortKey{sortKeyA}, featSSpec)
	onDemandView := test.CreateOnDemandFeatureViewProto(
		"odfv",
		map[string][]*core.FeatureSpecV2{"viewB": {featCSpec}, "viewC": {featESpec}},
		onDemandFeature1, onDemandFeature2)
	testRegistry.SetModels([]*core.FeatureService{}, []*core.Entity{}, []*core.FeatureView{viewA, viewB, viewC}, []*core.SortedFeatureView{viewS}, []*core.OnDemandFeatureView{onDemandView})

	fvs, odfvs, err := GetFeatureViewsToUseByFeatureRefs(
		[]string{
			"viewA:featA",
			"viewA:featB",
			"viewB:featC",
			"odfv:featG",
		},
		testRegistry, projectName)

	assertCorrectUnpacking(t, fvs, odfvs, err)
}

func TestGetFeatureViewsToUseByService_returnsErrorWithInvalidFeatures(t *testing.T) {
	projectName := "test_project"
	testRegistry, err := createRegistry(projectName)
	assert.NoError(t, err)

	featASpec := test.CreateFeature("featA", types.ValueType_INT32)
	featBSpec := test.CreateFeature("featB", types.ValueType_INT32)
	featCSpec := test.CreateFeature("featC", types.ValueType_INT32)
	featDSpec := test.CreateFeature("featD", types.ValueType_INT32)
	featESpec := test.CreateFeature("featE", types.ValueType_FLOAT)
	onDemandFeature1 := test.CreateFeature("featF", types.ValueType_FLOAT)
	onDemandFeature2 := test.CreateFeature("featG", types.ValueType_FLOAT)
	featSSpec := test.CreateFeature("featS", types.ValueType_FLOAT)
	sortKeyA := test.CreateSortKeyProto("featS", core.SortOrder_DESC, types.ValueType_FLOAT)

	entities := []*core.Entity{test.CreateEntityProto("entity", types.ValueType_INT32, "entity")}
	viewA := test.CreateFeatureViewProto("viewA", entities, featASpec, featBSpec)
	viewB := test.CreateFeatureViewProto("viewB", entities, featCSpec, featDSpec)
	viewC := test.CreateFeatureViewProto("viewC", entities, featESpec)
	viewS := test.CreateSortedFeatureViewProto("viewS", entities, []*core.SortKey{sortKeyA}, featSSpec)
	onDemandView := test.CreateOnDemandFeatureViewProto(
		"odfv",
		map[string][]*core.FeatureSpecV2{"viewB": {featCSpec}, "viewC": {featESpec}},
		onDemandFeature1, onDemandFeature2)

	featInvalidSpec := test.CreateFeature("featInvalid", types.ValueType_INT32)
	fs := test.CreateFeatureService("service", map[string][]*core.FeatureSpecV2{
		"viewA": {featASpec, featBSpec},
		"viewB": {featCSpec, featInvalidSpec},
		"odfv":  {onDemandFeature2},
	})
	testRegistry.SetModels([]*core.FeatureService{}, []*core.Entity{}, []*core.FeatureView{viewA, viewB, viewC}, []*core.SortedFeatureView{viewS}, []*core.OnDemandFeatureView{onDemandView})

	_, _, invalidFeaturesErr := GetFeatureViewsToUseByService(fs, testRegistry, projectName)
	assert.EqualError(t, invalidFeaturesErr, "rpc error: code = InvalidArgument desc = the projection for viewB cannot be applied because it contains featInvalid which the FeatureView doesn't have")
}

func TestGetFeatureViewsToUseByService_returnsErrorWithInvalidOnDemandFeatures(t *testing.T) {
	projectName := "test_project"
	testRegistry, err := createRegistry(projectName)
	assert.NoError(t, err)

	featASpec := test.CreateFeature("featA", types.ValueType_INT32)
	featBSpec := test.CreateFeature("featB", types.ValueType_INT32)
	featCSpec := test.CreateFeature("featC", types.ValueType_INT32)
	featDSpec := test.CreateFeature("featD", types.ValueType_INT32)
	featESpec := test.CreateFeature("featE", types.ValueType_FLOAT)
	onDemandFeature1 := test.CreateFeature("featF", types.ValueType_FLOAT)
	onDemandFeature2 := test.CreateFeature("featG", types.ValueType_FLOAT)
	featSSpec := test.CreateFeature("featS", types.ValueType_FLOAT)
	sortKeyA := test.CreateSortKeyProto("featS", core.SortOrder_DESC, types.ValueType_FLOAT)

	entities := []*core.Entity{test.CreateEntityProto("entity", types.ValueType_INT32, "entity")}
	viewA := test.CreateFeatureViewProto("viewA", entities, featASpec, featBSpec)
	viewB := test.CreateFeatureViewProto("viewB", entities, featCSpec, featDSpec)
	viewC := test.CreateFeatureViewProto("viewC", entities, featESpec)
	viewS := test.CreateSortedFeatureViewProto("viewS", entities, []*core.SortKey{sortKeyA}, featSSpec)
	onDemandView := test.CreateOnDemandFeatureViewProto(
		"odfv",
		map[string][]*core.FeatureSpecV2{"viewB": {featCSpec}, "viewC": {featESpec}},
		onDemandFeature1, onDemandFeature2)

	featInvalidSpec := test.CreateFeature("featInvalid", types.ValueType_INT32)
	fs := test.CreateFeatureService("service", map[string][]*core.FeatureSpecV2{
		"viewA": {featASpec, featBSpec},
		"viewB": {featCSpec},
		"odfv":  {onDemandFeature2, featInvalidSpec},
	})
	testRegistry.SetModels([]*core.FeatureService{}, []*core.Entity{}, []*core.FeatureView{viewA, viewB, viewC}, []*core.SortedFeatureView{viewS}, []*core.OnDemandFeatureView{onDemandView})

	_, _, invalidFeaturesErr := GetFeatureViewsToUseByService(fs, testRegistry, projectName)
	assert.EqualError(t, invalidFeaturesErr, "rpc error: code = InvalidArgument desc = the projection for odfv cannot be applied because it contains featInvalid which the FeatureView doesn't have")
}

func TestGetSortedFeatureViewsToUseByService(t *testing.T) {
	projectName := "test_project"
	testRegistry, err := createRegistry(projectName)
	assert.NoError(t, err)

	featASpec := test.CreateFeature("featA", types.ValueType_INT32)
	featBSpec := test.CreateFeature("featB", types.ValueType_INT32)
	featCSpec := test.CreateFeature("featC", types.ValueType_INT32)
	featDSpec := test.CreateFeature("featD", types.ValueType_INT32)
	featESpec := test.CreateFeature("featE", types.ValueType_FLOAT)

	sortKeyA := test.CreateSortKeyProto("featS", core.SortOrder_DESC, types.ValueType_FLOAT)
	sortKeyB := test.CreateSortKeyProto("timestamp", core.SortOrder_ASC, types.ValueType_UNIX_TIMESTAMP)

	entities := []*core.Entity{test.CreateEntityProto("entity", types.ValueType_INT32, "entity")}

	sortedViewA := test.CreateSortedFeatureViewProto("sortedViewA", entities, []*core.SortKey{sortKeyA}, featASpec, featBSpec)
	sortedViewB := test.CreateSortedFeatureViewProto("sortedViewB", entities, []*core.SortKey{sortKeyB}, featCSpec, featDSpec)
	sortedViewC := test.CreateSortedFeatureViewProto("sortedViewC", entities, []*core.SortKey{sortKeyA}, featESpec)

	fs := test.CreateFeatureService("sorted_service", map[string][]*core.FeatureSpecV2{
		"sortedViewA": {featASpec, featBSpec},
		"sortedViewB": {featCSpec},
		"sortedViewC": {featESpec},
	})

	testRegistry.SetModels([]*core.FeatureService{}, []*core.Entity{}, []*core.FeatureView{}, []*core.SortedFeatureView{sortedViewA, sortedViewB, sortedViewC}, []*core.OnDemandFeatureView{})

	sfvs, err := GetSortedFeatureViewsToUseByService(fs, testRegistry, projectName)

	assert.Nil(t, err)
	assert.Len(t, sfvs, 3)

	sfvsByName := make(map[string]*SortedFeatureViewAndRefs)
	for _, sfv := range sfvs {
		sfvsByName[sfv.View.Base.Name] = sfv
	}

	assert.Equal(t, []string{"featA", "featB"}, sfvsByName["sortedViewA"].FeatureRefs)
	assert.Equal(t, []string{"featC"}, sfvsByName["sortedViewB"].FeatureRefs)
	assert.Equal(t, []string{"featE"}, sfvsByName["sortedViewC"].FeatureRefs)
	assert.Equal(t, "featS", sfvsByName["sortedViewA"].View.SortKeys[0].FieldName)
	assert.Equal(t, core.SortOrder_DESC, *sfvsByName["sortedViewA"].View.SortKeys[0].Order.Order.Enum())
	assert.Equal(t, "timestamp", sfvsByName["sortedViewB"].View.SortKeys[0].FieldName)
	assert.Equal(t, core.SortOrder_ASC, *sfvsByName["sortedViewB"].View.SortKeys[0].Order.Order.Enum())
}

func TestGetSortedFeatureViewsToUseByService_ReturnsErrorWithInvalidFeatures(t *testing.T) {
	projectName := "test_project"
	testRegistry, err := createRegistry(projectName)
	assert.NoError(t, err)

	featASpec := test.CreateFeature("featA", types.ValueType_INT32)
	featBSpec := test.CreateFeature("featB", types.ValueType_INT32)
	featInvalidSpec := test.CreateFeature("featInvalid", types.ValueType_INT32)

	sortKeyA := test.CreateSortKeyProto("timestamp", core.SortOrder_DESC, types.ValueType_UNIX_TIMESTAMP)
	entities := []*core.Entity{test.CreateEntityProto("entity", types.ValueType_INT32, "entity")}
	sortedViewA := test.CreateSortedFeatureViewProto("sortedViewA", entities, []*core.SortKey{sortKeyA}, featASpec, featBSpec)

	fs := test.CreateFeatureService("invalid_sorted_service", map[string][]*core.FeatureSpecV2{
		"sortedViewA": {featASpec, featBSpec, featInvalidSpec},
	})

	testRegistry.SetModels([]*core.FeatureService{}, []*core.Entity{}, []*core.FeatureView{}, []*core.SortedFeatureView{sortedViewA}, []*core.OnDemandFeatureView{})

	_, invalidFeaturesErr := GetSortedFeatureViewsToUseByService(fs, testRegistry, projectName)
	assert.Error(t, invalidFeaturesErr)
	assert.Contains(t, invalidFeaturesErr.Error(), "rpc error: code = InvalidArgument desc")
	assert.Contains(t, invalidFeaturesErr.Error(), "featInvalid which the FeatureView doesn't have")
}

func TestGetFeatureViewsToUseByFeatureRefs_returnsErrorWithInvalidFeatures(t *testing.T) {
	projectName := "test_project"
	testRegistry, err := createRegistry(projectName)
	assert.NoError(t, err)

	featASpec := test.CreateFeature("featA", types.ValueType_INT32)
	featBSpec := test.CreateFeature("featB", types.ValueType_INT32)
	featCSpec := test.CreateFeature("featC", types.ValueType_INT32)
	featDSpec := test.CreateFeature("featD", types.ValueType_INT32)
	featESpec := test.CreateFeature("featE", types.ValueType_FLOAT)
	onDemandFeature1 := test.CreateFeature("featF", types.ValueType_FLOAT)
	onDemandFeature2 := test.CreateFeature("featG", types.ValueType_FLOAT)
	featSSpec := test.CreateFeature("featS", types.ValueType_FLOAT)
	sortKeyA := test.CreateSortKeyProto("featS", core.SortOrder_DESC, types.ValueType_FLOAT)

	entities := []*core.Entity{test.CreateEntityProto("entity", types.ValueType_INT32, "entity")}
	viewA := test.CreateFeatureViewProto("viewA", entities, featASpec, featBSpec)
	viewB := test.CreateFeatureViewProto("viewB", entities, featCSpec, featDSpec)
	viewC := test.CreateFeatureViewProto("viewC", entities, featESpec)
	viewS := test.CreateSortedFeatureViewProto("viewS", entities, []*core.SortKey{sortKeyA}, featSSpec)
	onDemandView := test.CreateOnDemandFeatureViewProto(
		"odfv",
		map[string][]*core.FeatureSpecV2{"viewB": {featCSpec}, "viewC": {featESpec}},
		onDemandFeature1, onDemandFeature2)
	testRegistry.SetModels([]*core.FeatureService{}, []*core.Entity{}, []*core.FeatureView{viewA, viewB, viewC}, []*core.SortedFeatureView{viewS}, []*core.OnDemandFeatureView{onDemandView})

	_, _, fvErr := GetFeatureViewsToUseByFeatureRefs(
		[]string{
			"viewA:featA",
			"viewA:featB",
			"viewB:featInvalid",
			"odfv:odFeatInvalid",
		},
		testRegistry, projectName)
	assert.Error(t, fvErr)
	assert.Contains(t, fvErr.Error(), "rpc error: code = InvalidArgument desc")
	// Fail only on the first invalid feature
	assert.Contains(t, fvErr.Error(), "featInvalid does not exist in feature view viewB")
}

func TestGetSortedFeatureViewsToUseByFeatureRefs(t *testing.T) {
	projectName := "test_project"
	testRegistry, err := createRegistry(projectName)
	assert.NoError(t, err)

	featASpec := test.CreateFeature("featA", types.ValueType_INT32)
	featBSpec := test.CreateFeature("featB", types.ValueType_INT32)
	featCSpec := test.CreateFeature("featC", types.ValueType_INT32)
	featDSpec := test.CreateFeature("featD", types.ValueType_INT32)
	featESpec := test.CreateFeature("featE", types.ValueType_FLOAT)

	sortKeyA := test.CreateSortKeyProto("timestamp", core.SortOrder_DESC, types.ValueType_UNIX_TIMESTAMP)
	sortKeyB := test.CreateSortKeyProto("price", core.SortOrder_ASC, types.ValueType_DOUBLE)

	entities := []*core.Entity{test.CreateEntityProto("entity", types.ValueType_INT32, "entity")}

	sortedViewA := test.CreateSortedFeatureViewProto("sortedViewA", entities, []*core.SortKey{sortKeyA}, featASpec, featBSpec)
	sortedViewB := test.CreateSortedFeatureViewProto("sortedViewB", entities, []*core.SortKey{sortKeyB}, featCSpec, featDSpec)
	sortedViewC := test.CreateSortedFeatureViewProto("sortedViewC", entities, []*core.SortKey{sortKeyA}, featESpec)

	testRegistry.SetModels([]*core.FeatureService{}, []*core.Entity{}, []*core.FeatureView{}, []*core.SortedFeatureView{sortedViewA, sortedViewB, sortedViewC}, []*core.OnDemandFeatureView{})

	sfvs, err := GetSortedFeatureViewsToUseByFeatureRefs(
		[]string{
			"sortedViewA:featA",
			"sortedViewA:featB",
			"sortedViewB:featC",
			"sortedViewC:featE",
		},
		testRegistry, projectName)

	assert.Nil(t, err)
	assert.Len(t, sfvs, 3)

	sfvsByName := make(map[string]*SortedFeatureViewAndRefs)
	for _, sfv := range sfvs {
		sfvsByName[sfv.View.Base.Name] = sfv
	}

	assert.Equal(t, []string{"featA", "featB"}, sfvsByName["sortedViewA"].FeatureRefs)
	assert.Equal(t, []string{"featC"}, sfvsByName["sortedViewB"].FeatureRefs)
	assert.Equal(t, []string{"featE"}, sfvsByName["sortedViewC"].FeatureRefs)
	assert.Equal(t, "timestamp", sfvsByName["sortedViewA"].View.SortKeys[0].FieldName)
	assert.Equal(t, "price", sfvsByName["sortedViewB"].View.SortKeys[0].FieldName)
	assert.Equal(t, "timestamp", sfvsByName["sortedViewC"].View.SortKeys[0].FieldName)
}

func TestGetSortedFeatureViewsToUseByFeatureRefs_ReturnsErrorWithInvalidFeatures(t *testing.T) {
	projectName := "test_project"
	testRegistry, err := createRegistry(projectName)
	assert.NoError(t, err)

	featASpec := test.CreateFeature("featA", types.ValueType_INT32)
	featBSpec := test.CreateFeature("featB", types.ValueType_INT32)

	sortKeyA := test.CreateSortKeyProto("timestamp", core.SortOrder_DESC, types.ValueType_UNIX_TIMESTAMP)
	entities := []*core.Entity{test.CreateEntityProto("entity", types.ValueType_INT32, "entity")}

	sortedViewA := test.CreateSortedFeatureViewProto("sortedViewA", entities, []*core.SortKey{sortKeyA}, featASpec, featBSpec)

	testRegistry.SetModels([]*core.FeatureService{}, []*core.Entity{}, []*core.FeatureView{}, []*core.SortedFeatureView{sortedViewA}, []*core.OnDemandFeatureView{})

	_, sfvErr := GetSortedFeatureViewsToUseByFeatureRefs(
		[]string{
			"sortedViewA:featA",
			"sortedViewA:featB",
			"sortedViewA:featInvalid",
		},
		testRegistry, projectName)

	assert.Error(t, sfvErr)
	assert.Contains(t, sfvErr.Error(), "rpc error: code = InvalidArgument desc")
	assert.Contains(t, sfvErr.Error(), "featInvalid does not exist in feature view sortedViewA")
}

func TestValidateSortKeyFilters_ValidFilters(t *testing.T) {
	sortKey1 := test.CreateSortKeyProto("timestamp", core.SortOrder_DESC, types.ValueType_UNIX_TIMESTAMP)
	sortKey2 := test.CreateSortKeyProto("price", core.SortOrder_ASC, types.ValueType_DOUBLE)
	sortKey3 := test.CreateSortKeyProto("name", core.SortOrder_ASC, types.ValueType_STRING)

	entity1 := test.CreateEntityProto("driver", types.ValueType_INT64, "driver")
	entity2 := test.CreateEntityProto("customer", types.ValueType_STRING, "customer")
	sfv1 := test.CreateSortedFeatureViewModel("sfv1", []*core.Entity{entity1},
		[]*core.SortKey{sortKey1, sortKey2},
		test.CreateFeature("f1", types.ValueType_DOUBLE))

	sfv2 := test.CreateSortedFeatureViewModel("sfv2", []*core.Entity{entity2},
		[]*core.SortKey{sortKey3},
		test.CreateFeature("f2", types.ValueType_STRING))

	sortedViews := []*SortedFeatureViewAndRefs{
		{View: sfv1, FeatureRefs: []string{"f1"}},
		{View: sfv2, FeatureRefs: []string{"f2"}},
	}

	validFilters := []*serving.SortKeyFilter{
		{
			SortKeyName: "price",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeStart:     &types.Value{Val: &types.Value_NullVal{NullVal: types.Null_NULL}},
					RangeEnd:       &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 50.0}},
					StartInclusive: true,
					EndInclusive:   true,
				},
			},
		},
		{
			SortKeyName: "timestamp",
			Query: &serving.SortKeyFilter_Equals{
				Equals: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 1640995200}},
			},
		},
	}

	err := ValidateSortKeyFilters(validFilters, sortedViews)
	assert.NoError(t, err, "Valid filters should not produce an error")

	sfv3 := test.CreateSortedFeatureViewModel("sfv2", []*core.Entity{entity2},
		[]*core.SortKey{sortKey1, sortKey3, sortKey2},
		test.CreateFeature("f3", types.ValueType_STRING))

	sortedViews = []*SortedFeatureViewAndRefs{
		{View: sfv3, FeatureRefs: []string{"f3"}},
	}

	validFilters = []*serving.SortKeyFilter{
		{
			SortKeyName: "price",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeStart:     &types.Value{Val: &types.Value_NullVal{NullVal: types.Null_NULL}},
					RangeEnd:       &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 50.0}},
					StartInclusive: true,
					EndInclusive:   true,
				},
			},
		},
		{
			SortKeyName: "timestamp",
			Query: &serving.SortKeyFilter_Equals{
				Equals: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 1640995200}},
			},
		},
		{
			SortKeyName: "name",
			Query: &serving.SortKeyFilter_Equals{
				Equals: &types.Value{Val: &types.Value_StringVal{StringVal: "John"}},
			},
		},
	}

	err = ValidateSortKeyFilters(validFilters, sortedViews)
	assert.NoError(t, err, "Valid filters should not produce an error")

	validFilters = []*serving.SortKeyFilter{
		{
			SortKeyName: "timestamp",
			Query: &serving.SortKeyFilter_Equals{
				Equals: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 1640995200}},
			},
		},
	}

	err = ValidateSortKeyFilters(validFilters, sortedViews)
	assert.NoError(t, err, "Valid filters should not produce an error")

	validFilters = []*serving.SortKeyFilter{
		{
			SortKeyName: "timestamp",
			Query: &serving.SortKeyFilter_Equals{
				Equals: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 1640995200}},
			},
		},
		{
			SortKeyName: "name",
			Query: &serving.SortKeyFilter_Equals{
				Equals: &types.Value{Val: &types.Value_StringVal{StringVal: "John"}},
			},
		},
	}

	err = ValidateSortKeyFilters(validFilters, sortedViews)
	assert.NoError(t, err, "Valid filters should not produce an error")
}

func TestValidateSortKeyFilters_EmptyFilters(t *testing.T) {
	sortKey1 := test.CreateSortKeyProto("timestamp", core.SortOrder_DESC, types.ValueType_UNIX_TIMESTAMP)
	sortKey2 := test.CreateSortKeyProto("price", core.SortOrder_ASC, types.ValueType_DOUBLE)
	sortKey3 := test.CreateSortKeyProto("name", core.SortOrder_ASC, types.ValueType_STRING)

	entity1 := test.CreateEntityProto("driver", types.ValueType_INT64, "driver")
	entity2 := test.CreateEntityProto("customer", types.ValueType_STRING, "customer")
	sfv1 := test.CreateSortedFeatureViewModel("sfv1", []*core.Entity{entity1},
		[]*core.SortKey{sortKey1, sortKey2},
		test.CreateFeature("f1", types.ValueType_DOUBLE))

	sfv2 := test.CreateSortedFeatureViewModel("sfv2", []*core.Entity{entity2},
		[]*core.SortKey{sortKey3},
		test.CreateFeature("f2", types.ValueType_STRING))

	sortedViews := []*SortedFeatureViewAndRefs{
		{View: sfv1, FeatureRefs: []string{"f1"}},
		{View: sfv2, FeatureRefs: []string{"f2"}},
	}

	validFilters := make([]*serving.SortKeyFilter, 0)

	err := ValidateSortKeyFilters(validFilters, sortedViews)
	assert.NoError(t, err, "Valid filters should not produce an error")

	err = ValidateSortKeyFilters(nil, sortedViews)
	assert.NoError(t, err, "Valid filters should not produce an error")
}

func TestValidateSortKeyFilters_NonExistentKey(t *testing.T) {
	sortKey1 := test.CreateSortKeyProto("timestamp", core.SortOrder_DESC, types.ValueType_UNIX_TIMESTAMP)
	sortKey2 := test.CreateSortKeyProto("price", core.SortOrder_ASC, types.ValueType_DOUBLE)
	sortKey3 := test.CreateSortKeyProto("name", core.SortOrder_ASC, types.ValueType_STRING)

	entity1 := test.CreateEntityProto("driver", types.ValueType_INT64, "driver")
	entity2 := test.CreateEntityProto("customer", types.ValueType_STRING, "customer")
	sfv1 := test.CreateSortedFeatureViewModel("sfv1", []*core.Entity{entity1},
		[]*core.SortKey{sortKey1, sortKey2},
		test.CreateFeature("f1", types.ValueType_DOUBLE))

	sfv2 := test.CreateSortedFeatureViewModel("sfv2", []*core.Entity{entity2},
		[]*core.SortKey{sortKey3},
		test.CreateFeature("f2", types.ValueType_STRING))

	sortedViews := []*SortedFeatureViewAndRefs{
		{View: sfv1, FeatureRefs: []string{"f1"}},
		{View: sfv2, FeatureRefs: []string{"f2"}},
	}

	nonExistentKeyFilter := []*serving.SortKeyFilter{
		{
			SortKeyName: "non_existent_key",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeStart: &types.Value{Val: &types.Value_Int64Val{Int64Val: 123}},
				},
			},
		},
	}

	err := ValidateSortKeyFilters(nonExistentKeyFilter, sortedViews)
	assert.Error(t, err, "Non-existent sort key should produce an error")
	assert.Contains(t, err.Error(), "not found in any of the requested sorted feature views")
}

func TestValidateSortKeyFilters_TypeMismatch(t *testing.T) {
	sortKey1 := test.CreateSortKeyProto("timestamp", core.SortOrder_DESC, types.ValueType_UNIX_TIMESTAMP)
	sortKey2 := test.CreateSortKeyProto("price", core.SortOrder_ASC, types.ValueType_DOUBLE)
	sortKey3 := test.CreateSortKeyProto("name", core.SortOrder_ASC, types.ValueType_STRING)

	entity1 := test.CreateEntityProto("driver", types.ValueType_INT64, "driver")
	entity2 := test.CreateEntityProto("customer", types.ValueType_STRING, "customer")
	sfv1 := test.CreateSortedFeatureViewModel("sfv1", []*core.Entity{entity1},
		[]*core.SortKey{sortKey1, sortKey2},
		test.CreateFeature("f1", types.ValueType_DOUBLE))

	sfv2 := test.CreateSortedFeatureViewModel("sfv2", []*core.Entity{entity2},
		[]*core.SortKey{sortKey3},
		test.CreateFeature("f2", types.ValueType_STRING))

	sortedViews := []*SortedFeatureViewAndRefs{
		{View: sfv1, FeatureRefs: []string{"f1"}},
		{View: sfv2, FeatureRefs: []string{"f2"}},
	}

	typeMismatchFilter := []*serving.SortKeyFilter{
		{
			SortKeyName: "timestamp",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeStart: &types.Value{Val: &types.Value_StringVal{StringVal: "2022-01-01"}},
				},
			},
		},
	}

	err := ValidateSortKeyFilters(typeMismatchFilter, sortedViews)
	assert.Error(t, err, "Type mismatch should produce an error")
	assert.Contains(t, err.Error(), "has incompatible type")
}

func TestValidateSortKeyFilters_InvalidRangeFilter(t *testing.T) {
	sortKey1 := test.CreateSortKeyProto("timestamp", core.SortOrder_DESC, types.ValueType_UNIX_TIMESTAMP)
	sortKey2 := test.CreateSortKeyProto("price", core.SortOrder_ASC, types.ValueType_DOUBLE)
	sortKey3 := test.CreateSortKeyProto("name", core.SortOrder_ASC, types.ValueType_STRING)

	entity1 := test.CreateEntityProto("driver", types.ValueType_INT64, "driver")
	entity2 := test.CreateEntityProto("customer", types.ValueType_STRING, "customer")
	sfv1 := test.CreateSortedFeatureViewModel("sfv1", []*core.Entity{entity1},
		[]*core.SortKey{sortKey1, sortKey2},
		test.CreateFeature("f1", types.ValueType_DOUBLE))

	sfv2 := test.CreateSortedFeatureViewModel("sfv2", []*core.Entity{entity2},
		[]*core.SortKey{sortKey1, sortKey2, sortKey3},
		test.CreateFeature("f2", types.ValueType_STRING))

	sortedViews := []*SortedFeatureViewAndRefs{
		{View: sfv1, FeatureRefs: []string{"f1"}},
		{View: sfv2, FeatureRefs: []string{"f2"}},
	}

	invalidRangeFilter := []*serving.SortKeyFilter{
		{
			SortKeyName: "timestamp",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeStart: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 1640995200}},
				},
			},
		},
		{
			SortKeyName: "price",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeStart: &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 10.5}},
				},
			},
		},
	}

	err := ValidateSortKeyFilters(invalidRangeFilter, sortedViews)
	assert.Error(t, err, "Only the last sort key filter may have a range query")
	assert.Contains(t, err.Error(), "sort key filter for sort key 'timestamp' must have query type equals instead of range")

	invalidRangeFilter = []*serving.SortKeyFilter{
		{
			SortKeyName: "timestamp",
			Query: &serving.SortKeyFilter_Equals{
				Equals: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 1}},
			},
		},
		{
			SortKeyName: "price",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeEnd:     &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 10.5}},
					EndInclusive: true,
				},
			},
		},
		{
			SortKeyName: "name",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeStart: &types.Value{Val: &types.Value_StringVal{StringVal: "A"}},
				},
			},
		},
	}

	err = ValidateSortKeyFilters(invalidRangeFilter, sortedViews)
	assert.Error(t, err, "Sort key filter must have equality relations for all sort keys except the last one")
	assert.Contains(t, err.Error(), "sort key filter for sort key 'price' must have query type equals instead of range")
}

func TestValidateSortKeyFilters_InvalidEqualsFilter(t *testing.T) {
	sortKey1 := test.CreateSortKeyProto("timestamp", core.SortOrder_DESC, types.ValueType_UNIX_TIMESTAMP)
	sortKey2 := test.CreateSortKeyProto("price", core.SortOrder_ASC, types.ValueType_DOUBLE)

	entity1 := test.CreateEntityProto("driver", types.ValueType_INT64, "driver")
	sfv1 := test.CreateSortedFeatureViewModel("sfv1", []*core.Entity{entity1},
		[]*core.SortKey{sortKey1, sortKey2},
		test.CreateFeature("f1", types.ValueType_DOUBLE))

	sortedViews := []*SortedFeatureViewAndRefs{
		{View: sfv1, FeatureRefs: []string{"f1"}},
	}

	invalidRangeFilter := []*serving.SortKeyFilter{
		{
			SortKeyName: "timestamp",
			Query: &serving.SortKeyFilter_Equals{
				Equals: &types.Value{Val: &types.Value_NullVal{NullVal: types.Null_NULL}},
			},
		},
		{
			SortKeyName: "price",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeStart: &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 10.5}},
				},
			},
		},
	}

	err := ValidateSortKeyFilters(invalidRangeFilter, sortedViews)
	assert.Error(t, err, "Sort key filter equality value cannot be null")
	assert.Contains(t, err.Error(), "equals value for sort key 'timestamp' has incompatible type: expected UNIX_TIMESTAMP")
}

func TestValidateSortKeyFilters_MissingFilter(t *testing.T) {
	sortKey1 := test.CreateSortKeyProto("timestamp", core.SortOrder_DESC, types.ValueType_UNIX_TIMESTAMP)
	sortKey2 := test.CreateSortKeyProto("price", core.SortOrder_ASC, types.ValueType_DOUBLE)
	sortKey3 := test.CreateSortKeyProto("name", core.SortOrder_ASC, types.ValueType_STRING)

	entity1 := test.CreateEntityProto("driver", types.ValueType_INT64, "driver")
	entity2 := test.CreateEntityProto("customer", types.ValueType_STRING, "customer")
	sfv1 := test.CreateSortedFeatureViewModel("sfv1", []*core.Entity{entity1},
		[]*core.SortKey{sortKey1, sortKey2, sortKey3},
		test.CreateFeature("f1", types.ValueType_DOUBLE))

	sfv2 := test.CreateSortedFeatureViewModel("sfv2", []*core.Entity{entity2},
		[]*core.SortKey{sortKey3},
		test.CreateFeature("f2", types.ValueType_STRING))

	sortedViews := []*SortedFeatureViewAndRefs{
		{View: sfv1, FeatureRefs: []string{"f1"}},
		{View: sfv2, FeatureRefs: []string{"f2"}},
	}

	missingFilters := []*serving.SortKeyFilter{
		{
			SortKeyName: "timestamp",
			Query: &serving.SortKeyFilter_Equals{
				Equals: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 1640995200}},
			},
		},
		{
			SortKeyName: "name",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeStart: &types.Value{Val: &types.Value_StringVal{StringVal: "A"}},
				},
			},
		},
	}

	err := ValidateSortKeyFilters(missingFilters, sortedViews)
	assert.Error(t, err, "Must include all previous sort keys in the filter list")
	assert.Contains(t, err.Error(), "specify sort key filter in request for sort key: 'price' with query type equals")
}

func TestGroupSortedFeatureRefs(t *testing.T) {
	sortKey1 := test.CreateSortKeyProto("timestamp", core.SortOrder_DESC, types.ValueType_UNIX_TIMESTAMP)
	sortKey2 := test.CreateSortKeyProto("featureF", core.SortOrder_ASC, types.ValueType_DOUBLE)
	entity1 := test.CreateEntityProto("driver", types.ValueType_INT64, "driver")
	entity2 := test.CreateEntityProto("customer", types.ValueType_STRING, "customer")
	viewA := test.CreateSortedFeatureViewModel("viewA", []*core.Entity{entity1, entity2},
		[]*core.SortKey{sortKey1},
		test.CreateFeature("featureA", types.ValueType_DOUBLE),
		test.CreateFeature("featureB", types.ValueType_DOUBLE))

	viewB := test.CreateSortedFeatureViewModel("viewB", []*core.Entity{entity1, entity2},
		[]*core.SortKey{sortKey1},
		test.CreateFeature("featureC", types.ValueType_DOUBLE),
		test.CreateFeature("featureD", types.ValueType_DOUBLE))

	viewC := test.CreateSortedFeatureViewModel("viewC", []*core.Entity{entity1},
		[]*core.SortKey{sortKey1},
		test.CreateFeature("featureE", types.ValueType_DOUBLE))

	viewD := test.CreateSortedFeatureViewModel("viewD", []*core.Entity{entity2},
		[]*core.SortKey{sortKey2},
		test.CreateFeature("featureF", types.ValueType_DOUBLE))

	if viewA.Base != nil && viewA.Base.Projection == nil {
		viewA.Base.Projection = &model.FeatureViewProjection{
			NameAlias: "aliasViewA",
		}
	}

	sortKeyFilters := []*serving.SortKeyFilter{
		{
			SortKeyName: "timestamp",
			Query: &serving.SortKeyFilter_Equals{
				Equals: &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 1640995200}},
			},
		},
		{
			SortKeyName: "featureF",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeEnd:     &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 1.5}},
					EndInclusive: true,
				},
			},
		},
	}

	refGroups, err := GroupSortedFeatureRefs(
		[]*SortedFeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureC", "featureD"}},
			{View: viewC, FeatureRefs: []string{"featureE"}},
			{View: viewD, FeatureRefs: []string{"featureF"}},
		},
		map[string]*types.RepeatedValue{
			"driver_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
			}},
			"customer_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 2}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 4}},
			}},
		},
		map[string]string{
			"driver":   "driver_id",
			"customer": "customer_id",
		},
		sortKeyFilters,
		false,
		10,
		true,
	)

	t.Logf("GroupSortedFeatureRefs returned %d groups", len(refGroups))
	for i, group := range refGroups {
		t.Logf("Group %d:", i)
		t.Logf("  Features: %v", group.FeatureNames)
		t.Logf("  AliasedNames: %v", group.AliasedFeatureNames)
		filterNames := make([]string, len(group.SortKeyFilters))
		for j, filter := range group.SortKeyFilters {
			filterNames[j] = filter.SortKeyName
		}
		t.Logf("  SortKeyFilters: %v", filterNames)
	}

	assert.NoError(t, err)
	assert.NotEmpty(t, refGroups, "Should return at least one group")

	for _, group := range refGroups {
		assert.Equal(t, 1, len(group.SortKeyFilters))
		if group.SortKeyFilters[0].SortKeyName == "timestamp" {
			assert.Equal(t, sortKeyFilters[0].SortKeyName, group.SortKeyFilters[0].SortKeyName)
			assert.Equal(t, sortKeyFilters[0].GetEquals().GetUnixTimestampVal(), group.SortKeyFilters[0].Equals.(time.Time).Unix())
			assert.Nil(t, group.SortKeyFilters[0].RangeStart)
			assert.Nil(t, group.SortKeyFilters[0].RangeEnd)
			assert.Nil(t, group.SortKeyFilters[0].Order)
		} else {
			assert.Equal(t, sortKeyFilters[1].SortKeyName, group.SortKeyFilters[0].SortKeyName)
			assert.Equal(t, sortKeyFilters[1].GetRange().RangeEnd.GetDoubleVal(), group.SortKeyFilters[0].RangeEnd)
			assert.Equal(t, sortKeyFilters[1].GetRange().EndInclusive, group.SortKeyFilters[0].EndInclusive)
			assert.Nil(t, group.SortKeyFilters[0].RangeStart)
			assert.Nil(t, group.SortKeyFilters[0].Equals)
			assert.Nil(t, group.SortKeyFilters[0].Order)
		}
		assert.Equal(t, int32(10), group.Limit)
		assert.False(t, group.IsReverseSortOrder)
	}

	featureAFound := false
	featureCFound := false
	featureEFound := false

	for _, group := range refGroups {
		for _, feature := range group.FeatureNames {
			if feature == "featureA" {
				featureAFound = true
			}
			if feature == "featureC" {
				featureCFound = true
			}
			if feature == "featureE" {
				featureEFound = true
			}
		}
	}

	assert.True(t, featureAFound, "Feature A should be present in results")
	assert.True(t, featureCFound, "Feature C should be present in results")
	assert.True(t, featureEFound, "Feature E should be present in results")
}

func TestGroupSortedFeatureRefs_withReverseSortOrder(t *testing.T) {
	sortKey1 := test.CreateSortKeyProto("timestamp", core.SortOrder_DESC, types.ValueType_UNIX_TIMESTAMP)
	sortKey2 := test.CreateSortKeyProto("featureB", core.SortOrder_ASC, types.ValueType_DOUBLE)
	entity1 := test.CreateEntityProto("driver", types.ValueType_INT64, "driver")
	entity2 := test.CreateEntityProto("customer", types.ValueType_STRING, "customer")
	viewA := test.CreateSortedFeatureViewModel("viewA", []*core.Entity{entity1, entity2},
		[]*core.SortKey{sortKey1, sortKey2},
		test.CreateFeature("featureA", types.ValueType_DOUBLE),
		test.CreateFeature("featureB", types.ValueType_DOUBLE))

	sortKeyFilters := []*serving.SortKeyFilter{
		{
			SortKeyName: "timestamp",
			Query: &serving.SortKeyFilter_Range{
				Range: &serving.SortKeyFilter_RangeQuery{
					RangeStart:     &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 1640995200}},
					RangeEnd:       &types.Value{Val: &types.Value_UnixTimestampVal{UnixTimestampVal: 1672531200}},
					StartInclusive: true,
					EndInclusive:   false,
				},
			},
		},
	}

	refGroups, err := GroupSortedFeatureRefs(
		[]*SortedFeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
		},
		map[string]*types.RepeatedValue{
			"driver_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 0}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 1}},
			}},
			"customer_id": {Val: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_Int32Val{Int32Val: 2}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 3}},
				{Val: &types.Value_Int32Val{Int32Val: 4}},
			}},
		},
		map[string]string{
			"driver":   "driver_id",
			"customer": "customer_id",
		},
		sortKeyFilters,
		true,
		10,
		true,
	)

	t.Logf("GroupSortedFeatureRefs returned %d groups", len(refGroups))
	for i, group := range refGroups {
		t.Logf("Group %d:", i)
		t.Logf("  Features: %v", group.FeatureNames)
		t.Logf("  AliasedNames: %v", group.AliasedFeatureNames)
	}

	assert.NoError(t, err)
	assert.NotEmpty(t, refGroups, "Should return at least one group")

	for _, group := range refGroups {
		assert.Equal(t, 2, len(group.SortKeyFilters))
		assert.Equal(t, sortKeyFilters[0].SortKeyName, group.SortKeyFilters[0].SortKeyName)
		assert.Equal(t, sortKeyFilters[0].GetRange().RangeStart.GetUnixTimestampVal(), group.SortKeyFilters[0].RangeStart.(time.Time).Unix())
		assert.Equal(t, sortKeyFilters[0].GetRange().RangeEnd.GetUnixTimestampVal(), group.SortKeyFilters[0].RangeEnd.(time.Time).Unix())
		assert.Equal(t, sortKeyFilters[0].GetRange().StartInclusive, group.SortKeyFilters[0].StartInclusive)
		assert.Equal(t, sortKeyFilters[0].GetRange().EndInclusive, group.SortKeyFilters[0].EndInclusive)
		assert.Equal(t, "ASC", group.SortKeyFilters[0].Order.Order.String())

		// SortKeys missing from the filters should have a default filter with only Order assigned
		assert.Equal(t, sortKey2.Name, group.SortKeyFilters[1].SortKeyName)
		assert.Nil(t, group.SortKeyFilters[1].RangeStart)
		assert.Nil(t, group.SortKeyFilters[1].RangeEnd)
		assert.Equal(t, "DESC", group.SortKeyFilters[1].Order.Order.String())

		assert.Equal(t, int32(10), group.Limit)
		assert.True(t, group.IsReverseSortOrder)
	}

	featureAFound := false

	for _, group := range refGroups {
		for _, feature := range group.FeatureNames {
			if feature == "featureA" {
				featureAFound = true
			}
		}
	}

	assert.True(t, featureAFound, "Feature A should be present in results")
}

func TestGetUniqueEntityRows_WithUniqueValues(t *testing.T) {
	entityKeys := []*types.EntityKey{
		{
			JoinKeys:     []string{"id"},
			EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 1}}},
		},
		{
			JoinKeys:     []string{"id"},
			EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 2}}},
		},
		{
			JoinKeys:     []string{"id"},
			EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 3}}},
		},
	}

	uniqueEntityRows, mappingIndices, err := getUniqueEntityRows(entityKeys)

	require.NoError(t, err)
	assert.Len(t, uniqueEntityRows, 3)
	assert.Len(t, mappingIndices, 3)

	for i := 0; i < 3; i++ {
		assert.Equal(t, []int{i}, mappingIndices[i])
		assert.True(t, proto.Equal(uniqueEntityRows[i], entityKeys[i]))
	}
}

func TestGetUniqueEntityRows_WithDuplicates(t *testing.T) {
	entityKeys := []*types.EntityKey{
		{
			JoinKeys:     []string{"id"},
			EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 1}}},
		},
		{
			JoinKeys:     []string{"id"},
			EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 2}}},
		},
		{
			JoinKeys:     []string{"id"},
			EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 1}}},
		},
		{
			JoinKeys:     []string{"id"},
			EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 3}}},
		},
		{
			JoinKeys:     []string{"id"},
			EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 2}}},
		},
	}

	uniqueEntityRows, mappingIndices, err := getUniqueEntityRows(entityKeys)

	require.NoError(t, err)
	assert.Len(t, uniqueEntityRows, 3)
	assert.Len(t, mappingIndices, 3)

	assert.True(t, proto.Equal(uniqueEntityRows[0], entityKeys[0]))
	assert.ElementsMatch(t, []int{0, 2}, mappingIndices[0])

	assert.True(t, proto.Equal(uniqueEntityRows[1], entityKeys[1]))
	assert.ElementsMatch(t, []int{1, 4}, mappingIndices[1])

	assert.True(t, proto.Equal(uniqueEntityRows[2], entityKeys[3]))
	assert.Equal(t, []int{3}, mappingIndices[2])
}

func TestGetUniqueEntityRows_MultipleJoinKeys(t *testing.T) {
	entityKeys := []*types.EntityKey{
		{
			JoinKeys: []string{"driver_id", "customer_id"},
			EntityValues: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_StringVal{StringVal: "A"}},
			},
		},
		{
			JoinKeys: []string{"driver_id", "customer_id"},
			EntityValues: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_StringVal{StringVal: "B"}},
			},
		},
		{
			JoinKeys: []string{"driver_id", "customer_id"},
			EntityValues: []*types.Value{
				{Val: &types.Value_Int32Val{Int32Val: 1}},
				{Val: &types.Value_StringVal{StringVal: "A"}},
			},
		},
	}

	uniqueEntityRows, mappingIndices, err := getUniqueEntityRows(entityKeys)

	require.NoError(t, err)
	assert.Len(t, uniqueEntityRows, 2)
	assert.Len(t, mappingIndices, 2)

	assert.True(t, proto.Equal(uniqueEntityRows[0], entityKeys[0]))
	assert.ElementsMatch(t, []int{0, 2}, mappingIndices[0])

	assert.True(t, proto.Equal(uniqueEntityRows[1], entityKeys[1]))
	assert.Equal(t, []int{1}, mappingIndices[1])
}

func TestTransposeFeatureRowsIntoColumns(t *testing.T) {
	vector := testTransposeFeatureRowsIntoColumns(t, true)
	resultsA, e := vector.GetProtoValues()
	vector.Values.(arrow.Array).Release()
	assert.NoError(t, e)
	resultsB, e := testTransposeFeatureRowsIntoColumns(t, false).GetProtoValues()
	assert.NoError(t, e)
	assert.ElementsMatch(t, resultsA, resultsB)
}

func testTransposeFeatureRowsIntoColumns(t *testing.T, useArrow bool) *FeatureVector {
	arrowAllocator := memory.NewGoAllocator()
	numRows := 2

	entity1 := test.CreateEntityProto("driver", types.ValueType_INT64, "driver")
	fv := test.CreateFeatureViewModel("testView", []*core.Entity{entity1}, test.CreateFeature("f1", types.ValueType_DOUBLE), test.CreateFeature("f2", types.ValueType_DOUBLE_LIST))

	featureViews := []*FeatureViewAndRefs{
		{View: fv, FeatureRefs: []string{"f1"}},
	}

	entityKeys := make([]*types.EntityKey, numRows)
	for i := range numRows {
		entityKeys[i] = &types.EntityKey{
			JoinKeys:     []string{"driver"},
			EntityValues: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: int64(i)}}},
		}
	}

	groupRef := &GroupedFeaturesPerEntitySet{
		FeatureNames:        []string{"f1", "f2"},
		FeatureViewNames:    []string{"testView"},
		AliasedFeatureNames: []string{"testView__f1"},
		EntityKeys:          entityKeys,
		Indices:             [][]int{{0}, {1}},
	}

	nowTime := time.Now()
	yesterdayTime := nowTime.Add(-24 * time.Hour)

	featureData := [][]onlinestore.FeatureData{
		{
			{
				Reference: serving.FeatureReferenceV2{
					FeatureViewName: "testView",
					FeatureName:     "f1",
				},
				Timestamp: timestamppb.Timestamp{Seconds: nowTime.Unix()},
				Value:     types.Value{Val: &types.Value_DoubleVal{DoubleVal: 10.0}},
			},
			{
				Reference: serving.FeatureReferenceV2{
					FeatureViewName: "testView",
					FeatureName:     "f2",
				},
				Timestamp: timestamppb.Timestamp{Seconds: nowTime.Unix()},
				Value:     types.Value{Val: &types.Value_DoubleListVal{DoubleListVal: &types.DoubleList{Val: []float64{1.1, 1.2}}}},
			},
		},
		{
			{
				Reference: serving.FeatureReferenceV2{
					FeatureViewName: "testView",
					FeatureName:     "f1",
				},
				Timestamp: timestamppb.Timestamp{Seconds: yesterdayTime.Unix()},
				Value:     types.Value{Val: &types.Value_DoubleVal{DoubleVal: 20.0}},
			},
			{
				Reference: serving.FeatureReferenceV2{
					FeatureViewName: "testView",
					FeatureName:     "f2",
				},
				Timestamp: timestamppb.Timestamp{Seconds: yesterdayTime.Unix()},
				Value:     types.Value{Val: &types.Value_DoubleListVal{DoubleListVal: &types.DoubleList{Val: []float64{2.1, 2.2}}}},
			},
		},
	}

	vectors, err := TransposeFeatureRowsIntoColumns(featureData, groupRef, featureViews, arrowAllocator, numRows, useArrow, serving.UseDefaultsMode_USE_DEFAULTS_OFF)

	assert.NoError(t, err)
	assert.Len(t, vectors, 1)
	vector := vectors[0]
	assert.Equal(t, "testView__f1", vector.Name)
	assert.Len(t, vector.Statuses, numRows)
	assert.Len(t, vector.Timestamps, numRows)
	assert.Len(t, vector.Statuses, 2)
	assert.Len(t, vector.Timestamps, 2)
	assert.Equal(t, serving.FieldStatus_PRESENT, vector.Statuses[0])
	assert.Equal(t, serving.FieldStatus_PRESENT, vector.Statuses[1])
	assert.NotNil(t, vector.Values)
	return vector
}

func TestTransposeRangeFeatureRowsIntoColumns(t *testing.T) {
	vector := testTransposeRangeFeatureRowsIntoColumns(t, true)
	resultsA, e := vector.GetProtoValues()
	vector.RangeValues.(arrow.Array).Release()
	assert.NoError(t, e)
	resultsB, e := testTransposeRangeFeatureRowsIntoColumns(t, false).GetProtoValues()
	assert.NoError(t, e)
	assert.ElementsMatch(t, resultsA, resultsB)
}

func testTransposeRangeFeatureRowsIntoColumns(t *testing.T, useArrow bool) *RangeFeatureVector {
	arrowAllocator := memory.NewGoAllocator()
	numRows := 2

	sortKey1 := test.CreateSortKeyProto("timestamp", core.SortOrder_DESC, types.ValueType_UNIX_TIMESTAMP)
	entity1 := test.CreateEntityProto("driver", types.ValueType_INT64, "driver")
	sfv := test.CreateSortedFeatureViewModel("testView", []*core.Entity{entity1}, []*core.SortKey{sortKey1},
		test.CreateFeature("f1", types.ValueType_DOUBLE))

	sortedViews := []*SortedFeatureViewAndRefs{
		{View: sfv, FeatureRefs: []string{"f1"}},
	}

	groupRef := &model.GroupedRangeFeatureRefs{
		FeatureNames:        []string{"f1"},
		FeatureViewNames:    []string{"testView"},
		AliasedFeatureNames: []string{"testView__f1"},
		Indices:             [][]int{{0}, {1}},
	}

	nowTime := time.Now()
	yesterdayTime := nowTime.Add(-24 * time.Hour)

	featureData := [][]onlinestore.RangeFeatureData{
		{
			{
				FeatureView: "testView",
				FeatureName: "f1",
				Values:      []interface{}{42.5, 43.2},
				Statuses:    []serving.FieldStatus{serving.FieldStatus_PRESENT, serving.FieldStatus_PRESENT},
				EventTimestamps: []timestamppb.Timestamp{
					{Seconds: nowTime.Unix()},
					{Seconds: yesterdayTime.Unix()},
				},
			},
		},
		{
			{
				FeatureView: "testView",
				FeatureName: "f1",
				Values:      []interface{}{99.9},
				Statuses:    []serving.FieldStatus{serving.FieldStatus_PRESENT},
				EventTimestamps: []timestamppb.Timestamp{
					{Seconds: nowTime.Unix()},
				},
			},
		},
	}

	vectors, err := TransposeRangeFeatureRowsIntoColumns(featureData, groupRef, sortedViews, arrowAllocator, numRows, useArrow, serving.UseDefaultsMode_USE_DEFAULTS_OFF)

	assert.NoError(t, err)
	assert.Len(t, vectors, 1)
	vector := vectors[0]
	assert.Equal(t, "testView__f1", vector.Name)
	assert.Len(t, vector.RangeStatuses, numRows)
	assert.Len(t, vector.RangeTimestamps, numRows)
	assert.Len(t, vector.RangeStatuses[0], 2)
	assert.Len(t, vector.RangeTimestamps[0], 2)
	assert.Equal(t, serving.FieldStatus_PRESENT, vector.RangeStatuses[0][0])
	assert.Len(t, vector.RangeStatuses[1], 1)
	assert.Len(t, vector.RangeTimestamps[1], 1)
	assert.Equal(t, serving.FieldStatus_PRESENT, vector.RangeStatuses[1][0])
	assert.NotNil(t, vector.RangeValues)
	return vector
}

func TestApplyRangeDefaults(t *testing.T) {
	arrowAllocator := memory.NewGoAllocator()

	// Test cases for range value defaulting
	testCases := []struct {
		name            string
		useDefaults     serving.UseDefaultsMode
		hasDefault      bool
		defaultValue    *types.Value
		values          []interface{}
		statuses        []serving.FieldStatus
		expectedValues  []interface{}
		expectedStatuses []serving.FieldStatus
		expectError     bool
		errorContains   string
		entityNotFound  bool  // if true, simulate entity-not-found with nil Values
	}{
		{
			name:             "OFF mode with NOT_FOUND value",
			useDefaults:      serving.UseDefaultsMode_USE_DEFAULTS_OFF,
			hasDefault:       true,
			defaultValue:     &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 42.0}},
			values:           []interface{}{nil},
			statuses:         []serving.FieldStatus{serving.FieldStatus_NOT_FOUND},
			expectedValues:   []interface{}{nil},
			expectedStatuses: []serving.FieldStatus{serving.FieldStatus_NOT_FOUND},
		},
		{
			name:             "OFF mode with NULL_VALUE value",
			useDefaults:      serving.UseDefaultsMode_USE_DEFAULTS_OFF,
			hasDefault:       true,
			defaultValue:     &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 42.0}},
			values:           []interface{}{nil},
			statuses:         []serving.FieldStatus{serving.FieldStatus_NULL_VALUE},
			expectedValues:   []interface{}{nil},
			expectedStatuses: []serving.FieldStatus{serving.FieldStatus_NULL_VALUE},
		},
		{
			name:             "FLEXIBLE mode with NOT_FOUND and default exists",
			useDefaults:      serving.UseDefaultsMode_USE_DEFAULTS_FLEXIBLE,
			hasDefault:       true,
			defaultValue:     &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 42.0}},
			values:           []interface{}{nil},
			statuses:         []serving.FieldStatus{serving.FieldStatus_NOT_FOUND},
			expectedValues:   []interface{}{42.0},
			expectedStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT},
		},
		{
			name:             "FLEXIBLE mode with NULL_VALUE and default exists",
			useDefaults:      serving.UseDefaultsMode_USE_DEFAULTS_FLEXIBLE,
			hasDefault:       true,
			defaultValue:     &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 42.0}},
			values:           []interface{}{nil},
			statuses:         []serving.FieldStatus{serving.FieldStatus_NULL_VALUE},
			expectedValues:   []interface{}{42.0},
			expectedStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT},
		},
		{
			name:             "FLEXIBLE mode with NOT_FOUND and no default",
			useDefaults:      serving.UseDefaultsMode_USE_DEFAULTS_FLEXIBLE,
			hasDefault:       false,
			defaultValue:     nil,
			values:           []interface{}{nil},
			statuses:         []serving.FieldStatus{serving.FieldStatus_NOT_FOUND},
			expectedValues:   []interface{}{nil},
			expectedStatuses: []serving.FieldStatus{serving.FieldStatus_NOT_FOUND},
		},
		{
			name:             "FLEXIBLE mode with NULL_VALUE and no default",
			useDefaults:      serving.UseDefaultsMode_USE_DEFAULTS_FLEXIBLE,
			hasDefault:       false,
			defaultValue:     nil,
			values:           []interface{}{nil},
			statuses:         []serving.FieldStatus{serving.FieldStatus_NULL_VALUE},
			expectedValues:   []interface{}{nil},
			expectedStatuses: []serving.FieldStatus{serving.FieldStatus_NULL_VALUE},
		},
		{
			name:             "FLEXIBLE mode with PRESENT value",
			useDefaults:      serving.UseDefaultsMode_USE_DEFAULTS_FLEXIBLE,
			hasDefault:       true,
			defaultValue:     &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 42.0}},
			values:           []interface{}{99.9},
			statuses:         []serving.FieldStatus{serving.FieldStatus_PRESENT},
			expectedValues:   []interface{}{99.9},
			expectedStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT},
		},
		{
			name:             "FLEXIBLE mode with OUTSIDE_MAX_AGE",
			useDefaults:      serving.UseDefaultsMode_USE_DEFAULTS_FLEXIBLE,
			hasDefault:       true,
			defaultValue:     &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 42.0}},
			values:           []interface{}{99.9},
			statuses:         []serving.FieldStatus{serving.FieldStatus_OUTSIDE_MAX_AGE},
			expectedValues:   []interface{}{99.9},
			expectedStatuses: []serving.FieldStatus{serving.FieldStatus_OUTSIDE_MAX_AGE},
		},
		{
			name:             "UNSPECIFIED mode behaves like OFF",
			useDefaults:      serving.UseDefaultsMode_USE_DEFAULTS_UNSPECIFIED,
			hasDefault:       true,
			defaultValue:     &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 42.0}},
			values:           []interface{}{nil},
			statuses:         []serving.FieldStatus{serving.FieldStatus_NOT_FOUND},
			expectedValues:   []interface{}{nil},
			expectedStatuses: []serving.FieldStatus{serving.FieldStatus_NOT_FOUND},
		},
		// STRICT mode test cases
		{
			name:             "STRICT mode with NOT_FOUND and default exists",
			useDefaults:      serving.UseDefaultsMode_USE_DEFAULTS_STRICT,
			hasDefault:       true,
			defaultValue:     &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 42.0}},
			values:           []interface{}{nil},
			statuses:         []serving.FieldStatus{serving.FieldStatus_NOT_FOUND},
			expectedValues:   []interface{}{42.0},
			expectedStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT},
		},
		{
			name:             "STRICT mode with NULL_VALUE and default exists",
			useDefaults:      serving.UseDefaultsMode_USE_DEFAULTS_STRICT,
			hasDefault:       true,
			defaultValue:     &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 42.0}},
			values:           []interface{}{nil},
			statuses:         []serving.FieldStatus{serving.FieldStatus_NULL_VALUE},
			expectedValues:   []interface{}{42.0},
			expectedStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT},
		},
		{
			name:          "STRICT mode with NOT_FOUND and no default",
			useDefaults:   serving.UseDefaultsMode_USE_DEFAULTS_STRICT,
			hasDefault:    false,
			defaultValue:  nil,
			values:        []interface{}{nil},
			statuses:      []serving.FieldStatus{serving.FieldStatus_NOT_FOUND},
			expectError:   true,
			errorContains: "no default defined",
		},
		{
			name:          "STRICT mode with NULL_VALUE and no default",
			useDefaults:   serving.UseDefaultsMode_USE_DEFAULTS_STRICT,
			hasDefault:    false,
			defaultValue:  nil,
			values:        []interface{}{nil},
			statuses:      []serving.FieldStatus{serving.FieldStatus_NULL_VALUE},
			expectError:   true,
			errorContains: "no default defined",
		},
		{
			name:             "STRICT mode with PRESENT value",
			useDefaults:      serving.UseDefaultsMode_USE_DEFAULTS_STRICT,
			hasDefault:       true,
			defaultValue:     &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 42.0}},
			values:           []interface{}{99.9},
			statuses:         []serving.FieldStatus{serving.FieldStatus_PRESENT},
			expectedValues:   []interface{}{99.9},
			expectedStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT},
		},
		{
			name:             "STRICT mode with OUTSIDE_MAX_AGE",
			useDefaults:      serving.UseDefaultsMode_USE_DEFAULTS_STRICT,
			hasDefault:       true,
			defaultValue:     &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 42.0}},
			values:           []interface{}{99.9},
			statuses:         []serving.FieldStatus{serving.FieldStatus_OUTSIDE_MAX_AGE},
			expectedValues:   []interface{}{99.9},
			expectedStatuses: []serving.FieldStatus{serving.FieldStatus_OUTSIDE_MAX_AGE},
		},
		{
			name:             "STRICT mode entity-not-found with default exists",
			useDefaults:      serving.UseDefaultsMode_USE_DEFAULTS_STRICT,
			hasDefault:       true,
			defaultValue:     &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 42.0}},
			entityNotFound:   true,
			expectedValues:   []interface{}{42.0},
			expectedStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT},
		},
		{
			name:           "STRICT mode entity-not-found with no default",
			useDefaults:    serving.UseDefaultsMode_USE_DEFAULTS_STRICT,
			hasDefault:     false,
			defaultValue:   nil,
			entityNotFound: true,
			expectError:    true,
			errorContains:  "no default defined",
		},
	}

	for _, tc := range testCases {
		for _, useArrow := range []bool{true, false} {
			testName := tc.name
			if useArrow {
				testName += " (Arrow)"
			} else {
				testName += " (Proto)"
			}

			t.Run(testName, func(t *testing.T) {
				numRows := 1

				// Create sorted feature view with or without default
				sortKey1 := test.CreateSortKeyProto("timestamp", core.SortOrder_DESC, types.ValueType_UNIX_TIMESTAMP)
				entity1 := test.CreateEntityProto("driver", types.ValueType_INT64, "driver")

				var feature *core.FeatureSpecV2
				if tc.hasDefault {
					feature = test.CreateFeatureWithDefault("f1", types.ValueType_DOUBLE, tc.defaultValue)
				} else {
					feature = test.CreateFeature("f1", types.ValueType_DOUBLE)
				}

				sfv := test.CreateSortedFeatureViewModel("testView", []*core.Entity{entity1}, []*core.SortKey{sortKey1}, feature)

				sortedViews := []*SortedFeatureViewAndRefs{
					{View: sfv, FeatureRefs: []string{"f1"}},
				}

				groupRef := &model.GroupedRangeFeatureRefs{
					FeatureNames:        []string{"f1"},
					FeatureViewNames:    []string{"testView"},
					AliasedFeatureNames: []string{"testView__f1"},
					Indices:             [][]int{{0}},
				}

				nowTime := time.Now()

				var featureData [][]onlinestore.RangeFeatureData
				if tc.entityNotFound {
					// Simulate entity-not-found by setting Values to nil
					featureData = [][]onlinestore.RangeFeatureData{
						{
							{
								FeatureView:     "testView",
								FeatureName:     "f1",
								Values:          nil,
								Statuses:        nil,
								EventTimestamps: nil,
							},
						},
					}
				} else {
					featureData = [][]onlinestore.RangeFeatureData{
						{
							{
								FeatureView:     "testView",
								FeatureName:     "f1",
								Values:          tc.values,
								Statuses:        tc.statuses,
								EventTimestamps: []timestamppb.Timestamp{{Seconds: nowTime.Unix()}},
							},
						},
					}
				}

				// Call TransposeRangeFeatureRowsIntoColumns with useDefaults parameter
				vectors, err := TransposeRangeFeatureRowsIntoColumns(featureData, groupRef, sortedViews, arrowAllocator, numRows, useArrow, tc.useDefaults)

				// Handle error expectations
				if tc.expectError {
					assert.Error(t, err)
					if tc.errorContains != "" {
						assert.Contains(t, err.Error(), tc.errorContains)
					}
					return
				}

				assert.NoError(t, err)
				assert.Len(t, vectors, 1)

				vector := vectors[0]
				assert.Equal(t, "testView__f1", vector.Name)

				// Verify status
				assert.Len(t, vector.RangeStatuses, numRows)
				assert.Len(t, vector.RangeStatuses[0], 1)
				assert.Equal(t, tc.expectedStatuses[0], vector.RangeStatuses[0][0])

				// Verify value
				protoValues, err := vector.GetProtoValues()
				assert.NoError(t, err)
				assert.Len(t, protoValues, numRows)

				// For range values, we always get a RepeatedValue (not nil)
				// unless there are no values at all (entity not found case)
				assert.NotNil(t, protoValues[0])
				assert.Len(t, protoValues[0].Val, 1)

				if tc.expectedValues[0] == nil {
					// For NOT_FOUND/NULL_VALUE without default, value inside RepeatedValue is nil or empty
					// Arrow and Proto may handle this differently - Arrow may return empty Value, Proto returns nil
					if protoValues[0].Val[0] != nil {
						// Arrow case: empty Value with nil Val
						assert.Nil(t, protoValues[0].Val[0].Val)
					}
					// Proto case: nil Value (already checked by if statement)
				} else {
					// For values with defaults or present values
					assert.NotNil(t, protoValues[0].Val[0])
					expectedDouble := tc.expectedValues[0].(float64)
					actualDouble := protoValues[0].Val[0].GetDoubleVal()
					assert.Equal(t, expectedDouble, actualDouble)
				}

				// Clean up Arrow arrays if using Arrow
				if useArrow && vector.RangeValues != nil {
					if arr, ok := vector.RangeValues.(arrow.Array); ok {
						arr.Release()
					}
				}
			})
		}
	}
}

func TestValidateFeatureRefs(t *testing.T) {
	t.Run("NoCollisions", func(t *testing.T) {
		viewA := &model.FeatureView{
			Base: &model.BaseFeatureView{
				Name: "viewA",
				Projection: &model.FeatureViewProjection{
					NameAlias: "aliasViewA",
				},
			},
		}
		viewB := &model.FeatureView{
			Base: &model.BaseFeatureView{Name: "viewB"},
		}

		requestedFeatures := []*FeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureC", "featureD"}},
		}

		err := ValidateFeatureRefs(requestedFeatures, true)
		assert.NoError(t, err, "No collisions should result in no error")
	})

	t.Run("NoCollisionsWithFullFeatureNames", func(t *testing.T) {
		viewA := &model.FeatureView{
			Base: &model.BaseFeatureView{
				Name: "viewA",
				Projection: &model.FeatureViewProjection{
					NameAlias: "aliasViewA",
				},
			},
		}
		viewB := &model.FeatureView{
			Base: &model.BaseFeatureView{Name: "viewB"},
		}

		requestedFeatures := []*FeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureA", "featureD"}},
		}

		err := ValidateFeatureRefs(requestedFeatures, true)
		assert.NoError(t, err, "Collisions with full feature names should not result in an error")
	})

	t.Run("CollisionsWithoutFullFeatureNames", func(t *testing.T) {
		viewA := &model.FeatureView{
			Base: &model.BaseFeatureView{
				Name: "viewA",
				Projection: &model.FeatureViewProjection{
					NameAlias: "aliasViewA",
				},
			},
		}
		viewB := &model.FeatureView{
			Base: &model.BaseFeatureView{Name: "viewB"},
		}

		requestedFeatures := []*FeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureA", "featureD"}},
		}

		err := ValidateFeatureRefs(requestedFeatures, false)
		assert.Error(t, err, "Collisions without full feature names should result in an error")
		_, errIsStatus := status.FromError(err)
		assert.True(t, errIsStatus, "Collision error should be a grpc status error")
		assert.Contains(t, err.Error(), "featureA", "Error should include the collided feature name")
	})

	t.Run("SingleFeatureNoCollision", func(t *testing.T) {
		viewA := &model.FeatureView{
			Base: &model.BaseFeatureView{Name: "viewA"},
		}

		requestedFeatures := []*FeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA"}},
		}

		err := ValidateFeatureRefs(requestedFeatures, true)
		assert.NoError(t, err, "Single feature with no collision should not result in an error")
	})

	t.Run("EmptyFeatureRefs", func(t *testing.T) {
		viewA := &model.FeatureView{
			Base: &model.BaseFeatureView{Name: "viewA"},
		}

		requestedFeatures := []*FeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{}},
		}

		err := ValidateFeatureRefs(requestedFeatures, true)
		assert.NoError(t, err, "Empty feature references should not result in an error")
	})

	t.Run("MultipleCollisions", func(t *testing.T) {
		viewA := &model.FeatureView{
			Base: &model.BaseFeatureView{Name: "viewA"},
		}
		viewB := &model.FeatureView{
			Base: &model.BaseFeatureView{Name: "viewB"},
		}

		requestedFeatures := []*FeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureA", "featureB"}},
		}

		err := ValidateFeatureRefs(requestedFeatures, false)
		assert.Error(t, err, "Multiple collisions should result in an error")
		_, errIsStatus := status.FromError(err)
		assert.True(t, errIsStatus, "Collision error should be a grpc status error")
		assert.Contains(t, err.Error(), "featureA", "Error should include the collided feature name")
		assert.Contains(t, err.Error(), "featureB", "Error should include the collided feature name")
	})
}
func TestValidateSortedFeatureRefs(t *testing.T) {
	t.Run("NoCollisions", func(t *testing.T) {
		viewA := &model.SortedFeatureView{
			FeatureView: &model.FeatureView{
				Base: &model.BaseFeatureView{
					Name: "viewA",
					Projection: &model.FeatureViewProjection{
						NameAlias: "aliasViewA",
					},
				},
			},
		}
		viewB := &model.SortedFeatureView{
			FeatureView: &model.FeatureView{
				Base: &model.BaseFeatureView{Name: "viewB"},
			},
		}

		sortedViews := []*SortedFeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureC", "featureD"}},
		}

		err := ValidateSortedFeatureRefs(sortedViews, true)
		assert.NoError(t, err, "No collisions should result in no error")
	})

	t.Run("NoCollisionsWithFullFeatureNames", func(t *testing.T) {
		viewA := &model.SortedFeatureView{
			FeatureView: &model.FeatureView{
				Base: &model.BaseFeatureView{
					Name: "viewA",
					Projection: &model.FeatureViewProjection{
						NameAlias: "aliasViewA",
					},
				},
			},
		}
		viewB := &model.SortedFeatureView{
			FeatureView: &model.FeatureView{
				Base: &model.BaseFeatureView{Name: "viewB"},
			},
		}

		sortedViews := []*SortedFeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureA", "featureD"}},
		}

		err := ValidateSortedFeatureRefs(sortedViews, true)
		assert.NoError(t, err, "Collisions with full feature names should not result in an error")
	})

	t.Run("CollisionsWithoutFullFeatureNames", func(t *testing.T) {
		viewA := &model.SortedFeatureView{
			FeatureView: &model.FeatureView{
				Base: &model.BaseFeatureView{
					Name: "viewA",
					Projection: &model.FeatureViewProjection{
						NameAlias: "aliasViewA",
					},
				},
			},
		}
		viewB := &model.SortedFeatureView{
			FeatureView: &model.FeatureView{
				Base: &model.BaseFeatureView{Name: "viewB"},
			},
		}

		sortedViews := []*SortedFeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureA", "featureD"}},
		}

		err := ValidateSortedFeatureRefs(sortedViews, false)
		assert.Error(t, err, "Collisions without full feature names should result in an error")
		_, errIsStatus := status.FromError(err)
		assert.True(t, errIsStatus, "Collision error should be a grpc status error")
		assert.Contains(t, err.Error(), "featureA", "Error should include the collided feature name")
	})

	t.Run("SingleFeatureNoCollision", func(t *testing.T) {
		viewA := &model.SortedFeatureView{
			FeatureView: &model.FeatureView{
				Base: &model.BaseFeatureView{Name: "viewA"},
			},
		}

		sortedViews := []*SortedFeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA"}},
		}

		err := ValidateSortedFeatureRefs(sortedViews, true)
		assert.NoError(t, err, "Single feature with no collision should not result in an error")
	})

	t.Run("EmptyFeatureRefs", func(t *testing.T) {
		viewA := &model.SortedFeatureView{
			FeatureView: &model.FeatureView{
				Base: &model.BaseFeatureView{Name: "viewA"},
			},
		}

		sortedViews := []*SortedFeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{}},
		}

		err := ValidateSortedFeatureRefs(sortedViews, true)
		assert.NoError(t, err, "Empty feature references should not result in an error")
	})

	t.Run("MultipleCollisions", func(t *testing.T) {
		viewA := &model.SortedFeatureView{
			FeatureView: &model.FeatureView{
				Base: &model.BaseFeatureView{Name: "viewA"},
			},
		}
		viewB := &model.SortedFeatureView{
			FeatureView: &model.FeatureView{
				Base: &model.BaseFeatureView{Name: "viewB"},
			},
		}

		sortedViews := []*SortedFeatureViewAndRefs{
			{View: viewA, FeatureRefs: []string{"featureA", "featureB"}},
			{View: viewB, FeatureRefs: []string{"featureA", "featureB"}},
		}

		err := ValidateSortedFeatureRefs(sortedViews, false)
		assert.Error(t, err, "Multiple collisions should result in an error")
		_, errIsStatus := status.FromError(err)
		assert.True(t, errIsStatus, "Collision error should be a grpc status error")
		assert.Contains(t, err.Error(), "featureA", "Error should include the collided feature name")
		assert.Contains(t, err.Error(), "featureB", "Error should include the collided feature name")
	})
}
func BenchmarkValidateFeatureRefs(b *testing.B) {
	// Prepare mock data for the benchmark
	requestedFeatures := generateMockFeatureViewAndRefs(10, 100)
	fullFeatureNames := true

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := ValidateFeatureRefs(requestedFeatures, fullFeatureNames)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

// Helper function to generate mock FeatureViewAndRefs
func generateMockFeatureViewAndRefs(numViews, numFeatures int) []*FeatureViewAndRefs {
	featureViews := make([]*FeatureViewAndRefs, numViews)
	for i := 0; i < numViews; i++ {
		features := make([]string, numFeatures)
		for j := 0; j < numFeatures; j++ {
			features[j] = fmt.Sprintf("feature_%d", j)
		}
		featureViews[i] = &FeatureViewAndRefs{
			View: &model.FeatureView{
				Base: &model.BaseFeatureView{
					Name: fmt.Sprintf("view_%d", i),
				},
			},
			FeatureRefs: features,
		}
	}
	return featureViews
}

func BenchmarkTransposeFeatureRowsIntoColumnsWithArrowConversion(b *testing.B) {
	numRows, featureData2D, groupRef, requestedFeatureViews, arrowAllocator := mockDataForBenchmarkTransposeFeatureRowsIntoColumns()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := TransposeFeatureRowsIntoColumns(featureData2D, groupRef, requestedFeatureViews, arrowAllocator, numRows, true, serving.UseDefaultsMode_USE_DEFAULTS_OFF)
		if err != nil {
			b.Fatalf("Error during TransposeFeatureRowsIntoColumns: %v", err)
		}
	}
}

func BenchmarkTransposeFeatureRowsIntoColumnsWithoutArrowConversion(b *testing.B) {
	numRows, featureData2D, groupRef, requestedFeatureViews, arrowAllocator := mockDataForBenchmarkTransposeFeatureRowsIntoColumns()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := TransposeFeatureRowsIntoColumns(featureData2D, groupRef, requestedFeatureViews, arrowAllocator, numRows, false, serving.UseDefaultsMode_USE_DEFAULTS_OFF)
		if err != nil {
			b.Fatalf("Error during TransposeFeatureRowsIntoColumns: %v", err)
		}
	}
}

func BenchmarkFullLoopArrowConversion(b *testing.B) {
	numRows, featureData2D, groupRef, requestedFeatureViews, arrowAllocator := mockDataForBenchmarkTransposeFeatureRowsIntoColumns()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vectors, err := TransposeFeatureRowsIntoColumns(featureData2D, groupRef, requestedFeatureViews, arrowAllocator, numRows, true, serving.UseDefaultsMode_USE_DEFAULTS_OFF)
		if err != nil {
			b.Fatalf("Error during TransposeFeatureRowsIntoColumns: %v", err)
		}
		for _, v := range vectors {
			_, e := types2.ArrowValuesToProtoValues(v.Values.(arrow.Array))
			if e != nil {
				b.Fatalf("Error during ArrowValuesToProtoValues for %s: %v", v.Name, e)
			}
		}
	}
}

func mockDataForBenchmarkTransposeFeatureRowsIntoColumns() (int, [][]onlinestore.FeatureData, *GroupedFeaturesPerEntitySet, []*FeatureViewAndRefs, *memory.GoAllocator) {
	// Mock Data
	numRows := 1000
	numFeatures := 100

	featureData2D := make([][]onlinestore.FeatureData, numRows)
	for i := 0; i < numRows; i++ {
		featureData2D[i] = make([]onlinestore.FeatureData, numFeatures)
		for j := 0; j < numFeatures; j++ {
			featureData2D[i][j] = onlinestore.FeatureData{
				Value: types.Value{Val: &types.Value_Int64Val{Int64Val: int64(i * j)}},
				Timestamp: timestamppb.Timestamp{
					Seconds: int64(i * j),
				},
				Reference: serving.FeatureReferenceV2{
					FeatureViewName: "feature_view",
					FeatureName:     "feature_" + strconv.Itoa(j),
				},
			}
		}
	}

	groupRef := &GroupedFeaturesPerEntitySet{
		AliasedFeatureNames: make([]string, numFeatures),
		Indices:             make([][]int, numRows),
	}
	for i := 0; i < numFeatures; i++ {
		groupRef.AliasedFeatureNames[i] = "feature_" + strconv.Itoa(i)
	}
	for i := 0; i < numRows; i++ {
		groupRef.Indices[i] = []int{i}
	}

	requestedFeatureViews := []*FeatureViewAndRefs{
		{
			View: &model.FeatureView{
				Base: &model.BaseFeatureView{
					Name: "feature_view",
				},
				Ttl: &durationpb.Duration{Seconds: 0, Nanos: 0},
			},
		},
	}

	arrowAllocator := memory.NewGoAllocator()
	return numRows, featureData2D, groupRef, requestedFeatureViews, arrowAllocator
}

func TestBatchGroupedFeatureRef_VariableBatchSizes(t *testing.T) {
	groupRef := &GroupedFeaturesPerEntitySet{
		FeatureNames:        []string{"f1", "f2"},
		FeatureViewNames:    []string{"v1", "v2"},
		AliasedFeatureNames: []string{"v1__f1", "v2__f2"},
		Indices:             [][]int{{0}, {1}, {2, 3}, {4}},
		EntityKeys: []*types.EntityKey{
			{
				JoinKeys:     []string{"customer_id", "driver_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 1}}, {Val: &types.Value_Int32Val{Int32Val: 0}}},
			},
			{
				JoinKeys:     []string{"customer_id", "driver_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 2}}, {Val: &types.Value_Int32Val{Int32Val: 0}}},
			},
			{
				JoinKeys:     []string{"customer_id", "driver_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 3}}, {Val: &types.Value_Int32Val{Int32Val: 1}}},
			},
			{
				JoinKeys:     []string{"customer_id", "driver_id"},
				EntityValues: []*types.Value{{Val: &types.Value_Int32Val{Int32Val: 4}}, {Val: &types.Value_Int32Val{Int32Val: 1}}},
			},
		},
	}

	t.Run("BatchSizeOne", func(t *testing.T) {
		batches := BatchGroupedFeatureRef(groupRef, 1)
		assert.Len(t, batches, 4)
		for i, batch := range batches {
			assert.Len(t, batch.EntityKeys, 1)
			assert.Equal(t, i, batch.StartIndex)
			assert.True(t, proto.Equal(batch.EntityKeys[0], groupRef.EntityKeys[i]))
		}
	})

	t.Run("BatchSizeTwo", func(t *testing.T) {
		batches := BatchGroupedFeatureRef(groupRef, 2)
		assert.Len(t, batches, 2)
		assert.Equal(t, 0, batches[0].StartIndex)
		assert.Equal(t, 2, batches[1].StartIndex)
		assert.Len(t, batches[0].EntityKeys, 2)
		assert.Len(t, batches[1].EntityKeys, 2)
		for i := 0; i < 2; i++ {
			assert.True(t, proto.Equal(batches[0].EntityKeys[i], groupRef.EntityKeys[i]))
			assert.True(t, proto.Equal(batches[1].EntityKeys[i], groupRef.EntityKeys[i+2]))
		}
	})

	t.Run("BatchSizeThree", func(t *testing.T) {
		batches := BatchGroupedFeatureRef(groupRef, 3)
		assert.Len(t, batches, 2)
		assert.Equal(t, 0, batches[0].StartIndex)
		assert.Equal(t, 3, batches[1].StartIndex)
		assert.Len(t, batches[0].EntityKeys, 3)
		assert.Len(t, batches[1].EntityKeys, 1)
		for i := 0; i < 3; i++ {
			assert.True(t, proto.Equal(batches[0].EntityKeys[i], groupRef.EntityKeys[i]))
		}
		assert.True(t, proto.Equal(batches[1].EntityKeys[0], groupRef.EntityKeys[3]))
	})

	t.Run("BatchSizeFour", func(t *testing.T) {
		batches := BatchGroupedFeatureRef(groupRef, 4)
		assert.Len(t, batches, 1)
		assert.Equal(t, 0, batches[0].StartIndex)
		assert.Len(t, batches[0].EntityKeys, 4)
		for i := 0; i < 4; i++ {
			assert.True(t, proto.Equal(batches[0].EntityKeys[i], groupRef.EntityKeys[i]))
		}
	})

	t.Run("BatchSizeGreaterThanEntities", func(t *testing.T) {
		batches := BatchGroupedFeatureRef(groupRef, 10)
		assert.Len(t, batches, 1)
		assert.Equal(t, 0, batches[0].StartIndex)
		assert.Len(t, batches[0].EntityKeys, 4)
		for i := 0; i < 4; i++ {
			assert.True(t, proto.Equal(batches[0].EntityKeys[i], groupRef.EntityKeys[i]))
		}
	})

	t.Run("BatchSizeZero", func(t *testing.T) {
		batches := BatchGroupedFeatureRef(groupRef, 0)
		assert.Len(t, batches, 1)
		assert.Equal(t, 0, batches[0].StartIndex)
		assert.Len(t, batches[0].EntityKeys, 4)
		for i := 0; i < 4; i++ {
			assert.True(t, proto.Equal(batches[0].EntityKeys[i], groupRef.EntityKeys[i]))
		}
	})

	t.Run("BatchSizeNegative", func(t *testing.T) {
		batches := BatchGroupedFeatureRef(groupRef, -5)
		assert.Len(t, batches, 1)
		assert.Equal(t, 0, batches[0].StartIndex)
		assert.Len(t, batches[0].EntityKeys, 4)
		for i := 0; i < 4; i++ {
			assert.True(t, proto.Equal(batches[0].EntityKeys[i], groupRef.EntityKeys[i]))
		}
	})
}

func TestApplyDefaults(t *testing.T) {
	testCases := []struct {
		name             string
		useDefaults      serving.UseDefaultsMode
		hasDefault       bool
		defaultValue     *types.Value
		featureDataNil   bool         // if true, simulate NOT_FOUND via nil row
		featureValue     *types.Value // if nil and featureDataNil=false, use NullVal
		expiredTimestamp bool         // if true, use old timestamp to trigger OUTSIDE_MAX_AGE
		initialStatus    serving.FieldStatus
		expectValue      *types.Value
		expectStatus     serving.FieldStatus
		expectError      bool
		errorContains    string
	}{
		{
			name:           "OFF + NOT_FOUND + has default",
			useDefaults:    serving.UseDefaultsMode_USE_DEFAULTS_OFF,
			hasDefault:     true,
			defaultValue:   &types.Value{Val: &types.Value_Int64Val{Int64Val: 42}},
			featureDataNil: true,
			expectValue:    nil,
			expectStatus:   serving.FieldStatus_NOT_FOUND,
		},
		{
			name:          "OFF + NULL_VALUE + has default",
			useDefaults:   serving.UseDefaultsMode_USE_DEFAULTS_OFF,
			hasDefault:    true,
			defaultValue:  &types.Value{Val: &types.Value_Int64Val{Int64Val: 42}},
			featureValue:  &types.Value{Val: &types.Value_NullVal{}},
			expectValue:   nil,
			expectStatus:  serving.FieldStatus_NOT_FOUND,
		},
		{
			name:           "FLEXIBLE + NOT_FOUND + has default",
			useDefaults:    serving.UseDefaultsMode_USE_DEFAULTS_FLEXIBLE,
			hasDefault:     true,
			defaultValue:   &types.Value{Val: &types.Value_Int64Val{Int64Val: 42}},
			featureDataNil: true,
			expectValue:    &types.Value{Val: &types.Value_Int64Val{Int64Val: 42}},
			expectStatus:   serving.FieldStatus_PRESENT,
		},
		{
			name:         "FLEXIBLE + NULL_VALUE + has default",
			useDefaults:  serving.UseDefaultsMode_USE_DEFAULTS_FLEXIBLE,
			hasDefault:   true,
			defaultValue: &types.Value{Val: &types.Value_Int64Val{Int64Val: 42}},
			featureValue: &types.Value{Val: &types.Value_NullVal{}},
			expectValue:  &types.Value{Val: &types.Value_Int64Val{Int64Val: 42}},
			expectStatus: serving.FieldStatus_PRESENT,
		},
		{
			name:           "FLEXIBLE + NOT_FOUND + no default",
			useDefaults:    serving.UseDefaultsMode_USE_DEFAULTS_FLEXIBLE,
			hasDefault:     false,
			featureDataNil: true,
			expectValue:    nil,
			expectStatus:   serving.FieldStatus_NOT_FOUND,
		},
		{
			name:         "FLEXIBLE + NULL_VALUE + no default",
			useDefaults:  serving.UseDefaultsMode_USE_DEFAULTS_FLEXIBLE,
			hasDefault:   false,
			featureValue: &types.Value{Val: &types.Value_NullVal{}},
			expectValue:  nil,
			expectStatus: serving.FieldStatus_NOT_FOUND,
		},
		{
			name:         "FLEXIBLE + PRESENT value",
			useDefaults:  serving.UseDefaultsMode_USE_DEFAULTS_FLEXIBLE,
			hasDefault:   true,
			defaultValue: &types.Value{Val: &types.Value_Int64Val{Int64Val: 42}},
			featureValue: &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 99.9}},
			expectValue:  &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 99.9}},
			expectStatus: serving.FieldStatus_PRESENT,
		},
		{
			name:             "FLEXIBLE + OUTSIDE_MAX_AGE",
			useDefaults:      serving.UseDefaultsMode_USE_DEFAULTS_FLEXIBLE,
			hasDefault:       true,
			defaultValue:     &types.Value{Val: &types.Value_Int64Val{Int64Val: 42}},
			featureValue:     &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 1.0}},
			expiredTimestamp: true,
			expectValue:      &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 1.0}},
			expectStatus:     serving.FieldStatus_OUTSIDE_MAX_AGE,
		},
		{
			name:           "UNSPECIFIED + NOT_FOUND + has default",
			useDefaults:    serving.UseDefaultsMode_USE_DEFAULTS_UNSPECIFIED,
			hasDefault:     true,
			defaultValue:   &types.Value{Val: &types.Value_Int64Val{Int64Val: 42}},
			featureDataNil: true,
			expectValue:    nil,
			expectStatus:   serving.FieldStatus_NOT_FOUND,
		},
		// STRICT mode test cases
		{
			name:           "STRICT + NOT_FOUND + has default",
			useDefaults:    serving.UseDefaultsMode_USE_DEFAULTS_STRICT,
			hasDefault:     true,
			defaultValue:   &types.Value{Val: &types.Value_Int64Val{Int64Val: 42}},
			featureDataNil: true,
			expectValue:    &types.Value{Val: &types.Value_Int64Val{Int64Val: 42}},
			expectStatus:   serving.FieldStatus_PRESENT,
		},
		{
			name:         "STRICT + NULL_VALUE + has default",
			useDefaults:  serving.UseDefaultsMode_USE_DEFAULTS_STRICT,
			hasDefault:   true,
			defaultValue: &types.Value{Val: &types.Value_Int64Val{Int64Val: 42}},
			featureValue: &types.Value{Val: &types.Value_NullVal{}},
			expectValue:  &types.Value{Val: &types.Value_Int64Val{Int64Val: 42}},
			expectStatus: serving.FieldStatus_PRESENT,
		},
		{
			name:           "STRICT + NOT_FOUND + no default",
			useDefaults:    serving.UseDefaultsMode_USE_DEFAULTS_STRICT,
			hasDefault:     false,
			featureDataNil: true,
			expectError:    true,
			errorContains:  "no default defined",
		},
		{
			name:          "STRICT + NULL_VALUE + no default",
			useDefaults:   serving.UseDefaultsMode_USE_DEFAULTS_STRICT,
			hasDefault:    false,
			featureValue:  &types.Value{Val: &types.Value_NullVal{}},
			expectError:   true,
			errorContains: "no default defined",
		},
		{
			name:         "STRICT + PRESENT value",
			useDefaults:  serving.UseDefaultsMode_USE_DEFAULTS_STRICT,
			hasDefault:   true,
			defaultValue: &types.Value{Val: &types.Value_Int64Val{Int64Val: 42}},
			featureValue: &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 99.9}},
			expectValue:  &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 99.9}},
			expectStatus: serving.FieldStatus_PRESENT,
		},
		{
			name:             "STRICT + OUTSIDE_MAX_AGE",
			useDefaults:      serving.UseDefaultsMode_USE_DEFAULTS_STRICT,
			hasDefault:       true,
			defaultValue:     &types.Value{Val: &types.Value_Int64Val{Int64Val: 42}},
			featureValue:     &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 1.0}},
			expiredTimestamp: true,
			expectValue:      &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 1.0}},
			expectStatus:     serving.FieldStatus_OUTSIDE_MAX_AGE,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test with both Arrow and non-Arrow
			for _, useArrow := range []bool{true, false} {
				testName := fmt.Sprintf("useArrow=%v", useArrow)
				t.Run(testName, func(t *testing.T) {
					arrowAllocator := memory.NewGoAllocator()
					numRows := 1

					// Create entity
					entity1 := test.CreateEntityProto("driver", types.ValueType_INT64, "driver")

					// Create feature with or without default
					var feature *core.FeatureSpecV2
					if tc.hasDefault {
						feature = test.CreateFeatureWithDefault("f1", types.ValueType_INT64, tc.defaultValue)
					} else {
						feature = test.CreateFeature("f1", types.ValueType_INT64)
					}

					fv := test.CreateFeatureViewModel("testView", []*core.Entity{entity1}, feature)
					// Set a TTL for OUTSIDE_MAX_AGE test
					fv.Ttl = &durationpb.Duration{Seconds: 3600} // 1 hour TTL

					featureViews := []*FeatureViewAndRefs{
						{View: fv, FeatureRefs: []string{"f1"}},
					}

					entityKeys := []*types.EntityKey{
						{
							JoinKeys:     []string{"driver"},
							EntityValues: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1}}},
						},
					}

					groupRef := &GroupedFeaturesPerEntitySet{
						FeatureNames:        []string{"f1"},
						FeatureViewNames:    []string{"testView"},
						AliasedFeatureNames: []string{"testView__f1"},
						EntityKeys:          entityKeys,
						Indices:             [][]int{{0}},
					}

					nowTime := time.Now()
					// Use old timestamp for OUTSIDE_MAX_AGE test (2 hours old, TTL is 1 hour)
					timestampToUse := nowTime
					if tc.expiredTimestamp {
						timestampToUse = nowTime.Add(-2 * time.Hour)
					}
					var featureData [][]onlinestore.FeatureData

					if tc.featureDataNil {
						// Simulate NOT_FOUND by nil row
						featureData = [][]onlinestore.FeatureData{nil}
					} else if tc.featureValue == nil || tc.featureValue.Val == nil {
						// Create feature data with NULL value
						featureData = [][]onlinestore.FeatureData{
							{
								{
									Reference: serving.FeatureReferenceV2{
										FeatureViewName: "testView",
										FeatureName:     "f1",
									},
									Timestamp: timestamppb.Timestamp{Seconds: timestampToUse.Unix()},
									Value:     types.Value{Val: &types.Value_NullVal{}},
								},
							},
						}
					} else {
						// Create feature data with the specified value
						featureData = [][]onlinestore.FeatureData{
							{
								{
									Reference: serving.FeatureReferenceV2{
										FeatureViewName: "testView",
										FeatureName:     "f1",
									},
									Timestamp: timestamppb.Timestamp{Seconds: timestampToUse.Unix()},
									Value:     types.Value{Val: tc.featureValue.Val},
								},
							},
						}
					}

					// Call TransposeFeatureRowsIntoColumns with useDefaults parameter
					vectors, err := TransposeFeatureRowsIntoColumns(featureData, groupRef, featureViews, arrowAllocator, numRows, useArrow, tc.useDefaults)

					// Handle error expectations
					if tc.expectError {
						assert.Error(t, err)
						if tc.errorContains != "" {
							assert.Contains(t, err.Error(), tc.errorContains)
						}
						return
					}

					assert.NoError(t, err)
					assert.Len(t, vectors, 1)
					vector := vectors[0]

					// Get proto values for comparison
					protoValues, err := vector.GetProtoValues()
					assert.NoError(t, err)

					if useArrow {
						vector.Values.(arrow.Array).Release()
					}

					// Check status
					assert.Equal(t, tc.expectStatus, vector.Statuses[0], "Status mismatch")

					// Check value
					if tc.expectValue == nil {
						// Arrow conversion creates empty Value objects for nil, non-Arrow returns nil
						if useArrow {
							assert.NotNil(t, protoValues[0], "Arrow should create non-nil Value")
							assert.Nil(t, protoValues[0].Val, "Arrow Value.Val should be nil")
						} else {
							assert.Nil(t, protoValues[0], "Non-Arrow should return nil value")
						}
					} else {
						assert.NotNil(t, protoValues[0], "Expected non-nil value")
						assert.True(t, proto.Equal(tc.expectValue, protoValues[0]), "Value mismatch")
					}
				})
			}
		})
	}
}

func TestFieldDefaultValueLoadedFromProto(t *testing.T) {
	// Create a FeatureSpecV2 proto with a default value
	defaultVal := &types.Value{Val: &types.Value_Int64Val{Int64Val: 42}}
	featureProto := test.CreateFeatureWithDefault("feature_with_default", types.ValueType_INT64, defaultVal)

	// Create a FeatureView from proto (this is how Feature Server loads metadata)
	entity := test.CreateEntityProto("driver", types.ValueType_INT64, "driver")
	fvProto := test.CreateFeatureViewProto("test_fv", []*core.Entity{entity}, featureProto)
	fv := model.NewFeatureViewFromProto(fvProto)

	// Verify the default value is loaded into the in-memory model
	require.Len(t, fv.Base.Features, 1)
	field := fv.Base.Features[0]
	assert.Equal(t, "feature_with_default", field.Name)
	require.NotNil(t, field.DefaultValue, "DefaultValue must be loaded from proto")
	assert.Equal(t, int64(42), field.DefaultValue.GetInt64Val())

	// Also verify a feature WITHOUT default has nil DefaultValue
	featureNoDefault := test.CreateFeature("feature_no_default", types.ValueType_INT64)
	fvProto2 := test.CreateFeatureViewProto("test_fv2", []*core.Entity{entity}, featureNoDefault)
	fv2 := model.NewFeatureViewFromProto(fvProto2)
	require.Len(t, fv2.Base.Features, 1)
	assert.Nil(t, fv2.Base.Features[0].DefaultValue, "Feature without proto default must have nil DefaultValue")
}
