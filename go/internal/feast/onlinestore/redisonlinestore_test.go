//go:build !integration

package onlinestore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/internal/feast/utils"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNewRedisOnlineStore(t *testing.T) {
	var config = map[string]interface{}{
		"connection_string": "redis://localhost:6379",
	}
	rc := &registry.RepoConfig{
		OnlineStore:                   config,
		EntityKeySerializationVersion: 3,
	}
	store, err := NewRedisOnlineStore("test", rc, config)
	assert.Nil(t, err)
	var opts = store.client.Options()
	assert.Equal(t, opts.Addr, "redis://localhost:6379")
	assert.Equal(t, opts.Password, "")
	assert.Equal(t, opts.DB, 0)
	assert.Nil(t, opts.TLSConfig)
}

func TestNewRedisOnlineStoreWithPassword(t *testing.T) {
	var config = map[string]interface{}{
		"connection_string": "redis://localhost:6379,password=secret",
	}
	rc := &registry.RepoConfig{
		OnlineStore:                   config,
		EntityKeySerializationVersion: 3,
	}
	store, err := NewRedisOnlineStore("test", rc, config)
	assert.Nil(t, err)
	var opts = store.client.Options()
	assert.Equal(t, opts.Addr, "redis://localhost:6379")
	assert.Equal(t, opts.Password, "secret")
}

func TestNewRedisOnlineStoreWithDB(t *testing.T) {
	var config = map[string]interface{}{
		"connection_string": "redis://localhost:6379,db=1",
	}
	rc := &registry.RepoConfig{
		OnlineStore:                   config,
		EntityKeySerializationVersion: 3,
	}
	store, err := NewRedisOnlineStore("test", rc, config)
	assert.Nil(t, err)
	var opts = store.client.Options()
	assert.Equal(t, opts.Addr, "redis://localhost:6379")
	assert.Equal(t, opts.DB, 1)
}

func TestNewRedisOnlineStoreWithSsl(t *testing.T) {
	var config = map[string]interface{}{
		"connection_string": "redis://localhost:6379,ssl=true",
	}
	rc := &registry.RepoConfig{
		OnlineStore:                   config,
		EntityKeySerializationVersion: 3,
	}
	store, err := NewRedisOnlineStore("test", rc, config)
	assert.Nil(t, err)
	var opts = store.client.Options()
	assert.Equal(t, opts.Addr, "redis://localhost:6379")
	assert.NotNil(t, opts.TLSConfig)
}

func TestBuildFeatureViewIndices(t *testing.T) {
	r := &RedisOnlineStore{}

	t.Run("test with empty featureViewNames and featureNames", func(t *testing.T) {
		featureViewIndices, indicesFeatureView, index := r.buildFeatureViewIndices([]string{}, []string{})
		assert.Equal(t, 0, len(featureViewIndices))
		assert.Equal(t, 0, len(indicesFeatureView))
		assert.Equal(t, 0, index)
	})

	t.Run("test with non-empty featureNames and empty featureViewNames", func(t *testing.T) {
		featureViewIndices, indicesFeatureView, index := r.buildFeatureViewIndices([]string{}, []string{"feature1", "feature2"})
		assert.Equal(t, 0, len(featureViewIndices))
		assert.Equal(t, 0, len(indicesFeatureView))
		assert.Equal(t, 2, index)
	})

	t.Run("test with non-empty featureViewNames and featureNames", func(t *testing.T) {
		featureViewIndices, indicesFeatureView, index := r.buildFeatureViewIndices([]string{"view1", "view2"}, []string{"feature1", "feature2"})
		assert.Equal(t, 2, len(featureViewIndices))
		assert.Equal(t, 2, len(indicesFeatureView))
		assert.Equal(t, 4, index)
		assert.Equal(t, "view1", indicesFeatureView[2])
		assert.Equal(t, "view2", indicesFeatureView[3])
	})

	t.Run("test with duplicate featureViewNames", func(t *testing.T) {
		featureViewIndices, indicesFeatureView, index := r.buildFeatureViewIndices([]string{"view1", "view1"}, []string{"feature1", "feature2"})
		assert.Equal(t, 1, len(featureViewIndices))
		assert.Equal(t, 1, len(indicesFeatureView))
		assert.Equal(t, 3, index)
		assert.Equal(t, "view1", indicesFeatureView[2])
	})
}

func TestBuildHsetKeys(t *testing.T) {
	r := &RedisOnlineStore{}

	t.Run("test with empty featureViewNames and featureNames", func(t *testing.T) {
		hsetKeys, featureNames := r.buildRedisHashSetKeys([]string{}, []string{}, map[int]string{}, 0)
		assert.Equal(t, 0, len(hsetKeys))
		assert.Equal(t, 0, len(featureNames))
	})

	t.Run("test with non-empty featureViewNames and featureNames", func(t *testing.T) {
		hsetKeys, featureNames := r.buildRedisHashSetKeys([]string{"view1", "view2"}, []string{"feature1", "feature2"}, map[int]string{2: "view1", 3: "view2"}, 4)
		assert.Equal(t, 4, len(hsetKeys))
		assert.Equal(t, 4, len(featureNames))
		assert.Equal(t, "_ts:view1", hsetKeys[2])
		assert.Equal(t, "_ts:view2", hsetKeys[3])
		assert.Contains(t, featureNames, "_ts:view1")
		assert.Contains(t, featureNames, "_ts:view2")
	})

	t.Run("test with more featureViewNames than featureNames", func(t *testing.T) {
		hsetKeys, featureNames := r.buildRedisHashSetKeys([]string{"view1", "view2", "view3"}, []string{"feature1", "feature2", "feature3"}, map[int]string{3: "view1", 4: "view2", 5: "view3"}, 6)
		assert.Equal(t, 6, len(hsetKeys))
		assert.Equal(t, 6, len(featureNames))
		assert.Equal(t, "_ts:view1", hsetKeys[3])
		assert.Equal(t, "_ts:view2", hsetKeys[4])
		assert.Equal(t, "_ts:view3", hsetKeys[5])
		assert.Contains(t, featureNames, "_ts:view1")
		assert.Contains(t, featureNames, "_ts:view2")
		assert.Contains(t, featureNames, "_ts:view3")
	})
}

func TestBuildRedisKeys(t *testing.T) {
	r := &RedisOnlineStore{
		project: "test_project",
		config: &registry.RepoConfig{
			EntityKeySerializationVersion: 3,
		},
	}

	entity_key1 := types.EntityKey{
		JoinKeys:     []string{"driver_id"},
		EntityValues: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1005}}},
	}

	entity_key2 := types.EntityKey{
		JoinKeys:     []string{"driver_id"},
		EntityValues: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1001}}},
	}

	error_entity_key1 := types.EntityKey{
		JoinKeys:     []string{"driver_id", "vehicle_id"},
		EntityValues: []*types.Value{{Val: &types.Value_Int64Val{Int64Val: 1005}}},
	}

	t.Run("test with empty entityKeys", func(t *testing.T) {
		redisKeys, redisKeyToEntityIndex, err := r.buildRedisKeys([]*types.EntityKey{})
		assert.Nil(t, err)
		assert.Equal(t, 0, len(redisKeys))
		assert.Equal(t, 0, len(redisKeyToEntityIndex))
	})

	t.Run("test with single entityKey", func(t *testing.T) {
		entityKeys := []*types.EntityKey{&entity_key1}
		redisKeys, redisKeyToEntityIndex, err := r.buildRedisKeys(entityKeys)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(redisKeys))
		assert.Equal(t, 1, len(redisKeyToEntityIndex))
	})

	t.Run("test with multiple entityKeys", func(t *testing.T) {
		entityKeys := []*types.EntityKey{
			&entity_key1, &entity_key2,
		}
		redisKeys, redisKeyToEntityIndex, err := r.buildRedisKeys(entityKeys)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(redisKeys))
		assert.Equal(t, 2, len(redisKeyToEntityIndex))
	})

	t.Run("test with error in buildRedisKey", func(t *testing.T) {
		entityKeys := []*types.EntityKey{&error_entity_key1}
		_, _, err := r.buildRedisKeys(entityKeys)
		assert.NotNil(t, err)
	})
}

func newMockRedisStore(t *testing.T) (*RedisOnlineStore, *miniredis.Miniredis) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	store := &RedisOnlineStore{
		project: "test_project",
		t:       redisNode,
		client:  client,
		config:  &registry.RepoConfig{EntityKeySerializationVersion: 3},
	}

	return store, mr
}

func TestOnlineReadRange(t *testing.T) {
	ctx := context.Background()
	store, _ := newMockRedisStore(t)

	entityKey := &types.EntityKey{
		JoinKeys:     []string{"driver_id"},
		EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: "1006"}}},
	}
	serKey, _ := utils.SerializeEntityKey(entityKey, 3)
	entityKeyBin := string(*serKey)

	view := "driver_features"
	sortKey := "event_timestamp"
	feat := "trip_completed"
	zkey := fmt.Sprintf("%s:%s:%s:%s", store.project, view, sortKey, entityKeyBin)

	t1 := time.Unix(1738699283, 0)
	t2 := time.Unix(1738699290, 0)
	t3 := time.Unix(1738699300, 0)
	timestamps := []*timestamppb.Timestamp{timestamppb.New(t1), timestamppb.New(t2), timestamppb.New(t3)}

	hsetKeys := []string{
		fmt.Sprintf("%s:%s:%s:1", store.project, view, entityKeyBin),
		fmt.Sprintf("%s:%s:%s:2", store.project, view, entityKeyBin),
		fmt.Sprintf("%s:%s:%s:3", store.project, view, entityKeyBin),
	}
	for i, hk := range hsetKeys {
		tsKey := fmt.Sprintf("_ts:%s", view)
		fieldHash := utils.Mmh3(fmt.Sprintf("%s:%s", view, feat))
		tsBytes, _ := proto.Marshal(timestamps[i])
		valBytes, _ := proto.Marshal(&types.Value{Val: &types.Value_StringVal{StringVal: fmt.Sprintf("val%d", i+1)}})
		require.NoError(t, store.client.HSet(ctx, hk, tsKey, tsBytes, fieldHash, valBytes).Err())
		require.NoError(t, store.client.ZAdd(ctx, zkey, redis.Z{Score: float64(timestamps[i].Seconds), Member: hk}).Err())
	}

	groupedRefs := &model.GroupedRangeFeatureRefs{
		EntityKeys:         []*types.EntityKey{entityKey},
		FeatureNames:       []string{feat},
		FeatureViewNames:   []string{view},
		Limit:              10,
		IsReverseSortOrder: false,
		SortKeyFilters: []*model.SortKeyFilter{
			{SortKeyName: sortKey, RangeStart: int64(1738699283), RangeEnd: int64(1738699290), StartInclusive: true, EndInclusive: true},
		},
	}

	results, err := store.OnlineReadRange(ctx, groupedRefs)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Len(t, results[0], 1)
	require.Len(t, results[0][0].Values, 2)
	require.Equal(t, "val1", results[0][0].Values[0].(*types.Value).GetStringVal())
	require.Equal(t, "val2", results[0][0].Values[1].(*types.Value).GetStringVal())
}

func TestOnlineReadRange_ReverseOrder(t *testing.T) {
	ctx := context.Background()
	store, _ := newMockRedisStore(t)

	entityKey := &types.EntityKey{
		JoinKeys:     []string{"user_id"},
		EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: "r1"}}},
	}
	serKey, _ := utils.SerializeEntityKey(entityKey, 3)
	entityKeyBin := string(*serKey)

	view := "reverse_features"
	sortKey := "event_ts"
	feat := "rev_count"
	zkey := fmt.Sprintf("%s:%s:%s:%s", store.project, view, sortKey, entityKeyBin)

	t1 := timestamppb.New(time.Unix(10, 0))
	t2 := timestamppb.New(time.Unix(20, 0))
	hk1 := fmt.Sprintf("%s:%s:%s:1", store.project, view, entityKeyBin)
	hk2 := fmt.Sprintf("%s:%s:%s:2", store.project, view, entityKeyBin)
	fieldHash := utils.Mmh3(fmt.Sprintf("%s:%s", view, feat))
	tsKey := fmt.Sprintf("_ts:%s", view)
	ts1, _ := proto.Marshal(t1)
	ts2, _ := proto.Marshal(t2)
	val1, _ := proto.Marshal(&types.Value{Val: &types.Value_StringVal{StringVal: "v1"}})
	val2, _ := proto.Marshal(&types.Value{Val: &types.Value_StringVal{StringVal: "v2"}})
	store.client.HSet(ctx, hk1, tsKey, ts1, fieldHash, val1)
	store.client.HSet(ctx, hk2, tsKey, ts2, fieldHash, val2)
	store.client.ZAdd(ctx, zkey, redis.Z{Score: float64(t1.Seconds), Member: hk1}, redis.Z{Score: float64(t2.Seconds), Member: hk2})

	groupedRefs := &model.GroupedRangeFeatureRefs{
		EntityKeys:         []*types.EntityKey{entityKey},
		FeatureNames:       []string{feat},
		FeatureViewNames:   []string{view},
		Limit:              5,
		IsReverseSortOrder: true,
		SortKeyFilters: []*model.SortKeyFilter{
			{SortKeyName: sortKey, RangeStart: int64(0), RangeEnd: int64(30)},
		},
	}

	results, err := store.OnlineReadRange(ctx, groupedRefs)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Len(t, results[0][0].Values, 2)
	// reverse order: v2 first
	require.Equal(t, "v2", results[0][0].Values[0].(*types.Value).GetStringVal())
	require.Equal(t, "v1", results[0][0].Values[1].(*types.Value).GetStringVal())
}

func TestOnlineReadRange_EmptyZset(t *testing.T) {
	ctx := context.Background()
	store, _ := newMockRedisStore(t)

	entityKey := &types.EntityKey{
		JoinKeys:     []string{"uid"},
		EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: "nope"}}},
	}
	groupedRefs := &model.GroupedRangeFeatureRefs{
		EntityKeys:         []*types.EntityKey{entityKey},
		FeatureNames:       []string{"fv"},
		FeatureViewNames:   []string{"fvn"},
		Limit:              3,
		IsReverseSortOrder: false,
		SortKeyFilters: []*model.SortKeyFilter{
			{SortKeyName: "ts", RangeStart: int64(1), RangeEnd: int64(5)},
		},
	}
	results, err := store.OnlineReadRange(ctx, groupedRefs)
	require.NoError(t, err)
	require.Equal(t, serving.FieldStatus_NOT_FOUND, results[0][0].Statuses[0])
}
