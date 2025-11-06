//go:build !integration

package onlinestore

import (
	"context"
	"fmt"
	"testing"
	"time"

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

func TestOnlineReadRange_TimestampRange(t *testing.T) {
	ctx := context.Background()

	// --- Setup Redis client ---
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.FlushDB(ctx)

	store := RedisOnlineStore{
		project: "test_project",
		t:       0, // redisNode
		client:  client,
		config:  &registry.RepoConfig{EntityKeySerializationVersion: 3},
	}

	// --- Mock Entity Key ---
	entityKey := &types.EntityKey{
		JoinKeys:     []string{"driver_id"},
		EntityValues: []*types.Value{{Val: &types.Value_StringVal{StringVal: "1006"}}},
	}

	// --- Serialize key (matches materialization) ---
	serializedKey, err := utils.SerializeEntityKey(entityKey, 3)
	require.NoError(t, err)
	redisKeyBin := string(*serializedKey)

	// --- Test setup parameters ---
	featureView := "driver_features"
	sortKeyName := "event_timestamp" // the ZSET is grouped by sort key, not feature name
	featureName := "trip_completed"

	// --- ZSET key now uses sort key name (Model B) ---
	zsetKey := fmt.Sprintf("%s:%s:%s:%s", store.project, featureView, sortKeyName, redisKeyBin)

	// --- Simulate Model B write (ZSET + multi-feature HSETs) ---
	t1 := time.Unix(1738699283, 0) // within range
	t2 := time.Unix(1738699290, 0) // within range
	t3 := time.Unix(1738699300, 0) // outside range

	hsetKeys := []string{
		fmt.Sprintf("%s:%s:%s:1", store.project, featureView, redisKeyBin),
		fmt.Sprintf("%s:%s:%s:2", store.project, featureView, redisKeyBin),
		fmt.Sprintf("%s:%s:%s:3", store.project, featureView, redisKeyBin),
	}
	eventTimes := []*timestamppb.Timestamp{
		timestamppb.New(t1),
		timestamppb.New(t2),
		timestamppb.New(t3),
	}

	fmt.Println("=== Writing HSET snapshots (Model B layout) ===")
	for i, hk := range hsetKeys {
		tsKey := fmt.Sprintf("_ts:%s", featureView)

		// All feature fields hashed by mmh3(<fv>:<feature>)
		fKeyTrip := utils.Mmh3(fmt.Sprintf("%s:%s", featureView, featureName))
		fKeyDriver := utils.Mmh3(fmt.Sprintf("%s:%s", featureView, "driver_rating"))

		tsBytes, _ := proto.Marshal(eventTimes[i])
		valTrip := &types.Value{Val: &types.Value_StringVal{StringVal: fmt.Sprintf("trip%d", i+1)}}
		valDriver := &types.Value{Val: &types.Value_Int32Val{Int32Val: int32(80 + i)}}

		valTripBytes, _ := proto.Marshal(valTrip)
		valDriverBytes, _ := proto.Marshal(valDriver)

		entityHset := map[string]interface{}{
			tsKey:      tsBytes,
			fKeyTrip:   valTripBytes,
			fKeyDriver: valDriverBytes,
		}

		err := client.HSet(ctx, hk, entityHset).Err()
		require.NoError(t, err)

		score := float64(eventTimes[i].Seconds)
		err = client.ZAdd(ctx, zsetKey, redis.Z{Score: score, Member: hk}).Err()
		require.NoError(t, err)

		fmt.Printf("HSET [%s]: _ts=%v  trip_completed=%v  driver_rating=%v (score=%.0f)\n",
			hk, eventTimes[i].Seconds, fmt.Sprintf("trip%d", i+1), 80+i, score)
	}

	zmembers, _ := client.ZRangeWithScores(ctx, zsetKey, 0, -1).Result()
	fmt.Println("=== ZSET Members ===")
	for _, z := range zmembers {
		fmt.Printf("Member: %s | Score: %.0f\n", z.Member, z.Score)
	}
	fmt.Println("=======================================")

	// --- GroupedRangeFeatureRefs reflects the new key layout ---
	groupedRefs := &model.GroupedRangeFeatureRefs{
		EntityKeys:         []*types.EntityKey{entityKey},
		FeatureNames:       []string{featureName},
		FeatureViewNames:   []string{featureView},
		Limit:              10,
		IsReverseSortOrder: false,
		SortKeyFilters: []*model.SortKeyFilter{
			{
				SortKeyName:    sortKeyName,
				RangeStart:     int64(1738699283),
				RangeEnd:       int64(1738699290),
				StartInclusive: true,
				EndInclusive:   true,
			},
		},
	}

	// Execute OnlineReadRange
	results, err := store.OnlineReadRange(ctx, groupedRefs)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Len(t, results[0], 1)

	rangeData := results[0][0]
	require.Equal(t, featureView, rangeData.FeatureView)
	require.Equal(t, featureName, rangeData.FeatureName)

	// --- Validate: only t1 and t2 are within range ---
	require.Len(t, rangeData.Values, 2)
	require.Equal(t, serving.FieldStatus_PRESENT, rangeData.Statuses[0])
	require.Equal(t, serving.FieldStatus_PRESENT, rangeData.Statuses[1])

	fmt.Printf("Retrieved values: %+v\n", rangeData.Values)
	fmt.Printf("Event timestamps: %+v\n", rangeData.EventTimestamps)
}

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
