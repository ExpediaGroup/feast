package onlinestore

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/internal/feast/utils"

	"github.com/redis/go-redis/extra/redisprometheus/v9"
	"github.com/redis/go-redis/v9"
	"github.com/spaolacci/murmur3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	redistrace "github.com/DataDog/dd-trace-go/contrib/redis/go-redis.v9/v2"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

type redisType int

const (
	redisNode    redisType = 0
	redisCluster redisType = 1
)

type RedisOnlineStore struct {

	// Feast project name
	// TODO (woop): Should we remove project as state that is tracked at the store level?
	project string

	// Redis database type, either a single node server (RedisType.Redis) or a cluster (RedisType.RedisCluster)
	t redisType

	// Redis client connector
	client *redis.Client

	// Redis cluster client connector
	clusterClient *redis.ClusterClient

	config *registry.RepoConfig

	// Number of keys to read in a batch
	ReadBatchSize int
}

func NewRedisOnlineStore(project string, config *registry.RepoConfig, onlineStoreConfig map[string]interface{}) (*RedisOnlineStore, error) {
	store := RedisOnlineStore{
		project: project,
		config:  config,
	}

	var address []string
	var password string
	var tlsConfig *tls.Config
	var db int // Default to 0

	// Parse redis_type and write it into conf.redisStoreType
	redisStoreType, err := getRedisType(onlineStoreConfig)
	if err != nil {
		return nil, err
	}
	store.t = redisStoreType

	// Parse connection_string and write it into conf.address, conf.password, and conf.ssl
	redisConnJson, ok := onlineStoreConfig["connection_string"]
	if !ok {
		// Default to "localhost:6379"
		redisConnJson = "localhost:6379"
	}
	if redisConnStr, ok := redisConnJson.(string); !ok {
		return nil, fmt.Errorf("failed to convert connection_string to string: %+v", redisConnJson)
	} else {
		parts := strings.Split(redisConnStr, ",")
		for _, part := range parts {
			if strings.Contains(part, ":") {
				address = append(address, part)
			} else if strings.Contains(part, "=") {
				kv := strings.SplitN(part, "=", 2)
				if kv[0] == "password" {
					password = kv[1]
				} else if kv[0] == "ssl" {
					result, err := strconv.ParseBool(kv[1])
					if err != nil {
						return nil, err
					} else if result {
						tlsConfig = &tls.Config{}
					}
				} else if kv[0] == "db" {
					db, err = strconv.Atoi(kv[1])
					if err != nil {
						return nil, err
					}
				} else {
					return nil, fmt.Errorf("unrecognized option in connection_string: %s. Must be one of 'password', 'ssl'", kv[0])
				}
			} else {
				return nil, fmt.Errorf("unable to parse a part of connection_string: %s. Must contain either ':' (addresses) or '=' (options", part)
			}
		}
	}

	// Parse read batch size
	var readBatchSize float64
	if readBatchSizeJsonValue, ok := onlineStoreConfig["read_batch_size"]; !ok {
		readBatchSize = 100.0 // Default to 100 Keys Per Batch
	} else if readBatchSize, ok = readBatchSizeJsonValue.(float64); !ok {
		return nil, fmt.Errorf("failed to convert read_batch_size: %+v", readBatchSizeJsonValue)
	}
	store.ReadBatchSize = int(readBatchSize)

	if store.ReadBatchSize >= 1 {
		log.Info().Msgf("Reads will be done in key batches of size: %d", store.ReadBatchSize)
	}

	// Metrics are not showing up when the service name is set to DD_SERVICE
	redisTraceServiceName := os.Getenv("DD_SERVICE") + "-redis"
	if redisTraceServiceName == "" {
		redisTraceServiceName = "redis.client" // default service name if DD_SERVICE is not set
	}

	if redisStoreType == redisNode {
		log.Info().Msgf("Using Redis: %s", address[0])
		store.client = redis.NewClient(&redis.Options{
			Addr:      address[0],
			Password:  password,
			DB:        db,
			TLSConfig: tlsConfig,
		})
		if (strings.ToLower(os.Getenv("ENABLE_DATADOG_REDIS_TRACING")) == "true") || (strings.ToLower(os.Getenv("ENABLE_ONLINE_STORE_TRACING")) == "true") {
			if strings.ToLower(os.Getenv("ENABLE_DATADOG_REDIS_TRACING")) == "true" {
				log.Warn().Msg("ENABLE_DATADOG_REDIS_TRACING is deprecated. Use ENABLE_ONLINE_STORE_TRACING instead.")
			}

			redistrace.WrapClient(store.client, redistrace.WithService(redisTraceServiceName))
			collector := redisprometheus.NewCollector("mlpfs", "redis", store.client)
			prometheus.MustRegister(collector)
		}
	} else if redisStoreType == redisCluster {
		log.Info().Msgf("Using Redis Cluster: %s", address)
		store.clusterClient = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:     address,
			Password:  password,
			TLSConfig: tlsConfig,
			ReadOnly:  true,
		})
		if (strings.ToLower(os.Getenv("ENABLE_DATADOG_REDIS_TRACING")) == "true") || (strings.ToLower(os.Getenv("ENABLE_ONLINE_STORE_TRACING")) == "true") {
			redistrace.WrapClient(store.clusterClient, redistrace.WithService(redisTraceServiceName))
			collector := redisprometheus.NewCollector("mlpfs", "redis", store.clusterClient)
			prometheus.MustRegister(collector)
		}
	}

	return &store, nil
}

func getRedisType(onlineStoreConfig map[string]interface{}) (redisType, error) {
	var t redisType

	redisTypeJson, ok := onlineStoreConfig["redis_type"]
	if !ok {
		// Default to "redis"
		redisTypeJson = "redis"
	} else if redisTypeStr, ok := redisTypeJson.(string); !ok {
		return -1, fmt.Errorf("failed to convert redis_type to string: %+v", redisTypeJson)
	} else {
		if redisTypeStr == "redis" {
			t = redisNode
		} else if redisTypeStr == "redis_cluster" {
			t = redisCluster
		} else {
			return -1, fmt.Errorf("failed to convert redis_type to enum: %s. Must be one of 'redis', 'redis_cluster'", redisTypeStr)
		}
	}
	return t, nil
}

func (r *RedisOnlineStore) buildFeatureViewIndices(featureViewNames []string, featureNames []string) (map[string]int, map[int]string, int) {
	featureViewIndices := make(map[string]int)
	indicesFeatureView := make(map[int]string)
	index := len(featureNames)
	for _, featureViewName := range featureViewNames {
		if _, ok := featureViewIndices[featureViewName]; !ok {
			featureViewIndices[featureViewName] = index
			indicesFeatureView[index] = featureViewName
			index += 1
		}
	}
	return featureViewIndices, indicesFeatureView, index
}

func (r *RedisOnlineStore) buildRedisHashSetKeys(featureViewNames []string, featureNames []string, indicesFeatureView map[int]string, index int) ([]string, []string) {
	featureCount := len(featureNames)
	var hsetKeys = make([]string, index)
	h := murmur3.New32()
	intBuffer := h.Sum32()
	byteBuffer := make([]byte, 4)

	for i := 0; i < featureCount; i++ {
		h.Write([]byte(fmt.Sprintf("%s:%s", featureViewNames[i], featureNames[i])))
		intBuffer = h.Sum32()
		binary.LittleEndian.PutUint32(byteBuffer, intBuffer)
		hsetKeys[i] = string(byteBuffer)
		h.Reset()
	}
	for i := featureCount; i < index; i++ {
		view := indicesFeatureView[i]
		tsKey := fmt.Sprintf("_ts:%s", view)
		hsetKeys[i] = tsKey
		featureNames = append(featureNames, tsKey)
	}
	return hsetKeys, featureNames
}

func (r *RedisOnlineStore) buildRedisKeys(entityKeys []*types.EntityKey) ([]*[]byte, map[string]int, error) {
	redisKeys := make([]*[]byte, len(entityKeys))
	redisKeyToEntityIndex := make(map[string]int)
	for i := 0; i < len(entityKeys); i++ {
		var key, err = BuildRedisKey(r.project, entityKeys[i], r.config.EntityKeySerializationVersion)
		if err != nil {
			return nil, nil, err
		}
		redisKeys[i] = key
		redisKeyToEntityIndex[string(*key)] = i
	}
	return redisKeys, redisKeyToEntityIndex, nil
}

func (r *RedisOnlineStore) OnlineReadV2(ctx context.Context, entityKeys []*types.EntityKey, featureViewNames []string, featureNames []string) ([][]FeatureData, error) {
	return r.OnlineRead(ctx, entityKeys, featureViewNames, featureNames)
}

func (r *RedisOnlineStore) OnlineRead(ctx context.Context, entityKeys []*types.EntityKey, featureViewNames []string, featureNames []string) ([][]FeatureData, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "redis.OnlineRead")
	defer span.Finish()

	featureCount := len(featureNames)
	featureViewIndices, indicesFeatureView, index := r.buildFeatureViewIndices(featureViewNames, featureNames)
	hsetKeys, featureNamesWithTimeStamps := r.buildRedisHashSetKeys(featureViewNames, featureNames, indicesFeatureView, index)
	redisKeys, redisKeyToEntityIndex, err := r.buildRedisKeys(entityKeys)
	if err != nil {
		return nil, err
	}

	results := make([][]FeatureData, len(entityKeys))
	commands := map[string]*redis.SliceCmd{}

	if r.t == redisNode {
		pipe := r.client.Pipeline()
		for _, redisKey := range redisKeys {
			keyString := string(*redisKey)
			commands[keyString] = pipe.HMGet(ctx, keyString, hsetKeys...)
		}
		_, err = pipe.Exec(ctx)
		if err != nil {
			return nil, err
		}
	} else if r.t == redisCluster {
		pipe := r.clusterClient.Pipeline()
		for _, redisKey := range redisKeys {
			keyString := string(*redisKey)
			commands[keyString] = pipe.HMGet(ctx, keyString, hsetKeys...)
		}
		_, err = pipe.Exec(ctx)
		if err != nil {
			return nil, err
		}
	}
	var entityIndex int
	var resContainsNonNil bool
	for redisKey, values := range commands {

		entityIndex = redisKeyToEntityIndex[redisKey]
		resContainsNonNil = false

		results[entityIndex] = make([]FeatureData, featureCount)
		res, err := values.Result()
		if err != nil {
			return nil, err
		}

		var timeStamp timestamppb.Timestamp

		for featureIndex, resString := range res {
			if featureIndex == featureCount {
				break
			}

			if resString == nil {
				// TODO (Ly): Can there be nil result within each feature or they will all be returned as string proto of types.Value_NullVal proto?
				featureName := featureNamesWithTimeStamps[featureIndex]
				featureViewName := featureViewNames[featureIndex]
				timeStampIndex := featureViewIndices[featureViewName]
				timeStampInterface := res[timeStampIndex]
				if timeStampInterface != nil {
					if timeStampString, ok := timeStampInterface.(string); !ok {
						return nil, errors.New("error parsing value from redis")
					} else {
						if err := proto.Unmarshal([]byte(timeStampString), &timeStamp); err != nil {
							return nil, errors.New("error converting parsed redis value to timestamppb.Timestamp")
						}
					}
				}

				results[entityIndex][featureIndex] = FeatureData{Reference: serving.FeatureReferenceV2{FeatureViewName: featureViewName, FeatureName: featureName},
					Timestamp: timestamppb.Timestamp{Seconds: timeStamp.Seconds, Nanos: timeStamp.Nanos},
					Value:     types.Value{Val: &types.Value_NullVal{NullVal: types.Null_NULL}},
				}

			} else if valueString, ok := resString.(string); !ok {
				return nil, errors.New("error parsing Value from redis")
			} else {
				resContainsNonNil = true
				var value *types.Value
				if value, _, err = utils.UnmarshalStoredProto([]byte(valueString)); err != nil {
					return nil, errors.New("error converting parsed redis Value to types.Value")
				} else {
					featureName := featureNamesWithTimeStamps[featureIndex]
					featureViewName := featureViewNames[featureIndex]
					timeStampIndex := featureViewIndices[featureViewName]
					timeStampInterface := res[timeStampIndex]
					if timeStampInterface != nil {
						if timeStampString, ok := timeStampInterface.(string); !ok {
							return nil, errors.New("error parsing Value from redis")
						} else {
							if err := proto.Unmarshal([]byte(timeStampString), &timeStamp); err != nil {
								return nil, errors.New("error converting parsed redis Value to timestamppb.Timestamp")
							}
						}
					}
					results[entityIndex][featureIndex] = FeatureData{Reference: serving.FeatureReferenceV2{FeatureViewName: featureViewName, FeatureName: featureName},
						Timestamp: timestamppb.Timestamp{Seconds: timeStamp.Seconds, Nanos: timeStamp.Nanos},
						Value:     types.Value{Val: value.Val},
					}
				}
			}
		}

		if !resContainsNonNil {
			results[entityIndex] = nil
		}

	}

	return results, nil
}

func SerializeEntityKeyWithProject(
	project string,
	entityKey *types.EntityKey,
	version int64,
) ([]byte, error) {
	key, err := BuildRedisKey(project, entityKey, version)
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, fmt.Errorf("buildRedisKey returned nil")
	}
	return *key, nil
}

// batchHMGET performs parallel batched HMGET operations for a slice of members
func batchHMGET(
	ctx context.Context,
	client redis.UniversalClient,
	entityKeyBin []byte,
	members [][]byte,
	fields []string,
	fv string,
	grp *utils.FvGroup,
	results [][]RangeFeatureData,
	eIdx int,
	batchSize int,
) error {
	if len(members) == 0 {
		return nil
	}
	if batchSize <= 0 {
		batchSize = utils.DefaultBatchSize
	}

	nBatches := (len(members) + batchSize - 1) / batchSize

	// Results array to store results from each member
	batchResults := make([]*utils.MgetBatchResult, len(members))

	var wg sync.WaitGroup
	errChan := make(chan error, nBatches)

	// Process batches in parallel
	for b := 0; b < nBatches; b++ {
		select {
		case <-ctx.Done():
			wg.Wait()
			close(errChan)
			return ctx.Err()
		default:
		}

		startIdx := b * batchSize
		end := startIdx + batchSize
		if end > len(members) {
			end = len(members)
		}
		batch := members[startIdx:end]

		wg.Add(1)
		go func(startIdx int, batch [][]byte) {
			defer wg.Done()

			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
			}

			// Build pipeline for this batch
			pipe := client.Pipeline()
			hm := make(map[string]*redis.SliceCmd, len(batch))

			for _, sortKeyBytes := range batch {
				memberKey := base64.StdEncoding.EncodeToString(sortKeyBytes)
				hashKey := utils.BuildHashKey(entityKeyBin, sortKeyBytes)
				hm[memberKey] = pipe.HMGet(ctx, hashKey, fields...)
			}

			// Execute pipeline
			if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
				errChan <- fmt.Errorf("HMGET pipeline failed: %w", err)
				return
			}

			// Process results from this batch
			for i, sortKeyBytes := range batch {
				memberIdx := startIdx + i
				memberKey := base64.StdEncoding.EncodeToString(sortKeyBytes)

				cmd, ok := hm[memberKey]
				if !ok {
					continue
				}

				arr, err := cmd.Result()
				if err != nil && !errors.Is(err, redis.Nil) {
					continue
				}

				featureCount := len(grp.FeatNames)

				// Check if all feature fields are nil
				allNil := true
				for fi := 0; fi < featureCount && fi < len(arr)-1; fi++ {
					if arr[fi] != nil {
						allNil = false
						break
					}
				}
				if allNil {
					continue
				}

				// Decode timestamp (last field)
				var eventTS timestamppb.Timestamp
				if len(arr) > 0 {
					eventTS = utils.DecodeTimestamp(arr[len(arr)-1])
				}

				res := &utils.MgetBatchResult{
					MemberIdx: memberIdx,
					MemberKey: memberKey,
					Values:    make(map[int]interface{}),
					Statuses:  make(map[int]serving.FieldStatus),
					Timestamp: eventTS,
				}

				// Decode each feature
				for localIdx, col := range grp.ColumnIndexes {
					if localIdx >= len(arr)-1 {
						continue
					}

					var val interface{}
					var status serving.FieldStatus

					if arr[localIdx] == nil {
						val = nil
						status = serving.FieldStatus_NULL_VALUE
					} else {
						decoded, st := utils.DecodeFeatureValue(
							arr[localIdx], fv, grp.FeatNames[localIdx], memberKey,
						)

						if st == serving.FieldStatus_NULL_VALUE {
							val = nil
						} else {
							val = decoded
						}
						status = st
					}

					_ = col
					res.Values[localIdx] = val
					res.Statuses[localIdx] = status
				}

				batchResults[memberIdx] = res
			}
		}(startIdx, batch)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	var allErrors []error
	for e := range errChan {
		if e != nil {
			allErrors = append(allErrors, e)
		}
	}
	if len(allErrors) > 0 {
		return errors.Join(allErrors...)
	}

	// Append results in order
	for _, result := range batchResults {
		if result == nil {
			continue
		}
		for localIdx, col := range grp.ColumnIndexes {
			results[eIdx][col].Values = append(results[eIdx][col].Values, result.Values[localIdx])
			results[eIdx][col].Statuses = append(results[eIdx][col].Statuses, result.Statuses[localIdx])
			results[eIdx][col].EventTimestamps = append(results[eIdx][col].EventTimestamps, result.Timestamp)
		}
	}

	return nil
}

// processEntityKey processes a single entity key
func (r *RedisOnlineStore) processEntityKey(
	ctx context.Context,
	eIdx int,
	entityKey *types.EntityKey,
	fvGroups map[string]*utils.FvGroup,
	effectiveReverse bool,
	minScore, maxScore string,
	limit int64,
	results [][]RangeFeatureData,
	featNames, fvNames []string,
	client redis.UniversalClient,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	entityKeyBin, err := SerializeEntityKeyWithProject(
		r.project,
		entityKey,
		r.config.EntityKeySerializationVersion,
	)
	if err != nil {
		return fmt.Errorf("failed to serialize entity key: %w", err)
	}

	// Initialize results row
	results[eIdx] = make([]RangeFeatureData, len(featNames))
	for i := range featNames {
		results[eIdx][i] = RangeFeatureData{
			FeatureView:     fvNames[i],
			FeatureName:     featNames[i],
			Values:          []interface{}{},
			Statuses:        []serving.FieldStatus{},
			EventTimestamps: []timestamppb.Timestamp{},
		}
	}

	// Build ZRANGE commands for all feature views
	// Swap min/max when reversed to match Valkey behavior
	var start, stop string
	if effectiveReverse {
		start = maxScore
		stop = minScore
	} else {
		start = minScore
		stop = maxScore
	}

	p := client.Pipeline()
	zCmds := make(map[string]*redis.StringSliceCmd)

	for fv := range fvGroups {
		zkey := utils.BuildZsetKey(fv, entityKeyBin)
		args := redis.ZRangeArgs{
			Key:     zkey,
			Start:   start,
			Stop:    stop,
			ByScore: true,
			Rev:     effectiveReverse,
			Offset:  0,
			Count:   limit,
		}
		zCmds[fv] = p.ZRangeArgs(ctx, args)
	}

	// Execute ZRANGE pipeline
	if _, err := p.Exec(ctx); err != nil && err != redis.Nil {
		return fmt.Errorf("ZRANGE pipeline failed: %w", err)
	}

	// Parse ZRANGE results
	zMembers := make(map[string][][]byte)
	for fv, cmd := range zCmds {
		members, err := cmd.Result()
		if err != nil && err != redis.Nil {
			log.Warn().
				Int("entity_index", eIdx).
				Str("feature_view", fv).
				Err(err).
				Msg("ZRANGE error")
			zMembers[fv] = nil
			continue
		}

		// Convert string members to byte slices
		memberBytes := make([][]byte, len(members))
		for i, m := range members {
			memberBytes[i] = []byte(m)
		}
		zMembers[fv] = memberBytes
	}

	// HMGET feature values for each feature view
	for fv, grp := range fvGroups {
		members := zMembers[fv]

		if len(members) == 0 {
			for _, col := range grp.ColumnIndexes {
				results[eIdx][col].Values = append(results[eIdx][col].Values, nil)
				results[eIdx][col].Statuses = append(results[eIdx][col].Statuses, serving.FieldStatus_NOT_FOUND)
				results[eIdx][col].EventTimestamps = append(results[eIdx][col].EventTimestamps, timestamppb.Timestamp{})
			}
			continue
		}

		// Build list of hash fields to retrieve
		fields := make([]string, 0, len(grp.FieldHashes)+1)
		fields = append(fields, grp.FieldHashes...)
		fields = append(fields, grp.TsKey)

		if err := batchHMGET(
			ctx,
			client,
			entityKeyBin,
			members,
			fields,
			fv,
			grp,
			results,
			eIdx,
			utils.DefaultBatchSize,
		); err != nil {
			return err
		}

		// Apply limit if set by truncating each feature's result lists
		if limit > 0 {
			for _, col := range grp.ColumnIndexes {
				r := &results[eIdx][col]
				if len(r.Values) > int(limit) {
					r.Values = r.Values[:limit]
					r.Statuses = r.Statuses[:limit]
					r.EventTimestamps = r.EventTimestamps[:limit]
				}
			}
		}
	}

	return nil
}

// OnlineReadRange performs the online read for range querying with parallel entity processing
func (r *RedisOnlineStore) OnlineReadRange(
	ctx context.Context,
	groupedRefs *model.GroupedRangeFeatureRefs,
) ([][]RangeFeatureData, error) {

	if groupedRefs == nil || len(groupedRefs.EntityKeys) == 0 {
		log.Warn().Msg("OnlineReadRange: no entity keys provided")
		return nil, fmt.Errorf("no entity keys provided")
	}

	featNames := groupedRefs.FeatureNames
	fvNames := groupedRefs.FeatureViewNames
	limit := int64(groupedRefs.Limit)

	effectiveReverse := utils.ComputeEffectiveReverse(
		groupedRefs.SortKeyFilters,
		groupedRefs.IsReverseSortOrder,
	)

	minScore, maxScore := "-inf", "+inf"
	if len(groupedRefs.SortKeyFilters) != 0 {
		minScore, maxScore = utils.GetScoreRange(groupedRefs.SortKeyFilters)
		if len(groupedRefs.SortKeyFilters) > 1 {
			log.Warn().
				Int("sort_key_count", len(groupedRefs.SortKeyFilters)).
				Msg("OnlineReadRange: more than one sort key filter provided; only the first will be used")
		}
	}

	// Determine which client to use
	var client redis.UniversalClient
	if r.t == redisCluster {
		client = r.clusterClient
	} else {
		client = r.client
	}

	// Group features by feature view
	fvGroups := map[string]*utils.FvGroup{}
	for i := range featNames {
		fv, fn := fvNames[i], featNames[i]
		g := fvGroups[fv]
		if g == nil {
			g = &utils.FvGroup{
				View:          fv,
				TsKey:         fmt.Sprintf("_ts:%s", fv),
				FeatNames:     []string{},
				FieldHashes:   []string{},
				ColumnIndexes: []int{},
			}
			fvGroups[fv] = g
		}
		g.FeatNames = append(g.FeatNames, fn)
		g.FieldHashes = append(g.FieldHashes, utils.Mmh3FieldHash(fv, fn))
		g.ColumnIndexes = append(g.ColumnIndexes, i)
	}

	results := make([][]RangeFeatureData, len(groupedRefs.EntityKeys))

	var wg sync.WaitGroup
	errChan := make(chan error, len(groupedRefs.EntityKeys))

	// Process each entity key in parallel
	for eIdx, entityKey := range groupedRefs.EntityKeys {
		wg.Add(1)
		go func(idx int, ek *types.EntityKey) {
			defer wg.Done()
			if err := r.processEntityKey(
				ctx,
				idx,
				ek,
				fvGroups,
				effectiveReverse,
				minScore, maxScore,
				limit,
				results,
				featNames, fvNames,
				client,
			); err != nil {
				errChan <- err
			}
		}(eIdx, entityKey)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	var allErrors []error
	for err := range errChan {
		if err != nil {
			allErrors = append(allErrors, err)
		}
	}

	if len(allErrors) > 0 {
		return nil, errors.Join(allErrors...)
	}

	return results, nil
}

// Dummy destruct function to conform with plugin OnlineStore interface
func (r *RedisOnlineStore) Destruct() {

}

func BuildRedisKey(project string, entityKey *types.EntityKey, entityKeySerializationVersion int64) (*[]byte, error) {
	serKey, err := utils.SerializeEntityKey(entityKey, entityKeySerializationVersion)
	if err != nil {
		return nil, err
	}
	fullKey := append(*serKey, []byte(project)...)
	return &fullKey, nil
}

func (r *RedisOnlineStore) GetDataModelType() OnlineStoreDataModel {
	return EntityLevel
}

func (r *RedisOnlineStore) GetReadBatchSize() int {
	return -1 // No Batching

}
