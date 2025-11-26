package onlinestore

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/utils"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"github.com/feast-dev/feast/go/internal/feast/registry"

	"github.com/spaolacci/murmur3"
	"github.com/valkey-io/valkey-go"

	valkeytrace "github.com/DataDog/dd-trace-go/contrib/valkey-io/valkey-go/v2"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/rs/zerolog/log"
)

const defaultConnectionString = "localhost:6379"

////////////////////////////////////////////////////////////////////////////////
// TYPES & CONSTANTS
////////////////////////////////////////////////////////////////////////////////

// valkeyType represents the Valkey deployment topology.
type valkeyType int

const (
	valkeyNode    valkeyType = 0 // Single-node deployment
	valkeyCluster valkeyType = 1 // Cluster deployment
)

// ValkeyOnlineStore implements the Feast OnlineStore interface using Valkey.
// Supports entity-level reads (HMGET) and range reads (ZSET + HASH model).
type ValkeyOnlineStore struct {
	project       string
	t             valkeyType
	client        valkey.Client
	config        *registry.RepoConfig
	ReadBatchSize int // Batch size for HMGET / pipeline operations
}

////////////////////////////////////////////////////////////////////////////////
// CONNECTION SETUP
////////////////////////////////////////////////////////////////////////////////

// parseConnectionString constructs a Valkey ClientOption from the Feast config.
// Supports:
//   - host:port multi-endpoint lists
//   - password=... , ssl=true , db=...
//   - standalone replica read routing for readonly commands
func parseConnectionString(cfg map[string]interface{}, t valkeyType) (valkey.ClientOption, error) {
	var opt valkey.ClientOption

	// Ensure read-only commands are routed to replicas (when available).
	opt.SendToReplicas = func(cmd valkey.Completed) bool { return cmd.IsReadOnly() }

	// Standalone replica support (not used in cluster mode).
	if t == valkeyNode {
		if raw, ok := cfg["replica_address"]; ok {
			repStr, ok := raw.(string)
			if !ok {
				return opt, fmt.Errorf("replica_address must be string")
			}
			for _, part := range strings.Split(repStr, ",") {
				if strings.Contains(part, ":") {
					opt.Standalone.ReplicaAddress = append(opt.Standalone.ReplicaAddress, part)
				} else {
					return opt, fmt.Errorf("invalid replica address segment: %s", part)
				}
			}
		} else {
			log.Warn().Msg("replica_address not provided; replica reads disabled")
		}
	}

	// Base connection string
	valkeyConn := defaultConnectionString
	if raw, ok := cfg["connection_string"]; ok {
		var ok2 bool
		valkeyConn, ok2 = raw.(string)
		if !ok2 {
			return opt, fmt.Errorf("connection_string must be string")
		}
	}

	// Parse "host:port" entries or "key=value" parameters.
	for _, part := range strings.Split(valkeyConn, ",") {

		// host:port
		if strings.Contains(part, ":") && !strings.Contains(part, "=") {
			opt.InitAddress = append(opt.InitAddress, part)
			continue
		}

		// key=value
		if strings.Contains(part, "=") {
			kv := strings.SplitN(part, "=", 2)
			key, value := kv[0], kv[1]

			switch key {
			case "password":
				opt.Password = value
			case "ssl":
				useSSL, err := strconv.ParseBool(value)
				if err != nil {
					return opt, err
				}
				if useSSL {
					opt.TLSConfig = &tls.Config{}
				}
			case "db":
				db, err := strconv.Atoi(value)
				if err != nil {
					return opt, err
				}
				opt.SelectDB = db
			default:
				return opt, fmt.Errorf("unknown connection_string option: %s", key)
			}
			continue
		}

		return opt, fmt.Errorf("cannot parse segment: %s", part)
	}

	return opt, nil
}

// getValkeyTraceServiceName derives the Datadog tracing service name for Valkey.
func getValkeyTraceServiceName() string {
	if svc := os.Getenv("DD_SERVICE"); svc != "" {
		return svc + "-valkey"
	}
	return "valkey.client"
}

// initializeValkeyClient returns a Valkey client, optionally wrapped with Datadog tracing.
func initializeValkeyClient(opt valkey.ClientOption, serviceName string) (valkey.Client, error) {
	if strings.ToLower(os.Getenv("ENABLE_ONLINE_STORE_TRACING")) == "true" {
		return valkeytrace.NewClient(opt, valkeytrace.WithService(serviceName))
	}
	return valkey.NewClient(opt)
}

// NewValkeyOnlineStore initializes the Valkey-based online store.
func NewValkeyOnlineStore(project string, cfg *registry.RepoConfig, storeCfg map[string]interface{}) (*ValkeyOnlineStore, error) {
	store := &ValkeyOnlineStore{
		project: project,
		config:  cfg,
	}

	// Determine standalone vs cluster deployment.
	valkeyType, err := getValkeyType(storeCfg)
	if err != nil {
		return nil, err
	}
	store.t = valkeyType

	// Build client.
	clientOpt, err := parseConnectionString(storeCfg, valkeyType)
	if err != nil {
		return nil, err
	}
	store.client, err = initializeValkeyClient(clientOpt, getValkeyTraceServiceName())
	if err != nil {
		return nil, err
	}

	// Read batch size for HMGET.
	batchSize := 100.0
	if raw, ok := storeCfg["read_batch_size"]; ok {
		batchSize, ok = raw.(float64)
		if !ok {
			return nil, fmt.Errorf("read_batch_size must be numeric")
		}
	}
	store.ReadBatchSize = int(batchSize)

	log.Info().Msgf("Valkey batch size: %d", store.ReadBatchSize)
	log.Info().Msgf("Valkey config endpoints: %v", clientOpt.InitAddress)

	return store, nil
}

// getValkeyType parses "valkey_type" into the internal enum.
func getValkeyType(cfg map[string]interface{}) (valkeyType, error) {
	typ := "valkey"

	if raw, ok := cfg["valkey_type"]; ok {
		var ok2 bool
		typ, ok2 = raw.(string)
		if !ok2 {
			return -1, fmt.Errorf("valkey_type must be string")
		}
	}

	switch typ {
	case "valkey":
		return valkeyNode, nil
	case "valkey_cluster":
		return valkeyCluster, nil
	default:
		return -1, fmt.Errorf("unknown valkey_type: %s", typ)
	}
}

////////////////////////////////////////////////////////////////////////////////
// FEATURE INDEXING & KEY ENCODING
////////////////////////////////////////////////////////////////////////////////

// buildFeatureViewIndices assigns each feature view a unique timestamp column index.
// Feature columns come first, and timestamp columns follow.
func (v *ValkeyOnlineStore) buildFeatureViewIndices(fvNames, featNames []string) (map[string]int, map[int]string, int) {
	fvToIndex := make(map[string]int)
	indexToFv := make(map[int]string)

	idx := len(featNames)
	for _, fv := range fvNames {
		if _, exists := fvToIndex[fv]; !exists {
			fvToIndex[fv] = idx
			indexToFv[idx] = fv
			idx++
		}
	}
	return fvToIndex, indexToFv, idx
}

// buildHsetKeys produces hashed HSET field names for features and raw names for timestamps.
func (v *ValkeyOnlineStore) buildHsetKeys(fvNames, featNames []string, indexToFv map[int]string, total int) ([]string, []string) {
	hsetKeys := make([]string, total)

	hasher := murmur3.New32()
	buf := make([]byte, 4)
	featCount := len(featNames)

	// Hash feature entries
	for i := 0; i < featCount; i++ {
		hasher.Write([]byte(fmt.Sprintf("%s:%s", fvNames[i], featNames[i])))
		binary.LittleEndian.PutUint32(buf, hasher.Sum32())
		hsetKeys[i] = string(buf)
		hasher.Reset()
	}

	// Timestamp columns
	for i := featCount; i < total; i++ {
		fv := indexToFv[i]
		tsKey := fmt.Sprintf("_ts:%s", fv)
		hsetKeys[i] = tsKey
		featNames = append(featNames, tsKey)
	}

	return hsetKeys, featNames
}

// buildValkeyKeys serializes entity keys and appends the project for namespacing.
func (v *ValkeyOnlineStore) buildValkeyKeys(entityKeys []*types.EntityKey) ([]*[]byte, error) {
	keys := make([]*[]byte, len(entityKeys))

	for i, ek := range entityKeys {
		key, err := buildValkeyKey(v.project, ek, v.config.EntityKeySerializationVersion)
		if err != nil {
			return nil, err
		}
		keys[i] = key
	}

	return keys, nil
}

////////////////////////////////////////////////////////////////////////////////
// ONLINE READ (ENTITY LEVEL)
////////////////////////////////////////////////////////////////////////////////

// OnlineReadV2 exists for backward compatibility with Feast’s interface.
func (v *ValkeyOnlineStore) OnlineReadV2(ctx context.Context, keys []*types.EntityKey, fvNames, featNames []string) ([][]FeatureData, error) {
	return v.OnlineRead(ctx, keys, fvNames, featNames)
}

// OnlineRead executes entity-level feature retrieval using HMGET.
// Uses parallel batching for large workloads.
func (v *ValkeyOnlineStore) OnlineRead(ctx context.Context, keys []*types.EntityKey, fvNames, featNames []string) ([][]FeatureData, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "OnlineRead")
	defer span.Finish()

	// Parallel path is preferred for scalability.
	return v.onlineReadParallel(ctx, keys, fvNames, featNames)
}

////////////////////////////////////////////////////////////////////////////////
// ONLINE READ: PARALLEL + SEQUENTIAL EXECUTION
////////////////////////////////////////////////////////////////////////////////

// batchInfo defines metadata for each batch of entity keys.
type batchInfo struct {
	keys     []*types.EntityKey
	startIdx int // position in the global result matrix
}

// onlineReadSequential executes a single batch via HMGET.
// Used by the parallel backend for each batch.
func (v *ValkeyOnlineStore) onlineReadSequential(ctx context.Context, entityKeys []*types.EntityKey, fvNames, featNames []string) ([][]FeatureData, error) {
	if len(entityKeys) == 0 {
		return [][]FeatureData{}, nil
	}

	featCount := len(featNames)
	results := make([][]FeatureData, len(entityKeys))
	for i := range results {
		results[i] = make([]FeatureData, featCount)
	}

	batch := batchInfo{keys: entityKeys, startIdx: 0}

	if err := v.executeBatch(ctx, batch, fvNames, featNames, results); err != nil {
		return nil, err
	}

	return results, nil
}

// onlineReadParallel splits entity keys into batches and processes them concurrently.
func (v *ValkeyOnlineStore) onlineReadParallel(ctx context.Context, entityKeys []*types.EntityKey, fvNames, featNames []string) ([][]FeatureData, error) {
	if len(entityKeys) == 0 {
		return [][]FeatureData{}, nil
	}

	featCount := len(featNames)
	results := make([][]FeatureData, len(entityKeys))
	for i := range results {
		results[i] = make([]FeatureData, featCount)
	}

	batches := v.createBatchesWithIndices(entityKeys)

	var wg sync.WaitGroup
	errChan := make(chan error, len(batches))

	for _, batch := range batches {
		batch := batch
		wg.Add(1)

		go func() {
			defer wg.Done()

			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
			}

			if err := v.executeBatch(ctx, batch, fvNames, featNames, results); err != nil {
				errChan <- err
			}
		}()
	}

	wg.Wait()
	close(errChan)

	var errs []error
	for err := range errChan {
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	return results, nil
}

// executeBatch runs HMGET for all keys in the batch and writes results directly into the result matrix.
func (v *ValkeyOnlineStore) executeBatch(
	ctx context.Context,
	batch batchInfo,
	fvNames, featNames []string,
	results [][]FeatureData,
) error {

	featCount := len(featNames)

	// Index feature views → timestamp field layout.
	fvIdx, idxFV, totalCols := v.buildFeatureViewIndices(fvNames, featNames)
	hsetKeys, featNamesWithTS := v.buildHsetKeys(fvNames, featNames, idxFV, totalCols)

	// Generate serialized Valkey keys.
	valkeyKeys, err := v.buildValkeyKeys(batch.keys)
	if err != nil {
		return err
	}

	numViews := len(fvIdx)
	cmds := make(valkey.Commands, 0, len(batch.keys))

	for _, key := range valkeyKeys {
		cmds = append(cmds, v.client.B().Hmget().Key(string(*key)).Field(hsetKeys...).Build())
	}

	// HMGET pipeline execution.
	for localIdx, reply := range v.client.DoMulti(ctx, cmds...) {
		if err := reply.Error(); err != nil {
			return err
		}

		arr, err := reply.ToArray()
		if err != nil {
			return err
		}

		globalIdx := batch.startIdx + localIdx
		timeStampMap := make(map[string]*timestamppb.Timestamp, numViews)

		hasValue := false

		for i := 0; i < featCount; i++ {
			fv := fvNames[i]
			fn := featNamesWithTS[i]

			value := &types.Value{Val: &types.Value_NullVal{NullVal: types.Null_NULL}}

			// Feature decoding
			if !arr[i].IsNil() {
				hasValue = true

				rawStr, err := arr[i].ToString()
				if err != nil {
					return err
				}

				vv, _, err := utils.UnmarshalStoredProto([]byte(rawStr))
				if err != nil {
					return errors.New("invalid stored proto")
				}
				value = vv
			}

			// Timestamp decoding (once per feature view).
			if _, found := timeStampMap[fv]; !found {
				tsIdx := fvIdx[fv]
				ts := timestamppb.Timestamp{}

				if !arr[tsIdx].IsNil() {
					rawTS, err := arr[tsIdx].ToString()
					if err != nil {
						return err
					}
					if err := proto.Unmarshal([]byte(rawTS), &ts); err != nil {
						return errors.New("failed to unmarshal timestamp")
					}
				}
				timeStampMap[fv] = &timestamppb.Timestamp{Seconds: ts.Seconds, Nanos: ts.Nanos}
			}

			results[globalIdx][i] = FeatureData{
				Reference: serving.FeatureReferenceV2{
					FeatureViewName: fv,
					FeatureName:     fn,
				},
				Timestamp: *timeStampMap[fv],
				Value:     types.Value{Val: value.Val},
			}
		}

		if !hasValue {
			results[globalIdx] = nil
		}
	}

	return nil
}

// valkeyBatchHMGET executes MGET for range results and populates RangeFeatureData.
func valkeyBatchHMGET(
	ctx context.Context,
	client valkey.Client,
	entityKeyBin []byte,
	members [][]byte,
	fields []string,
	fv string,
	grp *fvGroup,
	results [][]RangeFeatureData,
	eIdx int,
	batchSize int,
) error {

	// Build HASH keys for all ZSET members.
	keys := make([]string, len(members))
	for i, sk := range members {
		keys[i] = utils.BuildHashKey(entityKeyBin, sk)
	}

	raw, err := client.Do(ctx, client.B().Mget().Key(keys...).Build()).AsStrSlice()
	if err != nil {
		return fmt.Errorf("mget failed: %w", err)
	}

	// Decode each record.
	for i, sk := range members {
		if i >= len(raw) || raw[i] == "" {
			continue
		}

		var record map[string]interface{}
		if err := json.Unmarshal([]byte(raw[i]), &record); err != nil {
			continue
		}

		// Skip if all feature fields are nil.
		allNil := true
		for _, field := range fields[:len(fields)-1] {
			if v, exists := record[field]; exists && v != nil {
				allNil = false
				break
			}
		}
		if allNil {
			continue
		}

		var ts timestamppb.Timestamp
		if rawTS, ok := record[fields[len(fields)-1]]; ok {
			if s, ok := rawTS.(string); ok {
				ts = utils.DecodeTimestamp(s)
			}
		}

		memberKey := base64.StdEncoding.EncodeToString(sk)

		for colIdx, col := range grp.columnIndexes {
			fieldName := fields[colIdx]

			rawVal, exists := record[fieldName]
			var (
				outVal interface{}
				status serving.FieldStatus
			)

			if !exists || rawVal == nil {
				status = serving.FieldStatus_NULL_VALUE
			} else {
				strVal := fmt.Sprintf("%v", rawVal)
				outVal, status = utils.DecodeFeatureValue(strVal, fv, grp.featNames[colIdx], memberKey)

				if status == serving.FieldStatus_NULL_VALUE {
					outVal = nil
				}
			}

			results[eIdx][col].Values = append(results[eIdx][col].Values, outVal)
			results[eIdx][col].Statuses = append(results[eIdx][col].Statuses, status)
			results[eIdx][col].EventTimestamps = append(results[eIdx][col].EventTimestamps, ts)
		}
	}

	return nil
}

// OnlineReadRange performs a time-series lookup using the ZSET+HASH model.
// ZSET(entity_key) stores sort keys ordered by timestamp or user-provided score.
// HASH(entity_key + sort_key) stores feature columns and timestamp.
func (v *ValkeyOnlineStore) OnlineReadRange(
	ctx context.Context,
	grouped *model.GroupedRangeFeatureRefs,
) ([][]RangeFeatureData, error) {

	if grouped == nil || len(grouped.EntityKeys) == 0 {
		return nil, fmt.Errorf("no entity keys provided")
	}

	featNames := grouped.FeatureNames
	fvNames := grouped.FeatureViewNames
	limit := int64(grouped.Limit)

	// Compute direction and score boundaries.
	effectiveReverse := utils.ComputeEffectiveReverse(grouped.SortKeyFilters, grouped.IsReverseSortOrder)
	minScore, maxScore := "-inf", "+inf"
	if len(grouped.SortKeyFilters) > 0 {
		minScore, maxScore = utils.GetScoreRange(grouped.SortKeyFilters)
	}

	// Group columns by feature view.
	fvGroups := map[string]*fvGroup{}
	for i := range featNames {
		fv, fn := fvNames[i], featNames[i]

		g := fvGroups[fv]
		if g == nil {
			g = &fvGroup{
				view:          fv,
				tsKey:         fmt.Sprintf("_ts:%s", fv),
				featNames:     []string{},
				fieldHashes:   []string{},
				columnIndexes: []int{},
			}
			fvGroups[fv] = g
		}

		g.featNames = append(g.featNames, fn)
		g.fieldHashes = append(g.fieldHashes, utils.Mmh3FieldHash(fv, fn))
		g.columnIndexes = append(g.columnIndexes, i)
	}

	// Allocate output.
	results := make([][]RangeFeatureData, len(grouped.EntityKeys))

	// Process each entity key independently.
	for eIdx, ek := range grouped.EntityKeys {

		entityKeyBin, err := SerializeEntityKeyWithProject(v.project, ek, v.config.EntityKeySerializationVersion)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize entity key: %w", err)
		}

		// Initialize rows.
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

		zCmds := make([]valkey.Completed, 0, len(fvGroups))
		fvOrder := make([]string, 0, len(fvGroups))

		for fv := range fvGroups {
			zkey := utils.BuildZsetKey(fv, entityKeyBin)

			var cmd valkey.Completed
			if effectiveReverse {
				cmd = v.client.B().
					Zrange().Key(zkey).Min(maxScore).Max(minScore).
					Byscore().Rev().Build()
			} else {
				cmd = v.client.B().
					Zrange().Key(zkey).Min(minScore).Max(maxScore).
					Byscore().Build()
			}

			zCmds = append(zCmds, cmd)
			fvOrder = append(fvOrder, fv)
		}

		zResults := v.client.DoMulti(ctx, zCmds...)

		// Decode ZSET results.
		memberMap := make(map[string][][]byte)
		for i, fv := range fvOrder {
			raw := zResults[i]
			if err := raw.Error(); err != nil {
				memberMap[fv] = nil
				continue
			}

			arr, err := raw.ToArray()
			if err != nil {
				memberMap[fv] = nil
				continue
			}

			members := make([][]byte, 0, len(arr))
			for _, itm := range arr {
				if itm.IsNil() {
					continue
				}
				s, _ := itm.ToString()
				members = append(members, []byte(s))
			}

			memberMap[fv] = members
		}
		for fv, grp := range fvGroups {

			members := memberMap[fv]

			// No records in the ZSET result.
			if len(members) == 0 {
				for _, col := range grp.columnIndexes {
					results[eIdx][col].Values = append(results[eIdx][col].Values, nil)
					results[eIdx][col].Statuses = append(results[eIdx][col].Statuses, serving.FieldStatus_NOT_FOUND)
					results[eIdx][col].EventTimestamps = append(results[eIdx][col].EventTimestamps, timestamppb.Timestamp{})
				}
				continue
			}

			fields := append(append([]string{}, grp.fieldHashes...), grp.tsKey)

			if err := valkeyBatchHMGET(ctx, v.client, entityKeyBin, members, fields, fv, grp, results, eIdx, v.ReadBatchSize); err != nil {
				return nil, err
			}

			// Apply limit.
			if limit > 0 {
				for _, col := range grp.columnIndexes {
					r := &results[eIdx][col]
					if len(r.Values) > int(limit) {
						r.Values = r.Values[:limit]
						r.Statuses = r.Statuses[:limit]
						r.EventTimestamps = r.EventTimestamps[:limit]
					}
				}
			}
		}
	}

	return results, nil
}

////////////////////////////////////////////////////////////////////////////////
// BATCH CREATION
////////////////////////////////////////////////////////////////////////////////

// createBatchesWithIndices splits the keys into contiguous batches
// and includes the starting offset for result indexing.
func (v *ValkeyOnlineStore) createBatchesWithIndices(keys []*types.EntityKey) []batchInfo {
	n := len(keys)
	bs := v.ReadBatchSize
	nBatches := int(math.Ceil(float64(n) / float64(bs)))

	batches := make([]batchInfo, nBatches)
	nAssigned := 0

	for i := 0; i < nBatches; i++ {
		startIdx := i * bs
		size := int(math.Min(float64(bs), float64(n-nAssigned)))
		nAssigned += size
		batches[i] = batchInfo{
			keys:     keys[startIdx : startIdx+size],
			startIdx: startIdx,
		}
	}

	return batches
}

////////////////////////////////////////////////////////////////////////////////
// MISC
////////////////////////////////////////////////////////////////////////////////

// Destruct satisfies the OnlineStore interface but is a no-op.
func (v *ValkeyOnlineStore) Destruct() {}

// buildValkeyKey serializes an entity key and appends the project name.
func buildValkeyKey(project string, ek *types.EntityKey, version int64) (*[]byte, error) {
	ser, err := utils.SerializeEntityKey(ek, version)
	if err != nil {
		return nil, err
	}
	full := append(*ser, []byte(project)...)
	return &full, nil
}

func (v *ValkeyOnlineStore) GetDataModelType() OnlineStoreDataModel {
	return EntityLevel
}

func (v *ValkeyOnlineStore) GetReadBatchSize() int {
	return v.ReadBatchSize
}
