package onlinestore

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
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

// Internal representation of Valkey deployment type
type valkeyType int

const (
	valkeyNode    valkeyType = 0
	valkeyCluster valkeyType = 1
)

// ValkeyOnlineStore implements the Feast OnlineStore interface
type ValkeyOnlineStore struct {
	project       string        // Feast project name
	t             valkeyType    // Node or Cluster mode
	client        valkey.Client // Valkey client connection
	config        *registry.RepoConfig
	ReadBatchSize int // Key batch size for HMGET
}

// mgetBatchResult holds intermediate results from a single MGET batch
// Used for collecting batch results in parallel MGET operations
type mgetBatchResult struct {
	memberIdx int                         // Original index of the member in the members slice
	memberKey string                      // Base64 encoded sort key
	values    map[int]interface{}         // featureIdx -> decoded value
	statuses  map[int]serving.FieldStatus // featureIdx -> field status
	timestamp timestamppb.Timestamp       // Event timestamp for this record
}

/* ------------------------------ CONNECTION SETUP ------------------------------ */

// Parses the JSON-style onlineStoreConfig map into a Valkey ClientOption struct.
func parseConnectionString(onlineStoreConfig map[string]interface{}, valkeyStoreType valkeyType) (valkey.ClientOption, error) {
	var clientOption valkey.ClientOption

	// Use replicas automatically for readonly commands
	clientOption.SendToReplicas = func(cmd valkey.Completed) bool {
		return cmd.IsReadOnly()
	}

	// Standalone replica support
	if valkeyStoreType == valkeyNode {
		if replicaAddressJson, ok := onlineStoreConfig["replica_address"]; ok {
			replicaAddress, ok := replicaAddressJson.(string)
			if !ok {
				return clientOption, fmt.Errorf("replica_address must be string")
			}

			// Multiple addresses separated by commas
			for _, part := range strings.Split(replicaAddress, ",") {
				if strings.Contains(part, ":") {
					clientOption.Standalone.ReplicaAddress = append(clientOption.Standalone.ReplicaAddress, part)
				} else {
					return clientOption, fmt.Errorf("invalid replica address: %s", part)
				}
			}
		} else {
			log.Warn().Msg("define replica_address or reader endpoint to read from cluster replicas")
		}
	}

	// Parse main connection string (can include password=db=ssl=)
	valkeyConn := defaultConnectionString
	if raw, ok := onlineStoreConfig["connection_string"]; ok {
		valkeyConn, ok = raw.(string)
		if !ok {
			return clientOption, fmt.Errorf("connection_string must be string")
		}
	}

	// Parse comma-separated parameters
	for _, part := range strings.Split(valkeyConn, ",") {
		switch {
		case strings.Contains(part, ":"): // host:port
			clientOption.InitAddress = append(clientOption.InitAddress, part)

		case strings.Contains(part, "="): // key=value options
			kv := strings.SplitN(part, "=", 2)
			switch kv[0] {
			case "password":
				clientOption.Password = kv[1]
			case "ssl":
				useSSL, err := strconv.ParseBool(kv[1])
				if err != nil {
					return clientOption, err
				}
				if useSSL {
					clientOption.TLSConfig = &tls.Config{}
				}
			case "db":
				db, err := strconv.Atoi(kv[1])
				if err != nil {
					return clientOption, err
				}
				clientOption.SelectDB = db
			default:
				return clientOption, fmt.Errorf("unknown connection_string option %s", kv[0])
			}

		default:
			return clientOption, fmt.Errorf("invalid connection_string part: %s", part)
		}
	}
	return clientOption, nil
}

// Provides the Datadog tracing service name for Valkey
func getValkeyTraceServiceName() string {
	if svc := os.Getenv("DD_SERVICE"); svc != "" {
		return svc + "-valkey"
	}
	return "valkey.client"
}

// Builds the actual client (traced or untraced)
func initializeValkeyClient(clientOption valkey.ClientOption, serviceName string) (valkey.Client, error) {
	if strings.ToLower(os.Getenv("ENABLE_ONLINE_STORE_TRACING")) == "true" {
		return valkeytrace.NewClient(clientOption, valkeytrace.WithService(serviceName))
	}
	return valkey.NewClient(clientOption)
}

// Factory for ValkeyOnlineStore
func NewValkeyOnlineStore(project string, config *registry.RepoConfig, onlineStoreConfig map[string]interface{}) (*ValkeyOnlineStore, error) {
	store := ValkeyOnlineStore{
		project: project,
		config:  config,
	}

	// Determine node vs cluster
	valkeyStoreType, err := getValkeyType(onlineStoreConfig)
	if err != nil {
		return nil, err
	}
	store.t = valkeyStoreType

	// Build connection options
	clientOpt, err := parseConnectionString(onlineStoreConfig, valkeyStoreType)
	if err != nil {
		return nil, err
	}

	// Instantiate client
	store.client, err = initializeValkeyClient(clientOpt, getValkeyTraceServiceName())
	if err != nil {
		return nil, err
	}

	// Parse batch size
	readBatchSize := 100.0 // default
	if raw, ok := onlineStoreConfig["read_batch_size"]; ok {
		readBatchSize, ok = raw.(float64)
		if !ok {
			return nil, fmt.Errorf("read_batch_size must be numeric")
		}
	}
	store.ReadBatchSize = int(readBatchSize)

	log.Info().Msgf("Reads will be done in key batches of size: %d", store.ReadBatchSize)
	log.Info().Msgf("Using Valkey: %s", clientOpt.InitAddress)

	return &store, nil
}

// Parses valkey_type string into enum
func getValkeyType(onlineStoreConfig map[string]interface{}) (valkeyType, error) {
	typ := "valkey"
	if raw, ok := onlineStoreConfig["valkey_type"]; ok {
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
		return -1, fmt.Errorf("invalid valkey_type: %s", typ)
	}
}

/* ------------------------------ FEATURE INDEXING ------------------------------ */

// Maps each feature view to an index (used for timestamp lookup)
func (v *ValkeyOnlineStore) buildFeatureViewIndices(featureViewNames []string, featureNames []string) (map[string]int, map[int]string, int) {
	featureViewIndices := make(map[string]int)
	indicesFeatureView := make(map[int]string)

	// Start after feature columns (timestamp columns appended after)
	index := len(featureNames)

	for _, fv := range featureViewNames {
		if _, exists := featureViewIndices[fv]; !exists {
			featureViewIndices[fv] = index
			indicesFeatureView[index] = fv
			index++
		}
	}
	return featureViewIndices, indicesFeatureView, index
}

// Computes hashed HSET field names for features + raw timestamp keys
func (v *ValkeyOnlineStore) buildHsetKeys(featureViewNames []string, featureNames []string, indicesFeatureView map[int]string, index int) ([]string, []string) {
	featureCount := len(featureNames)
	hsetKeys := make([]string, index)
	h := murmur3.New32()
	byteBuffer := make([]byte, 4)

	// Hash all feature fields
	for i := 0; i < featureCount; i++ {
		h.Write([]byte(fmt.Sprintf("%s:%s", featureViewNames[i], featureNames[i])))
		binary.LittleEndian.PutUint32(byteBuffer, h.Sum32())
		hsetKeys[i] = string(byteBuffer)
		h.Reset()
	}

	// Add timestamp fields
	for i := featureCount; i < index; i++ {
		view := indicesFeatureView[i]
		tsKey := fmt.Sprintf("_ts:%s", view)
		hsetKeys[i] = tsKey
		featureNames = append(featureNames, tsKey)
	}
	return hsetKeys, featureNames
}

// Builds redis keys like: serialized_entity_key + project_name
func (v *ValkeyOnlineStore) buildValkeyKeys(entityKeys []*types.EntityKey) ([]*[]byte, error) {
	out := make([]*[]byte, len(entityKeys))
	for i, ek := range entityKeys {
		key, err := buildValkeyKey(v.project, ek, v.config.EntityKeySerializationVersion)
		if err != nil {
			return nil, err
		}
		out[i] = key
	}
	return out, nil
}

/* ------------------------------ ONLINE READ (ENTITY LEVEL) ------------------------------ */

// Feast compatibility wrapper
func (v *ValkeyOnlineStore) OnlineReadV2(ctx context.Context, entityKeys []*types.EntityKey, featureViewNames []string, featureNames []string) ([][]FeatureData, error) {
	return v.OnlineRead(ctx, entityKeys, featureViewNames, featureNames)
}

// Reads features via HMGET
func (v *ValkeyOnlineStore) OnlineRead(ctx context.Context, entityKeys []*types.EntityKey, featureViewNames []string, featureNames []string) ([][]FeatureData, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "OnlineRead")
	defer span.Finish()

	featureCount := len(featureNames)

	// Setup indexing of timestamps per FV
	fvIdx, idxFV, totalCols := v.buildFeatureViewIndices(featureViewNames, featureNames)
	hsetKeys, featureNamesWithTS := v.buildHsetKeys(featureViewNames, featureNames, idxFV, totalCols)

	// Build redis keys
	valkeyKeys, err := v.buildValkeyKeys(entityKeys)
	if err != nil {
		return nil, err
	}

	results := make([][]FeatureData, len(entityKeys))

	// Generate HMGET commands
	cmds := make(valkey.Commands, 0, len(entityKeys))
	for _, key := range valkeyKeys {
		cmds = append(cmds,
			v.client.B().Hmget().Key(string(*key)).Field(hsetKeys...).Build(),
		)
	}

	// Execute pipelined HMGET
	for entityIndex, reply := range v.client.DoMulti(ctx, cmds...) {
		if err := reply.Error(); err != nil {
			return nil, err
		}

		arr, err := reply.ToArray()
		if err != nil {
			return nil, err
		}

		// If all features are nil → treat as "no data"
		containsNonNil := false
		entityResults := make([]FeatureData, featureCount)
		timeStampMap := make(map[string]*timestamppb.Timestamp)

		for i := 0; i < featureCount; i++ {
			fv := featureViewNames[i]
			fn := featureNamesWithTS[i]

			value := &types.Value{Val: &types.Value_NullVal{NullVal: types.Null_NULL}}

			// Extract feature value
			if !arr[i].IsNil() {
				containsNonNil = true

				rawStr, err := arr[i].ToString()
				if err != nil {
					return nil, err
				}

				vv, _, err := utils.UnmarshalStoredProto([]byte(rawStr))
				if err != nil {
					return nil, errors.New("invalid stored proto")
				}
				value = vv
			}

			// Extract timestamp for this feature view once
			if _, found := timeStampMap[fv]; !found {
				tsIndex := fvIdx[fv]
				ts := timestamppb.Timestamp{}

				if !arr[tsIndex].IsNil() {
					rawTS, err := arr[tsIndex].ToString()
					if err != nil {
						return nil, err
					}
					if err := proto.Unmarshal([]byte(rawTS), &ts); err != nil {
						return nil, errors.New("failed to unmarshal timestamp")
					}
				}

				timeStampMap[fv] = &timestamppb.Timestamp{Seconds: ts.Seconds, Nanos: ts.Nanos}
			}

			entityResults[i] = FeatureData{
				Reference: serving.FeatureReferenceV2{
					FeatureViewName: fv,
					FeatureName:     fn,
				},
				Timestamp: *timeStampMap[fv],
				Value:     types.Value{Val: value.Val},
			}
		}

		// If no values exist for this entity
		if !containsNonNil {
			results[entityIndex] = nil
		} else {
			results[entityIndex] = entityResults
		}
	}

	return results, nil
}

/* ------------------------------ RANGE READ SUPPORT ------------------------------ */

// processEntityKey handles all processing for a single entity key in OnlineReadRange.
// This includes entity key serialization, result initialization, ZRANGE execution,
// and HMGET calls for all feature views.
func (v *ValkeyOnlineStore) processEntityKey(
	ctx context.Context,
	eIdx int,
	entityKey *types.EntityKey,
	fvGroups map[string]*fvGroup,
	effectiveReverse bool,
	minScore, maxScore string,
	limit int64,
	results [][]RangeFeatureData,
	featNames, fvNames []string,
) error {
	// Check context cancellation before starting
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Serialize entity key
	entityKeyBin, err := SerializeEntityKeyWithProject(v.project, entityKey, v.config.EntityKeySerializationVersion)
	if err != nil {
		return fmt.Errorf("failed to serialize entity key: %w", err)
	}

	// Initialize empty rows for this entity
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

	/* ------------------------------ ZRANGE ------------------------------ */

	// Check context cancellation before ZRANGE operations
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Build ZRANGE commands per feature view
	zCmds := make([]valkey.Completed, 0, len(fvGroups))
	fvOrder := make([]string, 0, len(fvGroups))

	for fv := range fvGroups {
		zkey := utils.BuildZsetKey(fv, entityKeyBin)
		var cmd valkey.Completed

		if effectiveReverse {
			cmd = v.client.B().
				Zrange().
				Key(zkey).
				Min(maxScore).Max(minScore).Byscore().Rev().
				Build()
		} else {
			cmd = v.client.B().
				Zrange().
				Key(zkey).
				Min(minScore).Max(maxScore).Byscore().
				Build()
		}

		zCmds = append(zCmds, cmd)
		fvOrder = append(fvOrder, fv)
	}

	// Execute in pipeline
	zResults := v.client.DoMulti(ctx, zCmds...)

	// Decode ZRANGE results
	zMembers := map[string][][]byte{}
	for i, fv := range fvOrder {
		raw := zResults[i]
		if err := raw.Error(); err != nil {
			zMembers[fv] = nil
			continue
		}

		arr, err := raw.ToArray()
		if err != nil {
			zMembers[fv] = nil
			continue
		}

		out := make([][]byte, 0, len(arr))
		for _, itm := range arr {
			if itm.IsNil() {
				continue
			}
			s, _ := itm.ToString()
			out = append(out, []byte(s))
		}

		zMembers[fv] = out
	}

	/* ------------------------------ HMGET ------------------------------ */

	// Check context cancellation before HMGET operations
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	for fv, grp := range fvGroups {
		members := zMembers[fv]

		// If no records exist → mark NOT_FOUND
		if len(members) == 0 {
			for _, col := range grp.columnIndexes {
				results[eIdx][col].Values = append(results[eIdx][col].Values, nil)
				results[eIdx][col].Statuses = append(results[eIdx][col].Statuses, serving.FieldStatus_NOT_FOUND)
				results[eIdx][col].EventTimestamps = append(results[eIdx][col].EventTimestamps, timestamppb.Timestamp{})
			}
			continue
		}

		// Fields = feature hashes + timestamp column
		fields := append(append([]string{}, grp.fieldHashes...), grp.tsKey)

		// Perform pipelined HMGET batches
		if err := valkeyBatchHMGET(
			ctx,
			v.client,
			entityKeyBin,
			members,
			fields,
			fv,
			grp,
			results,
			eIdx,
			v.ReadBatchSize,
		); err != nil {
			return err
		}

		// Apply limit per feature column
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

	return nil
}

// Performs pipelined HMGET batches for Range API with parallel batch execution
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
	if len(members) == 0 {
		return nil
	}

	// Build keys for MGET lookup
	keys := make([]string, len(members))
	for i, sk := range members {
		keys[i] = utils.BuildHashKey(entityKeyBin, sk)
	}

	// Calculate number of batches based on members length and batchSize
	numBatches := (len(members) + batchSize - 1) / batchSize
	if batchSize <= 0 || batchSize == 1 {
		// Batching disabled - process all in single batch
		numBatches = 1
		batchSize = len(members)
	}

	// Collect batch results in order-preserving structure
	// Pre-allocate slice to hold results for each member
	batchResults := make([]*mgetBatchResult, len(members))

	// Create WaitGroup and error channel for batch goroutines
	var wg sync.WaitGroup
	errChan := make(chan error, numBatches)

	// Launch goroutine for each batch executing MGET
	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		// Check context cancellation before launching each batch
		select {
		case <-ctx.Done():
			// Wait for any already-launched goroutines to complete
			wg.Wait()
			close(errChan)
			return ctx.Err()
		default:
		}

		start := batchIdx * batchSize
		end := start + batchSize
		if end > len(members) {
			end = len(members)
		}

		wg.Add(1)
		go func(bIdx, startIdx, endIdx int) {
			defer wg.Done()

			// Check context cancellation before executing batch
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
			}

			// Get batch slice of keys
			batchKeys := keys[startIdx:endIdx]
			batchMembers := members[startIdx:endIdx]

			// Execute MGET for this batch
			raw, err := client.Do(ctx, client.B().Mget().Key(batchKeys...).Build()).AsStrSlice()
			if err != nil {
				errChan <- fmt.Errorf("mget batch %d failed: %w", bIdx, err)
				return
			}

			// Process each member in this batch
			for i, sk := range batchMembers {
				memberIdx := startIdx + i // Original index in members slice

				if i >= len(raw) || raw[i] == "" {
					continue
				}

				// Parse JSON record
				var record map[string]interface{}
				if err := json.Unmarshal([]byte(raw[i]), &record); err != nil {
					continue
				}

				// Check if all feature fields are nil
				allNil := true
				for _, field := range fields[:len(fields)-1] { // exclude timestamp field
					if val, ok := record[field]; ok && val != nil {
						allNil = false
						break
					}
				}
				if allNil {
					continue
				}

				// Decode timestamp
				var ts timestamppb.Timestamp
				if tsRaw, ok := record[fields[len(fields)-1]]; ok {
					if s, ok := tsRaw.(string); ok {
						ts = utils.DecodeTimestamp(s)
					}
				}

				// Encode base64 sort key
				memberKey := base64.StdEncoding.EncodeToString(sk)

				// Build result for this member
				result := &mgetBatchResult{
					memberIdx: memberIdx,
					memberKey: memberKey,
					values:    make(map[int]interface{}),
					statuses:  make(map[int]serving.FieldStatus),
					timestamp: ts,
				}

				// Decode each feature column
				for colIdx := range grp.columnIndexes {
					fieldName := fields[colIdx]

					rawVal, exists := record[fieldName]
					var (
						val    interface{}
						status serving.FieldStatus
					)

					if !exists || rawVal == nil {
						status = serving.FieldStatus_NULL_VALUE
					} else {
						// Normalize to string
						var strVal string
						switch v := rawVal.(type) {
						case string:
							strVal = v
						default:
							strVal = fmt.Sprintf("%v", v)
						}
						val, status = utils.DecodeFeatureValue(strVal, fv, grp.featNames[colIdx], memberKey)
						if status == serving.FieldStatus_NULL_VALUE {
							val = nil
						}
					}

					result.values[colIdx] = val
					result.statuses[colIdx] = status
				}

				// Store result at original member index (thread-safe: each goroutine writes to distinct indices)
				batchResults[memberIdx] = result
			}
		}(batchIdx, start, end)
	}

	// Wait for all batch goroutines to complete
	wg.Wait()
	close(errChan)

	// Aggregate batch errors and return
	var allErrors []error
	for err := range errChan {
		allErrors = append(allErrors, err)
	}
	if len(allErrors) > 0 {
		return errors.Join(allErrors...)
	}

	// Merge batch results preserving order - process in member order
	for _, result := range batchResults {
		if result == nil {
			continue
		}

		// Append values, statuses, and timestamps to results slice in original order
		for colIdx, col := range grp.columnIndexes {
			val := result.values[colIdx]
			status := result.statuses[colIdx]

			results[eIdx][col].Values = append(results[eIdx][col].Values, val)
			results[eIdx][col].Statuses = append(results[eIdx][col].Statuses, status)
			results[eIdx][col].EventTimestamps = append(results[eIdx][col].EventTimestamps, result.timestamp)
		}
	}

	return nil
}

/* ------------------------------ RANGE READ API ------------------------------ */

// Performs a Range lookup over ZSET + HASH data model
func (v *ValkeyOnlineStore) OnlineReadRange(
	ctx context.Context,
	groupedRefs *model.GroupedRangeFeatureRefs,
) ([][]RangeFeatureData, error) {

	if groupedRefs == nil || len(groupedRefs.EntityKeys) == 0 {
		return nil, fmt.Errorf("no entity keys provided")
	}

	featNames := groupedRefs.FeatureNames
	fvNames := groupedRefs.FeatureViewNames
	limit := int64(groupedRefs.Limit)

	// Compute forward/reverse and score filters
	effectiveReverse := utils.ComputeEffectiveReverse(groupedRefs.SortKeyFilters, groupedRefs.IsReverseSortOrder)
	minScore, maxScore := "-inf", "+inf"
	if len(groupedRefs.SortKeyFilters) > 0 {
		minScore, maxScore = utils.GetScoreRange(groupedRefs.SortKeyFilters)
	}

	// Group refs by feature view
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

	// Prepare output
	results := make([][]RangeFeatureData, len(groupedRefs.EntityKeys))

	// Create WaitGroup for goroutine synchronization and buffered error channel
	var wg sync.WaitGroup
	errChan := make(chan error, len(groupedRefs.EntityKeys))

	// Process each entity key in parallel using goroutines
	for eIdx, entityKey := range groupedRefs.EntityKeys {
		wg.Add(1)
		go func(idx int, ek *types.EntityKey) {
			defer wg.Done()
			if err := v.processEntityKey(
				ctx,
				idx,
				ek,
				fvGroups,
				effectiveReverse,
				minScore, maxScore,
				limit,
				results,
				featNames, fvNames,
			); err != nil {
				errChan <- err
			}
		}(eIdx, entityKey)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(errChan)

	// Aggregate all errors from the channel
	var allErrors []error
	for err := range errChan {
		allErrors = append(allErrors, err)
	}
	if len(allErrors) > 0 {
		return nil, errors.Join(allErrors...)
	}

	return results, nil
}

/* ------------------------------ MISC ------------------------------ */

func (v *ValkeyOnlineStore) Destruct() {}

func buildValkeyKey(project string, entityKey *types.EntityKey, version int64) (*[]byte, error) {
	serKey, err := utils.SerializeEntityKey(entityKey, version)
	if err != nil {
		return nil, err
	}

	fullKey := append(*serKey, []byte(project)...)
	return &fullKey, nil
}

func (v *ValkeyOnlineStore) GetDataModelType() OnlineStoreDataModel {
	return EntityLevel
}

func (v *ValkeyOnlineStore) GetReadBatchSize() int {
	return v.ReadBatchSize
}
