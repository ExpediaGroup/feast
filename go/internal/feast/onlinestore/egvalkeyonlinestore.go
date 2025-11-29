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
	"time"

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
	// Feast project name
	project string

	// Valkey database type, either a single node server (valkeyNode) or a cluster (valkeyCluster)
	t valkeyType

	// Valkey client connector
	client valkey.Client

	config *registry.RepoConfig

	// Number of keys to read in a batch
	ReadBatchSize int
}

// fvGroup groups features by feature view for range reads.
// This is the original structure you wanted to keep.

// mgetBatchResult holds intermediate results from a single HMGET batch
// Used for collecting batch results in parallel HMGET operations while
// preserving order and avoiding data races.
type mgetBatchResult struct {
	memberIdx int                         // Original index of the member in the members slice
	memberKey string                      // Base64 encoded sort key
	values    map[int]interface{}         // localFeatureIdx -> decoded value
	statuses  map[int]serving.FieldStatus // localFeatureIdx -> field status
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
		replicaAddressJson, ok := onlineStoreConfig["replica_address"]
		if !ok {
			log.Warn().Msg("define replica_address or reader endpoint to read from cluster replicas")
		} else {
			replicaAddress, ok := replicaAddressJson.(string)
			if !ok {
				return clientOption, fmt.Errorf("failed to convert replica_address to string: %+v", replicaAddressJson)
			}

			for _, part := range strings.Split(replicaAddress, ",") {
				if strings.Contains(part, ":") {
					clientOption.Standalone.ReplicaAddress = append(clientOption.Standalone.ReplicaAddress, part)
				} else {
					return clientOption, fmt.Errorf("unable to parse part of replica_address: %s", part)
				}
			}
		}
	}

	// Parse main connection string (can include password=db=ssl=)
	valkeyConn := defaultConnectionString
	if raw, ok := onlineStoreConfig["connection_string"]; ok {
		valkeyConn, ok = raw.(string)
		if !ok {
			return clientOption, fmt.Errorf("failed to convert connection_string to string: %+v", raw)
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
				return clientOption, fmt.Errorf("unrecognized option in connection_string: %s", kv[0])
			}

		default:
			return clientOption, fmt.Errorf("unable to parse part of connection_string: %s", part)
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
	if raw, ok := onlineStoreConfig["read_batch_size"]; !ok {
		readBatchSize = 100.0
	} else if readBatchSize, ok = raw.(float64); !ok {
		return nil, fmt.Errorf("failed to convert read_batch_size: %+v", raw)
	}
	store.ReadBatchSize = int(readBatchSize)

	if store.ReadBatchSize >= 1 {
		log.Info().Msgf("Reads will be done in key batches of size: %d", store.ReadBatchSize)
	}

	log.Info().Msgf("Using Valkey: %s", clientOpt.InitAddress)
	return &store, nil
}

// Parses valkey_type string into enum.
func getValkeyType(onlineStoreConfig map[string]interface{}) (valkeyType, error) {
	typ := "valkey"
	if raw, ok := onlineStoreConfig["valkey_type"]; ok {
		var ok2 bool
		typ, ok2 = raw.(string)
		if !ok2 {
			return -1, fmt.Errorf("failed to convert valkey_type to string: %+v", raw)
		}
	}

	switch typ {
	case "valkey":
		return valkeyNode, nil
	case "valkey_cluster":
		return valkeyCluster, nil
	default:
		return -1, fmt.Errorf("failed to convert valkey_type to enum: %s. Must be one of 'valkey', 'valkey_cluster'", typ)
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

// Builds valkey keys like: serialized_entity_key + project_name
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

// Reads features via HMGET (logic from original code, with slightly cleaned structure).
func (v *ValkeyOnlineStore) OnlineRead(ctx context.Context, entityKeys []*types.EntityKey, featureViewNames []string, featureNames []string) ([][]FeatureData, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "OnlineRead")
	defer span.Finish()

	featureCount := len(featureNames)

	// Setup indexing of timestamps per FV
	featureViewIndices, indicesFeatureView, index := v.buildFeatureViewIndices(featureViewNames, featureNames)
	hsetKeys, featureNamesWithTS := v.buildHsetKeys(featureViewNames, featureNames, indicesFeatureView, index)

	// Build valkey keys
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

		res, err := reply.ToArray()
		if err != nil {
			return nil, err
		}

		resContainsNonNil := false
		results[entityIndex] = make([]FeatureData, featureCount)
		timeStampMap := make(map[string]*timestamppb.Timestamp, 1)

		for featureIndex := 0; featureIndex < featureCount; featureIndex++ {
			featureValue := res[featureIndex]

			featureName := featureNamesWithTS[featureIndex]
			featureViewName := featureViewNames[featureIndex]

			value := &types.Value{Val: &types.Value_NullVal{NullVal: types.Null_NULL}}

			if !featureValue.IsNil() {
				valueStr, err := featureValue.ToString()
				if err != nil {
					return nil, err
				}
				value, _, err = utils.UnmarshalStoredProto([]byte(valueStr))
				if err != nil {
					return nil, errors.New("error converting parsed valkey Value to types.Value")
				}
				resContainsNonNil = true
			}

			if _, ok := timeStampMap[featureViewName]; !ok {
				timeStamp := timestamppb.Timestamp{}
				timeStampIndex := featureViewIndices[featureViewName]

				if !res[timeStampIndex].IsNil() {
					timeStampString, err := res[timeStampIndex].ToString()
					if err != nil {
						return nil, err
					}
					if err := proto.Unmarshal([]byte(timeStampString), &timeStamp); err != nil {
						return nil, errors.New("error converting parsed valkey Value to timestamppb.Timestamp")
					}
				}
				timeStampMap[featureViewName] = &timestamppb.Timestamp{Seconds: timeStamp.Seconds, Nanos: timeStamp.Nanos}
			}

			results[entityIndex][featureIndex] = FeatureData{
				Reference: serving.FeatureReferenceV2{
					FeatureViewName: featureViewName,
					FeatureName:     featureName,
				},
				Timestamp: timestamppb.Timestamp{
					Seconds: timeStampMap[featureViewName].Seconds,
					Nanos:   timeStampMap[featureViewName].Nanos,
				},
				Value: types.Value{Val: value.Val},
			}
		}

		// If all features are nil → treat as "no data"
		if !resContainsNonNil {
			results[entityIndex] = nil
		}
	}

	return results, nil
}

/* ------------------------------ RANGE READ SUPPORT ------------------------------ */

// processEntityKey handles all processing for a single entity key in OnlineReadRange.
// This includes entity key serialization, result initialization, ZRANGE execution,
// and HMGET calls for all feature views (parallel & batched).
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
	start := time.Now()
	log.Debug().Msgf("OnlineReadRange: processEntityKey[%d]: start", eIdx)
	defer func() {
		dur := time.Since(start)
		log.Debug().Msgf("OnlineReadRange: processEntityKey[%d]: completed in %s", eIdx, dur)
	}()

	// Check context cancellation before starting
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Serialize entity key
	entityKeyBin, err := SerializeEntityKeyWithProject(
		v.project,
		entityKey,
		v.config.EntityKeySerializationVersion,
	)
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

	zrangeStart := time.Now()

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
			s, err := itm.ToString()
			if err != nil {
				continue
			}
			out = append(out, []byte(s))
		}

		zMembers[fv] = out
	}

	log.Debug().Msgf(
		"OnlineReadRange: processEntityKey[%d]: ZRANGE completed in %s (feature_views=%d)",
		eIdx, time.Since(zrangeStart), len(fvGroups),
	)

	/* ------------------------------ HMGET ------------------------------ */

	hmStart := time.Now()

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

		// Perform pipelined HMGET batches (original logic, with
		// order-preserving, race-free aggregation).
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

	log.Debug().Msgf(
		"OnlineReadRange: processEntityKey[%d]: HMGET+merge completed in %s",
		eIdx, time.Since(hmStart),
	)

	return nil
}

// valkeyBatchHMGET executes HMGET in pipelined batches for a single feature view.
// This uses the original HMGET + DecodeFeatureValue logic, but aggregates
// results in memory and merges them in a single goroutine to avoid races.
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
	start := time.Now()
	log.Debug().Msgf(
		"OnlineReadRange: valkeyBatchHMGET: start (feature_view=%s, members=%d, batch_size=%d)",
		fv, len(members), batchSize,
	)
	defer func() {
		dur := time.Since(start)
		log.Debug().Msgf(
			"OnlineReadRange: valkeyBatchHMGET: completed in %s (feature_view=%s, members=%d)",
			dur, fv, len(members),
		)
	}()

	if len(members) == 0 {
		return nil
	}
	if batchSize <= 0 {
		batchSize = len(members)
	}

	nBatches := (len(members) + batchSize - 1) / batchSize

	// Pre-allocate slice to hold results for each member index
	batchResults := make([]*mgetBatchResult, len(members))

	var wg sync.WaitGroup
	errChan := make(chan error, nBatches)

	for b := 0; b < nBatches; b++ {
		// Check context cancellation before launching each batch
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
		go func(batchIdx, startIdx int, batch [][]byte) {
			defer wg.Done()

			// Check context cancellation before executing batch
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
			}

			// Build all HMGET commands for this batch
			cmds := make([]valkey.Completed, 0, len(batch))
			for _, sortKeyBytes := range batch {
				hashKey := utils.BuildHashKey(entityKeyBin, sortKeyBytes)
				cmds = append(cmds, client.B().Hmget().Key(hashKey).Field(fields...).Build())
			}

			multi := client.DoMulti(ctx, cmds...)

			for i, sortKeyBytes := range batch {
				memberIdx := startIdx + i
				memberKey := base64.StdEncoding.EncodeToString(sortKeyBytes)
				cmdRes := multi[i]

				if err := cmdRes.Error(); err != nil {
					// mimic original behavior: just skip this member
					continue
				}

				arr, err := cmdRes.ToArray()
				if err != nil || len(arr) == 0 {
					continue
				}

				featureFieldCount := len(grp.featNames)

				// All-nil detection (excluding timestamp field)
				allNil := true
				for fi := 0; fi < featureFieldCount && fi < len(arr)-1; fi++ {
					if !arr[fi].IsNil() {
						allNil = false
						break
					}
				}
				if allNil {
					continue
				}

				// Decode timestamp (last element)
				var eventTS timestamppb.Timestamp
				if len(arr) > 0 {
					tsVal := arr[len(arr)-1]
					if !tsVal.IsNil() {
						tsStr, err := tsVal.ToString()
						if err == nil {
							eventTS = utils.DecodeTimestamp(tsStr)
						}
					}
				}

				// Build result for this member
				res := &mgetBatchResult{
					memberIdx: memberIdx,
					memberKey: memberKey,
					values:    make(map[int]interface{}),
					statuses:  make(map[int]serving.FieldStatus),
					timestamp: eventTS,
				}

				// Append feature values (original decoding logic)
				for localIdx, col := range grp.columnIndexes {
					fieldIdx := localIdx
					if fieldIdx >= len(arr)-1 {
						continue
					}

					fvResp := arr[fieldIdx]

					var (
						val    interface{}
						status serving.FieldStatus
					)

					if fvResp.IsNil() {
						val = nil
						status = serving.FieldStatus_NULL_VALUE
					} else {
						strVal, err := fvResp.ToString()
						if err != nil {
							continue
						}
						raw := interface{}(strVal)
						decoded, st := utils.DecodeFeatureValue(raw, fv, grp.featNames[localIdx], memberKey)
						if st == serving.FieldStatus_NULL_VALUE {
							val = nil
						} else {
							val = decoded
						}
						status = st
					}

					_ = col // col is used only when merging; here we just keep localIdx
					res.values[localIdx] = val
					res.statuses[localIdx] = status
				}

				batchResults[memberIdx] = res
			}
		}(b, startIdx, batch)
	}

	wg.Wait()
	close(errChan)

	// Aggregate batch errors and return
	var allErrors []error
	for err := range errChan {
		if err != nil {
			allErrors = append(allErrors, err)
		}
	}
	if len(allErrors) > 0 {
		return errors.Join(allErrors...)
	}

	// Merge batch results preserving order (single goroutine; no race on results)
	for _, result := range batchResults {
		if result == nil {
			continue
		}
		for localIdx, col := range grp.columnIndexes {
			val := result.values[localIdx]
			status := result.statuses[localIdx]

			results[eIdx][col].Values = append(results[eIdx][col].Values, val)
			results[eIdx][col].Statuses = append(results[eIdx][col].Statuses, status)
			results[eIdx][col].EventTimestamps = append(results[eIdx][col].EventTimestamps, result.timestamp)
		}
	}

	return nil
}

/* ------------------------------ RANGE READ API ------------------------------ */

// Performs a Range lookup over ZSET + HASH data model.
// Uses the new parallel entity processing + original HMGET logic.
func (v *ValkeyOnlineStore) OnlineReadRange(
	ctx context.Context,
	groupedRefs *model.GroupedRangeFeatureRefs,
) ([][]RangeFeatureData, error) {

	start := time.Now()
	defer func() {
		dur := time.Since(start)
		log.Info().Msgf("OnlineReadRange: completed in %s", dur)
	}()

	if groupedRefs == nil || len(groupedRefs.EntityKeys) == 0 {
		log.Warn().Msg("OnlineReadRange: no entity keys provided")
		return nil, fmt.Errorf("no entity keys provided")
	}

	log.Info().Msgf(
		"OnlineReadRange: started (entity_keys=%d, features=%d, limit=%d)",
		len(groupedRefs.EntityKeys),
		len(groupedRefs.FeatureNames),
		groupedRefs.Limit,
	)

	featNames := groupedRefs.FeatureNames
	fvNames := groupedRefs.FeatureViewNames
	limit := int64(groupedRefs.Limit)

	// Compute forward/reverse and score filters
	effectiveReverse := utils.ComputeEffectiveReverse(
		groupedRefs.SortKeyFilters,
		groupedRefs.IsReverseSortOrder,
	)
	minScore, maxScore := "-inf", "+inf"
	if len(groupedRefs.SortKeyFilters) == 0 {
		// No predicate on sort key: fetch all, subject to Limit.
		minScore = "-inf"
		maxScore = "+inf"
	} else {
		minScore, maxScore = utils.GetScoreRange(groupedRefs.SortKeyFilters)
		if len(groupedRefs.SortKeyFilters) > 1 {
			log.Warn().
				Int("sort_key_count", len(groupedRefs.SortKeyFilters)).
				Msg("OnlineReadRange: more than one sort key filter provided; only the first will be used")
		}
	}

	// Group refs by feature view
	groupStart := time.Now()
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
	log.Debug().Msgf(
		"OnlineReadRange: feature view grouping completed in %s (feature_views=%d)",
		time.Since(groupStart),
		len(fvGroups),
	)

	// Prepare output
	results := make([][]RangeFeatureData, len(groupedRefs.EntityKeys))

	// Create WaitGroup for goroutine synchronization and buffered error channel
	var wg sync.WaitGroup
	errChan := make(chan error, len(groupedRefs.EntityKeys))

	log.Debug().Msgf(
		"OnlineReadRange: launching %d entity workers (limit=%d, reverse=%v, score=[%s,%s])",
		len(groupedRefs.EntityKeys),
		limit,
		effectiveReverse,
		minScore,
		maxScore,
	)

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

	wg.Wait()
	log.Debug().Msg("OnlineReadRange: all entity workers finished")
	close(errChan)

	// Aggregate all errors from the channel
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

/* ------------------------------ MISC ------------------------------ */

func (v *ValkeyOnlineStore) Destruct() {}

// buildValkeyKey: same semantics as original; just slightly renamed param.
func buildValkeyKey(project string, entityKey *types.EntityKey, entityKeySerializationVersion int64) (*[]byte, error) {
	serKey, err := utils.SerializeEntityKey(entityKey, entityKeySerializationVersion)
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
