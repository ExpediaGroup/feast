package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/feast-dev/feast/go/internal/feast"
	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/onlineserving"
	"github.com/feast-dev/feast/go/internal/feast/server/logging"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	prototypes "github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/types"
	"github.com/rs/zerolog/log"
	httptrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/net/http"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type httpServer struct {
	fs             *feast.FeatureStore
	loggingService *logging.LoggingService
	server         *http.Server
}

// This represents mapping between a path and an http Handler.
// Note a handler can be created out of any func with type signature
// func(w http.ResponseWriter, r *http.Request) via HandleFunc()
type Handler struct {
	path        string
	handlerFunc http.Handler
}

// Some Feast types aren't supported during JSON conversion
type repeatedValue struct {
	stringVal     []string
	int64Val      []int64
	doubleVal     []float64
	boolVal       []bool
	stringListVal [][]string
	int64ListVal  [][]int64
	doubleListVal [][]float64
	boolListVal   [][]bool
}

func (u *repeatedValue) UnmarshalJSON(data []byte) error {
	isString := false
	isDouble := false
	isInt64 := false
	isArray := false
	openBraketCounter := 0
	for _, b := range data {
		if b == '"' {
			isString = true
		}
		if b == '.' {
			isDouble = true
		}
		if b >= '0' && b <= '9' {
			isInt64 = true
		}
		if b == '[' {
			openBraketCounter++
			if openBraketCounter > 1 {
				isArray = true
			}
		}
	}
	var err error
	if !isArray {
		if isString {
			err = json.Unmarshal(data, &u.stringVal)
		} else if isDouble {
			err = json.Unmarshal(data, &u.doubleVal)
		} else if isInt64 {
			err = json.Unmarshal(data, &u.int64Val)
		} else {
			err = json.Unmarshal(data, &u.boolVal)
		}
	} else {
		if isString {
			err = json.Unmarshal(data, &u.stringListVal)
		} else if isDouble {
			err = json.Unmarshal(data, &u.doubleListVal)
		} else if isInt64 {
			err = json.Unmarshal(data, &u.int64ListVal)
		} else {
			err = json.Unmarshal(data, &u.boolListVal)
		}
	}
	return err
}

func parseValueFromJSON(data json.RawMessage) (*prototypes.Value, error) {
	var result prototypes.Value

	var stringVal string
	if err := json.Unmarshal(data, &stringVal); err == nil {
		result.Val = &prototypes.Value_StringVal{StringVal: stringVal}
		return &result, nil
	}

	var intVal int64
	if err := json.Unmarshal(data, &intVal); err == nil {
		result.Val = &prototypes.Value_Int64Val{Int64Val: intVal}
		return &result, nil
	}

	var floatVal float64
	if err := json.Unmarshal(data, &floatVal); err == nil {
		result.Val = &prototypes.Value_DoubleVal{DoubleVal: floatVal}
		return &result, nil
	}

	var boolVal bool
	if err := json.Unmarshal(data, &boolVal); err == nil {
		result.Val = &prototypes.Value_BoolVal{BoolVal: boolVal}
		return &result, nil
	}

	var valueObj map[string]interface{}
	if err := json.Unmarshal(data, &valueObj); err == nil {
		if timestampVal, ok := valueObj["unix_timestamp_val"]; ok {
			if ts, ok := timestampVal.(float64); ok {
				result.Val = &prototypes.Value_UnixTimestampVal{UnixTimestampVal: int64(ts)}
				return &result, nil
			}
		}
	}

	return nil, fmt.Errorf("could not parse JSON value: %s", string(data))
}

func processFeatureVectors(vectors []*onlineserving.RangeFeatureVector, includeMetadata bool, entitiesProto map[string]*prototypes.RepeatedValue) ([]string, []map[string]interface{}) {
	featureNames := make([]string, len(vectors))
	results := make([]map[string]interface{}, len(vectors))

	entityNames := make(map[string]bool)
	for entityName := range entitiesProto {
		entityNames[entityName] = true
	}

	for i, vector := range vectors {
		featureNames[i] = vector.Name
		result := make(map[string]interface{})

		rangeValues, err := types.ArrowValuesToRepeatedProtoValues(vector.RangeValues)
		if err != nil {
			result["values"] = []interface{}{}
			results[i] = result
			continue
		}

		isEntity := entityNames[vector.Name]

		if isEntity {
			entityValues := make([]interface{}, len(rangeValues))
			for j, repeatedValue := range rangeValues {
				if repeatedValue == nil || len(repeatedValue.Val) == 0 {
					entityValues[j] = nil
				} else {
					if j < len(vector.RangeStatuses) && len(vector.RangeStatuses[j]) > 0 {
						statusCode := vector.RangeStatuses[j][0]
						if statusCode == serving.FieldStatus_NOT_FOUND ||
							statusCode == serving.FieldStatus_NULL_VALUE {
							entityValues[j] = nil
						} else {
							entityValues[j] = types.ValueTypeToGoType(repeatedValue.Val[0])
						}
					} else {
						entityValues[j] = types.ValueTypeToGoType(repeatedValue.Val[0])
					}
				}
			}
			result["values"] = entityValues
		} else {
			simplifiedValues := make([]interface{}, len(rangeValues))
			for j, repeatedValue := range rangeValues {
				if repeatedValue == nil || len(repeatedValue.Val) == 0 {
					simplifiedValues[j] = nil
					continue
				}

				rangeForEntity := make([]interface{}, len(repeatedValue.Val))
				for k, val := range repeatedValue.Val {
					if j < len(vector.RangeStatuses) && k < len(vector.RangeStatuses[j]) {
						statusCode := vector.RangeStatuses[j][k]
						if statusCode == serving.FieldStatus_NOT_FOUND ||
							statusCode == serving.FieldStatus_NULL_VALUE {
							rangeForEntity[k] = nil
							continue
						}
					}

					if val == nil {
						rangeForEntity[k] = nil
					} else {
						rangeForEntity[k] = types.ValueTypeToGoType(val)
					}
				}
				simplifiedValues[j] = rangeForEntity
			}
			result["values"] = simplifiedValues
		}

		if includeMetadata {
			if len(vector.RangeStatuses) > 0 {
				statusValues := make([][]string, len(vector.RangeStatuses))
				for j, entityStatuses := range vector.RangeStatuses {
					statusValues[j] = make([]string, len(entityStatuses))
					for k, stat := range entityStatuses {
						statusValues[j][k] = stat.String()
					}
				}
				result["statuses"] = statusValues
			} else {
				result["statuses"] = [][]string{}
			}

			if len(vector.RangeTimestamps) > 0 {
				timestampValues := make([][]interface{}, len(vector.RangeTimestamps))
				for j, entityTimestamps := range vector.RangeTimestamps {
					timestampValues[j] = make([]interface{}, len(entityTimestamps))
					for k, ts := range entityTimestamps {
						if j < len(vector.RangeStatuses) && k < len(vector.RangeStatuses[j]) {
							statusCode := vector.RangeStatuses[j][k]
							if statusCode == serving.FieldStatus_NOT_FOUND ||
								statusCode == serving.FieldStatus_NULL_VALUE {
								timestampValues[j][k] = nil
								continue
							}
						}
						timestampValues[j][k] = ts.AsTime().Format(time.RFC3339)
					}
				}
				result["event_timestamps"] = timestampValues
			} else {
				result["event_timestamps"] = [][]interface{}{}
			}
		}

		results[i] = result
	}

	return featureNames, results
}

func (u *repeatedValue) ToProto() *prototypes.RepeatedValue {
	proto := new(prototypes.RepeatedValue)
	if u.stringVal != nil {
		for _, val := range u.stringVal {
			proto.Val = append(proto.Val, &prototypes.Value{Val: &prototypes.Value_StringVal{StringVal: val}})
		}
	}
	if u.int64Val != nil {
		for _, val := range u.int64Val {
			proto.Val = append(proto.Val, &prototypes.Value{Val: &prototypes.Value_Int64Val{Int64Val: val}})
		}
	}
	if u.doubleVal != nil {
		for _, val := range u.doubleVal {
			proto.Val = append(proto.Val, &prototypes.Value{Val: &prototypes.Value_DoubleVal{DoubleVal: val}})
		}
	}
	if u.boolVal != nil {
		for _, val := range u.boolVal {
			proto.Val = append(proto.Val, &prototypes.Value{Val: &prototypes.Value_BoolVal{BoolVal: val}})
		}
	}
	if u.stringListVal != nil {
		for _, val := range u.stringListVal {
			proto.Val = append(proto.Val, &prototypes.Value{Val: &prototypes.Value_StringListVal{StringListVal: &prototypes.StringList{Val: val}}})
		}
	}
	if u.int64ListVal != nil {
		for _, val := range u.int64ListVal {
			proto.Val = append(proto.Val, &prototypes.Value{Val: &prototypes.Value_Int64ListVal{Int64ListVal: &prototypes.Int64List{Val: val}}})
		}
	}
	if u.doubleListVal != nil {
		for _, val := range u.doubleListVal {
			proto.Val = append(proto.Val, &prototypes.Value{Val: &prototypes.Value_DoubleListVal{DoubleListVal: &prototypes.DoubleList{Val: val}}})
		}
	}
	if u.boolListVal != nil {
		for _, val := range u.boolListVal {
			proto.Val = append(proto.Val, &prototypes.Value{Val: &prototypes.Value_BoolListVal{BoolListVal: &prototypes.BoolList{Val: val}}})
		}
	}
	return proto
}

func (filter sortKeyFilter) ToProto() (*serving.SortKeyFilter, error) {
	proto := &serving.SortKeyFilter{
		SortKeyName: filter.SortKeyName,
	}

	if filter.Equals == nil && filter.Range.RangeStart == nil && filter.Range.RangeEnd == nil {
		return nil, fmt.Errorf("SortKeyFilter must have either equals or range")
	}

	if filter.Equals != nil && (filter.Range.RangeStart != nil || filter.Range.RangeEnd != nil) {
		return nil, fmt.Errorf("SortKeyFilter must have either equals or range, but not both")
	}

	if filter.Equals != nil {
		value, err := parseValueFromJSON(filter.Equals)
		if err != nil {
			return nil, fmt.Errorf("error parsing equals filter: %w", err)
		}

		proto.Query = &serving.SortKeyFilter_Equals{
			Equals: value,
		}
		return proto, nil
	}

	rangeQuery := &serving.SortKeyFilter_RangeQuery{
		StartInclusive: filter.Range.StartInclusive,
		EndInclusive:   filter.Range.EndInclusive,
	}

	if filter.Range.RangeStart != nil {
		value, err := parseValueFromJSON(filter.Range.RangeStart)
		if err != nil {
			return nil, fmt.Errorf("error parsing range_start: %w", err)
		}
		rangeQuery.RangeStart = value
	}

	if filter.Range.RangeEnd != nil {
		value, err := parseValueFromJSON(filter.Range.RangeEnd)
		if err != nil {
			return nil, fmt.Errorf("error parsing range_end: %w", err)
		}
		rangeQuery.RangeEnd = value
	}

	proto.Query = &serving.SortKeyFilter_Range{
		Range: rangeQuery,
	}

	return proto, nil
}

type getOnlineFeaturesRequest struct {
	FeatureService   *string                  `json:"feature_service"`
	Features         []string                 `json:"features"`
	Entities         map[string]repeatedValue `json:"entities"`
	FullFeatureNames bool                     `json:"full_feature_names"`
	RequestContext   map[string]repeatedValue `json:"request_context"`
}

func NewHttpServer(fs *feast.FeatureStore, loggingService *logging.LoggingService) *httpServer {
	return &httpServer{fs: fs, loggingService: loggingService}
}

func (s *httpServer) getOnlineFeatures(w http.ResponseWriter, r *http.Request) {
	var err error
	var featureVectors []*onlineserving.FeatureVector

	span, ctx := tracer.StartSpanFromContext(r.Context(), "getOnlineFeatures", tracer.ResourceName("/get-online-features"))
	defer span.Finish(tracer.WithError(err))

	logSpanContext := LogWithSpanContext(span)

	if r.Method != "POST" {
		http.NotFound(w, r)
		return
	}

	includeMetadataQuery := r.URL.Query().Get("includeMetadata")

	includeMetadata := false
	if includeMetadataQuery != "" {
		includeMetadata, err = strconv.ParseBool(includeMetadataQuery)
		if err != nil {
			logSpanContext.Error().Err(err).Msg("Error parsing includeMetadata query parameter")
			writeJSONError(w, fmt.Errorf("Error parsing includeMetadata query parameter: %+v", err), http.StatusBadRequest)
			return
		}
	}

	decoder := json.NewDecoder(r.Body)
	var request getOnlineFeaturesRequest
	err = decoder.Decode(&request)
	if err != nil {
		logSpanContext.Error().Err(err).Msg("Error decoding JSON request data")
		writeJSONError(w, fmt.Errorf("Error decoding JSON request data: %+v", err), http.StatusInternalServerError)
		return
	}
	var featureService *model.FeatureService
	if request.FeatureService != nil {
		featureService, err = s.fs.GetFeatureService(*request.FeatureService)
		if err != nil {
			logSpanContext.Error().Err(err).Msg("Error getting feature service from registry")
			writeJSONError(w, fmt.Errorf("Error getting feature service from registry: %+v", err), http.StatusInternalServerError)
			return
		}
	}
	entitiesProto := make(map[string]*prototypes.RepeatedValue)
	for key, value := range request.Entities {
		entitiesProto[key] = value.ToProto()
	}
	requestContextProto := make(map[string]*prototypes.RepeatedValue)
	for key, value := range request.RequestContext {
		requestContextProto[key] = value.ToProto()
	}

	featureVectors, err = s.fs.GetOnlineFeatures(
		ctx,
		request.Features,
		featureService,
		entitiesProto,
		requestContextProto,
		request.FullFeatureNames)

	defer func() {
		if featureVectors != nil {
			go releaseCGOMemory(featureVectors)
		}
	}()

	if err != nil {
		logSpanContext.Error().Err(err).Msg("Error getting feature vector")
		writeJSONError(w, fmt.Errorf("Error getting feature vector: %+v", err), http.StatusInternalServerError)
		return
	}

	var featureNames []string
	var results []map[string]interface{}
	for _, vector := range featureVectors {
		featureNames = append(featureNames, vector.Name)
		result := make(map[string]interface{})
		if includeMetadata {
			var statuses []string
			for _, status := range vector.Statuses {
				statuses = append(statuses, status.String())
			}
			var timestamps []string
			for _, timestamp := range vector.Timestamps {
				timestamps = append(timestamps, timestamp.AsTime().Format(time.RFC3339))
			}

			result["statuses"] = statuses
			result["event_timestamps"] = timestamps
		}
		// Note, that vector.Values is an Arrow Array, but this type implements JSON Marshaller.
		// So, it's not necessary to pre-process it in any way.
		result["values"] = vector.Values

		results = append(results, result)
	}

	response := map[string]interface{}{
		"metadata": map[string]interface{}{
			"feature_names": featureNames,
		},
		"results": results,
	}

	w.Header().Set("Content-Type", "application/json")

	err = json.NewEncoder(w).Encode(response)

	if err != nil {
		logSpanContext.Error().Err(err).Msg("Error encoding response")
		writeJSONError(w, fmt.Errorf("Error encoding response: %+v", err), http.StatusInternalServerError)
		return
	}

	if featureService != nil && featureService.LoggingConfig != nil && s.loggingService != nil {
		logger, err := s.loggingService.GetOrCreateLogger(featureService)
		if err != nil {
			logSpanContext.Error().Err(err).Msgf("Couldn't instantiate logger for feature service %s", featureService.Name)
			writeJSONError(w, fmt.Errorf("Couldn't instantiate logger for feature service %s: %+v", featureService.Name, err), http.StatusInternalServerError)
			return
		}

		requestId := GenerateRequestId()

		// Note: we're converting arrow to proto for feature logging. In the future we should
		// base feature logging on arrow so that we don't have to do this extra conversion.
		var featureVectorProtos []*serving.GetOnlineFeaturesResponse_FeatureVector
		for _, vector := range featureVectors[len(request.Entities):] {
			values, err := types.ArrowValuesToProtoValues(vector.Values)
			if err != nil {
				logSpanContext.Error().Err(err).Msg("Couldn't convert arrow values into protobuf")
				writeJSONError(w, fmt.Errorf("Couldn't convert arrow values into protobuf: %+v", err), http.StatusInternalServerError)
				return
			}
			featureVectorProtos = append(featureVectorProtos, &serving.GetOnlineFeaturesResponse_FeatureVector{
				Values:          values,
				Statuses:        vector.Statuses,
				EventTimestamps: vector.Timestamps,
			})
		}

		err = logger.Log(entitiesProto, featureVectorProtos, featureNames[len(request.Entities):], requestContextProto, requestId)
		if err != nil {
			writeJSONError(w, fmt.Errorf("LoggerImpl error[%s]: %+v", featureService.Name, err), http.StatusInternalServerError)
			return
		}
	}
}

type getOnlineFeaturesRangeRequest struct {
	FeatureService   *string                  `json:"feature_service"`
	Features         []string                 `json:"features"`
	Entities         map[string]repeatedValue `json:"entities"`
	SortKeyFilters   []sortKeyFilter          `json:"sort_key_filters"`
	ReverseSortOrder bool                     `json:"reverse_sort_order"`
	Limit            int32                    `json:"limit"`
	FullFeatureNames bool                     `json:"full_feature_names"`
	RequestContext   map[string]repeatedValue `json:"request_context"`
}

type sortKeyFilter struct {
	SortKeyName string          `json:"sort_key_name"`
	Range       rangeQuery      `json:"range"`
	Equals      json.RawMessage `json:"equals"`
}

type rangeQuery struct {
	RangeStart     json.RawMessage `json:"range_start"`
	RangeEnd       json.RawMessage `json:"range_end"`
	StartInclusive bool            `json:"start_inclusive"`
	EndInclusive   bool            `json:"end_inclusive"`
}

func getSortKeyFiltersProto(filters []sortKeyFilter) ([]*serving.SortKeyFilter, error) {
	sortKeyFiltersProto := make([]*serving.SortKeyFilter, len(filters))
	for i, filter := range filters {
		protoFilter, err := filter.ToProto()
		if err != nil {
			return nil, err
		}
		sortKeyFiltersProto[i] = protoFilter
	}
	return sortKeyFiltersProto, nil
}

func (s *httpServer) getOnlineFeaturesRange(w http.ResponseWriter, r *http.Request) {
	var err error

	span, ctx := tracer.StartSpanFromContext(r.Context(), "getOnlineFeaturesRange", tracer.ResourceName("/get-online-features-range"))
	defer span.Finish(tracer.WithError(err))

	logSpanContext := LogWithSpanContext(span)

	if r.Method != "POST" {
		http.NotFound(w, r)
		return
	}

	includeMetadataQuery := r.URL.Query().Get("includeMetadata")
	includeMetadata := false
	if includeMetadataQuery != "" {
		includeMetadata, err = strconv.ParseBool(includeMetadataQuery)
		if err != nil {
			logSpanContext.Error().Err(err).Msg("Error parsing includeMetadata query parameter")
			writeJSONError(w, fmt.Errorf("error parsing includeMetadata query parameter: %w", err), http.StatusBadRequest)
			return
		}
	}

	decoder := json.NewDecoder(r.Body)
	var request getOnlineFeaturesRangeRequest
	err = decoder.Decode(&request)
	if err != nil {
		logSpanContext.Error().Err(err).Msg("Error decoding JSON request data")
		writeJSONError(w, fmt.Errorf("error decoding JSON request data: %w", err), http.StatusInternalServerError)
		return
	}

	// TODO: Implement support for feature services with range queries
	var featureService *model.FeatureService
	if request.FeatureService != nil {
		writeJSONError(w, fmt.Errorf("feature services are not supported for range queries"), http.StatusBadRequest)
		return
	}

	entitiesProto := make(map[string]*prototypes.RepeatedValue)
	for key, value := range request.Entities {
		entitiesProto[key] = value.ToProto()
	}

	requestContextProto := make(map[string]*prototypes.RepeatedValue)
	if request.RequestContext != nil {
		for key, value := range request.RequestContext {
			requestContextProto[key] = value.ToProto()
		}
	}

	sortKeyFiltersProto, err := getSortKeyFiltersProto(request.SortKeyFilters)
	if err != nil {
		logSpanContext.Error().Err(err).Msg("Error converting sort key filter to protobuf")
		writeJSONError(w, fmt.Errorf("error converting sort key filter to protobuf: %w", err), http.StatusInternalServerError)
		return
	}

	rangeFeatureVectors, err := s.fs.GetOnlineFeaturesRange(
		ctx,
		request.Features,
		featureService,
		entitiesProto,
		sortKeyFiltersProto,
		request.ReverseSortOrder,
		request.Limit,
		requestContextProto,
		request.FullFeatureNames)

	defer func() {
		if rangeFeatureVectors != nil {
			go releaseCGORangeMemory(rangeFeatureVectors)
		}
	}()

	if err != nil {
		logSpanContext.Error().Err(err).Msg("Error getting range feature vectors")
		writeJSONError(w, fmt.Errorf("error getting range feature vectors: %w", err), http.StatusInternalServerError)
		return
	}

	featureNames, results := processFeatureVectors(rangeFeatureVectors, includeMetadata, entitiesProto)

	response := map[string]interface{}{
		"metadata": map[string]interface{}{
			"feature_names": featureNames,
		},
		"results": results,
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		logSpanContext.Error().Err(err).Msg("Error encoding response")
		writeJSONError(w, fmt.Errorf("error encoding response: %w", err), http.StatusInternalServerError)
		return
	}
}

func releaseCGOMemory(featureVectors []*onlineserving.FeatureVector) {
	for _, vector := range featureVectors {
		vector.Values.Release()
	}
}

func releaseCGORangeMemory(featureVectors []*onlineserving.RangeFeatureVector) {
	for _, vector := range featureVectors {
		vector.RangeValues.Release()
	}
}

func logStackTrace() {
	// Start with a small buffer and grow it until the full stack trace fits.
	buf := make([]byte, 1024)
	for {
		stackSize := runtime.Stack(buf, false)
		if stackSize < len(buf) {
			// The stack trace fits in the buffer, so we can log it now.
			log.Error().Str("stack_trace", string(buf[:stackSize])).Msg("")
			return
		}
		// The stack trace doesn't fit in the buffer, so we need to grow the buffer and try again.
		buf = make([]byte, 2*len(buf))
	}
}

func writeJSONError(w http.ResponseWriter, err error, statusCode int) {
	errMap := map[string]interface{}{
		"error":       fmt.Sprintf("%+v", err),
		"status_code": statusCode,
	}
	errJSON, _ := json.Marshal(errMap)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	w.Write(errJSON)
}

func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if r := recover(); r != nil {
				log.Error().Err(fmt.Errorf("Panic recovered: %v", r)).Msg("A panic occurred in the server")
				// Log the stack trace
				logStackTrace()

				writeJSONError(w, fmt.Errorf("Internal Server Error: %v", r), http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *httpServer) Serve(host string, port int, handlers []Handler) error {
	if strings.ToLower(os.Getenv("ENABLE_DATADOG_TRACING")) == "true" {
		tracer.Start(tracer.WithRuntimeMetrics())
		defer tracer.Stop()
	}
	mux := httptrace.NewServeMux()

	for _, handler := range handlers {
		mux.Handle(handler.path, handler.handlerFunc)
	}

	s.server = &http.Server{Addr: fmt.Sprintf("%s:%d", host, port), Handler: mux, ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 15 * time.Second}
	err := s.server.ListenAndServe()
	// Don't return the error if it's caused by graceful shutdown using Stop()
	if err == http.ErrServerClosed {
		return nil
	}
	log.Fatal().Stack().Err(err).Msg("Failed to start HTTP server")
	return err
}

func DefaultHttpHandlers(s *httpServer) []Handler {
	return CommonHttpHandlers(s, healthCheckHandler)
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Healthy")
}

func (s *httpServer) Stop() error {
	if s.server != nil {
		return s.server.Shutdown(context.Background())
	}
	return nil
}
