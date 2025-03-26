package onlineserving

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/rs/zerolog/log"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/onlinestore"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	prototypes "github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/types"
)

/*
FeatureVector type represent result of retrieving single feature for multiple rows.
It can be imagined as a column in output dataframe / table.
It contains of feature name, list of values (across all rows),
list of statuses and list of timestamp. All these lists have equal length.
And this length is also equal to number of entity rows received in request.
*/
type FeatureVector struct {
	Name       string
	Values     arrow.Array
	Statuses   []serving.FieldStatus
	Timestamps []*timestamppb.Timestamp
}

/*
RangeFeatureVector type represent result of retrieving a range of features for multiple entities.
It is similar to FeatureVector but contains a list of lists of values, a list of lists of statuses and a list of lists
of timestamps where each inner list represents the range of values, statuses, timestamps for a single entity.
Each of these lists are of equal dimensionality.
*/
type RangeFeatureVector struct {
	Name            string
	RangeValues     arrow.Array
	RangeStatuses   [][]serving.FieldStatus
	RangeTimestamps [][]*timestamppb.Timestamp
}

type FeatureViewAndRefs struct {
	View        *model.FeatureView
	FeatureRefs []string
}

type SortedFeatureViewAndRefs struct {
	View        *model.SortedFeatureView
	FeatureRefs []string
}

/*
We group all features from a single request by entities they attached to.
Thus, we will be able to call online retrieval per entity and not per each feature View.
In this struct we collect all features and views that belongs to a group.
We also store here projected entity keys (only ones that needed to retrieve these features)
and indexes to map result of retrieval into output response.
*/
type GroupedFeaturesPerEntitySet struct {
	// A list of requested feature references of the form featureViewName:featureName that share this entity set
	FeatureNames     []string
	FeatureViewNames []string
	// full feature references as they supposed to appear in response
	AliasedFeatureNames []string
	// Entity set as a list of EntityKeys to pass to OnlineRead
	EntityKeys []*prototypes.EntityKey
	// Reversed mapping to project result of retrieval from storage to response
	Indices [][]int
}

/*
We group all features from a single request by entities they attached to.
Thus, we will be able to call online range retrieval per entity and not per each feature View.
In this struct we collect all features and views that belongs to a group.
We store here projected entity keys (only ones that needed to retrieve these features)
and indexes to map result of retrieval into output response.
We also store range query parameters like sort key filters, reverse sort order and limit.
*/
type GroupedRangeFeatureRefs struct {
	// A list of requested feature references of the form featureViewName:featureName that share this entity set
	FeatureNames     []string
	FeatureViewNames []string
	// full feature references as they supposed to appear in response
	AliasedFeatureNames []string
	// Entity set as a list of EntityKeys to pass to OnlineReadRange
	EntityKeys []*prototypes.EntityKey
	// Reversed mapping to project result of retrieval from storage to response
	Indices [][]int

	// Sort key filters to pass to OnlineReadRange
	SortKeyFilters []*model.SortKeyFilter
	// Limit to pass to OnlineReadRange
	Limit int32
}

/*
Return

	(1) requested feature views and features grouped per View
	(2) requested on demand feature views

existed in the registry
*/
func GetFeatureViewsToUseByService(
	featureService *model.FeatureService,
	featureViews map[string]*model.FeatureView,
	sortedFeatureViews map[string]*model.SortedFeatureView,
	onDemandFeatureViews map[string]*model.OnDemandFeatureView) ([]*FeatureViewAndRefs, []*SortedFeatureViewAndRefs, []*model.OnDemandFeatureView, error) {

	viewNameToViewAndRefs := make(map[string]*FeatureViewAndRefs)
	viewNameToSortedViewAndRefs := make(map[string]*SortedFeatureViewAndRefs)
	odFvsToUse := make([]*model.OnDemandFeatureView, 0)

	for _, featureProjection := range featureService.Projections {
		// Create copies of FeatureView that may contains the same *FeatureView but
		// each differentiated by a *FeatureViewProjection
		featureViewName := featureProjection.Name
		if fv, ok := featureViews[featureViewName]; ok {
			base, err := fv.Base.WithProjection(featureProjection)
			if err != nil {
				return nil, nil, nil, err
			}
			if _, ok := viewNameToViewAndRefs[featureProjection.NameToUse()]; !ok {
				viewNameToViewAndRefs[featureProjection.NameToUse()] = &FeatureViewAndRefs{
					View:        fv.NewFeatureViewFromBase(base),
					FeatureRefs: []string{},
				}
			}

			for _, feature := range featureProjection.Features {
				viewNameToViewAndRefs[featureProjection.NameToUse()].FeatureRefs =
					addStringIfNotContains(viewNameToViewAndRefs[featureProjection.NameToUse()].FeatureRefs,
						feature.Name)
			}

		} else if sortedFv, ok := sortedFeatureViews[featureViewName]; ok {
			base, err := sortedFv.Base.WithProjection(featureProjection)
			if err != nil {
				return nil, nil, nil, err
			}
			if _, ok := viewNameToSortedViewAndRefs[featureProjection.NameToUse()]; !ok {
				viewNameToSortedViewAndRefs[featureProjection.NameToUse()] = &SortedFeatureViewAndRefs{
					View:        sortedFv.NewSortedFeatureViewFromBase(base),
					FeatureRefs: []string{},
				}
			}

			for _, feature := range featureProjection.Features {
				viewNameToSortedViewAndRefs[featureProjection.NameToUse()].FeatureRefs =
					addStringIfNotContains(viewNameToSortedViewAndRefs[featureProjection.NameToUse()].FeatureRefs,
						feature.Name)
			}
		} else if odFv, ok := onDemandFeatureViews[featureViewName]; ok {
			projectedOdFv, err := odFv.NewWithProjection(featureProjection)
			if err != nil {
				return nil, nil, nil, err
			}
			odFvsToUse = append(odFvsToUse, projectedOdFv)
			err = extractOdFvDependencies(
				projectedOdFv,
				featureViews,
				viewNameToViewAndRefs)
			if err != nil {
				return nil, nil, nil, err
			}
		} else {
			return nil, nil, nil, fmt.Errorf("the provided feature service %s contains a reference to a feature View"+
				"%s which doesn't exist, please make sure that you have created the feature View"+
				"%s and that you have registered it by running \"apply\"", featureService.Name, featureViewName, featureViewName)
		}
	}

	fvsToUse := make([]*FeatureViewAndRefs, 0)
	for _, viewAndRef := range viewNameToViewAndRefs {
		fvsToUse = append(fvsToUse, viewAndRef)
	}
	sortedFvsToUse := make([]*SortedFeatureViewAndRefs, 0)
	for _, viewAndRef := range viewNameToSortedViewAndRefs {
		sortedFvsToUse = append(sortedFvsToUse, viewAndRef)
	}

	return fvsToUse, sortedFvsToUse, odFvsToUse, nil
}

/*
Return

	(1) requested feature views and features grouped per View
	(2) requested on demand feature views

existed in the registry
*/
func GetFeatureViewsToUseByFeatureRefs(
	features []string,
	featureViews map[string]*model.FeatureView,
	sortedFeatureViews map[string]*model.SortedFeatureView,
	onDemandFeatureViews map[string]*model.OnDemandFeatureView) ([]*FeatureViewAndRefs, []*SortedFeatureViewAndRefs, []*model.OnDemandFeatureView, error) {
	viewNameToViewAndRefs := make(map[string]*FeatureViewAndRefs)
	viewNameToSortedViewAndRefs := make(map[string]*SortedFeatureViewAndRefs)
	odFvToFeatures := make(map[string][]string)

	for _, featureRef := range features {
		featureViewName, featureName, err := ParseFeatureReference(featureRef)
		if err != nil {
			return nil, nil, nil, err
		}
		if fv, ok := featureViews[featureViewName]; ok {
			if viewAndRef, ok := viewNameToViewAndRefs[fv.Base.Name]; ok {
				viewAndRef.FeatureRefs = addStringIfNotContains(viewAndRef.FeatureRefs, featureName)
			} else {
				viewNameToViewAndRefs[fv.Base.Name] = &FeatureViewAndRefs{
					View:        fv,
					FeatureRefs: []string{featureName},
				}
			}
		} else if sortedFv, ok := sortedFeatureViews[featureViewName]; ok {
			if viewAndRef, ok := viewNameToSortedViewAndRefs[sortedFv.Base.Name]; ok {
				viewAndRef.FeatureRefs = addStringIfNotContains(viewAndRef.FeatureRefs, featureName)
			} else {
				viewNameToSortedViewAndRefs[sortedFv.Base.Name] = &SortedFeatureViewAndRefs{
					View:        sortedFv,
					FeatureRefs: []string{featureName},
				}
			}
		} else if odfv, ok := onDemandFeatureViews[featureViewName]; ok {
			if _, ok := odFvToFeatures[odfv.Base.Name]; !ok {
				odFvToFeatures[odfv.Base.Name] = []string{featureName}
			} else {
				odFvToFeatures[odfv.Base.Name] = append(
					odFvToFeatures[odfv.Base.Name], featureName)
			}
		} else {
			return nil, nil, nil, fmt.Errorf("feature View %s doesn't exist, please make sure that you have created the"+
				" feature View %s and that you have registered it by running \"apply\"", featureViewName, featureViewName)
		}
	}

	odFvsToUse := make([]*model.OnDemandFeatureView, 0)

	for odFvName, featureNames := range odFvToFeatures {
		projectedOdFv, err := onDemandFeatureViews[odFvName].ProjectWithFeatures(featureNames)
		if err != nil {
			return nil, nil, nil, err
		}

		err = extractOdFvDependencies(
			projectedOdFv,
			featureViews,
			viewNameToViewAndRefs)
		if err != nil {
			return nil, nil, nil, err
		}
		odFvsToUse = append(odFvsToUse, projectedOdFv)
	}

	fvsToUse := make([]*FeatureViewAndRefs, 0)
	for _, viewAndRefs := range viewNameToViewAndRefs {
		fvsToUse = append(fvsToUse, viewAndRefs)
	}
	sortedFvsToUse := make([]*SortedFeatureViewAndRefs, 0)
	for _, viewAndRefs := range viewNameToSortedViewAndRefs {
		sortedFvsToUse = append(sortedFvsToUse, viewAndRefs)
	}

	return fvsToUse, sortedFvsToUse, odFvsToUse, nil
}

func extractOdFvDependencies(
	odFv *model.OnDemandFeatureView,
	sourceFvs map[string]*model.FeatureView,
	requestedFeatures map[string]*FeatureViewAndRefs,
) error {

	for _, sourceFvProjection := range odFv.SourceFeatureViewProjections {
		fv := sourceFvs[sourceFvProjection.Name]
		base, err := fv.Base.WithProjection(sourceFvProjection)
		if err != nil {
			return err
		}
		newFv := fv.NewFeatureViewFromBase(base)

		if _, ok := requestedFeatures[sourceFvProjection.NameToUse()]; !ok {
			requestedFeatures[sourceFvProjection.NameToUse()] = &FeatureViewAndRefs{
				View:        newFv,
				FeatureRefs: []string{},
			}
		}

		for _, feature := range sourceFvProjection.Features {
			requestedFeatures[sourceFvProjection.NameToUse()].FeatureRefs = addStringIfNotContains(
				requestedFeatures[sourceFvProjection.NameToUse()].FeatureRefs, feature.Name)
		}
	}

	return nil
}

func addStringIfNotContains(slice []string, element string) []string {
	found := false
	for _, item := range slice {
		if element == item {
			found = true
		}
	}
	if !found {
		slice = append(slice, element)
	}
	return slice
}

func GetEntityMaps(requestedFeatureViews []*FeatureViewAndRefs, entities []*model.Entity) (map[string]string, map[string]interface{}, error) {
	entityNameToJoinKeyMap := make(map[string]string)
	expectedJoinKeysSet := make(map[string]interface{})

	entitiesByName := make(map[string]*model.Entity)

	for _, entity := range entities {
		entitiesByName[entity.Name] = entity
	}

	for _, featuresAndView := range requestedFeatureViews {
		featureView := featuresAndView.View
		var joinKeyToAliasMap map[string]string
		if featureView.Base.Projection != nil && featureView.Base.Projection.JoinKeyMap != nil {
			joinKeyToAliasMap = featureView.Base.Projection.JoinKeyMap
		} else {
			joinKeyToAliasMap = map[string]string{}
		}

		for _, entityName := range featureView.EntityNames {
			joinKey := entitiesByName[entityName].JoinKey
			entityNameToJoinKeyMap[entityName] = joinKey

			if alias, ok := joinKeyToAliasMap[joinKey]; ok {
				expectedJoinKeysSet[alias] = nil
			} else {
				expectedJoinKeysSet[joinKey] = nil
			}
		}
	}
	return entityNameToJoinKeyMap, expectedJoinKeysSet, nil
}

func GetEntityMapsForSortedViews(sortedViews []*SortedFeatureViewAndRefs, entities []*model.Entity) (map[string]string, map[string]interface{}, error) {
	entityNameToJoinKeyMap := make(map[string]string)
	expectedJoinKeysSet := make(map[string]interface{})

	log.Printf("Debug - Entity count: %d", len(entities))
	for _, entity := range entities {
		log.Printf("Debug - Entity: %s, JoinKey: %s", entity.Name, entity.JoinKey)
	}

	entitiesByName := make(map[string]*model.Entity)
	for _, entity := range entities {
		entitiesByName[entity.Name] = entity
	}

	log.Printf("Debug - Sorted views count: %d", len(sortedViews))
	for _, featuresAndView := range sortedViews {
		featureView := featuresAndView.View
		log.Printf("Debug - Feature view: %s, EntityNames: %v", featureView.Base.Name, featureView.EntityNames)

		var joinKeyToAliasMap map[string]string

		if featureView.Base.Projection != nil && featureView.Base.Projection.JoinKeyMap != nil {
			joinKeyToAliasMap = featureView.Base.Projection.JoinKeyMap
			log.Printf("Debug - Using projection join key map: %v", joinKeyToAliasMap)
		} else {
			joinKeyToAliasMap = map[string]string{}
			log.Printf("Debug - No projection join key map")
		}

		for _, entityName := range featureView.EntityNames {
			entity, exists := entitiesByName[entityName]
			if !exists {
				log.Printf("Error - Entity not found in registry: %s", entityName)
				continue
			}

			joinKey := entity.JoinKey
			log.Printf("Debug - Adding join key mapping: %s -> %s", entityName, joinKey)
			entityNameToJoinKeyMap[entityName] = joinKey

			if alias, ok := joinKeyToAliasMap[joinKey]; ok {
				log.Printf("Debug - Using aliased join key: %s -> %s", joinKey, alias)
				expectedJoinKeysSet[alias] = nil
			} else {
				log.Printf("Debug - Using direct join key: %s", joinKey)
				expectedJoinKeysSet[joinKey] = nil
			}
		}
	}

	log.Printf("Debug - Final expected join keys: %v", expectedJoinKeysSet)
	return entityNameToJoinKeyMap, expectedJoinKeysSet, nil
}

func ValidateEntityValues(joinKeyValues map[string]*prototypes.RepeatedValue,
	requestData map[string]*prototypes.RepeatedValue,
	expectedJoinKeysSet map[string]interface{}) (int, error) {
	numRows := -1
	log.Info().Msgf("DEBUG: joinKeyValues: %v", joinKeyValues)
	log.Info().Msgf("DEBUG: requestData: %v", requestData)
	log.Info().Msgf("DEBUG: expectedJoinKeysSet: %v", expectedJoinKeysSet)

	for joinKey, values := range joinKeyValues {
		if _, ok := expectedJoinKeysSet[joinKey]; !ok {
			requestData[joinKey] = values
			delete(joinKeyValues, joinKey)
			// ToDo: when request data will be passed correctly (not as part of entity rows)
			// ToDo: throw this error instead
			// return 0, fmt.Errorf("JoinKey is not expected in this request: %s\n%v", JoinKey, expectedJoinKeysSet)
		} else {
			if numRows < 0 {
				numRows = len(values.Val)
			} else if len(values.Val) != numRows {
				return -1, errors.New("valueError: All entity rows must have the same columns")
			}

		}
	}
	log.Info().Msgf("DEBUG: numRows: %d", numRows)
	return numRows, nil
}

func ValidateFeatureRefs(requestedFeatures []*FeatureViewAndRefs, fullFeatureNames bool) error {
	featureRefCounter := make(map[string]int)
	featureRefs := make([]string, 0)
	for _, viewAndFeatures := range requestedFeatures {
		for _, feature := range viewAndFeatures.FeatureRefs {
			projectedViewName := viewAndFeatures.View.Base.Name
			if viewAndFeatures.View.Base.Projection != nil {
				projectedViewName = viewAndFeatures.View.Base.Projection.NameToUse()
			}

			featureRefs = append(featureRefs,
				fmt.Sprintf("%s:%s", projectedViewName, feature))
		}
	}

	for _, featureRef := range featureRefs {
		if fullFeatureNames {
			featureRefCounter[featureRef]++
		} else {
			_, featureName, _ := ParseFeatureReference(featureRef)
			featureRefCounter[featureName]++
		}

	}
	for featureName, occurrences := range featureRefCounter {
		if occurrences == 1 {
			delete(featureRefCounter, featureName)
		}
	}
	if len(featureRefCounter) >= 1 {
		collidedFeatureRefs := make([]string, 0)
		for collidedFeatureRef := range featureRefCounter {
			if fullFeatureNames {
				collidedFeatureRefs = append(collidedFeatureRefs, collidedFeatureRef)
			} else {
				for _, featureRef := range featureRefs {
					_, featureName, _ := ParseFeatureReference(featureRef)
					if featureName == collidedFeatureRef {
						collidedFeatureRefs = append(collidedFeatureRefs, featureRef)
					}
				}
			}
		}
		return featureNameCollisionError{collidedFeatureRefs, fullFeatureNames}
	}

	return nil
}

func ValidateSortedFeatureRefs(sortedViews []*SortedFeatureViewAndRefs, fullFeatureNames bool) error {
	featureRefCounter := make(map[string]int)
	featureRefs := make([]string, 0)

	for _, viewAndFeatures := range sortedViews {
		for _, feature := range viewAndFeatures.FeatureRefs {
			projectedViewName := viewAndFeatures.View.Base.Name
			if viewAndFeatures.View.Base.Projection != nil {
				projectedViewName = viewAndFeatures.View.Base.Projection.NameToUse()
			}

			featureRefs = append(featureRefs,
				fmt.Sprintf("%s:%s", projectedViewName, feature))
		}
	}

	for _, featureRef := range featureRefs {
		if fullFeatureNames {
			featureRefCounter[featureRef]++
		} else {
			_, featureName, _ := ParseFeatureReference(featureRef)
			featureRefCounter[featureName]++
		}
	}

	for featureName, occurrences := range featureRefCounter {
		if occurrences == 1 {
			delete(featureRefCounter, featureName)
		}
	}

	if len(featureRefCounter) >= 1 {
		collidedFeatureRefs := make([]string, 0)
		for collidedFeatureRef := range featureRefCounter {
			if fullFeatureNames {
				collidedFeatureRefs = append(collidedFeatureRefs, collidedFeatureRef)
			} else {
				for _, featureRef := range featureRefs {
					_, featureName, _ := ParseFeatureReference(featureRef)
					if featureName == collidedFeatureRef {
						collidedFeatureRefs = append(collidedFeatureRefs, featureRef)
					}
				}
			}
		}
		return featureNameCollisionError{collidedFeatureRefs, fullFeatureNames}
	}

	return nil
}

func ValidateSortKeyFilters(filters []*serving.SortKeyFilter, sortedViews []*SortedFeatureViewAndRefs) error {
	sortKeyTypes := make(map[string]prototypes.ValueType_Enum)

	for _, sortedView := range sortedViews {
		for _, sortKey := range sortedView.View.SortKeys {
			sortKeyTypes[sortKey.FieldName] = sortKey.ValueType
		}
	}

	for _, filter := range filters {
		expectedType, exists := sortKeyTypes[filter.SortKeyName]
		if !exists {
			return fmt.Errorf("sort key '%s' not found in any of the requested sorted feature views",
				filter.SortKeyName)
		}

		if filter.RangeStart != nil {
			if !isValueTypeCompatible(filter.RangeStart, expectedType) {
				return fmt.Errorf("range_start value for sort key '%s' has incompatible type: expected %s",
					filter.SortKeyName, valueTypeToString(expectedType))
			}
		}

		if filter.RangeEnd != nil {
			if !isValueTypeCompatible(filter.RangeEnd, expectedType) {
				return fmt.Errorf("range_end value for sort key '%s' has incompatible type: expected %s",
					filter.SortKeyName, valueTypeToString(expectedType))
			}
		}
	}

	return nil
}

func isValueTypeCompatible(value *prototypes.Value, expectedType prototypes.ValueType_Enum) bool {
	if value == nil {
		return true
	}

	if value.Val == nil {
		return false
	}

	switch expectedType {
	case prototypes.ValueType_INT32:
		_, ok := value.Val.(*prototypes.Value_Int32Val)
		return ok
	case prototypes.ValueType_INT64:
		_, ok := value.Val.(*prototypes.Value_Int64Val)
		return ok
	case prototypes.ValueType_FLOAT:
		_, ok := value.Val.(*prototypes.Value_FloatVal)
		return ok
	case prototypes.ValueType_DOUBLE:
		_, ok := value.Val.(*prototypes.Value_DoubleVal)
		return ok
	case prototypes.ValueType_STRING:
		_, ok := value.Val.(*prototypes.Value_StringVal)
		return ok
	case prototypes.ValueType_BOOL:
		_, ok := value.Val.(*prototypes.Value_BoolVal)
		return ok
	case prototypes.ValueType_BYTES:
		_, ok := value.Val.(*prototypes.Value_BytesVal)
		return ok
	case prototypes.ValueType_UNIX_TIMESTAMP:
		_, ok := value.Val.(*prototypes.Value_UnixTimestampVal)
		return ok
	default:
		return false
	}
}

func valueTypeToString(valueType prototypes.ValueType_Enum) string {
	switch valueType {
	case prototypes.ValueType_INT32:
		return "INT32"
	case prototypes.ValueType_INT64:
		return "INT64"
	case prototypes.ValueType_FLOAT:
		return "FLOAT"
	case prototypes.ValueType_DOUBLE:
		return "DOUBLE"
	case prototypes.ValueType_STRING:
		return "STRING"
	case prototypes.ValueType_BOOL:
		return "BOOL"
	case prototypes.ValueType_BYTES:
		return "BYTES"
	case prototypes.ValueType_UNIX_TIMESTAMP:
		return "UNIX_TIMESTAMP"
	default:
		return fmt.Sprintf("UNKNOWN_TYPE(%d)", int(valueType))
	}
}

func TransposeFeatureRowsIntoColumns(featureData2D [][]onlinestore.FeatureData,
	groupRef *GroupedFeaturesPerEntitySet,
	requestedFeatureViews []*FeatureViewAndRefs,
	arrowAllocator memory.Allocator,
	numRows int) ([]*FeatureVector, error) {

	numFeatures := len(groupRef.AliasedFeatureNames)
	fvs := make(map[string]*model.FeatureView)
	for _, viewAndRefs := range requestedFeatureViews {
		fvs[viewAndRefs.View.Base.Name] = viewAndRefs.View
	}

	var value *prototypes.Value
	var status serving.FieldStatus
	var eventTimeStamp *timestamppb.Timestamp
	var featureData *onlinestore.FeatureData
	var fv *model.FeatureView
	var featureViewName string

	vectors := make([]*FeatureVector, 0)

	for featureIndex := 0; featureIndex < numFeatures; featureIndex++ {
		currentVector := &FeatureVector{
			Name:       groupRef.AliasedFeatureNames[featureIndex],
			Statuses:   make([]serving.FieldStatus, numRows),
			Timestamps: make([]*timestamppb.Timestamp, numRows),
		}
		vectors = append(vectors, currentVector)
		protoValues := make([]*prototypes.Value, numRows)

		for rowEntityIndex, outputIndexes := range groupRef.Indices {
			if featureData2D[rowEntityIndex] == nil {
				value = nil
				status = serving.FieldStatus_NOT_FOUND
				eventTimeStamp = &timestamppb.Timestamp{}
			} else {
				featureData = &featureData2D[rowEntityIndex][featureIndex]
				eventTimeStamp = &timestamppb.Timestamp{Seconds: featureData.Timestamp.Seconds, Nanos: featureData.Timestamp.Nanos}
				featureViewName = featureData.Reference.FeatureViewName
				fv = fvs[featureViewName]
				if _, ok := featureData.Value.Val.(*prototypes.Value_NullVal); ok {
					value = nil
					status = serving.FieldStatus_NOT_FOUND
				} else if checkOutsideTtl(eventTimeStamp, timestamppb.Now(), fv.Ttl) {
					value = &prototypes.Value{Val: featureData.Value.Val}
					status = serving.FieldStatus_OUTSIDE_MAX_AGE
				} else {
					value = &prototypes.Value{Val: featureData.Value.Val}
					status = serving.FieldStatus_PRESENT
				}
			}
			for _, rowIndex := range outputIndexes {
				protoValues[rowIndex] = value
				currentVector.Statuses[rowIndex] = status
				currentVector.Timestamps[rowIndex] = eventTimeStamp
			}
		}
		arrowValues, err := types.ProtoValuesToArrowArray(protoValues, arrowAllocator, numRows)
		if err != nil {
			return nil, err
		}
		currentVector.Values = arrowValues
	}

	return vectors, nil

}

func TransposeRangeFeatureRowsIntoColumns(
	featureData2D [][]onlinestore.RangeFeatureData,
	groupRef *GroupedRangeFeatureRefs,
	sortedViews []*SortedFeatureViewAndRefs,
	arrowAllocator memory.Allocator,
	numRows int) ([]*RangeFeatureVector, error) {

	numFeatures := len(groupRef.AliasedFeatureNames)
	sfvs := make(map[string]*model.SortedFeatureView)
	for _, viewAndRefs := range sortedViews {
		sfvs[viewAndRefs.View.Base.Name] = viewAndRefs.View
	}

	vectors := make([]*RangeFeatureVector, 0)

	for featureIndex := 0; featureIndex < numFeatures; featureIndex++ {
		currentVector := &RangeFeatureVector{
			Name:            groupRef.AliasedFeatureNames[featureIndex],
			RangeStatuses:   make([][]serving.FieldStatus, numRows),
			RangeTimestamps: make([][]*timestamppb.Timestamp, numRows),
		}
		vectors = append(vectors, currentVector)
		rangeValuesByRow := make([]*prototypes.RepeatedValue, numRows)
		for i := range rangeValuesByRow {
			rangeValuesByRow[i] = &prototypes.RepeatedValue{Val: make([]*prototypes.Value, 0)}
		}

		for rowEntityIndex, outputIndexes := range groupRef.Indices {
			var rangeValues []*prototypes.Value
			var rangeStatuses []serving.FieldStatus
			var rangeTimestamps []*timestamppb.Timestamp

			if featureData2D[rowEntityIndex] == nil || len(featureData2D[rowEntityIndex]) <= featureIndex {
				rangeValues = make([]*prototypes.Value, 0)
				rangeStatuses = make([]serving.FieldStatus, 0)
				rangeTimestamps = make([]*timestamppb.Timestamp, 0)
			} else {
				featureData := featureData2D[rowEntityIndex][featureIndex]

				rangeValues = make([]*prototypes.Value, len(featureData.Values))
				rangeStatuses = make([]serving.FieldStatus, len(featureData.Values))
				rangeTimestamps = make([]*timestamppb.Timestamp, len(featureData.Values))

				featureViewName := featureData.FeatureView
				sfv, exists := sfvs[featureViewName]
				if !exists {
					return nil, fmt.Errorf("feature view '%s' not found in the provided sorted feature views", featureViewName)
				}

				for i, val := range featureData.Values {
					if val == nil {
						rangeValues[i] = nil
						rangeStatuses[i] = serving.FieldStatus_NOT_FOUND
						rangeTimestamps[i] = &timestamppb.Timestamp{}
						continue
					}

					protoVal := &prototypes.Value{}

					switch v := val.(type) {
					case []byte:
						protoVal.Val = &prototypes.Value_BytesVal{BytesVal: v}
					case string:
						protoVal.Val = &prototypes.Value_StringVal{StringVal: v}
					case int32:
						protoVal.Val = &prototypes.Value_Int32Val{Int32Val: v}
					case int64:
						protoVal.Val = &prototypes.Value_Int64Val{Int64Val: v}
					case float64:
						protoVal.Val = &prototypes.Value_DoubleVal{DoubleVal: v}
					case float32:
						protoVal.Val = &prototypes.Value_FloatVal{FloatVal: v}
					case bool:
						protoVal.Val = &prototypes.Value_BoolVal{BoolVal: v}
					case time.Time:
						protoVal.Val = &prototypes.Value_UnixTimestampVal{UnixTimestampVal: v.Unix()}
					case *timestamppb.Timestamp:
						protoVal.Val = &prototypes.Value_UnixTimestampVal{UnixTimestampVal: v.GetSeconds()}

					case [][]byte:
						bytesList := &prototypes.BytesList{Val: v}
						protoVal.Val = &prototypes.Value_BytesListVal{BytesListVal: bytesList}
					case prototypes.BytesList:
						protoVal.Val = &prototypes.Value_BytesListVal{BytesListVal: &v}
					case *prototypes.BytesList:
						protoVal.Val = &prototypes.Value_BytesListVal{BytesListVal: v}

					case []string:
						stringList := &prototypes.StringList{Val: v}
						protoVal.Val = &prototypes.Value_StringListVal{StringListVal: stringList}
					case prototypes.StringList:
						protoVal.Val = &prototypes.Value_StringListVal{StringListVal: &v}
					case *prototypes.StringList:
						protoVal.Val = &prototypes.Value_StringListVal{StringListVal: v}

					case []int32:
						int32List := &prototypes.Int32List{Val: v}
						protoVal.Val = &prototypes.Value_Int32ListVal{Int32ListVal: int32List}
					case prototypes.Int32List:
						protoVal.Val = &prototypes.Value_Int32ListVal{Int32ListVal: &v}
					case *prototypes.Int32List:
						protoVal.Val = &prototypes.Value_Int32ListVal{Int32ListVal: v}

					case []int64:
						int64List := &prototypes.Int64List{Val: v}
						protoVal.Val = &prototypes.Value_Int64ListVal{Int64ListVal: int64List}
					case prototypes.Int64List:
						protoVal.Val = &prototypes.Value_Int64ListVal{Int64ListVal: &v}
					case *prototypes.Int64List:
						protoVal.Val = &prototypes.Value_Int64ListVal{Int64ListVal: v}

					case []float64:
						doubleList := &prototypes.DoubleList{Val: v}
						protoVal.Val = &prototypes.Value_DoubleListVal{DoubleListVal: doubleList}
					case prototypes.DoubleList:
						protoVal.Val = &prototypes.Value_DoubleListVal{DoubleListVal: &v}
					case *prototypes.DoubleList:
						protoVal.Val = &prototypes.Value_DoubleListVal{DoubleListVal: v}

					case []float32:
						floatList := &prototypes.FloatList{Val: v}
						protoVal.Val = &prototypes.Value_FloatListVal{FloatListVal: floatList}
					case prototypes.FloatList:
						protoVal.Val = &prototypes.Value_FloatListVal{FloatListVal: &v}
					case *prototypes.FloatList:
						protoVal.Val = &prototypes.Value_FloatListVal{FloatListVal: v}

					case []bool:
						boolList := &prototypes.BoolList{Val: v}
						protoVal.Val = &prototypes.Value_BoolListVal{BoolListVal: boolList}
					case prototypes.BoolList:
						protoVal.Val = &prototypes.Value_BoolListVal{BoolListVal: &v}
					case *prototypes.BoolList:
						protoVal.Val = &prototypes.Value_BoolListVal{BoolListVal: v}

					case []time.Time:
						timestamps := make([]int64, len(v))
						for j, t := range v {
							timestamps[j] = t.Unix()
						}
						timestampList := &prototypes.Int64List{Val: timestamps}
						protoVal.Val = &prototypes.Value_UnixTimestampListVal{UnixTimestampListVal: timestampList}

					case []*timestamppb.Timestamp:
						timestamps := make([]int64, len(v))
						for j, t := range v {
							timestamps[j] = t.GetSeconds()
						}
						timestampList := &prototypes.Int64List{Val: timestamps}
						protoVal.Val = &prototypes.Value_UnixTimestampListVal{UnixTimestampListVal: timestampList}

					case prototypes.Null:
						protoVal.Val = &prototypes.Value_NullVal{NullVal: prototypes.Null_NULL}

					case *prototypes.Value:
						protoVal = v

					default:
						switch {
						case tryConvertToInt32(&protoVal, v):
						case tryConvertToInt64(&protoVal, v):
						case tryConvertToFloat(&protoVal, v):
						case tryConvertToDouble(&protoVal, v):
						default:
							return nil, fmt.Errorf("unsupported value type for feature %s: %T",
								currentVector.Name, v)
						}
					}

					var timestamp *timestamppb.Timestamp
					if i < len(featureData.EventTimestamps) {
						ts := &featureData.EventTimestamps[i]
						if ts.GetSeconds() != 0 || ts.GetNanos() != 0 {
							timestamp = &timestamppb.Timestamp{
								Seconds: ts.GetSeconds(),
								Nanos:   ts.GetNanos(),
							}
						} else {
							timestamp = &timestamppb.Timestamp{}
						}
					} else {
						timestamp = &timestamppb.Timestamp{}
					}

					if timestamp.GetSeconds() > 0 && checkOutsideTtl(timestamp, timestamppb.Now(), sfv.FeatureView.Ttl) {
						rangeStatuses[i] = serving.FieldStatus_OUTSIDE_MAX_AGE
					} else {
						rangeStatuses[i] = serving.FieldStatus_PRESENT
					}

					rangeValues[i] = protoVal
					rangeTimestamps[i] = timestamp
				}
			}

			for _, rowIndex := range outputIndexes {
				rangeValuesByRow[rowIndex] = &prototypes.RepeatedValue{Val: rangeValues}
				currentVector.RangeStatuses[rowIndex] = rangeStatuses
				currentVector.RangeTimestamps[rowIndex] = rangeTimestamps
			}
		}

		arrowRangeValues, err := types.RepeatedProtoValuesToArrowArray(rangeValuesByRow, arrowAllocator, numRows)
		if err != nil {
			return nil, err
		}
		currentVector.RangeValues = arrowRangeValues
	}

	return vectors, nil
}

func tryConvertToInt32(protoVal **prototypes.Value, v interface{}) bool {
	switch num := v.(type) {
	case int:
		if num <= math.MaxInt32 && num >= math.MinInt32 {
			(*protoVal).Val = &prototypes.Value_Int32Val{Int32Val: int32(num)}
			return true
		}
	case int8:
		(*protoVal).Val = &prototypes.Value_Int32Val{Int32Val: int32(num)}
		return true
	case int16:
		(*protoVal).Val = &prototypes.Value_Int32Val{Int32Val: int32(num)}
		return true
	case uint8:
		(*protoVal).Val = &prototypes.Value_Int32Val{Int32Val: int32(num)}
		return true
	case uint16:
		(*protoVal).Val = &prototypes.Value_Int32Val{Int32Val: int32(num)}
		return true
	case uint:
		if num <= math.MaxInt32 {
			(*protoVal).Val = &prototypes.Value_Int32Val{Int32Val: int32(num)}
			return true
		}
	}
	return false
}

func tryConvertToInt64(protoVal **prototypes.Value, v interface{}) bool {
	switch num := v.(type) {
	case int:
		(*protoVal).Val = &prototypes.Value_Int64Val{Int64Val: int64(num)}
		return true
	case uint:
		if num <= math.MaxInt64 {
			(*protoVal).Val = &prototypes.Value_Int64Val{Int64Val: int64(num)}
			return true
		}
	case uint32:
		(*protoVal).Val = &prototypes.Value_Int64Val{Int64Val: int64(num)}
		return true
	case uint64:
		if num <= math.MaxInt64 {
			(*protoVal).Val = &prototypes.Value_Int64Val{Int64Val: int64(num)}
			return true
		}
	}
	return false
}

func tryConvertToFloat(protoVal **prototypes.Value, v interface{}) bool {
	switch num := v.(type) {
	case float32:
		(*protoVal).Val = &prototypes.Value_FloatVal{FloatVal: num}
		return true
	}
	return false
}

func tryConvertToDouble(protoVal **prototypes.Value, v interface{}) bool {
	switch num := v.(type) {
	case float64:
		(*protoVal).Val = &prototypes.Value_DoubleVal{DoubleVal: num}
		return true
	}
	return false
}

func KeepOnlyRequestedFeatures(
	vectors []*FeatureVector,
	requestedFeatureRefs []string,
	featureService *model.FeatureService,
	fullFeatureNames bool) ([]*FeatureVector, error) {
	vectorsByName := make(map[string]*FeatureVector)
	expectedVectors := make([]*FeatureVector, 0)

	usedVectors := make(map[string]bool)

	for _, vector := range vectors {
		vectorsByName[vector.Name] = vector
	}

	if featureService != nil {
		for _, projection := range featureService.Projections {
			for _, f := range projection.Features {
				requestedFeatureRefs = append(requestedFeatureRefs,
					fmt.Sprintf("%s:%s", projection.NameToUse(), f.Name))
			}
		}
	}

	for _, featureRef := range requestedFeatureRefs {
		viewName, featureName, err := ParseFeatureReference(featureRef)
		if err != nil {
			return nil, err
		}
		qualifiedName := getQualifiedFeatureName(viewName, featureName, fullFeatureNames)
		if _, ok := vectorsByName[qualifiedName]; !ok {
			return nil, fmt.Errorf("requested feature %s can't be retrieved", featureRef)
		}
		expectedVectors = append(expectedVectors, vectorsByName[qualifiedName])
		usedVectors[qualifiedName] = true
	}

	// Free arrow arrays for vectors that were not used.
	for _, vector := range vectors {
		if _, ok := usedVectors[vector.Name]; !ok {
			vector.Values.Release()
		}
	}

	return expectedVectors, nil
}

func EntitiesToFeatureVectors(entityColumns map[string]*prototypes.RepeatedValue, arrowAllocator memory.Allocator, numRows int) ([]*FeatureVector, error) {
	vectors := make([]*FeatureVector, 0)
	presentVector := make([]serving.FieldStatus, numRows)
	timestampVector := make([]*timestamppb.Timestamp, numRows)
	for idx := 0; idx < numRows; idx++ {
		presentVector[idx] = serving.FieldStatus_PRESENT
		timestampVector[idx] = timestamppb.Now()
	}
	for entityName, values := range entityColumns {
		arrowColumn, err := types.ProtoValuesToArrowArray(values.Val, arrowAllocator, numRows)
		if err != nil {
			return nil, err
		}
		vectors = append(vectors, &FeatureVector{
			Name:       entityName,
			Values:     arrowColumn,
			Statuses:   presentVector,
			Timestamps: timestampVector,
		})
	}
	return vectors, nil
}

func EntitiesToRangeFeatureVectors(
	entityColumns map[string]*prototypes.RepeatedValue,
	arrowAllocator memory.Allocator,
	numRows int) ([]*RangeFeatureVector, error) {

	vectors := make([]*RangeFeatureVector, 0)

	for entityName, values := range entityColumns {
		entityRangeValues := make([]*prototypes.RepeatedValue, numRows)
		rangeStatuses := make([][]serving.FieldStatus, numRows)
		rangeTimestamps := make([][]*timestamppb.Timestamp, numRows)

		for idx := 0; idx < numRows; idx++ {
			entityRangeValues[idx] = &prototypes.RepeatedValue{Val: []*prototypes.Value{values.Val[idx]}}
			rangeStatuses[idx] = []serving.FieldStatus{serving.FieldStatus_PRESENT}
			rangeTimestamps[idx] = []*timestamppb.Timestamp{timestamppb.Now()}
		}

		arrowRangeValues, err := types.RepeatedProtoValuesToArrowArray(entityRangeValues, arrowAllocator, numRows)
		if err != nil {
			return nil, err
		}

		vectors = append(vectors, &RangeFeatureVector{
			Name:            entityName,
			RangeValues:     arrowRangeValues,
			RangeStatuses:   rangeStatuses,
			RangeTimestamps: rangeTimestamps,
		})
	}

	return vectors, nil
}

func ParseFeatureReference(featureRef string) (featureViewName, featureName string, e error) {
	parsedFeatureName := strings.Split(featureRef, ":")

	if len(parsedFeatureName) == 0 {
		e = errors.New("featureReference should be in the format: 'FeatureViewName:FeatureName'")
	} else if len(parsedFeatureName) == 1 {
		featureName = parsedFeatureName[0]
	} else {
		featureViewName = parsedFeatureName[0]
		featureName = parsedFeatureName[1]
	}
	return
}

func entityKeysToProtos(joinKeyValues map[string]*prototypes.RepeatedValue) []*prototypes.EntityKey {
	keys := make([]string, len(joinKeyValues))
	index := 0
	var numRows int
	for k, v := range joinKeyValues {
		keys[index] = k
		index += 1
		numRows = len(v.Val)
	}
	sort.Strings(keys)
	entityKeys := make([]*prototypes.EntityKey, numRows)
	numJoinKeys := len(keys)
	// Construct each EntityKey object
	for index = 0; index < numRows; index++ {
		entityKeys[index] = &prototypes.EntityKey{JoinKeys: keys, EntityValues: make([]*prototypes.Value, numJoinKeys)}
	}

	for colIndex, key := range keys {
		for index, value := range joinKeyValues[key].GetVal() {
			entityKeys[index].EntityValues[colIndex] = value
		}
	}
	return entityKeys
}

func GroupFeatureRefs(requestedFeatureViews []*FeatureViewAndRefs,
	joinKeyValues map[string]*prototypes.RepeatedValue,
	entityNameToJoinKeyMap map[string]string,
	fullFeatureNames bool,
) (map[string]*GroupedFeaturesPerEntitySet,
	error,
) {
	groups := make(map[string]*GroupedFeaturesPerEntitySet)

	for _, featuresAndView := range requestedFeatureViews {
		joinKeys := make([]string, 0)
		fv := featuresAndView.View
		featureNames := featuresAndView.FeatureRefs
		for _, entityName := range fv.EntityNames {
			joinKeys = append(joinKeys, entityNameToJoinKeyMap[entityName])
		}

		groupKeyBuilder := make([]string, 0)
		joinKeysValuesProjection := make(map[string]*prototypes.RepeatedValue)

		joinKeyToAliasMap := make(map[string]string)
		if fv.Base.Projection != nil && fv.Base.Projection.JoinKeyMap != nil {
			joinKeyToAliasMap = fv.Base.Projection.JoinKeyMap
		}

		for _, joinKey := range joinKeys {
			var joinKeyOrAlias string

			if alias, ok := joinKeyToAliasMap[joinKey]; ok {
				groupKeyBuilder = append(groupKeyBuilder, fmt.Sprintf("%s[%s]", joinKey, alias))
				joinKeyOrAlias = alias
			} else {
				groupKeyBuilder = append(groupKeyBuilder, joinKey)
				joinKeyOrAlias = joinKey
			}

			if _, ok := joinKeyValues[joinKeyOrAlias]; !ok {
				return nil, fmt.Errorf("key %s is missing in provided entity rows", joinKey)
			}
			joinKeysValuesProjection[joinKey] = joinKeyValues[joinKeyOrAlias]
		}

		sort.Strings(groupKeyBuilder)
		groupKey := strings.Join(groupKeyBuilder, ",")

		aliasedFeatureNames := make([]string, 0)
		featureViewNames := make([]string, 0)
		var viewNameToUse string
		if fv.Base.Projection != nil {
			viewNameToUse = fv.Base.Projection.NameToUse()
		} else {
			viewNameToUse = fv.Base.Name
		}

		for _, featureName := range featureNames {
			aliasedFeatureNames = append(aliasedFeatureNames,
				getQualifiedFeatureName(viewNameToUse, featureName, fullFeatureNames))
			featureViewNames = append(featureViewNames, fv.Base.Name)
		}

		if _, ok := groups[groupKey]; !ok {
			joinKeysProto := entityKeysToProtos(joinKeysValuesProjection)
			uniqueEntityRows, mappingIndices, err := getUniqueEntityRows(joinKeysProto)
			if err != nil {
				return nil, err
			}

			groups[groupKey] = &GroupedFeaturesPerEntitySet{
				FeatureNames:        featureNames,
				FeatureViewNames:    featureViewNames,
				AliasedFeatureNames: aliasedFeatureNames,
				Indices:             mappingIndices,
				EntityKeys:          uniqueEntityRows,
			}

		} else {
			groups[groupKey].FeatureNames = append(groups[groupKey].FeatureNames, featureNames...)
			groups[groupKey].AliasedFeatureNames = append(groups[groupKey].AliasedFeatureNames, aliasedFeatureNames...)
			groups[groupKey].FeatureViewNames = append(groups[groupKey].FeatureViewNames, featureViewNames...)
		}
	}
	return groups, nil
}

func GroupSortedFeatureRefs(
	sortedViews []*SortedFeatureViewAndRefs,
	joinKeyValues map[string]*prototypes.RepeatedValue,
	entityNameToJoinKeyMap map[string]string,
	sortKeyFilters []*serving.SortKeyFilter,
	reverseSortOrder bool,
	limit int32,
	fullFeatureNames bool) ([]*GroupedRangeFeatureRefs, error) {

	groups := make(map[string]*GroupedRangeFeatureRefs)
	sortKeyFilterMap := make(map[string]*serving.SortKeyFilter)
	for _, sortKeyFilter := range sortKeyFilters {
		sortKeyFilterMap[sortKeyFilter.SortKeyName] = sortKeyFilter
	}

	for _, featuresAndView := range sortedViews {
		joinKeys := make([]string, 0)
		sfv := featuresAndView.View
		featureNames := featuresAndView.FeatureRefs

		for _, entityName := range sfv.FeatureView.EntityNames {
			joinKeys = append(joinKeys, entityNameToJoinKeyMap[entityName])
		}

		groupKeyBuilder := make([]string, 0)
		joinKeysValuesProjection := make(map[string]*prototypes.RepeatedValue)

		joinKeyToAliasMap := make(map[string]string)
		if sfv.Base.Projection != nil && sfv.Base.Projection.JoinKeyMap != nil {
			joinKeyToAliasMap = sfv.Base.Projection.JoinKeyMap
		}

		for _, joinKey := range joinKeys {
			var joinKeyOrAlias string

			if alias, ok := joinKeyToAliasMap[joinKey]; ok {
				groupKeyBuilder = append(groupKeyBuilder, fmt.Sprintf("%s[%s]", joinKey, alias))
				joinKeyOrAlias = alias
			} else {
				groupKeyBuilder = append(groupKeyBuilder, joinKey)
				joinKeyOrAlias = joinKey
			}

			if _, ok := joinKeyValues[joinKeyOrAlias]; !ok {
				return nil, fmt.Errorf("key %s is missing in provided entity rows", joinKey)
			}
			joinKeysValuesProjection[joinKey] = joinKeyValues[joinKeyOrAlias]
		}

		sort.Strings(groupKeyBuilder)
		groupKey := strings.Join(groupKeyBuilder, ",")

		aliasedFeatureNames := make([]string, 0)
		featureViewNames := make([]string, 0)
		var viewNameToUse string
		if sfv.Base.Projection != nil {
			viewNameToUse = sfv.Base.Projection.NameToUse()
		} else {
			viewNameToUse = sfv.Base.Name
		}

		for _, featureName := range featureNames {
			aliasedFeatureNames = append(aliasedFeatureNames,
				getQualifiedFeatureName(viewNameToUse, featureName, fullFeatureNames))
			featureViewNames = append(featureViewNames, sfv.Base.Name)
		}

		sortKeyFilterModels := make([]*model.SortKeyFilter, 0)
		sortKeyOrderMap := make(map[string]model.SortOrder)
		for _, sortKey := range featuresAndView.View.SortKeys {
			sortKeyOrderMap[sortKey.FieldName] = *sortKey.Order
		}
		for _, sortKey := range featuresAndView.View.SortKeys {
			var sortOrder core.SortOrder_Enum
			if reverseSortOrder {
				if *sortKey.Order.Order.Enum() == core.SortOrder_ASC {
					sortOrder = core.SortOrder_DESC
				} else {
					sortOrder = core.SortOrder_ASC
				}
			} else {
				sortOrder = *sortKey.Order.Order.Enum()
			}
			var filterModel *model.SortKeyFilter
			if filter, ok := sortKeyFilterMap[sortKey.FieldName]; ok {
				filterModel = model.NewSortKeyFilterFromProto(filter, sortOrder)
			} else {
				// create empty filter model with only sort order
				filterModel = &model.SortKeyFilter{
					SortKeyName: sortKey.FieldName,
					Order:       model.NewSortOrderFromProto(sortOrder),
				}
			}
			sortKeyFilterModels = append(sortKeyFilterModels, filterModel)
		}

		if _, ok := groups[groupKey]; !ok {
			joinKeysProto := entityKeysToProtos(joinKeysValuesProjection)
			uniqueEntityRows, mappingIndices, err := getUniqueEntityRows(joinKeysProto)
			if err != nil {
				return nil, err
			}

			groups[groupKey] = &GroupedRangeFeatureRefs{
				FeatureNames:        featureNames,
				FeatureViewNames:    featureViewNames,
				AliasedFeatureNames: aliasedFeatureNames,
				Indices:             mappingIndices,
				EntityKeys:          uniqueEntityRows,
				SortKeyFilters:      sortKeyFilterModels,
				Limit:               limit,
			}

		} else {
			groups[groupKey].FeatureNames = append(groups[groupKey].FeatureNames, featureNames...)
			groups[groupKey].AliasedFeatureNames = append(groups[groupKey].AliasedFeatureNames, aliasedFeatureNames...)
			groups[groupKey].FeatureViewNames = append(groups[groupKey].FeatureViewNames, featureViewNames...)
		}
	}

	result := make([]*GroupedRangeFeatureRefs, 0, len(groups))
	for _, group := range groups {
		result = append(result, group)
	}

	return result, nil
}

func getUniqueEntityRows(joinKeysProto []*prototypes.EntityKey) ([]*prototypes.EntityKey, [][]int, error) {
	uniqueValues := make(map[[sha256.Size]byte]*prototypes.EntityKey, 0)
	positions := make(map[[sha256.Size]byte][]int, 0)

	for index, entityKey := range joinKeysProto {
		serializedRow, err := proto.Marshal(entityKey)
		if err != nil {
			return nil, nil, err
		}

		rowHash := sha256.Sum256(serializedRow)
		if _, ok := uniqueValues[rowHash]; !ok {
			uniqueValues[rowHash] = entityKey
			positions[rowHash] = []int{index}
		} else {
			positions[rowHash] = append(positions[rowHash], index)
		}
	}

	mappingIndices := make([][]int, len(uniqueValues))
	uniqueEntityRows := make([]*prototypes.EntityKey, 0)
	for rowHash, row := range uniqueValues {
		nextIdx := len(uniqueEntityRows)

		mappingIndices[nextIdx] = positions[rowHash]
		uniqueEntityRows = append(uniqueEntityRows, row)
	}
	return uniqueEntityRows, mappingIndices, nil
}

func HasEntityInSortedFeatureView(view *model.SortedFeatureView, entityName string) bool {
	for _, name := range view.FeatureView.EntityNames {
		if name == entityName {
			return true
		}
	}
	return false
}

func checkOutsideTtl(featureTimestamp *timestamppb.Timestamp, currentTimestamp *timestamppb.Timestamp, ttl *durationpb.Duration) bool {
	if ttl.Seconds == 0 {
		return false
	}
	return currentTimestamp.GetSeconds()-featureTimestamp.GetSeconds() > ttl.Seconds
}

func getQualifiedFeatureName(viewName string, featureName string, fullFeatureNames bool) string {
	if fullFeatureNames {
		return fmt.Sprintf("%s__%s", viewName, featureName)
	} else {
		return featureName
	}
}

type featureNameCollisionError struct {
	featureRefCollisions []string
	fullFeatureNames     bool
}

func (e featureNameCollisionError) Error() string {
	return fmt.Sprintf("featureNameCollisionError: %s; %t", strings.Join(e.featureRefCollisions, ", "), e.fullFeatureNames)
}
