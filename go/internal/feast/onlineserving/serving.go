package onlineserving

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/feast-dev/feast/go/internal/feast/model"
	"github.com/feast-dev/feast/go/internal/feast/onlinestore"
	"github.com/feast-dev/feast/go/internal/feast/registry"
	"github.com/feast-dev/feast/go/protos/feast/core"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	prototypes "github.com/feast-dev/feast/go/protos/feast/types"
	"github.com/feast-dev/feast/go/types"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
Return

	(1) requested feature views and features grouped per View
	(2) requested on demand feature views

existed in the registry
*/
func GetFeatureViewsToUseByService(
	featureService *model.FeatureService,
	registry *registry.Registry,
	projectName string) ([]*FeatureViewAndRefs, []*SortedFeatureViewAndRefs, []*model.OnDemandFeatureView, error) {

	viewNameToViewAndRefs := make(map[string]*FeatureViewAndRefs)
	viewNameToSortedViewAndRefs := make(map[string]*SortedFeatureViewAndRefs)
	odFvsToUse := make([]*model.OnDemandFeatureView, 0)

	for _, featureProjection := range featureService.Projections {
		// Create copies of FeatureView that may contains the same *FeatureView but
		// each differentiated by a *FeatureViewProjection
		featureViewName := featureProjection.Name
		if fv, fvErr := registry.GetFeatureView(projectName, featureViewName); fvErr == nil {
			base, err := fv.Base.WithProjection(featureProjection)
			if err != nil {
				return nil, nil, nil, err
			}
			if _, ok := viewNameToViewAndRefs[featureProjection.NameToUse()]; !ok {
				view := fv.NewFeatureViewFromBase(base)
				view.EntityColumns = fv.EntityColumns
				viewNameToViewAndRefs[featureProjection.NameToUse()] = &FeatureViewAndRefs{
					View:        view,
					FeatureRefs: []string{},
				}
			}

			for _, feature := range featureProjection.Features {
				viewNameToViewAndRefs[featureProjection.NameToUse()].FeatureRefs =
					addStringIfNotContains(viewNameToViewAndRefs[featureProjection.NameToUse()].FeatureRefs,
						feature.Name)
			}

		} else if sortedFv, sortedFvErr := registry.GetSortedFeatureView(projectName, featureViewName); sortedFvErr == nil {
			base, err := sortedFv.Base.WithProjection(featureProjection)
			if err != nil {
				return nil, nil, nil, err
			}
			if _, ok := viewNameToSortedViewAndRefs[featureProjection.NameToUse()]; !ok {
				view := sortedFv.NewSortedFeatureViewFromBase(base)
				view.EntityColumns = sortedFv.EntityColumns
				viewNameToSortedViewAndRefs[featureProjection.NameToUse()] = &SortedFeatureViewAndRefs{
					View:        view,
					FeatureRefs: []string{},
				}
			}

			for _, feature := range featureProjection.Features {
				viewNameToSortedViewAndRefs[featureProjection.NameToUse()].FeatureRefs =
					addStringIfNotContains(viewNameToSortedViewAndRefs[featureProjection.NameToUse()].FeatureRefs,
						feature.Name)
			}
		} else if odFv, odFvErr := registry.GetOnDemandFeatureView(projectName, featureViewName); odFvErr == nil {
			projectedOdFv, err := odFv.NewWithProjection(featureProjection)
			if err != nil {
				return nil, nil, nil, err
			}
			odFvsToUse = append(odFvsToUse, projectedOdFv)
			err = extractOdFvDependencies(
				projectedOdFv,
				registry,
				projectName,
				viewNameToViewAndRefs)
			if err != nil {
				return nil, nil, nil, err
			}
		} else {
			log.Error().Errs("any feature view", []error{fvErr, sortedFvErr, odFvErr}).Msgf("Feature view %s not found", featureViewName)
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
	registry *registry.Registry,
	projectName string) ([]*FeatureViewAndRefs, []*SortedFeatureViewAndRefs, []*model.OnDemandFeatureView, error) {
	viewNameToViewAndRefs := make(map[string]*FeatureViewAndRefs)
	viewNameToSortedViewAndRefs := make(map[string]*SortedFeatureViewAndRefs)
	odFvToFeatures := make(map[string][]string)
	odFvToProjectWithFeatures := make(map[string]*model.OnDemandFeatureView)

	for _, featureRef := range features {
		featureViewName, featureName, err := ParseFeatureReference(featureRef)
		if err != nil {
			return nil, nil, nil, err
		}
		if fv, err := registry.GetFeatureView(projectName, featureViewName); err == nil {
			if viewAndRef, ok := viewNameToViewAndRefs[fv.Base.Name]; ok {
				viewAndRef.FeatureRefs = addStringIfNotContains(viewAndRef.FeatureRefs, featureName)
			} else {
				viewNameToViewAndRefs[fv.Base.Name] = &FeatureViewAndRefs{
					View:        fv,
					FeatureRefs: []string{featureName},
				}
			}
		} else if sortedFv, err := registry.GetSortedFeatureView(projectName, featureViewName); err == nil {
			if viewAndRef, ok := viewNameToSortedViewAndRefs[sortedFv.Base.Name]; ok {
				viewAndRef.FeatureRefs = addStringIfNotContains(viewAndRef.FeatureRefs, featureName)
			} else {
				viewNameToSortedViewAndRefs[sortedFv.Base.Name] = &SortedFeatureViewAndRefs{
					View:        sortedFv,
					FeatureRefs: []string{featureName},
				}
			}
		} else if odfv, err := registry.GetOnDemandFeatureView(projectName, featureViewName); err == nil {
			if _, ok := odFvToFeatures[odfv.Base.Name]; !ok {
				odFvToFeatures[odfv.Base.Name] = []string{featureName}
			} else {
				odFvToFeatures[odfv.Base.Name] = append(
					odFvToFeatures[odfv.Base.Name], featureName)
			}
			odFvToProjectWithFeatures[odfv.Base.Name] = odfv
		} else {
			return nil, nil, nil, fmt.Errorf("feature View %s doesn't exist, please make sure that you have created the"+
				" feature View %s and that you have registered it by running \"apply\"", featureViewName, featureViewName)
		}
	}

	odFvsToUse := make([]*model.OnDemandFeatureView, 0)

	for odFvName, featureNames := range odFvToFeatures {
		projectedOdFv, err := odFvToProjectWithFeatures[odFvName].ProjectWithFeatures(featureNames)
		if err != nil {
			return nil, nil, nil, err
		}

		err = extractOdFvDependencies(
			projectedOdFv,
			registry,
			projectName,
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
	registry *registry.Registry,
	projectName string,
	requestedFeatures map[string]*FeatureViewAndRefs,
) error {

	for _, sourceFvProjection := range odFv.SourceFeatureViewProjections {
		fv, err := registry.GetFeatureView(projectName, sourceFvProjection.Name)
		if err != nil {
			return err
		}
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

func GetEntityMaps(requestedFeatureViews []*FeatureViewAndRefs, registry *registry.Registry, projectName string) (map[string]string, map[string]interface{}, error) {
	entityNameToJoinKeyMap := make(map[string]string)
	expectedJoinKeysSet := make(map[string]interface{})

	for _, featuresAndView := range requestedFeatureViews {
		featureView := featuresAndView.View
		var joinKeyToAliasMap map[string]string
		if featureView.Base.Projection != nil && featureView.Base.Projection.JoinKeyMap != nil {
			joinKeyToAliasMap = featureView.Base.Projection.JoinKeyMap
		} else {
			joinKeyToAliasMap = map[string]string{}
		}

		for _, entityName := range featureView.EntityNames {
			entity, err := registry.GetEntity(projectName, entityName)
			if err != nil {
				return nil, nil, fmt.Errorf("entity %s doesn't exist in the registry", entityName)
			}
			entityNameToJoinKeyMap[entityName] = entity.JoinKey

			if alias, ok := joinKeyToAliasMap[entity.JoinKey]; ok {
				expectedJoinKeysSet[alias] = nil
			} else {
				expectedJoinKeysSet[entity.JoinKey] = nil
			}
		}
	}
	return entityNameToJoinKeyMap, expectedJoinKeysSet, nil
}

func GetEntityMapsForSortedViews(sortedViews []*SortedFeatureViewAndRefs, registry *registry.Registry, projectName string) (map[string]string, map[string]interface{}, error) {
	entityNameToJoinKeyMap := make(map[string]string)
	expectedJoinKeysSet := make(map[string]interface{})

	for _, featuresAndView := range sortedViews {
		featureView := featuresAndView.View
		var joinKeyToAliasMap map[string]string

		if featureView.Base.Projection != nil && featureView.Base.Projection.JoinKeyMap != nil {
			joinKeyToAliasMap = featureView.Base.Projection.JoinKeyMap
		} else {
			joinKeyToAliasMap = map[string]string{}
		}

		for _, entityName := range featureView.EntityNames {
			entity, err := registry.GetEntity(projectName, entityName)
			if err != nil {
				return nil, nil, fmt.Errorf("entity %s doesn't exist in the registry", entityName)
			}
			entityNameToJoinKeyMap[entityName] = entity.JoinKey

			if alias, ok := joinKeyToAliasMap[entity.JoinKey]; ok {
				expectedJoinKeysSet[alias] = nil
			} else {
				expectedJoinKeysSet[entity.JoinKey] = nil
			}
		}
	}

	return entityNameToJoinKeyMap, expectedJoinKeysSet, nil
}

func ValidateEntityValues(joinKeyValues map[string]*prototypes.RepeatedValue,
	requestData map[string]*prototypes.RepeatedValue,
	expectedJoinKeysSet map[string]interface{}) (int, error) {
	numRows := -1

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

	return numRows, nil
}

func ValidateFeatureRefs(requestedFeatures []*FeatureViewAndRefs, fullFeatureNames bool) error {
	uniqueFeatureRefs := make(map[string]bool)
	collidedFeatureRefs := make([]string, 0)
	for _, viewAndFeatures := range requestedFeatures {
		for _, feature := range viewAndFeatures.FeatureRefs {
			featureName := feature

			if fullFeatureNames {
				projectedViewName := viewAndFeatures.View.Base.Name
				if viewAndFeatures.View.Base.Projection != nil {
					projectedViewName = viewAndFeatures.View.Base.Projection.NameToUse()
				}
				featureName = fmt.Sprintf("%s:%s", projectedViewName, feature)
			}

			if uniqueFeatureRefs[featureName] {
				collidedFeatureRefs = append(collidedFeatureRefs, featureName)
			} else {
				uniqueFeatureRefs[featureName] = true
			}
		}
	}

	if len(collidedFeatureRefs) >= 1 {
		return featureNameCollisionError{collidedFeatureRefs, fullFeatureNames}
	}
	return nil
}

func ValidateSortedFeatureRefs(sortedViews []*SortedFeatureViewAndRefs, fullFeatureNames bool) error {
	uniqueFeatureRefs := make(map[string]bool)
	collidedFeatureRefs := make([]string, 0)
	for _, viewAndFeatures := range sortedViews {
		for _, feature := range viewAndFeatures.FeatureRefs {
			featureName := feature

			if fullFeatureNames {
				projectedViewName := viewAndFeatures.View.Base.Name
				if viewAndFeatures.View.Base.Projection != nil {
					projectedViewName = viewAndFeatures.View.Base.Projection.NameToUse()
				}
				featureName = fmt.Sprintf("%s:%s", projectedViewName, feature)
			}

			if uniqueFeatureRefs[featureName] {
				collidedFeatureRefs = append(collidedFeatureRefs, featureName)
			} else {
				uniqueFeatureRefs[featureName] = true
			}
		}
	}

	if len(collidedFeatureRefs) >= 1 {
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

		if filter.GetEquals() != nil {
			if !isValueTypeCompatible(filter.GetEquals(), expectedType, false) {
				return fmt.Errorf("equals value for sort key '%s' has incompatible type: expected %s",
					filter.SortKeyName, valueTypeToString(expectedType))
			}
		} else if filter.GetRange() == nil {
			return fmt.Errorf("sort key filter for sort key '%s' must have either equals or range_query set",
				filter.SortKeyName)
		} else {
			if filter.GetRange().RangeStart != nil {
				if !isValueTypeCompatible(filter.GetRange().RangeStart, expectedType, true) {
					return fmt.Errorf("range_start value for sort key '%s' has incompatible type: expected %s",
						filter.SortKeyName, valueTypeToString(expectedType))
				}
			}

			if filter.GetRange().RangeEnd != nil {
				if !isValueTypeCompatible(filter.GetRange().RangeEnd, expectedType, true) {
					return fmt.Errorf("range_end value for sort key '%s' has incompatible type: expected %s",
						filter.SortKeyName, valueTypeToString(expectedType))
				}
			}
		}
	}

	return ValidateSortKeyFilterOrder(filters, sortedViews)
}

func ValidateSortKeyFilterOrder(filters []*serving.SortKeyFilter, sortedViews []*SortedFeatureViewAndRefs) error {
	filtersByName := make(map[string]*serving.SortKeyFilter)
	for _, filter := range filters {
		filtersByName[filter.SortKeyName] = filter
	}

	for _, sortedView := range sortedViews {
		if len(sortedView.View.SortKeys) > 1 {
			orderedFilters := make([]*serving.SortKeyFilter, 0)

			for _, sortKey := range sortedView.View.SortKeys {
				orderedFilters = append(orderedFilters, filtersByName[sortKey.FieldName])
			}

			for i, filter := range orderedFilters[:len(orderedFilters)-1] {
				if filter == nil {
					return fmt.Errorf("specify sort key filter in request for sort key: '%s' with query type equals", sortedView.View.SortKeys[i].FieldName)
				}

				if filter.GetEquals() == nil {
					return fmt.Errorf("sort key filter for sort key '%s' must have query type equals instead of range",
						filter.SortKeyName)
				}
			}
		}
	}

	return nil
}

func isValueTypeCompatible(value *prototypes.Value, expectedType prototypes.ValueType_Enum, canBeNull bool) bool {
	if value == nil || value.Val == nil {
		return canBeNull
	}

	switch value.Val.(type) {
	case *prototypes.Value_Int32Val:
		return expectedType == prototypes.ValueType_INT32
	case *prototypes.Value_Int64Val:
		return expectedType == prototypes.ValueType_INT64
	case *prototypes.Value_FloatVal:
		return expectedType == prototypes.ValueType_FLOAT
	case *prototypes.Value_DoubleVal:
		return expectedType == prototypes.ValueType_DOUBLE
	case *prototypes.Value_UnixTimestampVal:
		return expectedType == prototypes.ValueType_UNIX_TIMESTAMP
	case *prototypes.Value_StringVal:
		return expectedType == prototypes.ValueType_STRING
	case *prototypes.Value_BoolVal:
		return expectedType == prototypes.ValueType_BOOL
	case *prototypes.Value_BytesVal:
		return expectedType == prototypes.ValueType_BYTES
	case *prototypes.Value_NullVal:
		return canBeNull
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

	var featureData *onlinestore.FeatureData
	var fv *model.FeatureView
	var featureViewName string

	vectors := make([]*FeatureVector, numFeatures)

	for featureIndex := 0; featureIndex < numFeatures; featureIndex++ {
		currentVector := &FeatureVector{
			Name:       groupRef.AliasedFeatureNames[featureIndex],
			Statuses:   make([]serving.FieldStatus, numRows),
			Timestamps: make([]*timestamppb.Timestamp, numRows),
		}
		vectors[featureIndex] = currentVector
		protoValues := make([]*prototypes.Value, numRows)

		for rowEntityIndex, outputIndexes := range groupRef.Indices {

			var (
				value          *prototypes.Value
				status         serving.FieldStatus
				eventTimeStamp *timestamppb.Timestamp
			)
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
	groupRef *model.GroupedRangeFeatureRefs,
	sortedViews []*SortedFeatureViewAndRefs,
	arrowAllocator memory.Allocator,
	numRows int) ([]*RangeFeatureVector, error) {

	numFeatures := len(groupRef.AliasedFeatureNames)
	sfvs := make(map[string]*model.SortedFeatureView)
	for _, viewAndRefs := range sortedViews {
		sfvs[viewAndRefs.View.Base.Name] = viewAndRefs.View
	}

	vectors := make([]*RangeFeatureVector, numFeatures)

	for featureIndex := 0; featureIndex < numFeatures; featureIndex++ {
		currentVector := initializeRangeFeatureVector(groupRef.AliasedFeatureNames[featureIndex], numRows)
		vectors[featureIndex] = currentVector

		rangeValuesByRow := make([]*prototypes.RepeatedValue, numRows)
		for i := range rangeValuesByRow {
			rangeValuesByRow[i] = &prototypes.RepeatedValue{Val: make([]*prototypes.Value, 0)}
		}

		for rowEntityIndex, outputIndexes := range groupRef.Indices {
			rangeValues, rangeStatuses, rangeTimestamps, err := processFeatureRowData(
				featureData2D, rowEntityIndex, featureIndex, sfvs)
			if err != nil {
				return nil, err
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

func initializeRangeFeatureVector(name string, numRows int) *RangeFeatureVector {
	return &RangeFeatureVector{
		Name:            name,
		RangeStatuses:   make([][]serving.FieldStatus, numRows),
		RangeTimestamps: make([][]*timestamppb.Timestamp, numRows),
	}
}

func processFeatureRowData(
	featureData2D [][]onlinestore.RangeFeatureData,
	rowEntityIndex int,
	featureIndex int,
	sfvs map[string]*model.SortedFeatureView) ([]*prototypes.Value, []serving.FieldStatus, []*timestamppb.Timestamp, error) {

	if featureData2D[rowEntityIndex] == nil || len(featureData2D[rowEntityIndex]) <= featureIndex {
		return make([]*prototypes.Value, 0),
			make([]serving.FieldStatus, 0),
			make([]*timestamppb.Timestamp, 0),
			nil
	}

	featureData := featureData2D[rowEntityIndex][featureIndex]
	featureViewName := featureData.FeatureView

	sfv, exists := sfvs[featureViewName]
	if !exists {
		return nil, nil, nil, fmt.Errorf("feature view '%s' not found in the provided sorted feature views", featureViewName)
	}

	numValues := len(featureData.Values)
	rangeValues := make([]*prototypes.Value, numValues)
	rangeStatuses := make([]serving.FieldStatus, numValues)
	rangeTimestamps := make([]*timestamppb.Timestamp, numValues)

	for i, val := range featureData.Values {
		if val == nil {
			rangeValues[i] = nil
			rangeStatuses[i] = serving.FieldStatus_NOT_FOUND
			rangeTimestamps[i] = &timestamppb.Timestamp{}
			continue
		}

		protoVal, err := types.InterfaceToProtoValue(val)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("error converting value for feature %s: %v", featureData.FeatureName, err)
		}

		// Explicitly set to nil if status is NOT_FOUND
		if i < len(featureData.Statuses) &&
			(featureData.Statuses[i] == serving.FieldStatus_NOT_FOUND ||
				featureData.Statuses[i] == serving.FieldStatus_NULL_VALUE) {
			rangeValues[i] = nil
		} else {
			rangeValues[i] = protoVal
		}

		eventTimestamp := getEventTimestamp(featureData.EventTimestamps, i)

		status := serving.FieldStatus_PRESENT
		if i < len(featureData.Statuses) {
			status = featureData.Statuses[i]
		} else if eventTimestamp.GetSeconds() > 0 && checkOutsideTtl(eventTimestamp, timestamppb.Now(), sfv.FeatureView.Ttl) {
			status = serving.FieldStatus_OUTSIDE_MAX_AGE
		}

		rangeStatuses[i] = status
		rangeTimestamps[i] = eventTimestamp
	}

	return rangeValues, rangeStatuses, rangeTimestamps, nil
}

func getEventTimestamp(timestamps []timestamp.Timestamp, index int) *timestamppb.Timestamp {
	if index < len(timestamps) {
		ts := &timestamps[index]
		if ts.GetSeconds() != 0 || ts.GetNanos() != 0 {
			return &timestamppb.Timestamp{
				Seconds: ts.GetSeconds(),
				Nanos:   ts.GetNanos(),
			}
		}
	}
	return &timestamppb.Timestamp{}
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
				return nil, fmt.Errorf("key %s is missing in provided entity rows for view %s", joinKey, fv.Base.Name)
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
	fullFeatureNames bool) ([]*model.GroupedRangeFeatureRefs, error) {

	groups := make(map[string]*model.GroupedRangeFeatureRefs)
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
		sortKeyNamesMap := make(map[string]bool)
		for _, sortKey := range featuresAndView.View.SortKeys {
			sortKeyNamesMap[sortKey.FieldName] = true
			var sortOrder *core.SortOrder_Enum
			if reverseSortOrder {
				flipped := core.SortOrder_DESC
				if *sortKey.Order.Order.Enum() == core.SortOrder_DESC {
					flipped = core.SortOrder_ASC
				}
				sortOrder = &flipped // non-nil only when sort key order is reversed
			}

			var filterModel *model.SortKeyFilter
			if filter, ok := sortKeyFilterMap[sortKey.FieldName]; ok {
				filterModel = model.NewSortKeyFilterFromProto(filter, sortOrder)
			} else if reverseSortOrder {
				filterModel = &model.SortKeyFilter{
					SortKeyName: sortKey.FieldName,
					Order:       model.NewSortOrderFromProto(*sortOrder),
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

			groups[groupKey] = &model.GroupedRangeFeatureRefs{
				FeatureNames:        featureNames,
				FeatureViewNames:    featureViewNames,
				AliasedFeatureNames: aliasedFeatureNames,
				Indices:             mappingIndices,
				EntityKeys:          uniqueEntityRows,
				SortKeyFilters:      sortKeyFilterModels,
				Limit:               limit,
				IsReverseSortOrder:  reverseSortOrder,
				SortKeyNames:        sortKeyNamesMap,
			}

		} else {
			groups[groupKey].FeatureNames = append(groups[groupKey].FeatureNames, featureNames...)
			groups[groupKey].AliasedFeatureNames = append(groups[groupKey].AliasedFeatureNames, aliasedFeatureNames...)
			groups[groupKey].FeatureViewNames = append(groups[groupKey].FeatureViewNames, featureViewNames...)
		}
	}

	result := make([]*model.GroupedRangeFeatureRefs, 0, len(groups))
	for _, group := range groups {
		result = append(result, group)
	}

	return result, nil
}

func getUniqueEntityRows(joinKeysProto []*prototypes.EntityKey) ([]*prototypes.EntityKey, [][]int, error) {
	seen := make(map[[sha256.Size]byte]int)
	uniqueEntityRows := make([]*prototypes.EntityKey, 0)
	mappingIndices := make([][]int, 0)

	for index, entityKey := range joinKeysProto {
		serializedRow, err := proto.Marshal(entityKey)
		if err != nil {
			return nil, nil, err
		}

		rowHash := sha256.Sum256(serializedRow)
		if existingIndex, exists := seen[rowHash]; exists {
			mappingIndices[existingIndex] = append(mappingIndices[existingIndex], index)
		} else {
			seen[rowHash] = len(uniqueEntityRows)
			uniqueEntityRows = append(uniqueEntityRows, entityKey)
			mappingIndices = append(mappingIndices, []int{index})
		}
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
