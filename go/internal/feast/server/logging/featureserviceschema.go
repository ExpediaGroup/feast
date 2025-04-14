package logging

import (
	"github.com/feast-dev/feast/go/protos/feast/types"
)

type FeatureServiceSchema struct {
	JoinKeys    []string
	Features    []string
	RequestData []string

	JoinKeysTypes    map[string]types.ValueType_Enum
	FeaturesTypes    map[string]types.ValueType_Enum
	RequestDataTypes map[string]types.ValueType_Enum
}

func GenerateSchemaFromFeatureService(fs FeatureStore, featureServiceName string) (*FeatureServiceSchema, error) {
	joinKeys, features, requestData, entityJoinKeyToType, allFeatureTypes, requestDataTypes, err := fs.GetFcosMap(featureServiceName)
	if err != nil {
		return nil, err
	}

	schema := &FeatureServiceSchema{
		JoinKeys:    joinKeys,
		Features:    features,
		RequestData: requestData,

		JoinKeysTypes:    entityJoinKeyToType,
		FeaturesTypes:    allFeatureTypes,
		RequestDataTypes: requestDataTypes,
	}
	return schema, nil
}
