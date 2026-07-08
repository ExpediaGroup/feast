package debuglogging

import (
	"testing"

	"github.com/feast-dev/feast/go/internal/feast/onlineserving"
	"github.com/feast-dev/feast/go/protos/feast/serving"
	"github.com/stretchr/testify/assert"
)

func TestCountNullOrExpired_NoVectors(t *testing.T) {
	assert.Equal(t, 0, CountNullOrExpired(nil))
}

func TestCountNullOrExpired_CountsNullAndOutsideMaxAgeOnly(t *testing.T) {
	vectors := []*onlineserving.FeatureVector{
		{
			Name: "f1",
			Statuses: []serving.FieldStatus{
				serving.FieldStatus_PRESENT,
				serving.FieldStatus_NULL_VALUE,
				serving.FieldStatus_OUTSIDE_MAX_AGE,
			},
		},
		{
			Name: "f2",
			Statuses: []serving.FieldStatus{
				serving.FieldStatus_PRESENT,
				serving.FieldStatus_NOT_FOUND,
			},
		},
	}

	assert.Equal(t, 2, CountNullOrExpired(vectors))
}

func TestCountNullOrExpiredRange_NoVectors(t *testing.T) {
	assert.Equal(t, 0, CountNullOrExpiredRange(nil))
}

func TestCountNullOrExpiredRange_CountsAcrossNestedRows(t *testing.T) {
	vectors := []*onlineserving.RangeFeatureVector{
		{
			Name: "f1",
			RangeStatuses: [][]serving.FieldStatus{
				{serving.FieldStatus_PRESENT, serving.FieldStatus_NULL_VALUE},
				{serving.FieldStatus_OUTSIDE_MAX_AGE},
			},
		},
	}

	assert.Equal(t, 2, CountNullOrExpiredRange(vectors))
}
