package debuglogging

import (
	"github.com/feast-dev/feast/go/internal/feast/onlineserving"
	"github.com/feast-dev/feast/go/protos/feast/serving"
)

// CountNullOrExpired returns the total number of NULL_VALUE or
// OUTSIDE_MAX_AGE statuses across all returned feature vectors, for the
// null_field_count debug-log field.
func CountNullOrExpired(vectors []*onlineserving.FeatureVector) int {
	count := 0
	for _, vector := range vectors {
		for _, status := range vector.Statuses {
			if isNullOrExpired(status) {
				count++
			}
		}
	}
	return count
}

// CountNullOrExpiredRange is the RangeFeatureVector equivalent of
// CountNullOrExpired, summing across every row's status list.
func CountNullOrExpiredRange(vectors []*onlineserving.RangeFeatureVector) int {
	count := 0
	for _, vector := range vectors {
		for _, statuses := range vector.RangeStatuses {
			for _, status := range statuses {
				if isNullOrExpired(status) {
					count++
				}
			}
		}
	}
	return count
}

func isNullOrExpired(status serving.FieldStatus) bool {
	return status == serving.FieldStatus_NULL_VALUE || status == serving.FieldStatus_OUTSIDE_MAX_AGE
}
