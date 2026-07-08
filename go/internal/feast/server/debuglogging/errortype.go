package debuglogging

import (
	"github.com/feast-dev/feast/go/internal/feast/errors"
	"google.golang.org/grpc/status"
)

// ClassifyError returns a short error-type string for the error_type
// debug-log field, or nil if err is nil. Reuses the existing
// errors.GrpcFromError classification (grpc status code, or Internal for
// unclassified errors) rather than inventing a new taxonomy.
func ClassifyError(err error) *string {
	if err == nil {
		return nil
	}
	grpcErr := errors.GrpcFromError(err)
	code := status.Code(grpcErr).String()
	return &code
}
