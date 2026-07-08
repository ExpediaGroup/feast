package debuglogging

import (
	"testing"

	"github.com/feast-dev/feast/go/internal/feast/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
)

func TestClassifyError_NilErrorReturnsNil(t *testing.T) {
	assert.Nil(t, ClassifyError(nil))
}

func TestClassifyError_GrpcStatusErrorReturnsCodeString(t *testing.T) {
	err := errors.GrpcNotFoundErrorf("feature view not found")

	result := ClassifyError(err)

	assert.NotNil(t, result)
	assert.Equal(t, codes.NotFound.String(), *result)
}

func TestClassifyError_PlainErrorClassifiedAsInternal(t *testing.T) {
	err := assert.AnError

	result := ClassifyError(err)

	assert.NotNil(t, result)
	assert.Equal(t, codes.Internal.String(), *result)
}
