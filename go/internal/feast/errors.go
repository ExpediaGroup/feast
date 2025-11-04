package feast

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FeastTransformationServiceNotConfigured struct{}

func (FeastTransformationServiceNotConfigured) GRPCStatus() *status.Status {
	errorStatus := status.New(codes.Internal, "No transformation service configured")
	ds, err := errorStatus.WithDetails(&errdetails.LocalizedMessage{Message: "No transformation service configured, required for on-demand feature transformations"})
	if err != nil {
		return errorStatus
	}
	return ds
}

func (e FeastTransformationServiceNotConfigured) Error() string {
	return e.GRPCStatus().Err().Error()
}

// NotImplementedError represents an error for a function that is not yet implemented.
type NotImplementedError struct {
	FunctionName string
}

// Error implements the error interface for NotImplementedError.
func (e *NotImplementedError) Error() string {
	return fmt.Sprintf("Function '%s' not implemented", e.FunctionName)
}
