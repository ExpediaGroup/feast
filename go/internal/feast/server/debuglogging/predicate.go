package debuglogging

import (
	"context"
	"net/http"
	"strings"

	"google.golang.org/grpc/metadata"
)

const (
	// HTTPHeaderName is the HTTP header a caller sets to request debug logging
	// for a single request (Plan A). Same header name used for PILS/UISE
	// Controller under EAPC-21404, for cross-service consistency.
	HTTPHeaderName = "X-Debug-Logging"

	// GRPCMetadataKey is the gRPC metadata equivalent of HTTPHeaderName.
	// gRPC metadata keys are conventionally lowercase.
	GRPCMetadataKey = "debug-logging"
)

// RequestFlaggedHTTP returns true if the caller explicitly requested debug
// logging for this HTTP request. Absent, malformed, or non-"true" values are
// treated as false (fail safe).
func RequestFlaggedHTTP(header http.Header) bool {
	return isTrue(header.Get(HTTPHeaderName))
}

// RequestFlaggedGRPC returns true if the caller explicitly requested debug
// logging for this gRPC request via incoming metadata.
func RequestFlaggedGRPC(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	values := md.Get(GRPCMetadataKey)
	if len(values) == 0 {
		return false
	}
	return isTrue(values[0])
}

func isTrue(raw string) bool {
	return strings.EqualFold(strings.TrimSpace(raw), "true")
}
