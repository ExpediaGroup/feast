// These contain configs/methods that are used by the hybrid server function.
package server

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

var defaultCheckTimeout = 10 * time.Second

// Register default HTTP handlers specific to the hybrid server configuration.
func DefaultHybridHandlers(s *httpServer, hs *health.Server) []Handler {
	return []Handler{
		{
			path:        "/get-online-features",
			handlerFunc: recoverMiddleware(http.HandlerFunc(s.getOnlineFeatures)),
		},
		{
			path:        "/metrics",
			handlerFunc: promhttp.Handler(),
		},
		{
			path:        "/health",
			handlerFunc: http.HandlerFunc(combinedHealthCheck(hs)),
		},
	}
}

// Closure that injects health.Server during registration of http.Handler
func combinedHealthCheck(hs *health.Server) http.HandlerFunc {
	// This function should be registered within the httpHandler.
	// Calls the grpc.server healthcheck check endpoint. Saves us a call to gRPC dial.

	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(d, defaultCheckTimeout)
		defer cancel()

		req := &healthpb.HealthCheckRequest{
			Service: "", // Empty string means that it will simply check overall servingStatus
		}

		resp, err := hs.Check(ctx, req)
		if err != nil {
			http.Error(w, "gRPC health check failed", http.StatusInternalServerError)
			return
		}

		// Use to map servingStatus to httpStatus
		var status int
		switch resp.Status {
		case healthpb.HealthCheckResponse_SERVING:
			status = http.StatusOK
		case healthpb.HealthCheckResponse_NOT_SERVING:
			status = http.StatusServiceUnavailable
		default:
			status = http.StatusInternalServerError
		}

		w.WriteHeader(status)
		w.Write([]byte(resp.Status.String()))
	}
}
