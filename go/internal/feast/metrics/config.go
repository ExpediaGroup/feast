package metrics

import (
	"fmt"
	"os"
	"strings"

	"github.com/feast-dev/feast/go/internal/feast/registry"
)

func IsMissingKeyMetricsEnabled() bool {
	return strings.ToLower(os.Getenv("ENABLE_MISSING_KEY_METRICS")) == "true"
}

func GetOnlineStoreType(config *registry.RepoConfig) string {
	if storeType, ok := config.OnlineStore["type"]; ok {
		return fmt.Sprintf("%v", storeType)
	}
	return "unknown"
}

func GetServiceName() string {
	if svc := os.Getenv("SERVICE_NAME"); svc != "" {
		return svc
	}
	if app := os.Getenv("APPLICATION"); app != "" {
		return app
	}
	return "unknown_service"
}

func GetEnvironment() string {
	if env := os.Getenv("EXPEDIA_ENVIRONMENT_CATEGORY"); env != "" {
		return env
	}
	if env := os.Getenv("EXPEDIA_ENVIRONMENT"); env != "" {
		return env
	}
	return "unknown_env"
}

// GetStatsDAddress returns the DogStatsD address from environment variables.
// Returns empty string if DD_AGENT_HOST is not set.
// Port can be configured via DD_DOGSTATSD_PORT (defaults to 8125).
func GetStatsDAddress() string {
	host := os.Getenv("DD_AGENT_HOST")
	if host == "" {
		return ""
	}

	port := os.Getenv("DD_DOGSTATSD_PORT")
	if port == "" {
		port = "8125"
	}

	return fmt.Sprintf("%s:%s", host, port)
}
