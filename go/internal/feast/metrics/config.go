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
