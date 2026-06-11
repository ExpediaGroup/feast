package metrics

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/feast-dev/feast/go/internal/feast/registry"
)

const DefaultSampleRate = 0.01

func IsMissingKeyMetricsEnabled() bool {
	return strings.ToLower(os.Getenv("ENABLE_MISSING_KEY_METRICS")) == "true"
}

func IsFVMetricsEnabled() bool {
	return strings.ToLower(os.Getenv("ENABLE_FV_LEVEL_METRICS")) == "true" ||
		IsMissingKeyMetricsEnabled()
}

func GetOnlineStoreType(config *registry.RepoConfig) string {
	if storeType, ok := config.OnlineStore["type"]; ok {
		return fmt.Sprintf("%v", storeType)
	}
	return "unknown"
}

// ParseSampleRate reads FEAST_METRICS_SAMPLE_RATE from the environment once.
// Returns DefaultSampleRate (0.01) if unset or invalid.
func ParseSampleRate() float64 {
	rateStr := os.Getenv("FEAST_METRICS_SAMPLE_RATE")
	if rateStr == "" {
		return DefaultSampleRate
	}
	rate, err := strconv.ParseFloat(rateStr, 64)
	if err != nil || rate <= 0 || rate > 1.0 {
		return DefaultSampleRate
	}
	return rate
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
