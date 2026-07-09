package debuglogging

import (
	"os"
	"strconv"
)

const (
	debugLoggingEnabledEnvVar = "DEBUG_LOGGING"
	sampleRateEnvVar          = "FEAST_LOG_SAMPLE_RATE"

	// DefaultSampleRate is used when FEAST_LOG_SAMPLE_RATE is unset or invalid.
	DefaultSampleRate = 0.0
)

// Config holds server-wide settings for request debug logging (Plan B: config + sampling).
// The per-request predicate (Plan A) is handled separately in predicate.go.
type Config struct {
	Enabled    bool
	SampleRate float64
}

// NewConfig reads Config from environment variables. Call once at server startup.
func NewConfig() Config {
	enabled, err := strconv.ParseBool(os.Getenv(debugLoggingEnabledEnvVar))
	if err != nil {
		enabled = false
	}

	return Config{
		Enabled:    enabled,
		SampleRate: parseSampleRate(os.Getenv(sampleRateEnvVar)),
	}
}

func parseSampleRate(raw string) float64 {
	if raw == "" {
		return DefaultSampleRate
	}
	rate, err := strconv.ParseFloat(raw, 64)
	if err != nil || rate < 0 || rate > 1.0 {
		return DefaultSampleRate
	}
	return rate
}
