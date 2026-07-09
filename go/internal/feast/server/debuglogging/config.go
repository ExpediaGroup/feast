package debuglogging

import (
	"os"
	"strconv"

	"github.com/rs/zerolog/log"
)

const (
	debugLoggingEnabledEnvVar = "DEBUG_LOGGING"
	sampleRateEnvVar          = "FEAST_LOG_SAMPLE_RATE"
	entityKeySaltEnvVar       = "FEAST_LOG_ENTITY_KEY_SALT"

	// DefaultSampleRate is used when FEAST_LOG_SAMPLE_RATE is unset or invalid.
	DefaultSampleRate = 0.0
)

// Config holds server-wide settings for request debug logging (Plan B: config + sampling).
// The per-request predicate (Plan A) is handled separately in predicate.go.
type Config struct {
	Enabled    bool
	SampleRate float64
	Salt       string
}

// NewConfig reads Config from environment variables. Call once at server startup.
func NewConfig() Config {
	enabled, err := strconv.ParseBool(os.Getenv(debugLoggingEnabledEnvVar))
	if err != nil {
		enabled = false
	}
	salt := os.Getenv(entityKeySaltEnvVar)
	if salt == "" {
		log.Warn().Msgf(
			"%s is not set: entity-key hashes used in debug logging are sha256(value) with no salt, "+
				"which is reversible/brute-forceable for low-cardinality IDs (e.g. small integer entity IDs). "+
				"Set %s to a secret value in production to prevent entity IDs from being recovered from logs.",
			entityKeySaltEnvVar, entityKeySaltEnvVar,
		)
	}

	return Config{
		Enabled:    enabled,
		SampleRate: parseSampleRate(os.Getenv(sampleRateEnvVar)),
		Salt:       salt,
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
