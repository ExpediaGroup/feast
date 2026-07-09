package debuglogging

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func clearDebugLoggingEnv() {
	os.Unsetenv("DEBUG_LOGGING")
	os.Unsetenv("FEAST_LOG_SAMPLE_RATE")
}

func TestNewConfig_Defaults(t *testing.T) {
	clearDebugLoggingEnv()
	defer clearDebugLoggingEnv()

	cfg := NewConfig()

	assert.False(t, cfg.Enabled)
	assert.Equal(t, 0.0, cfg.SampleRate)
}

func TestNewConfig_EnabledTrue(t *testing.T) {
	clearDebugLoggingEnv()
	defer clearDebugLoggingEnv()
	os.Setenv("DEBUG_LOGGING", "true")

	assert.True(t, NewConfig().Enabled)
}

func TestNewConfig_EnabledMalformedFallsBackToFalse(t *testing.T) {
	clearDebugLoggingEnv()
	defer clearDebugLoggingEnv()
	os.Setenv("DEBUG_LOGGING", "not-a-bool")

	assert.False(t, NewConfig().Enabled)
}

func TestNewConfig_SampleRateValid(t *testing.T) {
	clearDebugLoggingEnv()
	defer clearDebugLoggingEnv()
	os.Setenv("FEAST_LOG_SAMPLE_RATE", "0.25")

	assert.Equal(t, 0.25, NewConfig().SampleRate)
}

func TestNewConfig_SampleRateOutOfRangeFallsBackToDefault(t *testing.T) {
	clearDebugLoggingEnv()
	defer clearDebugLoggingEnv()
	os.Setenv("FEAST_LOG_SAMPLE_RATE", "1.5")

	assert.Equal(t, DefaultSampleRate, NewConfig().SampleRate)
}
