package debuglogging

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

func clearDebugLoggingEnv() {
	os.Unsetenv("DEBUG_LOGGING")
	os.Unsetenv("FEAST_LOG_SAMPLE_RATE")
	os.Unsetenv("FEAST_LOG_ENTITY_KEY_SALT")
}

func TestNewConfig_Defaults(t *testing.T) {
	clearDebugLoggingEnv()
	defer clearDebugLoggingEnv()

	cfg := NewConfig()

	assert.False(t, cfg.Enabled)
	assert.Equal(t, 0.0, cfg.SampleRate)
	assert.Equal(t, "", cfg.Salt)
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

func TestNewConfig_Salt(t *testing.T) {
	clearDebugLoggingEnv()
	defer clearDebugLoggingEnv()
	os.Setenv("FEAST_LOG_ENTITY_KEY_SALT", "s3cr3t")

	assert.Equal(t, "s3cr3t", NewConfig().Salt)
}

// captureLogOutput temporarily swaps the global zerolog logger for one that
// writes to an in-memory buffer, invokes fn, restores the original logger,
// and returns everything written during fn.
func captureLogOutput(fn func()) string {
	originalLogger := log.Logger
	var buf bytes.Buffer
	log.Logger = zerolog.New(&buf)
	defer func() { log.Logger = originalLogger }()

	fn()

	return buf.String()
}

func TestNewConfig_MissingSaltLogsWarning(t *testing.T) {
	clearDebugLoggingEnv()
	defer clearDebugLoggingEnv()

	output := captureLogOutput(func() {
		NewConfig()
	})

	assert.Contains(t, output, "FEAST_LOG_ENTITY_KEY_SALT")
	assert.True(t, strings.Contains(output, "\"level\":\"warn\""), "expected a warn-level log line, got: %s", output)
}

func TestNewConfig_SetSaltDoesNotLogWarning(t *testing.T) {
	clearDebugLoggingEnv()
	defer clearDebugLoggingEnv()
	os.Setenv("FEAST_LOG_ENTITY_KEY_SALT", "s3cr3t")

	output := captureLogOutput(func() {
		NewConfig()
	})

	assert.Equal(t, "", output, "expected no log output when salt is set, got: %s", output)
}
