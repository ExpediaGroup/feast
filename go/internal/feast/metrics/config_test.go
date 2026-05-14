package metrics

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetStatsDAddress(t *testing.T) {
	tests := []struct {
		name     string
		host     string
		port     string
		expected string
	}{
		{
			name:     "no env vars set",
			host:     "",
			port:     "",
			expected: "",
		},
		{
			name:     "only host set, default port",
			host:     "datadog-agent",
			port:     "",
			expected: "datadog-agent:8125",
		},
		{
			name:     "host and custom port",
			host:     "datadog-agent",
			port:     "9125",
			expected: "datadog-agent:9125",
		},
		{
			name:     "localhost with default port",
			host:     "localhost",
			port:     "",
			expected: "localhost:8125",
		},
		{
			name:     "IP address with custom port",
			host:     "10.0.0.5",
			port:     "8126",
			expected: "10.0.0.5:8126",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set env vars
			if tt.host != "" {
				os.Setenv("DD_AGENT_HOST", tt.host)
				defer os.Unsetenv("DD_AGENT_HOST")
			} else {
				os.Unsetenv("DD_AGENT_HOST")
			}

			if tt.port != "" {
				os.Setenv("DD_DOGSTATSD_PORT", tt.port)
				defer os.Unsetenv("DD_DOGSTATSD_PORT")
			} else {
				os.Unsetenv("DD_DOGSTATSD_PORT")
			}

			result := GetStatsDAddress()
			assert.Equal(t, tt.expected, result)
		})
	}
}
