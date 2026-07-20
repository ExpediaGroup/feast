package debuglogging

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

func TestRequestFlaggedHTTP(t *testing.T) {
	tests := []struct {
		name     string
		headerV  string
		hasKey   bool
		expected bool
	}{
		{name: "true lowercase", headerV: "true", hasKey: true, expected: true},
		{name: "True mixed case", headerV: "True", hasKey: true, expected: true},
		{name: "TRUE uppercase", headerV: "TRUE", hasKey: true, expected: true},
		{name: "false", headerV: "false", hasKey: true, expected: false},
		{name: "absent", headerV: "", hasKey: false, expected: false},
		{name: "malformed", headerV: "yes", hasKey: true, expected: false},
		{name: "whitespace padded true", headerV: " true ", hasKey: true, expected: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			header := http.Header{}
			if tc.hasKey {
				header.Set("X-Debug-Logging", tc.headerV)
			}
			assert.Equal(t, tc.expected, RequestFlaggedHTTP(header))
		})
	}
}

func TestRequestFlaggedGRPC(t *testing.T) {
	tests := []struct {
		name     string
		values   []string
		expected bool
	}{
		{name: "true", values: []string{"true"}, expected: true},
		{name: "True mixed case", values: []string{"True"}, expected: true},
		{name: "false", values: []string{"false"}, expected: false},
		{name: "absent", values: nil, expected: false},
		{name: "malformed", values: []string{"yes"}, expected: false},
		{name: "multiple values uses first", values: []string{"true", "false"}, expected: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			md := metadata.MD{}
			if tc.values != nil {
				md.Set("debug-logging", tc.values...)
			}
			ctx := metadata.NewIncomingContext(context.Background(), md)
			assert.Equal(t, tc.expected, RequestFlaggedGRPC(ctx))
		})
	}
}

func TestRequestFlaggedGRPC_NoIncomingMetadata(t *testing.T) {
	assert.False(t, RequestFlaggedGRPC(context.Background()))
}
