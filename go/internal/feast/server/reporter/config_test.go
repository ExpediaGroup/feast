//go:build !integration

package reporter

import (
	"bytes"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

// captureLogs redirects the global logger for the duration of a test.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	previous := log.Logger
	log.Logger = zerolog.New(buf)
	t.Cleanup(func() { log.Logger = previous })
	return buf
}

func TestConfigFromEnv_Defaults(t *testing.T) {
	cfg := ConfigFromEnv(envOf(nil), "1.2.3")
	assert.False(t, cfg.Enabled)
	assert.Empty(t, cfg.Urls)
	assert.Equal(t, map[string]string{}, cfg.Labels)
	assert.Equal(t, "1.2.3", cfg.Version)
	assert.Equal(t, 24*time.Hour, cfg.Interval)
}

func TestConfigFromEnv_Enabled(t *testing.T) {
	cases := map[string]bool{
		"on": true, "ON": true, " on ": true, "true": true,
		"off": false, "OFF": false, "": false, "false": false,
		"yes": false, "1": false, // unknown values keep reporting off
	}
	for raw, want := range cases {
		t.Run(raw, func(t *testing.T) {
			assert.Equal(t, want, ConfigFromEnv(envOf(map[string]string{EnvReporting: raw}), "v").Enabled)
		})
	}
}

func TestConfigFromEnv_UnknownEnabledValueWarns(t *testing.T) {
	logs := captureLogs(t)
	ConfigFromEnv(envOf(map[string]string{EnvReporting: "yes"}), "v")
	assert.Contains(t, logs.String(), `"level":"warn"`)
	assert.Contains(t, logs.String(), EnvReporting)
}

func TestConfigFromEnv_AdvertisedUrls(t *testing.T) {
	raw := " external=grpcs://app.rcp.us-east-1.example.net:443 , nearest=grpcs://app.rcp.example.net:443," +
		"https://app.example.net,internal=grpc://app.ns.svc.cluster.local:6566,," +
		"http://app.example.net/x?a=b,scoped=https://app.example.net/y?c=d"
	cfg := ConfigFromEnv(envOf(map[string]string{EnvAdvertisedUrls: raw}), "v")
	assert.Equal(t, []AdvertisedUrl{
		{Url: "grpcs://app.rcp.us-east-1.example.net:443", Scope: "external"},
		{Url: "grpcs://app.rcp.example.net:443", Scope: "nearest"},
		{Url: "https://app.example.net", Scope: ""},
		{Url: "grpc://app.ns.svc.cluster.local:6566", Scope: "internal"},
		{Url: "http://app.example.net/x?a=b", Scope: ""},
		{Url: "https://app.example.net/y?c=d", Scope: "scoped"},
	}, cfg.Urls)
}

func TestConfigFromEnv_BadUrlsIgnoredWithWarning(t *testing.T) {
	bad := []string{
		"external=",                           // no URL
		"ftp://app.example.net",               // scheme not allowed
		"app.example.net:443",                 // not absolute
		"https://",                            // no host
		"https://user:secret@app.example.net", // credentials
		"external=not a url",
	}
	for _, entry := range bad {
		t.Run(entry, func(t *testing.T) {
			logs := captureLogs(t)
			cfg := ConfigFromEnv(envOf(map[string]string{
				EnvAdvertisedUrls: entry + ",internal=grpc://ok.ns.svc.cluster.local:6566",
			}), "v")
			assert.Equal(t, []AdvertisedUrl{{Url: "grpc://ok.ns.svc.cluster.local:6566", Scope: "internal"}}, cfg.Urls)
			assert.Contains(t, logs.String(), `"level":"warn"`)
			assert.Contains(t, logs.String(), EnvAdvertisedUrls)
		})
	}
}

func TestConfigFromEnv_Labels(t *testing.T) {
	logs := captureLogs(t)
	cfg := ConfigFromEnv(envOf(map[string]string{
		EnvLabels: " app = ads-ml , instance=mlpfs,novalue,=orphan,,empty=,exposure=confirmed,app=override",
	}), "v")
	assert.Equal(t, map[string]string{
		"app":      "override", // last one wins
		"instance": "mlpfs",
		"empty":    "",
		"exposure": "confirmed",
	}, cfg.Labels)
	assert.Contains(t, logs.String(), `novalue`)
	assert.Contains(t, logs.String(), `=orphan`)
}

func TestConfigFromEnv_Version(t *testing.T) {
	assert.Equal(t, "mlpfs-1a2b", ConfigFromEnv(envOf(map[string]string{EnvVersion: " mlpfs-1a2b "}), "build").Version)
	assert.Equal(t, "build", ConfigFromEnv(envOf(map[string]string{EnvVersion: "  "}), "build").Version)
}

func TestConfigFromEnv_Interval(t *testing.T) {
	cases := map[string]time.Duration{
		"":      24 * time.Hour,
		"12h":   12 * time.Hour,
		"90m":   90 * time.Minute,
		"daily": 24 * time.Hour, // not a duration: default
		"-1h":   time.Minute,    // below the minimum
		"5s":    time.Minute,
	}
	for raw, want := range cases {
		t.Run(raw, func(t *testing.T) {
			assert.Equal(t, want, ConfigFromEnv(envOf(map[string]string{EnvInterval: raw}), "v").Interval)
		})
	}
}
