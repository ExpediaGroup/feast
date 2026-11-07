//go:build !integration

package reporter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
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
		"https://app.example.net:99999",       // port out of range
		"https://app.example.net:0",           // port out of range
		"https://app.example.net:",            // empty port
		"https://app.exa\u0007mple.net",       // control character
		"https://app.example.net/" + strings.Repeat("a", MaxUrlLength), // too long
		strings.Repeat("s", MaxScopeLength+1) + "=https://app.example.net", // scope too long
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


func TestConfigFromEnv_DuplicateUrlDropped(t *testing.T) {
	logs := captureLogs(t)
	cfg := ConfigFromEnv(envOf(map[string]string{
		EnvAdvertisedUrls: "external=https://a.example.net,nearest=https://a.example.net",
	}), "v")
	assert.Equal(t, []AdvertisedUrl{{Url: "https://a.example.net", Scope: "external"}}, cfg.Urls)
	assert.Contains(t, logs.String(), "duplicate URL")
}

func TestConfigFromEnv_AtMostMaxUrlsKeepingTheFirst(t *testing.T) {
	entries := make([]string, 0, MaxUrls+3)
	for i := 0; i < MaxUrls+3; i++ {
		entries = append(entries, fmt.Sprintf("https://h%d.example.net", i))
	}
	logs := captureLogs(t)
	cfg := ConfigFromEnv(envOf(map[string]string{EnvAdvertisedUrls: strings.Join(entries, ",")}), "v")
	assert.Len(t, cfg.Urls, MaxUrls)
	assert.Equal(t, "https://h0.example.net", cfg.Urls[0].Url)
	assert.Equal(t, fmt.Sprintf("https://h%d.example.net", MaxUrls-1), cfg.Urls[MaxUrls-1].Url)
	assert.Contains(t, logs.String(), "at most")
}

func TestConfigFromEnv_ReportKeptUnderSizeLimit(t *testing.T) {
	// Ten URLs each just under the length limit add up to more than 16 KB.
	entries := make([]string, 0, MaxUrls)
	for i := 0; i < MaxUrls; i++ {
		base := fmt.Sprintf("https://h%d.example.net/", i)
		entries = append(entries, base+strings.Repeat("a", MaxUrlLength-len(base)))
	}
	logs := captureLogs(t)
	cfg := ConfigFromEnv(envOf(map[string]string{EnvAdvertisedUrls: strings.Join(entries, ",")}), "v")
	assert.Less(t, len(cfg.Urls), MaxUrls)
	assert.NotEmpty(t, cfg.Urls)
	assert.Equal(t, entries[0], cfg.Urls[0].Url, "the first URLs are kept")
	assert.LessOrEqual(t, reportSize(cfg, cfg.Urls), MaxReportBytes)
	assert.Contains(t, logs.String(), "under")

	// The registry measures the JSON it receives; check the real encoding too.
	type u struct {
		Url   string `json:"url"`
		Scope string `json:"scope"`
	}
	var urls []u
	for _, x := range cfg.Urls {
		urls = append(urls, u{x.Url, x.Scope})
	}
	encoded, _ := json.Marshal(map[string]any{"contract": 1, "urls": urls, "version": cfg.Version, "registry": "grpc", "labels": cfg.Labels})
	assert.LessOrEqual(t, len(encoded), MaxReportBytes)
}

func TestConfigFromEnv_BadLabelsDropped(t *testing.T) {
	logs := captureLogs(t)
	cfg := ConfigFromEnv(envOf(map[string]string{
		EnvLabels: strings.Join([]string{
			"app=ok",
			strings.Repeat("k", MaxLabelKeyLength+1) + "=v",
			"long=" + strings.Repeat("v", MaxLabelValueLength+1),
			"ctl=a\u0001b",
		}, ","),
	}), "v")
	assert.Equal(t, map[string]string{"app": "ok"}, cfg.Labels)
	assert.Equal(t, 3, strings.Count(logs.String(), `"level":"warn"`))
}

func TestConfigFromEnv_AtMostMaxLabels(t *testing.T) {
	entries := make([]string, 0, MaxLabels+5)
	for i := 0; i < MaxLabels+5; i++ {
		entries = append(entries, fmt.Sprintf("k%02d=v", i))
	}
	captureLogs(t)
	cfg := ConfigFromEnv(envOf(map[string]string{EnvLabels: strings.Join(entries, ",")}), "v")
	assert.Len(t, cfg.Labels, MaxLabels)
	assert.Contains(t, cfg.Labels, "k00")
	assert.NotContains(t, cfg.Labels, fmt.Sprintf("k%02d", MaxLabels), "kept deterministically: the first keys in order")
}

func TestConfigFromEnv_BadVersionFallsBackToBuildVersion(t *testing.T) {
	for _, bad := range []string{strings.Repeat("v", MaxVersionLength+1), "v1\nv2"} {
		captureLogs(t)
		cfg := ConfigFromEnv(envOf(map[string]string{EnvVersion: bad}), "build-1")
		assert.Equal(t, "build-1", cfg.Version)
	}
}

func TestConfigFromEnv_ValidPortsKept(t *testing.T) {
	cfg := ConfigFromEnv(envOf(map[string]string{
		EnvAdvertisedUrls: "a=grpcs://h.example.net:1,b=grpcs://h.example.net:65535,c=https://h.example.net",
	}), "v")
	assert.Len(t, cfg.Urls, 3)
}
