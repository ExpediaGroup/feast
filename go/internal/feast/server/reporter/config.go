// Package reporter sends "feature server reports" to the Feast registry: the
// URLs a caller can use to reach this server, its version and free-form
// labels. A report goes once the server is ready and then once per interval,
// over the registry connection the server already has (REST for an HTTP
// registry, the ReportFeatureServer RPC for a gRPC registry). Reporting runs in
// the background and never affects serving.
package reporter

import (
	"encoding/json"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/rs/zerolog/log"
)

// Environment variables read by ConfigFromEnv.
const (
	EnvReporting      = "FEATURE_SERVER_REPORTING"       // on|off, default off
	EnvAdvertisedUrls = "FEATURE_SERVER_ADVERTISED_URLS" // comma-separated scope=url (scope optional)
	EnvLabels         = "FEATURE_SERVER_REPORT_LABELS"   // comma-separated k=v
	EnvVersion        = "FEATURE_SERVER_REPORT_VERSION"  // default: build version
	EnvInterval       = "FEATURE_SERVER_REPORT_INTERVAL" // Go duration, default 24h
)

const (
	DefaultInterval = 24 * time.Hour
	// MinInterval guards against a typo such as "24s" turning every pod into
	// a tight loop against the registry.
	MinInterval = time.Minute
	// ReportContract is the report format version this server sends.
	ReportContract = 1
)

// The registry's limits (feast/infra/registry/feature_server_reports.py).
// The registry rejects a whole report that breaks any of them, and a rejection
// is never retried, so the reporter drops the offending entry here instead.
const (
	MaxUrls             = 10
	MaxUrlLength        = 2048
	MaxScopeLength      = 32
	MaxLabels           = 20
	MaxLabelKeyLength   = 63
	MaxLabelValueLength = 256
	MaxVersionLength    = 128
	MaxReportBytes      = 16 * 1024
	// maxRegistryFieldLength leaves room for the "registry" field, which is
	// only known once the transport is chosen ("http" or "grpc").
	maxRegistryFieldLength = 32
)

var allowedUrlSchemes = map[string]bool{"http": true, "https": true, "grpc": true, "grpcs": true}

// AdvertisedUrl is one URL a caller can use to reach this server.
type AdvertisedUrl struct {
	Url   string
	Scope string
}

// Config is the reporter configuration taken from the environment.
type Config struct {
	Enabled  bool
	Urls     []AdvertisedUrl
	Labels   map[string]string
	Version  string
	Interval time.Duration
}

// ConfigFromEnv reads the reporter settings. getenv is os.Getenv in
// production; defaultVersion is the build version. Malformed entries are
// dropped with a warning; they never fail startup.
func ConfigFromEnv(getenv func(string) string, defaultVersion string) Config {
	cfg := Config{
		Enabled:  parseEnabled(getenv(EnvReporting)),
		Urls:     parseAdvertisedUrls(getenv(EnvAdvertisedUrls)),
		Labels:   parseLabels(getenv(EnvLabels)),
		Version:  strings.TrimSpace(getenv(EnvVersion)),
		Interval: parseInterval(getenv(EnvInterval)),
	}
	if reason := invalidVersionReason(cfg.Version); reason != "" {
		log.Warn().Msgf("%s: ignoring %q: %s; using the build version", EnvVersion, cfg.Version, reason)
		cfg.Version = ""
	}
	if cfg.Version == "" {
		cfg.Version = defaultVersion
		if invalidVersionReason(cfg.Version) != "" {
			cfg.Version = ""
		}
	}
	cfg.Urls = fitReportSize(cfg)
	return cfg
}

func hasControlCharacter(value string) bool {
	return strings.IndexFunc(value, unicode.IsControl) >= 0
}

func invalidVersionReason(version string) string {
	if len([]rune(version)) > MaxVersionLength {
		return "longer than " + strconv.Itoa(MaxVersionLength) + " characters"
	}
	if hasControlCharacter(version) {
		return "contains a control character"
	}
	return ""
}

// reportSize is the size of the report as JSON, the way the registry measures
// it. Go escapes <, > and & where the registry doesn't, so this never
// underestimates.
func reportSize(cfg Config, urls []AdvertisedUrl) int {
	type jsonUrl struct {
		Url   string `json:"url"`
		Scope string `json:"scope"`
	}
	body := struct {
		Contract int               `json:"contract"`
		Urls     []jsonUrl         `json:"urls"`
		Version  string            `json:"version"`
		Registry string            `json:"registry"`
		Labels   map[string]string `json:"labels"`
	}{Contract: ReportContract, Version: cfg.Version, Registry: strings.Repeat("x", maxRegistryFieldLength), Labels: cfg.Labels}
	for _, u := range urls {
		body.Urls = append(body.Urls, jsonUrl{Url: u.Url, Scope: u.Scope})
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return MaxReportBytes + 1
	}
	return len(encoded)
}

// fitReportSize drops URLs from the end until the report fits the registry's
// size limit; the first URLs are the most useful (external before internal).
func fitReportSize(cfg Config) []AdvertisedUrl {
	urls := cfg.Urls
	for len(urls) > 0 && reportSize(cfg, urls) > MaxReportBytes {
		dropped := urls[len(urls)-1]
		log.Warn().Msgf("%s: dropping %q to keep the report under %d bytes", EnvAdvertisedUrls, dropped.Url, MaxReportBytes)
		urls = urls[:len(urls)-1]
	}
	return urls
}

func parseEnabled(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "on", "true":
		return true
	case "", "off", "false":
		return false
	default:
		log.Warn().Msgf("%s=%q is not on or off; feature server reporting stays off", EnvReporting, raw)
		return false
	}
}

// parseAdvertisedUrls parses "scope=url,scope=url,url". An entry is split at
// its first "=" only when that "=" comes before the URL's "://", so a URL
// whose query contains "=" and has no scope is kept whole.
func parseAdvertisedUrls(raw string) []AdvertisedUrl {
	var out []AdvertisedUrl
	seen := map[string]bool{}
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		scope, rawUrl := "", entry
		eq := strings.Index(entry, "=")
		schemeSep := strings.Index(entry, "://")
		if eq >= 0 && (schemeSep < 0 || eq < schemeSep) {
			scope = strings.TrimSpace(entry[:eq])
			rawUrl = strings.TrimSpace(entry[eq+1:])
		}
		reason := invalidUrlReason(rawUrl)
		switch {
		case reason != "":
		case len([]rune(scope)) > MaxScopeLength:
			reason = "scope is longer than " + strconv.Itoa(MaxScopeLength) + " characters"
		case hasControlCharacter(scope):
			reason = "scope contains a control character"
		case seen[rawUrl]:
			reason = "duplicate URL"
		case len(out) == MaxUrls:
			reason = "a report carries at most " + strconv.Itoa(MaxUrls) + " URLs"
		}
		if reason != "" {
			log.Warn().Msgf("%s: ignoring entry %q: %s", EnvAdvertisedUrls, entry, reason)
			continue
		}
		seen[rawUrl] = true
		out = append(out, AdvertisedUrl{Url: rawUrl, Scope: scope})
	}
	return out
}

// invalidUrlReason applies the registry's URL check, so one bad entry is
// dropped here instead of getting the whole report rejected.
func invalidUrlReason(rawUrl string) string {
	if rawUrl == "" {
		return "empty URL"
	}
	if len([]rune(rawUrl)) > MaxUrlLength {
		return "longer than " + strconv.Itoa(MaxUrlLength) + " characters"
	}
	if strings.IndexFunc(rawUrl, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return "contains whitespace or a control character"
	}
	u, err := url.Parse(rawUrl)
	if err != nil {
		return "not a URL: " + err.Error()
	}
	if !allowedUrlSchemes[strings.ToLower(u.Scheme)] {
		return "scheme must be http, https, grpc or grpcs"
	}
	if u.Hostname() == "" {
		return "no host"
	}
	if u.User != nil {
		return "must not contain a user name or password"
	}
	if strings.HasSuffix(u.Host, ":") {
		return "empty port"
	}
	if p := u.Port(); p != "" {
		if n, err := strconv.Atoi(p); err != nil || n < 1 || n > 65535 {
			return "port must be 1-65535"
		}
	}
	return ""
}

func parseLabels(raw string) map[string]string {
	labels := map[string]string{}
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		key, value, found := strings.Cut(entry, "=")
		key = strings.TrimSpace(key)
		if !found || key == "" {
			log.Warn().Msgf("%s: ignoring entry %q: expected key=value", EnvLabels, entry)
			continue
		}
		value = strings.TrimSpace(value)
		reason := ""
		switch {
		case len([]rune(key)) > MaxLabelKeyLength:
			reason = "key is longer than " + strconv.Itoa(MaxLabelKeyLength) + " characters"
		case len([]rune(value)) > MaxLabelValueLength:
			reason = "value is longer than " + strconv.Itoa(MaxLabelValueLength) + " characters"
		case hasControlCharacter(key) || hasControlCharacter(value):
			reason = "contains a control character"
		}
		if reason != "" {
			log.Warn().Msgf("%s: ignoring entry %q: %s", EnvLabels, entry, reason)
			continue
		}
		labels[key] = value
	}
	if len(labels) > MaxLabels {
		keys := make([]string, 0, len(labels))
		for k := range labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys[MaxLabels:] {
			log.Warn().Msgf("%s: ignoring label %q: a report carries at most %d labels", EnvLabels, k, MaxLabels)
			delete(labels, k)
		}
	}
	return labels
}

func parseInterval(raw string) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return DefaultInterval
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		log.Warn().Msgf("%s=%q is not a Go duration; using %s", EnvInterval, raw, DefaultInterval)
		return DefaultInterval
	}
	if d < MinInterval {
		log.Warn().Msgf("%s=%q is below the minimum %s; using %s", EnvInterval, raw, MinInterval, MinInterval)
		return MinInterval
	}
	return d
}
