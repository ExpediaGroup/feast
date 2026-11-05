// Package reporter sends "feature server reports" to the Feast registry: the
// URLs a caller can use to reach this server, its version and free-form
// labels. A report goes once the server is ready and then once per interval,
// over the registry connection the server already has (REST for an HTTP
// registry, the ReportFeatureServer RPC for a gRPC registry). Reporting runs in
// the background and never affects serving.
package reporter

import (
	"net/url"
	"strings"
	"time"

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
	if cfg.Version == "" {
		cfg.Version = defaultVersion
	}
	return cfg
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
		if reason := invalidUrlReason(rawUrl); reason != "" {
			log.Warn().Msgf("%s: ignoring entry %q: %s", EnvAdvertisedUrls, entry, reason)
			continue
		}
		out = append(out, AdvertisedUrl{Url: rawUrl, Scope: scope})
	}
	return out
}

// invalidUrlReason mirrors the registry's URL check so one bad entry is
// dropped here instead of getting the whole report rejected.
func invalidUrlReason(rawUrl string) string {
	if rawUrl == "" {
		return "empty URL"
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
		labels[key] = strings.TrimSpace(value)
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
