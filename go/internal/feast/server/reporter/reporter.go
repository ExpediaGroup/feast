package reporter

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/feast-dev/feast/go/internal/feast/registry"
	registryPb "github.com/feast-dev/feast/go/protos/feast/registry"
	"github.com/rs/zerolog/log"
)

const (
	// DefaultTimeout bounds each report call.
	DefaultTimeout = 2 * time.Second
	// DefaultRetryDelay is how long after a retryable failure the single retry goes.
	DefaultRetryDelay = time.Hour
)

// Clock is the time source of the scheduler; replaced in tests.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Reporter sends one report at start and then once per interval, at an
// offset into the interval chosen at random per process so pods of one
// deployment don't report together.
type Reporter struct {
	transport registry.FeatureServerReportTransport
	project   string
	report    *registryPb.FeatureServerReport

	Interval   time.Duration
	RetryDelay time.Duration
	Timeout    time.Duration
	Clock      Clock
	// Offset picks the delay from start to the first periodic report, in [0, interval).
	Offset func(interval time.Duration) time.Duration
}

// New builds a reporter for cfg over transport. The defaults (2 s timeout,
// 1 h retry delay, real clock, random offset) can be replaced before Run.
func New(cfg Config, transport registry.FeatureServerReportTransport, project string) *Reporter {
	return &Reporter{
		transport:  transport,
		project:    project,
		report:     buildReport(cfg, transport.FeatureServerReportRegistry()),
		Interval:   cfg.Interval,
		RetryDelay: DefaultRetryDelay,
		Timeout:    DefaultTimeout,
		Clock:      realClock{},
		Offset:     randomOffset,
	}
}

func randomOffset(interval time.Duration) time.Duration {
	if interval <= 0 {
		return 0
	}
	return rand.N(interval)
}

func buildReport(cfg Config, registryType string) *registryPb.FeatureServerReport {
	urls := make([]*registryPb.FeatureServerUrl, 0, len(cfg.Urls))
	for _, u := range cfg.Urls {
		urls = append(urls, &registryPb.FeatureServerUrl{Url: u.Url, Scope: u.Scope})
	}
	labels := make(map[string]string, len(cfg.Labels))
	for k, v := range cfg.Labels {
		labels[k] = v
	}
	return &registryPb.FeatureServerReport{
		Contract: ReportContract,
		Urls:     urls,
		Version:  cfg.Version,
		Registry: registryType,
		Labels:   labels,
	}
}

// Report returns the report this reporter sends.
func (r *Reporter) Report() *registryPb.FeatureServerReport { return r.report }

// Run reports until ctx is done. Call it in its own goroutine.
//
// Schedule: a report now; periodic reports at start+offset, then every
// Interval. A retryable failure of a report (no response, 5xx, UNAVAILABLE,
// DEADLINE_EXCEEDED) gets exactly one retry RetryDelay later, unless the next
// periodic report comes first. Rejections are logged and never retried.
func (r *Reporter) Run(ctx context.Context) {
	start := r.Clock.Now()
	nextTick := start.Add(r.Offset(r.Interval))
	var retryAt time.Time
	if r.send(ctx, false) {
		retryAt = start.Add(r.RetryDelay)
	}

	for {
		wakeAt, isRetry := nextTick, false
		if !retryAt.IsZero() && retryAt.Before(nextTick) {
			wakeAt, isRetry = retryAt, true
		}
		select {
		case <-ctx.Done():
			return
		case <-r.Clock.After(wakeAt.Sub(r.Clock.Now())):
		}
		if ctx.Err() != nil {
			return
		}

		retryAt = time.Time{}
		if isRetry {
			r.send(ctx, true)
			continue
		}

		now := r.Clock.Now()
		nextTick = nextTick.Add(r.Interval)
		if !nextTick.After(now) {
			// The process was suspended past a whole interval; don't catch up.
			nextTick = now.Add(r.Interval)
		}
		if r.send(ctx, false) {
			retryAt = now.Add(r.RetryDelay)
		}
	}
}

// send makes one report call and logs the outcome. It returns true when the
// outcome is retryable and this was not already the retry.
func (r *Reporter) send(ctx context.Context, isRetry bool) (retry bool) {
	defer func() {
		if p := recover(); p != nil {
			log.Error().Msgf("feature_server_report panicked: %v", p)
			retry = false
		}
	}()

	callCtx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	err := r.transport.ReportFeatureServer(callCtx, r.report)
	if ctx.Err() != nil {
		return false // shutting down
	}

	app := r.report.GetLabels()["app"]
	if err == nil {
		log.Info().Str("project", r.project).Str("registry", r.report.GetRegistry()).Str("app", app).
			Int("urls", len(r.report.GetUrls())).Bool("retry", isRetry).
			Msg("feature_server_report outcome=accepted")
		return false
	}

	var reportErr *registry.FeatureServerReportError
	if !errors.As(err, &reportErr) {
		reportErr = &registry.FeatureServerReportError{Retryable: true, Outcome: "error", Err: err}
	}
	event := log.Warn().Err(err).Str("project", r.project).Str("registry", r.report.GetRegistry()).
		Str("app", app).Str("outcome", reportErr.Outcome).Bool("retry", isRetry)
	switch {
	case !reportErr.Retryable:
		event.Msg("feature_server_report outcome=rejected; not retrying")
		return false
	case isRetry:
		event.Msg("feature_server_report outcome=failed; retry failed, waiting for the next report")
		return false
	default:
		event.Dur("retry_in", r.RetryDelay).Msg("feature_server_report outcome=failed; will retry once")
		return true
	}
}

// Start reads the reporter settings from getenv and, when reporting is on and
// the registry is an HTTP or gRPC registry, starts the reporter in a
// background goroutine. It returns a function that stops it. It never fails:
// when reporting can't run it logs why and returns a no-op.
func Start(reg *registry.Registry, project string, getenv func(string) string, defaultVersion string) (stop func()) {
	noop := func() {}
	cfg := ConfigFromEnv(getenv, defaultVersion)
	if !cfg.Enabled {
		log.Debug().Msgf("Feature server reporting is off (%s)", EnvReporting)
		return noop
	}
	if reg == nil {
		log.Info().Msg("Feature server reporting is on but there is no registry; not reporting")
		return noop
	}
	transport, ok := reg.FeatureServerReportTransport()
	if !ok {
		log.Info().Msg("Feature server reporting is on but the registry is neither an HTTP nor a gRPC registry; not reporting")
		return noop
	}
	return StartWithTransport(cfg, transport, project)
}

// StartWithTransport starts a reporter for cfg over transport in a background
// goroutine and returns a function that stops it.
func StartWithTransport(cfg Config, transport registry.FeatureServerReportTransport, project string) (stop func()) {
	r := New(cfg, transport, project)
	log.Info().Str("project", project).Str("registry", r.report.GetRegistry()).
		Int("urls", len(cfg.Urls)).Int("labels", len(cfg.Labels)).Str("version", cfg.Version).
		Dur("interval", cfg.Interval).Msg("Feature server reporting is on")
	ctx, cancel := context.WithCancel(context.Background())
	go r.Run(ctx)
	return cancel
}
