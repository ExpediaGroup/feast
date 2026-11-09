//go:build !integration

package reporter

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/feast-dev/feast/go/internal/feast/registry"
	registryPb "github.com/feast-dev/feast/go/protos/feast/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- fakes ----

// fakeClock hands every After call to the test, which advances time to that
// wait's deadline and fires it. Nothing happens until the test says so.
type fakeClock struct {
	mu    sync.Mutex
	now   time.Time
	waits chan fakeWait
}

type fakeWait struct {
	d  time.Duration
	ch chan time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC), waits: make(chan fakeWait, 16)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	c.waits <- fakeWait{d: d, ch: ch}
	return ch
}

// nextWait returns the duration of the next wait the scheduler asked for.
func (c *fakeClock) nextWait(t *testing.T) fakeWait {
	t.Helper()
	select {
	case w := <-c.waits:
		return w
	case <-time.After(5 * time.Second):
		t.Fatal("scheduler did not wait")
		return fakeWait{}
	}
}

func (c *fakeClock) fire(w fakeWait) {
	c.mu.Lock()
	c.now = c.now.Add(w.d)
	now := c.now
	c.mu.Unlock()
	w.ch <- now
}

type fakeCall struct {
	at       time.Time
	deadline time.Duration // time left on the call's context
	report   *registryPb.FeatureServerReport
}

// fakeTransport returns scripted results in order (nil once they run out).
type fakeTransport struct {
	clock    *fakeClock
	registry string
	mu       sync.Mutex
	results  []error
	calls    chan fakeCall
	panicOn  int // 1-based call number that panics; 0 = never
	count    int
}

func newFakeTransport(clock *fakeClock, results ...error) *fakeTransport {
	return &fakeTransport{clock: clock, registry: "grpc", results: results, calls: make(chan fakeCall, 16)}
}

func (f *fakeTransport) FeatureServerReportRegistry() string { return f.registry }

func (f *fakeTransport) ReportFeatureServer(ctx context.Context, report *registryPb.FeatureServerReport) error {
	f.mu.Lock()
	f.count++
	n := f.count
	var err error
	if len(f.results) > 0 {
		err, f.results = f.results[0], f.results[1:]
	}
	f.mu.Unlock()

	deadline, _ := ctx.Deadline()
	f.calls <- fakeCall{at: f.clock.Now(), deadline: time.Until(deadline), report: report}
	if n == f.panicOn {
		panic("transport bug")
	}
	return err
}

func (f *fakeTransport) nextCall(t *testing.T) fakeCall {
	t.Helper()
	select {
	case c := <-f.calls:
		return c
	case <-time.After(5 * time.Second):
		t.Fatal("no report call")
		return fakeCall{}
	}
}

func (f *fakeTransport) assertNoCall(t *testing.T) {
	t.Helper()
	select {
	case c := <-f.calls:
		t.Fatalf("unexpected report call at %s", c.at)
	default:
	}
}

var (
	errRetryable = &registry.FeatureServerReportError{Retryable: true, Outcome: "UNAVAILABLE"}
	errRejected  = &registry.FeatureServerReportError{Retryable: false, Outcome: "INVALID_ARGUMENT"}
)

func testConfig() Config {
	return Config{
		Enabled:  true,
		Urls:     []AdvertisedUrl{{Url: "grpcs://app.example.net:443", Scope: "external"}},
		Labels:   map[string]string{"app": "app"},
		Version:  "v1",
		Interval: 24 * time.Hour,
	}
}

// startTestReporter runs a reporter with a fake clock, a fixed offset and a
// 1 h retry delay; it stops when the test ends.
func startTestReporter(t *testing.T, transport *fakeTransport, clock *fakeClock, offset time.Duration) *Reporter {
	t.Helper()
	r := New(testConfig(), transport, "my_project")
	r.Clock = clock
	r.RetryDelay = time.Hour
	r.Offset = func(time.Duration) time.Duration { return offset }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		// Unblock a pending wait so Run can see the cancellation.
		select {
		case w := <-clock.waits:
			w.ch <- clock.Now()
		default:
		}
		<-done
	})
	return r
}

// ---- report contents ----

func TestNew_BuildsReport(t *testing.T) {
	cfg := testConfig()
	r := New(cfg, newFakeTransport(newFakeClock()), "my_project")
	report := r.Report()
	assert.Equal(t, int32(1), report.GetContract())
	assert.Equal(t, "grpc", report.GetRegistry())
	assert.Equal(t, "v1", report.GetVersion())
	assert.Equal(t, map[string]string{"app": "app"}, report.GetLabels())
	require.Len(t, report.GetUrls(), 1)
	assert.Equal(t, "grpcs://app.example.net:443", report.GetUrls()[0].GetUrl())
	assert.Equal(t, "external", report.GetUrls()[0].GetScope())

	// The report owns its labels: later changes to the config don't leak in.
	cfg.Labels["app"] = "changed"
	assert.Equal(t, "app", report.GetLabels()["app"])
}

func TestRandomOffsetWithinInterval(t *testing.T) {
	for i := 0; i < 1000; i++ {
		d := randomOffset(24 * time.Hour)
		// Never in the first half, so the first periodic report is always
		// well after the startup one (no double call per pod in a rollout).
		assert.GreaterOrEqual(t, d, 12*time.Hour)
		assert.Less(t, d, 24*time.Hour)
	}
	assert.Equal(t, time.Duration(0), randomOffset(0))
}

// ---- schedule ----

func TestRun_ReportsAtStartThenAtOffsetThenEveryInterval(t *testing.T) {
	clock := newFakeClock()
	start := clock.Now()
	transport := newFakeTransport(clock)
	startTestReporter(t, transport, clock, 5*time.Hour)

	first := transport.nextCall(t)
	assert.Equal(t, start, first.at)
	assert.Equal(t, "grpc", first.report.GetRegistry())

	w := clock.nextWait(t)
	assert.Equal(t, 5*time.Hour, w.d)
	clock.fire(w)
	assert.Equal(t, start.Add(5*time.Hour), transport.nextCall(t).at)

	for i := 1; i <= 3; i++ {
		w = clock.nextWait(t)
		assert.Equal(t, 24*time.Hour, w.d)
		clock.fire(w)
		assert.Equal(t, start.Add(5*time.Hour+time.Duration(i)*24*time.Hour), transport.nextCall(t).at)
	}
}

func TestRun_EachCallHasTheTimeout(t *testing.T) {
	clock := newFakeClock()
	transport := newFakeTransport(clock)
	startTestReporter(t, transport, clock, time.Hour)

	call := transport.nextCall(t)
	assert.Greater(t, call.deadline, time.Duration(0))
	assert.LessOrEqual(t, call.deadline, DefaultTimeout)
}

// ---- retry policy ----

func TestRun_RetryableFailureRetriedOnceAnHourLater(t *testing.T) {
	clock := newFakeClock()
	start := clock.Now()
	// start: retryable; retry: retryable again (no second retry); tick: ok.
	transport := newFakeTransport(clock, errRetryable, errRetryable, nil)
	startTestReporter(t, transport, clock, 10*time.Hour)

	assert.Equal(t, start, transport.nextCall(t).at)

	w := clock.nextWait(t)
	assert.Equal(t, time.Hour, w.d, "retry goes one hour later")
	clock.fire(w)
	assert.Equal(t, start.Add(time.Hour), transport.nextCall(t).at)

	w = clock.nextWait(t)
	assert.Equal(t, 9*time.Hour, w.d, "a failed retry is not retried; next is the periodic report")
	clock.fire(w)
	assert.Equal(t, start.Add(10*time.Hour), transport.nextCall(t).at)

	w = clock.nextWait(t)
	assert.Equal(t, 24*time.Hour, w.d)
	transport.assertNoCall(t)
}

func TestRun_PeriodicReportFailureAlsoRetried(t *testing.T) {
	clock := newFakeClock()
	start := clock.Now()
	transport := newFakeTransport(clock, nil, errRetryable, nil)
	startTestReporter(t, transport, clock, 2*time.Hour)

	transport.nextCall(t)
	w := clock.nextWait(t)
	assert.Equal(t, 2*time.Hour, w.d)
	clock.fire(w)
	assert.Equal(t, start.Add(2*time.Hour), transport.nextCall(t).at)

	w = clock.nextWait(t)
	assert.Equal(t, time.Hour, w.d)
	clock.fire(w)
	assert.Equal(t, start.Add(3*time.Hour), transport.nextCall(t).at)

	// Cadence stays anchored to the offset: next report at 2h + 24h.
	w = clock.nextWait(t)
	assert.Equal(t, 23*time.Hour, w.d)
}

func TestRun_RejectionNotRetried(t *testing.T) {
	clock := newFakeClock()
	transport := newFakeTransport(clock, errRejected)
	startTestReporter(t, transport, clock, 10*time.Hour)

	transport.nextCall(t)
	w := clock.nextWait(t)
	assert.Equal(t, 10*time.Hour, w.d, "no retry after a rejection")
}

func TestRun_UnclassifiedErrorIsRetryable(t *testing.T) {
	clock := newFakeClock()
	transport := newFakeTransport(clock, errors.New("boom"))
	startTestReporter(t, transport, clock, 10*time.Hour)

	transport.nextCall(t)
	assert.Equal(t, time.Hour, clock.nextWait(t).d)
}

func TestRun_RetryDroppedWhenPeriodicReportComesFirst(t *testing.T) {
	clock := newFakeClock()
	start := clock.Now()
	transport := newFakeTransport(clock, errRetryable, nil)
	startTestReporter(t, transport, clock, 30*time.Minute)

	transport.nextCall(t)
	w := clock.nextWait(t)
	assert.Equal(t, 30*time.Minute, w.d, "periodic report before the 1 h retry")
	clock.fire(w)
	assert.Equal(t, start.Add(30*time.Minute), transport.nextCall(t).at)

	w = clock.nextWait(t)
	assert.Equal(t, 24*time.Hour, w.d, "the pending retry is dropped")
}

func TestRun_PanickingTransportDoesNotStopReporting(t *testing.T) {
	clock := newFakeClock()
	transport := newFakeTransport(clock)
	transport.panicOn = 1
	startTestReporter(t, transport, clock, 3*time.Hour)

	transport.nextCall(t)
	w := clock.nextWait(t)
	assert.Equal(t, 3*time.Hour, w.d, "a panic is not retried")
	clock.fire(w)
	transport.nextCall(t)
}

func TestRun_StopsOnCancel(t *testing.T) {
	clock := newFakeClock()
	transport := newFakeTransport(clock)
	r := New(testConfig(), transport, "p")
	r.Clock = clock
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	transport.nextCall(t)
	clock.nextWait(t) // Run is now waiting
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// ---- Start: off switch and registry types ----

// newHttpRegistry builds a real HTTP registry against server.
func newHttpRegistry(t *testing.T, serverUrl string) *registry.Registry {
	t.Helper()
	reg, err := registry.NewRegistry(&registry.RegistryConfig{
		RegistryStoreType: "HttpRegistryStore", Path: serverUrl, ClientId: "c", CacheTtlSeconds: 600,
	}, t.TempDir(), "my_project")
	require.NoError(t, err)
	return reg
}

func countingRegistryServer(t *testing.T) (*httptest.Server, *atomic.Int32, chan struct{}) {
	t.Helper()
	posts := &atomic.Int32{}
	got := make(chan struct{}, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/projects/my_project/feature_servers" {
			posts.Add(1)
			got <- struct{}{}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusOK) // connectivity check
	}))
	t.Cleanup(server.Close)
	return server, posts, got
}

func TestStart_OffSendsNothing(t *testing.T) {
	server, posts, _ := countingRegistryServer(t)
	reg := newHttpRegistry(t, server.URL)

	for _, value := range []string{"", "off"} {
		stop := Start(reg, "my_project", envOf(map[string]string{EnvReporting: value}), "v")
		stop()
	}
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(0), posts.Load())
}

func TestStart_OnReportsOverHttpRegistry(t *testing.T) {
	server, posts, got := countingRegistryServer(t)
	reg := newHttpRegistry(t, server.URL)

	stop := Start(reg, "my_project", envOf(map[string]string{
		EnvReporting:      "on",
		EnvAdvertisedUrls: "external=https://app.example.net",
	}), "v")
	defer stop()

	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("no report reached the registry")
	}
	assert.Equal(t, int32(1), posts.Load())
}

func TestStart_FileRegistryIsSkipped(t *testing.T) {
	logs := captureLogs(t)
	reg, err := registry.NewRegistry(&registry.RegistryConfig{
		Path: filepath.Join(t.TempDir(), "registry.db"), CacheTtlSeconds: 600,
	}, t.TempDir(), "my_project")
	require.NoError(t, err)

	stop := Start(reg, "my_project", envOf(map[string]string{EnvReporting: "on"}), "v")
	stop()
	assert.Contains(t, logs.String(), "neither an HTTP nor a gRPC registry")
}

func TestStart_NilRegistryIsSkipped(t *testing.T) {
	stop := Start(nil, "my_project", envOf(map[string]string{EnvReporting: "on"}), "v")
	stop()
}

func TestRun_DisabledRegistryIsNotRetriedAndLogsInfo(t *testing.T) {
	logs := captureLogs(t)
	clock := newFakeClock()
	disabled := &registry.FeatureServerReportError{Disabled: true, Outcome: "disabled"}
	transport := newFakeTransport(clock, disabled)
	startTestReporter(t, transport, clock, 10*time.Hour)

	transport.nextCall(t)
	w := clock.nextWait(t)
	assert.Equal(t, 10*time.Hour, w.d, "reports switched off is a setting, not an outage: no retry")
	assert.Contains(t, logs.String(), "outcome=disabled")
	assert.NotContains(t, logs.String(), `"level":"warn"`)
}
