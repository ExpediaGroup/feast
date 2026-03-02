---
status: diagnosed
trigger: "Diagnose UAT Test 6: Prometheus metric feature_defaults_applied_total not accessible - user reported 'No need to expose any metric?'"
created: 2026-03-02T00:00:00Z
updated: 2026-03-02T00:00:00Z
symptoms_prefilled: true
goal: find_root_cause_only
---

## Current Focus

hypothesis: Metric is properly registered but user misunderstood the requirement or couldn't access the endpoint
test: Code inspection of metric registration and endpoint configuration
expecting: Find either missing wiring or clarify that metric IS exposed
next_action: complete diagnosis

## Symptoms

expected: The Feature Server exposes a Prometheus metric named "feature_defaults_applied_total" with labels "feature_view" and "feature_name", incremented each time a default is applied in either FLEXIBLE or STRICT mode.
actual: User reported "No need to expose any metric?" in UAT Test 6
errors: None reported
reproduction: Start Feature Server and check /metrics endpoint for feature_defaults_applied_total metric
started: UAT Phase 05, Test 6

## Eliminated

- hypothesis: Metric not registered in Prometheus
  evidence: go/internal/feast/onlineserving/serving.go lines 141-151 show metric is defined and registered via init() with prometheus.MustRegister()
  timestamp: 2026-03-02T00:00:00Z

- hypothesis: Metric not incremented
  evidence: Verification shows 6 increment points at lines 844, 868, 1007, 1024, 1064, 1080 in serving.go
  timestamp: 2026-03-02T00:00:00Z

- hypothesis: HTTP server missing /metrics endpoint
  evidence: server_commons.go line 39-40 registers promhttp.Handler() at /metrics path in CommonHttpHandlers
  timestamp: 2026-03-02T00:00:00Z

- hypothesis: gRPC server missing /metrics endpoint
  evidence: main.go lines 209-213 start separate HTTP endpoint on port 8080 with /metrics for gRPC mode
  timestamp: 2026-03-02T00:00:00Z

## Evidence

- timestamp: 2026-03-02T00:00:00Z
  checked: go/internal/feast/onlineserving/serving.go metric registration
  found: Lines 141-151 define featureDefaultsApplied CounterVec with correct name and labels, registered in init()
  implication: Metric is properly registered with Prometheus default registry

- timestamp: 2026-03-02T00:00:00Z
  checked: go/internal/feast/server/server_commons.go HTTP handlers
  found: Line 39-40 register promhttp.Handler() at /metrics in CommonHttpHandlers
  implication: HTTP and hybrid servers expose /metrics endpoint

- timestamp: 2026-03-02T00:00:00Z
  checked: go/main.go gRPC server setup
  found: Lines 209-213 start goroutine with http.Handle("/metrics", promhttp.Handler()) on port 8080
  implication: gRPC-only server exposes /metrics on separate HTTP port 8080

- timestamp: 2026-03-02T00:00:00Z
  checked: HTTP server handler registration
  found: http_server.go line 769 DefaultHttpHandlers calls CommonHttpHandlers which includes /metrics
  implication: HTTP server serves metrics on same port as API endpoints

- timestamp: 2026-03-02T00:00:00Z
  checked: Hybrid server handler registration
  found: hybrid_server.go line 19 DefaultHybridHandlers calls CommonHttpHandlers which includes /metrics
  implication: Hybrid server serves metrics on HTTP port

- timestamp: 2026-03-02T00:00:00Z
  checked: 05-RESEARCH.md requirements
  found: Pattern 4 (lines 100-122) documents exact metric pattern with registration and increment
  implication: Metric exposure was explicitly required by research phase

- timestamp: 2026-03-02T00:00:00Z
  checked: 05-02-PLAN.md must_haves
  found: Line 15 states "Feature Server emits feature_defaults_applied_total metric with feature_view and feature_name labels"
  implication: Metric exposure was explicit requirement in plan

## Resolution

root_cause: User confusion or testing error - the metric IS properly exposed. The implementation is correct:

1. **Metric Registration**: The metric is correctly defined as a package-level variable in go/internal/feast/onlineserving/serving.go (lines 141-147) and registered with prometheus.MustRegister() in init() function (lines 149-151). This registers it with the default Prometheus registry.

2. **Metric Increments**: The metric is correctly incremented at 6 default application points (lines 844, 868, 1007, 1024, 1064, 1080).

3. **HTTP/Hybrid Server Exposure**: For HTTP and hybrid servers, the /metrics endpoint is registered via CommonHttpHandlers in server_commons.go (lines 39-40) which uses promhttp.Handler() to expose all metrics registered with the default Prometheus registry.

4. **gRPC Server Exposure**: For gRPC-only servers, main.go (lines 209-213) starts a separate HTTP server on port 8080 specifically to expose the /metrics endpoint.

The user's comment "No need to expose any metric?" suggests either:
- They misunderstood the test and thought metrics were NOT required (requirements clarification issue)
- They couldn't access the /metrics endpoint (testing/config issue)
- They question whether metrics SHOULD be exposed (requirements challenge)

**Most likely cause**: User was testing gRPC mode and didn't realize metrics are on separate port 8080, OR they are questioning the requirement itself.

fix: N/A - implementation is correct
verification: Test by starting each server type and accessing /metrics endpoint
files_changed: []
