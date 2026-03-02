---
status: diagnosed
phase: 05-feature-server-strict-mode
source: [05-01-SUMMARY.md, 05-02-SUMMARY.md]
started: 2026-03-02T00:50:00Z
updated: 2026-03-02T01:00:00Z
---

## Current Test

[testing complete]

## Tests

### 1. STRICT Mode Replaces NULL with Defaults When Available
expected: When calling GetOnlineFeatures with use_defaults=STRICT on a feature with NULL/NOT_FOUND value and a defined default, the response returns the default value with status PRESENT (not NULL/NOT_FOUND).
result: pass

### 2. STRICT Mode Fails Request When Default Missing
expected: When calling GetOnlineFeatures with use_defaults=STRICT on a feature with NULL/NOT_FOUND value but NO defined default, the request fails with gRPC InvalidArgument error containing the feature name, feature view name, and "(use_defaults=STRICT)" text.
result: pass

### 3. STRICT Mode Keeps Non-NULL Values Unchanged
expected: When calling GetOnlineFeatures with use_defaults=STRICT on a feature with a PRESENT (non-NULL) value, the response returns the original value unchanged even if a default is defined.
result: pass

### 4. STRICT Mode Excludes OUTSIDE_MAX_AGE from Validation
expected: When calling GetOnlineFeatures with use_defaults=STRICT on a feature with OUTSIDE_MAX_AGE status (stale data), the request succeeds without error and the value remains OUTSIDE_MAX_AGE (not replaced with default).
result: pass

### 5. STRICT Mode Works for Range Queries (Sorted FVs)
expected: When calling GetOnlineFeaturesRange with use_defaults=STRICT on a Sorted FeatureView, the same STRICT validation and defaulting behavior applies - replaces NULLs with defaults when available, fails if default missing.
result: pass

### 6. Prometheus Metric Exposed
expected: The Feature Server exposes a Prometheus metric named "feature_defaults_applied_total" with labels "feature_view" and "feature_name", incremented each time a default is applied in either FLEXIBLE or STRICT mode.
result: issue
reported: "No need to expose any metric?"
severity: major

### 7. Debug Logging for Default Applications
expected: When defaults are applied (FLEXIBLE or STRICT mode), the Feature Server logs debug-level messages (not Info/Warn) containing the feature view name, feature name, and default value applied.
result: pass

## Summary

total: 7
passed: 6
issues: 1
pending: 0
skipped: 0

## Gaps

- truth: "Feature Server exposes feature_defaults_applied_total metric accessible to operators"
  status: failed
  reason: "User reported: No need to expose any metric?"
  severity: major
  test: 6
  root_cause: "User confusion or testing error - the Prometheus metric IS properly exposed. gRPC mode exposes metrics on separate HTTP port 8080 (not the gRPC port). HTTP/hybrid modes expose metrics on same port as API. Implementation is complete and correct."
  artifacts:
    - path: "go/internal/feast/onlineserving/serving.go"
      issue: "No issue - metric correctly registered and incremented"
    - path: "go/internal/feast/server/server_commons.go"
      issue: "No issue - /metrics endpoint correctly wired for HTTP/hybrid servers"
    - path: "go/main.go"
      issue: "No issue - /metrics endpoint correctly wired for gRPC server (port 8080)"
  missing:
    - "Clarification: gRPC mode check http://localhost:8080/metrics (separate port)"
    - "Clarification: HTTP/hybrid mode check http://localhost:8080/metrics (same port as API)"
  debug_session: "/Users/vbhagwat/feast/.planning/debug/prometheus-metric-not-exposed.md"
