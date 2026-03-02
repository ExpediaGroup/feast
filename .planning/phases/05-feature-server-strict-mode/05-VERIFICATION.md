---
phase: 05-feature-server-strict-mode
verified: 2026-03-02T08:52:36Z
status: passed
score: 11/11 must-haves verified
---

# Phase 05: Feature Server STRICT Mode Verification Report

**Phase Goal:** Feature Server supports STRICT mode with failure on missing defaults

**Verified:** 2026-03-02T08:52:36Z

**Status:** passed

**Re-verification:** No - initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | GetOnlineFeatures with use_defaults=STRICT replaces NULLs with defaults when available | VERIFIED | Lines 847-869 in serving.go: STRICT mode checks NOT_FOUND/NULL_VALUE, applies default, sets PRESENT status. Tests pass for both Arrow modes. |
| 2 | GetOnlineFeatures with use_defaults=STRICT fails request if NULL found with no default defined | VERIFIED | Lines 851-857 in serving.go: Returns GrpcInvalidArgumentErrorf with message "feature '%s' in feature view '%s' has NULL/NOT_FOUND value but no default defined (use_defaults=STRICT)". Tests confirm error behavior. |
| 3 | GetOnlineFeatures with use_defaults=STRICT keeps non-NULL values unchanged | VERIFIED | Lines 825-827 in serving.go: PRESENT values bypass defaulting logic entirely (if statement on line 849 only triggers for NOT_FOUND/NULL_VALUE). Tests verify PRESENT values unchanged. |
| 4 | STRICT mode excludes OUTSIDE_MAX_AGE from defaulting (consistent with FLEXIBLE) | VERIFIED | Lines 847-849 in serving.go: STRICT mode if condition checks only NOT_FOUND/NULL_VALUE, explicitly excluding OUTSIDE_MAX_AGE. Test case "STRICT + OUTSIDE_MAX_AGE" passes. |
| 5 | STRICT mode works identically for regular FVs and Sorted FVs (range queries) | VERIFIED | Lines 1010-1030 (entity-not-found) and 1068-1087 (per-value) in serving.go: processFeatureRowData implements STRICT with same error behavior. TestApplyRangeDefaults includes 8 STRICT test cases (all passing). |
| 6 | Feature Server logs default applications at debug level with feature_view and feature_name context | VERIFIED | Lines 839-843, 863-867, 1002-1006, 1019-1023, 1059-1063, 1075-1079 in serving.go: log.Debug() calls at 6 default application points with Str("feature_view",...) and Str("feature_name",...) context. Test output shows debug logs firing. |
| 7 | Feature Server emits feature_defaults_applied_total metric with feature_view and feature_name labels | VERIFIED | Lines 141-147: Prometheus CounterVec defined with labels ["feature_view", "feature_name"]. Lines 844, 868, 1007, 1024, 1064, 1080: featureDefaultsApplied.WithLabelValues().Inc() called at all 6 application points. |
| 8 | Logging and metrics fire for both FLEXIBLE and STRICT mode default applications | VERIFIED | FLEXIBLE logging: lines 839-844. STRICT logging: lines 863-868. Both paths increment featureDefaultsApplied. Test output confirms debug logs for both modes. |
| 9 | Logging and metrics fire for both regular and range query default applications | VERIFIED | Regular FV logging: lines 839-844 (FLEXIBLE), 863-868 (STRICT). Range query logging: lines 1002-1007, 1019-1024 (entity-not-found), 1059-1064, 1075-1080 (per-value). All paths have paired log.Debug() and metric Inc(). |
| 10 | Debug logging does not fire for non-defaulted values | VERIFIED | Code inspection: All log.Debug() calls are inside the default application branches (after checking default exists and applying it). No logging for PRESENT, OUTSIDE_MAX_AGE, or OFF mode paths. |
| 11 | Metric counter does not increment for non-defaulted values | VERIFIED | Code inspection: All featureDefaultsApplied.Inc() calls are inside default application branches, immediately after log.Debug(). Only increments when defaults actually applied. |

**Score:** 11/11 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| go/internal/feast/onlineserving/serving.go | STRICT mode validation and default application in TransposeFeatureRowsIntoColumns and processFeatureRowData | VERIFIED | 1642 lines. Contains USE_DEFAULTS_STRICT branches (lines 847-869, 1010-1030, 1068-1087). Prometheus import (line 22), metric definition (lines 141-147), init() registration (lines 149-151). |
| go/internal/feast/onlineserving/serving_test.go | STRICT mode test cases for both regular and range defaulting | VERIFIED | Contains 24 occurrences of "STRICT mode" or "USE_DEFAULTS_STRICT". TestApplyDefaults includes 6 STRICT test cases x 2 Arrow modes = 12 tests. TestApplyRangeDefaults includes 8 STRICT test cases. TestDefaultsMetricRegistered present (lines 2988-2997). |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|----|--------|---------|
| TransposeFeatureRowsIntoColumns | errors.GrpcInvalidArgumentErrorf | STRICT validation check before default application | WIRED | Line 854-856: returns GrpcInvalidArgumentErrorf with exact message format including feature name, view name, and "(use_defaults=STRICT)". |
| processFeatureRowData (entity-not-found) | errors.GrpcInvalidArgumentErrorf | STRICT validation check for range queries | WIRED | Line 1028-1030: returns GrpcInvalidArgumentErrorf with message "feature '%s' has NULL/NOT_FOUND value but no default defined (use_defaults=STRICT)". |
| processFeatureRowData (per-value) | errors.GrpcInvalidArgumentErrorf | STRICT validation check for range queries | WIRED | Line 1084-1086: returns GrpcInvalidArgumentErrorf with same message format as entity-not-found case. |
| go/internal/feast/onlineserving/serving.go | prometheus.NewCounterVec | package-level var + init() registration | WIRED | Lines 141-147: featureDefaultsApplied defined. Lines 149-151: init() calls prometheus.MustRegister(featureDefaultsApplied). |
| go/internal/feast/onlineserving/serving.go | zerolog/log | log.Debug() calls in default application branches | WIRED | Import on line 23. log.Debug() called at lines 839, 863, 1002, 1019, 1059, 1075 (6 application points). All calls include .Str("feature_view",...), .Str("feature_name",...), .Str("mode",...), .Msg(...). |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| serving.go | 173 | TODO comment | INFO | Pre-existing TODO unrelated to Phase 05. Does not block phase goal. |

No blockers or warnings found.

### Human Verification Required

#### 1. Prometheus Metrics Endpoint

**Test:** Deploy Feature Server and access the /metrics endpoint (or equivalent metrics exposure path). Search for "feature_defaults_applied_total".

**Expected:** Metric should be present with help text "Total number of times default values were applied to features" and no initial values. After making GetOnlineFeatures requests with defaults applied, counter should increment with correct feature_view and feature_name labels.

**Why human:** Requires running Feature Server and making actual gRPC requests. Cannot verify metric HTTP endpoint exposure programmatically from source code alone.

#### 2. Debug Logging in Production

**Test:** Deploy Feature Server with zerolog debug level enabled. Make GetOnlineFeatures request with use_defaults=STRICT where defaults are applied. Check server logs.

**Expected:** Should see JSON log lines with level="debug", feature_view="...", feature_name="...", mode="STRICT", message="Applied default value to feature". Logs should not appear for non-defaulted values.

**Why human:** Requires running server and inspecting actual log output. Unit tests verify log.Debug() is called but not the full zerolog pipeline.

#### 3. STRICT Mode Error Flow

**Test:** Deploy Feature Server. Make GetOnlineFeatures request with use_defaults=STRICT for a feature that has NULL values and no default defined in the feature view schema.

**Expected:** Request should fail with gRPC InvalidArgument status and error message "feature 'X' in feature view 'Y' has NULL/NOT_FOUND value but no default defined (use_defaults=STRICT)".

**Why human:** Requires end-to-end integration with Feature Server gRPC handler, registry, and online store. Unit tests verify the logic but not the full request/response flow.

---

## Verification Details

### Commits Verified

All commits referenced in SUMMARYs exist and are reachable:

- `9a79a93ac` - test(05-01): add failing STRICT mode tests for defaulting
- `102f884ef` - feat(05-01): implement STRICT mode validation and defaulting  
- `38f02d1ec` - feat(05-02): add observability to default value application
- `499437714` - test(05-02): add test for feature defaults metric registration

### Test Results

All tests pass with no failures:

```
=== RUN   TestApplyDefaults
  STRICT + NOT_FOUND + has_default: PASS (useArrow=true, useArrow=false)
  STRICT + NULL_VALUE + has_default: PASS (useArrow=true, useArrow=false)
  STRICT + NOT_FOUND + no_default: PASS (useArrow=true, useArrow=false)
  STRICT + NULL_VALUE + no_default: PASS (useArrow=true, useArrow=false)
  STRICT + PRESENT value: PASS (useArrow=true, useArrow=false)
  STRICT + OUTSIDE_MAX_AGE: PASS (useArrow=true, useArrow=false)
--- PASS: TestApplyDefaults (0.00s)

=== RUN   TestDefaultsMetricRegistered
--- PASS: TestDefaultsMetricRegistered (0.00s)

PASS
ok  	github.com/feast-dev/feast/go/internal/feast/onlineserving	0.580s
```

Debug logs visible in test output confirming logging is wired:
```
{"level":"debug","feature_view":"testView","feature_name":"f1","mode":"STRICT","time":"2026-03-02T00:52:36-08:00","message":"Applied default value to feature"}
```

### Code Quality

- `go vet ./internal/feast/onlineserving/...` - PASS (no warnings)
- `go build ./...` - PASS (project compiles cleanly)
- No stub implementations found (no empty returns, placeholder comments in modified code)
- No Info/Warn level logging for default applications (only Debug level as required)

### Design Verification

**Two-pass STRICT validation:** Code inspection confirms STRICT mode in TransposeFeatureRowsIntoColumns uses fail-fast approach (lines 847-869). On first NULL/NOT_FOUND without default, immediately returns error. Does not collect all errors before failing.

**Entity-not-found handling in range queries:** Lines 1010-1030 in processFeatureRowData handle the featureData.Values == nil case for both FLEXIBLE and STRICT modes. STRICT returns error if no default exists.

**Consistent OUTSIDE_MAX_AGE exclusion:** Both FLEXIBLE (line 832) and STRICT (line 849) mode checks explicitly test for `status == NOT_FOUND || status == NULL_VALUE`, excluding OUTSIDE_MAX_AGE. Matches research decisions D010 and D016.

**Low-cardinality metrics:** Prometheus counter uses only ["feature_view", "feature_name"] labels. No entity keys, timestamps, or request IDs that would cause cardinality explosion.

**Observability placement:** All 6 log.Debug() and featureDefaultsApplied.Inc() calls occur AFTER default is applied (after value assignment and status = PRESENT), not during validation pass. Metrics count actual usage, not validation checks.

---

_Verified: 2026-03-02T08:52:36Z_
_Verifier: Claude (gsd-verifier)_
