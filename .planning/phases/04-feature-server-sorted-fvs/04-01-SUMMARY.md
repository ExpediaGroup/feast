---
phase: 04-feature-server-sorted-fvs
plan: 01
subsystem: feature-server
tags: [go, sorted-feature-views, defaulting, tdd, range-queries]

# Dependency graph
requires:
  - phase: 03-feature-server-core
    provides: TransposeFeatureRowsIntoColumns with UseDefaultsMode parameter, defaulting logic for regular FVs
provides:
  - TransposeRangeFeatureRowsIntoColumns with useDefaults parameter
  - Range value defaulting in processFeatureRowData
  - Per-value defaulting for range queries preserving sort order
affects: [04-02, feature-server-range-queries, sorted-fv-integration]

# Tech tracking
tech-stack:
  added: []
  patterns: [TDD for range query features, per-value defaulting in arrays, Arrow/Proto dual handling]

key-files:
  created: []
  modified:
    - go/internal/feast/onlineserving/serving.go
    - go/internal/feast/onlineserving/serving_test.go
    - go/internal/feast/featurestore.go
    - go/internal/feast/featurestore_test.go

key-decisions:
  - "Range defaulting applies per-value independently while preserving sort order"
  - "featureDefaults map built from SortedFeatureView.Base.Features same pattern as regular FVs"
  - "FLEXIBLE mode replaces NOT_FOUND and NULL_VALUE in both entity-not-found case and per-value nulls"
  - "OUTSIDE_MAX_AGE values not replaced - expired values remain expired"
  - "Arrow and Proto handle nil values differently - Arrow returns empty Value with nil Val, Proto returns nil"

patterns-established:
  - "TDD approach: RED (failing test) → GREEN (implementation) → REFACTOR (cleanup) with atomic commits per phase"
  - "Per-value defaulting: each element in range array evaluated independently with its own status"
  - "Backward compatibility: all existing callers updated with OFF mode to prevent behavior changes"

# Metrics
duration: 293s (4min 53sec)
completed: 2026-02-28
---

# Phase 04 Plan 01: Range Value Defaulting Summary

**Range queries for Sorted FVs now support per-value defaulting with FLEXIBLE mode replacing NOT_FOUND/NULL_VALUE while preserving sort order**

## Performance

- **Duration:** 4 min 53 sec
- **Started:** 2026-02-28T08:19:02Z
- **Completed:** 2026-02-28T08:23:55Z
- **Tasks:** 1 (TDD task with 2 commits: test + feat)
- **Files modified:** 4

## Accomplishments
- Range value defaulting logic implemented in processFeatureRowData mirroring Phase 3's regular FV defaulting
- TransposeRangeFeatureRowsIntoColumns accepts useDefaults parameter and builds featureDefaults map from sorted views
- FLEXIBLE mode replaces NOT_FOUND and NULL_VALUE with defaults (entity-not-found case and per-value nulls)
- OUTSIDE_MAX_AGE values excluded from default application (correct TTL handling)
- Comprehensive table-driven tests: 9 test cases × 2 Arrow modes = 18 executions, all passing
- Full backward compatibility: existing tests and callers updated with OFF mode

## Task Commits

Each TDD phase was committed atomically:

1. **Task 1 RED: Add failing test** - `1caac508` (test)
   - TestApplyRangeDefaults with 9 test cases covering all modes and statuses
   - Test fails as expected: function signature doesn't accept useDefaults yet

2. **Task 1 GREEN: Implement defaulting** - `f6e86a26` (feat)
   - Updated TransposeRangeFeatureRowsIntoColumns and processFeatureRowData signatures
   - Built featureDefaults map from SortedFeatureView.Base.Features
   - Applied defaults for entity-not-found case (featureData.Values == nil)
   - Applied defaults in per-value loop for nil values with NOT_FOUND/NULL_VALUE status
   - Updated all callers with OFF mode for backward compatibility
   - All tests pass, project compiles, no vet issues

3. **Task 1 REFACTOR:** No refactoring needed - code clean and follows Phase 3 patterns

## Files Created/Modified
- `go/internal/feast/onlineserving/serving.go` - Added useDefaults parameter to TransposeRangeFeatureRowsIntoColumns and processFeatureRowData; implemented range value defaulting logic
- `go/internal/feast/onlineserving/serving_test.go` - Added TestApplyRangeDefaults with 9 test cases; updated existing test with OFF mode
- `go/internal/feast/featurestore.go` - Updated TransposeRangeFeatureRowsIntoColumns call with OFF mode
- `go/internal/feast/featurestore_test.go` - Updated TransposeRangeFeatureRowsIntoColumns call with OFF mode

## Decisions Made

**Range defaulting applies per-value independently**
- Each value in range array evaluated with its own status
- Sort order preserved: values stay at same array indices before and after defaulting
- Matches Phase 3 logic for regular FVs but applied to each element in array

**featureDefaults map pattern consistent with Phase 3**
- Built from SortedFeatureView.Base.Features field iteration
- Lookup by feature name (not aliased name) for consistency

**FLEXIBLE mode replacement rules**
- Entity-not-found case: when featureData.Values is nil, replace entire range with single default value
- Per-value nulls: when individual values are nil with NOT_FOUND or NULL_VALUE status, replace with default
- OUTSIDE_MAX_AGE excluded: expired values remain expired (correct TTL semantics)

**Arrow vs Proto nil handling**
- Arrow converts nil values to empty Value objects with nil Val field
- Proto keeps nil values as nil pointers
- Test assertions handle both cases for proper dual-mode testing

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

**Arrow/Proto nil value representation difference**
- **Issue:** Test initially expected nil for NOT_FOUND values without defaults, but Arrow returns empty Value with nil Val
- **Resolution:** Updated test assertions to handle both Arrow (empty Value) and Proto (nil) representations
- **Impact:** Tests now correctly validate both execution modes

## Next Phase Readiness
- Range defaulting core logic complete
- Ready for Plan 02: Wire useDefaults parameter from request through GetOnlineFeatures to TransposeRangeFeatureRowsIntoColumns
- Pattern established for Sorted FV defaulting matches regular FV approach from Phase 3

## Self-Check: PASSED

**Files verified:**
- ✓ go/internal/feast/onlineserving/serving.go
- ✓ go/internal/feast/onlineserving/serving_test.go
- ✓ go/internal/feast/featurestore.go
- ✓ go/internal/feast/featurestore_test.go

**Commits verified:**
- ✓ 1caac508 (test phase)
- ✓ f6e86a26 (implementation phase)

All files and commits exist as documented.

---
*Phase: 04-feature-server-sorted-fvs*
*Completed: 2026-02-28*
