---
phase: 05-feature-server-strict-mode
plan: 01
subsystem: feature-server
tags: [go, grpc, validation, defaults, strict-mode]

# Dependency graph
requires:
  - phase: none
    provides: "Base feature serving infrastructure with FLEXIBLE defaulting"
provides:
  - "STRICT mode validation in TransposeFeatureRowsIntoColumns"
  - "STRICT mode validation in processFeatureRowData for range queries"
  - "Error on NULL/NOT_FOUND without defaults in STRICT mode"
  - "Automatic default application when defaults exist in STRICT mode"
affects: [05-02-api-integration, feature-server-validation]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Two-phase defaulting: validate first, apply second (STRICT)"
    - "GrpcInvalidArgumentError for validation failures"
    - "Consistent handling across regular and range queries"

key-files:
  created: []
  modified:
    - go/internal/feast/onlineserving/serving.go
    - go/internal/feast/onlineserving/serving_test.go

key-decisions:
  - "STRICT mode validates all NULL/NOT_FOUND before applying defaults (fail-fast approach)"
  - "OUTSIDE_MAX_AGE values excluded from STRICT validation (consistent with FLEXIBLE)"
  - "Entity-not-found cases in range queries handled identically to per-value cases"

patterns-established:
  - "Pattern 1: STRICT validation uses GrpcInvalidArgumentError with feature and view context"
  - "Pattern 2: Error messages include 'use_defaults=STRICT' for operator clarity"
  - "Pattern 3: Test table-driven approach with expectError and errorContains fields"

# Metrics
duration: 4min
completed: 2026-03-02
---

# Phase 05 Plan 01: STRICT Mode Default Validation and Application Summary

**STRICT mode validation enforces fail-fast defaulting with GrpcInvalidArgumentError on missing defaults for both regular and range feature queries**

## Performance

- **Duration:** 4 min 10 sec
- **Started:** 2026-03-02T08:40:03Z
- **Completed:** 2026-03-02T08:44:13Z
- **Tasks:** 1 (TDD: RED → GREEN)
- **Files modified:** 2

## Accomplishments
- Implemented STRICT mode validation in TransposeFeatureRowsIntoColumns with fail-fast error on missing defaults
- Implemented STRICT mode validation in processFeatureRowData for both entity-not-found and per-value cases
- Added 14 test cases for STRICT mode (6 regular FV + 8 range FV) x 2 Arrow modes = 28 new passing tests
- STRICT mode properly excludes OUTSIDE_MAX_AGE from validation (consistent with FLEXIBLE)

## Task Commits

Each TDD phase was committed atomically:

1. **RED Phase: Add failing STRICT mode tests** - `9a79a93ac` (test)
   - Added expectError and errorContains fields to test structs
   - Added 6 STRICT test cases to TestApplyDefaults
   - Added 8 STRICT test cases to TestApplyRangeDefaults with entityNotFound support
   - Tests failed as expected (STRICT not yet implemented)

2. **GREEN Phase: Implement STRICT mode validation** - `102f884ef` (feat)
   - Added STRICT mode logic to TransposeFeatureRowsIntoColumns
   - Added STRICT mode logic to processFeatureRowData for range queries
   - All 28 new STRICT tests pass across both Arrow and Proto modes

## Files Created/Modified
- `go/internal/feast/onlineserving/serving.go` - Added STRICT mode validation and defaulting to both TransposeFeatureRowsIntoColumns and processFeatureRowData
- `go/internal/feast/onlineserving/serving_test.go` - Added 14 STRICT test cases with error handling infrastructure

## Decisions Made

**Decision 1: Fail immediately on first missing default**
- Rationale: STRICT mode's purpose is fail-fast data integrity. No need to collect all errors before failing.
- Impact: Simpler implementation, faster failure detection, clear error messages

**Decision 2: Use GrpcInvalidArgumentError with feature and view context**
- Rationale: Operator needs to know which feature and view lacks defaults
- Format: "feature 'X' in feature view 'Y' has NULL/NOT_FOUND value but no default defined (use_defaults=STRICT)"
- Impact: Clear error messages for debugging

**Decision 3: Consistent OUTSIDE_MAX_AGE exclusion**
- Rationale: OUTSIDE_MAX_AGE indicates data is present but stale. Not a NULL/NOT_FOUND case requiring defaults.
- Impact: Consistent behavior between FLEXIBLE and STRICT modes per research decisions D010 and D016

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None - TDD approach worked smoothly. Tests failed as expected in RED, passed after GREEN implementation.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- STRICT mode core logic complete and tested
- Ready for API integration in 05-02-PLAN.md
- Protobuf definitions and gRPC service handler updates can now reference USE_DEFAULTS_STRICT
- All existing tests continue to pass (backward compatibility confirmed)

## Self-Check: PASSED

- ✓ serving.go exists and contains STRICT mode implementation
- ✓ serving_test.go exists with 14 new STRICT test cases
- ✓ Commit 9a79a93ac exists (RED phase)
- ✓ Commit 102f884ef exists (GREEN phase)
- ✓ All tests pass (28 STRICT mode tests + existing tests)
- ✓ go vet passes with no warnings
- ✓ Project compiles cleanly

---
*Phase: 05-feature-server-strict-mode*
*Completed: 2026-03-02*
