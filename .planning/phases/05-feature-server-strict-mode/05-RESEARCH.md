# Phase 5: Feature Server STRICT Mode - Research

**Researched:** 2026-03-02
**Domain:** Go feature serving with error handling and observability
**Confidence:** HIGH

## Summary

Phase 5 implements STRICT mode for feature defaulting in Feast's Go feature server. STRICT mode differs from FLEXIBLE mode (implemented in Phases 3-4) by requiring that ALL NULL/NOT_FOUND values have defined defaults - if any value lacks a default, the entire request fails with a gRPC InvalidArgument error.

The implementation requires changes to two transpose functions (`TransposeFeatureRowsIntoColumns` and `TransposeRangeFeatureRowsIntoColumns`) to detect missing defaults and return errors, plus instrumentation with structured logging (zerolog) and Prometheus metrics.

**Primary recommendation:** Extend existing defaulting logic with early-fail validation. Check all NULL/NOT_FOUND values for defaults before applying any, then fail-fast if any default is missing.

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| google.golang.org/grpc | (existing) | gRPC error handling | Already used for all serving errors |
| github.com/rs/zerolog | v1.34.0 | Structured logging | Already imported and used throughout codebase |
| github.com/prometheus/client_golang | v1.22.0 | Metrics collection | Already integrated with server |
| github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus | v1.1.0 | gRPC metrics | Already configured in server setup |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| google.golang.org/grpc/codes | (existing) | Error code constants | For InvalidArgument status codes |
| google.golang.org/grpc/status | (existing) | gRPC status creation | For error responses |

**Installation:**
All dependencies already present in go.mod - no new installations needed.

## Architecture Patterns

### Recommended Function Structure
```
go/internal/feast/onlineserving/
├── serving.go              # Main transpose functions with STRICT logic
└── serving_test.go         # Test cases for STRICT mode
```

### Pattern 1: Two-Pass Default Application (STRICT Mode)
**What:** First pass validates all defaults exist, second pass applies them
**When to use:** STRICT mode only - ensures fail-fast before modifying any values
**Example:**
```go
// Source: Inferred from existing FLEXIBLE implementation at serving.go:817-826
if useDefaults == serving.UseDefaultsMode_USE_DEFAULTS_STRICT {
    // Pass 1: Validate all required defaults exist
    for featureIndex := 0; featureIndex < numFeatures; featureIndex++ {
        // ... iterate through values
        if status == serving.FieldStatus_NOT_FOUND || status == serving.FieldStatus_NULL_VALUE {
            featureName := groupRef.FeatureNames[featureIndex]
            if _, ok := featureDefaults[featureName]; !ok {
                return nil, errors.GrpcInvalidArgumentErrorf(
                    "feature '%s' has NULL/NOT_FOUND value but no default defined (use_defaults=STRICT)",
                    featureName)
            }
        }
    }
    // Pass 2: Apply defaults (same as FLEXIBLE)
    // ... apply defaults knowing all exist
}
```

### Pattern 2: Single-Pass Default Application (FLEXIBLE Mode)
**What:** Apply defaults as values are encountered, skip if default missing
**When to use:** FLEXIBLE mode (already implemented in Phases 3-4)
**Example:**
```go
// Source: go/internal/feast/onlineserving/serving.go:817-826
if useDefaults == serving.UseDefaultsMode_USE_DEFAULTS_FLEXIBLE {
    if status == serving.FieldStatus_NOT_FOUND || status == serving.FieldStatus_NULL_VALUE {
        featureName := groupRef.FeatureNames[featureIndex]
        if defaultVal, ok := featureDefaults[featureName]; ok {
            value = &prototypes.Value{Val: defaultVal.Val}
            status = serving.FieldStatus_PRESENT
        }
        // If default missing, leave as NULL - no error
    }
}
```

### Pattern 3: Structured Logging with zerolog
**What:** Use zerolog for debug-level default application logging
**When to use:** Every default application event
**Example:**
```go
// Source: go/internal/feast/onlineserving/serving.go:195, 237
import "github.com/rs/zerolog/log"

log.Debug().
    Str("feature_view", featureViewName).
    Str("feature_name", featureName).
    Str("default_value", defaultVal.String()).
    Msg("Applied default value to NULL feature")
```

### Pattern 4: Prometheus Counter Metrics
**What:** Counter metric incremented per-default-application with labels
**When to use:** Track default application frequency by feature view and feature
**Example:**
```go
// Source: go/internal/feast/server/grpc_server.go:234-235
import "github.com/prometheus/client_golang/prometheus"

var featureDefaultsApplied = prometheus.NewCounterVec(
    prometheus.CounterOpts{
        Name: "feature_defaults_applied_total",
        Help: "Total number of times default values were applied to features",
    },
    []string{"feature_view", "feature_name"},
)

func init() {
    prometheus.MustRegister(featureDefaultsApplied)
}

// In transpose function:
featureDefaultsApplied.WithLabelValues(featureViewName, featureName).Inc()
```

### Anti-Patterns to Avoid
- **Partial default application then error:** Don't modify values before validating all defaults exist in STRICT mode
- **Mixing FLEXIBLE and STRICT logic:** Keep mode branches completely separate for clarity
- **Logging at wrong level:** Default applications are DEBUG, errors are ERROR
- **Missing metric labels:** Always include both feature_view and feature_name labels

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| gRPC error creation | Custom error types | `errors.GrpcInvalidArgumentErrorf()` | Existing helper handles status codes, messages, formatting |
| Structured logging | fmt.Printf debugging | `github.com/rs/zerolog/log` | Already configured with proper levels, context propagation |
| Metrics registration | Manual counter maps | `prometheus.CounterVec` | Handles concurrency, labels, registration automatically |
| Error status codes | String matching/parsing | `google.golang.org/grpc/codes` | Type-safe constants, interop with gRPC ecosystem |

**Key insight:** Feast already has robust error handling and observability infrastructure. Don't recreate patterns - extend existing helpers.

## Common Pitfalls

### Pitfall 1: Validating After Mutation
**What goes wrong:** Checking for missing defaults after already applying some defaults causes inconsistent state
**Why it happens:** Natural to validate inline during iteration
**How to avoid:** Use two-pass approach - validate ALL, then apply ALL
**Warning signs:** Tests show partial default application before error

### Pitfall 2: Wrong gRPC Status Code
**What goes wrong:** Using codes.Internal or codes.FailedPrecondition instead of codes.InvalidArgument
**Why it happens:** Missing default feels like server error, not client error
**How to avoid:** Missing defaults are client's responsibility - always use InvalidArgument
**Warning signs:** Error appears as 500 instead of 400 in HTTP layer

### Pitfall 3: Logging at Wrong Level
**What goes wrong:** Using Info() or Warn() for default applications floods logs
**Why it happens:** Defaulting feels important enough to log prominently
**How to avoid:** Debug() for successful operations (frequent), Error() only for failures
**Warning signs:** Production logs contain thousands of default messages

### Pitfall 4: Metric Cardinality Explosion
**What goes wrong:** Including entity values or timestamps in metric labels creates unbounded cardinality
**Why it happens:** Wanting detailed per-request tracking
**How to avoid:** Only label by feature_view and feature_name (low cardinality)
**Warning signs:** Prometheus memory usage grows unbounded

### Pitfall 5: Inconsistent Range vs Regular Handling
**What goes wrong:** STRICT mode logic differs between TransposeFeatureRowsIntoColumns and TransposeRangeFeatureRowsIntoColumns
**Why it happens:** Copying logic manually instead of sharing validation
**How to avoid:** Use identical validation logic structure in both functions
**Warning signs:** Tests pass for regular FVs but fail for sorted FVs

### Pitfall 6: Not Preserving Sort Order
**What goes wrong:** Error checking disrupts sort order in range queries
**Why it happens:** Adding validation loops that don't respect sorted iteration
**How to avoid:** Validate in the same iteration order as value processing
**Warning signs:** Range query results return out-of-order even without errors

## Code Examples

Verified patterns from existing codebase:

### Error Creation (InvalidArgument)
```go
// Source: go/internal/feast/errors/grpc_error.go:25-27
import "github.com/feast-dev/feast/go/internal/feast/errors"

return nil, errors.GrpcInvalidArgumentErrorf(
    "feature '%s' in feature view '%s' has NULL value but no default defined (use_defaults=STRICT)",
    featureName, featureViewName)
```

### Zerolog Debug Logging
```go
// Source: go/internal/feast/onlineserving/serving.go:22 (import)
import "github.com/rs/zerolog/log"

log.Debug().
    Str("feature_view", featureViewName).
    Str("feature_name", featureName).
    Interface("default_value", defaultVal).
    Msg("Applied default value")
```

### Prometheus Counter Registration
```go
// Source: Pattern from go/internal/feast/server/grpc_server.go:234-235
package onlineserving

import "github.com/prometheus/client_golang/prometheus"

var featureDefaultsApplied = prometheus.NewCounterVec(
    prometheus.CounterOpts{
        Name: "feature_defaults_applied_total",
        Help: "Total number of times default values were applied to features",
    },
    []string{"feature_view", "feature_name"},
)

func init() {
    prometheus.MustRegister(featureDefaultsApplied)
}
```

### Increment Counter with Labels
```go
// After successfully applying default:
featureDefaultsApplied.WithLabelValues(featureViewName, featureName).Inc()
```

### Test Case Structure for STRICT Mode
```go
// Source: Pattern from go/internal/feast/onlineserving/serving_test.go:1888-1987
{
    name:             "STRICT mode with NOT_FOUND and default exists",
    useDefaults:      serving.UseDefaultsMode_USE_DEFAULTS_STRICT,
    hasDefault:       true,
    defaultValue:     &types.Value{Val: &types.Value_DoubleVal{DoubleVal: 42.0}},
    values:           []interface{}{nil},
    statuses:         []serving.FieldStatus{serving.FieldStatus_NOT_FOUND},
    expectedValues:   []interface{}{42.0},
    expectedStatuses: []serving.FieldStatus{serving.FieldStatus_PRESENT},
},
{
    name:             "STRICT mode with NOT_FOUND and no default - should error",
    useDefaults:      serving.UseDefaultsMode_USE_DEFAULTS_STRICT,
    hasDefault:       false,
    defaultValue:     nil,
    values:           []interface{}{nil},
    statuses:         []serving.FieldStatus{serving.FieldStatus_NOT_FOUND},
    expectError:      true,
    errorContains:    "no default defined",
},
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| No default support | OFF, FLEXIBLE, STRICT modes | Phases 3-5 (2026) | Clients can enforce default value contracts |
| Silent NULL returns | Explicit defaulting modes | Phases 3-5 | Clearer NULL vs missing-data semantics |
| No observability | Debug logs + Prometheus metrics | Phase 5 | Track default application patterns |

**Deprecated/outdated:**
- USE_DEFAULTS_UNSPECIFIED: Behaves as OFF but deprecated - clients should explicitly choose OFF

## Open Questions

1. **Should STRICT mode fail on first missing default or collect all missing defaults?**
   - What we know: gRPC errors support single messages, not multi-error responses
   - What's unclear: User preference for detailed vs fast-fail errors
   - Recommendation: Fail on first missing default (simpler, faster). If users need batch validation, that's a separate feature.

2. **Should metrics track per-entity-row or aggregate?**
   - What we know: High cardinality kills Prometheus
   - What's unclear: Value of per-row tracking
   - Recommendation: Aggregate only - track feature_view + feature_name, ignore entity keys

3. **Should OUTSIDE_MAX_AGE trigger defaults in STRICT mode?**
   - What we know: Phases 3-4 decided to exclude OUTSIDE_MAX_AGE from defaulting
   - What's unclear: Should STRICT honor this or treat stale data as NULL?
   - Recommendation: Follow Phase 3-4 decision - exclude OUTSIDE_MAX_AGE. Value exists but is stale, not missing.

## Sources

### Primary (HIGH confidence)
- `/Users/vbhagwat/feast/go/internal/feast/onlineserving/serving.go` - Existing FLEXIBLE implementation (lines 817-826, 982-991)
- `/Users/vbhagwat/feast/go/internal/feast/errors/grpc_error.go` - Error creation patterns
- `/Users/vbhagwat/feast/protos/feast/serving/ServingService.proto` - UseDefaultsMode enum definition (lines 116-121)
- `/Users/vbhagwat/feast/go.mod` - Dependency versions for prometheus, zerolog
- `/Users/vbhagwat/feast/go/internal/feast/onlineserving/serving_test.go` - Test patterns for defaulting (lines 1888-1987)

### Secondary (MEDIUM confidence)
- Prior phase decisions (D008-D010, D014-D017) - Defaulting scope and behavior contracts

### Tertiary (LOW confidence)
- None - all findings verified against codebase

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - all dependencies already present and actively used
- Architecture: HIGH - patterns verified in existing code
- Pitfalls: MEDIUM - inferred from Go/gRPC best practices, not Feast-specific docs

**Research date:** 2026-03-02
**Valid until:** 2026-04-02 (30 days - stable Go ecosystem, existing codebase patterns)
