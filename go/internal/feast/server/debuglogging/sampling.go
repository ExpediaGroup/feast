package debuglogging

import "math/rand"

// ShouldSampleRequest rolls a random sample against rate. rate <= 0 never
// samples; rate >= 1 always samples.
func ShouldSampleRequest(rate float64) bool {
	if rate <= 0 {
		return false
	}
	if rate >= 1 {
		return true
	}
	return rand.Float64() < rate
}

// ShouldEmit combines the per-request predicate (Plan A) with the
// server-wide config + sampling (Plan B): emit if the caller explicitly
// flagged this request, OR the server-wide switch is on and this request
// was sampled. Either mechanism alone is sufficient — this is what lets
// operators choose which one to use.
func ShouldEmit(requestFlagged bool, cfg Config) bool {
	if requestFlagged {
		return true
	}
	if !cfg.Enabled {
		return false
	}
	return ShouldSampleRequest(cfg.SampleRate)
}
