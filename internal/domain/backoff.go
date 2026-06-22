package domain

import (
	"math/rand/v2"
	"time"
)

// Backoff computes an exponential-with-full-jitter delay for retryCount,
// shared by the outbox dispatcher's next_retry_at scheduling and the
// consumer's per-method *.retry queue TTL (Phase 5 Track 2.A) so both sides
// back off on the same schedule: min(base*2^retryCount, cap), then a full
// jitter (uniform random in [0, computed]) so many simultaneously-failing
// messages don't retry in lockstep.
func Backoff(retryCount int, base, cap time.Duration) time.Duration {
	if retryCount < 0 {
		retryCount = 0
	}
	// Cap the shift to avoid overflow for large retry counts.
	shift := retryCount
	if shift > 62 {
		shift = 62
	}
	d := base * time.Duration(1<<shift)
	if d <= 0 || d > cap {
		d = cap
	}
	if d <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(d)) + 1)
}
