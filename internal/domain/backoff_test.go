package domain

import (
	"testing"
	"time"
)

func TestBackoff_WithinComputedCeiling(t *testing.T) {
	base := 100 * time.Millisecond
	cap := 10 * time.Second
	// For retryCount n the (pre-jitter) ceiling is min(base*2^n, cap); full
	// jitter then picks a value in (0, ceiling]. Assert every draw stays in
	// that window across a range of counts.
	for n := 0; n < 8; n++ {
		ceiling := base * time.Duration(1<<n)
		if ceiling > cap {
			ceiling = cap
		}
		for i := 0; i < 100; i++ {
			d := Backoff(n, base, cap)
			if d <= 0 {
				t.Fatalf("retryCount=%d: backoff must be positive, got %v", n, d)
			}
			if d > ceiling {
				t.Fatalf("retryCount=%d: backoff %v exceeds ceiling %v", n, d, ceiling)
			}
		}
	}
}

func TestBackoff_NegativeRetryCountTreatedAsZero(t *testing.T) {
	base := 100 * time.Millisecond
	cap := 10 * time.Second
	for i := 0; i < 100; i++ {
		d := Backoff(-5, base, cap)
		if d <= 0 || d > base {
			t.Fatalf("negative retryCount should behave like 0 (ceiling=base=%v), got %v", base, d)
		}
	}
}

func TestBackoff_LargeRetryCountClampsToCap(t *testing.T) {
	base := time.Second
	cap := 5 * time.Second
	// A retryCount well past the shift guard (62) must not overflow; the
	// ceiling is the cap, so every draw is in (0, cap].
	for i := 0; i < 100; i++ {
		d := Backoff(1000, base, cap)
		if d <= 0 || d > cap {
			t.Fatalf("large retryCount should clamp to cap %v, got %v", cap, d)
		}
	}
}

func TestBackoff_ZeroCapReturnsZero(t *testing.T) {
	if d := Backoff(3, time.Second, 0); d != 0 {
		t.Fatalf("a non-positive cap should yield 0, got %v", d)
	}
}
