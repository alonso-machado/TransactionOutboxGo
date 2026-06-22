package ratelimit

import (
	"testing"
	"time"
)

func TestInMemoryStore_AdmitsBurstThenLeaksAtRate(t *testing.T) {
	s := NewInMemoryStore(time.Minute)
	now := time.Now()
	const rate, burst = 10.0, 5

	for i := 0; i < burst; i++ {
		allowed, retryAfter := s.Allow("k", rate, burst, now)
		if !allowed {
			t.Fatalf("request %d: expected admit within burst, got rejected (retryAfter=%v)", i, retryAfter)
		}
	}

	allowed, retryAfter := s.Allow("k", rate, burst, now)
	if allowed {
		t.Fatalf("expected rejection once burst is exhausted")
	}
	if retryAfter <= 0 {
		t.Fatalf("expected positive retryAfter, got %v", retryAfter)
	}

	later := now.Add(time.Duration(float64(time.Second) / rate))
	allowed, _ = s.Allow("k", rate, burst, later)
	if !allowed {
		t.Fatalf("expected admit after waiting one leak interval")
	}
}

func TestInMemoryStore_RetryAfterMath(t *testing.T) {
	s := NewInMemoryStore(time.Minute)
	now := time.Now()
	const rate, burst = 2.0, 1

	if allowed, _ := s.Allow("k", rate, burst, now); !allowed {
		t.Fatalf("expected first request admitted")
	}
	_, retryAfter := s.Allow("k", rate, burst, now)
	// overflow = level(1) + 1 - burst(1) = 1; retrySeconds = ceil(1/2) = 1s.
	if retryAfter != time.Second {
		t.Fatalf("expected retryAfter=1s, got %v", retryAfter)
	}
}

func TestInMemoryStore_PerKeyIsolation(t *testing.T) {
	s := NewInMemoryStore(time.Minute)
	now := time.Now()
	const rate, burst = 1.0, 1

	if allowed, _ := s.Allow("a", rate, burst, now); !allowed {
		t.Fatalf("expected key a admitted")
	}
	if allowed, _ := s.Allow("a", rate, burst, now); allowed {
		t.Fatalf("expected key a rejected on second request")
	}
	if allowed, _ := s.Allow("b", rate, burst, now); !allowed {
		t.Fatalf("expected key b unaffected by key a's bucket")
	}
}

func TestInMemoryStore_JanitorEvictsIdleKeys(t *testing.T) {
	s := NewInMemoryStore(10 * time.Millisecond)
	now := time.Now()
	if allowed, _ := s.Allow("k", 1, 1, now); !allowed {
		t.Fatalf("expected initial admit")
	}

	s.evict(now.Add(20 * time.Millisecond))

	s.mu.Lock()
	_, exists := s.buckets["k"]
	s.mu.Unlock()
	if exists {
		t.Fatalf("expected idle key to be evicted")
	}
}
