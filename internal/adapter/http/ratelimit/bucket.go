// Package ratelimit implements a per-key leaky-bucket meter used to throttle
// HTTP clients by IP. The algorithm is pure (no framework dependency) so it
// is unit-testable in isolation from Gin.
package ratelimit

import (
	"math"
	"sync"
	"time"
)

// BucketStore tracks per-key leaky-bucket state. The in-memory implementation
// below is per-pod only; a Redis-backed implementation could satisfy this
// same interface for a globally-accurate limit across replicas.
type BucketStore interface {
	// Allow reports whether a request for key is admitted under the given
	// leak rate (requests/second) and burst capacity. It returns the
	// retryAfter duration to wait before the next request would be admitted
	// when the request is rejected (zero when allowed).
	Allow(key string, rate float64, burst int, now time.Time) (allowed bool, retryAfter time.Duration)
}

type bucket struct {
	level    float64
	lastLeak time.Time
}

// InMemoryStore is a BucketStore guarded by a mutex, with a janitor that
// evicts buckets idle longer than ttl so the map cannot grow unbounded.
type InMemoryStore struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	ttl     time.Duration
}

// NewInMemoryStore creates a store whose janitor evicts buckets idle longer
// than ttl. Call Janitor in a goroutine to run the eviction loop.
func NewInMemoryStore(ttl time.Duration) *InMemoryStore {
	return &InMemoryStore{
		buckets: make(map[string]*bucket),
		ttl:     ttl,
	}
}

func (s *InMemoryStore) Allow(key string, rate float64, burst int, now time.Time) (bool, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.buckets[key]
	if !ok {
		b = &bucket{lastLeak: now}
		s.buckets[key] = b
	}

	elapsed := now.Sub(b.lastLeak).Seconds()
	if elapsed > 0 {
		leaked := elapsed * rate
		b.level -= leaked
		if b.level < 0 {
			b.level = 0
		}
		b.lastLeak = now
	}

	if b.level+1 > float64(burst) {
		overflow := b.level + 1 - float64(burst)
		retrySeconds := math.Ceil(overflow / rate)
		return false, time.Duration(retrySeconds) * time.Second
	}

	b.level++
	return true, 0
}

// Janitor runs until ctxDone is closed, periodically evicting buckets idle
// longer than the store's ttl.
func (s *InMemoryStore) Janitor(stop <-chan struct{}, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case now := <-ticker.C:
			s.evict(now)
		}
	}
}

func (s *InMemoryStore) evict(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, b := range s.buckets {
		if now.Sub(b.lastLeak) > s.ttl {
			delete(s.buckets, k)
		}
	}
}
