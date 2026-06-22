package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/http/ratelimit"
)

func TestRouter_Healthz_NeverRateLimited(t *testing.T) {
	store := ratelimit.NewInMemoryStore(time.Minute)
	r := NewRouter(NewPaymentHandler(nil), "test", false, RouterConfig{
		RateLimitEnabled: true,
		RateLimitStore:   store,
		RateLimitRate:    1,
		RateLimitBurst:   1,
	})

	for i := 0; i < 5; i++ {
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("request %d: expected /healthz to always succeed, got %d", i, w.Code)
		}
	}
}

func TestRouter_RateLimit_429AfterBurstWithHeaders(t *testing.T) {
	store := ratelimit.NewInMemoryStore(time.Minute)
	r := NewRouter(NewPaymentHandler(nil), "test", false, RouterConfig{
		RateLimitEnabled: true,
		RateLimitStore:   store,
		RateLimitRate:    1,
		RateLimitBurst:   1,
	})

	post := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/payments", nil)
		req.RemoteAddr = "10.0.0.1:1234"
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}

	first := post()
	if first.Code == http.StatusTooManyRequests {
		t.Fatalf("expected first request within burst to be admitted, got 429")
	}

	second := post()
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("expected second request to be rate-limited, got %d", second.Code)
	}
	if second.Header().Get("Retry-After") == "" {
		t.Fatal("expected Retry-After header on 429 response")
	}
	if second.Header().Get("X-RateLimit-Remaining") != "0" {
		t.Fatalf("expected X-RateLimit-Remaining=0, got %q", second.Header().Get("X-RateLimit-Remaining"))
	}
}

func TestRouter_RateLimit_PerIPIsolation(t *testing.T) {
	store := ratelimit.NewInMemoryStore(time.Minute)
	r := NewRouter(NewPaymentHandler(nil), "test", false, RouterConfig{
		RateLimitEnabled: true,
		RateLimitStore:   store,
		RateLimitRate:    1,
		RateLimitBurst:   1,
	})

	postFrom := func(ip string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/payments", nil)
		req.RemoteAddr = ip + ":1234"
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}

	if w := postFrom("10.0.0.1"); w.Code == http.StatusTooManyRequests {
		t.Fatalf("expected first request from 10.0.0.1 to be admitted")
	}
	if w := postFrom("10.0.0.1"); w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected second request from 10.0.0.1 to be rate-limited, got %d", w.Code)
	}
	if w := postFrom("10.0.0.2"); w.Code == http.StatusTooManyRequests {
		t.Fatalf("expected first request from a different IP (10.0.0.2) to be unaffected")
	}
}

func TestRouter_RateLimit_SpoofedXFFIgnoredWithoutTrustedProxies(t *testing.T) {
	store := ratelimit.NewInMemoryStore(time.Minute)
	r := NewRouter(NewPaymentHandler(nil), "test", false, RouterConfig{
		TrustedProxies:   nil, // no proxies trusted -> c.ClientIP() must use RemoteAddr, not XFF
		RateLimitEnabled: true,
		RateLimitStore:   store,
		RateLimitRate:    1,
		RateLimitBurst:   1,
	})

	post := func(remoteAddr, xff string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/payments", nil)
		req.RemoteAddr = remoteAddr + ":1234"
		if xff != "" {
			req.Header.Set("X-Forwarded-For", xff)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}

	if w := post("10.0.0.1", ""); w.Code == http.StatusTooManyRequests {
		t.Fatalf("expected first request to be admitted")
	}
	// Same RemoteAddr, spoofed XFF claiming to be a different IP — with no
	// trusted proxies configured, this must still be bucketed as 10.0.0.1
	// and therefore rejected, not treated as a fresh IP.
	if w := post("10.0.0.1", "1.2.3.4"); w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected spoofed X-Forwarded-For to be ignored and the request rate-limited, got %d", w.Code)
	}
}

func TestRouter_RateLimit_Disabled_NeverRejects(t *testing.T) {
	r := NewRouter(NewPaymentHandler(nil), "test", false, RouterConfig{
		RateLimitEnabled: false,
	})

	for i := 0; i < 5; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/payments", nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code == http.StatusTooManyRequests {
			t.Fatalf("request %d: expected no rate limiting when disabled, got 429", i)
		}
	}
}
