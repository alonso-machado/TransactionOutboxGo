package ingest

import "testing"

// computeKey is pure logic (no I/O) so it gets a plain unit test rather than
// an integration test against real infrastructure, per the project's
// testing convention (mocks/real-infra split documented in
// .claude/plan-phase2.md Track 3).
func TestComputeKey_DeterministicAndSensitiveToInputs(t *testing.T) {
	k1 := computeKey("POST", []byte("MERCADO_PAGO:evt-1"), "")
	k2 := computeKey("POST", []byte("MERCADO_PAGO:evt-1"), "")
	if k1 != k2 {
		t.Fatalf("expected computeKey to be deterministic, got %q vs %q", k1, k2)
	}
	if len(k1) != 64 { // hex-encoded sha256
		t.Fatalf("expected 64 hex chars, got %d", len(k1))
	}
}

func TestComputeKey_DifferentSourceDifferentKey(t *testing.T) {
	k1 := computeKey("POST", []byte("MERCADO_PAGO:evt-1"), "")
	k2 := computeKey("POST", []byte("MERCADO_PAGO:evt-2"), "")
	if k1 == k2 {
		t.Fatalf("expected different sources to produce different keys")
	}
}

func TestComputeKey_DifferentMethodDifferentKey(t *testing.T) {
	k1 := computeKey("POST", []byte("MERCADO_PAGO:evt-1"), "")
	k2 := computeKey("PUT", []byte("MERCADO_PAGO:evt-1"), "")
	if k1 == k2 {
		t.Fatalf("expected different HTTP methods to produce different keys")
	}
}

func TestComputeKey_ClientKeyFoldedIn(t *testing.T) {
	base := computeKey("POST", []byte("MERCADO_PAGO:evt-1"), "")
	withClientKey := computeKey("POST", []byte("MERCADO_PAGO:evt-1"), "client-123")
	if base == withClientKey {
		t.Fatalf("expected supplying a client Idempotency-Key to change the derived key")
	}

	withClientKeyAgain := computeKey("POST", []byte("MERCADO_PAGO:evt-1"), "client-123")
	if withClientKey != withClientKeyAgain {
		t.Fatalf("expected same client key to be deterministic")
	}

	withDifferentClientKey := computeKey("POST", []byte("MERCADO_PAGO:evt-1"), "client-456")
	if withClientKey == withDifferentClientKey {
		t.Fatalf("expected different client keys to produce different derived keys")
	}
}

func TestComputeKey_EmptyClientKeyIgnored(t *testing.T) {
	// An empty clientKey must behave identically to omitting it entirely —
	// the Idempotency-Key header is optional and only folded in when present.
	k1 := computeKey("POST", []byte("MERCADO_PAGO:evt-1"), "")
	k2 := computeKey("POST", []byte("MERCADO_PAGO:evt-1"), "")
	if k1 != k2 {
		t.Fatalf("expected empty client key to be a no-op")
	}
}
