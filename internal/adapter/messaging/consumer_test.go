package messaging

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRetryCountFromHeaders_MissingHeader_ReturnsZero(t *testing.T) {
	if got := retryCountFromHeaders(amqp.Table{}); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}

func TestRetryCountFromHeaders_SupportsAllIntegerTypes(t *testing.T) {
	cases := []amqp.Table{
		{retryCountHeader: int32(3)},
		{retryCountHeader: int64(3)},
		{retryCountHeader: int(3)},
	}
	for _, h := range cases {
		if got := retryCountFromHeaders(h); got != 3 {
			t.Errorf("retryCountFromHeaders(%v) = %d, want 3", h, got)
		}
	}
}

func TestRetryCountFromHeaders_UnknownType_ReturnsZero(t *testing.T) {
	if got := retryCountFromHeaders(amqp.Table{retryCountHeader: "not-a-number"}); got != 0 {
		t.Fatalf("expected 0 for unrecognized header type, got %d", got)
	}
}
