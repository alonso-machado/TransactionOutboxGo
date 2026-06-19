package messaging

import (
	"sort"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestAmqpHeaderCarrier_SetGet_RoundTrips(t *testing.T) {
	c := amqpHeaderCarrier(amqp.Table{})
	c.Set("traceparent", "00-abc-def-01")
	if got := c.Get("traceparent"); got != "00-abc-def-01" {
		t.Fatalf("expected round-tripped value, got %q", got)
	}
}

func TestAmqpHeaderCarrier_Get_MissingOrNonStringKey_ReturnsEmpty(t *testing.T) {
	c := amqpHeaderCarrier(amqp.Table{"count": int32(5)})
	if got := c.Get("missing"); got != "" {
		t.Fatalf("expected empty string for missing key, got %q", got)
	}
	if got := c.Get("count"); got != "" {
		t.Fatalf("expected empty string for non-string value, got %q", got)
	}
}

func TestAmqpHeaderCarrier_Keys_ReturnsAllKeys(t *testing.T) {
	c := amqpHeaderCarrier(amqp.Table{"a": "1", "b": "2"})
	keys := c.Keys()
	sort.Strings(keys)
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Fatalf("expected [a b], got %v", keys)
	}
}
