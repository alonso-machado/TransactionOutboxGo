package rabbitmq

import "testing"

func TestWithAMQPS(t *testing.T) {
	cases := []struct {
		name string
		url  string
		tls  bool
		want string
	}{
		{"tls upgrades amqp scheme", "amqp://guest:guest@host:5672/", true, "amqps://guest:guest@host:5672/"},
		{"tls leaves already-amqps untouched", "amqps://host:5671/", true, "amqps://host:5671/"},
		{"no tls leaves amqp untouched", "amqp://host:5672/", false, "amqp://host:5672/"},
		{"tls leaves non-amqp scheme untouched", "amqps://host/", false, "amqps://host/"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := withAMQPS(tc.url, tc.tls); got != tc.want {
				t.Fatalf("withAMQPS(%q, %v) = %q, want %q", tc.url, tc.tls, got, tc.want)
			}
		})
	}
}

func TestIsValidMethod(t *testing.T) {
	for _, m := range Methods {
		if !IsValidMethod(m) {
			t.Errorf("expected %q to be a valid method", m)
		}
	}
	if IsValidMethod("CARD") {
		t.Error("expected an unknown method to be rejected")
	}
}

func TestMethodForQueue(t *testing.T) {
	if m, ok := MethodForQueue(QueueFor("PIX")); !ok || m != "PIX" {
		t.Fatalf("MethodForQueue(%q) = %q,%v; want PIX,true", QueueFor("PIX"), m, ok)
	}
	if _, ok := MethodForQueue("payments.unknown.queue"); ok {
		t.Fatal("expected an unmapped queue name to return ok=false")
	}
}

func TestQueueNameHelpers(t *testing.T) {
	cases := map[string]string{
		QueueFor("PIX"):         "payments.pix.queue",
		DLQFor("PIX"):           "payments.pix.dlq",
		RetryQueueFor("PIX"):    "payments.pix.retry",
		RoutingKeyFor("PIX"):    "payment.pix",
		DLXRoutingKeyFor("PIX"): "payment.pix.dead",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}
