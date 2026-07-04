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

func TestIsValidEventType(t *testing.T) {
	for eventType, subtypes := range EventTypes {
		for _, subtype := range subtypes {
			if !IsValidEventType(eventType, subtype) {
				t.Errorf("expected (%q, %q) to be valid", eventType, subtype)
			}
		}
	}
	if IsValidEventType("CONCERT", "OPERA") {
		t.Error("expected an unknown subtype to be rejected")
	}
	if IsValidEventType("UNKNOWN", "ROCK") {
		t.Error("expected an unknown event type to be rejected")
	}
}

func TestParseQueueName(t *testing.T) {
	queue := QueueFor(OrderStream, "CONCERT", "ROCK")
	stream, eventType, eventSubtype, ok := ParseQueueName(queue)
	if !ok || stream != OrderStream || eventType != "CONCERT" || eventSubtype != "ROCK" {
		t.Fatalf("ParseQueueName(%q) = %v,%q,%q,%v; want OrderStream,CONCERT,ROCK,true", queue, stream, eventType, eventSubtype, ok)
	}
	if _, _, _, ok := ParseQueueName("events.unknown.unknown.queue"); ok {
		t.Fatal("expected an unmapped queue name to return ok=false")
	}
}

func TestQueueNameHelpers(t *testing.T) {
	cases := map[string]string{
		QueueFor(OrderStream, "CONCERT", "ROCK"):               "events.concert.rock.queue",
		DLQFor(OrderStream, "CONCERT", "ROCK"):                 "events.concert.rock.dlq",
		RetryQueueFor(OrderStream, "CONCERT", "ROCK"):          "events.concert.rock.retry",
		RoutingKeyFor(OrderStream, "CONCERT", "ROCK"):          "order.concert.rock",
		DLXRoutingKeyFor(OrderStream, "CONCERT", "ROCK"):       "order.concert.rock.dead",
		QueueFor(PaymentEventStream, "CONCERT", "ROCK"):        "payments.concert.rock.queue",
		DLQFor(PaymentEventStream, "CONCERT", "ROCK"):          "payments.concert.rock.dlq",
		RetryQueueFor(PaymentEventStream, "CONCERT", "ROCK"):   "payments.concert.rock.retry",
		RoutingKeyFor(PaymentEventStream, "CONCERT", "ROCK"):   "payment.concert.rock",
		DLXRoutingKeyFor(PaymentEventStream, "CONCERT", "ROCK"): "payment.concert.rock.dead",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}

func TestStreamForAggregateType(t *testing.T) {
	if s, ok := StreamForAggregateType("order"); !ok || s != OrderStream {
		t.Fatalf("StreamForAggregateType(order) = %v,%v; want OrderStream,true", s, ok)
	}
	if s, ok := StreamForAggregateType("payment_event"); !ok || s != PaymentEventStream {
		t.Fatalf("StreamForAggregateType(payment_event) = %v,%v; want PaymentEventStream,true", s, ok)
	}
	if _, ok := StreamForAggregateType("unknown"); ok {
		t.Fatal("expected an unknown aggregate type to return ok=false")
	}
}
