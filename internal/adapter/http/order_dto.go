package handler

import (
	"errors"
	"fmt"

	rmq "github.com/alonsomachado/transaction-outbox-go/internal/infrastructure/rabbitmq"
)

// VenueDTO is the order payload's "venue" sibling object — upserted into
// the events DB's locations table by order-consumer-worker.
type VenueDTO struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	City string `json:"city"`
}

// TicketItemDTO is one requested ticket line.
type TicketItemDTO struct {
	ID       string  `json:"id"`
	Section  string  `json:"section"`
	Row      string  `json:"row"`
	Seat     string  `json:"seat"`
	Price    float64 `json:"price"`
	Currency string  `json:"currency"`
}

// CustomerDTO is the buyer's contact info.
type CustomerDTO struct {
	Name     string `json:"name"`
	Email    string `json:"email"`
	Document string `json:"document"`
}

// OrderRequestDTO is the inbound body for POST /api/v1/orders: a request
// for tickets to an Event, categorized by (eventType, eventSubtype) — the
// pair that drives RabbitMQ routing (internal/infrastructure/rabbitmq) all
// the way through order-consumer-worker and fulfillment-consumer-worker.
type OrderRequestDTO struct {
	SourceOrderID string          `json:"sourceOrderId"`
	EventType     string          `json:"eventType"`
	EventSubtype  string          `json:"eventSubtype"`
	EventID       string          `json:"eventId"`
	EventName     string          `json:"eventName,omitempty"`
	Venue         VenueDTO        `json:"venue"`
	Tickets       []TicketItemDTO `json:"tickets"`
	Customer      CustomerDTO     `json:"customer"`
}

// Validate checks the minimum an order must carry to be processable:
// identity fields, at least one ticket with valid price/currency, all
// sharing one currency (mixed-currency orders aren't supported), and buyer
// contact info.
func (d OrderRequestDTO) Validate() error {
	switch {
	case d.SourceOrderID == "":
		return errors.New("sourceOrderId is required")
	case d.EventType == "":
		return errors.New("eventType is required")
	case d.EventSubtype == "":
		return errors.New("eventSubtype is required")
	case d.EventID == "":
		return errors.New("eventId is required")
	case d.Venue.ID == "":
		return errors.New("venue.id is required")
	case len(d.Tickets) == 0:
		return errors.New("tickets must contain at least one ticket")
	case d.Customer.Email == "":
		return errors.New("customer.email is required")
	}

	currency := d.Tickets[0].Currency
	for i, t := range d.Tickets {
		switch {
		case t.ID == "":
			return fmt.Errorf("tickets[%d].id is required", i)
		case t.Price <= 0:
			return fmt.Errorf("tickets[%d].price must be > 0", i)
		case len(t.Currency) != 3:
			return fmt.Errorf("tickets[%d].currency must be a 3-letter ISO 4217 code", i)
		case t.Currency != currency:
			return fmt.Errorf("tickets[%d].currency %q does not match order currency %q (mixed-currency orders are not supported)", i, t.Currency, currency)
		}
	}
	return nil
}

// ValidateEventType rejects an (eventType, eventSubtype) with no bound
// RabbitMQ queue — see rmq.EventTypes' doc comment for why: publishing to a
// pair with no bound queue would be a topic-exchange black hole.
func (d OrderRequestDTO) ValidateEventType() error {
	if !rmq.IsValidEventType(d.EventType, d.EventSubtype) {
		return fmt.Errorf("eventType/eventSubtype %q/%q is not supported (see the registry in internal/infrastructure/rabbitmq)", d.EventType, d.EventSubtype)
	}
	return nil
}

// OrderResponseDTO is the 201 Created response body.
type OrderResponseDTO struct {
	OrderID        string `json:"orderId"`
	IdempotencyKey string `json:"idempotencyKey"`
	Status         string `json:"status"` // "accepted" | "duplicate"
}
