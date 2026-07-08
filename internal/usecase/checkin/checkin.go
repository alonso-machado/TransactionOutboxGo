// Package checkin holds the CheckIn use-case: tickets-api's handling of
// POST /api/v1/checkin. It verifies a scanned ticket's HMAC signature
// against the stored row (never the request's fields in isolation, so a
// QR copied from a different/voided ticket can't be replayed), optionally
// enforces the authenticated staff member's venue scoping, and flips the
// ticket VALID -> CHECKED_IN. Idempotent: checking in an already-checked-in
// ticket is a safe no-op outcome, not an error.
package checkin

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/ticketqr"
	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/alonsomachado/transaction-outbox-go/internal/observability"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("usecase/checkin")

type Outcome string

const (
	OutcomeCheckedIn        Outcome = "CHECKED_IN"         // fresh, this call did it
	OutcomeAlreadyCheckedIn Outcome = "ALREADY_CHECKED_IN" // idempotent no-op
	OutcomeInvalidSignature Outcome = "INVALID_SIGNATURE"
	OutcomeNotIssued        Outcome = "NOT_ISSUED" // RESERVED/VOID — never became VALID
	OutcomeWrongVenue       Outcome = "WRONG_VENUE"
)

// ErrTicketNotFound is returned when TicketID doesn't resolve to any row —
// the handler maps this to 404, distinct from every Outcome above (which
// all describe a ticket that does exist).
var ErrTicketNotFound = errors.New("checkin: ticket not found")

type Request struct {
	TicketID       uuid.UUID
	ValidationCode string
	Signature      string
	// StaffLocationID is the authenticated staff member's venue scope
	// (nil = unscoped, can check in anywhere) — populated by the handler
	// from the staffauth middleware's context, never trusted from the
	// request body itself.
	StaffLocationID *uuid.UUID
}

type Response struct {
	Outcome Outcome
	Ticket  *domain.Ticket // populated on CHECKED_IN/ALREADY_CHECKED_IN so staff sees buyer/section/row/seat
}

type CheckIn struct {
	ticketRepo    domain.TicketRepository
	eventRepo     domain.EventRepository
	secret        string
	attemptsTotal metric.Int64Counter
}

func New(ticketRepo domain.TicketRepository, eventRepo domain.EventRepository, secret string) *CheckIn {
	meter := otel.GetMeterProvider().Meter("usecase/checkin")
	return &CheckIn{
		ticketRepo:    ticketRepo,
		eventRepo:     eventRepo,
		secret:        secret,
		attemptsTotal: observability.Int64Counter(meter, "checkin.attempts_total"),
	}
}

func (uc *CheckIn) Execute(ctx context.Context, req Request) (*Response, error) {
	ctx, span := tracer.Start(ctx, "checkin.execute", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	span.SetAttributes(attribute.String("ticket_id", req.TicketID.String()))

	t, err := uc.ticketRepo.FindByID(ctx, req.TicketID)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrTicketNotFound, err)
	}

	if !ticketqr.Verify(t.ID.String(), t.ValidationCode, req.Signature, uc.secret) || req.ValidationCode != t.ValidationCode {
		return uc.recordOutcome(ctx, span, OutcomeInvalidSignature, nil), nil
	}

	if req.StaffLocationID != nil {
		event, err := uc.eventRepo.FindByID(ctx, t.EventID)
		if err != nil {
			return nil, fmt.Errorf("find event for ticket %s: %w", t.ID, err)
		}
		if event.LocationID != *req.StaffLocationID {
			return uc.recordOutcome(ctx, span, OutcomeWrongVenue, nil), nil
		}
	}

	switch t.Status {
	case domain.TicketStatusCheckedIn:
		return uc.recordOutcome(ctx, span, OutcomeAlreadyCheckedIn, t), nil
	case domain.TicketStatusReserved, domain.TicketStatusVoid:
		return uc.recordOutcome(ctx, span, OutcomeNotIssued, nil), nil
	}

	now := time.Now().UTC()
	alreadyCheckedIn, err := uc.ticketRepo.CheckIn(ctx, nil, t.ID, now)
	if err != nil {
		return nil, fmt.Errorf("check in ticket %s: %w", t.ID, err)
	}
	if alreadyCheckedIn {
		// Raced with a concurrent check-in between our FindByID read and
		// the CheckIn update — still a safe, idempotent outcome.
		return uc.recordOutcome(ctx, span, OutcomeAlreadyCheckedIn, t), nil
	}
	t.Status = domain.TicketStatusCheckedIn
	t.CheckedInAt = &now
	return uc.recordOutcome(ctx, span, OutcomeCheckedIn, t), nil
}

func (uc *CheckIn) recordOutcome(ctx context.Context, span trace.Span, outcome Outcome, t *domain.Ticket) *Response {
	span.SetAttributes(attribute.String("outcome", string(outcome)))
	uc.attemptsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", string(outcome))))
	return &Response{Outcome: outcome, Ticket: t}
}
