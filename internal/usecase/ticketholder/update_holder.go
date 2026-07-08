// Package ticketholder holds the UpdateHolder use-case: tickets-api's
// handling of PATCH /api/v1/tickets/{id}/holder — correcting a ticket's
// buyer name (transfers/typos happen). No staff auth (confirmed with the
// user); the endpoint is protected by rate-limiting alone at the HTTP
// layer (internal/adapter/http/ratelimit).
package ticketholder

import (
	"context"
	"errors"
	"fmt"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/google/uuid"
)

// ErrTicketNotFound is mapped to 404 by the handler.
var ErrTicketNotFound = errors.New("ticketholder: ticket not found")

// ErrNotEditable is mapped to 409 Conflict by the handler — the ticket was
// never issued (RESERVED) or its reservation was released (VOID), so there
// is nothing to correct yet.
var ErrNotEditable = errors.New("ticketholder: ticket is not in an editable state")

type Request struct {
	TicketID uuid.UUID
	Name     string
}

type UpdateHolder struct {
	ticketRepo domain.TicketRepository
}

func New(ticketRepo domain.TicketRepository) *UpdateHolder {
	return &UpdateHolder{ticketRepo: ticketRepo}
}

// Execute allows correcting the buyer name on VALID and CHECKED_IN tickets
// — a typo caught after door check-in is still worth fixing for
// record-keeping, with no re-issuance/re-emailing/re-check-in implied.
// RESERVED/VOID tickets are rejected: nothing to correct yet, or the
// reservation was released.
func (uc *UpdateHolder) Execute(ctx context.Context, req Request) error {
	t, err := uc.ticketRepo.FindByID(ctx, req.TicketID)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrTicketNotFound, err)
	}

	switch t.Status {
	case domain.TicketStatusValid, domain.TicketStatusCheckedIn:
	default:
		return ErrNotEditable
	}

	if err := uc.ticketRepo.UpdateHolderName(ctx, nil, req.TicketID, req.Name); err != nil {
		return fmt.Errorf("update holder name for ticket %s: %w", req.TicketID, err)
	}
	return nil
}
