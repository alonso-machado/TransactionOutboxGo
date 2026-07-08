package handler

import "errors"

// CheckinRequestDTO is the POST /api/v1/checkin request body — the three
// fields a scanning app splits out of the ticket's QR content string
// ("ticket:{ticketID}:{validationCode}:{signature}"). Splitting the raw QR
// string is the scanning client's concern, not this endpoint's.
type CheckinRequestDTO struct {
	TicketID       string `json:"ticketId"`
	ValidationCode string `json:"validationCode"`
	Signature      string `json:"signature"`
}

func (d CheckinRequestDTO) Validate() error {
	switch {
	case d.TicketID == "":
		return errors.New("ticketId is required")
	case d.ValidationCode == "":
		return errors.New("validationCode is required")
	case d.Signature == "":
		return errors.New("signature is required")
	}
	return nil
}

// CheckinTicketDTO is shown to door staff so they can visually compare the
// buyer's name against a shown ID — deliberately NOT redacted, see
// internal/domain/pii's doc comment and the Phase 8 plan's Part I.
type CheckinTicketDTO struct {
	BuyerName string `json:"buyerName"`
	Section   string `json:"section"`
	Row       string `json:"row"`
	Seat      string `json:"seat"`
}

type CheckinResponseDTO struct {
	Outcome string            `json:"outcome"`
	Ticket  *CheckinTicketDTO `json:"ticket,omitempty"`
}
