package handler

import "errors"

// UpdateHolderRequestDTO is the PATCH /api/v1/tickets/{id}/holder request
// body — name only (no age field; dropped from this endpoint's scope).
type UpdateHolderRequestDTO struct {
	Name string `json:"name"`
}

func (d UpdateHolderRequestDTO) Validate() error {
	if d.Name == "" {
		return errors.New("name is required")
	}
	return nil
}

type UpdateHolderResponseDTO struct {
	TicketID  string `json:"ticketId"`
	BuyerName string `json:"buyerName"`
}
