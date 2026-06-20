package handler

import (
	"encoding/json"
	"errors"
	"strings"
)

// maskPAN rewrites the "cardNumber" field inside raw (a card sibling object,
// e.g. {"cardNumber":"4111111111111111","cardType":"CREDIT","cardIssuer":"VISA"})
// so only the last 4 digits survive, e.g. "************1111". The full PAN
// must never be stored, published, or logged — this runs at the HTTP
// boundary before methodDetails reaches ingest.
func maskPAN(raw json.RawMessage) (json.RawMessage, error) {
	var card CardDetailsDTO
	if err := json.Unmarshal(raw, &card); err != nil {
		return nil, err
	}
	if !isDigits(card.CardNumber) {
		return nil, errors.New("card.cardNumber must contain only digits")
	}
	last4 := card.CardNumber[len(card.CardNumber)-4:]
	card.CardNumber = strings.Repeat("*", len(card.CardNumber)-4) + last4
	return json.Marshal(card)
}
