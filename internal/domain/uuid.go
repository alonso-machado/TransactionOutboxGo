package domain

import "github.com/google/uuid"

// ParseOptionalUUID converts an optional string pointer into an optional
// *uuid.UUID: a nil or empty input yields (nil, nil), a valid string yields
// the parsed UUID, and a malformed string yields an error. It is the single
// home for the "optional UUID at a boundary" conversion shared by the HTTP
// handler (payerId/recipientId in the inbound DTO) and the consumer use-case
// (the same fields in the outbox payload), so both decode them identically.
func ParseOptionalUUID(s *string) (*uuid.UUID, error) {
	if s == nil || *s == "" {
		return nil, nil
	}
	id, err := uuid.Parse(*s)
	if err != nil {
		return nil, err
	}
	return &id, nil
}
