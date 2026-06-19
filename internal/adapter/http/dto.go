package handler

// PaymentRequestDTO is the inbound HTTP body for POST / PUT / PATCH /api/v1/payments.
// It mirrors the TypeScript PaymentRequestDTO contract with one deliberate change:
// Amount is int64 in minor units (e.g. cents) instead of float64 to prevent
// floating-point precision loss in financial calculations.
//
// TypeScript equivalent:
//
//	export interface PaymentRequestDTO {
//	    recipientId: string;  // UUID of the payment recipient
//	    amount:      number;  // ⚠ use minor units (cents) — 4250 = $42.50 USD
//	    payerId:     string;  // UUID of the payer
//	}
type PaymentRequestDTO struct {
	RecipientID string `json:"recipientId" binding:"required,uuid"`
	Amount      int64  `json:"amount"      binding:"required,gt=0"`
	PayerID     string `json:"payerId"     binding:"required,uuid"`
}

// PaymentResponseDTO is the 202 Accepted response body.
// PaymentID is the UUID v7 primary key generated for the new Payment entity.
// IdempotencyKey is the sha256 dedup key stored in the outbox — clients can
// use it to correlate retries.
type PaymentResponseDTO struct {
	PaymentID      string `json:"paymentId"`
	IdempotencyKey string `json:"idempotencyKey"`
	Status         string `json:"status"` // "accepted" | "duplicate"
}
