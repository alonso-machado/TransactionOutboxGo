// Package ticketqr implements domain.TicketQR: it derives a signed
// validation token for a ticket and renders it as a QR PNG.
package ticketqr

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/google/uuid"
	qrcode "github.com/skip2/go-qrcode"
)

// qrSizePx is the rendered PNG's width/height in pixels.
const qrSizePx = 256

type Generator struct {
	secret string
}

// New builds a Generator bound to secret (config.TicketSigningSecret) — the
// HMAC key every ticket's signature is computed with.
func New(secret string) *Generator {
	return &Generator{secret: secret}
}

// Generate derives a random validation code, signs (ticketID, validationCode)
// with HMAC-SHA256, packs a compact validation-token string as the QR
// payload, and renders it as a PNG.
func (g *Generator) Generate(ticketID uuid.UUID) (qrPNG []byte, qrContent, validationCode, signature string, err error) {
	vc, err := uuid.NewV7()
	if err != nil {
		return nil, "", "", "", fmt.Errorf("generate validation code: %w", err)
	}
	validationCode = vc.String()
	signature = sign(ticketID.String(), validationCode, g.secret)
	qrContent = fmt.Sprintf("ticket:%s:%s:%s", ticketID, validationCode, signature)

	png, err := qrcode.Encode(qrContent, qrcode.Medium, qrSizePx)
	if err != nil {
		return nil, "", "", "", fmt.Errorf("encode qr png: %w", err)
	}
	return png, qrContent, validationCode, signature, nil
}

func sign(ticketID, validationCode, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ticketID + ":" + validationCode))
	return hex.EncodeToString(mac.Sum(nil))
}

// Verify recomputes the signature for (ticketID, validationCode) and
// compares it against signature in constant time — for a future ticket-
// validation endpoint and the integration test suite.
func Verify(ticketID, validationCode, signature, secret string) bool {
	expected := sign(ticketID, validationCode, secret)
	return hmac.Equal([]byte(expected), []byte(signature))
}
