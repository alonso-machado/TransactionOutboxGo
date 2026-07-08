// Package fake implements domain.EmailSender with no network calls — the
// default provider for local dev (make up) and the integration test
// suite, neither of which have a real SMTP server to send through.
package fake

import (
	"fmt"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
)

type Sender struct{}

func New() *Sender { return &Sender{} }

func (s *Sender) Send(req domain.EmailRequest) (*domain.EmailResult, error) {
	return &domain.EmailResult{
		ProviderMessageID: fmt.Sprintf("fake_%s_%s", req.ToEmail, req.Subject),
	}, nil
}
