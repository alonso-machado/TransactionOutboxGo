// Package emailsender selects the domain.EmailSender adapter by
// config.Config.EmailProvider — shared by fulfillment-consumer-worker
// (sends the ticket email synchronously right after issuance) and
// notification-retry-cron (retries any ticket whose email hasn't been sent
// yet), so the fake/smtp selection logic lives in exactly one place instead
// of being duplicated across both binaries' main.go.
package emailsender

import (
	"fmt"

	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/emailsender/fake"
	"github.com/alonsomachado/transaction-outbox-go/internal/adapter/emailsender/smtp"
	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
)

// New selects the domain.EmailSender adapter: "smtp" (real, stdlib
// net/smtp) or "fake"/"" (no network, the default).
func New(provider, smtpHost string, smtpPort int, smtpUsername, smtpPassword, smtpFromEmail, smtpFromName string) (domain.EmailSender, error) {
	switch provider {
	case "smtp":
		return smtp.New(smtpHost, smtpPort, smtpUsername, smtpPassword, smtpFromEmail, smtpFromName), nil
	case "fake", "":
		return fake.New(), nil
	default:
		return nil, fmt.Errorf("unknown EMAIL_PROVIDER %q", provider)
	}
}
