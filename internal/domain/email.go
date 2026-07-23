package domain

// EmailSender is the outbound port for delivering an issued ticket by
// email — the only place the domain touches email at all.
// usecase/notification.SendTicketNotification calls Send once per ticket,
// either synchronously right after issuance (fulfillment-consumer-worker)
// or from a retry (notification-retry-cron). Unlike PaymentGateway, there is
// no inbound/webhook half: email delivery has no confirmation callback in
// this system's scope, it's outbound-only.
type EmailSender interface {
	Send(req EmailRequest) (*EmailResult, error)
}

// EmailRequest is what SendTicketNotification asks the sender to deliver.
// Attachment is the rendered QR PNG.
type EmailRequest struct {
	ToEmail               string
	ToName                string
	Subject               string
	BodyText              string
	AttachmentName        string
	AttachmentContentType string // e.g. "image/png"
	Attachment            []byte
}

// EmailResult is what the sender hands back on success. ProviderMessageID
// is opaque (e.g. an SMTP message-id) — kept for logging/parity, not
// currently joined back to anything.
type EmailResult struct {
	ProviderMessageID string
}
