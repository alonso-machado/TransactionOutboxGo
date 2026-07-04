package handler

import (
	"io"
	"net/http"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/alonsomachado/transaction-outbox-go/internal/domain/pii"
	"github.com/alonsomachado/transaction-outbox-go/internal/usecase/webhook"
	"github.com/gin-gonic/gin"
)

// webhookHeaders is the allowlist of inbound headers forwarded to
// PaymentGateway.VerifyWebhook — e.g. Stripe-Signature. A gateway that
// doesn't need any of these (the fake sandbox) simply ignores them.
var webhookHeaders = []string{
	"Stripe-Signature",
}

type WebhookHandler struct {
	gateway  domain.PaymentGateway
	receive  *webhook.ReceivePaymentEvent
	provider string
}

// NewWebhookHandler binds a handler to exactly one configured provider
// (config.PaymentProvider) — the :provider path segment is checked against
// it, not used to select a gateway at request time, since the gateway
// adapter is wired once at process startup (cmd/ingestion-api/main.go).
func NewWebhookHandler(gateway domain.PaymentGateway, receive *webhook.ReceivePaymentEvent, provider string) *WebhookHandler {
	return &WebhookHandler{gateway: gateway, receive: receive, provider: provider}
}

// Handle verifies and lands a payment-gateway webhook delivery.
//
//	@Summary		Receive a payment-gateway webhook
//	@Description	Verifies the gateway's signature and lands the confirmation in payment_event_outbox
//	@Description	for asynchronous relay to fulfillment-consumer-worker. Idempotent on the gateway's own event id.
//	@Tags			webhooks
//	@Accept			json
//	@Produce		json
//	@Param			provider	path		string	true	"Configured gateway provider name (e.g. stripe, fake)"
//	@Success		200			{object}	map[string]string
//	@Failure		400			{object}	map[string]string
//	@Failure		404			{object}	map[string]string
//	@Failure		500			{object}	map[string]string
//	@Router			/api/v1/webhooks/payments/{provider} [post]
func (h *WebhookHandler) Handle(c *gin.Context) {
	if provider := c.Param("provider"); provider != h.provider {
		c.JSON(http.StatusNotFound, gin.H{"error": "unknown provider"})
		return
	}

	// Read the RAW body — never gin's JSON binding — because gateway
	// signature verification (e.g. Stripe's webhook.ConstructEvent) is
	// computed over the exact bytes the gateway signed; any re-encoding
	// would break it.
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cannot read body"})
		return
	}

	headers := make(map[string]string, len(webhookHeaders))
	for _, name := range webhookHeaders {
		if v := c.GetHeader(name); v != "" {
			headers[name] = v
		}
	}

	event, err := h.gateway.VerifyWebhook(body, headers)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": pii.Redact(err.Error())})
		return
	}

	if _, err := h.receive.Execute(c.Request.Context(), webhook.Request{Provider: h.provider, Event: *event}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": pii.Redact(err.Error())})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "received"})
}
