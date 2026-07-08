package handler

// OrderStatusResponseDTO is the GET /api/v1/orders/{id} response body.
// CheckoutURL is empty until order-consumer-worker has opened a checkout
// for this order — the client is expected to keep polling in that case,
// not treat it as an error.
type OrderStatusResponseDTO struct {
	OrderID     string `json:"orderId"`
	Status      string `json:"status"`
	CheckoutURL string `json:"checkoutUrl"`
}
