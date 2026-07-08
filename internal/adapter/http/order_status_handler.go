package handler

import (
	"errors"
	"net/http"

	"github.com/alonsomachado/transaction-outbox-go/internal/domain"
	"github.com/alonsomachado/transaction-outbox-go/internal/domain/pii"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type OrderStatusHandler struct {
	orderRepo  domain.OrderRepository
	chargeRepo domain.ChargeRepository
}

func NewOrderStatusHandler(orderRepo domain.OrderRepository, chargeRepo domain.ChargeRepository) *OrderStatusHandler {
	return &OrderStatusHandler{orderRepo: orderRepo, chargeRepo: chargeRepo}
}

// Handle reports an order's status and checkout URL — the client polls
// this after POST /orders's 201 to learn where to send the customer to pay.
//
//	@Summary		Get order status
//	@Description	Returns the order's status and checkout URL. checkoutUrl is empty until
//	@Description	order-consumer-worker has opened a checkout for this order — keep polling, not an error.
//	@Tags			orders
//	@Produce		json
//	@Param			id	path		string	true	"Order ID"
//	@Success		200	{object}	OrderStatusResponseDTO
//	@Failure		400	{object}	map[string]string
//	@Failure		404	{object}	map[string]string
//	@Router			/api/v1/orders/{id} [get]
func (h *OrderStatusHandler) Handle(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid order id"})
		return
	}

	order, err := h.orderRepo.FindByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "order not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": pii.Redact(err.Error())})
		return
	}

	var checkoutURL string
	charge, err := h.chargeRepo.FindByOrderID(c.Request.Context(), id)
	switch {
	case err == nil:
		checkoutURL = charge.CheckoutURL
	case errors.Is(err, gorm.ErrRecordNotFound):
		// Normal while the client is still polling right after 201 —
		// order-consumer-worker hasn't opened a checkout yet.
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": pii.Redact(err.Error())})
		return
	}

	c.JSON(http.StatusOK, OrderStatusResponseDTO{
		OrderID:     order.ID.String(),
		Status:      string(order.Status),
		CheckoutURL: checkoutURL,
	})
}
