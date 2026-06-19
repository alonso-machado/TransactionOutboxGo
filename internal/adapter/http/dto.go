package handler

type RecordRequestDTO struct {
	Payload map[string]any `json:"payload" binding:"required"`
}

type RecordResponseDTO struct {
	MessageID      string `json:"messageId"`
	IdempotencyKey string `json:"idempotencyKey"`
	Status         string `json:"status"`
}
