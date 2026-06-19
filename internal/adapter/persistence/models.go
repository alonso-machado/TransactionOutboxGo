package persistence

import (
	"time"

	"github.com/google/uuid"
)

type OutboxMessageModel struct {
	ID             uuid.UUID  `gorm:"type:uuid;primaryKey"`
	IdempotencyKey string     `gorm:"uniqueIndex;not null"`
	AggregateType  string
	HTTPMethod     string
	Route          string
	Payload        []byte     `gorm:"type:jsonb"`
	Headers        []byte     `gorm:"type:jsonb"`
	Status         string     `gorm:"index;default:pending"`
	RetryCount     int        `gorm:"default:0"`
	LastError      string
	CreatedAt      time.Time
	PublishedAt    *time.Time
}

func (OutboxMessageModel) TableName() string { return "outbox_messages" }

type InboxMessageModel struct {
	MessageID   string    `gorm:"primaryKey"`
	Status      string
	ProcessedAt time.Time
}

func (InboxMessageModel) TableName() string { return "inbox_messages" }

type RecordModel struct {
	ID              uuid.UUID `gorm:"type:uuid;primaryKey"`
	SourceMessageID string    `gorm:"uniqueIndex"`
	Method          string
	Route           string
	Payload         []byte    `gorm:"type:jsonb"`
	CreatedAt       time.Time
}

func (RecordModel) TableName() string { return "records" }
