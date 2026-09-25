package entities

import (
	"encoding/json"
	"time"
)

type OutboxEvent struct {
	ID          string          `json:"id"`
	EventID     string          `json:"event_id"`
	EventType   string          `json:"event_type"`
	Payload     json.RawMessage `json:"payload"`
	Status      string          `json:"status"`
	RetryCount  int             `json:"retry_count"`
	MaxRetries  int             `json:"max_retries"`
	LastError   string          `json:"last_error,omitempty"`
	LockedBy    string          `json:"locked_by,omitempty"`
	LockedUntil *time.Time      `json:"locked_until,omitempty"`
	NextRetryAt time.Time       `json:"next_retry_at"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}
