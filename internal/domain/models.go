package domain

import (
	"encoding/json"
	"time"
)

type EventRequest struct {
	UserID    string         `json:"user_id"`
	OrderID   string         `json:"order_id"`
	EventType string         `json:"event_type"`
	Payload   map[string]any `json:"payload"`
}

type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

type CreateEventResponse struct {
	EventID string `json:"event_id"`
	Status  string `json:"status"`
}

type ErrorDetail struct {
	Code    string            `json:"code"`
	Message string            `json:"message"`
	Fields  map[string]string `json:"fields"`
}

type EventMessage struct {
	EventID       string          `json:"event_id"`
	UserID        string          `json:"user_id"`
	OrderID       string          `json:"order_id"`
	EventType     string          `json:"event_type"`
	Payload       json.RawMessage `json:"payload"`
	CorrelationID string          `json:"correlation_id"`
}

type EventResponse struct {
	EventID      string          `json:"event_id"`
	UserID       string          `json:"user_id"`
	OrderID      string          `json:"order_id"`
	EventType    string          `json:"event_type"`
	Payload      json.RawMessage `json:"payload" swaggertype:"object"`
	Status       string          `json:"status"`
	RetryCount   int             `json:"retry_count"`
	ErrorMessage *string         `json:"error_message"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}
