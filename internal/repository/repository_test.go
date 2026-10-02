package repository

import (
	"context"
	"encoding/json"
	"errors"
	models "practiceRabbitMQ/internal/domain"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestNewRepository(t *testing.T) {
	pool := &pgxpool.Pool{}
	repo := NewRepository(pool)
	if repo == nil || repo.pool != pool {
		t.Fatal("expected repository to contain the provided pool")
	}
}

func TestRepositoryCreateEventInvalidPayload(t *testing.T) {
	repo := NewRepository(nil)
	event := models.EventRequest{Payload: map[string]any{"unsupported": make(chan int)}}
	err := repo.CreateEvent(context.Background(), "event-id", "correlation-id", event)
	var unsupportedType *json.UnsupportedTypeError
	if !errors.As(err, &unsupportedType) {
		t.Fatalf("expected JSON unsupported-type error before accessing the database, got %v", err)
	}
}
