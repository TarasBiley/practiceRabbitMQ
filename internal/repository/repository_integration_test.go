//go:build integration

package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	models "practiceRabbitMQ/internal/domain"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func repositoryTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func newTestRepository(t *testing.T) (*Repository, *pgxpool.Pool) {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("set TEST_DATABASE_URL to a PostgreSQL database where the test user can create schemas")
	}

	ctx := repositoryTestContext(t)
	admin, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to test PostgreSQL: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := admin.Close(cleanupCtx); err != nil {
			t.Errorf("close schema administration connection: %v", err)
		}
	})

	schema := "test_repository_" + strings.ReplaceAll(uuid.NewString(), "-", "_")
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		t.Fatalf("create isolated test schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("drop isolated test schema %s: %v", schema, err)
		}
	})

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse test database URL: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("create test pool: %v", err)
	}
	t.Cleanup(pool.Close)

	migrations, err := filepath.Glob(filepath.Join("..", "..", "migrations", "*.sql"))
	if err != nil || len(migrations) == 0 {
		t.Fatalf("find database migrations: files=%v, error=%v", migrations, err)
	}
	for _, path := range migrations {
		sql, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read migration %s: %v", path, err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply migration %s: %v", path, err)
		}
	}
	return NewRepository(pool), pool
}

func createTestEvent(t *testing.T, repo *Repository, userID, orderID string) (string, string) {
	t.Helper()
	eventID, correlationID := uuid.NewString(), uuid.NewString()
	event := models.EventRequest{
		UserID: userID, OrderID: orderID, EventType: "paid",
		Payload: map[string]any{"amount": 1000},
	}
	if err := repo.CreateEvent(repositoryTestContext(t), eventID, correlationID, event); err != nil {
		t.Fatalf("create test event: %v", err)
	}
	return eventID, correlationID
}

func execTestSQL(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(repositoryTestContext(t), sql, args...); err != nil {
		t.Fatalf("prepare test data: %v", err)
	}
}

func TestRepositoryCreateEvent(t *testing.T) {
	repo, pool := newTestRepository(t)
	eventID, correlationID := createTestEvent(t, repo, "test-user", "test-order")
	var userID, orderID, eventType, status, savedCorrelationID, amount string
	var retryCount int
	var errorMessage *string
	err := pool.QueryRow(repositoryTestContext(t), `
		SELECT user_id, order_id, event_type, status, correlation_id,
		       payload->>'amount', retry_count, error_message
		FROM events WHERE event_id = $1`, eventID).Scan(
		&userID, &orderID, &eventType, &status, &savedCorrelationID, &amount, &retryCount, &errorMessage,
	)
	if err != nil {
		t.Fatalf("read created event: %v", err)
	}
	if userID != "test-user" || orderID != "test-order" || eventType != "paid" {
		t.Errorf("unexpected saved fields: user=%q, order=%q, type=%q", userID, orderID, eventType)
	}
	if savedCorrelationID != correlationID || amount != "1000" {
		t.Errorf("unexpected correlation ID or payload: correlation=%q, amount=%q", savedCorrelationID, amount)
	}
	if status != "pending" || retryCount != 0 || errorMessage != nil {
		t.Errorf("unexpected defaults: status=%q, retries=%d, error=%v", status, retryCount, errorMessage)
	}
}

func TestRepositoryCreateEventDuplicateID(t *testing.T) {
	repo, _ := newTestRepository(t)
	eventID, correlationID := createTestEvent(t, repo, "original-user", "original-order")
	err := repo.CreateEvent(repositoryTestContext(t), eventID, correlationID, models.EventRequest{
		UserID: "replacement-user", OrderID: "replacement-order", EventType: "paid",
		Payload: map[string]any{"amount": 2000},
	})
	var pgError *pgconn.PgError
	if !errors.As(err, &pgError) || pgError.Code != "23505" {
		t.Fatalf("expected unique constraint error, got %v", err)
	}
	event, err := repo.GetEventByID(repositoryTestContext(t), eventID)
	if err != nil {
		t.Fatalf("read original event: %v", err)
	}
	if event.UserID != "original-user" || event.OrderID != "original-order" {
		t.Fatalf("duplicate insert changed original event: %+v", event)
	}
}

func TestRepositoryGetEvents(t *testing.T) {
	repo, pool := newTestRepository(t)

	first, _ := createTestEvent(t, repo, "user-1", "order-1")
	second, _ := createTestEvent(t, repo, "user-1", "order-2")
	third, _ := createTestEvent(t, repo, "user-2", "order-3")
	fourth, _ := createTestEvent(t, repo, "user-1", "order-4")

	baseTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	for i, id := range []string{first, second, third, fourth} {
		execTestSQL(
			t,
			pool,
			"UPDATE events SET created_at = $2 WHERE event_id = $1",
			id,
			baseTime.Add(time.Duration(i)*time.Hour),
		)
	}

	execTestSQL(
		t,
		pool,
		"UPDATE events SET status = 'sent' WHERE event_id IN ($1, $2, $3)",
		second,
		third,
		fourth,
	)

	// Сделаем fourth другим типом, чтобы реально проверить event_type.
	execTestSQL(
		t,
		pool,
		"UPDATE events SET event_type = 'shipped' WHERE event_id = $1",
		fourth,
	)

	cases := []struct {
		name      string
		status    string
		userID    string
		eventType string
		limit     int
		offset    int
		want      []string
	}{
		{
			name:   "user only",
			userID: "user-1",
			limit:  10,
			want:   []string{fourth, second, first},
		},
		{
			name:   "status and user",
			status: "sent",
			userID: "user-1",
			limit:  10,
			want:   []string{fourth, second},
		},
		{
			name:      "paid events",
			userID:    "user-1",
			eventType: "paid",
			limit:     10,
			want:      []string{second, first},
		},
		{
			name:      "shipped events",
			userID:    "user-1",
			eventType: "shipped",
			limit:     10,
			want:      []string{fourth},
		},
		{
			name:      "status user and event type",
			status:    "sent",
			userID:    "user-1",
			eventType: "paid",
			limit:     10,
			want:      []string{second},
		},
		{
			name:   "different user",
			status: "sent",
			userID: "user-2",
			limit:  10,
			want:   []string{third},
		},
		{
			name:   "pagination",
			userID: "user-1",
			limit:  1,
			offset: 1,
			want:   []string{second},
		},
		{
			name:   "no matching status",
			status: "failed",
			userID: "user-1",
			limit:  10,
		},
		{
			name:      "no matching event type",
			userID:    "user-1",
			eventType: "created",
			limit:     10,
		},
		{
			name:   "no matching user",
			userID: "unknown",
			limit:  10,
		},
		{
			name:   "offset past end",
			userID: "user-1",
			limit:  10,
			offset: 3,
		},
		{
			name:   "zero limit",
			userID: "user-1",
			limit:  0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			events, err := repo.GetEvents(
				repositoryTestContext(t),
				tc.status,
				tc.userID,
				tc.eventType,
				tc.limit,
				tc.offset,
			)
			if err != nil {
				t.Fatalf("GetEvents: %v", err)
			}

			got := make([]string, len(events))

			for i, event := range events {
				got[i] = event.EventID
			}

			if !slices.Equal(got, tc.want) {
				t.Errorf(
					"event IDs in order: got %v, want %v",
					got,
					tc.want,
				)
			}
		})
	}
}

func TestRepositoryGetEventByID(t *testing.T) {
	repo, _ := newTestRepository(t)
	eventID, _ := createTestEvent(t, repo, "user-123", "order-123")
	event, err := repo.GetEventByID(repositoryTestContext(t), eventID)
	if err != nil {
		t.Fatalf("GetEventByID: %v", err)
	}
	if event.EventID != eventID || event.UserID != "user-123" || event.OrderID != "order-123" || event.EventType != "paid" {
		t.Errorf("unexpected event fields: %+v", event)
	}
	if event.Status != "pending" || event.RetryCount != 0 || event.ErrorMessage != nil {
		t.Errorf("unexpected event state: %+v", event)
	}
	var payload struct{ Amount int }
	if err := json.Unmarshal(event.Payload, &payload); err != nil || payload.Amount != 1000 {
		t.Errorf("unexpected payload %s: %v", event.Payload, err)
	}
	if event.CreatedAt.IsZero() || event.UpdatedAt.IsZero() {
		t.Error("expected creation and update timestamps")
	}
}

func TestRepositoryGetEventByIDNotFound(t *testing.T) {
	repo, _ := newTestRepository(t)
	_, err := repo.GetEventByID(repositoryTestContext(t), uuid.NewString())
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expected pgx.ErrNoRows, got %v", err)
	}
}

func TestRepositoryRetryFailedEvents(t *testing.T) {
	repo, pool := newTestRepository(t)
	failed, _ := createTestEvent(t, repo, "user-1", "failed-order")
	sent, _ := createTestEvent(t, repo, "user-2", "sent-order")
	pending, _ := createTestEvent(t, repo, "user-3", "pending-order")
	oldTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	execTestSQL(t, pool, "UPDATE events SET retry_count = 3, error_message = 'test error', updated_at = $1", oldTime)
	execTestSQL(t, pool, "UPDATE events SET status = 'failed' WHERE event_id = $1", failed)
	execTestSQL(t, pool, "UPDATE events SET status = 'sent' WHERE event_id = $1", sent)

	count, err := repo.RetryFailedEvents(repositoryTestContext(t))
	if err != nil || count != 1 {
		t.Fatalf("RetryFailedEvents: count=%d, error=%v; want count 1", count, err)
	}
	for _, tc := range []struct{ id, status string }{{failed, "pending"}, {sent, "sent"}, {pending, "pending"}} {
		event, err := repo.GetEventByID(repositoryTestContext(t), tc.id)
		if err != nil {
			t.Fatalf("read event after retry: %v", err)
		}
		if event.Status != tc.status {
			t.Errorf("event %s status: got %s, want %s", tc.id, event.Status, tc.status)
		}
		if tc.id == failed {
			if event.RetryCount != 0 || event.ErrorMessage != nil || !event.UpdatedAt.After(oldTime) {
				t.Errorf("failed event was not fully reset: %+v", event)
			}
		} else if event.RetryCount != 3 || event.ErrorMessage == nil || *event.ErrorMessage != "test error" || !event.UpdatedAt.Equal(oldTime) {
			t.Errorf("retry changed an event that was not failed: %+v", event)
		}
	}
	count, err = repo.RetryFailedEvents(repositoryTestContext(t))
	if err != nil || count != 0 {
		t.Errorf("second retry: count=%d, error=%v; want count 0", count, err)
	}
}

func TestRepositoryGetPendingEvents(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		repo, _ := newTestRepository(t)
		events, err := repo.GetPendingEvents(repositoryTestContext(t))
		if err != nil || len(events) != 0 {
			t.Fatalf("expected no pending events, got %v, error=%v", events, err)
		}
	})
	t.Run("oldest ten pending events", func(t *testing.T) {
		repo, pool := newTestRepository(t)
		ids, correlations := make([]string, 12), make([]string, 12)
		baseTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		// Insert newest first to ensure the assertion does not depend on insertion order.
		for i := len(ids) - 1; i >= 0; i-- {
			ids[i], correlations[i] = createTestEvent(t, repo, "pending-user", fmt.Sprintf("order-%d", i))
			execTestSQL(t, pool, "UPDATE events SET created_at = $2 WHERE event_id = $1", ids[i], baseTime.Add(time.Duration(i)*time.Hour))
		}
		for _, status := range []string{"sent", "failed"} {
			id, _ := createTestEvent(t, repo, "other-user", status+"-order")
			execTestSQL(t, pool, "UPDATE events SET status = $2, created_at = $3 WHERE event_id = $1", id, status, baseTime.Add(-time.Hour))
		}
		execTestSQL(t, pool, "UPDATE events SET correlation_id = NULL WHERE event_id = $1", ids[0])
		correlations[0] = ""

		events, err := repo.GetPendingEvents(repositoryTestContext(t))
		if err != nil {
			t.Fatalf("GetPendingEvents: %v", err)
		}
		if len(events) != 10 {
			t.Fatalf("expected oldest 10 pending events, got %d", len(events))
		}
		for i, event := range events {
			if event.EventID != ids[i] || event.CorrelationID != correlations[i] {
				t.Errorf("event %d: ID=%q correlation=%q, want ID=%q correlation=%q", i, event.EventID, event.CorrelationID, ids[i], correlations[i])
			}
			if event.UserID != "pending-user" || event.OrderID != fmt.Sprintf("order-%d", i) || event.EventType != "paid" {
				t.Errorf("event %d has unexpected message fields: %+v", i, event)
			}
			var payload struct{ Amount int }
			if err := json.Unmarshal(event.Payload, &payload); err != nil || payload.Amount != 1000 {
				t.Errorf("event %d has unexpected payload %s: %v", i, event.Payload, err)
			}
		}
	})
}

func TestRepositoryMarkEvent(t *testing.T) {
	for _, status := range []string{"sent", "failed"} {
		t.Run(status, func(t *testing.T) {
			repo, pool := newTestRepository(t)
			eventID, _ := createTestEvent(t, repo, "user-1", "order-1")
			otherID, _ := createTestEvent(t, repo, "user-2", "order-2")
			oldTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
			execTestSQL(t, pool, "UPDATE events SET updated_at = $1", oldTime)
			var err error
			if status == "sent" {
				err = repo.MarkEventSent(repositoryTestContext(t), eventID)
			} else {
				err = repo.MarkEventFailed(repositoryTestContext(t), eventID)
			}
			if err != nil {
				t.Fatalf("mark event %s: %v", status, err)
			}
			event, err := repo.GetEventByID(repositoryTestContext(t), eventID)
			if err != nil {
				t.Fatalf("read updated event: %v", err)
			}
			if event.Status != status || !event.UpdatedAt.After(oldTime) {
				t.Errorf("expected status %s and refreshed timestamp: %+v", status, event)
			}
			if status == "failed" && (event.ErrorMessage == nil || *event.ErrorMessage != "notification sending failed") {
				t.Errorf("unexpected failure message: %v", event.ErrorMessage)
			}
			other, err := repo.GetEventByID(repositoryTestContext(t), otherID)
			if err != nil {
				t.Fatalf("read unrelated event: %v", err)
			}
			if other.Status != "pending" || !other.UpdatedAt.Equal(oldTime) {
				t.Errorf("unrelated event changed: %+v", other)
			}
		})
	}
}

func TestRepositoryIncrementRetryCount(t *testing.T) {
	repo, pool := newTestRepository(t)
	eventID, _ := createTestEvent(t, repo, "user-1", "order-1")
	oldTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	execTestSQL(t, pool, "UPDATE events SET updated_at = $2 WHERE event_id = $1", eventID, oldTime)
	for want := 1; want <= 2; want++ {
		count, err := repo.IncrementRetryCount(repositoryTestContext(t), eventID)
		if err != nil || count != want {
			t.Fatalf("increment: count=%d, error=%v; want %d", count, err, want)
		}
	}
	event, err := repo.GetEventByID(repositoryTestContext(t), eventID)
	if err != nil {
		t.Fatalf("read updated retry count: %v", err)
	}
	if event.RetryCount != 2 || !event.UpdatedAt.After(oldTime) || event.Status != "pending" {
		t.Errorf("unexpected state after increments: %+v", event)
	}
	_, err = repo.IncrementRetryCount(repositoryTestContext(t), uuid.NewString())
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Errorf("increment nonexistent event: expected pgx.ErrNoRows, got %v", err)
	}
}
