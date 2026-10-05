package service

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	models "practiceRabbitMQ/internal/domain"

	"github.com/google/uuid"
)

type repositoryCall struct {
	method string
	ctx    context.Context
	args   []any
}

type fakeRepository struct {
	calls         []repositoryCall
	err           error
	events        []models.EventResponse
	event         models.EventResponse
	pendingEvents []models.EventMessage
	retriedCount  int64
	retryCount    int
}

func (f *fakeRepository) record(method string, ctx context.Context, args ...any) {
	f.calls = append(f.calls, repositoryCall{method: method, ctx: ctx, args: args})
}

func (f *fakeRepository) CreateEvent(ctx context.Context, id, correlationID string, event models.EventRequest) error {
	f.record("CreateEvent", ctx, id, correlationID, event)
	return f.err
}

func (f *fakeRepository) GetEvents(ctx context.Context, status, userID, eventType string, limit, offset int) ([]models.EventResponse, error) {
	f.record("GetEvents", ctx, status, userID, eventType, limit, offset)
	return f.events, f.err
}
func (f *fakeRepository) GetEventByID(ctx context.Context, id string) (models.EventResponse, error) {
	f.record("GetEventByID", ctx, id)
	return f.event, f.err
}

func (f *fakeRepository) RetryFailedEvents(ctx context.Context) (int64, error) {
	f.record("RetryFailedEvents", ctx)
	return f.retriedCount, f.err
}

func (f *fakeRepository) GetPendingEvents(ctx context.Context) ([]models.EventMessage, error) {
	f.record("GetPendingEvents", ctx)
	return f.pendingEvents, f.err
}

func (f *fakeRepository) MarkEventSent(ctx context.Context, id string) error {
	f.record("MarkEventSent", ctx, id)
	return f.err
}

func (f *fakeRepository) IncrementRetryCount(ctx context.Context, id string) (int, error) {
	f.record("IncrementRetryCount", ctx, id)
	return f.retryCount, f.err
}

func (f *fakeRepository) MarkEventFailed(ctx context.Context, id string) error {
	f.record("MarkEventFailed", ctx, id)
	return f.err
}

func assertRepositoryCall(t *testing.T, repo *fakeRepository, method string, ctx context.Context, args ...any) {
	t.Helper()
	if len(repo.calls) != 1 {
		t.Fatalf("repository calls = %+v, want exactly one %s call", repo.calls, method)
	}
	got := repo.calls[0]
	if got.method != method || got.ctx != ctx || !reflect.DeepEqual(got.args, args) {
		t.Fatalf("repository call = %+v, want method=%s original context and args=%+v", got, method, args)
	}
}

func TestCreateEventService(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "success"},
		{name: "repository error", err: errors.New("create failed")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			repo := &fakeRepository{err: tt.err}

			event := models.EventRequest{
				UserID:    "user-1",
				OrderID:   "order-2",
				EventType: "paid",
				Payload:   map[string]any{"amount": 1000, "currency": "RUB"},
			}
			correlationID := "test-correlation-id"

			id, err := CreateEventService(ctx, repo, correlationID, event)
			if !errors.Is(err, tt.err) {
				t.Fatalf("error = %v, want %v", err, tt.err)
			}
			if len(repo.calls) != 1 {
				t.Fatalf("repository calls = %d, want 1", len(repo.calls))
			}
			savedID, _ := repo.calls[0].args[0].(string)
			assertRepositoryCall(t, repo, "CreateEvent", ctx, savedID, correlationID, event)
			if parsed, err := uuid.Parse(savedID); err != nil || parsed == uuid.Nil {
				t.Errorf("eventID = %q, want valid UUID", savedID)
			}
			if tt.err != nil {
				if id != "" {
					t.Errorf("eventID = %q, want empty", id)
				}
				return
			}
			if id != savedID {
				t.Errorf("eventID = %q, want %q", id, savedID)
			}
		})
	}
}

func TestValidateEvent(t *testing.T) {
	const typeError = "must be created, paid or shipped"

	for _, tt := range []struct {
		name  string
		event models.EventRequest
		want  map[string]string
	}{
		{
			name:  "created",
			event: models.EventRequest{UserID: "u", OrderID: "o", EventType: "created"},
		},
		{
			name:  "paid",
			event: models.EventRequest{UserID: "u", OrderID: "o", EventType: "paid"},
		},
		{
			name:  "shipped",
			event: models.EventRequest{UserID: "u", OrderID: "o", EventType: "shipped"},
		},
		{
			name:  "missing user",
			event: models.EventRequest{OrderID: "o", EventType: "paid"},
			want:  map[string]string{"user_id": "required"},
		},
		{
			name:  "missing order",
			event: models.EventRequest{UserID: "u", EventType: "paid"},
			want:  map[string]string{"order_id": "required"},
		},
		{
			name:  "missing type",
			event: models.EventRequest{UserID: "u", OrderID: "o"},
			want:  map[string]string{"event_type": "required"},
		},
		{
			name:  "unknown type",
			event: models.EventRequest{UserID: "u", OrderID: "o", EventType: "deleted"},
			want:  map[string]string{"event_type": typeError},
		},
		{
			name: "all fields missing",
			want: map[string]string{"user_id": "required", "order_id": "required", "event_type": "required"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := ValidateEvent(tt.event)
			if len(got) != len(tt.want) {
				t.Fatalf("validation errors = %v, want %v", got, tt.want)
			}

			for field, message := range tt.want {
				if got[field] != message {
					t.Errorf("validation[%q] = %q, want %q", field, got[field], message)
				}
			}
		})
	}
}

func TestGetEventsService(t *testing.T) {
	for _, tt := range []struct {
		name   string
		events []models.EventResponse
		err    error
	}{
		{name: "events", events: []models.EventResponse{
			{EventID: "event-1", UserID: "user-7", Status: "sent", Payload: json.RawMessage(`{"amount":100}`)},
			{EventID: "event-2", UserID: "user-7", Status: "sent", RetryCount: 2},
		}},
		{name: "empty", events: []models.EventResponse{}},
		{name: "repository error", err: errors.New("list failed")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			repo := &fakeRepository{events: tt.events, err: tt.err}

			events, err := GetEventsService(ctx, repo, "sent", "user-7", "paid", 3, 6)
			if !errors.Is(err, tt.err) || !reflect.DeepEqual(events, tt.events) {
				t.Fatalf("result = (%+v, %v), want (%+v, %v)", events, err, tt.events, tt.err)
			}
			assertRepositoryCall(t, repo, "GetEvents", ctx, "sent", "user-7", "paid", 3, 6)
		})
	}
}

func TestRetryFailedEventsService(t *testing.T) {
	for _, tt := range []struct {
		name  string
		count int64
		err   error
	}{
		{name: "retried", count: 3},
		{name: "nothing to retry"},
		{name: "repository error", err: errors.New("retry failed")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			repo := &fakeRepository{retriedCount: tt.count, err: tt.err}
			count, err := RetryFailedEventsService(ctx, repo)
			if count != tt.count || !errors.Is(err, tt.err) {
				t.Fatalf("result = (%d, %v), want (%d, %v)", count, err, tt.count, tt.err)
			}
			assertRepositoryCall(t, repo, "RetryFailedEvents", ctx)
		})
	}
}

func TestPublishPendingEventService(t *testing.T) {
	for _, tt := range []struct {
		name   string
		events []models.EventMessage
		err    error
	}{
		{name: "events", events: []models.EventMessage{
			{EventID: "event-1", UserID: "user-2", OrderID: "order-3", EventType: "paid", CorrelationID: "correlation-4", Payload: json.RawMessage(`{"amount":100}`)},
			{EventID: "event-5", EventType: "shipped"},
		}},
		{name: "empty", events: []models.EventMessage{}},
		{name: "repository error", err: errors.New("pending lookup failed")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			repo := &fakeRepository{pendingEvents: tt.events, err: tt.err}
			events, err := PublishPendingEventService(ctx, repo)
			if !errors.Is(err, tt.err) || !reflect.DeepEqual(events, tt.events) {
				t.Fatalf("result = (%+v, %v), want (%+v, %v)", events, err, tt.events, tt.err)
			}
			assertRepositoryCall(t, repo, "GetPendingEvents", ctx)
		})
	}
}

func TestMarkEventSentService(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
	}{
		{name: "success"},
		{name: "repository error", err: errors.New("mark sent failed")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			repo := &fakeRepository{err: tt.err}
			if err := MarkEventSentService(ctx, repo, "event-23"); !errors.Is(err, tt.err) {
				t.Fatalf("error = %v, want %v", err, tt.err)
			}
			assertRepositoryCall(t, repo, "MarkEventSent", ctx, "event-23")
		})
	}
}

func TestMarkEventFailedService(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
	}{
		{name: "success"},
		{name: "repository error", err: errors.New("mark failed failed")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			repo := &fakeRepository{err: tt.err}
			if err := MarkEventFailedService(ctx, repo, "event-29"); !errors.Is(err, tt.err) {
				t.Fatalf("error = %v, want %v", err, tt.err)
			}
			assertRepositoryCall(t, repo, "MarkEventFailed", ctx, "event-29")
		})
	}
}

func TestIncrementRetryCountService(t *testing.T) {
	for _, tt := range []struct {
		name  string
		count int
		err   error
	}{
		{name: "success", count: 2},
		{name: "repository error", err: errors.New("increment failed")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			repo := &fakeRepository{retryCount: tt.count, err: tt.err}
			count, err := IncrementRetryCountService(ctx, repo, "event-31")
			if count != tt.count || !errors.Is(err, tt.err) {
				t.Fatalf("result = (%d, %v), want (%d, %v)", count, err, tt.count, tt.err)
			}
			assertRepositoryCall(t, repo, "IncrementRetryCount", ctx, "event-31")
		})
	}
}
