package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	models "practiceRabbitMQ/internal/domain"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Unconfigured methods fail immediately so a handler cannot silently use the
// wrong repository operation. Every operation also contributes to calls.
type fakeHandlerRepository struct {
	t            *testing.T
	calls        int
	createEvent  func(context.Context, string, string, models.EventRequest) error
	getEvents    func(context.Context, string, string, int, int) ([]models.EventResponse, error)
	getEventByID func(context.Context, string) (models.EventResponse, error)
	retryFailed  func(context.Context) (int64, error)
}

func (f *fakeHandlerRepository) CreateEvent(ctx context.Context, eventID, correlationID string, event models.EventRequest) error {
	f.t.Helper()
	f.calls++
	if f.createEvent == nil {
		f.t.Fatal("unexpected CreateEvent call")
	}
	return f.createEvent(ctx, eventID, correlationID, event)
}

func (f *fakeHandlerRepository) GetEvents(ctx context.Context, status, userID string, limit, offset int) ([]models.EventResponse, error) {
	f.t.Helper()
	f.calls++
	if f.getEvents == nil {
		f.t.Fatal("unexpected GetEvents call")
	}
	return f.getEvents(ctx, status, userID, limit, offset)
}

func (f *fakeHandlerRepository) GetEventByID(ctx context.Context, eventID string) (models.EventResponse, error) {
	f.t.Helper()
	f.calls++
	if f.getEventByID == nil {
		f.t.Fatal("unexpected GetEventByID call")
	}
	return f.getEventByID(ctx, eventID)
}

func (f *fakeHandlerRepository) RetryFailedEvents(ctx context.Context) (int64, error) {
	f.t.Helper()
	f.calls++
	if f.retryFailed == nil {
		f.t.Fatal("unexpected RetryFailedEvents call")
	}
	return f.retryFailed(ctx)
}

func assertHTTPResponse(t *testing.T, rec *httptest.ResponseRecorder, code int, contentType, text string) {
	t.Helper()
	if rec.Code != code {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != contentType {
		t.Errorf("Content-Type = %q, want %q", got, contentType)
	}
	if text != "" && rec.Body.String() != text+"\n" {
		t.Errorf("body = %q, want %q", rec.Body.String(), text+"\n")
	}
}

func decodeResponse[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var response T
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v; body: %s", err, rec.Body.String())
	}
	return response
}

func assertBadRequestJSON(t *testing.T, rec *httptest.ResponseRecorder, message string) {
	t.Helper()
	got := decodeResponse[map[string]any](t, rec)
	want := map[string]any{
		"error": map[string]any{
			"code":    "BAD_REQUEST",
			"message": message,
			"fields":  map[string]any{},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("bad request response = %#v, want %#v", got, want)
	}
}

func TestCreateEventHandler(t *testing.T) {
	const validBody = `{"user_id":"user-17","order_id":"order-42","event_type":"paid","payload":{"amount":42,"currency":"RUB"}}`
	wantEvent := models.EventRequest{
		UserID: "user-17", OrderID: "order-42", EventType: "paid",
		Payload: map[string]any{"amount": float64(42), "currency": "RUB"},
	}

	tests := []struct {
		name       string
		method     string
		body       string
		repoErr    error
		wantCode   int
		wantText   string
		wantFields map[string]string
		wantCalls  int
	}{
		{name: "success", method: http.MethodPost, body: validBody, wantCode: http.StatusCreated, wantCalls: 1},
		{name: "trailing JSON whitespace", method: http.MethodPost, body: validBody + " \t\r\n", wantCode: http.StatusCreated, wantCalls: 1},
		{name: "trailing garbage", method: http.MethodPost, body: validBody + " garbage", wantCode: http.StatusBadRequest, wantText: "Invalid request payload", wantCalls: 0},
		{name: "second JSON object", method: http.MethodPost, body: validBody + ` {"x":1}`, wantCode: http.StatusBadRequest, wantText: "Invalid request payload", wantCalls: 0},
		{name: "second JSON null", method: http.MethodPost, body: validBody + " null", wantCode: http.StatusBadRequest, wantText: "Invalid request payload", wantCalls: 0},
		{name: "wrong method", method: http.MethodGet, body: validBody, wantCode: http.StatusMethodNotAllowed, wantText: "Method not allowed"},
		{name: "empty body", method: http.MethodPost, wantCode: http.StatusBadRequest, wantText: "Invalid request payload"},
		{name: "malformed JSON", method: http.MethodPost, body: `{"user_id":`, wantCode: http.StatusBadRequest, wantText: "Invalid request payload"},
		{name: "array body", method: http.MethodPost, body: `[]`, wantCode: http.StatusBadRequest, wantText: "Invalid request payload"},
		{name: "wrong field type", method: http.MethodPost, body: `{"user_id":17}`, wantCode: http.StatusBadRequest, wantText: "Invalid request payload"},
		{
			name: "missing user", method: http.MethodPost,
			body: `{"order_id":"order-42","event_type":"paid"}`, wantCode: http.StatusBadRequest,
			wantFields: map[string]string{"user_id": "required"},
		},
		{
			name: "missing order", method: http.MethodPost,
			body: `{"user_id":"user-17","event_type":"paid"}`, wantCode: http.StatusBadRequest,
			wantFields: map[string]string{"order_id": "required"},
		},
		{
			name: "invalid event type", method: http.MethodPost,
			body: `{"user_id":"user-17","order_id":"order-42","event_type":"cancelled"}`, wantCode: http.StatusBadRequest,
			wantFields: map[string]string{"event_type": "must be created, paid or shipped"},
		},
		{
			name: "all required fields missing", method: http.MethodPost, body: `{}`, wantCode: http.StatusBadRequest,
			wantFields: map[string]string{"user_id": "required", "order_id": "required", "event_type": "must be created, paid or shipped"},
		},
		{
			name: "repository error", method: http.MethodPost, body: validBody,
			repoErr: errors.New("private database detail"), wantCode: http.StatusInternalServerError,
			wantText: "database error", wantCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/api/events", strings.NewReader(tt.body))
			ctx, cancel := context.WithCancel(req.Context())
			defer cancel()
			req = req.WithContext(ctx)
			rec := httptest.NewRecorder()
			var createdID string
			repo := &fakeHandlerRepository{t: t}
			if tt.wantCalls > 0 {
				repo.createEvent = func(gotCtx context.Context, eventID, correlationID string, event models.EventRequest) error {
					if gotCtx != ctx {
						t.Error("request context was not forwarded")
					}
					if !reflect.DeepEqual(event, wantEvent) {
						t.Errorf("event = %#v, want %#v", event, wantEvent)
					}
					for name, id := range map[string]string{"event ID": eventID, "correlation ID": correlationID} {
						if parsed, err := uuid.Parse(id); err != nil || parsed == uuid.Nil {
							t.Errorf("%s must be a nonzero UUID, got %q", name, id)
						}
					}
					if eventID == correlationID {
						t.Error("event and correlation IDs must differ")
					}
					createdID = eventID
					return tt.repoErr
				}
			}

			CreateEventHandler(repo, rec, req)

			contentType := "text/plain; charset=utf-8"
			wantText := tt.wantText
			if tt.wantCode == http.StatusCreated || tt.wantCode == http.StatusBadRequest {
				contentType = "application/json"
			}
			if tt.wantCode == http.StatusBadRequest {
				wantText = ""
				if tt.wantFields == nil {
					assertBadRequestJSON(t, rec, tt.wantText)
				}
			}
			assertHTTPResponse(t, rec, tt.wantCode, contentType, wantText)
			if repo.calls != tt.wantCalls {
				t.Errorf("repository calls = %d, want %d", repo.calls, tt.wantCalls)
			}
			if tt.wantCode == http.StatusCreated {
				got := decodeResponse[models.CreateEventResponse](t, rec)
				want := models.CreateEventResponse{EventID: createdID, Status: "queued"}
				if got != want {
					t.Errorf("response = %#v, want %#v", got, want)
				}
			}
			if tt.wantFields != nil {
				got := decodeResponse[models.ErrorResponse](t, rec)
				want := models.ErrorResponse{Error: models.ErrorDetail{
					Code: "VALIDATION_ERROR", Message: "validation failed", Fields: tt.wantFields,
				}}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("validation response = %#v, want %#v", got, want)
				}
			}
		})
	}
}

func testEventResponse() models.EventResponse {
	errorMessage := "delivery failed"
	return models.EventResponse{
		EventID: "708b8a2f-222a-40da-850d-4ae5eff1324a", UserID: "user-17", OrderID: "order-42",
		EventType: "paid", Payload: json.RawMessage(`{"amount":42}`), Status: "failed", RetryCount: 3,
		ErrorMessage: &errorMessage,
		CreatedAt:    time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		UpdatedAt:    time.Date(2026, 1, 2, 3, 5, 5, 0, time.UTC),
	}
}

func TestGetEventsHandler(t *testing.T) {
	type listArgs struct {
		status, userID string
		limit, offset  int
	}
	tests := []struct {
		name      string
		method    string
		query     string
		wantArgs  listArgs
		repoErr   error
		empty     bool
		wantCode  int
		wantText  string
		wantCalls int
	}{
		{name: "defaults", wantArgs: listArgs{limit: 10}, wantCode: http.StatusOK, wantCalls: 1},
		{name: "empty explicit defaults", query: "?limit=&offset=", wantArgs: listArgs{limit: 10}, wantCode: http.StatusOK, wantCalls: 1},
		{name: "filters and pagination", query: "?status=failed&user_id=user-17&limit=2&offset=3", wantArgs: listArgs{"failed", "user-17", 2, 3}, wantCode: http.StatusOK, wantCalls: 1},
		{name: "pending status", query: "?status=pending&limit=1&offset=0", wantArgs: listArgs{status: "pending", limit: 1}, wantCode: http.StatusOK, wantCalls: 1},
		{name: "sent status", query: "?status=sent", wantArgs: listArgs{status: "sent", limit: 10}, wantCode: http.StatusOK, wantCalls: 1},
		{name: "empty result", empty: true, wantArgs: listArgs{limit: 10}, wantCode: http.StatusOK, wantCalls: 1},
		{name: "wrong method", method: http.MethodPost, wantCode: http.StatusMethodNotAllowed, wantText: "Method not allowed"},
		{name: "nonnumeric limit", query: "?limit=abc", wantCode: http.StatusBadRequest, wantText: "invalid limit"},
		{name: "zero limit", query: "?limit=0", wantCode: http.StatusBadRequest, wantText: "invalid limit"},
		{name: "negative limit", query: "?limit=-1", wantCode: http.StatusBadRequest, wantText: "invalid limit"},
		{name: "overflow limit", query: "?limit=999999999999999999999999", wantCode: http.StatusBadRequest, wantText: "invalid limit"},
		{name: "nonnumeric offset", query: "?offset=abc", wantCode: http.StatusBadRequest, wantText: "invalid offset"},
		{name: "negative offset", query: "?offset=-1", wantCode: http.StatusBadRequest, wantText: "invalid offset"},
		{name: "overflow offset", query: "?offset=999999999999999999999999", wantCode: http.StatusBadRequest, wantText: "invalid offset"},
		{name: "unknown status", query: "?status=queued", wantCode: http.StatusBadRequest, wantText: "invalid status"},
		{name: "repository error", wantArgs: listArgs{limit: 10}, repoErr: errors.New("private database detail"), wantCode: http.StatusInternalServerError, wantText: "failed to get events", wantCalls: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			method := tt.method
			if method == "" {
				method = http.MethodGet
			}
			req := httptest.NewRequest(method, "/api/events"+tt.query, nil)
			ctx, cancel := context.WithCancel(req.Context())
			defer cancel()
			req = req.WithContext(ctx)
			rec := httptest.NewRecorder()
			wantEvents := []models.EventResponse{testEventResponse()}
			if tt.empty {
				wantEvents = []models.EventResponse{}
			}
			repo := &fakeHandlerRepository{t: t}
			if tt.wantCalls > 0 {
				repo.getEvents = func(gotCtx context.Context, status, userID string, limit, offset int) ([]models.EventResponse, error) {
					if gotCtx != ctx {
						t.Error("request context was not forwarded")
					}
					if got := (listArgs{status, userID, limit, offset}); got != tt.wantArgs {
						t.Errorf("list arguments = %#v, want %#v", got, tt.wantArgs)
					}
					return wantEvents, tt.repoErr
				}
			}

			GetEventsHandler(repo, rec, req)

			contentType := "text/plain; charset=utf-8"
			wantText := tt.wantText
			if tt.wantCode == http.StatusOK || tt.wantCode == http.StatusBadRequest {
				contentType = "application/json"
			}
			if tt.wantCode == http.StatusBadRequest {
				wantText = ""
				assertBadRequestJSON(t, rec, tt.wantText)
			}
			assertHTTPResponse(t, rec, tt.wantCode, contentType, wantText)
			if repo.calls != tt.wantCalls {
				t.Errorf("repository calls = %d, want %d", repo.calls, tt.wantCalls)
			}
			if tt.wantCode == http.StatusOK {
				got := decodeResponse[[]models.EventResponse](t, rec)
				if !reflect.DeepEqual(got, wantEvents) {
					t.Errorf("events = %#v, want %#v", got, wantEvents)
				}
			}
		})
	}
}

func TestGetEventByIDHandler(t *testing.T) {
	wantEvent := testEventResponse()
	tests := []struct {
		name      string
		method    string
		id        string
		repoErr   error
		wantCode  int
		wantText  string
		wantCalls int
	}{
		{name: "success", id: wantEvent.EventID, wantCode: http.StatusOK, wantCalls: 1},
		{name: "wrong method", method: http.MethodPost, id: wantEvent.EventID, wantCode: http.StatusMethodNotAllowed, wantText: "Method not allowed"},
		{name: "invalid UUID", id: "not-a-uuid", wantCode: http.StatusBadRequest, wantText: "invalid event id"},
		{name: "missing ID", wantCode: http.StatusBadRequest, wantText: "invalid event id"},
		{name: "not found", id: wantEvent.EventID, repoErr: pgx.ErrNoRows, wantCode: http.StatusNotFound, wantText: "event not found", wantCalls: 1},
		{name: "wrapped not found", id: wantEvent.EventID, repoErr: fmt.Errorf("get event: %w", pgx.ErrNoRows), wantCode: http.StatusNotFound, wantText: "event not found", wantCalls: 1},
		{name: "repository error", id: wantEvent.EventID, repoErr: errors.New("private database detail"), wantCode: http.StatusInternalServerError, wantText: "failed to get event", wantCalls: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			method := tt.method
			if method == "" {
				method = http.MethodGet
			}
			req := httptest.NewRequest(method, "/api/events/"+tt.id, nil)
			ctx, cancel := context.WithCancel(req.Context())
			defer cancel()
			req = req.WithContext(ctx)
			rec := httptest.NewRecorder()
			repo := &fakeHandlerRepository{t: t}
			if tt.wantCalls > 0 {
				repo.getEventByID = func(gotCtx context.Context, eventID string) (models.EventResponse, error) {
					if gotCtx != ctx {
						t.Error("request context was not forwarded")
					}
					if eventID != tt.id {
						t.Errorf("event ID = %q, want %q", eventID, tt.id)
					}
					return wantEvent, tt.repoErr
				}
			}

			GetEventByIDHandler(repo, rec, req)

			contentType := "text/plain; charset=utf-8"
			if tt.wantCode == http.StatusOK {
				contentType = "application/json"
			}
			assertHTTPResponse(t, rec, tt.wantCode, contentType, tt.wantText)
			if repo.calls != tt.wantCalls {
				t.Errorf("repository calls = %d, want %d", repo.calls, tt.wantCalls)
			}
			if tt.wantCode == http.StatusOK {
				got := decodeResponse[models.EventResponse](t, rec)
				if !reflect.DeepEqual(got, wantEvent) {
					t.Errorf("event = %#v, want %#v", got, wantEvent)
				}
			}
		})
	}
}

func TestRetryPendingHandler(t *testing.T) {
	tests := []struct {
		name      string
		method    string
		count     int64
		repoErr   error
		wantCode  int
		wantText  string
		wantCalls int
	}{
		{name: "success", method: http.MethodPost, count: 3, wantCode: http.StatusOK, wantCalls: 1},
		{name: "no failed events", method: http.MethodPost, wantCode: http.StatusOK, wantCalls: 1},
		{name: "wrong method", method: http.MethodGet, wantCode: http.StatusMethodNotAllowed, wantText: "Method not allowed"},
		{name: "repository error", method: http.MethodPost, repoErr: errors.New("private database detail"), wantCode: http.StatusInternalServerError, wantText: "failed to retry events", wantCalls: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/api/admin/retry-pending", nil)
			ctx, cancel := context.WithCancel(req.Context())
			defer cancel()
			req = req.WithContext(ctx)
			rec := httptest.NewRecorder()
			repo := &fakeHandlerRepository{t: t}
			if tt.wantCalls > 0 {
				repo.retryFailed = func(gotCtx context.Context) (int64, error) {
					if gotCtx != ctx {
						t.Error("request context was not forwarded")
					}
					return tt.count, tt.repoErr
				}
			}

			RetryPendingHandler(repo, rec, req)

			contentType := "text/plain; charset=utf-8"
			if tt.wantCode == http.StatusOK {
				contentType = "application/json"
			}
			assertHTTPResponse(t, rec, tt.wantCode, contentType, tt.wantText)
			if repo.calls != tt.wantCalls {
				t.Errorf("repository calls = %d, want %d", repo.calls, tt.wantCalls)
			}
			if tt.wantCode == http.StatusOK {
				got := decodeResponse[map[string]int64](t, rec)
				want := map[string]int64{"retried": tt.count}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("response = %#v, want %#v", got, want)
				}
			}
		})
	}
}
