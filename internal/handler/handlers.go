package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	models "practiceRabbitMQ/internal/domain"
	"practiceRabbitMQ/internal/service"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// createEventHandler godoc
// @Summary Создать событие заказа
// @Description Создаёт новое событие уведомления и ставит его в очередь на обработку
// @Tags events
// @Accept json
// @Produce json
// @Param event body domain.EventRequest true "Данные события"
// @Success 201 {object} models.CreateEventResponse
// @Failure 400 {object} models.ErrorResponse
// @Failure 500 {string} string
// @Router /api/events [post]
func CreateEventHandler(repo service.EventRepository, w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event models.EventRequest

	// Decode the JSON request body into the EventRequest struct
	err := json.NewDecoder(r.Body).Decode(&event)
	if err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	fields := service.ValidateEvent(event)

	if len(fields) > 0 {
		response := models.ErrorResponse{
			Error: models.ErrorDetail{
				Code:    "VALIDATION_ERROR",
				Message: "validation failed",
				Fields:  fields,
			},
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Process the event (e.g., store it in a database, trigger other actions, etc.)
	fmt.Println("User:", event.UserID)
	fmt.Println("Order:", event.OrderID)
	fmt.Println("Event Type:", event.EventType)
	fmt.Println("Payload:", event.Payload)

	eventID, err := service.CreateEventService(
		r.Context(),
		repo,
		event,
	)

	if err != nil {
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}

	response := models.CreateEventResponse{
		EventID: eventID,
		Status:  "queued",
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)

}

// getEventsHandler godoc
// @Summary Получить список событий
// @Description Возвращает список событий с фильтрацией и пагинацией
// @Tags events
// @Produce json
// @Param status query string false "Фильтр по статусу: pending, sent, failed"
// @Param user_id query string false "Фильтр по user_id"
// @Param limit query int false "Количество событий" default(10)
// @Param offset query int false "Смещение" default(0)
// @Success 200 {array} domain.EventResponse
// @Failure 400 {object} domain.ErrorResponse
// @Failure 500 {string} string
// @Router /api/events [get]
func GetEventsHandler(
	repo service.EventLister,
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	limit := 10
	offset := 0

	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")

	if limitStr != "" {
		value, err := strconv.Atoi(limitStr)
		if err != nil || value <= 0 {
			http.Error(w, "invalid limit", http.StatusBadRequest)
			return
		}

		limit = value
	}

	if offsetStr != "" {
		value, err := strconv.Atoi(offsetStr)
		if err != nil || value < 0 {
			http.Error(w, "invalid offset", http.StatusBadRequest)
			return
		}

		offset = value
	}

	fmt.Println("limit:", limit)
	fmt.Println("offset:", offset)

	status := r.URL.Query().Get("status")
	if status != "" &&
		status != "pending" &&
		status != "sent" &&
		status != "failed" {

		http.Error(w, "invalid status", http.StatusBadRequest)
		return
	}

	userID := r.URL.Query().Get("user_id")
	fmt.Println("user_id:", userID)

	events, err := service.GetEventsService(
		r.Context(),
		repo,
		status,
		userID,
		limit,
		offset,
	)

	if err != nil {
		http.Error(w, "failed to get events", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	err = json.NewEncoder(w).Encode(events)
	if err != nil {
		fmt.Println("failed to encode events:", err)
	}

}

// getEventByIDHandler godoc
// @Summary Получить событие по ID
// @Description Возвращает одно событие по UUID
// @Tags events
// @Produce json
// @Param event_id path string true "UUID события"
// @Success 200 {object} domain.CreateEventResponse
// @Failure 400 {string} string
// @Failure 404 {string} string
// @Failure 500 {string} string
// @Router /api/events/{event_id} [get]
func GetEventByIDHandler(
	repo service.EventGetter,
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	eventID := strings.TrimPrefix(r.URL.Path, "/api/events/")

	_, err := uuid.Parse(eventID)
	if err != nil {
		http.Error(w, "invalid event id", http.StatusBadRequest)
		return
	}

	event, err := service.GetEventByIDService(r.Context(), repo, eventID)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "event not found", http.StatusNotFound)
			return
		}

		http.Error(w, "failed to get event", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(event)

}

// retryPendingHandler godoc
// @Summary Повторно поставить failed события в очередь
// @Description Переводит события со статусом failed обратно в pending и сбрасывает retry_count
// @Tags admin
// @Produce json
// @Success 200 {object} map[string]int64
// @Failure 405 {string} string
// @Failure 500 {string} string
// @Router /api/admin/retry-pending [post]
func RetryPendingHandler(
	repo service.EventRetryer,
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	count, err := service.RetryFailedEventsService(r.Context(), repo)
	if err != nil {
		http.Error(w, "failed to retry events", http.StatusInternalServerError)
		return
	}
	response := map[string]int64{
		"retried": count,
	}

	w.Header().Set("Content-Type", "application/json")

	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		fmt.Println("failed to encode response:", err)
	}

}
