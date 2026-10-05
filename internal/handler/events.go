package handler

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	models "practiceRabbitMQ/internal/domain"
	"practiceRabbitMQ/internal/middleware"
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
// @Success 201 {object} domain.CreateEventResponse
// @Failure 400 {object} domain.ErrorResponse
// @Failure 405 {object} domain.ErrorResponse
// @Failure 500 {object} domain.ErrorResponse
// @Router /api/events [post]
func CreateEventHandler(
	repo service.EventRepository,
	w http.ResponseWriter,
	r *http.Request,
) {
	correlationID := middleware.GetCorrelationID(
		r.Context(),
	)

	logger := slog.With(
		"correlation_id", correlationID,
	)

	if r.Method != http.MethodPost {
		logger.Warn(
			"method not allowed",
			"method", r.Method,
		)

		writeError(
			w,
			http.StatusMethodNotAllowed,
			CodeMethodNotAllowed,
			"method not allowed",
			nil,
		)
		return
	}

	var event models.EventRequest

	decoder := json.NewDecoder(r.Body)

	err := decoder.Decode(&event)
	if err != nil {
		logger.Warn(
			"invalid request payload",
			"error", err,
		)

		writeError(
			w,
			http.StatusBadRequest,
			CodeBadRequest,
			"invalid request payload",
			nil,
		)
		return
	}

	var extra any

	if err := decoder.Decode(&extra); err != io.EOF {
		logger.Warn(
			"invalid request payload: extra json data",
		)

		writeError(
			w,
			http.StatusBadRequest,
			CodeBadRequest,
			"invalid request payload",
			nil,
		)
		return
	}

	fields := service.ValidateEvent(event)

	if len(fields) > 0 {
		logger.Warn(
			"event validation failed",
			"fields", fields,
		)

		writeError(
			w,
			http.StatusBadRequest,
			CodeValidation,
			"validation failed",
			fields,
		)
		return
	}

	logger.Info(
		"create event request",
		"user_id", event.UserID,
		"order_id", event.OrderID,
		"event_type", event.EventType,
	)

	eventID, err := service.CreateEventService(
		r.Context(),
		repo,
		correlationID,
		event,
	)

	if err != nil {
		logger.Error(
			"failed to create event",
			"error", err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			CodeInternal,
			"internal server error",
			nil,
		)
		return
	}

	logger.Info(
		"event created",
		"event_id", eventID,
		"user_id", event.UserID,
		"order_id", event.OrderID,
		"event_type", event.EventType,
	)

	response := models.CreateEventResponse{
		EventID: eventID,
		Status:  "queued",
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	w.WriteHeader(
		http.StatusCreated,
	)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		logger.Error(
			"failed to encode create event response",
			"error", err,
		)
	}
}

// getEventsHandler godoc
// @Summary Получить список событий
// @Description Возвращает список событий с фильтрацией и пагинацией
// @Tags events
// @Produce json
// @Param user_id query string true "User ID"
// @Param status query string false "Фильтр по статусу: pending, sent, failed"
// @Param event_type query string false "Фильтр по типу события: created, paid, shipped"
// @Param page query int false "Номер страницы" default(1)
// @Param limit query int false "Количество событий на странице" default(20) maximum(100)
// @Success 200 {array} domain.EventResponse
// @Failure 400 {object} domain.ErrorResponse
// @Failure 405 {object} domain.ErrorResponse
// @Failure 500 {object} domain.ErrorResponse
// @Router /api/events [get]
func GetEventsHandler(
	repo service.EventLister,
	w http.ResponseWriter,
	r *http.Request,
) {
	correlationID := middleware.GetCorrelationID(
		r.Context(),
	)

	logger := slog.With(
		"correlation_id", correlationID,
	)

	if r.Method != http.MethodGet {
		logger.Warn(
			"method not allowed",
			"method", r.Method,
		)

		writeError(
			w,
			http.StatusMethodNotAllowed,
			CodeMethodNotAllowed,
			"method not allowed",
			nil,
		)
		return
	}

	limit := 20
	page := 1

	limitStr := r.URL.Query().Get("limit")
	pageStr := r.URL.Query().Get("page")

	if limitStr != "" {
		value, err := strconv.Atoi(limitStr)

		if err != nil ||
			value <= 0 ||
			value > 100 {

			writeError(
				w,
				http.StatusBadRequest,
				CodeBadRequest,
				"limit must be between 1 and 100",
				nil,
			)
			return
		}

		limit = value
	}

	if pageStr != "" {
		value, err := strconv.Atoi(pageStr)

		if err != nil || value <= 0 {
			writeError(
				w,
				http.StatusBadRequest,
				CodeBadRequest,
				"page must be greater than 0",
				nil,
			)
			return
		}

		page = value
	}

	offset := (page - 1) * limit

	userID := r.URL.Query().Get("user_id")

	if userID == "" {
		writeError(
			w,
			http.StatusBadRequest,
			CodeBadRequest,
			"user_id is required",
			map[string]string{
				"user_id": "required",
			},
		)
		return
	}

	status := r.URL.Query().Get("status")

	if status != "" &&
		status != "pending" &&
		status != "sent" &&
		status != "failed" {

		writeError(
			w,
			http.StatusBadRequest,
			CodeBadRequest,
			"invalid status",
			map[string]string{
				"status": "must be pending, sent or failed",
			},
		)
		return
	}

	eventType := r.URL.Query().Get(
		"event_type",
	)

	if eventType != "" &&
		eventType != "created" &&
		eventType != "paid" &&
		eventType != "shipped" {

		writeError(
			w,
			http.StatusBadRequest,
			CodeBadRequest,
			"invalid event_type",
			map[string]string{
				"event_type": "must be created, paid or shipped",
			},
		)
		return
	}

	logger.Info(
		"get events request",
		"user_id", userID,
		"status", status,
		"event_type", eventType,
		"page", page,
		"limit", limit,
		"offset", offset,
	)

	events, err := service.GetEventsService(
		r.Context(),
		repo,
		status,
		userID,
		eventType,
		limit,
		offset,
	)

	if err != nil {
		logger.Error(
			"failed to get events",
			"error", err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			CodeInternal,
			"internal server error",
			nil,
		)
		return
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	if err := json.NewEncoder(w).Encode(events); err != nil {
		logger.Error(
			"failed to encode events",
			"error", err,
		)
	}
}

// getEventByIDHandler godoc
// @Summary Получить событие по ID
// @Description Возвращает одно событие по UUID
// @Tags events
// @Produce json
// @Param event_id path string true "UUID события"
// @Success 200 {object} domain.EventResponse
// @Failure 400 {object} domain.ErrorResponse
// @Failure 404 {object} domain.ErrorResponse
// @Failure 405 {object} domain.ErrorResponse
// @Failure 500 {object} domain.ErrorResponse
// @Router /api/events/{event_id} [get]
func GetEventByIDHandler(
	repo service.EventGetter,
	w http.ResponseWriter,
	r *http.Request,
) {
	correlationID := middleware.GetCorrelationID(
		r.Context(),
	)

	logger := slog.With(
		"correlation_id", correlationID,
	)

	if r.Method != http.MethodGet {
		logger.Warn(
			"method not allowed",
			"method", r.Method,
		)

		writeError(
			w,
			http.StatusMethodNotAllowed,
			CodeMethodNotAllowed,
			"method not allowed",
			nil,
		)
		return
	}

	eventID := strings.TrimPrefix(
		r.URL.Path,
		"/api/events/",
	)

	_, err := uuid.Parse(eventID)

	if err != nil {
		logger.Warn(
			"invalid event id",
			"event_id", eventID,
		)

		writeError(
			w,
			http.StatusBadRequest,
			CodeBadRequest,
			"invalid event id",
			map[string]string{
				"event_id": "must be valid UUID",
			},
		)
		return
	}

	logger.Info(
		"get event by id",
		"event_id", eventID,
	)

	event, err := service.GetEventByIDService(
		r.Context(),
		repo,
		eventID,
	)

	if err != nil {
		if errors.Is(
			err,
			pgx.ErrNoRows,
		) {
			logger.Warn(
				"event not found",
				"event_id", eventID,
			)

			writeError(
				w,
				http.StatusNotFound,
				CodeNotFound,
				"event not found",
				nil,
			)
			return
		}

		logger.Error(
			"failed to get event",
			"event_id", eventID,
			"error", err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			CodeInternal,
			"internal server error",
			nil,
		)
		return
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	if err := json.NewEncoder(w).Encode(event); err != nil {
		logger.Error(
			"failed to encode event",
			"event_id", eventID,
			"error", err,
		)
	}
}
