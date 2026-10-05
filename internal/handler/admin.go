package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"practiceRabbitMQ/internal/middleware"
	"practiceRabbitMQ/internal/service"
)

// retryPendingHandler godoc
// @Summary Повторно поставить failed события в очередь
// @Description Переводит события со статусом failed обратно в pending и сбрасывает retry_count
// @Tags admin
// @Produce json
// @Success 200 {object} map[string]int64
// @Failure 405 {object} domain.ErrorResponse
// @Failure 500 {object} domain.ErrorResponse
// @Router /api/admin/retry-pending [post]
func RetryPendingHandler(
	repo service.EventRetryer,
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

	count, err := service.RetryFailedEventsService(
		r.Context(),
		repo,
	)

	if err != nil {
		logger.Error(
			"failed to retry events",
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
		"failed events moved to pending",
		"retried", count,
	)

	response := map[string]int64{
		"retried": count,
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		logger.Error(
			"failed to encode response",
			"error", err,
		)
	}
}
