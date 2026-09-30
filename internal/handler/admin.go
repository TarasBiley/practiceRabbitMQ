package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"practiceRabbitMQ/internal/service"
)

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
