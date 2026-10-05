package handler

import (
	"encoding/json"
	"net/http"

	models "practiceRabbitMQ/internal/domain"
)

const (
	CodeBadRequest       = "BAD_REQUEST"
	CodeValidation       = "VALIDATION_ERROR"
	CodeMethodNotAllowed = "METHOD_NOT_ALLOWED"
	CodeInternal         = "INTERNAL_ERROR"
	CodeNotFound         = "NOT_FOUND"
)

func writeError(
	w http.ResponseWriter,
	status int,
	code string,
	message string,
	fields map[string]string,
) {
	if fields == nil {
		fields = map[string]string{}
	}

	response := models.ErrorResponse{
		Error: models.ErrorDetail{
			Code:    code,
			Message: message,
			Fields:  fields,
		},
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(response)
}
