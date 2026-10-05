package middleware

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
)

type contextKey string

const correlationIDKey contextKey = "correlation_id"

func CorrelationID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		correlationID := r.Header.Get("X-Correlation-ID")

		if correlationID == "" {
			correlationID = uuid.NewString()
		}

		ctx := context.WithValue(
			r.Context(),
			correlationIDKey,
			correlationID,
		)

		w.Header().Set(
			"X-Correlation-ID",
			correlationID,
		)

		slog.Info(
			"http request",
			"method", r.Method,
			"path", r.URL.Path,
			"correlation_id", correlationID,
		)

		next.ServeHTTP(
			w,
			r.WithContext(ctx),
		)
	})
}

func GetCorrelationID(ctx context.Context) string {
	correlationID, _ := ctx.Value(
		correlationIDKey,
	).(string)

	return correlationID
}
