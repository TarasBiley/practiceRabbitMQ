package repository

import (
	"context"
	"encoding/json"
	models "practiceRabbitMQ/internal/domain"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{
		pool: pool,
	}
}

func (r *Repository) CreateEvent(
	ctx context.Context,
	eventID string,
	correlationID string,
	event models.EventRequest,
) error {

	payloadJSON, err := json.Marshal(event.Payload)
	if err != nil {
		return err
	}

	_, err = r.pool.Exec(
		ctx,
		`
		INSERT INTO events (
			event_id,
			user_id,
			order_id,
			event_type,
			payload,
			correlation_id
		)
		VALUES ($1, $2, $3, $4, $5, $6)
		`,
		eventID,
		event.UserID,
		event.OrderID,
		event.EventType,
		payloadJSON,
		correlationID,
	)

	return err
}

func (r *Repository) GetEvents(
	ctx context.Context,
	status string,
	userID string,
	eventType string,
	limit int,
	offset int,
) ([]models.EventResponse, error) {

	rows, err := r.pool.Query(
		ctx,
		`
		SELECT
			event_id::text,
			user_id,
			order_id,
			event_type,
			payload,
			status,
			retry_count,
			error_message,
			created_at,
			updated_at
		FROM events
		WHERE user_id = $1
		  AND ($2 = '' OR status = $2)
		  AND ($3 = '' OR event_type = $3)
		ORDER BY created_at DESC
		LIMIT $4
		OFFSET $5
		`,
		userID,
		status,
		eventType,
		limit,
		offset,
	)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	events := []models.EventResponse{}

	for rows.Next() {
		var event models.EventResponse

		err := rows.Scan(
			&event.EventID,
			&event.UserID,
			&event.OrderID,
			&event.EventType,
			&event.Payload,
			&event.Status,
			&event.RetryCount,
			&event.ErrorMessage,
			&event.CreatedAt,
			&event.UpdatedAt,
		)

		if err != nil {
			return nil, err
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
}

func (r *Repository) GetEventByID(
	ctx context.Context,

	eventID string,
) (models.EventResponse, error) {

	var event models.EventResponse

	err := r.pool.QueryRow(
		ctx,
		`SELECT event_id::text,
	        user_id,
	        order_id,
	        event_type,
	        payload,
	        status,
	        retry_count,
	        error_message,
	        created_at,
	        updated_at
	 FROM events
	 WHERE event_id = $1`,
		eventID,
	).Scan(
		&event.EventID,
		&event.UserID,
		&event.OrderID,
		&event.EventType,
		&event.Payload,
		&event.Status,
		&event.RetryCount,
		&event.ErrorMessage,
		&event.CreatedAt,
		&event.UpdatedAt,
	)

	if err != nil {
		return models.EventResponse{}, err
	}

	return event, nil

}

func (r *Repository) RetryFailedEvents(
	ctx context.Context,

) (int64, error) {

	result, err := r.pool.Exec(
		ctx,
		`UPDATE events
	SET status = 'pending',
    retry_count = 0,
    error_message = NULL,
    updated_at = NOW()
	WHERE status = 'failed';`,
	)
	if err != nil {
		return 0, err
	}

	count := result.RowsAffected()

	return count, nil

}

func (r *Repository) GetPendingEvents(
	ctx context.Context,

) ([]models.EventMessage, error) {

	rows, err := r.pool.Query(
		ctx,
		`SELECT event_id::text,
				user_id,
				order_id,
				event_type,
				payload,
				COALESCE(correlation_id, '')		
		FROM events
		WHERE status = 'pending'
		ORDER BY created_at
		LIMIT 10`,
	)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	events := []models.EventMessage{}

	for rows.Next() {
		var event models.EventMessage

		err = rows.Scan(
			&event.EventID,
			&event.UserID,
			&event.OrderID,
			&event.EventType,
			&event.Payload,
			&event.CorrelationID,
		)
		if err != nil {
			return nil, err
		}
		events = append(events, event)

	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
}

func (r *Repository) MarkEventSent(
	ctx context.Context,

	eventID string,
) error {

	_, err := r.pool.Exec(
		ctx,
		`UPDATE events
		 SET status = 'sent',
		     updated_at = NOW()
		 WHERE event_id = $1`,
		eventID,
	)

	return err
}

func (r *Repository) IncrementRetryCount(
	ctx context.Context,
	eventID string,
) (int, error) {

	var retryCount int

	err := r.pool.QueryRow(
		ctx,
		`UPDATE events
		 SET retry_count = retry_count + 1,
		     updated_at = NOW()
		 WHERE event_id = $1
		 RETURNING retry_count`,
		eventID,
	).Scan(&retryCount)

	if err != nil {
		return 0, err
	}

	return retryCount, nil
}

func (r *Repository) MarkEventFailed(
	ctx context.Context,
	eventID string,
) error {

	_, err := r.pool.Exec(
		ctx,
		`UPDATE events
		 SET status = 'failed',
		     error_message = 'notification sending failed',
		     updated_at = NOW()
		 WHERE event_id = $1`,
		eventID,
	)

	return err
}
