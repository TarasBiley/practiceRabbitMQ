package service

import (
	"context"
	models "practiceRabbitMQ/internal/domain"

	"github.com/google/uuid"
)

type EventRepository interface {
	CreateEvent(
		ctx context.Context,
		eventID string,
		correlationID string,
		event models.EventRequest,
	) error
}

type EventLister interface {
	GetEvents(
		ctx context.Context,
		status string,
		userID string,
		limit int,
		offset int,
	) ([]models.EventResponse, error)
}

type EventGetter interface {
	GetEventByID(
		ctx context.Context,
		eventID string,
	) (models.EventResponse, error)
}

type EventRetryer interface {
	RetryFailedEvents(
		ctx context.Context,
	) (int64, error)
}

type PendingEventReader interface {
	GetPendingEvents(
		ctx context.Context,
	) ([]models.EventMessage, error)
}

type EventSentMarker interface {
	MarkEventSent(
		ctx context.Context,
		eventID string,
	) error
}

type RetryCounter interface {
	IncrementRetryCount(
		ctx context.Context,
		eventID string,
	) (int, error)
}

type EventFailedMarker interface {
	MarkEventFailed(
		ctx context.Context,
		eventID string,
	) error
}

func CreateEventService(
	ctx context.Context,
	repo EventRepository,
	event models.EventRequest,
) (string, error) {

	eventID := uuid.New().String()
	correlationID := uuid.New().String()

	err := repo.CreateEvent(
		ctx,
		eventID,
		correlationID,
		event,
	)

	if err != nil {
		return "", err
	}

	return eventID, nil
}

func ValidateEvent(event models.EventRequest) map[string]string {
	fields := make(map[string]string)

	if event.UserID == "" {
		fields["user_id"] = "required"
	}

	if event.OrderID == "" {
		fields["order_id"] = "required"
	}

	if event.EventType != "created" &&
		event.EventType != "paid" &&
		event.EventType != "shipped" {

		fields["event_type"] = "must be created, paid or shipped"
	}

	return fields
}

func GetEventsService(
	ctx context.Context,
	repo EventLister,
	status string,
	userID string,
	limit int,
	offset int,
) ([]models.EventResponse, error) {

	return repo.GetEvents(
		ctx,
		status,
		userID,
		limit,
		offset,
	)
}
func GetEventByIDService(
	ctx context.Context,
	repo EventGetter,
	eventID string,
) (models.EventResponse, error) {

	return repo.GetEventByID(
		ctx,
		eventID,
	)
}

func RetryFailedEventsService(
	ctx context.Context,
	repo EventRetryer,
) (int64, error) {

	return repo.RetryFailedEvents(ctx)
}

func PublishPendingEventService(
	ctx context.Context,
	repo PendingEventReader,
) ([]models.EventMessage, error) {

	return repo.GetPendingEvents(ctx)
}

func MarkEventSentService(
	ctx context.Context,
	repo EventSentMarker,
	eventID string,
) error {

	return repo.MarkEventSent(
		ctx,
		eventID,
	)
}

func IncrementRetryCountService(
	ctx context.Context,
	repo RetryCounter,
	eventID string,
) (int, error) {

	return repo.IncrementRetryCount(
		ctx,
		eventID,
	)
}

func MarkEventFailedService(
	ctx context.Context,
	repo EventFailedMarker,
	eventID string,
) error {

	return repo.MarkEventFailed(
		ctx,
		eventID,
	)
}
