package service

import (
	"context"
	models "practiceRabbitMQ/internal/domain"

	"github.com/go-playground/validator/v10"
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
		eventType string,
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
	correlationID string,
	event models.EventRequest,
) (string, error) {

	eventID := uuid.New().String()

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

func ValidateEvent(
	event models.EventRequest,
) map[string]string {

	validate := validator.New()

	err := validate.Struct(event)
	if err == nil {
		return nil
	}

	fields := make(map[string]string)

	validationErrors, ok := err.(validator.ValidationErrors)
	if !ok {
		return fields
	}

	for _, fieldErr := range validationErrors {
		switch fieldErr.Field() {

		case "UserID":
			switch fieldErr.Tag() {
			case "required":
				fields["user_id"] = "required"
			case "max":
				fields["user_id"] = "maximum length is 64"
			}

		case "OrderID":
			switch fieldErr.Tag() {
			case "required":
				fields["order_id"] = "required"
			case "max":
				fields["order_id"] = "maximum length is 64"
			}

		case "EventType":
			switch fieldErr.Tag() {
			case "required":
				fields["event_type"] = "required"
			case "oneof":
				fields["event_type"] = "must be created, paid or shipped"
			}
		}
	}

	return fields
}
func GetEventsService(
	ctx context.Context,
	repo EventLister,
	status string,
	userID string,
	eventType string,
	limit int,
	offset int,
) ([]models.EventResponse, error) {

	return repo.GetEvents(
		ctx,
		status,
		userID,
		eventType,
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
