package broker

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand"
	models "practiceRabbitMQ/internal/domain"
	"practiceRabbitMQ/internal/service"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type MessageConsumer interface {
	Consume(
		queue string,
		consumer string,
		autoAck bool,
		exclusive bool,
		noLocal bool,
		noWait bool,
		args amqp091.Table,
	) (<-chan amqp091.Delivery, error)
}

type ConsumerRepository interface {
	service.EventSentMarker
	service.RetryCounter
	service.EventFailedMarker
}

func startConsumer(
	repo ConsumerRepository,
	channel MessageConsumer,
) error {
	_, err := startConsumerWithFailureDecider(
		repo,
		channel,
		func() bool {
			return rand.Intn(100) < 10
		},
	)

	return err
}

func startConsumerWithFailureDecider(
	repo ConsumerRepository,
	channel MessageConsumer,
	shouldFail func() bool,
) (<-chan struct{}, error) {
	messages, err := channel.Consume(
		"notifications.queue",
		"",
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return nil, err
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		for msg := range messages {
			err := processMessage(
				repo,
				msg,
				shouldFail(),
			)

			if err != nil {
				slog.Error(
					"consumer error",
					"correlation_id", msg.CorrelationId,
					"error", err,
				)
			}
		}
	}()

	return done, nil
}

func processMessage(
	repo ConsumerRepository,
	msg amqp091.Delivery,
	shouldFail bool,
) error {
	var event models.EventMessage

	err := json.Unmarshal(msg.Body, &event)
	if err != nil {
		slog.Error(
			"failed to decode message",
			"correlation_id", msg.CorrelationId,
			"error", err,
		)

		return err
	}

	logger := slog.With(
		"correlation_id", msg.CorrelationId,
		"event_id", event.EventID,
	)

	logger.Info("consumer received event")

	time.Sleep(100 * time.Millisecond)

	if shouldFail {
		logger.Warn("simulated notification error")

		retryCount, err := service.IncrementRetryCountService(
			context.Background(),
			repo,
			event.EventID,
		)

		if err != nil {
			logger.Error(
				"failed to increment retry count",
				"error", err,
			)

			return err
		}

		logger.Warn(
			"notification retry",
			"retry_count", retryCount,
		)

		if retryCount >= 3 {
			err = service.MarkEventFailedService(
				context.Background(),
				repo,
				event.EventID,
			)

			if err != nil {
				logger.Error(
					"failed to mark event as failed",
					"error", err,
				)

				return err
			}

			logger.Error(
				"event marked as failed",
				"retry_count", retryCount,
			)
		}

		return msg.Nack(false, false)
	}

	err = service.MarkEventSentService(
		context.Background(),
		repo,
		event.EventID,
	)

	if err != nil {
		logger.Error(
			"failed to mark event as sent",
			"error", err,
		)

		return err
	}

	err = msg.Ack(false)
	if err != nil {
		logger.Error(
			"failed to ack message",
			"error", err,
		)

		return err
	}

	logger.Info("event sent")

	return nil
}
