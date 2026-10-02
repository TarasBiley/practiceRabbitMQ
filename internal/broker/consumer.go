package broker

import (
	"context"
	"encoding/json"
	"fmt"
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
	_, err := startConsumerWithFailureDecider(repo, channel, func() bool {
		return rand.Intn(100) < 10
	})
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
				fmt.Println(
					"consumer error:",
					err,
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
		return err
	}

	fmt.Println("consumer received:", event.EventID)

	time.Sleep(100 * time.Millisecond)

	if shouldFail {

		fmt.Println(
			"simulated notification error:",
			event.EventID,
		)

		retryCount, err := service.IncrementRetryCountService(
			context.Background(),
			repo,
			event.EventID,
		)

		if err != nil {
			return err
		}

		if retryCount >= 3 {

			err = service.MarkEventFailedService(
				context.Background(),
				repo,
				event.EventID,
			)

			if err != nil {
				return err
			}
		}

		return msg.Nack(false, false)
	}

	err = service.MarkEventSentService(
		context.Background(),
		repo,
		event.EventID,
	)

	if err != nil {
		return err
	}

	err = msg.Ack(false)
	if err != nil {
		return err
	}

	fmt.Println("event sent:", event.EventID)
	fmt.Println("correlation_id:", msg.CorrelationId)

	return nil
}
