package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"practiceRabbitMQ/internal/service"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type MessagePublisher interface {
	PublishWithContext(
		ctx context.Context,
		exchange string,
		key string,
		mandatory bool,
		immediate bool,
		msg amqp091.Publishing,
	) error
}

func publishPendingEvent(
	ctx context.Context,
	repo service.PendingEventReader,
	rabbitChannel MessagePublisher,
) error {

	events, err := service.PublishPendingEventService(ctx, repo)

	if err != nil {
		return err
	}

	for _, event := range events {

		body, err := json.Marshal(event)
		if err != nil {
			return err
		}

		err = rabbitChannel.PublishWithContext(
			ctx,
			"orders.exchange",
			"notifications",
			false,
			false,
			amqp091.Publishing{
				ContentType:   "application/json",
				DeliveryMode:  amqp091.Persistent,
				CorrelationId: event.CorrelationID,
				Body:          body,
			},
		)
		if err != nil {
			return err
		}
		fmt.Println("published event:", event.EventID)
	}

	return nil
}

func startPublisher(
	ctx context.Context,
	repo service.PendingEventReader,
	rabbitChannel MessagePublisher,
) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		runPublisher(ctx, repo, rabbitChannel, ticker.C)
	}()
	return done
}

func runPublisher(
	ctx context.Context,
	repo service.PendingEventReader,
	rabbitChannel MessagePublisher,
	ticks <-chan time.Time,
) {
	for {
		select {
		case _, ok := <-ticks:
			if !ok || ctx.Err() != nil {
				return
			}
			if err := publishPendingEvent(ctx, repo, rabbitChannel); err != nil {
				fmt.Println("publisher error:", err)
			}
		case <-ctx.Done():
			fmt.Println("publisher stopped")
			return
		}
	}
}
