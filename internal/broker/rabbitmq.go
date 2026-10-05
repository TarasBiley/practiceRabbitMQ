package broker

import (
	"context"
	"fmt"
	"log/slog"
	"practiceRabbitMQ/internal/service"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type RabbitConnection interface {
	NotifyClose(chan *amqp091.Error) chan *amqp091.Error
	Close() error
}

type RabbitChannel interface {
	MessagePublisher
	MessageConsumer
	Close() error
}

type RabbitRepository interface {
	service.PendingEventReader
	ConsumerRepository
}

type RabbitConnector func() (
	RabbitConnection,
	RabbitChannel,
	error,
)

func realRabbitConnector(rabbitURL string) RabbitConnector {
	return func() (
		RabbitConnection,
		RabbitChannel,
		error,
	) {
		conn, channel, err := connectRabbitMQWithRetry(
			rabbitURL,
		)

		return conn, channel, err
	}
}

func setupRabbitMQ(
	rabbitURL string,
) (
	*amqp091.Connection,
	*amqp091.Channel,
	error,
) {
	rabbitConn, err := amqp091.Dial(
		rabbitURL,
	)
	if err != nil {
		return nil, nil, err
	}

	slog.Info(
		"connected to RabbitMQ",
	)

	rabbitChannel, err := rabbitConn.Channel()
	if err != nil {
		_ = rabbitConn.Close()
		return nil, nil, err
	}

	slog.Info(
		"RabbitMQ channel created",
	)

	err = rabbitChannel.ExchangeDeclare(
		"orders.exchange",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		slog.Error(
			"exchange declare error",
			"exchange", "orders.exchange",
			"error", err,
		)

		return nil, nil, err
	}

	slog.Info(
		"exchange created",
		"exchange", "orders.exchange",
	)

	err = rabbitChannel.ExchangeDeclare(
		"notifications.dlx",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		slog.Error(
			"DLX declare error",
			"exchange", "notifications.dlx",
			"error", err,
		)

		return nil, nil, err
	}

	slog.Info(
		"DLX created",
		"exchange", "notifications.dlx",
	)

	_, err = rabbitChannel.QueueDeclare(
		"notifications.dlq",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		slog.Error(
			"DLQ declare error",
			"queue", "notifications.dlq",
			"error", err,
		)

		return nil, nil, err
	}

	slog.Info(
		"DLQ created",
		"queue", "notifications.dlq",
	)

	err = rabbitChannel.QueueBind(
		"notifications.dlq",
		"failed",
		"notifications.dlx",
		false,
		nil,
	)

	if err != nil {
		slog.Error(
			"DLQ bind error",
			"queue", "notifications.dlq",
			"exchange", "notifications.dlx",
			"routing_key", "failed",
			"error", err,
		)

		return nil, nil, err
	}

	slog.Info(
		"DLQ bound",
		"queue", "notifications.dlq",
		"exchange", "notifications.dlx",
		"routing_key", "failed",
	)

	_, err = rabbitChannel.QueueDeclare(
		"notifications.queue",
		true,
		false,
		false,
		false,
		amqp091.Table{
			"x-dead-letter-exchange":    "notifications.dlx",
			"x-dead-letter-routing-key": "failed",
		},
	)

	if err != nil {
		slog.Error(
			"notifications queue declare error",
			"queue", "notifications.queue",
			"error", err,
		)

		return nil, nil, err
	}

	slog.Info(
		"notifications queue created",
		"queue", "notifications.queue",
	)

	err = rabbitChannel.QueueBind(
		"notifications.queue",
		"notifications",
		"orders.exchange",
		false,
		nil,
	)

	if err != nil {
		slog.Error(
			"notifications queue bind error",
			"queue", "notifications.queue",
			"exchange", "orders.exchange",
			"routing_key", "notifications",
			"error", err,
		)

		return nil, nil, err
	}

	slog.Info(
		"notifications queue bound",
		"queue", "notifications.queue",
		"exchange", "orders.exchange",
		"routing_key", "notifications",
	)

	return rabbitConn, rabbitChannel, nil
}

func connectRabbitMQWithRetry(
	rabbitURL string,
) (
	*amqp091.Connection,
	*amqp091.Channel,
	error,
) {
	return connectRabbitMQWithRetryUsing(
		func() (
			*amqp091.Connection,
			*amqp091.Channel,
			error,
		) {
			return setupRabbitMQ(
				rabbitURL,
			)
		},
		time.Sleep,
	)
}

func connectRabbitMQWithRetryUsing(
	connect func() (
		*amqp091.Connection,
		*amqp091.Channel,
		error,
	),
	sleep func(time.Duration),
) (
	*amqp091.Connection,
	*amqp091.Channel,
	error,
) {
	delay := 1 * time.Second

	var lastErr error

	for attempt := 1; attempt <= 5; attempt++ {
		conn, channel, err := connect()

		if err == nil {
			return conn, channel, nil
		}

		lastErr = err

		if attempt == 5 {
			break
		}

		slog.Warn(
			"RabbitMQ connection failed",
			"attempt", attempt,
			"retry_in", delay,
			"error", err,
		)

		sleep(delay)

		delay = delay * 2
	}

	return nil, nil, fmt.Errorf(
		"could not connect to RabbitMQ: %w",
		lastErr,
	)
}

func StartRabbitMQ(
	ctx context.Context,
	repo RabbitRepository,
	rabbitURL string,
) {
	startRabbitMQWithConnector(
		ctx,
		repo,
		realRabbitConnector(rabbitURL),
	)
}

func startRabbitMQWithConnector(
	ctx context.Context,
	repo RabbitRepository,
	connect RabbitConnector,
) {
	for {
		select {
		case <-ctx.Done():
			slog.Info(
				"RabbitMQ manager stopped",
			)
			return

		default:
		}

		conn, rabbitChannel, err := connect()

		if err != nil {
			slog.Error(
				"RabbitMQ connection error",
				"error", err,
			)

			continue
		}

		closeChan := make(
			chan *amqp091.Error,
			1,
		)

		conn.NotifyClose(
			closeChan,
		)

		publisherCtx, cancelPublisher :=
			context.WithCancel(ctx)

		startPublisher(
			publisherCtx,
			repo,
			rabbitChannel,
		)

		err = startConsumer(
			repo,
			rabbitChannel,
		)

		if err != nil {
			slog.Error(
				"consumer start error",
				"error", err,
			)

			cancelPublisher()

			_ = rabbitChannel.Close()
			_ = conn.Close()

			continue
		}

		select {
		case <-ctx.Done():
			cancelPublisher()

			_ = rabbitChannel.Close()
			_ = conn.Close()

			slog.Info(
				"RabbitMQ manager stopped",
			)

			return

		case err := <-closeChan:
			slog.Warn(
				"RabbitMQ connection lost",
				"error", err,
			)

			cancelPublisher()

			_ = rabbitChannel.Close()
			_ = conn.Close()

			// Цикл начинается заново,
			// поэтому приложение попробует
			// подключиться к RabbitMQ снова.
		}
	}
}
