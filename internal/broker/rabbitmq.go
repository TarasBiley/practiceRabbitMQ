package broker

import (
	"context"
	"fmt"
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
		conn, channel, err := connectRabbitMQWithRetry(rabbitURL)

		return conn, channel, err
	}
}

func setupRabbitMQ(rabbitURL string) (*amqp091.Connection, *amqp091.Channel, error) {

	rabbitConn, err := amqp091.Dial(
		rabbitURL,
	)
	if err != nil {
		return nil, nil, err
	}

	fmt.Println("connected to RabbitMQ")

	rabbitChannel, err := rabbitConn.Channel()
	if err != nil {
		rabbitConn.Close()
		return nil, nil, err
	}

	fmt.Println("RabbitMQ channel created")

	err = rabbitChannel.ExchangeDeclare(
		"orders.exchange", // имя
		"direct",          // тип exchange
		true,              // durable
		false,             // autoDelete
		false,             // internal
		false,             // noWait
		nil,               // arguments
	)

	if err != nil {
		fmt.Println("exchange declare error:", err)
		return nil, nil, err
	}

	fmt.Println("orders.exchange created")

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
		fmt.Println("DLX declare error:", err)
		return nil, nil, err
	}

	fmt.Println("notifications.dlx created")

	_, err = rabbitChannel.QueueDeclare(
		"notifications.dlq", // имя очереди
		true,                // durable
		false,               // autoDelete
		false,               // exclusive
		false,               // noWait
		nil,                 // arguments
	)

	if err != nil {
		fmt.Println("DLQ declare error:", err)
		return nil, nil, err
	}

	fmt.Println("notifications.dlq created")

	err = rabbitChannel.QueueBind(
		"notifications.dlq", // queue
		"failed",            // routing key
		"notifications.dlx", // exchange
		false,
		nil,
	)

	if err != nil {
		fmt.Println("DLQ bind error:", err)
		return nil, nil, err
	}

	fmt.Println("notifications.dlq bound to notifications.dlx")

	_, err = rabbitChannel.QueueDeclare(
		"notifications.queue",
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		amqp091.Table{
			"x-dead-letter-exchange":    "notifications.dlx",
			"x-dead-letter-routing-key": "failed",
		},
	)

	if err != nil {
		fmt.Println("notifications queue declare error:", err)
		return nil, nil, err
	}

	fmt.Println("notifications.queue created")

	err = rabbitChannel.QueueBind(
		"notifications.queue", // queue
		"notifications",       // routing key
		"orders.exchange",     // exchange
		false,
		nil,
	)

	if err != nil {
		fmt.Println("notifications queue bind error:", err)
		return nil, nil, err
	}

	fmt.Println("notifications.queue bound to orders.exchange")

	return rabbitConn, rabbitChannel, nil
}

func connectRabbitMQWithRetry(rabbitURL string) (*amqp091.Connection, *amqp091.Channel, error) {
	return connectRabbitMQWithRetryUsing(func() (*amqp091.Connection, *amqp091.Channel, error) {
		return setupRabbitMQ(rabbitURL)
	}, time.Sleep)
}

func connectRabbitMQWithRetryUsing(
	connect func() (*amqp091.Connection, *amqp091.Channel, error),
	sleep func(time.Duration),
) (*amqp091.Connection, *amqp091.Channel, error) {
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

		fmt.Println(
			"RabbitMQ connection failed, attempt:",
			attempt,
			"retry in:",
			delay,
		)

		sleep(delay)

		delay = delay * 2
	}

	return nil, nil, fmt.Errorf("could not connect to RabbitMQ: %w", lastErr)
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

		// Если приложение попросили остановиться —
		// больше не подключаемся.
		select {
		case <-ctx.Done():
			fmt.Println("RabbitMQ manager stopped")
			return
		default:
		}

		conn, rabbitChannel, err := connect()
		if err != nil {
			fmt.Println(
				"RabbitMQ connection error:",
				err,
			)

			continue
		}

		closeChan := make(
			chan *amqp091.Error,
			1,
		)

		conn.NotifyClose(closeChan)

		// Отдельный context для publisher.
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
			fmt.Println(
				"consumer start error:",
				err,
			)

			cancelPublisher()

			_ = rabbitChannel.Close()
			_ = conn.Close()

			continue
		}

		select {

		// Всё приложение остановилось.
		case <-ctx.Done():

			cancelPublisher()

			_ = rabbitChannel.Close()
			_ = conn.Close()

			fmt.Println(
				"RabbitMQ manager stopped",
			)

			return

		// RabbitMQ соединение оборвалось.
		case err := <-closeChan:

			fmt.Println(
				"RabbitMQ connection lost:",
				err,
			)

			cancelPublisher()

			_ = rabbitChannel.Close()
			_ = conn.Close()

			// После этого for начинается заново
			// и происходит reconnect.
		}
	}
}
