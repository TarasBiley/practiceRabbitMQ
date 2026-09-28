package broker

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type fakeRabbitConnection struct {
	closeError  *amqp.Error
	closeNotify bool
	closeCalls  int
	notifyCalls int
}

func (f *fakeRabbitConnection) NotifyClose(ch chan *amqp.Error) chan *amqp.Error {
	f.notifyCalls++
	if f.closeError != nil {
		ch <- f.closeError
	} else if f.closeNotify {
		close(ch)
	}
	return ch
}

func (f *fakeRabbitConnection) Close() error {
	f.closeCalls++
	return nil
}

type fakeRabbitChannel struct {
	fakePublisher
	consumeErr   error
	consumeCalls int
	closeCalls   int
	onConsume    func()
}

func (f *fakeRabbitChannel) Consume(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
	f.consumeCalls++
	if f.onConsume != nil {
		f.onConsume()
	}
	if f.consumeErr != nil {
		return nil, f.consumeErr
	}
	messages := make(chan amqp.Delivery)
	close(messages)
	return messages, nil
}

func (f *fakeRabbitChannel) Close() error {
	f.closeCalls++
	return nil
}

func TestConnectRabbitMQWithRetry(t *testing.T) {
	wantErr := errors.New("rabbitmq unavailable")
	tests := []struct {
		name      string
		failures  int
		wantCalls int
		wantWaits []time.Duration
	}{
		{name: "first attempt succeeds", wantCalls: 1},
		{name: "two failures then success", failures: 2, wantCalls: 3, wantWaits: []time.Duration{time.Second, 2 * time.Second}},
		{name: "last attempt succeeds", failures: 4, wantCalls: 5, wantWaits: []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}},
		{name: "attempts exhausted", failures: 5, wantCalls: 5, wantWaits: []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			var waits []time.Duration
			wantConn, wantChannel := new(amqp.Connection), new(amqp.Channel)
			conn, channel, err := connectRabbitMQWithRetryUsing(func() (*amqp.Connection, *amqp.Channel, error) {
				calls++
				if calls <= tt.failures {
					return nil, nil, wantErr
				}
				return wantConn, wantChannel, nil
			}, func(delay time.Duration) { waits = append(waits, delay) })
			if tt.failures == 5 {
				if !errors.Is(err, wantErr) || conn != nil || channel != nil {
					t.Fatalf("exhausted retries: conn = %p, channel = %p, error = %v", conn, channel, err)
				}
			} else if err != nil || conn != wantConn || channel != wantChannel {
				t.Fatalf("successful retry: conn = %p, channel = %p, error = %v", conn, channel, err)
			}
			if calls != tt.wantCalls || !reflect.DeepEqual(waits, tt.wantWaits) {
				t.Errorf("calls = %d, waits = %v; want %d and %v", calls, waits, tt.wantCalls, tt.wantWaits)
			}
		})
	}
}

func TestStartRabbitMQReconnect(t *testing.T) {
	tests := []struct {
		name        string
		closeError  *amqp.Error
		closeNotify bool
		consumeErr  error
	}{
		{name: "connection lost", closeError: &amqp.Error{Code: 320, Reason: "connection lost"}},
		{name: "notification channel closed", closeNotify: true},
		{name: "consumer startup failed", consumeErr: errors.New("consume failed")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			firstConn := &fakeRabbitConnection{closeError: tt.closeError, closeNotify: tt.closeNotify}
			firstChannel := &fakeRabbitChannel{consumeErr: tt.consumeErr}
			secondConn := &fakeRabbitConnection{}
			secondChannel := &fakeRabbitChannel{onConsume: cancel}
			calls := 0
			done := make(chan struct{})
			go func() {
				defer close(done)
				startRabbitMQWithConnector(ctx, &fakeRepository{}, func() (RabbitConnection, RabbitChannel, error) {
					calls++
					if calls == 1 {
						return firstConn, firstChannel, nil
					}
					return secondConn, secondChannel, nil
				})
			}()
			waitForDone(t, done)
			if calls != 2 {
				t.Errorf("connection attempts = %d, want 2", calls)
			}
			for i, conn := range []*fakeRabbitConnection{firstConn, secondConn} {
				if conn.closeCalls != 1 || conn.notifyCalls != 1 {
					t.Errorf("connection %d: closes = %d, notifications = %d; want one each", i, conn.closeCalls, conn.notifyCalls)
				}
			}
			for i, channel := range []*fakeRabbitChannel{firstChannel, secondChannel} {
				if channel.closeCalls != 1 || channel.consumeCalls != 1 {
					t.Errorf("channel %d: closes = %d, consumes = %d; want one each", i, channel.closeCalls, channel.consumeCalls)
				}
			}
		})
	}
}

func TestStartRabbitMQRetriesConnectorError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &fakeRabbitConnection{}
	channel := &fakeRabbitChannel{onConsume: cancel}
	calls := 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		startRabbitMQWithConnector(ctx, &fakeRepository{}, func() (RabbitConnection, RabbitChannel, error) {
			calls++
			if calls == 1 {
				return nil, nil, errors.New("temporary connection failure")
			}
			return conn, channel, nil
		})
	}()
	waitForDone(t, done)
	if calls != 2 || channel.consumeCalls != 1 || conn.closeCalls != 1 || channel.closeCalls != 1 {
		t.Fatalf("calls = %d, consumes = %d, closes = %d/%d; want 2, 1, 1/1", calls, channel.consumeCalls, conn.closeCalls, channel.closeCalls)
	}
}

func TestStartRabbitMQCancelledBeforeConnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		startRabbitMQWithConnector(ctx, &fakeRepository{}, func() (RabbitConnection, RabbitChannel, error) {
			calls++
			return nil, nil, errors.New("unexpected connection attempt")
		})
	}()
	waitForDone(t, done)
	if calls != 0 {
		t.Fatalf("connection attempts after cancellation = %d, want 0", calls)
	}
}
