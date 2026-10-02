package broker

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"practiceRabbitMQ/internal/domain"

	amqp "github.com/rabbitmq/amqp091-go"
)

func pendingEvent(id string) domain.EventMessage {
	return domain.EventMessage{
		EventID: id, UserID: "user-4", OrderID: "order-9", EventType: "paid",
		CorrelationID: "correlation-" + id, Payload: json.RawMessage(`{"amount":125,"currency":"RUB"}`),
	}
}

func TestPublishPendingEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := []domain.EventMessage{pendingEvent("event-1"), pendingEvent("event-2"), pendingEvent("event-3")}
	repo := &fakeRepository{pendingEvents: events}
	publisher := &fakePublisher{}
	if err := publishPendingEvent(ctx, repo, publisher); err != nil {
		t.Fatal(err)
	}
	if len(repo.pendingContexts) != 1 || repo.pendingContexts[0] != ctx {
		t.Fatal("expected one repository query with the caller's context")
	}
	if len(publisher.calls) != len(events) {
		t.Fatalf("publications = %d, want %d", len(publisher.calls), len(events))
	}
	for i, call := range publisher.calls {
		if call.ctx != ctx || call.exchange != "orders.exchange" || call.key != "notifications" || call.mandatory || call.immediate {
			t.Errorf("publication %d has incorrect context or routing: %+v", i, call)
		}
		if call.message.ContentType != "application/json" || call.message.DeliveryMode != amqp.Persistent || call.message.CorrelationId != events[i].CorrelationID {
			t.Errorf("publication %d has incorrect metadata: %+v", i, call.message)
		}
		var got domain.EventMessage
		if err := json.Unmarshal(call.message.Body, &got); err != nil {
			t.Fatalf("publication %d is not valid JSON: %v", i, err)
		}
		if !reflect.DeepEqual(got, events[i]) {
			t.Errorf("publication %d body = %+v, want %+v", i, got, events[i])
		}
	}
}

func TestPublishPendingEventErrors(t *testing.T) {
	repositoryErr := errors.New("repository unavailable")
	publisherErr := errors.New("publish failed")
	tests := []struct {
		name          string
		events        []domain.EventMessage
		repoErr       error
		publishErrors map[int]error
		wantErr       error
		wantCalls     int
	}{
		{name: "nil events"},
		{name: "empty events", events: []domain.EventMessage{}},
		{name: "repository error", repoErr: repositoryErr, wantErr: repositoryErr},
		{name: "first publication fails", events: []domain.EventMessage{pendingEvent("1"), pendingEvent("2")}, publishErrors: map[int]error{1: publisherErr}, wantErr: publisherErr, wantCalls: 1},
		{name: "second publication fails", events: []domain.EventMessage{pendingEvent("1"), pendingEvent("2"), pendingEvent("3")}, publishErrors: map[int]error{2: publisherErr}, wantErr: publisherErr, wantCalls: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeRepository{pendingEvents: tt.events, pendingErr: tt.repoErr}
			publisher := &fakePublisher{errors: tt.publishErrors}
			err := publishPendingEvent(context.Background(), repo, publisher)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
			if len(publisher.calls) != tt.wantCalls {
				t.Fatalf("publications = %d, want %d", len(publisher.calls), tt.wantCalls)
			}
		})
	}
}

func TestPublishPendingEventInvalidPayload(t *testing.T) {
	event := pendingEvent("event-1")
	event.Payload = json.RawMessage(`{invalid}`)
	publisher := &fakePublisher{}
	err := publishPendingEvent(context.Background(), &fakeRepository{pendingEvents: []domain.EventMessage{event}}, publisher)
	if err == nil {
		t.Fatal("expected malformed payload to fail JSON encoding")
	}
	if len(publisher.calls) != 0 {
		t.Fatal("malformed event must not be published")
	}
}

func TestRunPublisherTicksAndCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ticks := make(chan time.Time)
	published := make(chan struct{}, 2)
	repo := &fakeRepository{pendingEvents: []domain.EventMessage{pendingEvent("event-1")}}
	publisher := &fakePublisher{
		errors:    map[int]error{1: errors.New("temporary publish failure")},
		onPublish: func() { published <- struct{}{} },
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		runPublisher(ctx, repo, publisher, ticks)
	}()
	for range 2 {
		select {
		case ticks <- time.Time{}:
		case <-time.After(3 * time.Second):
			t.Fatal("publisher did not accept a tick")
		}
		select {
		case <-published:
		case <-time.After(3 * time.Second):
			t.Fatal("publisher did not publish on a tick")
		}
	}
	cancel()
	waitForDone(t, done)
	if len(publisher.calls) != 2 || len(repo.pendingContexts) != 2 {
		t.Fatalf("publications = %d, queries = %d; want two of each", len(publisher.calls), len(repo.pendingContexts))
	}
}

func TestRunPublisherCancelledWithPendingTick(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ticks := make(chan time.Time, 1)
	ticks <- time.Time{}
	repo := &fakeRepository{pendingEvents: []domain.EventMessage{pendingEvent("event-1")}}
	publisher := &fakePublisher{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		runPublisher(ctx, repo, publisher, ticks)
	}()
	waitForDone(t, done)
	if len(publisher.calls) != 0 || len(repo.pendingContexts) != 0 {
		t.Fatal("cancelled publisher must not fetch or publish events")
	}
}

func TestStartPublisherContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	repo := &fakeRepository{pendingEvents: []domain.EventMessage{pendingEvent("event-1")}}
	publisher := &fakePublisher{}
	waitForDone(t, startPublisher(ctx, repo, publisher))
	if len(publisher.calls) != 0 || len(repo.pendingContexts) != 0 {
		t.Fatal("cancelled publisher must not fetch or publish events")
	}
}
