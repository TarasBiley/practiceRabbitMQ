package broker

import (
	"errors"
	"reflect"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

type acknowledgement struct {
	kind     string
	tag      uint64
	multiple bool
	requeue  bool
}

type fakeAcknowledger struct {
	calls      []acknowledgement
	operations *[]string
	ackErr     error
	nackErr    error
}

func (f *fakeAcknowledger) record(kind string, tag uint64, multiple, requeue bool) {
	f.calls = append(f.calls, acknowledgement{kind, tag, multiple, requeue})
	*f.operations = append(*f.operations, kind)
}

func (f *fakeAcknowledger) Ack(tag uint64, multiple bool) error {
	f.record("ack", tag, multiple, false)
	return f.ackErr
}

func (f *fakeAcknowledger) Nack(tag uint64, multiple, requeue bool) error {
	f.record("nack", tag, multiple, requeue)
	return f.nackErr
}

func (f *fakeAcknowledger) Reject(tag uint64, requeue bool) error {
	f.record("reject", tag, false, requeue)
	return nil
}

type consumeCall struct {
	queue     string
	consumer  string
	autoAck   bool
	exclusive bool
	noLocal   bool
	noWait    bool
	args      amqp.Table
}

type fakeMessageConsumer struct {
	messages <-chan amqp.Delivery
	err      error
	calls    []consumeCall
}

func (f *fakeMessageConsumer) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	f.calls = append(f.calls, consumeCall{queue, consumer, autoAck, exclusive, noLocal, noWait, args})
	return f.messages, f.err
}

func TestProcessMessage(t *testing.T) {
	databaseErr := errors.New("database unavailable")
	ackErr := errors.New("ack failed")
	nackErr := errors.New("nack failed")
	tests := []struct {
		name        string
		shouldFail  bool
		repo        fakeRepository
		ackErr      error
		nackErr     error
		body        string
		wantErr     error
		invalidJSON bool
		wantOps     []string
		wantAck     []acknowledgement
	}{
		{name: "success", wantOps: []string{"sent:event-17", "ack"}, wantAck: []acknowledgement{{kind: "ack", tag: 42}}},
		{name: "first failure", shouldFail: true, repo: fakeRepository{retryCount: 1}, wantOps: []string{"retry:event-17", "nack"}, wantAck: []acknowledgement{{kind: "nack", tag: 42}}},
		{name: "below retry limit", shouldFail: true, repo: fakeRepository{retryCount: 2}, wantOps: []string{"retry:event-17", "nack"}, wantAck: []acknowledgement{{kind: "nack", tag: 42}}},
		{name: "at retry limit", shouldFail: true, repo: fakeRepository{retryCount: 3}, wantOps: []string{"retry:event-17", "failed:event-17", "nack"}, wantAck: []acknowledgement{{kind: "nack", tag: 42}}},
		{name: "above retry limit", shouldFail: true, repo: fakeRepository{retryCount: 4}, wantOps: []string{"retry:event-17", "failed:event-17", "nack"}, wantAck: []acknowledgement{{kind: "nack", tag: 42}}},
		{name: "invalid JSON", body: `{invalid}`, invalidJSON: true},
		{name: "mark sent error", repo: fakeRepository{markSentErr: databaseErr}, wantErr: databaseErr, wantOps: []string{"sent:event-17"}},
		{name: "retry error", shouldFail: true, repo: fakeRepository{retryErr: databaseErr}, wantErr: databaseErr, wantOps: []string{"retry:event-17"}},
		{name: "mark failed error", shouldFail: true, repo: fakeRepository{retryCount: 3, markFailedErr: databaseErr}, wantErr: databaseErr, wantOps: []string{"retry:event-17", "failed:event-17"}},
		{name: "ack error", ackErr: ackErr, wantErr: ackErr, wantOps: []string{"sent:event-17", "ack"}, wantAck: []acknowledgement{{kind: "ack", tag: 42}}},
		{name: "nack error", shouldFail: true, repo: fakeRepository{retryCount: 1}, nackErr: nackErr, wantErr: nackErr, wantOps: []string{"retry:event-17", "nack"}, wantAck: []acknowledgement{{kind: "nack", tag: 42}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := tt.repo
			ack := &fakeAcknowledger{operations: &repo.operations, ackErr: tt.ackErr, nackErr: tt.nackErr}
			body := tt.body
			if body == "" {
				body = `{"event_id":"event-17","user_id":"user-2","order_id":"order-3","event_type":"paid"}`
			}
			err := processMessage(&repo, amqp.Delivery{Acknowledger: ack, DeliveryTag: 42, Body: []byte(body)}, tt.shouldFail)
			if tt.invalidJSON {
				if err == nil {
					t.Fatal("expected invalid JSON to return an error")
				}
			} else if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(repo.operations, tt.wantOps) {
				t.Errorf("operations = %v, want %v", repo.operations, tt.wantOps)
			}
			if !reflect.DeepEqual(ack.calls, tt.wantAck) {
				t.Errorf("acknowledgements = %+v, want %+v", ack.calls, tt.wantAck)
			}
		})
	}
}

func TestStartConsumerProcessesMessages(t *testing.T) {
	repo := &fakeRepository{retryCount: 1}
	ack := &fakeAcknowledger{operations: &repo.operations}
	messages := make(chan amqp.Delivery, 3)
	// An invalid message must not prevent the following messages from being handled.
	messages <- amqp.Delivery{Acknowledger: ack, DeliveryTag: 1, Body: []byte(`{invalid}`)}
	messages <- amqp.Delivery{Acknowledger: ack, DeliveryTag: 2, Body: []byte(`{"event_id":"event-2"}`)}
	messages <- amqp.Delivery{Acknowledger: ack, DeliveryTag: 3, Body: []byte(`{"event_id":"event-3"}`)}
	close(messages)
	consumer := &fakeMessageConsumer{messages: messages}
	decisions := 0
	done, err := startConsumerWithFailureDecider(repo, consumer, func() bool {
		decisions++
		return decisions == 3
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForDone(t, done)
	if decisions != 3 {
		t.Errorf("failure decisions = %d, want 3", decisions)
	}
	wantOps := []string{"sent:event-2", "ack", "retry:event-3", "nack"}
	if !reflect.DeepEqual(repo.operations, wantOps) {
		t.Errorf("operations = %v, want %v", repo.operations, wantOps)
	}
	wantAcks := []acknowledgement{{kind: "ack", tag: 2}, {kind: "nack", tag: 3}}
	if !reflect.DeepEqual(ack.calls, wantAcks) {
		t.Errorf("acknowledgements = %+v, want %+v", ack.calls, wantAcks)
	}
	wantConsume := []consumeCall{{queue: "notifications.queue"}}
	if !reflect.DeepEqual(consumer.calls, wantConsume) {
		t.Errorf("consume calls = %+v, want %+v", consumer.calls, wantConsume)
	}
}

func TestStartConsumerConsumeError(t *testing.T) {
	wantErr := errors.New("consume failed")
	consumer := &fakeMessageConsumer{err: wantErr}
	if err := startConsumer(&fakeRepository{}, consumer); !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
}

func TestStartConsumerClosedChannel(t *testing.T) {
	messages := make(chan amqp.Delivery)
	close(messages)
	decisions := 0
	done, err := startConsumerWithFailureDecider(&fakeRepository{}, &fakeMessageConsumer{messages: messages}, func() bool {
		decisions++
		return false
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForDone(t, done)
	if decisions != 0 {
		t.Fatalf("failure decider called %d times for closed channel", decisions)
	}
}
