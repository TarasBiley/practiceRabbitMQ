package broker

import (
	"context"
	"testing"
	"time"

	"practiceRabbitMQ/internal/domain"

	amqp "github.com/rabbitmq/amqp091-go"
)

type fakeRepository struct {
	pendingEvents   []domain.EventMessage
	pendingErr      error
	pendingContexts []context.Context
	markSentErr     error
	retryCount      int
	retryErr        error
	markFailedErr   error
	operations      []string
}

func (f *fakeRepository) GetPendingEvents(ctx context.Context) ([]domain.EventMessage, error) {
	f.pendingContexts = append(f.pendingContexts, ctx)
	return f.pendingEvents, f.pendingErr
}

func (f *fakeRepository) MarkEventSent(_ context.Context, eventID string) error {
	f.operations = append(f.operations, "sent:"+eventID)
	return f.markSentErr
}

func (f *fakeRepository) IncrementRetryCount(_ context.Context, eventID string) (int, error) {
	f.operations = append(f.operations, "retry:"+eventID)
	return f.retryCount, f.retryErr
}

func (f *fakeRepository) MarkEventFailed(_ context.Context, eventID string) error {
	f.operations = append(f.operations, "failed:"+eventID)
	return f.markFailedErr
}

type publishCall struct {
	ctx       context.Context
	exchange  string
	key       string
	mandatory bool
	immediate bool
	message   amqp.Publishing
}

type fakePublisher struct {
	calls     []publishCall
	errors    map[int]error
	onPublish func()
}

func (f *fakePublisher) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, message amqp.Publishing) error {
	f.calls = append(f.calls, publishCall{ctx, exchange, key, mandatory, immediate, message})
	if f.onPublish != nil {
		f.onPublish()
	}
	return f.errors[len(f.calls)]
}

func waitForDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not finish")
	}
}
