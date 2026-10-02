//go:build integration

package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"practiceRabbitMQ/internal/domain"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// The configured user needs management access to create vhosts and permissions.
// Each test owns a new vhost, so it never consumes or removes application queues.
func isolatedRabbitURL(t *testing.T) string {
	t.Helper()
	rabbitURL := os.Getenv("TEST_RABBITMQ_URL")
	managementURL := os.Getenv("TEST_RABBITMQ_MANAGEMENT_URL")
	if rabbitURL == "" || managementURL == "" {
		t.Fatal("set TEST_RABBITMQ_URL and TEST_RABBITMQ_MANAGEMENT_URL to run RabbitMQ integration tests")
	}
	uri, err := amqp.ParseURI(rabbitURL)
	if err != nil {
		t.Fatal("invalid TEST_RABBITMQ_URL")
	}
	parsedURL, err := url.Parse(rabbitURL)
	if err != nil {
		t.Fatal("invalid TEST_RABBITMQ_URL")
	}
	vhost := "practice-rabbitmq-test-" + uuid.NewString()
	client := &http.Client{Timeout: 5 * time.Second}
	request := func(method, path, body string) error {
		req, err := http.NewRequest(method, strings.TrimRight(managementURL, "/")+path, strings.NewReader(body))
		if err != nil {
			return err
		}
		req.SetBasicAuth(uri.Username, uri.Password)
		req.Header.Set("Content-Type", "application/json")
		response, err := client.Do(req)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
			return fmt.Errorf("management %s %s: HTTP %d: %s", method, path, response.StatusCode, body)
		}
		return nil
	}
	vhostPath := "/api/vhosts/" + url.PathEscape(vhost)
	if err := request(http.MethodPut, vhostPath, `{}`); err != nil {
		t.Fatalf("create isolated vhost: %v", err)
	}
	t.Cleanup(func() {
		if err := request(http.MethodDelete, vhostPath, ""); err != nil {
			t.Errorf("remove isolated vhost %s: %v", vhost, err)
		}
	})
	permissionsPath := "/api/permissions/" + url.PathEscape(vhost) + "/" + url.PathEscape(uri.Username)
	if err := request(http.MethodPut, permissionsPath, `{"configure":".*","write":".*","read":".*"}`); err != nil {
		t.Fatalf("grant permissions for isolated vhost: %v", err)
	}
	parsedURL.Path = "/" + vhost
	parsedURL.RawPath = ""
	query := parsedURL.Query()
	query.Set("connection_timeout", "5000")
	parsedURL.RawQuery = query.Encode()
	return parsedURL.String()
}

func receiveDelivery(t *testing.T, messages <-chan amqp.Delivery) amqp.Delivery {
	t.Helper()
	select {
	case msg, ok := <-messages:
		if !ok {
			t.Fatal("delivery channel closed before receiving a message")
		}
		return msg
	case <-time.After(5 * time.Second):
		t.Fatal("message was not routed within five seconds")
		return amqp.Delivery{}
	}
}

func TestSetupRabbitMQRoutesAndDeadLetters(t *testing.T) {
	conn, channel, err := setupRabbitMQ(isolatedRabbitURL(t))
	if err != nil {
		t.Fatalf("setup RabbitMQ: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	t.Cleanup(func() { _ = channel.Close() })
	mainMessages, err := channel.Consume("notifications.queue", "", false, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	deadLetters, err := channel.Consume("notifications.dlq", "", false, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	event := pendingEvent("integration-event")
	repo := &fakeRepository{pendingEvents: []domain.EventMessage{event}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := publishPendingEvent(ctx, repo, channel); err != nil {
		t.Fatalf("publish event: %v", err)
	}
	msg := receiveDelivery(t, mainMessages)
	var received domain.EventMessage
	if err := json.Unmarshal(msg.Body, &received); err != nil {
		t.Fatalf("decode delivered event: %v", err)
	}
	if !reflect.DeepEqual(received, event) {
		t.Errorf("delivered event = %+v, want %+v", received, event)
	}
	if msg.Exchange != "orders.exchange" || msg.RoutingKey != "notifications" || msg.CorrelationId != event.CorrelationID || msg.ContentType != "application/json" || msg.DeliveryMode != amqp.Persistent {
		t.Errorf("incorrect delivered metadata: %+v", msg)
	}
	if err := msg.Nack(false, false); err != nil {
		t.Fatalf("reject message for dead lettering: %v", err)
	}
	deadLetter := receiveDelivery(t, deadLetters)
	if !bytes.Equal(deadLetter.Body, msg.Body) || deadLetter.CorrelationId != event.CorrelationID || deadLetter.Exchange != "notifications.dlx" || deadLetter.RoutingKey != "failed" {
		t.Errorf("incorrect dead-letter delivery: %+v", deadLetter)
	}
	if err := deadLetter.Ack(false); err != nil {
		t.Fatal(err)
	}
}

func TestConnectRabbitMQWithRetryIntegration(t *testing.T) {
	conn, channel, err := connectRabbitMQWithRetry(isolatedRabbitURL(t))
	if err != nil {
		t.Fatalf("connect RabbitMQ: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	t.Cleanup(func() { _ = channel.Close() })
	if conn.IsClosed() || channel.IsClosed() {
		t.Fatal("expected open connection and channel")
	}
}
