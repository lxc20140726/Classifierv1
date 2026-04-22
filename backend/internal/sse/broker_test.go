package sse

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestBrokerSubscribePublishReceivesEvent(t *testing.T) {
	b := NewBroker()
	ch := b.Subscribe()
	defer b.Unsubscribe(ch)

	payload := map[string]any{"ok": true}
	if err := b.Publish("job.progress", payload); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case evt := <-ch:
		if evt.Type != "job.progress" {
			t.Fatalf("event type = %q, want %q", evt.Type, "job.progress")
		}
		if string(evt.Data) != `{"ok":true}` {
			t.Fatalf("event data = %q, want %q", string(evt.Data), `{"ok":true}`)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestBrokerUnsubscribeClosesChannel(t *testing.T) {
	b := NewBroker()
	ch := b.Subscribe()

	b.Unsubscribe(ch)

	b.mu.RLock()
	_, exists := b.clients[ch]
	b.mu.RUnlock()
	if exists {
		t.Fatal("client should be removed after Unsubscribe")
	}
}

func TestBrokerPublishNoSubscribers(t *testing.T) {
	b := NewBroker()

	if err := b.Publish("noop", map[string]any{"n": 1}); err != nil {
		t.Fatalf("Publish() with no subscribers error = %v", err)
	}
}

func TestBrokerPublishBlocksOnFullBufferUntilConsumerDrains(t *testing.T) {
	b := NewBroker()
	ch := b.Subscribe()
	defer b.Unsubscribe(ch)

	for i := 0; i < 16; i++ {
		if err := b.Publish("fill", map[string]any{"i": i}); err != nil {
			t.Fatalf("Publish() fill error = %v", err)
		}
	}

	done := make(chan error, 1)
	go func() {
		done <- b.Publish("block", map[string]any{"i": 16})
	}()

	select {
	case err := <-done:
		t.Fatalf("Publish() should block on full buffer, got err=%v", err)
	case <-time.After(80 * time.Millisecond):
	}

	select {
	case <-ch:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting to drain one event")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Publish() block error = %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Publish() did not unblock after consumer drained")
	}

	count := 0
	drainDone := false
	for !drainDone {
		select {
		case <-ch:
			count++
		default:
			drainDone = true
		}
	}

	if count != 16 {
		t.Fatalf("drained %d events, want 16", count)
	}
}

func TestBrokerPublishSkipsUnsubscribedClientWithoutDeadlock(t *testing.T) {
	b := NewBroker()
	ch := b.Subscribe()
	b.Unsubscribe(ch)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			if err := b.Publish("noop", map[string]any{"i": i}); err != nil {
				t.Errorf("Publish() error = %v", err)
				return
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("Publish() deadlocked on unsubscribed client")
	}
}

func TestStreamHandlerWritesValidSSEFraming(t *testing.T) {
	gin.SetMode(gin.TestMode)
	b := NewBroker()

	router := gin.New()
	router.GET("/events", StreamHandler(b))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest("GET", "/events", nil).WithContext(ctx)
	recorder := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		router.ServeHTTP(recorder, req)
	}()

	deadline := time.Now().Add(1 * time.Second)
	for {
		b.mu.RLock()
		n := len(b.clients)
		b.mu.RUnlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("subscriber was not registered in time")
		}
		time.Sleep(5 * time.Millisecond)
	}

	if err := b.Publish("job.progress", map[string]any{"value": 1}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	var clientCh chan Event
	b.mu.RLock()
	for ch := range b.clients {
		clientCh = ch
		break
	}
	b.mu.RUnlock()
	if clientCh == nil {
		t.Fatal("expected one subscribed client channel")
	}

	deadline = time.Now().Add(1 * time.Second)
	for {
		if len(clientCh) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for stream handler to consume published event")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("stream handler did not exit after request context cancellation")
	}

	body := recorder.Body.String()
	if !strings.Contains(body, "event: job.progress\n") {
		t.Fatalf("missing event frame, body = %q", body)
	}
	if !strings.Contains(body, "data: {\"value\":1}\n\n") {
		t.Fatalf("missing data frame, body = %q", body)
	}
}
