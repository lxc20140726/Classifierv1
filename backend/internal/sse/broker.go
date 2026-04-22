package sse

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
)

type Event struct {
	Type string
	Data []byte
}

type brokerClient struct {
	events chan Event
	done   chan struct{}
}

type Broker struct {
	mu      sync.RWMutex
	clients map[chan Event]*brokerClient
}

func NewBroker() *Broker {
	return &Broker{
		clients: make(map[chan Event]*brokerClient),
	}
}

func (b *Broker) Subscribe() chan Event {
	ch := make(chan Event, 16)
	client := &brokerClient{
		events: ch,
		done:   make(chan struct{}),
	}

	b.mu.Lock()
	b.clients[ch] = client
	b.mu.Unlock()

	return ch
}

func (b *Broker) Unsubscribe(ch chan Event) {
	b.mu.Lock()
	if client, ok := b.clients[ch]; ok {
		delete(b.clients, ch)
		close(client.done)
	}
	b.mu.Unlock()
}

func (b *Broker) Publish(eventType string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	evt := Event{
		Type: eventType,
		Data: data,
	}

	b.mu.RLock()
	clients := make([]*brokerClient, 0, len(b.clients))
	for _, client := range b.clients {
		clients = append(clients, client)
	}
	b.mu.RUnlock()

	for _, client := range clients {
		select {
		case <-client.done:
			continue
		default:
		}

		select {
		case <-client.done:
		case client.events <- evt:
		}
	}

	return nil
}

func StreamHandler(b *Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		flusher, ok := c.Writer.(http.Flusher)
		if !ok {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}

		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")
		c.Header("Connection", "keep-alive")

		ch := b.Subscribe()
		defer b.Unsubscribe(ch)

		for {
			select {
			case <-c.Request.Context().Done():
				return
			case evt, ok := <-ch:
				if !ok {
					return
				}
				_, _ = fmt.Fprintf(c.Writer, "event: %s\n", evt.Type)
				_, _ = fmt.Fprintf(c.Writer, "data: %s\n\n", evt.Data)
				flusher.Flush()
			}
		}
	}
}
