package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"conduit/internal/broker"
)

// consumeRequest is the JSON body consumers POST to /queues/{name}/consume.
type consumeRequest struct {
	VisibilityTimeoutSeconds int `json:"visibility_timeout_seconds"`
	MaxMessages int `json:"max_messages"`
}

// consumedMessage is the wire representation of a message handed to a
// consumer. Shared with admin.go's DLQ inspection endpoint, since both
// represent "a message the caller can look at" with the same fields.
type consumedMessage struct {
	MessageID     string    `json:"message_id"`
	Payload       string    `json:"payload"`
	Priority      string    `json:"priority"`
	DeliveryCount int       `json:"delivery_count"`
	QueuedAt      time.Time `json:"queued_at"`
}

// consumeResponse is the body of a successful consume response.
type consumeResponse struct {
	Messages []consumedMessage `json:"messages"`
}

// handleConsume pulls up to MaxMessages messages from the named queue
// (creating it if it doesn't exist yet — a queue that has never been
// published to is just an empty queue, not an error) and starts a
// visibility timeout for each one.
func (h *handlers) handleConsume(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("name")
	if queueName == "" {
		writeError(w, http.StatusBadRequest, "queue name is required")
		return
	}

	var req consumeRequest
	// An empty body is valid (use defaults). Only attempt to decode if the
	// client actually sent something — decoding an empty body with
	// json.Decoder returns io.EOF, which we don't want to treat as a 400.
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON body: %v", err))
			return
		}
	}

	visibilityTimeout := time.Duration(req.VisibilityTimeoutSeconds) * time.Second
	if visibilityTimeout <= 0 {
		visibilityTimeout = broker.DefaultVisibilityTimeout
	}

	maxMessages := req.MaxMessages
	if maxMessages <= 0 {
		maxMessages = 1
	}

	q := h.broker.GetOrCreateQueue(queueName)

	messages := make([]consumedMessage, 0, maxMessages)
	for i := 0; i < maxMessages; i++ {
		msg, ok := q.Dequeue(visibilityTimeout)
		if !ok {
			// Pending buckets are all empty. Not an error — return
			// whatever we've collected so far (possibly nothing).
			break
		}

		h.broker.RegisterInFlight(msg.ID, q)

		messages = append(messages, consumedMessage{
			MessageID:     msg.ID,
			Payload:       msg.Payload,
			Priority:      string(msg.Priority),
			DeliveryCount: msg.DeliveryCount,
			QueuedAt:      msg.QueuedAt,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(consumeResponse{Messages: messages})
}


