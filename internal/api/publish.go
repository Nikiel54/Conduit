package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"conduit/internal/broker"
)

// publishRequest is the JSON body producers POST to /queues/{name}/publish.
type publishRequest struct {
	Payload    string `json:"payload"`
	Priority   string `json:"priority"`
	MaxRetries int    `json:"max_retries"`
}

// publishResponse is what we return on a successful publish.
type publishResponse struct {
	MessageID string    `json:"message_id"`
	QueuedAt  time.Time `json:"queued_at"`
}

// handlePublish validates the request, builds a broker.Message, and
// enqueues it on the named queue (creating the queue if this is its first
// reference). Returns 201 with the generated message_id and queued_at.
func (h *handlers) handlePublish(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("name")
	if queueName == "" {
		writeError(w, http.StatusBadRequest, "queue name is required")
		return
	}

	var req publishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON body: %v", err))
		return
	}

	priority, err := normalizePriority(req.Priority)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	maxRetries := req.MaxRetries
	if maxRetries <= 0 {
		maxRetries = broker.DefaultMaxRetries
	}

	msg := &broker.Message{
		ID:         newMessageID(),
		Payload:    req.Payload,
		Priority:   priority,
		MaxRetries: maxRetries,
		QueuedAt:   time.Now().UTC(),
	}

	q := h.broker.GetOrCreateQueue(queueName)
	q.Enqueue(msg)

	resp := publishResponse{
		MessageID: msg.ID,
		QueuedAt:  msg.QueuedAt,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

// normalizePriority validates the "priority" field of a publish request.
//
//   - "" (omitted)            -> broker.PriorityMedium (a documented default;
//     the spec's examples always set priority explicitly, but a producer
//     that omits it shouldn't get a 400 for something this minor)
//   - "high"/"medium"/"low"   -> the corresponding broker.Priority
//   - anything else           -> error, mapped to 400 Bad Request by the caller
func normalizePriority(p string) (broker.Priority, error) {
	switch broker.Priority(p) {
	case "":
		return broker.PriorityMedium, nil
	case broker.PriorityHigh, broker.PriorityMedium, broker.PriorityLow:
		return broker.Priority(p), nil
	default:
		return "", fmt.Errorf("invalid priority %q: must be \"high\", \"medium\", or \"low\"", p)
	}
}

// newMessageID returns a 128-bit random identifier as a 32-char hex string.
func newMessageID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failing means the OS entropy source is broken.
		// There is no sensible recovery, so panic here.
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	return hex.EncodeToString(b)
}


