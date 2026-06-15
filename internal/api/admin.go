package api

import (
	"encoding/json"
	"net/http"
)

// dlqResponse is the body of GET /queues/{name}/dlq.
type dlqResponse struct {
	Messages []consumedMessage `json:"messages"`
}

// handleDLQ returns the contents of a queue's dead-letter queue for
// inspection. Returns 200 with an empty array if the queue has no DLQ'd
// messages.
//
// TODO: we could add a feature to add back dlq msgs to main queue later.
func (h *handlers) handleDLQ(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("name")
	if queueName == "" {
		writeError(w, http.StatusBadRequest, "queue name is required")
		return
	}

	q := h.broker.GetOrCreateQueue(queueName)
	dlqMessages := q.DLQMessages()

	out := make([]consumedMessage, 0, len(dlqMessages))
	for _, msg := range dlqMessages {
		out = append(out, consumedMessage{
			MessageID:     msg.ID,
			Payload:       msg.Payload,
			Priority:      string(msg.Priority),
			DeliveryCount: msg.DeliveryCount,
			QueuedAt:      msg.QueuedAt,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(dlqResponse{Messages: out})
}
