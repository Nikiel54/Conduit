package api

import (
	"errors"
	"fmt"
	"net/http"

	"conduit/internal/broker"
)

// handleAck acknowledges successful processing of a message, removing it
// from its queue permanently.
func (h *handlers) handleAck(w http.ResponseWriter, r *http.Request) {
	messageID := r.PathValue("id")
	if messageID == "" {
		writeError(w, http.StatusBadRequest, "message id is required")
		return
	}

	q, ok := h.broker.LookupQueue(messageID)
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("message %q not found", messageID))
		return
	}

	if err := q.Ack(messageID); err != nil {
		if errors.Is(err, broker.ErrMessageNotFound) {
			writeError(w, http.StatusNotFound, fmt.Sprintf("message %q not found", messageID))
			return
		}

		// Any other error here would be a bug in Queue.Ack
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.broker.UnregisterInFlight(messageID)

	// 204 No Content: success, no body
	w.WriteHeader(http.StatusNoContent)
}
