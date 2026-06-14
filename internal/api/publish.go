package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
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


// handlePublish accepts a publish request, generates a message ID, and
// returns 201 Created.
func handlePublish(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("name")
	if queueName == "" {
		// Defensive check, this shouldn't happen though.
		writeError(w, http.StatusBadRequest, "queue name is required")
		return
	}

	var req publishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON body: %v", err))
		return
	}

	resp := publishResponse{
		MessageID: newMessageID(),
		QueuedAt:  time.Now().UTC(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

// newMessageID returns a 128-bit random identifier as a 32-char hex string.
func newMessageID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failing means the OS entropy source is broken.
		// There is no sensible recovery, so we panic here.
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	return hex.EncodeToString(b)
}

