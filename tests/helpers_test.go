package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"conduit/internal/api"
	"conduit/internal/broker"
	"conduit/internal/config"
)

// newTestServer creates a real HTTP test server backed by a fresh
// *broker.Broker and returns it. The caller is responsible for nothing —
// t.Cleanup handles shutdown.
//
// Changed from Step 1: now constructs a broker.NewBroker() and passes it to
// api.NewServer, matching the new NewServer(cfg, b) signature. Each call to
// newTestServer gets its own broker, so tests are isolated from each other
// even though queue names ("orders", "test", etc.) are frequently reused
// across test functions.
//
// httptest.NewServer binds to a random free port on 127.0.0.1. This means:
//   - Tests can run in parallel without port conflicts.
//   - No hardcoded ports to manage.
//   - Works identically on Windows, macOS, and Linux.
func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	cfg := &config.Config{ListenAddr: ":0"}
	b := broker.NewBroker()
	srv := api.NewServer(cfg, b)
	ts := httptest.NewServer(srv.Handler)

	// ts.Close() first: it blocks until in-flight HTTP handlers finish, so
	// by the time b.Close() runs, no handler can be mid-call into a queue's
	// Enqueue/Dequeue/Ack -- which Queue.Close requires (Step 3).
	t.Cleanup(func() {
		ts.Close()
		b.Close()
	})

	return ts
}

// --- Wire-format DTOs used across test files -------------------------------
//
// These mirror the JSON shapes defined in internal/api but are independent
// copies. Tests decode into these rather than importing internal/api types
// directly — the tests package can only see exported names anyway, and
// keeping the DTOs local means a JSON field rename in internal/api shows up
// as a test failure (decoded field stays zero-valued) rather than a compile
// error that might mask the actual behavioral question: "does the wire
// format still look like this?"

type publishResponseDTO struct {
	MessageID string `json:"message_id"`
	QueuedAt  string `json:"queued_at"`
}

type consumedMessageDTO struct {
	MessageID     string `json:"message_id"`
	Payload       string `json:"payload"`
	Priority      string `json:"priority"`
	DeliveryCount int    `json:"delivery_count"`
	QueuedAt      string `json:"queued_at"`
}

type consumeResponseDTO struct {
	Messages []consumedMessageDTO `json:"messages"`
}

// --- HTTP helper functions ---------------------------------------------------

// publishMessage POSTs a single message to /queues/{queue}/publish and
// returns the generated message_id. Fails the test on any error or
// non-201 response.
func publishMessage(t *testing.T, ts *httptest.Server, queue, payload, priority string, maxRetries int) string {
	t.Helper()

	reqBody, err := json.Marshal(map[string]any{
		"payload":     payload,
		"priority":    priority,
		"max_retries": maxRetries,
	})
	if err != nil {
		t.Fatalf("marshal publish request: %v", err)
	}

	resp, err := http.Post(
		fmt.Sprintf("%s/queues/%s/publish", ts.URL, queue),
		"application/json",
		bytes.NewReader(reqBody),
	)
	if err != nil {
		t.Fatalf("POST /queues/%s/publish: %v", queue, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("publish to %q: expected 201, got %d", queue, resp.StatusCode)
	}

	var got publishResponseDTO
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode publish response: %v", err)
	}
	if got.MessageID == "" {
		t.Fatalf("publish to %q: empty message_id in response", queue)
	}
	return got.MessageID
}

// consumeMessages POSTs to /queues/{queue}/consume with the given
// visibility timeout and max_messages, and returns the decoded messages.
// A visibilityTimeoutSeconds of 0 lets the server apply its default.
func consumeMessages(t *testing.T, ts *httptest.Server, queue string, visibilityTimeoutSeconds, maxMessages int) []consumedMessageDTO {
	t.Helper()

	reqBody, err := json.Marshal(map[string]any{
		"visibility_timeout_seconds": visibilityTimeoutSeconds,
		"max_messages":               maxMessages,
	})
	if err != nil {
		t.Fatalf("marshal consume request: %v", err)
	}

	resp, err := http.Post(
		fmt.Sprintf("%s/queues/%s/consume", ts.URL, queue),
		"application/json",
		bytes.NewReader(reqBody),
	)
	if err != nil {
		t.Fatalf("POST /queues/%s/consume: %v", queue, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("consume from %q: expected 200, got %d", queue, resp.StatusCode)
	}

	var got consumeResponseDTO
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode consume response: %v", err)
	}
	return got.Messages
}

// ackMessage POSTs to /messages/{id}/ack and returns the HTTP status code,
// so callers can assert on both success (204) and error cases (404/409)
// without the helper itself deciding what's "expected."
func ackMessage(t *testing.T, ts *httptest.Server, messageID string) int {
	t.Helper()

	resp, err := http.Post(
		fmt.Sprintf("%s/messages/%s/ack", ts.URL, messageID),
		"application/json",
		nil,
	)
	if err != nil {
		t.Fatalf("POST /messages/%s/ack: %v", messageID, err)
	}
	defer resp.Body.Close()

	return resp.StatusCode
}

// getDLQ fetches GET /queues/{queue}/dlq and returns the decoded messages.
func getDLQ(t *testing.T, ts *httptest.Server, queue string) []consumedMessageDTO {
	t.Helper()

	resp, err := http.Get(fmt.Sprintf("%s/queues/%s/dlq", ts.URL, queue))
	if err != nil {
		t.Fatalf("GET /queues/%s/dlq: %v", queue, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("dlq for %q: expected 200, got %d", queue, resp.StatusCode)
	}

	var got consumeResponseDTO // same shape as consume: {"messages": [...]}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode dlq response: %v", err)
	}
	return got.Messages
}

// shortVisibilityTimeout is used by tests that need a visibility timeout
// short enough to expire within the test's runtime without making the
// whole suite slow. 1 second is the smallest value representable by the
// integer visibility_timeout_seconds field (see internal/api/consume.go).
const shortVisibilityTimeout = 1 * time.Second
