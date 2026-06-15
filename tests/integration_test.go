// Package tests contains end-to-end integration tests for the Conduit broker.
//
// These tests spin up a real HTTP server (via httptest.NewServer) and make
// real HTTP requests against it. This is distinct from unit tests, which live
// alongside the code they test and call functions directly.
//
// Why the separation?
//   - Integration tests validate the full request path: routing, JSON decoding,
//     header handling, status codes. Unit tests validate individual functions.
//   - Keeping them in a separate package (tests) means they can only use the
//     public API of internal packages, which is a useful constraint.
//
// Run all integration tests:
//
//	go test ./tests/...
//
// Run with verbose output to see each test name:
//
//	go test -v ./tests/...
package tests

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
)

// TestPublish_Returns201 is the happy-path test for the publish endpoint.
//
// It verifies the complete contract for a well-formed request:
//   - HTTP status is 201 Created (not 200 OK).
//   - Response body is valid JSON.
//   - message_id is present and non-empty.
//   - queued_at is present and non-empty.
//
// In Step 2 the handler actually enqueues the message on the named queue,
// but the response contract is unchanged from Step 1 — consumers and
// producers depend on these fields from day one.
func TestPublish_Returns201(t *testing.T) {
	ts := newTestServer(t)

	body := bytes.NewBufferString(`{
		"payload":     "hello, conduit",
		"priority":    "medium",
		"max_retries": 3
	}`)

	resp, err := http.Post(
		ts.URL+"/queues/orders/publish",
		"application/json",
		body,
	)
	if err != nil {
		t.Fatalf("POST /queues/orders/publish failed: %v", err)
	}
	defer resp.Body.Close()

	// --- Status code ---
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected status 201, got %d", resp.StatusCode)
	}

	// --- Response shape ---
	var got struct {
		MessageID string `json:"message_id"`
		QueuedAt  string `json:"queued_at"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("could not decode response body: %v", err)
	}

	if got.MessageID == "" {
		t.Error("message_id is missing or empty")
	}
	if got.QueuedAt == "" {
		t.Error("queued_at is missing or empty")
	}

	// --- Content-Type header ---
	// Clients depend on this to know they should decode JSON.
	ct := resp.Header.Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", ct)
	}
}

// TestPublish_UniqueMessageIDs verifies that two publish calls return
// different message_ids. The queue's correctness depends on message_ids
// being globally unique — acks, in-flight tracking, and DLQ routing all
// key on them. If this test fails we have a broken ID generator.
func TestPublish_UniqueMessageIDs(t *testing.T) {
	ts := newTestServer(t)

	publishOne := func() string {
		body := bytes.NewBufferString(`{"payload":"x","priority":"low","max_retries":1}`)
		resp, err := http.Post(ts.URL+"/queues/test/publish", "application/json", body)
		if err != nil {
			t.Fatalf("POST failed: %v", err)
		}
		defer resp.Body.Close()

		var got struct {
			MessageID string `json:"message_id"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
			t.Fatalf("decode failed: %v", err)
		}
		return got.MessageID
	}

	id1 := publishOne()
	id2 := publishOne()

	if id1 == id2 {
		t.Errorf("expected unique message IDs, got identical: %q", id1)
	}
}

// TestPublish_RejectsInvalidJSON verifies that malformed request bodies
// produce a 400 Bad Request with a JSON error payload.
//
// Testing error paths from day one is important: the spec lists edge cases
// (double ack, unknown message ID, empty queue consume) as things to handle
// explicitly. This test establishes the pattern for all of them.
func TestPublish_RejectsInvalidJSON(t *testing.T) {
	ts := newTestServer(t)

	resp, err := http.Post(
		ts.URL+"/queues/test/publish",
		"application/json",
		bytes.NewBufferString("this is not json {{{"),
	)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}

	// The response should still be JSON with an "error" key.
	var got map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("error response is not valid JSON: %v", err)
	}
	if got["error"] == "" {
		t.Error("expected non-empty error field in response body")
	}
}

// TestPublish_WrongMethod verifies that the router rejects non-POST methods.
// Go 1.22's ServeMux enforces the method in the pattern, returning 405.
// We test this explicitly so we know our routing is working as expected.
func TestPublish_WrongMethod(t *testing.T) {
	ts := newTestServer(t)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/queues/test/publish", nil)
	if err != nil {
		t.Fatalf("building GET request failed: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 Method Not Allowed, got %d", resp.StatusCode)
	}
}
