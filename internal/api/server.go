// Package api contains the HTTP transport layer: server setup, routing,
// request decoding, response encoding, and error formatting.
//
// Handlers in this package translate HTTP into calls on the broker's
// internal types. They do not contain queue logic — that lives in
// internal/broker. Keeping transport and domain logic separate means
// we can change the wire format (e.g. add gRPC later) without touching
// queue internals.
package api

import (
	"encoding/json"
	"net/http"
	"time"

	"conduit/internal/broker"
	"conduit/internal/config"
)

// handlers groups the dependencies shared by every HTTP handler in this
// package.
type handlers struct {
	broker *broker.Broker
}

// NewServer wires up routes and returns an *http.Server ready to be started.
func NewServer(cfg *config.Config, b *broker.Broker) *http.Server {
	h := &handlers{broker: b}

	mux := http.NewServeMux()

	mux.HandleFunc("POST /queues/{name}/publish", h.handlePublish)
	mux.HandleFunc("POST /queues/{name}/consume", h.handleConsume)
	mux.HandleFunc("POST /messages/{id}/ack", h.handleAck)
	mux.HandleFunc("GET /queues/{name}/dlq", h.handleDLQ)

	return &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: mux,

		// ReadHeaderTimeout protects against Slowloris-style attacks where
		// a client opens a connection and dribbles headers indefinitely.
		// Always set this on any net/http server exposed to the network.
		ReadHeaderTimeout: 5 * time.Second,
	}
}

// writeError sends a JSON-formatted error response with the given status.
// Centralising this means every error response across the api package has
// the same shape: {"error": "message"}.
func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// Encode errors are ignored intentionally: if the client has already
	// disconnected, there's nothing useful we can do here.
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}


