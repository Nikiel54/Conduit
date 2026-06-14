// Package api contains the HTTP transport layer: server setup, routing,
// request decoding, response encoding, and error formatting.
//
// Handlers in this package translate HTTP into calls on the broker's
// internal types. They do not contain queue logic, this is in in
// internal/broker.
package api

import (
	"encoding/json"
	"net/http"
	"time"

	"conduit/internal/config"
)

// NewServer wires up routes and returns an *http.Server ready to be started.
func NewServer(cfg *config.Config) *http.Server {
	mux := http.NewServeMux()

	// TODO: api routes to be added here.
	mux.HandleFunc("POST /queues/{name}/publish", handlePublish)

	return &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: mux,

		// ReadHeaderTimeout protects against Slowloris-style attacks where
		// a client opens a connection and dribbles headers indefinitely.
		ReadHeaderTimeout: 5 * time.Second,
	}
}


// writeError sends a JSON-formatted error response with the given status.
func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}


