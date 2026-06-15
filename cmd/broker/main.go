// Command broker is the Conduit message queue broker.
//
// It starts an HTTP server, registers signal handlers for graceful shutdown,
// and blocks until it receives SIGINT (Ctrl+C) or SIGTERM (Docker stop).
// All business logic lives in internal/; this file is intentionally thin.
//
// Usage:
//
//	go run ./cmd/broker              # default: listen on :8080
//	go run ./cmd/broker -listen :9090
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"conduit/internal/api"
	"conduit/internal/broker"
	"conduit/internal/config"
)

func main() {
	// Load configuration from flags. All defaults are defined in internal/config.
	cfg := config.Load()

	// single broker
	b := broker.NewBroker()

	// Build the HTTP server.
	srv := api.NewServer(cfg, b)

	// Start the server in a separate goroutine so that it doesn't block the main thread.
	go func() {
		log.Printf("conduit broker listening on %s", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Block here until the OS sends an interrupt signal.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("shutdown signal received — draining in-flight requests")

	// Give the server up to 10 seconds to finish in-flight HTTP requests
	// before forcibly closing connections. 
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown incomplete: %v", err)
	}

	log.Println("broker stopped cleanly")
}

