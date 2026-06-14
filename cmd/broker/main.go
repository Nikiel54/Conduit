// This starts an http server and is main entry point for the broker.
//
// Usage:
//	go run ./cmd/broker  # default: listen on :8080
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
	"conduit/internal/config"
)


func main() {
	cfg := config.Load() // Default configs in internal/config.

	srv := api.NewServer(cfg)

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

	log.Println("Shutdown signal received. Draining in-flight requests")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown incomplete: %v", err)
	}

	log.Println("broker stopped cleanly")
}
