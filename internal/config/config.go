// Package config centralises runtime configuration for the broker.
package config

import "flag"

// Config holds runtime configuration. Pointers to a single instance are
// passed into subsystems that need configuration so that callers can't
// accidentally mutate values mid-flight.
type Config struct {
	// ListenAddr is the host:port the HTTP server binds to.
	// Use ":8080" to listen on all interfaces, "127.0.0.1:8080" for loopback only.
	ListenAddr string
}

// Load parses command-line flags and returns a populated Config.
func Load() *Config {
	cfg := &Config{}
	flag.StringVar(&cfg.ListenAddr, "listen", ":8080", "HTTP listen address (host:port)")
	flag.Parse()
	return cfg
}
