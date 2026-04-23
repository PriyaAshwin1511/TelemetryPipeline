// Streamer service reads CSV telemetry data and streams it to the queue.
package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"telemetry-collector/services/config"
	"telemetry-collector/services/logger"
	"telemetry-collector/services/streamer"
)

func main() {
	// Load configuration
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	if err := logger.Init(cfg.Logging.Level, cfg.Logging.Format, cfg.Logging.Output); err != nil {
		fmt.Printf("Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	// Create streamer
	queueEndpoint := os.Getenv("TELEMETRY_QUEUE_ENDPOINT")
	if queueEndpoint == "" {
		queueEndpoint = fmt.Sprintf("http://localhost:%d/produce", cfg.Server.Port)
	}
	s := streamer.NewStreamer(&cfg.Streamer, queueEndpoint, cfg.Queue.NumPartitions)

	// Start streaming in goroutine
	go func() {
		if err := s.Start(); err != nil {
			logger.Fatalf("Failed to start streamer: %v", err)
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("Streamer shutting down")
	s.Stop()
}
