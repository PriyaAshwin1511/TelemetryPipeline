// Collector service consumes messages from the queue and persists them to the database.
package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"telemetry-collector/services/collector"
	"telemetry-collector/services/config"
	"telemetry-collector/services/logger"
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

	// Create collector
	c, err := collector.NewCollector(&cfg.Collector, &cfg.Database)
	if err != nil {
		logger.Fatalf("Failed to create collector: %v", err)
	}
	defer c.Stop()

	// Start collector
	if err := c.Start(); err != nil {
		logger.Fatalf("Failed to start collector: %v", err)
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("Collector shutting down")
}
