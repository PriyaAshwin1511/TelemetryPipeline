// Gateway service provides an HTTP API for querying telemetry data.
package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	_ "telemetry-collector/docs"
	"telemetry-collector/services/config"
	"telemetry-collector/services/gateway"
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

	// Create gateway
	g, err := gateway.NewGateway(&cfg.Server, &cfg.Database)
	if err != nil {
		logger.Fatalf("Failed to create gateway: %v", err)
	}

	// Start gateway
	if err := g.Start(); err != nil {
		logger.Fatalf("Failed to start gateway: %v", err)
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("Gateway shutting down")
}
