package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	// Create temporary config file
	tempFile := "test_config.yaml"
	defer os.Remove(tempFile)

	yamlContent := `
server:
  port: 8080
  read_timeout: 30s
  write_timeout: 30s

queue:
  data_dir: /tmp/queue
  num_partitions: 3
  cache_duration: 2m
  cache_cleanup_interval: 1m
  max_message_size: 1048576
  segment_size: 104857600

database:
  host: localhost
  port: 5432
  user: postgres
  password: postgres
  db_name: telemetry
  max_open_conns: 25
  max_idle_conns: 5

streamer:
  csv_file_path: ./data/telemetry.csv
  batch_size: 100
  retry_max_attempts: 3
  retry_delay: 3s

collector:
  queue_endpoint: http://localhost:8080
  batch_size: 100
  poll_interval: 1s
  retry_max_attempts: 3
  retry_delay: 1s

logging:
  level: info
  format: json
`

	err := os.WriteFile(tempFile, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}

	cfg, err := LoadConfig(tempFile)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Verify server config
	if cfg.Server.Port != 8080 {
		t.Errorf("Expected port 8080, got %d", cfg.Server.Port)
	}
	if cfg.Server.ReadTimeout != 30*time.Second {
		t.Errorf("Expected read timeout 30s, got %v", cfg.Server.ReadTimeout)
	}

	// Verify queue config
	if cfg.Queue.DataDir != "/tmp/queue" {
		t.Errorf("Expected data dir /tmp/queue, got %s", cfg.Queue.DataDir)
	}
	if cfg.Queue.NumPartitions != 3 {
		t.Errorf("Expected num partitions 3, got %d", cfg.Queue.NumPartitions)
	}

	// Verify database config
	if cfg.Database.Host != "localhost" {
		t.Errorf("Expected host localhost, got %s", cfg.Database.Host)
	}
	if cfg.Database.Port != 5432 {
		t.Errorf("Expected port 5432, got %d", cfg.Database.Port)
	}

	// Verify streamer config
	if cfg.Streamer.CSVFilePath != "./data/telemetry.csv" {
		t.Errorf("Expected csv file path ./data/telemetry.csv, got %s", cfg.Streamer.CSVFilePath)
	}
	if cfg.Streamer.BatchSize != 100 {
		t.Errorf("Expected batch size 100, got %d", cfg.Streamer.BatchSize)
	}

	// Verify collector config
	if cfg.Collector.QueueEndpoint != "http://localhost:8080" {
		t.Errorf("Expected queue endpoint http://localhost:8080, got %s", cfg.Collector.QueueEndpoint)
	}

	// Verify logging config
	if cfg.Logging.Level != "info" {
		t.Errorf("Expected log level info, got %s", cfg.Logging.Level)
	}
}

func TestLoadConfig_Defaults(t *testing.T) {
	// Create minimal config file
	tempFile := "test_config_minimal.yaml"
	defer os.Remove(tempFile)

	yamlContent := `
server:
  port: 8080
`

	err := os.WriteFile(tempFile, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test config file: %v", err)
	}

	cfg, err := LoadConfig(tempFile)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Verify defaults are set
	if cfg.Server.ReadTimeout == 0 {
		t.Error("Expected default read timeout to be set")
	}
	if cfg.Queue.NumPartitions == 0 {
		t.Error("Expected default num partitions to be set")
	}
}

func TestLoadConfig_InvalidFile(t *testing.T) {
	_, err := LoadConfig("non_existent_file.yaml")
	if err == nil {
		t.Error("Expected error for non-existent file, got nil")
	}
}
