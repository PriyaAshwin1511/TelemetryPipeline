// Package collector provides functionality to consume telemetry messages from the queue
// and persist them to the database. It supports consumer groups, batch processing,
// and configurable polling intervals.
package collector

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"telemetry-collector/services/config"
	"telemetry-collector/services/logger"
	"telemetry-collector/services/util"
)

// Collector consumes messages from the queue and persists them to the database
type Collector struct {
	config   *config.CollectorConfig
	client   util.HTTPClient
	db       util.Database
	stopChan chan struct{}
}

// NewCollector creates a new collector
func NewCollector(cfg *config.CollectorConfig, dbConfig *config.DatabaseConfig) (*Collector, error) {
	db, err := util.ConnectToDB(dbConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	return &Collector{
		config:   cfg,
		client:   util.NewRealHTTPClient(30 * time.Second),
		db:       db,
		stopChan: make(chan struct{}),
	}, nil
}

// NewCollectorWithDB creates a new collector with a provided database interface (for testing)
func NewCollectorWithDB(cfg *config.CollectorConfig, db util.Database) *Collector {
	return &Collector{
		config:   cfg,
		client:   util.NewRealHTTPClient(30 * time.Second),
		db:       db,
		stopChan: make(chan struct{}),
	}
}

// Start begins consuming messages from the queue
func (c *Collector) Start() error {
	logger.WithFields(map[string]interface{}{
		"batch_size":        c.config.BatchSize,
		"poll_interval":     c.config.PollInterval,
		"consumer_group_id": c.config.ConsumerGroupID,
	}).Info("Starting collector")

	// Start polling loop
	go c.pollLoop()

	return nil
}

// Stop stops the collector
func (c *Collector) Stop() {
	logger.Info("Stopping collector")
	close(c.stopChan)
	if c.db != nil {
		c.db.Close()
	}
}

// pollLoop continuously polls the queue for new messages
func (c *Collector) pollLoop() {
	ticker := time.NewTicker(c.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopChan:
			return
		case <-ticker.C:
			c.pollPartitions()
		}
	}
}

// pollPartitions polls all partitions for new messages
func (c *Collector) pollPartitions() {
	// For HTTP-based queue, we consume from all partitions via a single endpoint
	if err := c.pollQueue(); err != nil {
		logger.WithFields(map[string]interface{}{
			"error": err,
		}).Error("Failed to poll queue")
	}
}

// pollQueue polls the external queue service for new messages
func (c *Collector) pollQueue() error {
	// Build consume request URL with consumer group ID
	url := fmt.Sprintf("%s/consume?count=%d&consumer_group_id=%s", c.config.QueueEndpoint, c.config.BatchSize, c.config.ConsumerGroupID)

	req := util.NewRequestFromBytes("GET", url, nil, "")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to consume from queue: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("queue service returned status %d", resp.StatusCode)
	}

	var response struct {
		Records         []util.TelemetryRecord `json:"records"`
		ConsumerGroupID string                 `json:"consumer_group_id"`
	}

	if err := json.Unmarshal(resp.Body, &response); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if len(response.Records) == 0 {
		return nil
	}

	// Persist records to database
	if err := c.persistRecords(response.Records); err != nil {
		logger.WithFields(map[string]interface{}{
			"records":           len(response.Records),
			"consumer_group_id": response.ConsumerGroupID,
			"error":             err,
		}).Error("Failed to persist records")
		return err
	}

	logger.WithFields(map[string]interface{}{
		"records":           len(response.Records),
		"consumer_group_id": response.ConsumerGroupID,
	}).Info("Records processed")

	return nil
}

// persistRecords inserts records into the database
func (c *Collector) persistRecords(records []util.TelemetryRecord) error {
	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO telemetry (timestamp, metric_name, gpu_id, device, uuid, model_name, hostname, container, pod, namespace, value, labels)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT DO NOTHING
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, record := range records {
		labelsJSON, err := json.Marshal(record.Labels)
		if err != nil {
			logger.WithFields(map[string]interface{}{
				"error": err,
			}).Warn("Failed to marshal labels, using null")
			labelsJSON = []byte("null")
		}

		_, err = stmt.Exec(
			record.Timestamp,
			record.MetricName,
			record.GPUID,
			record.Device,
			record.UUID,
			record.ModelName,
			record.Hostname,
			record.Container,
			record.Pod,
			record.Namespace,
			record.Value,
			labelsJSON,
		)
		if err != nil {
			return fmt.Errorf("failed to execute statement: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
