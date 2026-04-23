// Package streamer provides functionality to read CSV telemetry data and stream it to the queue.
// It supports batch processing, retry logic with exponential backoff, and continuous streaming.
package streamer

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"telemetry-collector/services/config"
	"telemetry-collector/services/logger"
	"telemetry-collector/services/util"
)

// Streamer reads CSV data and streams it to the queue
type Streamer struct {
	config        *config.StreamerConfig
	queueEndpoint string
	numPartitions int
	httpClient    util.HTTPClient
	stopChan      chan struct{}
	stopOnce      sync.Once
}

// NewStreamer creates a new streamer
func NewStreamer(cfg *config.StreamerConfig, queueEndpoint string, numPartitions int) *Streamer {
	return &Streamer{
		config:        cfg,
		queueEndpoint: queueEndpoint,
		numPartitions: numPartitions,
		httpClient:    util.NewRealHTTPClient(30 * time.Second),
		stopChan:      make(chan struct{}),
		stopOnce:      sync.Once{},
	}
}

// Start begins streaming data from CSV to the queue continuously
func (s *Streamer) Start() error {
	logger.WithFields(map[string]interface{}{
		"csv_file":   s.config.CSVFilePath,
		"batch_size": s.config.BatchSize,
	}).Info("Starting streamer with continuous CSV processing")

	// Process CSV continuously in a loop
	for {
		if err := s.processCSV(); err != nil {
			logger.WithFields(map[string]interface{}{
				"error": err,
			}).Error("Failed to process CSV")
		}

		// Wait before processing again to simulate real-time streaming
		logger.Info("Waiting before next CSV processing cycle...")
		select {
		case <-s.stopChan:
			logger.Info("Streamer received shutdown signal")
			return nil
		case <-time.After(10 * time.Second):
			// Continue to next iteration
		}
	}
}

// processCSV reads and processes the CSV file
func (s *Streamer) processCSV() error {
	file, err := os.Open(s.config.CSVFilePath)
	if err != nil {
		return fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read CSV: %w", err)
	}

	logger.WithFields(map[string]interface{}{
		"total_records": len(records),
	}).Info("CSV file loaded")

	// Stream records in batches
	batch := make([]util.TelemetryRecord, 0, s.config.BatchSize)
	for i, row := range records {
		if i == 0 {
			// Skip header row
			continue
		}

		record, err := s.parseRow(row)
		if err != nil {
			logger.WithFields(map[string]interface{}{
				"row":   i,
				"error": err,
			}).Error("Failed to parse row")
			continue
		}

		batch = append(batch, record)

		if len(batch) >= s.config.BatchSize {
			if err := s.sendBatch(batch); err != nil {
				logger.WithFields(map[string]interface{}{
					"batch_size": len(batch),
					"error":      err,
				}).Error("Failed to send batch")
			}
			batch = batch[:0] // Clear batch
		}

		// Small delay to simulate real-time streaming
		time.Sleep(10 * time.Millisecond)
	}

	// Send remaining records
	if len(batch) > 0 {
		if err := s.sendBatch(batch); err != nil {
			logger.WithFields(map[string]interface{}{
				"batch_size": len(batch),
				"error":      err,
			}).Error("Failed to send final batch")
		}
	}

	logger.Info("CSV processing completed")
	return nil
}

// parsePrometheusLabels converts Prometheus label format (key="value" pairs) to JSON
func parsePrometheusLabels(labelsStr string) (map[string]interface{}, error) {
	labels := make(map[string]interface{})
	if labelsStr == "" {
		return labels, nil
	}

	// Split by comma to get individual key="value" pairs
	pairs := strings.Split(labelsStr, ",")
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		// Split by first '=' to get key and value
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Remove quotes from value if present
		if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
			value = value[1 : len(value)-1]
		}

		labels[key] = value
	}

	return labels, nil
}

// parseRow converts a CSV row to a TelemetryRecord
func (s *Streamer) parseRow(row []string) (util.TelemetryRecord, error) {
	if len(row) < util.MinCSVColumns {
		return util.TelemetryRecord{}, fmt.Errorf("invalid row: expected at least 12 columns, got %d", len(row))
	}

	// Use current time.
	timestamp := time.Now()

	// Parse value
	value, err := strconv.ParseFloat(row[10], 64)
	if err != nil {
		return util.TelemetryRecord{}, fmt.Errorf("invalid value: %w", err)
	}

	// Parse labels from Prometheus format
	var labels map[string]interface{}
	if len(row) > util.LabelsColumnIndex && row[util.LabelsColumnIndex] != "" {
		labels, err = parsePrometheusLabels(row[util.LabelsColumnIndex])
		if err != nil {
			logger.WithFields(map[string]interface{}{
				"error": err,
			}).Warn("Failed to parse labels, using empty map")
			labels = make(map[string]interface{})
		}
	}

	return util.TelemetryRecord{
		Timestamp:  timestamp,
		MetricName: row[1],
		GPUID:      row[2],
		Device:     row[3],
		UUID:       row[4],
		ModelName:  row[5],
		Hostname:   row[6],
		Container:  row[7],
		Pod:        row[8],
		Namespace:  row[9],
		Value:      value,
		Labels:     labels,
	}, nil
}

// sendBatch sends a batch of records to the queue
func (s *Streamer) sendBatch(records []util.TelemetryRecord) error {
	// Group records by partition
	partitionRecords := make(map[int][]util.TelemetryRecord)
	for _, record := range records {
		partitionID := util.GetPartitionID(record.UUID, s.numPartitions)
		logger.WithFields(map[string]interface{}{
			"uuid":         record.UUID,
			"partition_id": partitionID,
		}).Debug("Record assigned to partition")
		partitionRecords[partitionID] = append(partitionRecords[partitionID], record)
	}

	// Send each partition's records separately
	for partitionID, records := range partitionRecords {
		request := util.ProduceRequest{
			PartitionID: partitionID,
			Records:     records,
		}

		data, err := json.Marshal(request)
		if err != nil {
			return fmt.Errorf("failed to marshal request: %w", err)
		}

		if err := s.sendWithRetry(data); err != nil {
			return fmt.Errorf("failed to send partition %d: %w", partitionID, err)
		}
	}

	return nil
}

// sendWithRetry sends data to the queue with exponential backoff retry logic
func (s *Streamer) sendWithRetry(data []byte) error {
	var lastErr error
	for i := 0; i < s.config.RetryMaxAttempts; i++ {
		if i > 0 {
			delay := util.CalculateBackoffDelay(i, s.config.RetryInitialDelay, s.config.RetryMaxDelay, s.config.RetryMultiplier)
			logger.WithFields(map[string]interface{}{
				"attempt": i + 1,
				"delay":   delay,
			}).Info("Retrying with exponential backoff")
			time.Sleep(delay)
		}

		req := util.NewRequestFromBytes("POST", s.queueEndpoint, data, "application/json")

		resp, err := s.httpClient.Do(req)
		if err != nil {
			lastErr = err
			logger.WithFields(map[string]interface{}{
				"attempt": i + 1,
				"error":   err,
			}).Warn("Request failed, retrying")
			continue
		}

		if resp.StatusCode == http.StatusOK {
			return nil
		}

		lastErr = fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		logger.WithFields(map[string]interface{}{
			"attempt":     i + 1,
			"status_code": resp.StatusCode,
		}).Warn("Request failed with non-OK status")
	}

	return fmt.Errorf("failed after %d attempts: %w", s.config.RetryMaxAttempts, lastErr)
}

// Stop gracefully shuts down the streamer
func (s *Streamer) Stop() {
	s.stopOnce.Do(func() {
		logger.Info("Stopping streamer")
		close(s.stopChan)
	})
}
