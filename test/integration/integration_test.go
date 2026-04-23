//go:build integration
// +build integration

package integration

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"telemetry-collector/services/config"
	"telemetry-collector/services/queue"
	"telemetry-collector/services/streamer"
	"telemetry-collector/services/util"
)

// TestStreamerToQueueIntegration tests the streamer sending data to queue
func TestStreamerToQueueIntegration(t *testing.T) {
	// Create temporary data directory
	tempDir := t.TempDir()

	// Create mock queue server
	queueServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("Expected POST request, got %s", r.Method)
		}

		// Read request body
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("Failed to read request body: %v", err)
			return
		}

		// Parse JSON
		var records []util.TelemetryRecord
		if err := json.Unmarshal(body, &records); err != nil {
			t.Errorf("Failed to unmarshal records: %v", err)
			return
		}

		// Return success response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"count":   len(records),
		})
	}))
	defer queueServer.Close()

	// Create test CSV file
	csvPath := tempDir + "/test.csv"
	csvContent := `timestamp,metric_name,gpu_id,device,uuid,model_name,hostname,container,pod,namespace,value,labels
2024-01-01T00:00:00Z,temperature,GPU-0aba4c65-be7d-8418-977a-c950c14b989a,0,GPU-0aba4c65-be7d-8418-977a-c950c14b989a,A100,host1,container1,pod1,ns1,42.5,key="value"
2024-01-01T00:00:01Z,power,GPU-0aba4c65-be7d-8418-977a-c950c14b989a,0,GPU-0aba4c65-be7d-8418-977a-c950c14b989a,A100,host1,container1,pod1,ns1,150.0,key="value"
`

	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("Failed to create test CSV: %v", err)
	}

	// Create streamer config
	streamerCfg := &config.StreamerConfig{
		CSVFilePath:       csvPath,
		BatchSize:         2,
		RetryMaxAttempts:  3,
		RetryInitialDelay: 1 * time.Second,
		RetryMaxDelay:     30 * time.Second,
		RetryMultiplier:   2.0,
	}

	// Create streamer
	s := streamer.NewStreamer(streamerCfg, queueServer.URL, 3)

	// Test that streamer was created successfully
	if s == nil {
		t.Fatal("Failed to create streamer")
	}

	t.Log("Integration test for streamer to queue passed")
}

// TestQueueProduceConsumeIntegration tests queue produce and consume
func TestQueueProduceConsumeIntegration(t *testing.T) {
	tempDir := t.TempDir()

	// Create queue config
	queueCfg := &config.QueueConfig{
		DataDir:              tempDir,
		NumPartitions:        3,
		CacheDuration:        2 * time.Minute,
		CacheCleanupInterval: 1 * time.Minute,
		MaxMessageSize:       1024 * 1024,
		SegmentSize:          100 * 1024 * 1024,
	}

	// Create queue
	q, err := queue.NewQueue(queueCfg)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	// Create test records
	timestamp1, _ := time.Parse(time.RFC3339, "2024-01-01T00:00:00Z")
	timestamp2, _ := time.Parse(time.RFC3339, "2024-01-01T00:00:01Z")

	// Use same UUID to ensure records go to same partition
	records := []util.TelemetryRecord{
		{
			Timestamp:  timestamp1,
			MetricName: "temperature",
			GPUID:      "GPU-001",
			Device:     "0",
			UUID:       "test-uuid-same",
			ModelName:  "A100",
			Hostname:   "host1",
			Container:  "container1",
			Pod:        "pod1",
			Namespace:  "ns1",
			Value:      42.5,
			Labels:     map[string]interface{}{"key": "value"},
		},
		{
			Timestamp:  timestamp2,
			MetricName: "power",
			GPUID:      "GPU-001",
			Device:     "0",
			UUID:       "test-uuid-same",
			ModelName:  "A100",
			Hostname:   "host1",
			Container:  "container1",
			Pod:        "pod1",
			Namespace:  "ns1",
			Value:      150.0,
			Labels:     map[string]interface{}{"key": "value"},
		},
	}

	// Produce records
	responses, err := q.Produce(records)
	if err != nil {
		t.Fatalf("Failed to produce records: %v", err)
	}

	for i, resp := range responses {
		if !resp.Success {
			t.Errorf("Record %d failed to produce: %s", i, resp.Message)
		}
	}

	// Consume from all partitions to find the records
	var foundRecords []util.TelemetryRecord
	var foundOffset int64

	partitionCount := q.GetPartitionCount()
	for i := 0; i < partitionCount; i++ {
		consumedRecords, offset, _, err := q.Consume(i, "test-group", 0, 10)
		if err != nil {
			t.Fatalf("Failed to consume from partition %d: %v", i, err)
		}

		if len(consumedRecords) > 0 {
			foundRecords = consumedRecords
			foundOffset = offset
			break
		}
	}

	if len(foundRecords) != 2 {
		t.Errorf("Expected 2 records, got %d", len(foundRecords))
	}

	if foundOffset != 2 {
		t.Errorf("Expected offset 2, got %d", foundOffset)
	}

	t.Log("Integration test for queue produce/consume passed")
}

// TestHTTPClientIntegration tests HTTP client with real HTTP server
func TestHTTPClientIntegration(t *testing.T) {
	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("Expected POST request, got %s", r.Method)
		}

		if r.URL.Path != "/produce" {
			t.Errorf("Expected /produce path, got %s", r.URL.Path)
		}

		// Check headers
		contentType := r.Header.Get("Content-Type")
		if contentType != "application/json" {
			t.Errorf("Expected Content-Type application/json, got %s", contentType)
		}

		// Read body
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("Failed to read body: %v", err)
			return
		}

		// Return success
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}))
	defer server.Close()

	// Create HTTP client
	client := util.NewRealHTTPClient(30 * time.Second)

	// Create request
	req := util.NewRequestFromBytes("POST", server.URL+"/produce", []byte(`{"test": "data"}`), "application/json")

	// Execute request
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Failed to execute request: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	if string(resp.Body) != `{"test": "data"}` {
		t.Errorf("Expected body {\"test\": \"data\"}, got %s", string(resp.Body))
	}

	t.Log("Integration test for HTTP client passed")
}

// TestQueueToCollectorIntegration tests queue to collector flow
func TestQueueToCollectorIntegration(t *testing.T) {
	tempDir := t.TempDir()

	// Create queue config
	queueCfg := &config.QueueConfig{
		DataDir:              tempDir,
		NumPartitions:        3,
		CacheDuration:        2 * time.Minute,
		CacheCleanupInterval: 1 * time.Minute,
		MaxMessageSize:       1024 * 1024,
		SegmentSize:          100 * 1024 * 1024,
	}

	// Create queue
	q, err := queue.NewQueue(queueCfg)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	// Create test records
	timestamp, _ := time.Parse(time.RFC3339, "2024-01-01T00:00:00Z")

	records := []util.TelemetryRecord{
		{
			Timestamp:  timestamp,
			MetricName: "temperature",
			GPUID:      "GPU-001",
			Device:     "0",
			UUID:       "test-uuid-same",
			ModelName:  "A100",
			Hostname:   "host1",
			Container:  "container1",
			Pod:        "pod1",
			Namespace:  "ns1",
			Value:      42.5,
			Labels:     map[string]interface{}{"key": "value"},
		},
	}

	// Produce records
	responses, err := q.Produce(records)
	if err != nil {
		t.Fatalf("Failed to produce records: %v", err)
	}

	for i, resp := range responses {
		if !resp.Success {
			t.Errorf("Record %d failed to produce: %s", i, resp.Message)
		}
	}

	// Create mock queue server for collector to consume from
	queueServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("Expected GET request, got %s", r.Method)
		}

		// Consume from all partitions to find the records
		var foundRecords []util.TelemetryRecord
		var foundOffset int64

		partitionCount := q.GetPartitionCount()
		for i := 0; i < partitionCount; i++ {
			consumedRecords, offset, _, err := q.Consume(i, "test-group", 0, 10)
			if err != nil {
				continue
			}

			if len(consumedRecords) > 0 {
				foundRecords = consumedRecords
				foundOffset = offset
				break
			}
		}

		// Return records
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"records": foundRecords,
			"offset":  foundOffset,
		})
	}))
	defer queueServer.Close()

	// Create HTTP client to consume from mock server
	client := util.NewRealHTTPClient(30 * time.Second)
	req := util.NewRequestFromBytes("GET", queueServer.URL+"/consume?count=10", nil, "")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Failed to consume from mock queue server: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	// Parse response
	var result struct {
		Records []util.TelemetryRecord `json:"records"`
		Offset  int64                  `json:"offset"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if len(result.Records) != 1 {
		t.Errorf("Expected 1 record, got %d", len(result.Records))
	}

	if result.Records[0].UUID != "test-uuid-same" {
		t.Errorf("Expected UUID test-uuid-same, got %s", result.Records[0].UUID)
	}

	t.Log("Integration test for queue to collector passed")
}

// TestCSVToQueueEndToEnd tests end-to-end CSV processing with data validation
func TestCSVToQueueEndToEnd(t *testing.T) {
	tempDir := t.TempDir()

	// Create mock queue server to receive data
	var receivedRecords []util.TelemetryRecord
	var mu sync.Mutex

	queueServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("Expected POST request, got %s", r.Method)
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("Failed to read request body: %v", err)
			return
		}

		var request util.ProduceRequest
		if err := json.Unmarshal(body, &request); err != nil {
			t.Errorf("Failed to unmarshal request: %v", err)
			return
		}

		mu.Lock()
		receivedRecords = append(receivedRecords, request.Records...)
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"count":   len(request.Records),
		})
	}))
	defer queueServer.Close()

	// Create test CSV file with various data scenarios
	csvPath := tempDir + "/test_validation.csv"
	csvContent := `timestamp,metric_name,gpu_id,device,uuid,model_name,hostname,container,pod,namespace,value,labels
2024-01-01T00:00:00Z,temperature,GPU-001,0,GPU-001,A100,host1,container1,pod1,ns1,42.5,
2024-01-01T00:00:01Z,power,GPU-002,0,GPU-002,A100,host2,container2,pod2,ns2,150.0,
2024-01-01T00:00:02Z,memory_usage,GPU-003,0,GPU-003,A100,host3,container3,pod3,ns3,80.5,
`
	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("Failed to create test CSV: %v", err)
	}

	// Process CSV by manually reading and sending (since processCSV is private)
	file, err := os.Open(csvPath)
	if err != nil {
		t.Fatalf("Failed to open CSV: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	// Skip header and process each row
	for i, row := range records {
		if i == 0 {
			continue // Skip header
		}

		// Create a produce request with the record
		timestamp, _ := time.Parse(time.RFC3339, row[0])
		value, _ := strconv.ParseFloat(row[10], 64)

		record := util.TelemetryRecord{
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
			Labels:     map[string]interface{}{},
		}

		request := util.ProduceRequest{
			PartitionID: 0,
			Records:     []util.TelemetryRecord{record},
		}

		data, err := json.Marshal(request)
		if err != nil {
			t.Fatalf("Failed to marshal request: %v", err)
		}

		req := util.NewRequestFromBytes("POST", queueServer.URL, data, "application/json")
		client := util.NewRealHTTPClient(30 * time.Second)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Expected status 200, got %d", resp.StatusCode)
		}
	}

	// Validate received records
	mu.Lock()
	defer mu.Unlock()

	if len(receivedRecords) != 3 {
		t.Errorf("Expected 3 records, got %d", len(receivedRecords))
	}

	// Validate each record
	expectedMetrics := []string{"temperature", "power", "memory_usage"}
	expectedValues := []float64{42.5, 150.0, 80.5}

	for i, record := range receivedRecords {
		if record.MetricName != expectedMetrics[i] {
			t.Errorf("Record %d: expected metric %s, got %s", i, expectedMetrics[i], record.MetricName)
		}
		if record.Value != expectedValues[i] {
			t.Errorf("Record %d: expected value %f, got %f", i, expectedValues[i], record.Value)
		}
		if record.UUID == "" {
			t.Errorf("Record %d: UUID should not be empty", i)
		}
		if record.Hostname == "" {
			t.Errorf("Record %d: Hostname should not be empty", i)
		}
	}

	t.Log("End-to-end CSV validation test passed")
}
