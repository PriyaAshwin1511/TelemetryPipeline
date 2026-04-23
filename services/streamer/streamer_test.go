package streamer

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"telemetry-collector/services/config"
	"telemetry-collector/services/util"

	"github.com/golang/mock/gomock"
)

func TestNewStreamer(t *testing.T) {
	cfg := &config.StreamerConfig{
		CSVFilePath:       "./data/telemetry.csv",
		BatchSize:         100,
		RetryMaxAttempts:  3,
		RetryInitialDelay: 1 * time.Second,
		RetryMaxDelay:     30 * time.Second,
		RetryMultiplier:   2.0,
	}

	s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)

	if s == nil {
		t.Fatal("NewStreamer returned nil")
	}

	if s.config != cfg {
		t.Error("config not set correctly")
	}

	if s.queueEndpoint != "http://queue-service:8080/produce" {
		t.Error("queueEndpoint not set correctly")
	}

	if s.httpClient == nil {
		t.Error("httpClient not initialized")
	}

	if s.stopChan == nil {
		t.Error("stopChan not initialized")
	}
}

func TestParsePrometheusLabels(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected map[string]interface{}
	}{
		{
			name:     "empty string",
			input:    "",
			expected: map[string]interface{}{},
		},
		{
			name:  "single pair",
			input: `key="value"`,
			expected: map[string]interface{}{
				"key": "value",
			},
		},
		{
			name:  "multiple pairs",
			input: `key1="value1",key2="value2"`,
			expected: map[string]interface{}{
				"key1": "value1",
				"key2": "value2",
			},
		},
		{
			name:  "with spaces",
			input: `key1 = "value1" , key2="value2"`,
			expected: map[string]interface{}{
				"key1": "value1",
				"key2": "value2",
			},
		},
		{
			name:  "malformed pair",
			input: `key=value`,
			expected: map[string]interface{}{
				"key": "value",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parsePrometheusLabels(tt.input)
			if err != nil {
				t.Errorf("parsePrometheusLabels() error = %v", err)
			}

			if len(result) != len(tt.expected) {
				t.Errorf("parsePrometheusLabels() = %v, want %v", result, tt.expected)
			}

			for k, v := range tt.expected {
				if result[k] != v {
					t.Errorf("parsePrometheusLabels()[%v] = %v, want %v", k, result[k], v)
				}
			}
		})
	}
}

func TestParseRow(t *testing.T) {
	cfg := &config.StreamerConfig{
		CSVFilePath:       "./data/telemetry.csv",
		BatchSize:         100,
		RetryMaxAttempts:  3,
		RetryInitialDelay: 1 * time.Second,
		RetryMaxDelay:     30 * time.Second,
		RetryMultiplier:   2.0,
	}

	s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)

	tests := []struct {
		name    string
		row     []string
		wantErr bool
	}{
		{
			name:    "valid row",
			row:     []string{"timestamp", "metric_name", "gpu_id", "device", "uuid", "model_name", "hostname", "container", "pod", "namespace", "42.5", `key="value"`},
			wantErr: false,
		},
		{
			name:    "invalid row - too few columns",
			row:     []string{"col1", "col2"},
			wantErr: true,
		},
		{
			name:    "invalid value",
			row:     []string{"timestamp", "metric_name", "gpu_id", "device", "uuid", "model_name", "hostname", "container", "pod", "namespace", "invalid", `key="value"`},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.parseRow(tt.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseRow() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParsePrometheusLabels_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected map[string]interface{}
	}{
		{
			name:     "empty value",
			input:    `key=""`,
			expected: map[string]interface{}{"key": ""},
		},
		{
			name:     "special characters",
			input:    `key="value-with_special.chars"`,
			expected: map[string]interface{}{"key": "value-with_special.chars"},
		},
		{
			name:     "numeric value",
			input:    `key="123"`,
			expected: map[string]interface{}{"key": "123"},
		},
		{
			name:     "value with spaces inside quotes",
			input:    `key="value with spaces"`,
			expected: map[string]interface{}{"key": "value with spaces"},
		},
		{
			name:     "uneven quotes",
			input:    `key="value`,
			expected: map[string]interface{}{"key": `"value`},
		},
		{
			name:     "no quotes",
			input:    `key=value`,
			expected: map[string]interface{}{"key": "value"},
		},
		{
			name:     "empty pair",
			input:    ``,
			expected: map[string]interface{}{},
		},
		{
			name:     "only comma",
			input:    `,`,
			expected: map[string]interface{}{},
		},
		{
			name:     "key without value",
			input:    `key=`,
			expected: map[string]interface{}{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parsePrometheusLabels(tt.input)
			if err != nil {
				t.Errorf("parsePrometheusLabels() error = %v", err)
			}

			for k, v := range tt.expected {
				if result[k] != v {
					t.Errorf("parsePrometheusLabels() = %v, want %v for key %s", result[k], v, k)
				}
			}
		})
	}
}

func TestParseRow_EdgeCases(t *testing.T) {
	cfg := &config.StreamerConfig{
		CSVFilePath:       "./data/telemetry.csv",
		BatchSize:         100,
		RetryMaxAttempts:  3,
		RetryInitialDelay: 1 * time.Second,
		RetryMaxDelay:     30 * time.Second,
		RetryMultiplier:   2.0,
	}

	s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)

	tests := []struct {
		name    string
		row     []string
		wantErr bool
	}{
		{
			name:    "row with empty fields",
			row:     []string{"timestamp", "metric_name", "gpu_id", "device", "", "model_name", "hostname", "container", "pod", "namespace", "42.5", `key="value"`},
			wantErr: false,
		},
		{
			name:    "row with zero value",
			row:     []string{"timestamp", "metric_name", "gpu_id", "device", "uuid", "model_name", "hostname", "container", "pod", "namespace", "0", `key="value"`},
			wantErr: false,
		},
		{
			name:    "row with negative value",
			row:     []string{"timestamp", "metric_name", "gpu_id", "device", "uuid", "model_name", "hostname", "container", "pod", "namespace", "-42.5", `key="value"`},
			wantErr: false,
		},
		{
			name:    "row with empty labels",
			row:     []string{"timestamp", "metric_name", "gpu_id", "device", "uuid", "model_name", "hostname", "container", "pod", "namespace", "42.5", ""},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record, err := s.parseRow(tt.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseRow() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				if record.UUID == "" && tt.row[4] != "" {
					t.Error("parseRow() UUID should not be empty for valid row")
				}
			}
		})
	}
}

func TestSendWithRetry_Gomock(t *testing.T) {
	// Test successful request
	t.Run("successful request", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockHTTPClient := util.NewMockHTTPClient(ctrl)

		cfg := &config.StreamerConfig{
			CSVFilePath:       "./data/telemetry.csv",
			BatchSize:         100,
			RetryMaxAttempts:  3,
			RetryInitialDelay: 1 * time.Second,
			RetryMaxDelay:     30 * time.Second,
			RetryMultiplier:   2.0,
		}

		s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)
		s.httpClient = mockHTTPClient

		mockHTTPClient.EXPECT().Do(gomock.Any()).Return(&util.HTTPResponse{
			StatusCode: 200,
			Body:       []byte(`{"success": true}`),
		}, nil)

		data := []byte(`{"test": "data"}`)
		err := s.sendWithRetry(data)
		if err != nil {
			t.Errorf("sendWithRetry() error = %v", err)
		}
	})

	// Test failed request with retry
	t.Run("failed request with retry", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockHTTPClient := util.NewMockHTTPClient(ctrl)

		cfg := &config.StreamerConfig{
			CSVFilePath:       "./data/telemetry.csv",
			BatchSize:         100,
			RetryMaxAttempts:  3,
			RetryInitialDelay: 1 * time.Second,
			RetryMaxDelay:     30 * time.Second,
			RetryMultiplier:   2.0,
		}

		s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)
		s.httpClient = mockHTTPClient

		mockHTTPClient.EXPECT().Do(gomock.Any()).Return(&util.HTTPResponse{
			StatusCode: 500,
			Body:       []byte(`{"error": "internal server error"}`),
		}, nil).Times(3)

		data := []byte(`{"test": "data"}`)
		err := s.sendWithRetry(data)
		if err == nil {
			t.Error("sendWithRetry() expected error, got nil")
		}
	})

	// Test network error with retry
	t.Run("network error with retry", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockHTTPClient := util.NewMockHTTPClient(ctrl)

		cfg := &config.StreamerConfig{
			CSVFilePath:       "./data/telemetry.csv",
			BatchSize:         100,
			RetryMaxAttempts:  3,
			RetryInitialDelay: 1 * time.Second,
			RetryMaxDelay:     30 * time.Second,
			RetryMultiplier:   2.0,
		}

		s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)
		s.httpClient = mockHTTPClient

		networkError := errors.New("connection refused")
		mockHTTPClient.EXPECT().Do(gomock.Any()).Return(nil, networkError).Times(3)

		data := []byte(`{"test": "data"}`)
		err := s.sendWithRetry(data)
		if err == nil {
			t.Error("sendWithRetry() expected error, got nil")
		}
	})

	// Test timeout error
	t.Run("timeout error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockHTTPClient := util.NewMockHTTPClient(ctrl)

		cfg := &config.StreamerConfig{
			CSVFilePath:       "./data/telemetry.csv",
			BatchSize:         100,
			RetryMaxAttempts:  3,
			RetryInitialDelay: 1 * time.Second,
			RetryMaxDelay:     30 * time.Second,
			RetryMultiplier:   2.0,
		}

		s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)
		s.httpClient = mockHTTPClient

		timeoutError := fmt.Errorf("request timeout")
		mockHTTPClient.EXPECT().Do(gomock.Any()).Return(nil, timeoutError).Times(3)

		data := []byte(`{"test": "data"}`)
		err := s.sendWithRetry(data)
		if err == nil {
			t.Error("sendWithRetry() expected error, got nil")
		}
	})

	// Test successful retry after initial failure
	t.Run("successful retry after failure", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockHTTPClient := util.NewMockHTTPClient(ctrl)

		cfg := &config.StreamerConfig{
			CSVFilePath:       "./data/telemetry.csv",
			BatchSize:         100,
			RetryMaxAttempts:  3,
			RetryInitialDelay: 1 * time.Millisecond, // Use short delay for testing
			RetryMaxDelay:     30 * time.Second,
			RetryMultiplier:   2.0,
		}

		s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)
		s.httpClient = mockHTTPClient

		// First attempt fails, second succeeds
		networkError := errors.New("connection refused")
		mockHTTPClient.EXPECT().Do(gomock.Any()).Return(nil, networkError).Times(1)
		mockHTTPClient.EXPECT().Do(gomock.Any()).Return(&util.HTTPResponse{
			StatusCode: 200,
			Body:       []byte(`{"success": true}`),
		}, nil).Times(1)

		data := []byte(`{"test": "data"}`)
		err := s.sendWithRetry(data)
		if err != nil {
			t.Errorf("sendWithRetry() unexpected error after successful retry: %v", err)
		}
	})

	// Test different non-OK status codes
	t.Run("different status codes", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockHTTPClient := util.NewMockHTTPClient(ctrl)

		cfg := &config.StreamerConfig{
			CSVFilePath:       "./data/telemetry.csv",
			BatchSize:         100,
			RetryMaxAttempts:  3,
			RetryInitialDelay: 1 * time.Millisecond,
			RetryMaxDelay:     30 * time.Second,
			RetryMultiplier:   2.0,
		}

		s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)
		s.httpClient = mockHTTPClient

		// Test 404 status code
		mockHTTPClient.EXPECT().Do(gomock.Any()).Return(&util.HTTPResponse{
			StatusCode: 404,
			Body:       []byte(`{"error": "not found"}`),
		}, nil).Times(3)

		data := []byte(`{"test": "data"}`)
		err := s.sendWithRetry(data)
		if err == nil {
			t.Error("sendWithRetry() expected error for 404 status")
		}
	})
}

func TestGetPartitionID_Consistency(t *testing.T) {
	cfg := &config.StreamerConfig{
		CSVFilePath:       "./data/telemetry.csv",
		BatchSize:         100,
		RetryMaxAttempts:  3,
		RetryInitialDelay: 1 * time.Second,
		RetryMaxDelay:     30 * time.Second,
		RetryMultiplier:   2.0,
	}

	s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)

	testUUIDs := []string{
		"test-uuid-123",
		"test-uuid-456",
		"test-uuid-789",
		"gpu-001-abc",
		"gpu-002-def",
	}

	for _, uuid := range testUUIDs {
		partition1 := util.GetPartitionID(uuid, s.numPartitions)
		partition2 := util.GetPartitionID(uuid, s.numPartitions)
		partition3 := util.GetPartitionID(uuid, s.numPartitions)

		if partition1 != partition2 || partition2 != partition3 {
			t.Errorf("UUID %s should always map to same partition: got %d, %d, %d", uuid, partition1, partition2, partition3)
		}

		if partition1 < 0 || partition1 >= 3 {
			t.Errorf("Partition ID should be between 0 and 2 for UUID %s: got %d", uuid, partition1)
		}
	}
}

func TestGetPartitionID_Distribution(t *testing.T) {
	cfg := &config.StreamerConfig{
		CSVFilePath:       "./data/telemetry.csv",
		BatchSize:         100,
		RetryMaxAttempts:  3,
		RetryInitialDelay: 1 * time.Second,
		RetryMaxDelay:     30 * time.Second,
		RetryMultiplier:   2.0,
	}

	s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)

	// Test with many UUIDs to check distribution
	partitionCount := make(map[int]int)
	numUUIDs := 100

	for i := 0; i < numUUIDs; i++ {
		uuid := fmt.Sprintf("test-uuid-%d", i)
		partitionID := util.GetPartitionID(uuid, s.numPartitions)
		partitionCount[partitionID]++
	}

	// All partitions should have some records (not all in one partition)
	partitionsWithRecords := 0
	for _, count := range partitionCount {
		if count > 0 {
			partitionsWithRecords++
		}
	}

	if partitionsWithRecords < 2 {
		t.Errorf("Expected records distributed across at least 2 partitions, got %d", partitionsWithRecords)
	}

	t.Logf("Partition distribution: %v", partitionCount)
}

func TestSendBatch_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := util.NewMockHTTPClient(ctrl)

	cfg := &config.StreamerConfig{
		CSVFilePath:       "./data/telemetry.csv",
		BatchSize:         100,
		RetryMaxAttempts:  3,
		RetryInitialDelay: 1 * time.Second,
		RetryMaxDelay:     30 * time.Second,
		RetryMultiplier:   2.0,
	}

	s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)
	s.httpClient = mockHTTPClient

	records := []util.TelemetryRecord{
		{
			Timestamp:  time.Now(),
			MetricName: "gpu_utilization",
			GPUID:      "gpu-001",
			UUID:       "uuid-001",
			Value:      75.5,
		},
		{
			Timestamp:  time.Now(),
			MetricName: "memory_usage",
			GPUID:      "gpu-002",
			UUID:       "uuid-002",
			Value:      50.0,
		},
	}

	mockHTTPClient.EXPECT().Do(gomock.Any()).Return(&util.HTTPResponse{
		StatusCode: 200,
		Body:       []byte(`{"success": true}`),
	}, nil).MinTimes(1)

	err := s.sendBatch(records)
	if err != nil {
		t.Fatalf("sendBatch() failed: %v", err)
	}
}

func TestSendBatch_MarshalError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := util.NewMockHTTPClient(ctrl)

	cfg := &config.StreamerConfig{
		CSVFilePath:       "./data/telemetry.csv",
		BatchSize:         100,
		RetryMaxAttempts:  3,
		RetryInitialDelay: 1 * time.Second,
		RetryMaxDelay:     30 * time.Second,
		RetryMultiplier:   2.0,
	}

	s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)
	s.httpClient = mockHTTPClient

	// Create a record with unmarshalable data to cause marshal error
	records := []util.TelemetryRecord{
		{
			Timestamp:  time.Now(),
			MetricName: "gpu_utilization",
			GPUID:      "gpu-001",
			UUID:       "uuid-001",
			Value:      75.5,
			Labels:     map[string]interface{}{"key": make(chan int)}, // Unmarshalable
		},
	}

	err := s.sendBatch(records)
	if err == nil {
		t.Fatal("sendBatch() expected error for marshal failure")
	}
}

func TestSendBatch_SendWithRetryError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := util.NewMockHTTPClient(ctrl)

	cfg := &config.StreamerConfig{
		CSVFilePath:       "./data/telemetry.csv",
		BatchSize:         100,
		RetryMaxAttempts:  3,
		RetryInitialDelay: 1 * time.Second,
		RetryMaxDelay:     30 * time.Second,
		RetryMultiplier:   2.0,
	}

	s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)
	s.httpClient = mockHTTPClient

	records := []util.TelemetryRecord{
		{
			Timestamp:  time.Now(),
			MetricName: "gpu_utilization",
			GPUID:      "gpu-001",
			UUID:       "uuid-001",
			Value:      75.5,
		},
	}

	mockHTTPClient.EXPECT().Do(gomock.Any()).Return(&util.HTTPResponse{
		StatusCode: 500,
		Body:       []byte(`{"error": "internal server error"}`),
	}, nil).Times(3)

	err := s.sendBatch(records)
	if err == nil {
		t.Fatal("sendBatch() expected error when sendWithRetry fails")
	}
}

func TestSendBatch_MultiplePartitions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := util.NewMockHTTPClient(ctrl)

	cfg := &config.StreamerConfig{
		CSVFilePath:       "./data/telemetry.csv",
		BatchSize:         100,
		RetryMaxAttempts:  3,
		RetryInitialDelay: 1 * time.Second,
		RetryMaxDelay:     30 * time.Second,
		RetryMultiplier:   2.0,
	}

	s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)
	s.httpClient = mockHTTPClient

	// Create records that will hash to different partitions
	records := []util.TelemetryRecord{
		{
			Timestamp:  time.Now(),
			MetricName: "gpu_utilization",
			GPUID:      "gpu-001",
			UUID:       "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
			Value:      75.5,
		},
		{
			Timestamp:  time.Now(),
			MetricName: "memory_usage",
			GPUID:      "gpu-002",
			UUID:       "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
			Value:      50.0,
		},
		{
			Timestamp:  time.Now(),
			MetricName: "temperature",
			GPUID:      "gpu-003",
			UUID:       "cccccccc-cccc-cccc-cccc-cccccccccccc",
			Value:      65.2,
		},
	}

	mockHTTPClient.EXPECT().Do(gomock.Any()).Return(&util.HTTPResponse{
		StatusCode: 200,
		Body:       []byte(`{"success": true}`),
	}, nil).MinTimes(1)

	err := s.sendBatch(records)
	if err != nil {
		t.Fatalf("sendBatch() failed with multiple partitions: %v", err)
	}
}

func TestProcessCSV_FileOpenError(t *testing.T) {
	cfg := &config.StreamerConfig{
		CSVFilePath:       "/nonexistent/path/to/file.csv",
		BatchSize:         100,
		RetryMaxAttempts:  3,
		RetryInitialDelay: 1 * time.Second,
		RetryMaxDelay:     30 * time.Second,
		RetryMultiplier:   2.0,
	}

	s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)

	err := s.processCSV()
	if err == nil {
		t.Fatal("processCSV() expected error for nonexistent file")
	}
}

func TestProcessCSV_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := util.NewMockHTTPClient(ctrl)

	// Create a temporary CSV file for testing
	tmpFile, err := os.CreateTemp("", "test_*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Write test CSV data
	csvContent := `timestamp,metric_name,gpu_id,device,uuid,model_name,hostname,container,pod,namespace,value,labels
2024-01-01T00:00:00Z,gpu_utilization,gpu-001,device-001,uuid-001,model-001,host-001,cont-001,pod-001,ns-001,75.5,
2024-01-01T00:00:01Z,memory_usage,gpu-002,device-002,uuid-002,model-002,host-002,cont-002,pod-002,ns-002,50.0,
`
	if _, err := tmpFile.WriteString(csvContent); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	cfg := &config.StreamerConfig{
		CSVFilePath:       tmpFile.Name(),
		BatchSize:         100,
		RetryMaxAttempts:  3,
		RetryInitialDelay: 1 * time.Second,
		RetryMaxDelay:     30 * time.Second,
		RetryMultiplier:   2.0,
	}

	s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)
	s.httpClient = mockHTTPClient

	mockHTTPClient.EXPECT().Do(gomock.Any()).Return(&util.HTTPResponse{
		StatusCode: 200,
		Body:       []byte(`{"success": true}`),
	}, nil).AnyTimes()

	err = s.processCSV()
	if err != nil {
		t.Fatalf("processCSV() failed: %v", err)
	}
}

func TestProcessCSV_WithBatchSize(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := util.NewMockHTTPClient(ctrl)

	// Create a temporary CSV file with more records than batch size
	tmpFile, err := os.CreateTemp("", "test_*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Write test CSV data with 5 records
	csvContent := `timestamp,metric_name,gpu_id,device,uuid,model_name,hostname,container,pod,namespace,value,labels
2024-01-01T00:00:00Z,gpu_utilization,gpu-001,device-001,uuid-001,model-001,host-001,cont-001,pod-001,ns-001,75.5,
2024-01-01T00:00:01Z,memory_usage,gpu-002,device-002,uuid-002,model-002,host-002,cont-002,pod-002,ns-002,50.0,
2024-01-01T00:00:02Z,temperature,gpu-003,device-003,uuid-003,model-003,host-003,cont-003,pod-003,ns-003,65.2,
2024-01-01T00:00:03Z,power_usage,gpu-004,device-004,uuid-004,model-004,host-004,cont-004,pod-004,ns-004,120.0,
2024-01-01T00:00:04Z,fan_speed,gpu-005,device-005,uuid-005,model-005,host-005,cont-005,pod-005,ns-005,3000.0,
`
	if _, err := tmpFile.WriteString(csvContent); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	cfg := &config.StreamerConfig{
		CSVFilePath:       tmpFile.Name(),
		BatchSize:         2, // Small batch size to test batching
		RetryMaxAttempts:  3,
		RetryInitialDelay: 1 * time.Second,
		RetryMaxDelay:     30 * time.Second,
		RetryMultiplier:   2.0,
	}

	s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)
	s.httpClient = mockHTTPClient

	mockHTTPClient.EXPECT().Do(gomock.Any()).Return(&util.HTTPResponse{
		StatusCode: 200,
		Body:       []byte(`{"success": true}`),
	}, nil).AnyTimes()

	err = s.processCSV()
	if err != nil {
		t.Fatalf("processCSV() failed with batching: %v", err)
	}
}

func TestProcessCSV_WithParseError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := util.NewMockHTTPClient(ctrl)

	// Create a temporary CSV file with invalid data
	tmpFile, err := os.CreateTemp("", "test_*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Write test CSV data with invalid value
	csvContent := `timestamp,metric_name,gpu_id,device,uuid,model_name,hostname,container,pod,namespace,value,labels
2024-01-01T00:00:00Z,gpu_utilization,gpu-001,device-001,uuid-001,model-001,host-001,cont-001,pod-001,ns-001,invalid,
2024-01-01T00:00:01Z,memory_usage,gpu-002,device-002,uuid-002,model-002,host-002,cont-002,pod-002,ns-002,50.0,
`
	if _, err := tmpFile.WriteString(csvContent); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	cfg := &config.StreamerConfig{
		CSVFilePath:       tmpFile.Name(),
		BatchSize:         100,
		RetryMaxAttempts:  3,
		RetryInitialDelay: 1 * time.Second,
		RetryMaxDelay:     30 * time.Second,
		RetryMultiplier:   2.0,
	}

	s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)
	s.httpClient = mockHTTPClient

	mockHTTPClient.EXPECT().Do(gomock.Any()).Return(&util.HTTPResponse{
		StatusCode: 200,
		Body:       []byte(`{"success": true}`),
	}, nil).AnyTimes()

	// Should succeed even with one invalid row (it should be skipped)
	err = s.processCSV()
	if err != nil {
		t.Fatalf("processCSV() should handle parse errors gracefully: %v", err)
	}
}

func TestParseRow_CSVSchemaValidation(t *testing.T) {
	cfg := &config.StreamerConfig{
		CSVFilePath:       "./data/telemetry.csv",
		BatchSize:         100,
		RetryMaxAttempts:  3,
		RetryInitialDelay: 1 * time.Second,
		RetryMaxDelay:     30 * time.Second,
		RetryMultiplier:   2.0,
	}

	s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)

	tests := []struct {
		name    string
		row     []string
		wantErr bool
	}{
		{
			name:    "missing columns - too few",
			row:     []string{"timestamp", "metric_name", "gpu_id"},
			wantErr: true,
		},
		{
			name:    "missing required field - uuid",
			row:     []string{"timestamp", "metric_name", "gpu_id", "device", "", "model_name", "hostname", "container", "pod", "namespace", "42.5", `key="value"`},
			wantErr: false, // Empty UUID is allowed based on current implementation
		},
		{
			name:    "extra columns - should still work",
			row:     []string{"timestamp", "metric_name", "gpu_id", "device", "uuid", "model_name", "hostname", "container", "pod", "namespace", "42.5", `key="value"`, "extra_col"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.parseRow(tt.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseRow() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseRow_DataTypeValidation(t *testing.T) {
	cfg := &config.StreamerConfig{
		CSVFilePath:       "./data/telemetry.csv",
		BatchSize:         100,
		RetryMaxAttempts:  3,
		RetryInitialDelay: 1 * time.Second,
		RetryMaxDelay:     30 * time.Second,
		RetryMultiplier:   2.0,
	}

	s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)

	tests := []struct {
		name    string
		row     []string
		wantErr bool
	}{
		{
			name:    "non-numeric value field",
			row:     []string{"2024-01-01T00:00:00Z", "metric_name", "gpu_id", "device", "uuid", "model_name", "hostname", "container", "pod", "namespace", "not-a-number", `key="value"`},
			wantErr: true,
		},
		{
			name:    "valid timestamp with RFC3339",
			row:     []string{"2024-01-01T00:00:00Z", "metric_name", "gpu_id", "device", "uuid", "model_name", "hostname", "container", "pod", "namespace", "42.5", `key="value"`},
			wantErr: false,
		},
		{
			name:    "special characters in string fields",
			row:     []string{"2024-01-01T00:00:00Z", "metric_name_with-special", "gpu_id", "device", "uuid", "model_name", "hostname", "container", "pod", "namespace", "42.5", `key="value-with_special"`},
			wantErr: false,
		},
		{
			name:    "scientific notation in value",
			row:     []string{"2024-01-01T00:00:00Z", "metric_name", "gpu_id", "device", "uuid", "model_name", "hostname", "container", "pod", "namespace", "1.5e2", `key="value"`},
			wantErr: false,
		},
		{
			name:    "timestamp field is ignored (uses current time)",
			row:     []string{"invalid-timestamp", "metric_name", "gpu_id", "device", "uuid", "model_name", "hostname", "container", "pod", "namespace", "42.5", `key="value"`},
			wantErr: false, // Code uses time.Now() instead of parsing CSV timestamp
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.parseRow(tt.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseRow() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestProcessCSV_EdgeCases(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := util.NewMockHTTPClient(ctrl)

	t.Run("empty CSV file", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "test_*.csv")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer os.Remove(tmpFile.Name())

		// Write only header
		csvContent := `timestamp,metric_name,gpu_id,device,uuid,model_name,hostname,container,pod,namespace,value,labels
`
		if _, err := tmpFile.WriteString(csvContent); err != nil {
			t.Fatalf("Failed to write to temp file: %v", err)
		}
		tmpFile.Close()

		cfg := &config.StreamerConfig{
			CSVFilePath:       tmpFile.Name(),
			BatchSize:         100,
			RetryMaxAttempts:  3,
			RetryInitialDelay: 1 * time.Second,
			RetryMaxDelay:     30 * time.Second,
			RetryMultiplier:   2.0,
		}

		s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)
		s.httpClient = mockHTTPClient

		mockHTTPClient.EXPECT().Do(gomock.Any()).Return(&util.HTTPResponse{
			StatusCode: 200,
			Body:       []byte(`{"success": true}`),
		}, nil).AnyTimes()

		err = s.processCSV()
		if err != nil {
			t.Fatalf("processCSV() should handle empty CSV gracefully: %v", err)
		}
	})

	t.Run("CSV with only headers", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "test_*.csv")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer os.Remove(tmpFile.Name())

		// Write only header
		csvContent := `timestamp,metric_name,gpu_id,device,uuid,model_name,hostname,container,pod,namespace,value,labels
`
		if _, err := tmpFile.WriteString(csvContent); err != nil {
			t.Fatalf("Failed to write to temp file: %v", err)
		}
		tmpFile.Close()

		cfg := &config.StreamerConfig{
			CSVFilePath:       tmpFile.Name(),
			BatchSize:         100,
			RetryMaxAttempts:  3,
			RetryInitialDelay: 1 * time.Second,
			RetryMaxDelay:     30 * time.Second,
			RetryMultiplier:   2.0,
		}

		s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)
		s.httpClient = mockHTTPClient

		mockHTTPClient.EXPECT().Do(gomock.Any()).Return(&util.HTTPResponse{
			StatusCode: 200,
			Body:       []byte(`{"success": true}`),
		}, nil).AnyTimes()

		err = s.processCSV()
		if err != nil {
			t.Fatalf("processCSV() should handle CSV with only headers: %v", err)
		}
	})

	t.Run("CSV with duplicate rows", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "test_*.csv")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer os.Remove(tmpFile.Name())

		// Write CSV with duplicate rows
		csvContent := `timestamp,metric_name,gpu_id,device,uuid,model_name,hostname,container,pod,namespace,value,labels
2024-01-01T00:00:00Z,gpu_utilization,gpu-001,device-001,uuid-001,model-001,host-001,cont-001,pod-001,ns-001,75.5,
2024-01-01T00:00:01Z,gpu_utilization,gpu-001,device-001,uuid-001,model-001,host-001,cont-001,pod-001,ns-001,75.5,
`
		if _, err := tmpFile.WriteString(csvContent); err != nil {
			t.Fatalf("Failed to write to temp file: %v", err)
		}
		tmpFile.Close()

		cfg := &config.StreamerConfig{
			CSVFilePath:       tmpFile.Name(),
			BatchSize:         100,
			RetryMaxAttempts:  3,
			RetryInitialDelay: 1 * time.Second,
			RetryMaxDelay:     30 * time.Second,
			RetryMultiplier:   2.0,
		}

		s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)
		s.httpClient = mockHTTPClient

		mockHTTPClient.EXPECT().Do(gomock.Any()).Return(&util.HTTPResponse{
			StatusCode: 200,
			Body:       []byte(`{"success": true}`),
		}, nil).AnyTimes()

		err = s.processCSV()
		if err != nil {
			t.Fatalf("processCSV() should handle duplicate rows: %v", err)
		}
	})

	t.Run("CSV with different device_ids for partitioning", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "test_*.csv")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer os.Remove(tmpFile.Name())

		// Write CSV with different UUIDs
		csvContent := `timestamp,metric_name,gpu_id,device,uuid,model_name,hostname,container,pod,namespace,value,labels
2024-01-01T00:00:00Z,gpu_utilization,gpu-001,device-001,uuid-aaa-aaa-aaa,model-001,host-001,cont-001,pod-001,ns-001,75.5,
2024-01-01T00:00:01Z,gpu_utilization,gpu-002,device-002,uuid-bbb-bbb-bbb,model-002,host-002,cont-002,pod-002,ns-002,50.0,
2024-01-01T00:00:02Z,gpu_utilization,gpu-003,device-003,uuid-ccc-ccc-ccc,model-003,host-003,cont-003,pod-003,ns-003,65.2,
`
		if _, err := tmpFile.WriteString(csvContent); err != nil {
			t.Fatalf("Failed to write to temp file: %v", err)
		}
		tmpFile.Close()

		cfg := &config.StreamerConfig{
			CSVFilePath:       tmpFile.Name(),
			BatchSize:         100,
			RetryMaxAttempts:  3,
			RetryInitialDelay: 1 * time.Second,
			RetryMaxDelay:     30 * time.Second,
			RetryMultiplier:   2.0,
		}

		s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)
		s.httpClient = mockHTTPClient

		mockHTTPClient.EXPECT().Do(gomock.Any()).Return(&util.HTTPResponse{
			StatusCode: 200,
			Body:       []byte(`{"success": true}`),
		}, nil).AnyTimes()

		err = s.processCSV()
		if err != nil {
			t.Fatalf("processCSV() should handle different device_ids: %v", err)
		}
	})
}

func TestStop(t *testing.T) {
	cfg := &config.StreamerConfig{
		CSVFilePath:       "./data/telemetry.csv",
		BatchSize:         100,
		RetryMaxAttempts:  3,
		RetryInitialDelay: 1 * time.Second,
		RetryMaxDelay:     30 * time.Second,
		RetryMultiplier:   2.0,
	}

	s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)

	// Stop should not panic
	s.Stop()

	// Calling Stop again should not panic (channel close is idempotent)
	s.Stop()
}

func TestStart_GracefulShutdown(t *testing.T) {
	cfg := &config.StreamerConfig{
		CSVFilePath:       "/nonexistent/path/to/file.csv",
		BatchSize:         100,
		RetryMaxAttempts:  3,
		RetryInitialDelay: 10 * time.Millisecond,
		RetryMaxDelay:     30 * time.Second,
		RetryMultiplier:   2.0,
	}

	s := NewStreamer(cfg, "http://queue-service:8080/produce", 3)

	// Start in goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- s.Start()
	}()

	// Wait a bit for it to start
	time.Sleep(50 * time.Millisecond)

	// Call Stop to gracefully shutdown
	s.Stop()

	// Wait for Start to return (allow time for sleep to complete)
	select {
	case err := <-errChan:
		if err != nil {
			t.Logf("Start returned error (expected for nonexistent file): %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return within timeout after Stop")
	}
}
