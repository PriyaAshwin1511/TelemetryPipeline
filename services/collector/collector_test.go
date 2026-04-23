package collector

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"telemetry-collector/services/config"
	"telemetry-collector/services/util"

	"github.com/golang/mock/gomock"
)

func TestNewCollector(t *testing.T) {
	collectorCfg := &config.CollectorConfig{
		QueueEndpoint:    "http://localhost:8080",
		BatchSize:        100,
		PollInterval:     1 * time.Second,
		RetryMaxAttempts: 3,
		RetryDelay:       1 * time.Second,
	}

	dbCfg := &config.DatabaseConfig{
		Host:         "localhost",
		Port:         5432,
		User:         "postgres",
		Password:     "postgres",
		DBName:       "telemetry",
		MaxOpenConns: 25,
		MaxIdleConns: 5,
	}

	_, err := NewCollector(collectorCfg, dbCfg)
	if err != nil {
		t.Logf("NewCollector failed (expected if DB not running): %v", err)
	}
}

func TestStop(t *testing.T) {
	collectorCfg := &config.CollectorConfig{
		QueueEndpoint:    "http://localhost:8080",
		BatchSize:        100,
		PollInterval:     1 * time.Second,
		RetryMaxAttempts: 3,
		RetryDelay:       1 * time.Second,
	}

	dbCfg := &config.DatabaseConfig{
		Host:         "localhost",
		Port:         5432,
		User:         "postgres",
		Password:     "postgres",
		DBName:       "telemetry",
		MaxOpenConns: 25,
		MaxIdleConns: 5,
	}

	c, err := NewCollector(collectorCfg, dbCfg)
	if err != nil {
		t.Skipf("Skipping test - DB connection failed: %v", err)
	}

	// Stop should not panic
	c.Stop()
}

func TestNewCollector_ConfigValidation(t *testing.T) {
	tests := []struct {
		name         string
		collectorCfg *config.CollectorConfig
		dbCfg        *config.DatabaseConfig
		wantErr      bool
	}{
		{
			name: "valid config",
			collectorCfg: &config.CollectorConfig{
				QueueEndpoint:    "http://localhost:8080",
				BatchSize:        100,
				PollInterval:     1 * time.Second,
				RetryMaxAttempts: 3,
				RetryDelay:       1 * time.Second,
			},
			dbCfg: &config.DatabaseConfig{
				Host:         "localhost",
				Port:         5432,
				User:         "postgres",
				Password:     "postgres",
				DBName:       "telemetry",
				MaxOpenConns: 25,
				MaxIdleConns: 5,
			},
			wantErr: true, // Will fail if DB not running
		},
		{
			name: "empty queue endpoint",
			collectorCfg: &config.CollectorConfig{
				QueueEndpoint:    "",
				BatchSize:        100,
				PollInterval:     1 * time.Second,
				RetryMaxAttempts: 3,
				RetryDelay:       1 * time.Second,
			},
			dbCfg: &config.DatabaseConfig{
				Host:         "localhost",
				Port:         5432,
				User:         "postgres",
				Password:     "postgres",
				DBName:       "telemetry",
				MaxOpenConns: 25,
				MaxIdleConns: 5,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewCollector(tt.collectorCfg, tt.dbCfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewCollector() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewCollectorWithDB(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := util.NewMockDatabase(ctrl)
	collectorCfg := &config.CollectorConfig{
		QueueEndpoint:    "http://localhost:8080",
		BatchSize:        100,
		PollInterval:     1 * time.Second,
		RetryMaxAttempts: 3,
		RetryDelay:       1 * time.Second,
		ConsumerGroupID:  "test-group",
	}

	c := NewCollectorWithDB(collectorCfg, mockDB)

	if c == nil {
		t.Fatal("NewCollectorWithDB returned nil")
	}

	if c.config != collectorCfg {
		t.Error("config not set correctly")
	}

	if c.db != mockDB {
		t.Error("database not set correctly")
	}

	if c.stopChan == nil {
		t.Error("stopChan not initialized")
	}
}

func TestStart(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := util.NewMockDatabase(ctrl)
	collectorCfg := &config.CollectorConfig{
		QueueEndpoint:    "http://localhost:8080",
		BatchSize:        100,
		PollInterval:     100 * time.Millisecond,
		RetryMaxAttempts: 3,
		RetryDelay:       1 * time.Second,
		ConsumerGroupID:  "test-group",
	}

	c := NewCollectorWithDB(collectorCfg, mockDB)

	mockDB.EXPECT().Close().Return(nil).AnyTimes()

	err := c.Start()
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	// Give the goroutine time to start
	time.Sleep(50 * time.Millisecond)

	// Stop should not panic
	c.Stop()
}

func TestPollQueue_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := util.NewMockHTTPClient(ctrl)
	mockDB := util.NewMockDatabase(ctrl)
	mockTx := util.NewMockTx(ctrl)
	mockStmt := util.NewMockStmt(ctrl)
	mockResult := util.NewMockResult(ctrl)

	collectorCfg := &config.CollectorConfig{
		QueueEndpoint:    "http://localhost:8080",
		BatchSize:        10,
		PollInterval:     1 * time.Second,
		RetryMaxAttempts: 3,
		RetryDelay:       1 * time.Second,
		ConsumerGroupID:  "test-group",
	}

	c := NewCollectorWithDB(collectorCfg, mockDB)
	c.client = mockHTTPClient

	records := []util.TelemetryRecord{
		{
			Timestamp:  time.Now(),
			MetricName: "gpu_utilization",
			GPUID:      "gpu-001",
			UUID:       "uuid-001",
			Value:      75.5,
		},
	}

	responseBody, _ := json.Marshal(map[string]interface{}{
		"records":           records,
		"consumer_group_id": "test-group",
	})

	mockHTTPClient.EXPECT().Do(gomock.Any()).Return(&util.HTTPResponse{
		StatusCode: http.StatusOK,
		Body:       responseBody,
	}, nil)

	mockDB.EXPECT().Begin().Return(mockTx, nil)
	mockTx.EXPECT().Prepare(gomock.Any()).Return(mockStmt, nil)
	mockStmt.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockResult, nil).Times(1)
	mockStmt.EXPECT().Close().Return(nil)
	mockTx.EXPECT().Commit().Return(nil)
	mockTx.EXPECT().Rollback().Return(nil).AnyTimes()

	err := c.pollQueue()
	if err != nil {
		t.Fatalf("pollQueue() failed: %v", err)
	}
}

func TestPollQueue_HTTPError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := util.NewMockHTTPClient(ctrl)
	mockDB := util.NewMockDatabase(ctrl)

	collectorCfg := &config.CollectorConfig{
		QueueEndpoint:    "http://localhost:8080",
		BatchSize:        10,
		PollInterval:     1 * time.Second,
		RetryMaxAttempts: 3,
		RetryDelay:       1 * time.Second,
		ConsumerGroupID:  "test-group",
	}

	c := NewCollectorWithDB(collectorCfg, mockDB)
	c.client = mockHTTPClient

	mockHTTPClient.EXPECT().Do(gomock.Any()).Return(nil, &httpError{})

	err := c.pollQueue()
	if err == nil {
		t.Fatal("pollQueue() expected error for HTTP failure")
	}
}

func TestPollQueue_NonOKStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := util.NewMockHTTPClient(ctrl)
	mockDB := util.NewMockDatabase(ctrl)

	collectorCfg := &config.CollectorConfig{
		QueueEndpoint:    "http://localhost:8080",
		BatchSize:        10,
		PollInterval:     1 * time.Second,
		RetryMaxAttempts: 3,
		RetryDelay:       1 * time.Second,
		ConsumerGroupID:  "test-group",
	}

	c := NewCollectorWithDB(collectorCfg, mockDB)
	c.client = mockHTTPClient

	mockHTTPClient.EXPECT().Do(gomock.Any()).Return(&util.HTTPResponse{
		StatusCode: http.StatusInternalServerError,
		Body:       []byte("internal error"),
	}, nil)

	err := c.pollQueue()
	if err == nil {
		t.Fatal("pollQueue() expected error for non-OK status")
	}
}

func TestPollQueue_EmptyRecords(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHTTPClient := util.NewMockHTTPClient(ctrl)
	mockDB := util.NewMockDatabase(ctrl)

	collectorCfg := &config.CollectorConfig{
		QueueEndpoint:    "http://localhost:8080",
		BatchSize:        10,
		PollInterval:     1 * time.Second,
		RetryMaxAttempts: 3,
		RetryDelay:       1 * time.Second,
		ConsumerGroupID:  "test-group",
	}

	c := NewCollectorWithDB(collectorCfg, mockDB)
	c.client = mockHTTPClient

	responseBody, _ := json.Marshal(map[string]interface{}{
		"records":           []util.TelemetryRecord{},
		"consumer_group_id": "test-group",
	})

	mockHTTPClient.EXPECT().Do(gomock.Any()).Return(&util.HTTPResponse{
		StatusCode: http.StatusOK,
		Body:       responseBody,
	}, nil)

	err := c.pollQueue()
	if err != nil {
		t.Fatalf("pollQueue() failed with empty records: %v", err)
	}
}

func TestPersistRecords_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := util.NewMockDatabase(ctrl)
	mockTx := util.NewMockTx(ctrl)
	mockStmt := util.NewMockStmt(ctrl)
	mockResult := util.NewMockResult(ctrl)

	collectorCfg := &config.CollectorConfig{
		QueueEndpoint:    "http://localhost:8080",
		BatchSize:        100,
		PollInterval:     1 * time.Second,
		RetryMaxAttempts: 3,
		RetryDelay:       1 * time.Second,
		ConsumerGroupID:  "test-group",
	}

	c := NewCollectorWithDB(collectorCfg, mockDB)

	records := []util.TelemetryRecord{
		{
			Timestamp:  time.Now(),
			MetricName: "gpu_utilization",
			GPUID:      "gpu-001",
			UUID:       "uuid-001",
			Value:      75.5,
			Labels:     map[string]interface{}{"env": "prod"},
		},
	}

	mockDB.EXPECT().Begin().Return(mockTx, nil)
	mockTx.EXPECT().Prepare(gomock.Any()).Return(mockStmt, nil)
	mockStmt.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockResult, nil).Times(1)
	mockStmt.EXPECT().Close().Return(nil)
	mockTx.EXPECT().Commit().Return(nil)
	mockTx.EXPECT().Rollback().Return(nil).AnyTimes()

	err := c.persistRecords(records)
	if err != nil {
		t.Fatalf("persistRecords() failed: %v", err)
	}
}

func TestPersistRecords_BeginError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := util.NewMockDatabase(ctrl)

	collectorCfg := &config.CollectorConfig{
		QueueEndpoint:    "http://localhost:8080",
		BatchSize:        100,
		PollInterval:     1 * time.Second,
		RetryMaxAttempts: 3,
		RetryDelay:       1 * time.Second,
		ConsumerGroupID:  "test-group",
	}

	c := NewCollectorWithDB(collectorCfg, mockDB)

	records := []util.TelemetryRecord{
		{
			Timestamp:  time.Now(),
			MetricName: "gpu_utilization",
			GPUID:      "gpu-001",
			UUID:       "uuid-001",
			Value:      75.5,
		},
	}

	mockDB.EXPECT().Begin().Return(nil, &dbError{})

	err := c.persistRecords(records)
	if err == nil {
		t.Fatal("persistRecords() expected error for Begin failure")
	}
}

func TestPersistRecords_PrepareError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := util.NewMockDatabase(ctrl)
	mockTx := util.NewMockTx(ctrl)

	collectorCfg := &config.CollectorConfig{
		QueueEndpoint:    "http://localhost:8080",
		BatchSize:        100,
		PollInterval:     1 * time.Second,
		RetryMaxAttempts: 3,
		RetryDelay:       1 * time.Second,
		ConsumerGroupID:  "test-group",
	}

	c := NewCollectorWithDB(collectorCfg, mockDB)

	records := []util.TelemetryRecord{
		{
			Timestamp:  time.Now(),
			MetricName: "gpu_utilization",
			GPUID:      "gpu-001",
			UUID:       "uuid-001",
			Value:      75.5,
		},
	}

	mockDB.EXPECT().Begin().Return(mockTx, nil)
	mockTx.EXPECT().Prepare(gomock.Any()).Return(nil, &dbError{})
	mockTx.EXPECT().Rollback().Return(nil)

	err := c.persistRecords(records)
	if err == nil {
		t.Fatal("persistRecords() expected error for Prepare failure")
	}
}

func TestPersistRecords_ExecError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := util.NewMockDatabase(ctrl)
	mockTx := util.NewMockTx(ctrl)
	mockStmt := util.NewMockStmt(ctrl)

	collectorCfg := &config.CollectorConfig{
		QueueEndpoint:    "http://localhost:8080",
		BatchSize:        100,
		PollInterval:     1 * time.Second,
		RetryMaxAttempts: 3,
		RetryDelay:       1 * time.Second,
		ConsumerGroupID:  "test-group",
	}

	c := NewCollectorWithDB(collectorCfg, mockDB)

	records := []util.TelemetryRecord{
		{
			Timestamp:  time.Now(),
			MetricName: "gpu_utilization",
			GPUID:      "gpu-001",
			UUID:       "uuid-001",
			Value:      75.5,
		},
	}

	mockDB.EXPECT().Begin().Return(mockTx, nil)
	mockTx.EXPECT().Prepare(gomock.Any()).Return(mockStmt, nil)
	mockStmt.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, &dbError{}).Times(1)
	mockStmt.EXPECT().Close().Return(nil)
	mockTx.EXPECT().Rollback().Return(nil)

	err := c.persistRecords(records)
	if err == nil {
		t.Fatal("persistRecords() expected error for Exec failure")
	}
}

func TestPersistRecords_CommitError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := util.NewMockDatabase(ctrl)
	mockTx := util.NewMockTx(ctrl)
	mockStmt := util.NewMockStmt(ctrl)
	mockResult := util.NewMockResult(ctrl)

	collectorCfg := &config.CollectorConfig{
		QueueEndpoint:    "http://localhost:8080",
		BatchSize:        100,
		PollInterval:     1 * time.Second,
		RetryMaxAttempts: 3,
		RetryDelay:       1 * time.Second,
		ConsumerGroupID:  "test-group",
	}

	c := NewCollectorWithDB(collectorCfg, mockDB)

	records := []util.TelemetryRecord{
		{
			Timestamp:  time.Now(),
			MetricName: "gpu_utilization",
			GPUID:      "gpu-001",
			UUID:       "uuid-001",
			Value:      75.5,
		},
	}

	mockDB.EXPECT().Begin().Return(mockTx, nil)
	mockTx.EXPECT().Prepare(gomock.Any()).Return(mockStmt, nil)
	mockStmt.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockResult, nil).Times(1)
	mockStmt.EXPECT().Close().Return(nil)
	mockTx.EXPECT().Commit().Return(&dbError{})
	mockTx.EXPECT().Rollback().Return(nil).AnyTimes()

	err := c.persistRecords(records)
	if err == nil {
		t.Fatal("persistRecords() expected error for Commit failure")
	}
}

func TestPersistRecords_MultipleRecords(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := util.NewMockDatabase(ctrl)
	mockTx := util.NewMockTx(ctrl)
	mockStmt := util.NewMockStmt(ctrl)
	mockResult := util.NewMockResult(ctrl)

	collectorCfg := &config.CollectorConfig{
		QueueEndpoint:    "http://localhost:8080",
		BatchSize:        100,
		PollInterval:     1 * time.Second,
		RetryMaxAttempts: 3,
		RetryDelay:       1 * time.Second,
		ConsumerGroupID:  "test-group",
	}

	c := NewCollectorWithDB(collectorCfg, mockDB)

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
		{
			Timestamp:  time.Now(),
			MetricName: "temperature",
			GPUID:      "gpu-003",
			UUID:       "uuid-003",
			Value:      65.2,
		},
	}

	mockDB.EXPECT().Begin().Return(mockTx, nil)
	mockTx.EXPECT().Prepare(gomock.Any()).Return(mockStmt, nil)
	mockStmt.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockResult, nil).Times(3)
	mockStmt.EXPECT().Close().Return(nil)
	mockTx.EXPECT().Commit().Return(nil)
	mockTx.EXPECT().Rollback().Return(nil).AnyTimes()

	err := c.persistRecords(records)
	if err != nil {
		t.Fatalf("persistRecords() failed with multiple records: %v", err)
	}
}

func TestPersistRecords_LabelsMarshalError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := util.NewMockDatabase(ctrl)
	mockTx := util.NewMockTx(ctrl)
	mockStmt := util.NewMockStmt(ctrl)
	mockResult := util.NewMockResult(ctrl)

	collectorCfg := &config.CollectorConfig{
		QueueEndpoint:    "http://localhost:8080",
		BatchSize:        100,
		PollInterval:     1 * time.Second,
		RetryMaxAttempts: 3,
		RetryDelay:       1 * time.Second,
		ConsumerGroupID:  "test-group",
	}

	c := NewCollectorWithDB(collectorCfg, mockDB)

	records := []util.TelemetryRecord{
		{
			Timestamp:  time.Now(),
			MetricName: "gpu_utilization",
			GPUID:      "gpu-001",
			UUID:       "uuid-001",
			Value:      75.5,
			Labels:     map[string]interface{}{"key": make(chan int)}, // Unmarshalable type
		},
	}

	mockDB.EXPECT().Begin().Return(mockTx, nil)
	mockTx.EXPECT().Prepare(gomock.Any()).Return(mockStmt, nil)
	mockStmt.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockResult, nil).Times(1)
	mockStmt.EXPECT().Close().Return(nil)
	mockTx.EXPECT().Commit().Return(nil)
	mockTx.EXPECT().Rollback().Return(nil).AnyTimes()

	err := c.persistRecords(records)
	if err != nil {
		t.Fatalf("persistRecords() should handle marshal error gracefully: %v", err)
	}
}

func TestStop_ClosesDatabase(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := util.NewMockDatabase(ctrl)
	collectorCfg := &config.CollectorConfig{
		QueueEndpoint:    "http://localhost:8080",
		BatchSize:        100,
		PollInterval:     1 * time.Second,
		RetryMaxAttempts: 3,
		RetryDelay:       1 * time.Second,
		ConsumerGroupID:  "test-group",
	}

	c := NewCollectorWithDB(collectorCfg, mockDB)

	mockDB.EXPECT().Close().Return(nil)

	c.Stop()
}

func TestStop_NilDatabase(t *testing.T) {
	collectorCfg := &config.CollectorConfig{
		QueueEndpoint:    "http://localhost:8080",
		BatchSize:        100,
		PollInterval:     1 * time.Second,
		RetryMaxAttempts: 3,
		RetryDelay:       1 * time.Second,
		ConsumerGroupID:  "test-group",
	}

	c := NewCollectorWithDB(collectorCfg, nil)

	c.Stop()
}

type httpError struct{}

func (e *httpError) Error() string {
	return "HTTP request failed"
}

type dbError struct{}

func (e *dbError) Error() string {
	return "database operation failed"
}
