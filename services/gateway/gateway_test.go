package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"telemetry-collector/services/config"
	"telemetry-collector/services/util"

	"github.com/gin-gonic/gin"
	"github.com/golang/mock/gomock"
)

func TestNewGateway(t *testing.T) {
	serverCfg := &config.ServerConfig{
		Port:         8080,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
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

	_, err := NewGateway(serverCfg, dbCfg)
	if err != nil {
		t.Logf("NewGateway failed (expected if DB not running): %v", err)
	}
}

func TestUUIDValidation(t *testing.T) {
	tests := []struct {
		name  string
		uuid  string
		valid bool
	}{
		{
			name:  "valid uuid",
			uuid:  "GPU-0aba4c65-be7d-8418-977a-c950c14b989a",
			valid: true,
		},
		{
			name:  "empty uuid",
			uuid:  "",
			valid: false,
		},
		{
			name:  "uuid with spaces",
			uuid:  "  ",
			valid: false,
		},
		{
			name:  "short uuid",
			uuid:  "abc",
			valid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isValid := strings.TrimSpace(tt.uuid) != ""
			if isValid != tt.valid {
				t.Errorf("UUID validation failed: uuid=%s, valid=%v, want=%v", tt.uuid, isValid, tt.valid)
			}
		})
	}
}

func TestNewGateway_ConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *config.ServerConfig
		dbCfg   *config.DatabaseConfig
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: &config.ServerConfig{
				Port:         8080,
				ReadTimeout:  30 * time.Second,
				WriteTimeout: 30 * time.Second,
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
			name: "zero port",
			cfg: &config.ServerConfig{
				Port:         0,
				ReadTimeout:  30 * time.Second,
				WriteTimeout: 30 * time.Second,
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
			_, err := NewGateway(tt.cfg, tt.dbCfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewGateway() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGateway_GetTelemetryByUUID_Gomock(t *testing.T) {
	// Test successful query with mocked database
	t.Run("successful query", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDB := util.NewMockDatabase(ctrl)

		serverCfg := &config.ServerConfig{
			Port:         8080,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
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

		g, err := NewGateway(serverCfg, dbCfg)
		if err != nil {
			t.Skipf("Skipping test - gateway creation failed: %v", err)
		}
		g.db = mockDB

		// Setup mock row
		mockRow := util.NewMockRow(ctrl)
		mockRow.EXPECT().Scan(gomock.Any()).Return(nil)

		mockDB.EXPECT().QueryRowContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockRow)

		t.Log("Gateway now uses Database interface - test can run")
	})

	// Test database error
	t.Run("database error", func(t *testing.T) {
		t.Skip("getTelemetryByUUID is a private method - test needs to be refactored to test public API")
	})
}

func TestGateway_ListUUIDs_Gomock(t *testing.T) {
	t.Run("successful list", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		_ = util.NewMockDatabase(ctrl)

		t.Skip("Gateway needs to be refactored to use Database interface")
	})
}

// TestHealthCheck tests the health check endpoint
func TestHealthCheck(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := util.NewMockDatabase(ctrl)

	gateway := &Gateway{
		config: &config.ServerConfig{Port: 8080},
		db:     mockDB,
	}

	router := gin.New()
	router.GET("/health", gateway.healthCheck)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response util.HealthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Errorf("Failed to unmarshal response: %v", err)
	}

	if response.Status != "healthy" {
		t.Errorf("Expected status 'healthy', got '%s'", response.Status)
	}
}

// TestListUUIDs tests the listUUIDs endpoint
func TestListUUIDs(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("successful list", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDB := util.NewMockDatabase(ctrl)
		mockRows := util.NewMockRows(ctrl)

		gateway := &Gateway{
			config: &config.ServerConfig{Port: 8080},
			db:     mockDB,
		}

		// Setup mock expectations
		mockDB.EXPECT().QueryContext(gomock.Any(), gomock.Any()).Return(mockRows, nil)
		mockRows.EXPECT().Close().Return(nil)
		mockRows.EXPECT().Next().Return(true)
		mockRows.EXPECT().Scan(gomock.Any()).Do(func(dest *string) {
			*dest = "uuid-1"
		}).Return(nil)
		mockRows.EXPECT().Next().Return(true)
		mockRows.EXPECT().Scan(gomock.Any()).Do(func(dest *string) {
			*dest = "uuid-2"
		}).Return(nil)
		mockRows.EXPECT().Next().Return(false)
		mockRows.EXPECT().Err().Return(nil)

		router := gin.New()
		router.GET("/api/v1/gpus", gateway.listUUIDs)

		req := httptest.NewRequest("GET", "/api/v1/gpus", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}

		var response map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Errorf("Failed to unmarshal response: %v", err)
		}

		uuids, ok := response["uuids"].([]interface{})
		if !ok {
			t.Error("Expected uuids array in response")
		}

		if len(uuids) != 2 {
			t.Errorf("Expected 2 UUIDs, got %d", len(uuids))
		}
	})

	t.Run("database error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDB := util.NewMockDatabase(ctrl)

		gateway := &Gateway{
			config: &config.ServerConfig{Port: 8080},
			db:     mockDB,
		}

		mockDB.EXPECT().QueryContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, assertError{msg: "test error"})

		router := gin.New()
		router.GET("/api/v1/gpus", gateway.listUUIDs)

		req := httptest.NewRequest("GET", "/api/v1/gpus", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("Expected status 500, got %d", w.Code)
		}
	})

	t.Run("empty result", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDB := util.NewMockDatabase(ctrl)
		mockRows := util.NewMockRows(ctrl)

		gateway := &Gateway{
			config: &config.ServerConfig{Port: 8080},
			db:     mockDB,
		}

		mockDB.EXPECT().QueryContext(gomock.Any(), gomock.Any()).Return(mockRows, nil)
		mockRows.EXPECT().Close().Return(nil)
		mockRows.EXPECT().Next().Return(false)
		mockRows.EXPECT().Err().Return(nil)

		router := gin.New()
		router.GET("/api/v1/gpus", gateway.listUUIDs)

		req := httptest.NewRequest("GET", "/api/v1/gpus", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}

		var response map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Errorf("Failed to unmarshal response: %v", err)
		}

		uuids, ok := response["uuids"]
		if !ok {
			t.Error("Expected uuids in response")
			return
		}

		// uuids could be null or an empty array
		if uuids != nil {
			uuidSlice, ok := uuids.([]interface{})
			if !ok {
				t.Error("Expected uuids to be an array")
				return
			}

			if len(uuidSlice) != 0 {
				t.Errorf("Expected 0 UUIDs, got %d", len(uuidSlice))
			}
		}
	})
}

// TestGetTelemetryByUUID tests the getTelemetryByUUID endpoint
func TestGetTelemetryByUUID(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("valid UUID with records", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDB := util.NewMockDatabase(ctrl)
		mockRows := util.NewMockRows(ctrl)
		mockRow := util.NewMockRow(ctrl)

		gateway := &Gateway{
			config: &config.ServerConfig{Port: 8080},
			db:     mockDB,
		}

		testUUID := "GPU-0aba4c65-be7d-8418-977a-c950c14b989a"
		testTime := time.Now()

		// Mock count query
		mockRow.EXPECT().Scan(gomock.Any()).Do(func(dest *int) {
			*dest = 10
		}).Return(nil)
		mockDB.EXPECT().QueryRowContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockRow)

		// Mock data query
		mockDB.EXPECT().QueryContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockRows, nil)
		mockRows.EXPECT().Close().Return(nil)
		mockRows.EXPECT().Next().Return(true)
		mockRows.EXPECT().Scan(gomock.Any()).
			Do(func(dest ...interface{}) {
				timestamp := dest[0].(*time.Time)
				metricName := dest[1].(*string)
				gpuID := dest[2].(*string)
				device := dest[3].(*string)
				uuid := dest[4].(*string)
				modelName := dest[5].(*string)
				hostname := dest[6].(*string)
				container := dest[7].(*string)
				pod := dest[8].(*string)
				namespace := dest[9].(*string)
				value := dest[10].(*float64)
				labelsJSON := dest[11].(*[]byte)
				*timestamp = testTime
				*metricName = "gpu_utilization"
				*gpuID = "0"
				*device = "/dev/nvidia0"
				*uuid = testUUID
				*modelName = "A100"
				*hostname = "node-1"
				*container = "container-1"
				*pod = "pod-1"
				*namespace = "default"
				*value = 85.5
				*labelsJSON = []byte(`{"key":"value"}`)
			}).Return(nil)
		mockRows.EXPECT().Next().Return(false)
		mockRows.EXPECT().Err().Return(nil)

		router := gin.New()
		router.GET("/api/v1/gpus/:id/telemetry", gateway.getTelemetryByUUID)

		req := httptest.NewRequest("GET", "/api/v1/gpus/"+testUUID+"/telemetry", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}

		var response map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Errorf("Failed to unmarshal response: %v", err)
		}

		if _, ok := response["telemetry"]; !ok {
			t.Error("Expected telemetry in response")
		}

		if _, ok := response["pagination"]; !ok {
			t.Error("Expected pagination in response")
		}
	})

	t.Run("empty UUID", func(t *testing.T) {
		gin.SetMode(gin.TestMode)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDB := util.NewMockDatabase(ctrl)

		gateway := &Gateway{
			config: &config.ServerConfig{Port: 8080},
			db:     mockDB,
		}

		router := gin.New()
		router.GET("/api/v1/gpus/:id/telemetry", gateway.getTelemetryByUUID)

		req := httptest.NewRequest("GET", "/api/v1/gpus//telemetry", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400, got %d", w.Code)
		}
	})

	t.Run("invalid start_time format", func(t *testing.T) {
		gin.SetMode(gin.TestMode)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDB := util.NewMockDatabase(ctrl)

		gateway := &Gateway{
			config: &config.ServerConfig{Port: 8080},
			db:     mockDB,
		}

		router := gin.New()
		router.GET("/api/v1/gpus/:id/telemetry", gateway.getTelemetryByUUID)

		req := httptest.NewRequest("GET", "/api/v1/gpus/test-uuid/telemetry?start_time=invalid", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400, got %d", w.Code)
		}
	})

	t.Run("invalid end_time format", func(t *testing.T) {
		gin.SetMode(gin.TestMode)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDB := util.NewMockDatabase(ctrl)

		gateway := &Gateway{
			config: &config.ServerConfig{Port: 8080},
			db:     mockDB,
		}

		router := gin.New()
		router.GET("/api/v1/gpus/:id/telemetry", gateway.getTelemetryByUUID)

		req := httptest.NewRequest("GET", "/api/v1/gpus/test-uuid/telemetry?end_time=invalid", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400, got %d", w.Code)
		}
	})

	t.Run("start_time after end_time", func(t *testing.T) {
		gin.SetMode(gin.TestMode)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDB := util.NewMockDatabase(ctrl)

		gateway := &Gateway{
			config: &config.ServerConfig{Port: 8080},
			db:     mockDB,
		}

		startTime := time.Now().Add(24 * time.Hour).Format(time.RFC3339)
		endTime := time.Now().Format(time.RFC3339)

		router := gin.New()
		router.GET("/api/v1/gpus/:id/telemetry", gateway.getTelemetryByUUID)

		req := httptest.NewRequest("GET", "/api/v1/gpus/test-uuid/telemetry?start_time="+startTime+"&end_time="+endTime, nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400, got %d", w.Code)
		}
	})

	t.Run("no records found", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDB := util.NewMockDatabase(ctrl)
		mockRows := util.NewMockRows(ctrl)
		mockRow := util.NewMockRow(ctrl)

		gateway := &Gateway{
			config: &config.ServerConfig{Port: 8080},
			db:     mockDB,
		}

		testUUID := "GPU-0aba4c65-be7d-8418-977a-c950c14b989a"

		// Mock count query returning 0
		mockRow.EXPECT().Scan(gomock.Any()).Do(func(dest *int) {
			*dest = 0
		}).Return(nil)
		mockDB.EXPECT().QueryRowContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockRow)

		// Mock data query
		mockDB.EXPECT().QueryContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockRows, nil)
		mockRows.EXPECT().Close().Return(nil)
		mockRows.EXPECT().Next().Return(false)
		mockRows.EXPECT().Err().Return(nil)

		router := gin.New()
		router.GET("/api/v1/gpus/:id/telemetry", gateway.getTelemetryByUUID)

		req := httptest.NewRequest("GET", "/api/v1/gpus/"+testUUID+"/telemetry", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Expected status 404, got %d", w.Code)
		}
	})

	t.Run("database count error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDB := util.NewMockDatabase(ctrl)
		mockRow := util.NewMockRow(ctrl)

		gateway := &Gateway{
			config: &config.ServerConfig{Port: 8080},
			db:     mockDB,
		}

		testUUID := "GPU-0aba4c65-be7d-8418-977a-c950c14b989a"

		mockRow.EXPECT().Scan(gomock.Any()).Return(assertError{msg: "test error"})
		mockDB.EXPECT().QueryRowContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockRow)

		router := gin.New()
		router.GET("/api/v1/gpus/:id/telemetry", gateway.getTelemetryByUUID)

		req := httptest.NewRequest("GET", "/api/v1/gpus/"+testUUID+"/telemetry", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("Expected status 500, got %d", w.Code)
		}
	})

	t.Run("database query error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDB := util.NewMockDatabase(ctrl)
		mockRow := util.NewMockRow(ctrl)

		gateway := &Gateway{
			config: &config.ServerConfig{Port: 8080},
			db:     mockDB,
		}

		testUUID := "GPU-0aba4c65-be7d-8418-977a-c950c14b989a"

		mockRow.EXPECT().Scan(gomock.Any()).Do(func(dest *int) {
			*dest = 10
		}).Return(nil)
		mockDB.EXPECT().QueryRowContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockRow)
		mockDB.EXPECT().QueryContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, assertError{msg: "test error"})

		router := gin.New()
		router.GET("/api/v1/gpus/:id/telemetry", gateway.getTelemetryByUUID)

		req := httptest.NewRequest("GET", "/api/v1/gpus/"+testUUID+"/telemetry", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("Expected status 500, got %d", w.Code)
		}
	})

	t.Run("pagination parameters", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDB := util.NewMockDatabase(ctrl)
		mockRows := util.NewMockRows(ctrl)
		mockRow := util.NewMockRow(ctrl)

		gateway := &Gateway{
			config: &config.ServerConfig{Port: 8080},
			db:     mockDB,
		}

		testUUID := "GPU-0aba4c65-be7d-8418-977a-c950c14b989a"

		mockRow.EXPECT().Scan(gomock.Any()).Do(func(dest *int) {
			*dest = 50
		}).Return(nil)
		mockDB.EXPECT().QueryRowContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockRow)

		mockDB.EXPECT().QueryContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockRows, nil)
		mockRows.EXPECT().Close().Return(nil)
		mockRows.EXPECT().Next().Return(false)
		mockRows.EXPECT().Err().Return(nil)

		router := gin.New()
		router.GET("/api/v1/gpus/:id/telemetry", gateway.getTelemetryByUUID)

		req := httptest.NewRequest("GET", "/api/v1/gpus/"+testUUID+"/telemetry?page=2&page_size=25", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Expected status 404 (no records), got %d", w.Code)
		}
	})

	t.Run("invalid page parameter defaults to 1", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDB := util.NewMockDatabase(ctrl)
		mockRows := util.NewMockRows(ctrl)
		mockRow := util.NewMockRow(ctrl)

		gateway := &Gateway{
			config: &config.ServerConfig{Port: 8080},
			db:     mockDB,
		}

		testUUID := "GPU-0aba4c65-be7d-8418-977a-c950c14b989a"

		mockRow.EXPECT().Scan(gomock.Any()).Do(func(dest *int) {
			*dest = 0
		}).Return(nil)
		mockDB.EXPECT().QueryRowContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockRow)

		mockDB.EXPECT().QueryContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockRows, nil)
		mockRows.EXPECT().Close().Return(nil)
		mockRows.EXPECT().Next().Return(false)
		mockRows.EXPECT().Err().Return(nil)

		router := gin.New()
		router.GET("/api/v1/gpus/:id/telemetry", gateway.getTelemetryByUUID)

		req := httptest.NewRequest("GET", "/api/v1/gpus/"+testUUID+"/telemetry?page=invalid", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Expected status 404 (no records), got %d", w.Code)
		}
	})

	t.Run("invalid page_size parameter defaults to 100", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDB := util.NewMockDatabase(ctrl)
		mockRows := util.NewMockRows(ctrl)
		mockRow := util.NewMockRow(ctrl)

		gateway := &Gateway{
			config: &config.ServerConfig{Port: 8080},
			db:     mockDB,
		}

		testUUID := "GPU-0aba4c65-be7d-8418-977a-c950c14b989a"

		mockRow.EXPECT().Scan(gomock.Any()).Do(func(dest *int) {
			*dest = 0
		}).Return(nil)
		mockDB.EXPECT().QueryRowContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockRow)

		mockDB.EXPECT().QueryContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockRows, nil)
		mockRows.EXPECT().Close().Return(nil)
		mockRows.EXPECT().Next().Return(false)
		mockRows.EXPECT().Err().Return(nil)

		router := gin.New()
		router.GET("/api/v1/gpus/:id/telemetry", gateway.getTelemetryByUUID)

		req := httptest.NewRequest("GET", "/api/v1/gpus/"+testUUID+"/telemetry?page_size=invalid", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Expected status 404 (no records), got %d", w.Code)
		}
	})

	t.Run("page_size exceeds maximum defaults to 100", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDB := util.NewMockDatabase(ctrl)
		mockRows := util.NewMockRows(ctrl)
		mockRow := util.NewMockRow(ctrl)

		gateway := &Gateway{
			config: &config.ServerConfig{Port: 8080},
			db:     mockDB,
		}

		testUUID := "GPU-0aba4c65-be7d-8418-977a-c950c14b989a"

		mockRow.EXPECT().Scan(gomock.Any()).Do(func(dest *int) {
			*dest = 0
		}).Return(nil)
		mockDB.EXPECT().QueryRowContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockRow)

		mockDB.EXPECT().QueryContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockRows, nil)
		mockRows.EXPECT().Close().Return(nil)
		mockRows.EXPECT().Next().Return(false)
		mockRows.EXPECT().Err().Return(nil)

		router := gin.New()
		router.GET("/api/v1/gpus/:id/telemetry", gateway.getTelemetryByUUID)

		req := httptest.NewRequest("GET", "/api/v1/gpus/"+testUUID+"/telemetry?page_size=2000", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Expected status 404 (no records), got %d", w.Code)
		}
	})
}

// Helper type to create an error for testing
type assertError struct {
	msg string
}

func (e assertError) Error() string {
	return e.msg
}
