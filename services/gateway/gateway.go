// Package gateway provides an HTTP API server for querying telemetry data.
// It implements REST endpoints with Swagger documentation for querying GPU telemetry records.
package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"telemetry-collector/services/config"
	"telemetry-collector/services/logger"
	"telemetry-collector/services/util"

	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

// @title Telemetry Collector API
// @version 1.0
// @description REST API for querying GPU telemetry data
// @host localhost:8080
// @BasePath /
// @schemes http
// Gateway is an HTTP server that provides API endpoints for querying telemetry data
type Gateway struct {
	config *config.ServerConfig
	db     util.Database
}

// NewGateway creates a new API gateway
func NewGateway(cfg *config.ServerConfig, dbConfig *config.DatabaseConfig) (*Gateway, error) {
	db, err := util.ConnectToDB(dbConfig)
	if err != nil {
		return nil, err
	}

	return &Gateway{
		config: cfg,
		db:     db,
	}, nil
}

// Start starts the HTTP server
func (g *Gateway) Start() error {
	router := gin.Default()

	// Health check
	router.GET("/health", g.healthCheck)

	// Swagger documentation
	url := ginSwagger.URL("/swagger/doc.json")
	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler, url))
	router.GET("/", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/swagger/index.html")
	})

	// API routes
	v1 := router.Group("/api/v1")
	{
		v1.GET("/gpus", g.listUUIDs)
		v1.GET("/gpus/:id/telemetry", g.getTelemetryByUUID)
	}

	addr := ":" + strconv.Itoa(g.config.Port)
	logger.WithFields(map[string]interface{}{
		"port": g.config.Port,
	}).Info("Starting API gateway")

	return router.Run(addr)
}

// healthCheck returns the health status
func (g *Gateway) healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, util.HealthResponse{Status: "healthy"})
}

// listUUIDs returns all unique UUIDs from the telemetry data
// @Summary List all unique GPU UUIDs
// @Description Returns a list of all unique GPU UUIDs in the telemetry data
// @Tags gpus
// @Accept json
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Failure 500 {object} map[string]interface{}
// @Router /api/v1/gpus [get]
func (g *Gateway) listUUIDs(c *gin.Context) {
	query := `SELECT DISTINCT uuid FROM telemetry ORDER BY uuid`

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows, err := g.db.QueryContext(ctx, query)
	if err != nil {
		logger.WithFields(map[string]interface{}{
			"error": err,
		}).Error("Failed to query UUIDs")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to query UUIDs"})
		return
	}
	defer rows.Close()

	var uuids []string
	for rows.Next() {
		var uuid string
		if err := rows.Scan(&uuid); err != nil {
			logger.WithFields(map[string]interface{}{
				"error": err,
			}).Error("Failed to scan UUID")
			continue
		}
		uuids = append(uuids, uuid)
	}

	if err := rows.Err(); err != nil {
		logger.WithFields(map[string]interface{}{
			"error": err,
		}).Error("Error iterating UUIDs")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Error iterating UUIDs"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"uuids": uuids})
}

// getTelemetryByUUID returns telemetry data for a specific UUID
// @Summary Get telemetry data for a specific GPU UUID
// @Description Returns telemetry records for a specific GPU UUID, optionally filtered by time range and pagination
// @Tags gpus
// @Accept json
// @Produce json
// @Param id path string true "GPU UUID"
// @Param start_time query string false "Start time in RFC3339 format"
// @Param end_time query string false "End time in RFC3339 format"
// @Param page query int false "Page number (default: 1)" minimum(1) default(1)
// @Param page_size query int false "Number of records per page (default: 100, max: 1000)" minimum(1) maximum(1000) default(100)
// @Success 200 {object} map[string]interface{}
// @Failure 400 {object} map[string]interface{}
// @Failure 404 {object} map[string]interface{}
// @Failure 500 {object} map[string]interface{}
// @Router /api/v1/gpus/{id}/telemetry [get]
func (g *Gateway) getTelemetryByUUID(c *gin.Context) {
	uuid := c.Param("id")

	// Validate UUID
	if uuid == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "UUID is required"})
		return
	}

	// Parse optional time range parameters
	startTimeStr := c.Query("start_time")
	endTimeStr := c.Query("end_time")

	// Parse pagination parameters
	pageStr := c.DefaultQuery("page", "1")
	pageSizeStr := c.DefaultQuery("page_size", "100")

	page, err := strconv.Atoi(pageStr)
	if err != nil || page < 1 {
		page = 1
	}

	pageSize, err := strconv.Atoi(pageSizeStr)
	if err != nil || pageSize < 1 || pageSize > 1000 {
		pageSize = 100
	}

	var startTime, endTime time.Time

	if startTimeStr != "" {
		var err error
		startTime, err = time.Parse(time.RFC3339, startTimeStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid start_time format, use RFC3339"})
			return
		}
	}

	if endTimeStr != "" {
		var err error
		endTime, err = time.Parse(time.RFC3339, endTimeStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid end_time format, use RFC3339"})
			return
		}
	}

	// Validate time range
	if !startTime.IsZero() && !endTime.IsZero() && startTime.After(endTime) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "start_time must be before or equal to end_time"})
		return
	}

	query, args := g.buildTelemetryQuery(uuid, startTime, endTime, pageSize, (page-1)*pageSize)
	countQuery, countArgs := g.buildCountQuery(uuid, startTime, endTime)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var total int
	err = g.db.QueryRowContext(ctx, countQuery, countArgs...).Scan(&total)
	if err != nil {
		logger.WithFields(map[string]interface{}{
			"error": err,
			"uuid":  uuid,
		}).Error("Failed to count records")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to count records"})
		return
	}

	totalPages := (total + pageSize - 1) / pageSize
	if totalPages == 0 {
		totalPages = 1
	}

	rows, err := g.db.QueryContext(ctx, query, args...)
	if err != nil {
		logger.WithFields(map[string]interface{}{
			"error": err,
			"uuid":  uuid,
		}).Error("Failed to query telemetry")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to query telemetry"})
		return
	}
	defer rows.Close()

	var records []util.TelemetryRecord
	for rows.Next() {
		var record util.TelemetryRecord
		var labelsJSON []byte

		err := rows.Scan(
			&record.Timestamp,
			&record.MetricName,
			&record.GPUID,
			&record.Device,
			&record.UUID,
			&record.ModelName,
			&record.Hostname,
			&record.Container,
			&record.Pod,
			&record.Namespace,
			&record.Value,
			&labelsJSON,
		)
		if err != nil {
			logger.WithFields(map[string]interface{}{
				"error": err,
			}).Error("Failed to scan record")
			continue
		}

		if len(labelsJSON) > 0 {
			if err := json.Unmarshal(labelsJSON, &record.Labels); err != nil {
				logger.WithFields(map[string]interface{}{
					"error": err,
				}).Warn("Failed to unmarshal labels")
				record.Labels = make(map[string]interface{})
			}
		}

		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		logger.WithFields(map[string]interface{}{
			"error": err,
		}).Error("Error iterating records")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Error iterating records"})
		return
	}

	// Return 404 if no records found
	if len(records) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "No telemetry records found for UUID"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"telemetry": records,
		"pagination": gin.H{
			"page":        page,
			"page_size":   pageSize,
			"total":       total,
			"total_pages": totalPages,
		},
	})
}

// buildTelemetryQuery builds the telemetry query with optional time filters and pagination
func (g *Gateway) buildTelemetryQuery(uuid string, startTime, endTime time.Time, pageSize, offset int) (string, []interface{}) {
	query := `SELECT timestamp, metric_name, gpu_id, device, uuid, model_name, hostname, container, pod, namespace, value, labels
			  FROM telemetry WHERE uuid = $1`
	args := []interface{}{uuid}
	argPos := 2

	if !startTime.IsZero() {
		query += ` AND timestamp >= $` + strconv.Itoa(argPos)
		args = append(args, startTime)
		argPos++
	}

	if !endTime.IsZero() {
		query += ` AND timestamp <= $` + strconv.Itoa(argPos)
		args = append(args, endTime)
		argPos++
	}

	query += ` ORDER BY timestamp ASC LIMIT $` + strconv.Itoa(argPos) + ` OFFSET $` + strconv.Itoa(argPos+1)
	args = append(args, pageSize, offset)

	return query, args
}

// buildCountQuery builds the count query with optional time filters
func (g *Gateway) buildCountQuery(uuid string, startTime, endTime time.Time) (string, []interface{}) {
	query := `SELECT COUNT(*) FROM telemetry WHERE uuid = $1`
	args := []interface{}{uuid}
	argPos := 2

	if !startTime.IsZero() {
		query += ` AND timestamp >= $` + strconv.Itoa(argPos)
		args = append(args, startTime)
		argPos++
	}

	if !endTime.IsZero() {
		query += ` AND timestamp <= $` + strconv.Itoa(argPos)
		args = append(args, endTime)
	}

	return query, args
}
