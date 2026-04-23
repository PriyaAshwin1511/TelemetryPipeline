// Queue service provides a message queue for telemetry data with HTTP endpoints.
package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"telemetry-collector/services/config"
	"telemetry-collector/services/logger"
	"telemetry-collector/services/queue"
	"telemetry-collector/services/util"

	"github.com/gin-gonic/gin"
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

	// Create queue
	q, err := queue.NewQueue(&cfg.Queue)
	if err != nil {
		logger.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	// Start HTTP server
	router := gin.Default()

	// Health check
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, util.HealthResponse{Status: "healthy"})
	})

	// Produce endpoint
	router.POST("/produce", produceHandler(q))

	// Consume endpoint
	router.POST("/consume", consumeHandler(q))
	router.GET("/consume", consumeHandlerGet(q))

	addr := fmt.Sprintf(":%d", cfg.Server.Port)
	go func() {
		logger.WithFields(map[string]interface{}{
			"port": cfg.Server.Port,
		}).Info("Queue service starting")
		if err := router.Run(addr); err != nil {
			logger.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("Queue service shutting down")
}

func produceHandler(q *queue.Queue) gin.HandlerFunc {
	return func(c *gin.Context) {
		var request util.ProduceRequest
		if err := c.ShouldBindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		responses, err := q.Produce(request.Records)
		if err != nil {
			logger.WithFields(map[string]interface{}{
				"error": err,
			}).Error("Failed to produce records")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to produce records"})
			return
		}

		c.JSON(http.StatusOK, gin.H{"responses": responses})
	}
}

func consumeHandler(q *queue.Queue) gin.HandlerFunc {
	return func(c *gin.Context) {
		var request util.ConsumeRequest
		if err := c.ShouldBindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		records, nextOffset, hasMore, err := q.Consume(request.PartitionID, request.ConsumerGroupID, request.Offset, request.MaxRecords)
		if err != nil {
			logger.WithFields(map[string]interface{}{
				"error": err,
			}).Error("Failed to consume records")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to consume records"})
			return
		}

		response := util.ConsumeResponse{
			Records:         records,
			NextOffset:      nextOffset,
			HasMore:         hasMore,
			ConsumerGroupID: request.ConsumerGroupID,
		}

		c.JSON(http.StatusOK, response)
	}
}

func consumeHandlerGet(q *queue.Queue) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Parse query parameters
		countStr := c.DefaultQuery("count", "100")
		consumerGroupID := c.DefaultQuery("consumer_group_id", "default-collector")
		maxRecords := 100
		if _, err := fmt.Sscanf(countStr, "%d", &maxRecords); err != nil {
			maxRecords = 100
		}

		// For HTTP-based consumption, consume from all partitions
		var allRecords []util.TelemetryRecord
		for partitionID := 0; partitionID < q.GetPartitionCount(); partitionID++ {
			// Use offset -1 to auto-resume from last stored offset for this consumer group
			records, _, _, err := q.Consume(partitionID, consumerGroupID, -1, maxRecords)
			if err != nil {
				logger.WithFields(map[string]interface{}{
					"partition_id":      partitionID,
					"consumer_group_id": consumerGroupID,
					"error":             err,
				}).Error("Failed to consume from partition")
				continue
			}
			allRecords = append(allRecords, records...)
		}

		c.JSON(http.StatusOK, gin.H{
			"records":           allRecords,
			"consumer_group_id": consumerGroupID,
		})
	}
}
