// Package queue provides a message queue service for telemetry data.
// It implements a partitioned queue with persistent storage, consumer group support,
// and an in-memory cache for recent records.
package queue

import (
	"fmt"
	"sync"

	"telemetry-collector/services/config"
	"telemetry-collector/services/logger"
	"telemetry-collector/services/util"
)

// Queue represents the message queue service
type Queue struct {
	config     *config.QueueConfig
	partitions []*Partition
	mu         sync.RWMutex
}

// NewQueue creates a new queue service
func NewQueue(cfg *config.QueueConfig) (*Queue, error) {
	q := &Queue{
		config: cfg,
	}

	partitions := make([]*Partition, cfg.NumPartitions)
	for i := 0; i < cfg.NumPartitions; i++ {
		dataDir := fmt.Sprintf("%s/partition-%d", cfg.DataDir, i)
		p, err := NewPartition(i, dataDir, cfg.SegmentSize, cfg.CacheDuration, cfg.CacheCleanupInterval)
		if err != nil {
			return nil, fmt.Errorf("failed to create partition %d: %w", i, err)
		}
		partitions[i] = p
	}

	q.partitions = partitions

	logger.WithFields(map[string]interface{}{
		"num_partitions": cfg.NumPartitions,
		"data_dir":       cfg.DataDir,
	}).Info("Queue service initialized")

	return q, nil
}

// Produce writes records to the appropriate partition based on UUID hash
func (q *Queue) Produce(records []util.TelemetryRecord) ([]util.ProduceResponse, error) {
	responses := make([]util.ProduceResponse, len(records))

	// Group records by partition
	partitionRecords := make(map[int][]util.TelemetryRecord)
	for _, record := range records {
		partitionID := util.GetPartitionID(record.UUID, q.config.NumPartitions)
		partitionRecords[partitionID] = append(partitionRecords[partitionID], record)
	}

	// Produce to each partition
	responseIndex := 0
	for partitionID, records := range partitionRecords {
		q.mu.RLock()
		p, err := q.getPartition(partitionID)
		q.mu.RUnlock()

		if err != nil {
			logger.WithFields(map[string]interface{}{
				"partition_id": partitionID,
				"error":        err,
			}).Error("Failed to get partition")
			// Mark responses as failed for these records
			for i := 0; i < len(records); i++ {
				if responseIndex < len(responses) {
					responses[responseIndex] = util.ProduceResponse{
						Success: false,
						Message: "Failed to get partition",
					}
					responseIndex++
				}
			}
			continue
		}

		offset, err := p.Produce(records)
		if err != nil {
			logger.WithFields(map[string]interface{}{
				"partition_id": partitionID,
				"error":        err,
			}).Error("Failed to produce records")
			// Mark responses as failed for these records
			for i := 0; i < len(records); i++ {
				if responseIndex < len(responses) {
					responses[responseIndex] = util.ProduceResponse{
						Success: false,
						Message: "Failed to produce records",
					}
					responseIndex++
				}
			}
			continue
		}

		// Update responses
		for i := 0; i < len(records); i++ {
			if responseIndex < len(responses) {
				responses[responseIndex] = util.ProduceResponse{
					Success: true,
					Offset:  offset,
				}
				responseIndex++
			}
		}
	}

	return responses, nil
}

// Consume reads records from a specific partition
func (q *Queue) Consume(partitionID int, consumerGroupID string, offset int64, maxRecords int) ([]util.TelemetryRecord, int64, bool, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	p, err := q.getPartition(partitionID)
	if err != nil {
		return nil, offset, false, fmt.Errorf("failed to get partition: %w", err)
	}

	return p.Consume(consumerGroupID, offset, maxRecords)
}

// getPartition returns the partition with the given ID
func (q *Queue) getPartition(id int) (*Partition, error) {
	if id < 0 || id >= len(q.partitions) {
		return nil, fmt.Errorf("invalid partition ID: %d", id)
	}
	return q.partitions[id], nil
}

// GetPartitionCount returns the number of partitions
func (q *Queue) GetPartitionCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.partitions)
}

// Close closes the queue and releases resources
func (q *Queue) Close() error {
	logger.Info("Closing queue service")
	q.mu.Lock()
	defer q.mu.Unlock()

	// Close all partitions
	for _, p := range q.partitions {
		if err := p.Close(); err != nil {
			logger.WithFields(map[string]interface{}{
				"partition_id": p.GetID(),
				"error":        err,
			}).Error("Failed to close partition")
		}
	}

	return nil
}
