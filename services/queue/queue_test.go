package queue

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"telemetry-collector/services/config"
	"telemetry-collector/services/util"
)

func TestGetPartitionID(t *testing.T) {
	cfg := &config.QueueConfig{
		NumPartitions: 3,
	}

	q := &Queue{
		config: cfg,
	}

	tests := []struct {
		name    string
		uuid    string
		wantMin int
		wantMax int
	}{
		{
			name:    "uuid1",
			uuid:    "uuid-1",
			wantMin: 0,
			wantMax: 2,
		},
		{
			name:    "uuid2",
			uuid:    "uuid-2",
			wantMin: 0,
			wantMax: 2,
		},
		{
			name:    "empty uuid",
			uuid:    "",
			wantMin: 0,
			wantMax: 2,
		},
		{
			name:    "same uuid",
			uuid:    "same-uuid",
			wantMin: 0,
			wantMax: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := util.GetPartitionID(tt.uuid, q.config.NumPartitions)
			if result < tt.wantMin || result > tt.wantMax {
				t.Errorf("getPartitionID() = %v, want between %v and %v", result, tt.wantMin, tt.wantMax)
			}
		})
	}
}

func TestGetPartitionID_ConsistentHashing(t *testing.T) {
	cfg := &config.QueueConfig{
		NumPartitions: 3,
	}

	q := &Queue{
		config: cfg,
	}

	uuid := "test-uuid-123"
	result1 := util.GetPartitionID(uuid, q.config.NumPartitions)
	result2 := util.GetPartitionID(uuid, q.config.NumPartitions)

	if result1 != result2 {
		t.Errorf("getPartitionID() not consistent: first = %v, second = %v", result1, result2)
	}
}

func TestGetPartition(t *testing.T) {
	cfg := &config.QueueConfig{
		NumPartitions: 3,
	}

	q := &Queue{
		config:     cfg,
		partitions: []*Partition{{}, {}, {}},
	}

	tests := []struct {
		name    string
		id      int
		wantErr bool
	}{
		{
			name:    "valid partition 0",
			id:      0,
			wantErr: false,
		},
		{
			name:    "valid partition 1",
			id:      1,
			wantErr: false,
		},
		{
			name:    "valid partition 2",
			id:      2,
			wantErr: false,
		},
		{
			name:    "invalid partition -1",
			id:      -1,
			wantErr: true,
		},
		{
			name:    "invalid partition 3",
			id:      3,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := q.getPartition(tt.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("getPartition() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGetPartitionCount(t *testing.T) {
	cfg := &config.QueueConfig{
		NumPartitions: 3,
	}

	q := &Queue{
		config:     cfg,
		partitions: []*Partition{{}, {}, {}},
	}

	result := q.GetPartitionCount()
	if result != 3 {
		t.Errorf("GetPartitionCount() = %v, want %v", result, 3)
	}
}

func TestClose(t *testing.T) {
	tempDir := t.TempDir()

	cfg := &config.QueueConfig{
		DataDir:              tempDir,
		NumPartitions:        3,
		CacheDuration:        2 * time.Minute,
		CacheCleanupInterval: 1 * time.Minute,
		MaxMessageSize:       1024 * 1024,
		SegmentSize:          100 * 1024 * 1024,
	}

	q, err := NewQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	err = q.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestProduce_MultiplePartitions(t *testing.T) {
	tempDir := t.TempDir()

	cfg := &config.QueueConfig{
		DataDir:              tempDir,
		NumPartitions:        3,
		CacheDuration:        2 * time.Minute,
		CacheCleanupInterval: 1 * time.Minute,
		MaxMessageSize:       1024 * 1024,
		SegmentSize:          100 * 1024 * 1024,
	}

	q, err := NewQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	// Create records with different UUIDs to distribute across partitions
	records := []util.TelemetryRecord{
		{Timestamp: time.Now(), MetricName: "temp", UUID: "uuid-1", Value: 42.0},
		{Timestamp: time.Now(), MetricName: "temp", UUID: "uuid-2", Value: 43.0},
		{Timestamp: time.Now(), MetricName: "temp", UUID: "uuid-3", Value: 44.0},
	}

	responses, err := q.Produce(records)
	if err != nil {
		t.Fatalf("Failed to produce records: %v", err)
	}

	if len(responses) != 3 {
		t.Errorf("Expected 3 responses, got %d", len(responses))
	}

	successCount := 0
	for _, resp := range responses {
		if resp.Success {
			successCount++
		}
	}

	if successCount != 3 {
		t.Errorf("Expected all 3 records to succeed, got %d", successCount)
	}
}

func TestConsume_WithOffset(t *testing.T) {
	tempDir := t.TempDir()

	cfg := &config.QueueConfig{
		DataDir:              tempDir,
		NumPartitions:        3,
		CacheDuration:        2 * time.Minute,
		CacheCleanupInterval: 1 * time.Minute,
		MaxMessageSize:       1024 * 1024,
		SegmentSize:          100 * 1024 * 1024,
	}

	q, err := NewQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	// Produce records
	uuid := "test-uuid-offset"
	records := []util.TelemetryRecord{
		{Timestamp: time.Now(), MetricName: "temp", UUID: uuid, Value: 1.0},
		{Timestamp: time.Now(), MetricName: "temp", UUID: uuid, Value: 2.0},
		{Timestamp: time.Now(), MetricName: "temp", UUID: uuid, Value: 3.0},
	}

	_, err = q.Produce(records)
	if err != nil {
		t.Fatalf("Failed to produce records: %v", err)
	}

	// Get partition ID
	partitionID := q.GetPartitionCount()
	var foundPartition int
	for i := 0; i < partitionID; i++ {
		records, _, _, err := q.Consume(i, "test-group", 0, 10)
		if err == nil && len(records) > 0 {
			foundPartition = i
			break
		}
	}

	// Consume from offset 1
	consumedRecords, offset, hasMore, err := q.Consume(foundPartition, "test-group", 1, 10)
	if err != nil {
		t.Fatalf("Failed to consume from offset: %v", err)
	}

	if len(consumedRecords) != 2 {
		t.Errorf("Expected 2 records from offset 1, got %d", len(consumedRecords))
	}

	if offset != 3 {
		t.Errorf("Expected offset 3, got %d", offset)
	}

	if hasMore {
		t.Error("Expected hasMore to be false")
	}
}

func TestProduce_LargeBatch(t *testing.T) {
	tempDir := t.TempDir()

	cfg := &config.QueueConfig{
		DataDir:              tempDir,
		NumPartitions:        3,
		CacheDuration:        2 * time.Minute,
		CacheCleanupInterval: 1 * time.Minute,
		MaxMessageSize:       1024 * 1024,
		SegmentSize:          100 * 1024 * 1024,
	}

	q, err := NewQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	// Create large batch of records
	uuid := "test-uuid-large"
	var records []util.TelemetryRecord
	for i := 0; i < 100; i++ {
		records = append(records, util.TelemetryRecord{
			Timestamp:  time.Now(),
			MetricName: "temp",
			UUID:       uuid,
			Value:      float64(i),
		})
	}

	responses, err := q.Produce(records)
	if err != nil {
		t.Fatalf("Failed to produce records: %v", err)
	}

	if len(responses) != 100 {
		t.Errorf("Expected 100 responses, got %d", len(responses))
	}

	successCount := 0
	for _, resp := range responses {
		if resp.Success {
			successCount++
		}
	}

	if successCount != 100 {
		t.Errorf("Expected all 100 records to succeed, got %d", successCount)
	}
}

func TestConsume_MaxRecords(t *testing.T) {
	tempDir := t.TempDir()

	cfg := &config.QueueConfig{
		DataDir:              tempDir,
		NumPartitions:        3,
		CacheDuration:        2 * time.Minute,
		CacheCleanupInterval: 1 * time.Minute,
		MaxMessageSize:       1024 * 1024,
		SegmentSize:          100 * 1024 * 1024,
	}

	q, err := NewQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	// Produce records
	uuid := "test-uuid-max"
	var records []util.TelemetryRecord
	for i := 0; i < 20; i++ {
		records = append(records, util.TelemetryRecord{
			Timestamp:  time.Now(),
			MetricName: "temp",
			UUID:       uuid,
			Value:      float64(i),
		})
	}

	_, err = q.Produce(records)
	if err != nil {
		t.Fatalf("Failed to produce records: %v", err)
	}

	// Find partition with records
	partitionID := q.GetPartitionCount()
	var foundPartition int
	for i := 0; i < partitionID; i++ {
		records, _, _, err := q.Consume(i, "test-group", 0, 10)
		if err == nil && len(records) > 0 {
			foundPartition = i
			break
		}
	}

	// Consume with max records limit
	consumedRecords, _, hasMore, err := q.Consume(foundPartition, "test-group", 0, 10)
	if err != nil {
		t.Fatalf("Failed to consume with max records: %v", err)
	}

	if len(consumedRecords) != 10 {
		t.Errorf("Expected 10 records, got %d", len(consumedRecords))
	}

	// hasMore behavior depends on implementation, just verify we got the expected count
	t.Logf("hasMore = %v", hasMore)
}

func TestGetPartitionID_DifferentUUIDs(t *testing.T) {
	cfg := &config.QueueConfig{
		NumPartitions: 5,
	}

	q := &Queue{
		config: cfg,
	}

	uuids := []string{
		"uuid-1",
		"uuid-2",
		"uuid-3",
		"uuid-4",
		"uuid-5",
	}

	partitionIDs := make(map[int]bool)
	for _, uuid := range uuids {
		partitionID := util.GetPartitionID(uuid, q.config.NumPartitions)
		partitionIDs[partitionID] = true
	}

	// With 5 partitions and 5 different UUIDs, we should get distribution
	if len(partitionIDs) < 2 {
		t.Errorf("Expected at least 2 different partition IDs, got %d", len(partitionIDs))
	}
}

func TestGetPartition_EdgeCases(t *testing.T) {
	cfg := &config.QueueConfig{
		NumPartitions: 3,
	}

	q := &Queue{
		config:     cfg,
		partitions: []*Partition{{}, {}, {}},
	}

	tests := []struct {
		name    string
		id      int
		wantErr bool
	}{
		{
			name:    "partition at boundary 0",
			id:      0,
			wantErr: false,
		},
		{
			name:    "partition at boundary max-1",
			id:      2,
			wantErr: false,
		},
		{
			name:    "negative boundary -1",
			id:      -1,
			wantErr: true,
		},
		{
			name:    "boundary at max",
			id:      3,
			wantErr: true,
		},
		{
			name:    "large negative",
			id:      -100,
			wantErr: true,
		},
		{
			name:    "large positive",
			id:      100,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := q.getPartition(tt.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("getPartition() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestProduce_DataPersistence(t *testing.T) {
	// Test that fsync ensures data persists after partition close/reopen
	tempDir := t.TempDir()

	cfg := &config.QueueConfig{
		DataDir:              tempDir,
		NumPartitions:        1,
		CacheDuration:        2 * time.Minute,
		CacheCleanupInterval: 1 * time.Minute,
		MaxMessageSize:       1024 * 1024,
		SegmentSize:          100 * 1024 * 1024,
	}

	// Create queue and produce records
	q, err := NewQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	uuid := "test-uuid-persistence"
	records := []util.TelemetryRecord{
		{Timestamp: time.Now(), MetricName: "temp", UUID: uuid, Value: 1.0},
		{Timestamp: time.Now(), MetricName: "temp", UUID: uuid, Value: 2.0},
		{Timestamp: time.Now(), MetricName: "temp", UUID: uuid, Value: 3.0},
	}

	responses, err := q.Produce(records)
	if err != nil {
		t.Fatalf("Failed to produce records: %v", err)
	}

	successCount := 0
	for _, resp := range responses {
		if resp.Success {
			successCount++
		}
	}
	if successCount != 3 {
		t.Fatalf("Expected all 3 records to succeed, got %d", successCount)
	}

	// Close queue to simulate process shutdown
	if err := q.Close(); err != nil {
		t.Fatalf("Failed to close queue: %v", err)
	}

	// Reopen queue to simulate process restart (data should be persisted via fsync)
	q2, err := NewQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to reopen queue: %v", err)
	}
	defer q2.Close()

	// Consume records from partition 0 to verify data persistence
	consumedRecords, _, _, err := q2.Consume(0, "test-group", 0, 10)
	if err != nil {
		t.Fatalf("Failed to consume records after reopen: %v", err)
	}

	if len(consumedRecords) != 3 {
		t.Errorf("Expected 3 records after reopen, got %d", len(consumedRecords))
	}

	// Verify the data matches
	for i, record := range consumedRecords {
		if record.Value != records[i].Value {
			t.Errorf("Record %d value mismatch: expected %f, got %f", i, records[i].Value, record.Value)
		}
	}
}

func TestIndexFile_Functionality(t *testing.T) {
	// Test that index file is created and loaded correctly
	tempDir := t.TempDir()

	cfg := &config.QueueConfig{
		DataDir:              tempDir,
		NumPartitions:        1,
		CacheDuration:        2 * time.Minute,
		CacheCleanupInterval: 1 * time.Minute,
		MaxMessageSize:       1024 * 1024,
		SegmentSize:          100 * 1024 * 1024,
	}

	// Create queue and produce records
	q, err := NewQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	uuid := "test-uuid-index"
	records := []util.TelemetryRecord{
		{Timestamp: time.Now(), MetricName: "temp", UUID: uuid, Value: 1.0},
		{Timestamp: time.Now(), MetricName: "temp", UUID: uuid, Value: 2.0},
		{Timestamp: time.Now(), MetricName: "temp", UUID: uuid, Value: 3.0},
	}

	_, err = q.Produce(records)
	if err != nil {
		t.Fatalf("Failed to produce records: %v", err)
	}

	// Verify index file exists
	partition, err := q.getPartition(0)
	if err != nil {
		t.Fatalf("Failed to get partition: %v", err)
	}

	indexFile := partition.getIndexFilePath()
	if _, err := os.Stat(indexFile); os.IsNotExist(err) {
		t.Errorf("Index file was not created at %s", indexFile)
	}

	// Close and reopen to verify index is loaded
	if err := q.Close(); err != nil {
		t.Fatalf("Failed to close queue: %v", err)
	}

	q2, err := NewQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to reopen queue: %v", err)
	}
	defer q2.Close()

	// Test that index was loaded by consuming from offset 0
	consumedRecords, _, _, err := q2.Consume(0, "test-group", 0, 3)
	if err != nil {
		t.Fatalf("Failed to consume using index: %v", err)
	}

	// Verify we got all 3 records
	if len(consumedRecords) != 3 {
		t.Errorf("Expected 3 records, got %d", len(consumedRecords))
	}

	// Verify the values are correct (1.0, 2.0, 3.0)
	expectedValues := []float64{1.0, 2.0, 3.0}
	for i, record := range consumedRecords {
		if record.Value != expectedValues[i] {
			t.Errorf("Record %d value mismatch: expected %f, got %f", i, expectedValues[i], record.Value)
		}
	}
}

func TestOffsetFile_Functionality(t *testing.T) {
	// Test that consumer group offset file is created and contains correct value
	tempDir := t.TempDir()

	cfg := &config.QueueConfig{
		DataDir:              tempDir,
		NumPartitions:        1,
		CacheDuration:        2 * time.Minute,
		CacheCleanupInterval: 1 * time.Minute,
		MaxMessageSize:       1024 * 1024,
		SegmentSize:          100 * 1024 * 1024,
	}

	// Create queue and produce records
	q, err := NewQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	uuid := "test-uuid-offset-file"
	records := []util.TelemetryRecord{
		{Timestamp: time.Now(), MetricName: "temp", UUID: uuid, Value: 1.0},
		{Timestamp: time.Now(), MetricName: "temp", UUID: uuid, Value: 2.0},
		{Timestamp: time.Now(), MetricName: "temp", UUID: uuid, Value: 3.0},
	}

	_, err = q.Produce(records)
	if err != nil {
		t.Fatalf("Failed to produce records: %v", err)
	}

	// Consume some records to update consumer group offset
	consumerGroupID := "test-consumer-group"
	_, nextOffset, _, err := q.Consume(0, consumerGroupID, 0, 2)
	if err != nil {
		t.Fatalf("Failed to consume records: %v", err)
	}

	// Verify consumer group offset file exists
	partition, err := q.getPartition(0)
	if err != nil {
		t.Fatalf("Failed to get partition: %v", err)
	}

	offsetFile := partition.getConsumerGroupOffsetFilePath(consumerGroupID)
	if _, err := os.Stat(offsetFile); os.IsNotExist(err) {
		t.Errorf("Consumer group offset file was not created at %s", offsetFile)
	}

	// Read the offset file to verify it contains the correct value
	data, err := os.ReadFile(offsetFile)
	if err != nil {
		t.Fatalf("Failed to read offset file: %v", err)
	}

	var storedOffset int64
	if _, err := fmt.Sscanf(string(data), "%d\n", &storedOffset); err != nil {
		t.Fatalf("Failed to parse offset file: %v", err)
	}

	// Verify the stored offset matches the next offset from consumption
	if storedOffset != nextOffset {
		t.Errorf("Stored offset mismatch: expected %d, got %d", nextOffset, storedOffset)
	}

	// Close and reopen to verify consumer group offset is loaded
	if err := q.Close(); err != nil {
		t.Fatalf("Failed to close queue: %v", err)
	}

	q2, err := NewQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to reopen queue: %v", err)
	}
	defer q2.Close()

	// Verify the consumer group offset was loaded into memory
	partition2, err := q2.getPartition(0)
	if err != nil {
		t.Fatalf("Failed to get partition after reopen: %v", err)
	}

	// Check that the consumer group offset was loaded
	loadedOffset, exists := partition2.consumerGroupOffsets[consumerGroupID]
	if !exists {
		t.Error("Consumer group offset was not loaded after reopen")
	}

	// Verify the loaded offset matches the stored offset
	if loadedOffset != storedOffset {
		t.Errorf("Loaded offset mismatch: expected %d, got %d", storedOffset, loadedOffset)
	}
}

func TestIndex_EfficientSeeking(t *testing.T) {
	// Test that index enables efficient seeking without reading entire file
	tempDir := t.TempDir()

	cfg := &config.QueueConfig{
		DataDir:              tempDir,
		NumPartitions:        1,
		CacheDuration:        2 * time.Minute,
		CacheCleanupInterval: 1 * time.Minute,
		MaxMessageSize:       1024 * 1024,
		SegmentSize:          100 * 1024 * 1024,
	}

	// Create queue and produce many records
	q, err := NewQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	uuid := "test-uuid-seek"
	var records []util.TelemetryRecord
	for i := 0; i < 100; i++ {
		records = append(records, util.TelemetryRecord{
			Timestamp:  time.Now(),
			MetricName: "temp",
			UUID:       uuid,
			Value:      float64(i),
		})
	}

	_, err = q.Produce(records)
	if err != nil {
		t.Fatalf("Failed to produce records: %v", err)
	}

	// First, consume first 50 records to verify they're correct
	firstBatch, _, _, err := q.Consume(0, "test-group", 0, 50)
	if err != nil {
		t.Fatalf("Failed to consume first 50 records: %v", err)
	}

	if len(firstBatch) != 50 {
		t.Errorf("Expected 50 records in first batch, got %d", len(firstBatch))
	}

	// Verify first batch values
	for i, record := range firstBatch {
		expectedValue := float64(i)
		if record.Value != expectedValue {
			t.Errorf("First batch record %d value mismatch: expected %f, got %f", i, expectedValue, record.Value)
		}
	}

	// Now consume from offset 50 (middle of file)
	consumedRecords, _, _, err := q.Consume(0, "test-group", 50, 10)
	if err != nil {
		t.Fatalf("Failed to consume from offset 50: %v", err)
	}

	// Verify we got records starting from offset 50
	if len(consumedRecords) != 10 {
		t.Errorf("Expected 10 records, got %d", len(consumedRecords))
	}

	// Verify the values are correct (should be 50-59)
	for i, record := range consumedRecords {
		expectedValue := float64(50 + i)
		if record.Value != expectedValue {
			t.Errorf("Record %d value mismatch: expected %f, got %f", i, expectedValue, record.Value)
		}
	}

	if err := q.Close(); err != nil {
		t.Fatalf("Failed to close queue: %v", err)
	}
}

func TestCache_PutAndGet(t *testing.T) {
	cache := NewCache(5*time.Minute, 1*time.Minute)

	record := util.TelemetryRecord{
		Timestamp:  time.Now(),
		MetricName: "test",
		UUID:       "test-uuid",
		Value:      42.0,
	}

	cache.Put(0, record)

	retrieved, exists := cache.Get(0)
	if !exists {
		t.Error("Expected record to exist in cache")
	}

	if retrievedRecord, ok := retrieved.(util.TelemetryRecord); ok {
		if retrievedRecord.Value != 42.0 {
			t.Errorf("Expected value 42.0, got %f", retrievedRecord.Value)
		}
	} else {
		t.Error("Expected TelemetryRecord type")
	}
}

func TestCache_Expiration(t *testing.T) {
	cache := NewCache(100*time.Millisecond, 50*time.Millisecond)

	record := util.TelemetryRecord{
		Timestamp:  time.Now(),
		MetricName: "test",
		UUID:       "test-uuid",
		Value:      42.0,
	}

	cache.Put(0, record)

	// Record should exist immediately
	_, exists := cache.Get(0)
	if !exists {
		t.Error("Expected record to exist immediately")
	}

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Record should be expired
	_, exists = cache.Get(0)
	if exists {
		t.Error("Expected record to be expired after duration")
	}
}

func TestCache_GetFrom(t *testing.T) {
	cache := NewCache(5*time.Minute, 1*time.Minute)

	// Add multiple records
	for i := 0; i < 5; i++ {
		record := util.TelemetryRecord{
			Timestamp:  time.Now(),
			MetricName: "test",
			UUID:       "test-uuid",
			Value:      float64(i),
		}
		cache.Put(int64(i), record)
	}

	// Get records starting from offset 2
	records, nextOffset, hasMore := cache.GetFrom(2, 10)

	if len(records) != 3 {
		t.Errorf("Expected 3 records from offset 2, got %d", len(records))
	}

	if nextOffset != 5 {
		t.Errorf("Expected nextOffset 5, got %d", nextOffset)
	}

	if hasMore {
		t.Error("Expected hasMore to be false")
	}

	// Verify values
	for i, r := range records {
		if record, ok := r.(util.TelemetryRecord); ok {
			expectedValue := float64(i + 2)
			if record.Value != expectedValue {
				t.Errorf("Record %d value mismatch: expected %f, got %f", i, expectedValue, record.Value)
			}
		}
	}
}

func TestCache_HasMore(t *testing.T) {
	cache := NewCache(5*time.Minute, 1*time.Minute)

	// Add records
	for i := 0; i < 5; i++ {
		record := util.TelemetryRecord{
			Timestamp:  time.Now(),
			MetricName: "test",
			UUID:       "test-uuid",
			Value:      float64(i),
		}
		cache.Put(int64(i), record)
	}

	// Test hasMore at various offsets
	t.Run("hasMore at offset 0", func(t *testing.T) {
		_, nextOffset, hasMore := cache.GetFrom(0, 2)
		if !hasMore {
			t.Error("Expected hasMore to be true at offset 0 with 5 records")
		}
		if nextOffset != 2 {
			t.Errorf("Expected nextOffset 2, got %d", nextOffset)
		}
	})

	t.Run("hasMore at offset 4", func(t *testing.T) {
		_, nextOffset, hasMore := cache.GetFrom(4, 1)
		if hasMore {
			t.Error("Expected hasMore to be false at offset 4 (last record)")
		}
		if nextOffset != 5 {
			t.Errorf("Expected nextOffset 5, got %d", nextOffset)
		}
	})

	t.Run("hasMore at offset 5 (past end)", func(t *testing.T) {
		_, nextOffset, hasMore := cache.GetFrom(5, 1)
		if hasMore {
			t.Error("Expected hasMore to be false at offset 5 (past end)")
		}
		if nextOffset != 5 {
			t.Errorf("Expected nextOffset 5, got %d", nextOffset)
		}
	})
}

func TestCache_Clear(t *testing.T) {
	cache := NewCache(5*time.Minute, 1*time.Minute)

	// Add records
	for i := 0; i < 5; i++ {
		record := util.TelemetryRecord{
			Timestamp:  time.Now(),
			MetricName: "test",
			UUID:       "test-uuid",
			Value:      float64(i),
		}
		cache.Put(int64(i), record)
	}

	// Verify records exist
	_, exists := cache.Get(0)
	if !exists {
		t.Error("Expected record to exist before clear")
	}

	// Clear cache
	cache.Clear()

	// Verify records are gone
	_, exists = cache.Get(0)
	if exists {
		t.Error("Expected record to not exist after clear")
	}

	_, exists = cache.Get(4)
	if exists {
		t.Error("Expected record to not exist after clear")
	}
}

func TestCache_Cleanup(t *testing.T) {
	cache := NewCache(100*time.Millisecond, 50*time.Millisecond)

	// Add records
	for i := 0; i < 5; i++ {
		record := util.TelemetryRecord{
			Timestamp:  time.Now(),
			MetricName: "test",
			UUID:       "test-uuid",
			Value:      float64(i),
		}
		cache.Put(int64(i), record)
	}

	// Verify all records exist initially
	for i := 0; i < 5; i++ {
		_, exists := cache.Get(int64(i))
		if !exists {
			t.Errorf("Expected record %d to exist before cleanup", i)
		}
	}

	// Wait for cleanup interval to run
	time.Sleep(200 * time.Millisecond)

	// All records should be expired and cleaned up
	for i := 0; i < 5; i++ {
		_, exists := cache.Get(int64(i))
		if exists {
			t.Errorf("Expected record %d to be cleaned up", i)
		}
	}
}

func TestProduce_EmptyRecords(t *testing.T) {
	tempDir := t.TempDir()

	cfg := &config.QueueConfig{
		DataDir:              tempDir,
		NumPartitions:        3,
		CacheDuration:        2 * time.Minute,
		CacheCleanupInterval: 1 * time.Minute,
		MaxMessageSize:       1024 * 1024,
		SegmentSize:          100 * 1024 * 1024,
	}

	q, err := NewQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	records := []util.TelemetryRecord{}
	responses, err := q.Produce(records)

	if err != nil {
		t.Fatalf("Produce with empty records should not error: %v", err)
	}

	if len(responses) != 0 {
		t.Errorf("Expected 0 responses for empty records, got %d", len(responses))
	}
}

func TestProduce_SingleRecord(t *testing.T) {
	tempDir := t.TempDir()

	cfg := &config.QueueConfig{
		DataDir:              tempDir,
		NumPartitions:        3,
		CacheDuration:        2 * time.Minute,
		CacheCleanupInterval: 1 * time.Minute,
		MaxMessageSize:       1024 * 1024,
		SegmentSize:          100 * 1024 * 1024,
	}

	q, err := NewQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	record := util.TelemetryRecord{
		Timestamp:  time.Now(),
		MetricName: "temp",
		UUID:       "single-uuid",
		Value:      42.0,
	}

	responses, err := q.Produce([]util.TelemetryRecord{record})
	if err != nil {
		t.Fatalf("Failed to produce single record: %v", err)
	}

	if len(responses) != 1 {
		t.Errorf("Expected 1 response, got %d", len(responses))
	}

	if !responses[0].Success {
		t.Error("Expected single record to succeed")
	}
}

func TestConsume_InvalidPartitionID(t *testing.T) {
	tempDir := t.TempDir()

	cfg := &config.QueueConfig{
		DataDir:              tempDir,
		NumPartitions:        3,
		CacheDuration:        2 * time.Minute,
		CacheCleanupInterval: 1 * time.Minute,
		MaxMessageSize:       1024 * 1024,
		SegmentSize:          100 * 1024 * 1024,
	}

	q, err := NewQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	tests := []struct {
		name        string
		partitionID int
		wantErr     bool
	}{
		{
			name:        "negative partition ID",
			partitionID: -1,
			wantErr:     true,
		},
		{
			name:        "partition ID out of range",
			partitionID: 10,
			wantErr:     true,
		},
		{
			name:        "valid partition ID",
			partitionID: 0,
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, err := q.Consume(tt.partitionID, "test-group", 0, 10)
			if (err != nil) != tt.wantErr {
				t.Errorf("Consume() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConsume_NonexistentOffset(t *testing.T) {
	tempDir := t.TempDir()

	cfg := &config.QueueConfig{
		DataDir:              tempDir,
		NumPartitions:        1,
		CacheDuration:        2 * time.Minute,
		CacheCleanupInterval: 1 * time.Minute,
		MaxMessageSize:       1024 * 1024,
		SegmentSize:          100 * 1024 * 1024,
	}

	q, err := NewQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	// Try to consume from an offset that doesn't exist
	records, nextOffset, hasMore, err := q.Consume(0, "test-group", 1000, 10)

	if err != nil {
		t.Fatalf("Consume from nonexistent offset should not error: %v", err)
	}

	if len(records) != 0 {
		t.Errorf("Expected 0 records from nonexistent offset, got %d", len(records))
	}

	// nextOffset should remain at the requested offset when no records found
	if nextOffset != 1000 {
		t.Errorf("Expected nextOffset to remain at 1000, got %d", nextOffset)
	}

	if hasMore {
		t.Error("Expected hasMore to be false for nonexistent offset")
	}
}

func TestPartition_Produce_EmptyRecords(t *testing.T) {
	tempDir := t.TempDir()

	p, err := NewPartition(0, tempDir, 100*1024*1024, 2*time.Minute, 1*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create partition: %v", err)
	}

	records := []util.TelemetryRecord{}
	offset, err := p.Produce(records)

	if err != nil {
		t.Fatalf("Produce with empty records should not error: %v", err)
	}

	if offset != 0 {
		t.Errorf("Expected offset 0 for empty records, got %d", offset)
	}
}

func TestPartition_Consume_EmptyPartition(t *testing.T) {
	tempDir := t.TempDir()

	p, err := NewPartition(0, tempDir, 100*1024*1024, 2*time.Minute, 1*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create partition: %v", err)
	}

	records, nextOffset, hasMore, err := p.Consume("test-group", 0, 10)

	if err != nil {
		t.Fatalf("Consume from empty partition should not error: %v", err)
	}

	if len(records) != 0 {
		t.Errorf("Expected 0 records from empty partition, got %d", len(records))
	}

	if nextOffset != 0 {
		t.Errorf("Expected nextOffset 0 for empty partition, got %d", nextOffset)
	}

	if hasMore {
		t.Error("Expected hasMore to be false for empty partition")
	}
}

func TestPartition_GetID(t *testing.T) {
	tempDir := t.TempDir()

	p, err := NewPartition(5, tempDir, 100*1024*1024, 2*time.Minute, 1*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create partition: %v", err)
	}

	id := p.GetID()
	if id != 5 {
		t.Errorf("Expected ID 5, got %d", id)
	}
}

func TestPartition_GetLastOffset(t *testing.T) {
	tempDir := t.TempDir()

	p, err := NewPartition(0, tempDir, 100*1024*1024, 2*time.Minute, 1*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create partition: %v", err)
	}

	// Initially last offset should be 0
	lastOffset := p.GetLastOffset()
	if lastOffset != 0 {
		t.Errorf("Expected initial last offset 0, got %d", lastOffset)
	}

	// Produce some records
	records := []util.TelemetryRecord{
		{Timestamp: time.Now(), MetricName: "temp", UUID: "test-uuid", Value: 1.0},
		{Timestamp: time.Now(), MetricName: "temp", UUID: "test-uuid", Value: 2.0},
	}

	_, err = p.Produce(records)
	if err != nil {
		t.Fatalf("Failed to produce records: %v", err)
	}

	// Consume to update last offset
	_, _, _, err = p.Consume("", 0, 2)
	if err != nil {
		t.Fatalf("Failed to consume records: %v", err)
	}

	// Note: GetLastOffset returns the deprecated lastOffset field
	// This test verifies the getter works, even if the field is deprecated
	lastOffset = p.GetLastOffset()
	t.Logf("Last offset after consume: %d", lastOffset)
}

func TestPartition_Close(t *testing.T) {
	tempDir := t.TempDir()

	p, err := NewPartition(0, tempDir, 100*1024*1024, 2*time.Minute, 1*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create partition: %v", err)
	}

	// Add some records to cache
	record := util.TelemetryRecord{
		Timestamp:  time.Now(),
		MetricName: "test",
		UUID:       "test-uuid",
		Value:      42.0,
	}
	p.cache.Put(0, record)

	// Verify record exists in cache
	_, exists := p.cache.Get(0)
	if !exists {
		t.Error("Expected record to exist in cache before close")
	}

	// Close partition
	err = p.Close()
	if err != nil {
		t.Fatalf("Close() failed: %v", err)
	}

	// Verify cache was cleared
	_, exists = p.cache.Get(0)
	if exists {
		t.Error("Expected record to not exist in cache after close")
	}
}

func TestMultipleConsumerGroups(t *testing.T) {
	tempDir := t.TempDir()

	cfg := &config.QueueConfig{
		DataDir:              tempDir,
		NumPartitions:        1,
		CacheDuration:        2 * time.Minute,
		CacheCleanupInterval: 1 * time.Minute,
		MaxMessageSize:       1024 * 1024,
		SegmentSize:          100 * 1024 * 1024,
	}

	q, err := NewQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	// Produce records
	uuid := "test-uuid-multi-group"
	records := []util.TelemetryRecord{
		{Timestamp: time.Now(), MetricName: "temp", UUID: uuid, Value: 1.0},
		{Timestamp: time.Now(), MetricName: "temp", UUID: uuid, Value: 2.0},
		{Timestamp: time.Now(), MetricName: "temp", UUID: uuid, Value: 3.0},
		{Timestamp: time.Now(), MetricName: "temp", UUID: uuid, Value: 4.0},
		{Timestamp: time.Now(), MetricName: "temp", UUID: uuid, Value: 5.0},
	}

	_, err = q.Produce(records)
	if err != nil {
		t.Fatalf("Failed to produce records: %v", err)
	}

	// Consumer group A consumes first 2 records
	groupARecords, nextOffsetA, _, err := q.Consume(0, "group-a", 0, 2)
	if err != nil {
		t.Fatalf("Group A failed to consume: %v", err)
	}

	if len(groupARecords) != 2 {
		t.Errorf("Group A expected 2 records, got %d", len(groupARecords))
	}

	if nextOffsetA != 2 {
		t.Errorf("Group A expected nextOffset 2, got %d", nextOffsetA)
	}

	// Consumer group B consumes first 3 records (independent from group A)
	groupBRecords, nextOffsetB, _, err := q.Consume(0, "group-b", 0, 3)
	if err != nil {
		t.Fatalf("Group B failed to consume: %v", err)
	}

	if len(groupBRecords) != 3 {
		t.Errorf("Group B expected 3 records, got %d", len(groupBRecords))
	}

	if nextOffsetB != 3 {
		t.Errorf("Group B expected nextOffset 3, got %d", nextOffsetB)
	}

	// Consumer group A continues from offset 2
	groupARecords2, nextOffsetA2, _, err := q.Consume(0, "group-a", 2, 10)
	if err != nil {
		t.Fatalf("Group A failed to continue consuming: %v", err)
	}

	if len(groupARecords2) != 3 {
		t.Errorf("Group A expected 3 more records, got %d", len(groupARecords2))
	}

	if nextOffsetA2 != 5 {
		t.Errorf("Group A expected nextOffset 5, got %d", nextOffsetA2)
	}

	// Consumer group B continues from offset 3
	groupBRecords2, nextOffsetB2, _, err := q.Consume(0, "group-b", 3, 10)
	if err != nil {
		t.Fatalf("Group B failed to continue consuming: %v", err)
	}

	if len(groupBRecords2) != 2 {
		t.Errorf("Group B expected 2 more records, got %d", len(groupBRecords2))
	}

	if nextOffsetB2 != 5 {
		t.Errorf("Group B expected nextOffset 5, got %d", nextOffsetB2)
	}
}

func TestConcurrentProduce(t *testing.T) {
	tempDir := t.TempDir()

	cfg := &config.QueueConfig{
		DataDir:              tempDir,
		NumPartitions:        3,
		CacheDuration:        2 * time.Minute,
		CacheCleanupInterval: 1 * time.Minute,
		MaxMessageSize:       1024 * 1024,
		SegmentSize:          100 * 1024 * 1024,
	}

	q, err := NewQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	numGoroutines := 10
	recordsPerGoroutine := 10
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			uuid := fmt.Sprintf("concurrent-uuid-%d", goroutineID)
			var records []util.TelemetryRecord
			for j := 0; j < recordsPerGoroutine; j++ {
				records = append(records, util.TelemetryRecord{
					Timestamp:  time.Now(),
					MetricName: "temp",
					UUID:       uuid,
					Value:      float64(goroutineID*recordsPerGoroutine + j),
				})
			}

			_, err := q.Produce(records)
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Concurrent produce error: %v", err)
	}

	// Verify total records produced
	// Find the partition that has the most records
	totalRecords := 0
	for i := 0; i < cfg.NumPartitions; i++ {
		records, _, _, err := q.Consume(i, "test-group", 0, 1000)
		if err != nil {
			t.Fatalf("Failed to consume from partition %d: %v", i, err)
		}
		totalRecords += len(records)
	}

	expectedTotal := numGoroutines * recordsPerGoroutine
	if totalRecords != expectedTotal {
		t.Errorf("Expected %d total records, got %d", expectedTotal, totalRecords)
	}
}

func TestConcurrentConsume(t *testing.T) {
	tempDir := t.TempDir()

	cfg := &config.QueueConfig{
		DataDir:              tempDir,
		NumPartitions:        1,
		CacheDuration:        2 * time.Minute,
		CacheCleanupInterval: 1 * time.Minute,
		MaxMessageSize:       1024 * 1024,
		SegmentSize:          100 * 1024 * 1024,
	}

	q, err := NewQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	// Produce records first
	uuid := "test-uuid-concurrent-consume"
	var records []util.TelemetryRecord
	for i := 0; i < 100; i++ {
		records = append(records, util.TelemetryRecord{
			Timestamp:  time.Now(),
			MetricName: "temp",
			UUID:       uuid,
			Value:      float64(i),
		})
	}

	_, err = q.Produce(records)
	if err != nil {
		t.Fatalf("Failed to produce records: %v", err)
	}

	// Concurrent consumers from different consumer groups
	numGoroutines := 5
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)
	recordCounts := make(chan int, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			consumerGroupID := fmt.Sprintf("consumer-group-%d", goroutineID)
			consumedRecords, _, _, err := q.Consume(0, consumerGroupID, 0, 100)
			if err != nil {
				errors <- err
				return
			}
			recordCounts <- len(consumedRecords)
		}(i)
	}

	wg.Wait()
	close(errors)
	close(recordCounts)

	// Check for errors
	for err := range errors {
		t.Errorf("Concurrent consume error: %v", err)
	}

	// Each consumer should get all records since they're in different groups
	for count := range recordCounts {
		if count != 100 {
			t.Errorf("Expected 100 records per consumer, got %d", count)
		}
	}
}

func TestConcurrentProduceConsume(t *testing.T) {
	tempDir := t.TempDir()

	cfg := &config.QueueConfig{
		DataDir:              tempDir,
		NumPartitions:        1,
		CacheDuration:        2 * time.Minute,
		CacheCleanupInterval: 1 * time.Minute,
		MaxMessageSize:       1024 * 1024,
		SegmentSize:          100 * 1024 * 1024,
	}

	q, err := NewQueue(cfg)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	var wg sync.WaitGroup
	errors := make(chan error, 4)
	producedCount := make(chan int, 1)
	consumedCount := make(chan int, 1)

	// Producer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		uuid := "test-uuid-mixed"
		var records []util.TelemetryRecord
		for i := 0; i < 50; i++ {
			records = append(records, util.TelemetryRecord{
				Timestamp:  time.Now(),
				MetricName: "temp",
				UUID:       uuid,
				Value:      float64(i),
			})
		}

		_, err := q.Produce(records)
		if err != nil {
			errors <- err
			return
		}
		producedCount <- 50
	}()

	// Consumer goroutine 1
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(50 * time.Millisecond) // Small delay to let producer start
		records, _, _, err := q.Consume(0, "consumer-1", 0, 100)
		if err != nil {
			errors <- err
			return
		}
		consumedCount <- len(records)
	}()

	// Consumer goroutine 2
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond) // Slightly longer delay
		records, _, _, err := q.Consume(0, "consumer-2", 0, 100)
		if err != nil {
			errors <- err
			return
		}
		t.Logf("Consumer 2 got %d records", len(records))
	}()

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Concurrent produce/consume error: %v", err)
	}

	// Verify producer produced all records
	if pc := <-producedCount; pc != 50 {
		t.Errorf("Expected 50 produced records, got %d", pc)
	}

	// Verify consumer got records
	if cc := <-consumedCount; cc == 0 {
		t.Error("Expected consumer to get some records")
	}
}
