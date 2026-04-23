package queue

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"telemetry-collector/services/logger"
	"telemetry-collector/services/util"
)

// Partition represents a single queue partition
type Partition struct {
	id                   int
	dataDir              string
	segmentSize          int64
	cache                *Cache
	mu                   sync.RWMutex
	index                map[int64]int64  // offset -> byte position mapping
	lastOffset           int64            // last consumed offset (deprecated, use consumerGroupOffsets)
	consumerGroupOffsets map[string]int64 // consumer group ID -> last consumed offset
}

// NewPartition creates a new partition
func NewPartition(id int, dataDir string, segmentSize int64, cacheDuration time.Duration, cacheCleanupInterval time.Duration) (*Partition, error) {
	p := &Partition{
		id:                   id,
		dataDir:              dataDir,
		segmentSize:          segmentSize,
		cache:                NewCache(cacheDuration, cacheCleanupInterval),
		consumerGroupOffsets: make(map[string]int64),
	}

	// Ensure data directory exists
	if err := os.MkdirAll(p.dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Load index from file if exists
	p.index = make(map[int64]int64)
	var loadErr error
	if loadErr = p.loadIndex(); loadErr != nil {
		logger.WithFields(map[string]interface{}{
			"partition_id": id,
			"error":        loadErr,
		}).Warn("Failed to load index, starting with empty index")
	}

	// Load last consumed offset from file if exists (for backward compatibility)
	p.lastOffset, loadErr = p.loadOffset()
	if loadErr != nil {
		logger.WithFields(map[string]interface{}{
			"partition_id": id,
			"error":        loadErr,
		}).Warn("Failed to load offset, starting from 0")
		p.lastOffset = 0
	}

	// Load consumer group offsets from files
	if loadErr = p.loadConsumerGroupOffsets(); loadErr != nil {
		logger.WithFields(map[string]interface{}{
			"partition_id": id,
			"error":        loadErr,
		}).Warn("Failed to load consumer group offsets, starting with empty offsets")
	}

	logger.WithFields(map[string]interface{}{
		"partition_id":         id,
		"data_dir":             dataDir,
		"index_entries":        len(p.index),
		"last_offset":          p.lastOffset,
		"consumer_group_count": len(p.consumerGroupOffsets),
	}).Info("Partition initialized")

	return p, nil
}

// Produce writes records to the partition
func (p *Partition) Produce(records []util.TelemetryRecord) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	offset, err := p.getWriteOffset()
	if err != nil {
		return 0, fmt.Errorf("failed to get write offset: %w", err)
	}

	dataFile := p.getDataFilePath()
	f, err := os.OpenFile(dataFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, fmt.Errorf("failed to open data file: %w", err)
	}
	defer f.Close()

	for _, record := range records {
		data, err := json.Marshal(record)
		if err != nil {
			logger.WithFields(map[string]interface{}{
				"partition_id": p.id,
				"error":        err,
			}).Error("Failed to marshal record")
			continue
		}

		// Get current byte position before writing (where this record starts)
		bytePos, err := f.Seek(0, 1) // Seek to current position
		if err != nil {
			return offset, fmt.Errorf("failed to get file position: %w", err)
		}

		// Write record with newline separator
		if _, err := f.Write(append(data, '\n')); err != nil {
			return offset, fmt.Errorf("failed to write record: %w", err)
		}

		// Add to cache at current offset
		p.cache.Put(offset, record)
		// Update index with offset -> byte position mapping
		// index maps offset to where that record STARTS in the file
		p.index[offset] = bytePos
		offset++
	}

	// Sync to disk to ensure data durability (fsync on every write as per design)
	if err := f.Sync(); err != nil {
		return offset, fmt.Errorf("failed to sync file to disk: %w", err)
	}

	logger.WithFields(map[string]interface{}{
		"partition_id": p.id,
		"records":      len(records),
		"offset":       offset,
	}).Info("Data synced to disk")

	// Persist index to file
	if err := p.saveIndex(); err != nil {
		logger.WithFields(map[string]interface{}{
			"partition_id": p.id,
			"error":        err,
		}).Error("Failed to save index")
	}

	logger.WithFields(map[string]interface{}{
		"partition_id": p.id,
		"records":      len(records),
		"offset":       offset,
	}).Debug("Records produced")

	return offset, nil
}

// Consume reads records from the partition starting from the given offset for a specific consumer group
func (p *Partition) Consume(consumerGroupID string, offset int64, maxRecords int) ([]util.TelemetryRecord, int64, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var records []util.TelemetryRecord
	currentOffset := offset
	dataFile := p.getDataFilePath()

	// If consumer group ID is provided, use its last offset if starting from -1 (auto)
	if consumerGroupID != "" && offset == -1 {
		if storedOffset, exists := p.consumerGroupOffsets[consumerGroupID]; exists {
			currentOffset = storedOffset
			logger.WithFields(map[string]interface{}{
				"partition_id":      p.id,
				"consumer_group_id": consumerGroupID,
				"stored_offset":     storedOffset,
			}).Debug("Using stored consumer group offset")
		} else {
			currentOffset = 0
			logger.WithFields(map[string]interface{}{
				"partition_id":      p.id,
				"consumer_group_id": consumerGroupID,
			}).Debug("No stored offset for consumer group, starting from 0")
		}
	}

	// Check cache first
	cachedRecords, nextOffset, hasMore := p.cache.GetFrom(currentOffset, maxRecords)
	if len(cachedRecords) > 0 {
		for _, r := range cachedRecords {
			if record, ok := r.(util.TelemetryRecord); ok {
				records = append(records, record)
			}
		}
		currentOffset = nextOffset
		if len(records) >= maxRecords || !hasMore {
			// Update consumer group offset before returning
			if consumerGroupID != "" {
				p.consumerGroupOffsets[consumerGroupID] = currentOffset
				if err := p.saveConsumerGroupOffset(consumerGroupID); err != nil {
					logger.WithFields(map[string]interface{}{
						"partition_id":      p.id,
						"consumer_group_id": consumerGroupID,
						"error":             err,
					}).Error("Failed to save consumer group offset")
				}
			}
			return records, currentOffset, hasMore, nil
		}
	}

	// Read from disk for remaining records
	f, err := os.Open(dataFile)
	if err != nil {
		if os.IsNotExist(err) {
			return records, currentOffset, false, nil
		}
		return nil, currentOffset, false, fmt.Errorf("failed to open data file: %w", err)
	}
	defer f.Close()

	// Use index to seek to the correct offset if available
	if bytePos, exists := p.index[currentOffset]; exists {
		if _, err := f.Seek(bytePos, 0); err != nil {
			logger.WithFields(map[string]interface{}{
				"partition_id":  p.id,
				"offset":        currentOffset,
				"byte_position": bytePos,
				"error":         err,
			}).Warn("Failed to seek using index, falling back to sequential read")
			// Fall back to sequential read from beginning
			if _, err := f.Seek(0, 0); err != nil {
				return nil, currentOffset, false, fmt.Errorf("failed to seek file: %w", err)
			}
		}
	} else {
		// No index entry for this offset, seek to beginning
		if _, err := f.Seek(0, 0); err != nil {
			return nil, currentOffset, false, fmt.Errorf("failed to seek file: %w", err)
		}
	}

	decoder := json.NewDecoder(f)
	lineNumber := currentOffset

	for decoder.More() && len(records) < maxRecords {
		var record util.TelemetryRecord
		if err := decoder.Decode(&record); err != nil {
			logger.WithFields(map[string]interface{}{
				"partition_id": p.id,
				"offset":       lineNumber,
				"error":        err,
			}).Error("Failed to decode record")
			lineNumber++
			continue
		}

		records = append(records, record)
		currentOffset = lineNumber + 1
		lineNumber++
	}

	// Check if there are more records
	hasMore = p.index[currentOffset] != 0 || lineNumber > currentOffset

	// Update consumer group offset
	if consumerGroupID != "" {
		p.consumerGroupOffsets[consumerGroupID] = currentOffset
		if err := p.saveConsumerGroupOffset(consumerGroupID); err != nil {
			logger.WithFields(map[string]interface{}{
				"partition_id":      p.id,
				"consumer_group_id": consumerGroupID,
				"error":             err,
			}).Error("Failed to save consumer group offset")
		}
	}

	logger.WithFields(map[string]interface{}{
		"partition_id":      p.id,
		"consumer_group_id": consumerGroupID,
		"start_offset":      offset,
		"records":           len(records),
		"next_offset":       currentOffset,
		"has_more":          hasMore,
	}).Debug("Records consumed")

	return records, currentOffset, hasMore, nil
}

// getWriteOffset returns the current write offset using index for efficiency
func (p *Partition) getWriteOffset() (int64, error) {
	dataFile := p.getDataFilePath()
	if _, err := os.Stat(dataFile); os.IsNotExist(err) {
		return 0, nil
	}

	// Use index if available - index maps offset to byte position
	// If index has entries for offsets 0..N-1, next offset is N
	if len(p.index) > 0 {
		// Find the maximum offset in the index
		maxOffset := int64(0)
		for offset := range p.index {
			if offset > maxOffset {
				maxOffset = offset
			}
		}
		// Next offset is maxOffset + 1
		return maxOffset + 1, nil
	}

	// Fallback to counting lines if index is empty
	f, err := os.Open(dataFile)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	count := int64(0)
	decoder := json.NewDecoder(f)
	for decoder.More() {
		var record interface{}
		if err := decoder.Decode(&record); err == nil {
			count++
		}
	}

	return count, nil
}

// getDataFilePath returns the path to the data file
func (p *Partition) getDataFilePath() string {
	return filepath.Join(p.dataDir, "data.dat")
}

// getIndexFilePath returns the path to the index file
func (p *Partition) getIndexFilePath() string {
	return filepath.Join(p.dataDir, "index.idx")
}

// getOffsetFilePath returns the path to the offset file
func (p *Partition) getOffsetFilePath() string {
	return filepath.Join(p.dataDir, "offset.off")
}

// getConsumerGroupOffsetFilePath returns the path to a consumer group offset file
func (p *Partition) getConsumerGroupOffsetFilePath(consumerGroupID string) string {
	// Sanitize consumer group ID to use as filename
	sanitizedID := strings.ReplaceAll(consumerGroupID, "/", "_")
	sanitizedID = strings.ReplaceAll(sanitizedID, "\\", "_")
	return filepath.Join(p.dataDir, fmt.Sprintf("consumer-%s.off", sanitizedID))
}

// loadIndex loads the index from file
func (p *Partition) loadIndex() error {
	indexFile := p.getIndexFilePath()
	f, err := os.Open(indexFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Index file doesn't exist yet
		}
		return err
	}
	defer f.Close()

	// Load index entries
	p.index = make(map[int64]int64)
	var offset, bytePos int64
	for {
		_, err := fmt.Fscanf(f, "%d:%d\n", &offset, &bytePos)
		if err != nil {
			break
		}
		p.index[offset] = bytePos
	}

	return nil
}

// saveIndex saves the index to file
func (p *Partition) saveIndex() error {
	indexFile := p.getIndexFilePath()
	f, err := os.OpenFile(indexFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// Write index entries
	for offset, bytePos := range p.index {
		if _, err := fmt.Fprintf(f, "%d:%d\n", offset, bytePos); err != nil {
			return err
		}
	}

	return f.Sync()
}

// loadOffset loads the last consumed offset from file
func (p *Partition) loadOffset() (int64, error) {
	offsetFile := p.getOffsetFilePath()
	f, err := os.Open(offsetFile)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil // Offset file doesn't exist yet
		}
		return 0, err
	}
	defer f.Close()

	var offset int64
	_, err = fmt.Fscanf(f, "%d\n", &offset)
	if err != nil {
		return 0, err
	}

	return offset, nil
}

// saveOffset saves the last consumed offset to file
func (p *Partition) saveOffset() error {
	offsetFile := p.getOffsetFilePath()
	logger.WithFields(map[string]interface{}{
		"partition_id": p.id,
		"offset_file":  offsetFile,
		"last_offset":  p.lastOffset,
	}).Debug("Saving offset to file")
	f, err := os.OpenFile(offsetFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := fmt.Fprintf(f, "%d\n", p.lastOffset); err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		return err
	}

	logger.WithFields(map[string]interface{}{
		"partition_id": p.id,
		"offset_file":  offsetFile,
	}).Debug("Offset saved successfully")
	return nil
}

// loadConsumerGroupOffsets loads all consumer group offsets from files
func (p *Partition) loadConsumerGroupOffsets() error {
	// List all files matching consumer-*.off pattern
	files, err := filepath.Glob(filepath.Join(p.dataDir, "consumer-*.off"))
	if err != nil {
		return err
	}

	for _, file := range files {
		// Extract consumer group ID from filename
		filename := filepath.Base(file)
		if len(filename) < 13 { // "consumer-" + ".off" = 12 chars minimum
			continue
		}
		consumerGroupID := filename[9 : len(filename)-4] // Remove "consumer-" prefix and ".off" suffix

		f, err := os.Open(file)
		if err != nil {
			logger.WithFields(map[string]interface{}{
				"partition_id":      p.id,
				"consumer_group_id": consumerGroupID,
				"error":             err,
			}).Warn("Failed to open consumer group offset file")
			continue
		}
		defer f.Close()

		var offset int64
		_, err = fmt.Fscanf(f, "%d\n", &offset)
		if err != nil {
			logger.WithFields(map[string]interface{}{
				"partition_id":      p.id,
				"consumer_group_id": consumerGroupID,
				"error":             err,
			}).Warn("Failed to read consumer group offset")
			continue
		}

		p.consumerGroupOffsets[consumerGroupID] = offset
		logger.WithFields(map[string]interface{}{
			"partition_id":      p.id,
			"consumer_group_id": consumerGroupID,
			"offset":            offset,
		}).Debug("Loaded consumer group offset")
	}

	return nil
}

// saveConsumerGroupOffset saves the offset for a specific consumer group
func (p *Partition) saveConsumerGroupOffset(consumerGroupID string) error {
	offsetFile := p.getConsumerGroupOffsetFilePath(consumerGroupID)
	offset := p.consumerGroupOffsets[consumerGroupID]

	logger.WithFields(map[string]interface{}{
		"partition_id":      p.id,
		"consumer_group_id": consumerGroupID,
		"offset_file":       offsetFile,
		"offset":            offset,
	}).Debug("Saving consumer group offset to file")

	f, err := os.OpenFile(offsetFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := fmt.Fprintf(f, "%d\n", offset); err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		return err
	}

	return nil
}

// GetID returns the partition ID
func (p *Partition) GetID() int {
	return p.id
}

// GetLastOffset returns the last consumed offset
func (p *Partition) GetLastOffset() int64 {
	return p.lastOffset
}

// Close closes the partition and releases resources
func (p *Partition) Close() error {
	logger.WithFields(map[string]interface{}{
		"partition_id": p.id,
	}).Info("Closing partition")
	p.cache.Clear()
	return nil
}
