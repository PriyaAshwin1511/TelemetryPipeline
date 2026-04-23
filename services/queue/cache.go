package queue

import (
	"sync"
	"time"
)

// Cache is an in-memory cache for recent records
type Cache struct {
	duration        time.Duration
	cleanupInterval time.Duration
	records         map[int64]recordEntry
	mu              sync.RWMutex
}

// recordEntry represents a cached record with expiration time.
type recordEntry struct {
	record     interface{} // The cached record
	expireTime time.Time   // When the record expires
}

// NewCache creates a new cache with the given duration and cleanup interval.
func NewCache(duration time.Duration, cleanupInterval time.Duration) *Cache {
	c := &Cache{
		duration:        duration,
		cleanupInterval: cleanupInterval,
		records:         make(map[int64]recordEntry),
	}

	// Start cleanup goroutine
	go c.cleanup()

	return c
}

// Put adds a record to the cache with the given offset.
func (c *Cache) Put(offset int64, record interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.records[offset] = recordEntry{
		record:     record,
		expireTime: time.Now().Add(c.duration),
	}
}

// Get retrieves a record from the cache by offset.
func (c *Cache) Get(offset int64) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.records[offset]
	if !exists {
		return nil, false
	}

	if time.Now().After(entry.expireTime) {
		return nil, false
	}

	return entry.record, true
}

// GetFrom retrieves records starting from the given offset up to maxRecords.
func (c *Cache) GetFrom(offset int64, maxRecords int) ([]interface{}, int64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var records []interface{}
	currentOffset := offset
	count := 0

	for currentOffset < offset+int64(maxRecords) {
		entry, exists := c.records[currentOffset]
		if !exists {
			break
		}

		if time.Now().After(entry.expireTime) {
			break
		}

		records = append(records, entry.record)
		currentOffset++
		count++
	}

	_, hasMore := c.records[currentOffset]

	return records, currentOffset, hasMore
}

// cleanup removes expired entries from the cache
func (c *Cache) cleanup() {
	ticker := time.NewTicker(c.cleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()
		now := time.Now()
		for offset, entry := range c.records {
			if now.After(entry.expireTime) {
				delete(c.records, offset)
			}
		}
		c.mu.Unlock()
	}
}

// Clear removes all entries from the cache.
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = make(map[int64]recordEntry)
}
