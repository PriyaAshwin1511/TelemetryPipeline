// Package util provides common utilities and shared types for the telemetry collector system.
// It includes database and HTTP client interfaces, type definitions, and helper functions.
package util

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// CSV-related constants
const (
	// MinCSVColumns is the minimum number of columns expected in a CSV row
	MinCSVColumns = 12
	// LabelsColumnIndex is the index of the labels column in the CSV row
	LabelsColumnIndex = 11
)

// GetPartitionID returns the partition ID for a given UUID using consistent hashing
func GetPartitionID(uuid string, numPartitions int) int {
	hash := sha256.Sum256([]byte(uuid))
	hashStr := hex.EncodeToString(hash[:])

	hashValue := 0
	for _, c := range hashStr {
		hashValue += int(c)
	}

	return hashValue % numPartitions
}

// CalculateBackoffDelay calculates exponential backoff delay for retry attempts
func CalculateBackoffDelay(attempt int, initialDelay, maxDelay time.Duration, multiplier float64) time.Duration {
	if attempt <= 0 {
		return initialDelay
	}

	delay := initialDelay
	for i := 1; i < attempt; i++ {
		delay = time.Duration(float64(delay) * multiplier)
		if delay >= maxDelay {
			return maxDelay
		}
	}
	return delay
}
