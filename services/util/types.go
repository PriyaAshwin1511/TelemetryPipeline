package util

import "time"

// TelemetryRecord represents a single telemetry data point from a GPU or device.
type TelemetryRecord struct {
	Timestamp  time.Time              `json:"timestamp"`            // When the metric was recorded
	MetricName string                 `json:"metric_name"`          // Name of the metric (e.g., gpu_utilization)
	GPUID      string                 `json:"gpu_id,omitempty"`     // GPU identifier
	Device     string                 `json:"device,omitempty"`     // Device name
	UUID       string                 `json:"uuid"`                 // Unique identifier for the GPU/device
	ModelName  string                 `json:"model_name,omitempty"` // GPU model name
	Hostname   string                 `json:"hostname,omitempty"`   // Hostname where the GPU resides
	Container  string                 `json:"container,omitempty"`  // Container name
	Pod        string                 `json:"pod,omitempty"`        // Kubernetes pod name
	Namespace  string                 `json:"namespace,omitempty"`  // Kubernetes namespace
	Value      float64                `json:"value"`                // Metric value
	Labels     map[string]interface{} `json:"labels,omitempty"`     // Additional Prometheus-style labels
}

// ProduceRequest is the payload for producing messages to the queue.
type ProduceRequest struct {
	PartitionID int               `json:"partition_id"` // Target partition ID
	Records     []TelemetryRecord `json:"records"`      // Records to produce
}

// ProduceResponse is the response from the produce operation.
type ProduceResponse struct {
	Success bool   `json:"success"`           // Whether the operation succeeded
	Message string `json:"message,omitempty"` // Error message if failed
	Offset  int64  `json:"offset,omitempty"`  // Offset of the produced record
}

// ConsumeRequest is the request to consume messages from a partition.
type ConsumeRequest struct {
	PartitionID     int    `json:"partition_id"`      // Partition to consume from
	ConsumerGroupID string `json:"consumer_group_id"` // Consumer group identifier
	Offset          int64  `json:"offset"`            // Starting offset (-1 for auto)
	MaxRecords      int    `json:"max_records"`       // Maximum records to return
}

// ConsumeResponse is the response from the consume operation.
type ConsumeResponse struct {
	Records         []TelemetryRecord `json:"records"`           // Consumed records
	NextOffset      int64             `json:"next_offset"`       // Next offset to consume from
	HasMore         bool              `json:"has_more"`          // Whether more records are available
	ConsumerGroupID string            `json:"consumer_group_id"` // Consumer group identifier
}

// HealthResponse is the health check response.
type HealthResponse struct {
	Status string `json:"status"` // Health status (e.g., "healthy")
}
