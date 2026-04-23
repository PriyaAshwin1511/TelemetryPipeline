# Telemetry Streaming System Design Document

This document describes the architecture and design decisions for a telemetry streaming system that reads telemetry data from CSV files, streams it through a custom message queue, persists it to a database, and provides REST APIs for querying.

## Table of Contents

- [Functional Requirements](#functional-requirements)
- [Non-Functional Requirements](#non-functional-requirements)
- [System Architecture](#system-architecture)
- [Design Considerations](#design-considerations)
- [Expected Error Scenarios](#expected-error-scenarios)

## Functional Requirements

1. **CSV Ingestion**: Read telemetry data from CSV files and stream it in real-time.
2. **Custom Message Queue**: Implement a partitioned message queue with append-only file storage and in-memory caching (configurable cache duration).
3. **Data Collection**: Collector reads from the queue and persists telemetry data to database.
4. **API Gateway**: Provide GET APIs to query the telemetry data.

## Non-Functional Requirements

1. **High Throughput**: Process telemetry data at scale with minimal latency.
2. **Horizontal Scalability**: Support scaling streamer and collector components independently.
3. **Fault Tolerance**: Ensure system remains operational during component failures (partial availability via partitions).
4. **Data Durability**: Guarantee no data loss during normal operations and crash recovery (fsync on every write).
5. **Performance**: Support high-throughput queue operations with in-memory caching for recent data.

## System Architecture

### High-Level Design

```
┌─────────────────┐
│  CSV File       │
│  (telemetry.csv)│
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  Telemetry Streamer (10 instances)   │
│  - Reads CSV in loop                 │
│  - Consistent hashing by uuid       │
│  - HTTP client for queue communication│
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  Message Queue Service               │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐│
│  │Partition│ │Partition│ │Partition││
│  │   0     │ │   1     │ │   2     ││
│  │ data    │ │ data    │ │ data    ││
│  │   .dat  │ │   .dat  │ │   .dat  ││
│  │ index   │ │ index   │ │ index   ││
│  │   .idx  │ │   .idx  │ │   .idx  ││
│  │ offset  │ │ offset  │ │ offset  ││
│  │   .off  │ │   .off  │ │   .off  ││
│  └─────────┘ └─────────┘ └─────────┘│
│  HTTP API for produce/consume        │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  Telemetry Collector (10 instances)  │
│  - Consumes via HTTP GET /consume   │
│  - Batches database writes          │
│  - Handles parsing errors           │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  PostgreSQL Database                │
│  - telemetry table                  │
│  - JSONB for labels                 │
│  - B-tree index on (uuid, timestamp)│
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  API Gateway (Gin)                │
│  - GET /api/v1/gpus                 │
│  - GET /api/v1/gpus/{id}/telemetry  │
│  - GET /api/v1/gpus/{id}/telemetry?start_time=..&end_time=..│
│  - Prometheus metrics                │
│  - Health endpoints                 │
│  - Swagger/OpenAPI for API documentation│
└─────────────────────────────────────┘
```

### Component Responsibilities

**Telemetry Streamer**
- **Real-Time Streaming**: Read CSV in loop and stream records immediately.
- Hash uuid to determine target queue partition
- Send data to queue partition via HTTP POST
- **Memory Management**: Bounded buffers for CSV reading, connection pooling for HTTP client


**Message Queue Service**
- Maintain append-only log files per partition
- Expose HTTP API for produce and consume operations
- **Hybrid Persistence**: In-memory cache for last 2 minutes (configurable) of records + append-only file storage
- **Cache Strategy**: Recent records in memory for low-latency reads, older records on disk
- **Cache Eviction**: Time-based eviction (records expire after configured duration)
- Track consumer group offsets persisted to disk for durability

**Telemetry Collector**
- Consume messages from queue via HTTP GET /consume endpoint
- Track consumer offsets via consumer group IDs persisted to disk for durability
- Parse and validate telemetry data
- Batch insert into PostgreSQL
- **Memory Management**: Bounded message buffers, database connection pooling, batch size limits

**API Gateway**
- Serve REST endpoints for telemetry queries
- GET /api/v1/gpus - lists all unique UUIDs from the data
- GET /api/v1/gpus/{id}/telemetry - id is the uuid
- GET /api/v1/gpus/{id}/telemetry?start_time=..&end_time=.. - query by time range
- GET /api/v1/gpus/{id}/telemetry?page=1&page_size=100 - query with pagination (default: page=1, page_size=100, max page_size=1000)
- **Pagination**: Returns pagination metadata (page, page_size, total, total_pages) in response
- **Swagger/OpenAPI**: Auto-generated OpenAPI spec using swaggo/gin-swagger with Gin annotations
- **Makefile Command**: `make swagger` to auto-generate OpenAPI spec
- **Swagger UI**: Available at /swagger/index.html

## Design Considerations


### 1. Messaging Architecture

**Choice**: Partitioned independent queues with append-only file storage

**Rationale**: Horizontal scalability, partial availability during failures

**Other Options Considered**:
- Simple message queue with no partition (no scalability)
- Leader-follower architecture (too complex for initial implementation)
- Pure in-memory queue using channels (no durability)
- Single shared queue service for all components (no separation of concerns)
- Single queue with WAL (no horizontal scalability)

**Industry Standard Comparison**: Simplified Kafka partitioned log model (no replication, no consumer groups, no distributed coordination).

**Trade-off**: No built-in replication or advanced features initially.

### 2. Partitioning Strategy

**Choice**: Consistent hashing by uuid

**Rationale**: Ensures same UUID telemetry in same partition, enables efficient queries by UUID, simplifies offset tracking.

**Other Options Considered**:
- Random assignment (inefficient queries)
- Round-robin (same UUID scattered across partitions)

**Trade-off**: Hot partitions possible if UUID imbalance; mitigated by monitoring and manual rebalancing.

### 3. Queue Persistence

**Choice**: Hybrid approach - in-memory cache for last 2 minutes (configurable) + append-only file storage with index files

**Rationale**: Low latency (memory cache) + durability (disk) + configurable cache duration + proven pattern (Kafka-like).

**Other Options Considered**:
- Memory-only (no durability)
- Disk-only (high latency)
- Database-backed (complexity and performance overhead)

**Trade-off**: Requires cache eviction logic; configurable duration allows tuning.

### 4. Number of Partitions

**Choice**: 3 initial partitions

**Rationale**: Balances complexity and scalability for 10 instances, provides partial availability, easy to add more later.

**Other Options Considered**:
- Single partition (no scalability)
- 10 partitions (too complex for initial implementation)

**Trade-off**: May need rebalancing as data volume grows; rehashing required when adding partitions.

### 5. Protocol Choice

**Choice**: HTTP for produce/consume operations

**Rationale**: Simplicity, no external dependencies, adequate performance for telemetry streaming use case. Connection pooling and HTTP/2 support can address performance needs if required.

**Other Options Considered**:
- AMQP (custom queue requirement, adds complexity, not needed for simple produce/consume pattern)
- gRPC (more complex with protobuf/code generation, though faster with binary serialization and HTTP/2 multiplexing, performance gain not significant for this use case)

**Trade-off**: Less feature-rich than AMQP; HTTP/1.1 lacks multiplexing (can upgrade to HTTP/2 later if needed), slower than gRPC due to JSON serialization but simpler to implement and debug.

### 6. Database

**Choice**: PostgreSQL with structured columns + JSONB for labels

**Rationale**: Efficient time-range queries (B-tree index), flexible labels (JSONB), mature ecosystem.

**Other Options Considered**:
- MongoDB (JSONB better for label queries)
- TimescaleDB (PostgreSQL sufficient for current needs)

**Trade-off**: JSONB queries slower than dedicated columns.

**Schema**:

```sql
CREATE TABLE telemetry (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    metric_name VARCHAR NOT NULL,
    gpu_id VARCHAR,
    device VARCHAR,
    uuid VARCHAR NOT NULL,
    model_name VARCHAR,
    hostname VARCHAR,
    container VARCHAR,
    pod VARCHAR,
    namespace VARCHAR,
    value NUMERIC NOT NULL,
    labels JSONB
);

CREATE INDEX idx_uuid_timestamp ON telemetry(uuid, timestamp);
```

### 7. API Framework

**Choice**: Gin

**Rationale**: Largest ecosystem, proven at scale (Uber, Alibaba), excellent middleware, good performance.

**Other Options Considered**:
- Echo (Gin has better ecosystem)
- Fiber (Gin has better production adoption)

**Trade-off**: Slightly more complex than minimalist frameworks; more features than needed for simple APIs.

### 8. Scope for Improvement

**Short-term**: Segment rotation, cache optimization, metrics enhancement, error handling (dead-letter queue, circuit breakers), batch size configuration via environment variables, consumer groups for partition assignment, connection pooling, bounded message queues, liveness and readiness probes for streamer, collector, gateway, and postgres services.

**Medium-term**: Replication per partition, auto-rebalancing, message replay, compaction, separate side table for UUID metadata, PostgreSQL deployment as StatefulSet for production Kubernetes environments, add PersistentVolumeClaim (PVC) for queue data persistence to survive pod restarts, redesign queue as distributed system with shared storage or distributed message queue (e.g., Kafka, NATS JetStream) to support multi-replica scaling

**Long-term**: Cross-region replication, security (TLS, mTLS), distributed tracing

## Expected Error Scenarios

**Note**: Mitigations listed below reflect initial implementation. Circuit breakers and dead-letter queues are planned for short-term improvements (see Scope for Improvement).

### 1. Queue Partition Failure

**Scenario**: One queue partition becomes unavailable (pod crash, disk failure)

**Detection**: Health checks fail, HTTP requests timeout

**Mitigation**:
- Streamers retry with exponential backoff
- Collectors skip failed partition, continue processing others
- Kubernetes restarts failed pod

**Recovery**: Pod restart replays from append-only files, resumes operation

**Data Impact**: Messages in memory may be lost; persisted data recovered from append-only files

### 2. Database Connection Failure

**Scenario**: PostgreSQL becomes unavailable (network issue, database crash)

**Detection**: Connection errors, query timeouts

**Mitigation**:
- Collector buffers messages in memory (with limit)
- Implements exponential backoff retry
- Database connection pool management

**Recovery**: Connection pool reestablishes, buffered messages flushed

**Data Impact**: Minimal if buffering configured; potential data loss if buffer overflow

### 3. CSV Parsing Errors

**Scenario**: CSV row has malformed data, missing columns, invalid format

**Detection**: Parse errors during CSV reading

**Mitigation**:
- Log error with row number and details
- Skip malformed row, continue processing
- Increment error counter in metrics
- Alert if error rate exceeds threshold

**Recovery**: Manual review of error logs, fix CSV source

**Data Impact**: Malformed rows not processed; valid rows unaffected

### 4. Disk Full

**Scenario**: Queue partition disk runs out of space

**Detection**: Write errors, disk usage metrics

**Mitigation**:
- Stop accepting writes from streamers
- Return 503 Service Unavailable
- Trigger compaction if configured

**Recovery**: Manual intervention (add disk, delete old data, compact queue)

**Data Impact**: No new data accepted until space freed

### 5. Network Partition

**Scenario**: Network issue between components (streamer-queue, queue-collector, collector-database)

**Detection**: Connection timeouts, failed HTTP requests

**Mitigation**:
- Kubernetes restarts affected pods
- Queue data persists on disk
- Retries with exponential backoff

**Recovery**: Network restored, pods reconnect, resume processing

**Data Impact**: Minimal if queue persists data; potential delays in processing
