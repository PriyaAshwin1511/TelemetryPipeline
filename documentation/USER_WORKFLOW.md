# User Workflow Guide

This document provides comprehensive workflow scenarios for operating the Telemetry Streaming System, including happy path operations, scaling scenarios, failure scenarios, and verification commands.

## Table of Contents

- [System Architecture Overview](#system-architecture-overview)
- [1. Happy Path Workflow](#1-happy-path-workflow)
  - [1.1 Initial System Deployment](#11-initial-system-deployment)
  - [1.2 Data Ingestion and Query Workflow](#12-data-ingestion-and-query-workflow)
- [2. Scaling Pods Up/Down](#2-scaling-pods-updown)
  - [2.1 Scaling Streamer Pods](#21-scaling-streamer-pods)
  - [2.2 Scaling Collector Pods](#22-scaling-collector-pods)
- [3. Failure Scenarios](#3-failure-scenarios)
  - [3.1 Bringing Down PostgreSQL Database](#31-bringing-down-postgresql-database)
  - [3.2 Bringing Down Queue Service](#32-bringing-down-queue-service)

## System Architecture Overview

The system consists of the following components:
- **Telemetry Streamer** (default: 2 replicas) - Reads CSV data and streams to message queue
- **Message Queue Service** (default: 1 replica, 3 partitions) - Partitioned message queue with persistent storage
- **Telemetry Collector** (default: 2 replicas) - Consumes from queue and persists to PostgreSQL
- **API Gateway** (default: 1 replica) - REST API for querying telemetry data
- **PostgreSQL Database** - Persistent storage for telemetry records

---

## 1. Happy Path Workflow

### 1.1 Initial System Deployment

**Prerequisites:**
- Kubernetes cluster (Docker Desktop, kind, or cloud provider)
- kubectl configured to access cluster
- Docker image built: `telemetry-collector:v1.1`

**Steps:**

1. **Deploy the system to Kubernetes:**
   ```bash
   # Apply dev overlay (namespace: telemetry-dev)
   kubectl apply -k k8s/overlays/dev
   ```

2. **Verify all pods are running:**
   ```bash
   # Check all pods in telemetry-dev namespace
   kubectl get pods -n telemetry-dev
   
   # Expected output:
   # NAME                              READY   STATUS    RESTARTS   AGE
   # collector-xxxxxxxxxx-xxxxx        1/1     Running   0          2m
   # collector-xxxxxxxxxx-xxxxx        1/1     Running   0          2m
   # (2 collector replicas)
   # queue-service-xxxxxxxxxx-xxxxx    1/1     Running   0          3m
   # streamer-xxxxxxxxxx-xxxxx         1/1     Running   0          2m
   # streamer-xxxxxxxxxx-xxxxx         1/1     Running   0          2m
   # (2 streamer replicas)
   # gateway-xxxxxxxxxx-xxxxx          1/1     Running   0          2m
   # postgres-xxxxxxxxxx-xxxxx         1/1     Running   0          4m
   ```

3. **Verify services are accessible:**
   ```bash
   # Check services in telemetry-dev namespace
   kubectl get svc -n telemetry-dev
   
   # Expected services:
   # - queue-service
   # - postgres-service
   # - gateway-service (if configured)
   ```

4. **Port forward to access Gateway API:**
   ```bash
   # Port forward to gateway service (namespace: telemetry-dev)
   # Service port is 80, container port is 8080
   kubectl port-forward svc/gateway-service 8080:80 -n telemetry-dev
   ```

### 1.2 Data Ingestion and Query Workflow

**Step 1: Verify data streaming is active**

```bash
# Check streamer logs for activity
kubectl logs -n telemetry-dev -l app=streamer --tail=50

# Look for log messages like:
# "Starting streamer with continuous CSV processing"
# "CSV file loaded"
# "total_records": <number>
```

**Step 2: Verify queue is receiving data**

```bash
# Check queue logs
kubectl logs -n telemetry-dev -l app=queue-service --tail=50

# Look for produce requests being processed
```

**Step 3: Verify collector is consuming and persisting**

```bash
# Check collector logs
kubectl logs -n telemetry-dev -l app=collector --tail=50

# Look for messages like:
# "Starting collector"
# "Consuming from partition X"
# "Batch inserted successfully"
```

**Step 4: Access Swagger Documentation**

```bash
# Open browser to Swagger UI
http://localhost:8080/swagger/index.html

# Or access Swagger JSON directly
curl http://localhost:8080/swagger/doc.json
```

**Step 5: Query available GPU UUIDs**

```bash
# List all unique GPU UUIDs
curl http://localhost:8080/api/v1/gpus

# Expected response:
# {
#   "uuids": ["uuid-001", "uuid-002", "uuid-003", ...]
# }
```

**Step 6: Query telemetry for a specific UUID**

```bash
# Get all telemetry for a UUID
curl "http://localhost:8080/api/v1/gpus/uuid-001/telemetry"

# Get telemetry with pagination
curl "http://localhost:8080/api/v1/gpus/uuid-001/telemetry?page=1&page_size=100"

# Get telemetry with time range filter
curl "http://localhost:8080/api/v1/gpus/uuid-001/telemetry?start_time=2024-01-01T00:00:00Z&end_time=2024-01-02T00:00:00Z"
```

**Step 7: Verify queue status (partitions, offsets, consumer groups)**

```bash
# Access queue pod shell to check partitions and offsets
kubectl exec -it -n telemetry-dev <queue-pod-name> -- sh

# Inside pod, check partition structure
ls -la /app/data/queue/
# Should show: partition-0/, partition-1/, partition-2/

# Check individual partition data and offsets
ls -lh /app/data/queue/partition-0/
ls -lh /app/data/queue/partition-1/
ls -lh /app/data/queue/partition-2/

# Monitor queue logs for consumer group activity
kubectl logs -n telemetry-dev -l app=queue-service -f | grep "consumer_group_id"

# Monitor queue logs for produce and consume operations
kubectl logs -n telemetry-dev -l app=queue-service -f | grep -E "produce|consume"

# Check queue logs for offset information
kubectl logs -n telemetry-dev -l app=queue-service --tail=100 | grep "offset"
```

---

## 2. Scaling Pods Up/Down

### 2.1 Scaling Streamer Pods

**Scenario:** Increase streaming throughput by adding more streamer instances

**Scale up:**
```bash
# Scale streamer to 10 replicas
kubectl scale deployment streamer -n telemetry-dev --replicas=10

# Verify scaling
kubectl get pods -n telemetry-dev -l app=streamer
kubectl rollout status deployment/streamer -n telemetry-dev
```

**Expected behavior:**
- New streamer pods start reading the same CSV file
- Multiple streamers will produce to the same queue partitions
- Queue handles concurrent produce requests
- No data duplication expected (same CSV data, but idempotent processing)

**Scale down:**
```bash
# Scale streamer down to 5 replicas
kubectl scale deployment streamer -n telemetry-dev --replicas=5

# Verify scaling
kubectl get pods -n telemetry-dev -l app=streamer
```

**Expected behavior:**
- Excess pods are terminated gracefully
- Remaining streamers continue processing
- No data loss during scale-down

### 2.2 Scaling Collector Pods

**Scenario:** Increase consumption throughput by adding more collector instances

**Scale up:**
```bash
# Scale collector to 10 replicas
kubectl scale deployment collector -n telemetry-dev --replicas=10

# Verify scaling
kubectl get pods -n telemetry-dev -l app=collector
kubectl rollout status deployment/collector -n telemetry-dev
```

**Expected behavior:**
- Multiple collectors consume from the same consumer group
- Each collector reads from different partitions (if partition assignment is implemented)
- Or all collectors compete for messages from all partitions
- Faster consumption from queue
- No data duplication (consumer group coordination)

**Scale down:**
```bash
# Scale collector down to 5 replicas
kubectl scale deployment collector -n telemetry-dev --replicas=5

# Verify scaling
kubectl get pods -n telemetry-dev -l app=collector
```

**Expected behavior:**
- Excess collectors stop consuming
- Partitions are rebalanced among remaining collectors
- No data loss during scale-down

---

## 3. Failure Scenarios

### 3.1 Bringing Down PostgreSQL Database

**Scenario:** Simulate database failure to test system resilience

**Steps:**

1. **Stop PostgreSQL:**
   ```bash
   # Scale down to 0 replicas
   kubectl scale deployment postgres -n telemetry-dev --replicas=0
   
   # Or delete the pod
   kubectl delete pod -n telemetry-dev -l app=postgres
   ```

2. **Observe collector behavior:**
   ```bash
   # Watch collector logs
   kubectl logs -n telemetry-dev -l app=collector -f
   
   # Expected behavior:
   # - Collectors fail to connect to database
   # - Retry logic with exponential backoff kicks in
   # - Collectors continue to consume from queue but fail to persist to database
   # - Messages are consumed from queue but not persisted (data loss during downtime)
   ```

3. **Verify queue health and message accumulation:**
   ```bash
   # Queue should continue accepting produce requests
   curl http://localhost:8080/health
   
   # Check queue logs - should show produce operations succeeding
   kubectl logs -n telemetry-dev -l app=queue-service --tail=50
   
   # Validate queue behavior
   # Monitor queue logs for produce vs consume operations
   # Should see both POST /produce and GET /consume operations
   kubectl logs -n telemetry-dev -l app=queue-service -f | grep -E "produce|consume"
   
   # Expected: You will see both produce and consume operations
   # - POST /produce: Streamers producing data to queue
   # - GET /consume: Collectors consuming from queue
   # Note: Messages are consumed from queue but not persisted to database during outage
   ```

4. **Verify API Gateway behavior:**
   ```bash
   # API queries will fail
   curl http://localhost:8080/api/v1/gpus
   
   # Expected: 500 Internal Server Error
   # Gateway cannot connect to database
   ```

5. **Restore PostgreSQL:**
   ```bash
   # Scale back up
   kubectl scale deployment postgres -n telemetry-dev --replicas=1
   
   # Wait for pod to be ready
   kubectl wait --for=condition=ready pod -l app=postgres -n telemetry-dev --timeout=60s
   ```

6. **Verify recovery:**
   ```bash
   # Collectors should reconnect automatically
   # Watch logs for successful reconnection
   kubectl logs -n telemetry-dev -l app=collector -f
   
   # Backlog in queue should be processed
   # API queries should succeed again
   curl http://localhost:8080/api/v1/gpus
   ```

**What to check:**
- Collector logs for retry attempts and successful reconnection
- Queue logs showing messages accumulating during downtime
- Database connection count after recovery
- Data integrity - no missing or duplicate records

### 3.2 Bringing Down Queue Service

**Scenario:** Simulate queue failure to test producer and consumer behavior

**Steps:**

1. **Stop Queue Service:**
   ```bash
   # Scale down to 0 replicas
   kubectl scale deployment queue-service -n telemetry-dev --replicas=0
   
   # Or delete the pod
   kubectl delete pod -n telemetry-dev -l app=queue-service
   ```

2. **Observe streamer behavior:**
   ```bash
   # Watch streamer logs
   kubectl logs -n telemetry-dev -l app=streamer -f
   
   # Expected behavior:
   # - Streamers fail to produce to queue
   # - Retry logic with exponential backoff activates
   # - CSV reading continues but data buffers in memory
   # - Eventually may fail if retry limit exceeded
   ```

3. **Observe collector behavior:**
   ```bash
   # Watch collector logs
   kubectl logs -n telemetry-dev -l app=collector -f
   
   # Expected behavior:
   # - Collectors fail to consume from queue
   # - Retry logic activates
   # - No new data is persisted to database
   ```

4. **Restore Queue Service:**
   ```bash
   # Scale back up
   kubectl scale deployment queue-service -n telemetry-dev --replicas=1
   
   # Wait for pod to be ready
   kubectl wait --for=condition=ready pod -l app=queue-service -n telemetry-dev --timeout=60s
   ```

5. **Verify recovery:**
   ```bash
   # Streamers should reconnect and resume producing
   # Collectors should reconnect and resume consuming
   # Check logs for successful operations
   
   kubectl logs -n telemetry-dev -l app=streamer --tail=20
   kubectl logs -n telemetry-dev -l app=collector --tail=20
   ```

**Known Issue: Stale DNS Cache**
After restoring the queue service, streamer and collector pods may fail to reconnect due to stale DNS cache. Symptoms include "connection refused" errors even though the queue service is running.

**Solution:**
```bash
# Restart the affected deployments to force DNS re-resolution
kubectl rollout restart deployment streamer -n telemetry-dev
kubectl rollout restart deployment collector -n telemetry-dev

# Verify the pods are working
kubectl logs -n telemetry-dev -l app=streamer --tail=20
kubectl logs -n telemetry-dev -l app=collector --tail=20
```

**What to check:**
- Streamer logs for failed produce attempts and retries
- Collector logs for failed consume attempts and retries
- Queue data persistence after restart (data should still be on disk)
- No data loss after recovery
- Lag in processing after queue comes back online
