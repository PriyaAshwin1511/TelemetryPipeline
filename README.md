# Telemetry Collector

A distributed telemetry streaming system that reads telemetry data from CSV files, streams it through a custom message queue, persists it to PostgreSQL, and provides REST APIs for querying.

## Table of Contents

- [Overview](#overview)
- [Documentation](#documentation)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [API Documentation](#api-documentation)
- [Testing](#testing)
- [Project Structure](#project-structure)
- [Contributing](#contributing)
- [License](#license)

## Overview

The Telemetry Collector is a scalable, fault-tolerant system designed to process and query telemetry data at scale. It consists of four main components:

1. **Telemetry Streamer** - Reads CSV data and streams it to the message queue
2. **Message Queue Service** - Custom partitioned message queue with append-only file storage and in-memory caching
3. **Telemetry Collector** - Consumes messages from the queue and persists to PostgreSQL
4. **API Gateway** - REST API for querying telemetry data

The system is designed for high throughput, horizontal scalability, and data durability with support for Kubernetes deployment.

## Documentation

This project includes comprehensive documentation covering all aspects of the system:

- **[System Design Document](documentation/SYSTEM_DESIGN.md)** - Detailed architecture, design decisions, component responsibilities, and error handling strategies
- **[Installation Guide](documentation/INSTALL.md)** - Step-by-step instructions for local development setup, Kubernetes deployment, and configuration
- **[User Workflow Guide](documentation/USER_WORKFLOW.md)** - Comprehensive workflow scenarios including happy path operations, scaling, and failure scenarios
- **[Test Plan](documentation/TEST_PLAN.md)** - Testing strategy, unit and integration test structure, and test execution commands
- **[API Documentation](docs/swagger.yaml)** - OpenAPI 3.0 specification for all REST endpoints (also available via Swagger UI at `/swagger`)

## Quick Start

### Prerequisites

- Go 1.21 or higher
- PostgreSQL 15 or higher
- Docker
- Kubernetes (kind recommended for local testing)

### Using Makefile

```bash
# Install dependencies
make install-deps

# Build all services
make build

# Run all tests
make test

# Build Docker image
make docker-build
```

### Local Development

1. Install dependencies:
   ```bash
   make install-deps
   ```

2. Configure the system by editing `config.yaml`

3. Initialize the database:
   ```bash
   psql -U postgres -d telemetry -f migrations/001_create_telemetry_table.sql
   ```

4. Build and run services:
   ```bash
   make build
   make run-queue      # Terminal 1
   make run-streamer   # Terminal 2
   make run-collector  # Terminal 3
   make run-gateway    # Terminal 4
   ```

### Kubernetes Deployment

```bash
# Create namespace
kubectl create namespace telemetry-dev

# Apply Kustomize manifests
kubectl apply -k k8s/overlays/dev

# Verify deployment
kubectl get pods -n telemetry-dev
```

See [Installation Guide](documentation/INSTALL.md) for detailed instructions.

## Architecture

```
┌─────────────────┐
│  CSV File       │
│  (telemetry.csv)│
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  Telemetry Streamer                 │
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
│  └─────────┘ └─────────┘ └─────────┘│
│  HTTP API for produce/consume        │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  Telemetry Collector                │
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
│  API Gateway                        │
│  - GET /api/v1/gpus                 │
│  - GET /api/v1/gpus/{id}/telemetry  │
│  - Swagger/OpenAPI documentation    │
└─────────────────────────────────────┘
```

For detailed architecture information, see the [System Design Document](documentation/SYSTEM_DESIGN.md).

## Features

- **Horizontal Scalability**: Independent scaling of streamer and collector components
- **Fault Tolerance**: Partial availability via partitioned queue
- **Data Durability**: Append-only file storage with fsync on every write
- **High Performance**: In-memory caching for recent data (configurable duration)
- **Partitioned Queue**: Consistent hashing by UUID for efficient queries
- **REST API**: Query telemetry by UUID with optional time range filtering
- **API Validation**: UUID validation, time range validation, proper error responses
- **Swagger Documentation**: Auto-generated OpenAPI 3.0 specification
- **Kubernetes Ready**: Complete K8s manifests with Kustomize overlays
- **Comprehensive Testing**: Unit tests with mocks and integration tests

## Prerequisites

- **Go** 1.21 or higher
  - Download from: https://golang.org/dl/
  - Verify installation: `go version`

- **PostgreSQL** 15 or higher
  - Download from: https://www.postgresql.org/download/
  - Verify installation: `psql --version`

- **Docker** (for containerization)
  - Download from: https://docs.docker.com/get-docker/
  - Verify installation: `docker --version`

- **Kubernetes** (for orchestration)
  - kind for local development: https://kind.sigs.k8s.io/docs/user/quick-start/
  - kubectl: https://kubernetes.io/docs/tasks/tools/
  - Verify installation: `kubectl version --client`

## Installation

For detailed installation instructions, including local development setup and Kubernetes deployment, see the [Installation Guide](documentation/INSTALL.md).

## Configuration

Key configuration options in `config.yaml`:

- **queue.num_partitions**: Number of queue partitions (default: 3)
- **queue.cache_duration**: In-memory cache duration (default: 2m)
- **collector.batch_size**: Batch size for database inserts (default: 100)
- **streamer.batch_size**: Batch size for streaming (default: 100)
- **logging.level**: Log level (debug, info, warn, error)
- **database.host, database.port, database.user, database.password, database.dbname**: PostgreSQL connection settings

## API Documentation

The API Gateway provides REST endpoints for querying telemetry data:

### Health Check
```
GET /health
Response: {"status": "healthy"}
```

### Swagger Documentation
```
GET /swagger/index.html
Interactive Swagger UI

GET /swagger/doc.json
OpenAPI 3.0 specification JSON
```

### List all UUIDs
```
GET /api/v1/gpus
Response: {"uuids": ["uuid-001", "uuid-002"]}
```

### Get telemetry by UUID
```
GET /api/v1/gpus/{id}/telemetry
Response: {"telemetry": [...], "pagination": {...}}
Error 404: No records found for UUID
```

### Get telemetry by UUID with time range
```
GET /api/v1/gpus/{id}/telemetry?start_time=2024-01-01T00:00:00Z&end_time=2024-01-02T00:00:00Z
Response: {"telemetry": [...], "pagination": {...}}
Error 400: Invalid time format or start_time > end_time
```

### Get telemetry with pagination
```
GET /api/v1/gpus/{id}/telemetry?page=1&page_size=100
Response: {"telemetry": [...], "pagination": {"page": 1, "page_size": 100, "total": 1000, "total_pages": 10}}
```

For complete API documentation, see [docs/swagger.yaml](docs/swagger.yaml) or access the Swagger UI at `/swagger/index.html`.

## Testing

The project includes comprehensive unit and integration tests:

### Using Make

```bash
# Run all tests
make test

# Run unit tests only (fast)
make test-unit

# Run integration tests (requires dependencies)
make test-integration

# Generate coverage report
make test-coverage

# View coverage summary
make test-coverage-summary
```

### Using Go Directly

```bash
# Run all tests
go test -v ./...

# Run unit tests only
go test -v -short ./...

# Run integration tests
go test -v -tags=integration ./...

# Generate coverage report
go test -v -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
```

For detailed testing information, see the [Test Plan](documentation/TEST_PLAN.md).

## Project Structure

```
.
├── cmd/
│   ├── collector/    # Collector main entry point
│   ├── gateway/      # API Gateway main entry point
│   ├── queue/        # Queue service main entry point
│   └── streamer/     # Streamer main entry point
├── services/         # Service implementations
│   ├── collector/    # Collector implementation
│   ├── config/       # Configuration management
│   ├── gateway/      # API Gateway implementation
│   ├── logger/       # Logging utilities
│   ├── queue/        # Message queue implementation
│   ├── streamer/     # Streamer implementation
│   └── util/         # Shared types and constants
├── k8s/              # Kubernetes manifests
│   ├── base/         # Base manifests
│   └── overlays/     # Environment-specific overlays
├── docs/             # API documentation
│   ├── swagger.yaml  # OpenAPI 3.0 specification
│   ├── swagger.json  # OpenAPI 3.0 JSON
│   └── swagger-ui.html
├── documentation/    # Project documentation
│   ├── SYSTEM_DESIGN.md   # Architecture and design
│   ├── INSTALL.md         # Installation guide
│   ├── USER_WORKFLOW.md   # User workflows
│   ├── TEST_PLAN.md       # Testing strategy
│   └── README.md          # Documentation overview
├── migrations/       # Database migrations
├── test/             # Integration tests
├── config.yaml       # Configuration file
├── Dockerfile        # Docker image definition
├── Makefile          # Build automation
├── go.mod            # Go module definition
└── go.sum            # Go module dependencies
```

## Contributing

Contributions are welcome! Please ensure all tests pass before submitting a pull request:

```bash
make test
make test-coverage
```

## License

MIT License
