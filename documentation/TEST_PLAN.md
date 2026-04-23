# Test Plan - Telemetry Collector

## Table of Contents

- [Test Structure](#test-structure)
  - [Unit Tests](#unit-tests)
    - [Collector Service Tests](#collector-service-tests-servicescollectorcollector_testgo)
    - [Gateway Service Tests](#gateway-service-tests-servicesgatewaygateway_testgo)
    - [Configuration Tests](#configuration-tests-servicesconfigconfig_testgo)
    - [Logger Tests](#logger-tests-servicesloggerlogger_testgo)
  - [Integration Tests](#integration-tests)
    - [Streamer to Queue Integration](#streamer-to-queue-integration-teststreamertoqueueintegration)
    - [Queue Produce/Consume Integration](#queue-produceconsume-integration-testqueueproduceconsumeintegration)
    - [HTTP Client Integration](#http-client-integration-testhttpclientintegration)
    - [Queue to Collector Integration](#queue-to-collector-integration-testqueuetocollectorintegration)
    - [CSV to Queue End-to-End Integration](#csv-to-queue-end-to-end-integration-testcsvtoqueueendtoend)
- [Commands](#commands)
  - [Using Make (Recommended)](#using-make-recommended)
  - [Using Go Directly (Alternate)](#using-go-directly-alternate)
  - [Run Specific Package](#run-specific-package)

## Test Structure

### Unit Tests

Unit tests are designed to run quickly without external dependencies by using gomock for mocking database and HTTP client interfaces. These tests focus on individual service logic and error handling.

#### Collector Service Tests (`services/collector/collector_test.go`)
The collector service tests verify the data collection and persistence logic with 15+ test functions:

- **Initialization Tests**
  - `TestNewCollector` - Verifies collector creation with valid configuration
  - `TestNewCollector_ConfigValidation` - Tests configuration validation including empty queue endpoints and invalid parameters
  - `TestNewCollectorWithDB` - Tests collector creation with mocked database interface

- **Lifecycle Tests**
  - `TestStart` - Verifies the collector starts its polling goroutine correctly
  - `TestStop` - Ensures graceful shutdown without panics
  - `TestStop_ClosesDatabase` - Verifies database connection is closed on stop
  - `TestStop_NilDatabase` - Tests stop behavior when database is nil

- **Queue Polling Tests**
  - `TestPollQueue_Success` - Tests successful polling and record retrieval from queue
  - `TestPollQueue_HTTPError` - Verifies error handling when HTTP requests fail
  - `TestPollQueue_NonOKStatus` - Tests handling of non-200 HTTP status codes
  - `TestPollQueue_EmptyRecords` - Verifies graceful handling of empty record sets

- **Persistence Tests**
  - `TestPersistRecords_Success` - Tests successful record persistence to database
  - `TestPersistRecords_BeginError` - Tests error handling when transaction begin fails
  - `TestPersistRecords_PrepareError` - Tests error handling when statement preparation fails
  - `TestPersistRecords_ExecError` - Tests error handling when statement execution fails
  - `TestPersistRecords_CommitError` - Tests error handling when transaction commit fails
  - `TestPersistRecords_MultipleRecords` - Tests batch insertion of multiple records
  - `TestPersistRecords_LabelsMarshalError` - Tests handling of unmarshalable label data

#### Gateway Service Tests (`services/gateway/gateway_test.go`)
The gateway service tests verify HTTP API endpoints with 20+ test functions using httptest for HTTP server simulation:

- **Initialization Tests**
  - `TestNewGateway` - Verifies gateway creation with server and database configuration
  - `TestNewGateway_ConfigValidation` - Tests configuration validation including port validation
  - `TestUUIDValidation` - Tests UUID input validation logic

- **Health Check Tests**
  - `TestHealthCheck` - Verifies the `/health` endpoint returns correct status and response format

- **UUID Listing Tests**
  - `TestListUUIDs` - Tests the `/api/v1/gpus` endpoint with multiple scenarios:
    - Successful UUID retrieval with mocked database
    - Database error handling (500 status)
    - Empty result handling (200 status with empty array)

- **Telemetry Retrieval Tests**
  - `TestGetTelemetryByUUID` - Comprehensive testing of `/api/v1/gpus/:id/telemetry` endpoint:
    - Valid UUID with records (200 status with pagination data)
    - Empty UUID validation (400 status)
    - Invalid start_time format (400 status)
    - Invalid end_time format (400 status)
    - start_time after end_time validation (400 status)
    - No records found scenario (404 status)
    - Database count query error (500 status)
    - Database data query error (500 status)
    - Pagination parameters (page and page_size)
    - Invalid page parameter defaults to 1
    - Invalid page_size parameter defaults to 100
    - page_size exceeds maximum defaults to 100

#### Configuration Tests (`services/config/config_test.go`)
Configuration tests verify YAML parsing and default value handling:

- **Configuration Loading**
  - `TestLoadConfig` - Tests loading complete configuration from YAML file, verifying all sections (server, queue, database, streamer, collector, logging)
  - `TestLoadConfig_Defaults` - Tests that default values are applied for missing configuration fields
  - `TestLoadConfig_InvalidFile` - Verifies error handling for non-existent configuration files

#### Logger Tests (`services/logger/logger_test.go`)
Logger tests verify logrus-based logging functionality:

- **Initialization Tests**
  - `TestInit` - Tests logger initialization with different log levels (info, debug) and formats (json, text)
  - `TestInit_InvalidLevel` - Verifies error handling for invalid log level strings
  - `TestGet` - Tests logger retrieval with fallback to default logger

- **Logging Method Tests**
  - `TestWithFields` - Tests structured logging with field context
  - `TestInfo` / `TestInfof` - Tests info-level logging with output capture validation
  - `TestError` / `TestErrorf` - Tests error-level logging with output capture validation
  - `TestWarn` / `TestWarnf` - Tests warning-level logging with output capture validation
  - `TestDebug` / `TestDebugf` - Tests debug-level logging with output capture validation

### Integration Tests

Integration tests are tagged with `//go:build integration` and require external dependencies. These tests verify service-to-service communication and end-to-end data flows.

#### Streamer to Queue Integration (`TestStreamerToQueueIntegration`)
- Creates a mock HTTP queue server to receive data
- Generates a temporary CSV file with sample telemetry data
- Initializes a streamer service with the CSV file and mock queue endpoint
- Verifies the streamer can successfully parse CSV and send records to the queue
- Validates the HTTP request format and response handling

#### Queue Produce/Consume Integration (`TestQueueProduceConsumeIntegration`)
- Creates a temporary directory for queue storage
- Initializes a queue service with test configuration (3 partitions)
- Produces multiple telemetry records with the same UUID (ensures same partition)
- Consumes records from all partitions to locate the produced data
- Verifies record count and offset values match expectations
- Tests the complete produce-consume cycle without external HTTP dependencies

#### HTTP Client Integration (`TestHTTPClientIntegration`)
- Creates a mock HTTP server with expected request validation
- Tests the real HTTP client implementation (not mocked)
- Verifies POST request method, path, and headers
- Validates request body transmission and response handling
- Ensures the HTTP client correctly handles successful responses

#### Queue to Collector Integration (`TestQueueToCollectorIntegration`)
- Initializes a queue service and produces test records
- Creates a mock queue server that wraps the real queue for HTTP consumption
- Simulates the collector's polling behavior by consuming from the mock server
- Verifies the collector can retrieve records via HTTP from the queue
- Tests the integration between queue storage and HTTP consumption layer

#### CSV to Queue End-to-End Integration (`TestCSVToQueueEndToEnd`)
- Creates a mock queue server to receive and validate incoming data
- Generates a CSV file with multiple records and various data scenarios
- Manually processes CSV rows (simulating streamer behavior)
- Sends each record to the mock queue server via HTTP
- Validates received records count, metric names, values, UUIDs, and hostnames
- Tests the complete data pipeline from CSV file to queue storage

## Commands

### Using Make (Recommended)

The Makefile provides convenient targets for common testing operations. These commands abstract the underlying Go test commands and provide consistent execution across different environments.

```bash
# Run all tests
make test
```
This command executes all unit and integration tests in the project using `go test -v ./...`. The verbose flag (`-v`) outputs detailed test results including test names, pass/fail status, and any log messages. This is the most comprehensive test run and should be used before merging code changes.

```bash
# Run unit tests only (fast)
make test-unit
```
This command executes only unit tests using `go test -v -short ./...`. The `-short` flag skips tests that are marked as long-running or require external dependencies. Unit tests use mocks for database and HTTP clients, so they execute quickly (typically under 30 seconds) without requiring PostgreSQL or other external services. This command is ideal for frequent development iterations.

```bash
# Run integration tests (requires build tag)
make test-integration
```
This command executes integration tests using `go test -v -tags=integration ./...`. The `-tags=integration` flag ensures only tests tagged with the integration build tag are executed. These tests require external dependencies (PostgreSQL database) and test service-to-service interactions. Use this command to verify the complete system integration before major releases.

```bash
# Generate coverage report
make test-coverage
```
This command runs all tests with coverage profiling enabled using `go test -v -coverprofile=coverage.out ./...`. After test execution, it generates an HTML coverage report using `go tool cover -html=coverage.out -o coverage.html`. The generated `coverage.html` file provides an interactive visualization showing which lines of code are covered by tests (highlighted in green) and which are not (highlighted in red). Open this file in a web browser to analyze test coverage.

```bash
# View coverage summary
make test-coverage-summary
```
This command runs all tests and displays a coverage percentage summary for each package using `go test -cover ./...`. The output shows the overall coverage percentage without generating detailed reports. This is useful for quick coverage checks during development.

### Using Go Directly (Alternate)

If Make is not available or you prefer to use Go commands directly, the following commands provide equivalent functionality. These commands give you more control over test execution parameters.

```bash
# Run all tests
go test -v ./...
```
Executes all tests in all subdirectories recursively. The `-v` flag enables verbose output to see individual test results. This includes both unit tests and integration tests (if the build tag is not specified).

```bash
# Run unit tests only
go test -v -short ./...
```
The `-short` flag instructs the Go test runner to skip tests that are marked as long-running. In this project, integration tests are designed to be skipped with this flag, allowing you to run only the fast unit tests. This is the recommended command for frequent development iterations.

```bash
# Run integration tests
go test -v -tags=integration ./...
```
The `-tags=integration` flag enables the build tag defined in the integration test files (`//go:build integration`). This ensures only integration tests are executed. Integration tests require PostgreSQL to be running and may take several minutes to complete.

```bash
# Generate coverage report
go test -v -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
```
The first command runs all tests with coverage profiling enabled and writes the coverage data to `coverage.out`. The second command converts the coverage data into an HTML report. The HTML report provides line-by-line coverage visualization and is the most detailed way to analyze test coverage.

```bash
# View coverage summary
go test -cover ./...
```
This command runs all tests and prints a coverage percentage summary for each package. The output is concise and shows overall coverage without detailed line-by-line information. Use this for quick coverage checks.

### Run Specific Package

When working on a specific service, you can run tests for only that package to reduce execution time and focus on relevant tests.

```bash
# Test collector only
go test -v ./services/collector/
```
Runs all tests in the collector service package. This is useful when making changes specifically to the collector logic.

```bash
# Test gateway only
go test -v ./services/gateway/
```
Runs all tests in the gateway service package. Use this when modifying HTTP API endpoints or gateway logic.

```bash
# Test config only
go test -v ./services/config/
```
Runs all tests in the configuration package. This is helpful when modifying configuration parsing or validation logic.




