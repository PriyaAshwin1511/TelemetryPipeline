.PHONY: help build test clean docker-build docker-push run install-deps

# Variables
BINARY_DIR=bin
CMD_DIR=cmd
SERVICES=queue streamer collector gateway
DOCKER_IMAGE=telemetry-collector
DOCKER_TAG=latest

help: ## Display this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build all service binaries
	@echo "Building services..."
	@mkdir -p $(BINARY_DIR)
	@for service in $(SERVICES); do \
		echo "Building $$service..."; \
		go build -o $(BINARY_DIR)/$$service ./$(CMD_DIR)/$$service; \
	done
	@echo "Build complete"

build-%: ## Build a specific service (make build-service-name)
	@echo "Building $*..."
	@mkdir -p $(BINARY_DIR)
	@go build -o $(BINARY_DIR)/$* ./$(CMD_DIR)/$*

test: ## Run all tests
	@echo "Running tests..."
	go test -v ./...

test-unit: ## Run unit tests only
	@echo "Running unit tests..."
	go test -v -short ./...

test-integration: ## Run integration tests
	@echo "Running integration tests..."
	go test -v -tags=integration ./...

test-coverage: ## Run tests with coverage report
	@echo "Running tests with coverage..."
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

test-coverage-summary: ## Run tests and display coverage summary
	@echo "Running tests with coverage summary..."
	go test -cover ./...

clean: ## Clean build artifacts
	@echo "Cleaning..."
	@rm -rf $(BINARY_DIR)
	@go clean
	@echo "Clean complete"

docker-build: ## Build Docker image
	@echo "Building Docker image..."
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .
	@echo "Docker image built: $(DOCKER_IMAGE):$(DOCKER_TAG)"

docker-push: ## Push Docker image (requires registry configuration)
	@echo "Pushing Docker image..."
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)

run-queue: ## Run queue service locally
	@echo "Starting queue service..."
	$(BINARY_DIR)/queue

run-streamer: ## Run streamer service locally
	@echo "Starting streamer service..."
	$(BINARY_DIR)/streamer

run-collector: ## Run collector service locally
	@echo "Starting collector service..."
	$(BINARY_DIR)/collector

run-gateway: ## Run gateway service locally
	@echo "Starting gateway service..."
	$(BINARY_DIR)/gateway

install-deps: ## Install Go dependencies
	@echo "Installing dependencies..."
	go mod download
	go mod tidy
	@echo "Dependencies installed"

format: ## Format Go code
	@echo "Formatting code..."
	go fmt ./...
	@echo "Code formatted"

lint: ## Run linter (requires golangci-lint)
	@echo "Running linter..."
	golangci-lint run ./...

swagger: ## Generate Swagger documentation
	@echo "Generating Swagger documentation..."
	@swag init -g cmd/gateway/main.go -o docs
	@echo "Swagger documentation generated at docs/swagger.yaml"
