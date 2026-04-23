# Installation Guide

This guide provides step-by-step instructions for installing and running the Telemetry Collector system.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Local Development Setup](#local-development-setup)
3. [Kubernetes Deployment](#kubernetes-deployment)
4. [Configuration](#configuration)
5. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Platform Support

This application supports **Windows**, **macOS**, and **Linux**. The Go application is cross-platform, but some commands in this guide have platform-specific alternatives noted below.

### Required Software

- **Go** 1.21 or higher
  - Download from: https://golang.org/dl/
  - Verify installation: `go version`


- **Docker** (for containerization)
  - Download from: https://docs.docker.com/get-docker/
  - Verify installation: `docker --version`

- **Kubernetes** (for orchestration)
  - kind for local development: https://kind.sigs.k8s.io/docs/user/quick-start/
  - kubectl: https://kubernetes.io/docs/tasks/tools/
  - Verify installation: `kubectl version --client`

### Optional Tools

- **Git** (for version control)
  - Download from: https://git-scm.com/downloads

---

## Local Development Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd TelemetryCollector
```

### 2. Install Go Dependencies

```bash
go mod download
go mod tidy
```

### 3. Verify Project Structure

Ensure the following directories exist:
```
TelemetryCollector/
├── cmd/
│   ├── queue/
│   ├── streamer/
│   ├── collector/
│   └── gateway/
├── services/
│   ├── config/
│   ├── logger/
│   ├── queue/
│   ├── streamer/
│   ├── collector/
│   ├── gateway/
│   └── util/
├── k8s/
├── docs/
│   └── swagger.yaml
├── migrations/
├── config.yaml
├── Dockerfile
├── Makefile
└── go.mod
```





## Kubernetes Deployment

### 1. Create kind Cluster (for local testing)

**Option A: Using Docker Desktop**

```bash
# Enable Kubernetes in Docker Desktop
# 1. Open Docker Desktop
# 2. Go to Settings > Kubernetes
# 3. Enable Kubernetes
# 4. Choose 3 or 4 nodes
# 5. Click Apply & Restart

# Verify Kubernetes is running
kubectl cluster-info

# List nodes
kubectl get nodes

kubectl config current-context
```

**Option B: Using kind CLI**

```bash
# Create a new kind cluster with minimum 3 nodes
kind create cluster --name telemetry-cluster --config - <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
- role: worker
- role: worker
- role: worker
EOF

# Verify cluster is running
kubectl cluster-info --context kind-telemetry-cluster

# List nodes
kubectl get nodes

kubectl config current-context
```

**Optional: Delete existing cluster**
```bash
# If you already have a cluster and want to recreate it
kind delete cluster --name telemetry-cluster
kind create cluster --name telemetry-cluster --config - <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
- role: worker
- role: worker
- role: worker
EOF
```

### 2. Build Docker Image

```bash
# Build image
docker build -t telemetry-collector:v1.1 .

# Build with no cache (clean build)
docker build --no-cache -t telemetry-collector:v1.1 .

# Tag for registry (optional)
docker tag telemetry-collector:v1.1 <your-registry>/telemetry-collector:v1.1

# Push to registry (optional)
docker push <your-registry>/telemetry-collector:v1.1
```

### 3. Pull PostgreSQL Image

```bash
# Pull PostgreSQL image
docker pull postgres:15-alpine
```

### 4. Load Image into Kubernetes (for local testing)

```bash
# Load image into kind cluster
kind load docker-image telemetry-collector:v1.1 --name telemetry-cluster
```
