GPU Telemetry Streaming System

This project implements a telemetry pipeline designed to monitor GPU clusters. It ingests data from local sources, processes it through a custom-built partitioned messaging system, and exposes it via a REST API.

🏗️ System Design Overview

The architecture is built on a decoupled microservices pattern using Golang. It features a custom Message Queue designed with append-only file storage and consistent hashing to ensure telemetry from specific GPUs remains ordered and scalable across multiple partitions.

    Streamers: Read CSV telemetry data and produce messages to the queue.

    Collectors: Consume messages and persist them to a PostgreSQL database.

    API Gateway: Provides RESTful access to telemetry logs with support for time-window filtering.

    For a deep dive into the architecture refer to: > 📄documentation/SYSTEM_DESIGN.md

🚀 Installation & Deployment

The system is containerized using Docker and orchestrated via Kubernetes. The deployment is managed through Kustomize to handle environment-specific configurations (like the dev overlay).

    Local Setup: Detailed steps for  local builds.

    Kubernetes: Instructions for loading images into kind and applying manifests.

    For step-by-step setup and environment requirements, refer to: > 📄  documentation/INSTALL.md

🔄 User Workflow & Operations

This guide provides practical scenarios for interacting with the system once it is deployed. It covers a couple of scenarios from initial data ingestion to handling infrastructure failures.

    Happy Path: Querying the API for GPU lists and telemetry data.

    Elasticity: Scaling Streamers and Collectors up to 10 instances.

    Resilience: Observation of system behavior during Database or Queue downtime.

    For detailed steps, refer to: > 📄 documentation/USER_WORKFLOW.md


🧪 Testing & Quality Assurance

The project  includes a comprehensive suite of unit tests using mocks for database and HTTP layers, as well as end-to-end integration tests. 

    Unit Tests: Mandatory coverage for core service logic.

    Integration Tests: Verification of service-to-service communication.

    Coverage: Automated reports available via Makefile commands.

The total coverage is ~75%

    For instructions on running tests and viewing coverage reports, refer to: > 📄 documentation/TEST_PLAN.md


🤖 AI Assistance Disclosure

In accordance with the requirements, this project was developed using AI assistance for bootstrapping, unit test generation, and architectural brainstorming.

    Bootstrapping: Initial repository structure and boilerplate code.

    Refinement: Manual intervention was used specifically for the Kustomize deployment strategy and fine-tuning the custom MQ logic.

    For a detailed list of prompts used and specific areas of manual intervention, refer to: > 📄 documentation/AI_prompts.txt
