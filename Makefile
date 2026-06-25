# =============================================================================
# Real-Time Events Analytics Pipeline - developer task runner
# -----------------------------------------------------------------------------
# Usage: `make <target>`. Run `make help` to list targets.
# Works on Linux/macOS and on Windows via Git Bash / WSL.
# =============================================================================

.DEFAULT_GOAL := help
SHELL := /bin/bash
PYTHON ?= python

# ----------------------------------------------------------------------------
.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# --- Environment -------------------------------------------------------------
.PHONY: install
install: ## Install Python dependencies
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt

.PHONY: env
env: ## Create a .env from the template
	cp -n .env.example .env && echo "Created .env (edit it with your secrets)"

# --- Code quality ------------------------------------------------------------
.PHONY: format
format: ## Auto-format with black + isort
	$(PYTHON) -m isort .
	$(PYTHON) -m black .

.PHONY: lint
lint: ## Lint with flake8 + type-check with mypy
	$(PYTHON) -m flake8 producer validation dlq spark monitoring config utils
	$(PYTHON) -m mypy producer validation dlq spark monitoring config utils

.PHONY: test
test: ## Run the unit test suite with coverage
	$(PYTHON) -m pytest

.PHONY: test-fast
test-fast: ## Run tests excluding Spark/JVM tests
	$(PYTHON) -m pytest -k "not spark" --no-cov

# --- Local run (no Docker) ---------------------------------------------------
.PHONY: run-producer
run-producer: ## Run the producer locally
	$(PYTHON) -m producer.run_producer

.PHONY: run-streaming
run-streaming: ## Run the Spark streaming job locally (needs Spark + packages)
	spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.41.0 \
		spark/streaming_job.py

.PHONY: dq-report
dq-report: ## Generate a data-quality report from the DLQ fallback file
	$(PYTHON) -c "from config import load_config; from monitoring.data_quality_report import DataQualityReporter as R; r=R(load_config()); rep=r.from_dlq_fallback(); print('No DLQ data' if rep is None else r.write(rep))"

# --- Docker ------------------------------------------------------------------
.PHONY: up
up: ## Start the core stack (Kafka, Schema Registry, topics)
	docker compose up -d zookeeper kafka schema-registry kafka-init

.PHONY: up-all
up-all: ## Start the entire stack (core + producer + Spark)
	docker compose up -d --build

.PHONY: demo-offline
demo-offline: ## Full offline demo (no GCP): Kafka + producer + Spark console sink
	SINK_TYPE=console docker compose up -d --build \
		zookeeper kafka schema-registry kafka-init producer \
		spark-master spark-worker spark-streaming
	@echo ""
	@echo "Offline demo running. Watch the streaming output with:"
	@echo "    docker compose logs -f spark-streaming"

.PHONY: ui
ui: ## Start the optional Kafka UI (http://localhost:8080)
	docker compose --profile ui up -d kafka-ui

.PHONY: logs
logs: ## Tail logs from all containers
	docker compose logs -f

.PHONY: down
down: ## Stop the stack
	docker compose down

.PHONY: clean
clean: ## Stop the stack and remove volumes + checkpoints
	docker compose down -v
	rm -rf checkpoints/* logs/*.log logs/dq_reports/* || true
