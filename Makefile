# promanomaly — top-level Makefile
#
# Convention: targets shell out into detector/ for Python work and into
# charts/ for Helm work. Run `make help` for a summary.

SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

PY        := python3
UV        ?= uv
DETECTOR  := detector
CHART_DIR := charts

IMAGE_REPO ?= ghcr.io/esops-dev/promanomaly
IMAGE_TAG  ?= dev

.PHONY: help
help: ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---------- Python ----------

.PHONY: install
install: ## Install Python dependencies into detector/.venv.
	cd $(DETECTOR) && $(UV) sync --all-extras

.PHONY: lint
lint: ## ruff + mypy --strict.
	cd $(DETECTOR) && $(UV) run ruff check .
	cd $(DETECTOR) && $(UV) run ruff format --check .
	cd $(DETECTOR) && $(UV) run mypy --strict src

.PHONY: format
format: ## Apply ruff formatting and import sorting.
	cd $(DETECTOR) && $(UV) run ruff format .
	cd $(DETECTOR) && $(UV) run ruff check --fix .

.PHONY: test
test: ## Run the Python test suite.
	cd $(DETECTOR) && $(UV) run pytest

# BACKTEST_CONFIG defaults to the docker-compose dev config because that's
# what `make dev-up` already points at. Override with
# `make backtest CONFIG=path/to/config.yaml GROUP=...` for any other config.
BACKTEST_CONFIG ?= docker/dev-config.yaml

.PHONY: backtest
backtest: ## Replay historical data, count true/false positives. Usage: make backtest GROUP=error_rates [CONFIG=...] [OUTPUT=markdown]
	cd $(DETECTOR) && $(UV) run python -m promanomaly.backtest \
	  --config ../$(BACKTEST_CONFIG) --group $(GROUP) --output $(or $(OUTPUT),json)

.PHONY: backtest-inject
backtest-inject: ## Inject synthetic anomalies into real history. Usage: make backtest-inject GROUP=error_rates [CONFIG=...] [OUTPUT=markdown]
	cd $(DETECTOR) && $(UV) run python -m promanomaly.backtest \
	  --config ../$(BACKTEST_CONFIG) --group $(GROUP) --inject \
	  --output $(or $(OUTPUT),markdown)

.PHONY: detect-once
detect-once: ## Ad-hoc CLI: run one detector against a PromQL query.
	cd $(DETECTOR) && $(UV) run promanomaly detect-once

.PHONY: inspect
inspect: ## Query a running detector's /debug/inspect for one label set.
	cd $(DETECTOR) && $(UV) run promanomaly inspect --target http://localhost:9092

.PHONY: detectors-list
detectors-list: ## Dump installed detectors with their parameters and defaults.
	cd $(DETECTOR) && $(UV) run promanomaly detectors list

# ---------- Container ----------

.PHONY: build
build: ## Build the detector container image.
	docker build -t $(IMAGE_REPO):$(IMAGE_TAG) $(DETECTOR)

# ---------- Helm ----------

.PHONY: chart-deps
chart-deps: ## Resolve umbrella subchart dependencies.
	helm dependency update $(CHART_DIR)/promanomaly-stack

.PHONY: chart-lint
chart-lint: chart-deps ## helm lint + kubeconform on rendered manifests.
	helm lint $(CHART_DIR)/promanomaly
	helm lint $(CHART_DIR)/promanomaly-stack
	helm lint $(CHART_DIR)/promanomaly-metrics-adapter
	helm template $(CHART_DIR)/promanomaly                  | kubeconform -strict -summary -ignore-missing-schemas
	helm template $(CHART_DIR)/promanomaly-stack            | kubeconform -strict -summary -ignore-missing-schemas
	helm template $(CHART_DIR)/promanomaly-metrics-adapter  | kubeconform -strict -summary -ignore-missing-schemas
	@if command -v yamllint >/dev/null; then \
	  yamllint -c .chart-testing-lintconf.yaml \
	    $(CHART_DIR)/promanomaly/Chart.yaml \
	    $(CHART_DIR)/promanomaly/values.yaml \
	    $(CHART_DIR)/promanomaly/ci \
	    $(CHART_DIR)/promanomaly-stack/Chart.yaml \
	    $(CHART_DIR)/promanomaly-stack/values.yaml \
	    $(CHART_DIR)/promanomaly-stack/ci \
	    $(CHART_DIR)/promanomaly-metrics-adapter/Chart.yaml \
	    $(CHART_DIR)/promanomaly-metrics-adapter/values.yaml \
	    $(CHART_DIR)/promanomaly-metrics-adapter/ci; \
	else \
	  echo "yamllint not installed — skipping (ct lint will run it in CI)"; \
	fi
	@for f in dashboards/grafana/*.json; do python3 -m json.tool "$$f" > /dev/null; done
	@echo "dashboards JSON ok"

.PHONY: chart-matrix
chart-matrix: chart-deps ## Render each chart with every ci/*-values.yaml + kubeconform.
	@set -e; \
	for chart in $(CHART_DIR)/promanomaly $(CHART_DIR)/promanomaly-stack $(CHART_DIR)/promanomaly-metrics-adapter; do \
	  for vf in $$chart/ci/*-values.yaml; do \
	    echo "--- $$chart :: $$(basename $$vf) ---"; \
	    helm template $$chart --values $$vf \
	      | kubeconform -strict -summary -ignore-missing-schemas; \
	  done; \
	done

# values.schema.json is hand-authored alongside values.yaml so it can express
# constraints (enums, ranges, the current single-replica cap) that a generator
# can't infer from a sample. Keep them in sync manually when adding fields.

# ---------- Cross-tool ----------

.PHONY: labels-contract-check
labels-contract-check: ## Diff LABELS_CONTRACT.md against promforecast's copy.
	@if [ -z "$$PROMFORECAST_REPO" ]; then \
	  echo "set PROMFORECAST_REPO=/path/to/promforecast"; exit 2; \
	fi
	@diff -u "$$PROMFORECAST_REPO/LABELS_CONTRACT.md" LABELS_CONTRACT.md \
	  && echo "LABELS_CONTRACT.md matches promforecast"

# ---------- Local dev ----------

.PHONY: dev-up
dev-up: ## Start the minimum dev stack: VM, Prometheus, Redis, node-exporter, detector.
	docker compose -f docker-compose.dev.yml up -d --build

.PHONY: dev-up-load
dev-up-load: ## Start the full demo stack: minimum + http-demo + loadgen + Grafana.
	docker compose -f docker-compose.dev.yml --profile load up -d --build

.PHONY: dev-down
dev-down: ## Stop the local dev stack (any profile).
	docker compose -f docker-compose.dev.yml --profile load down -v

# ---------- Aggregate ----------

.PHONY: ci
ci: lint test chart-lint chart-matrix ## Everything CI runs.
