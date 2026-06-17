# dq-framework Makefile
# Usage: make <target>  [TARGET=dev|prod]  [CATALOG=dev_catalog]

TARGET  ?= dev
CATALOG ?= dev_catalog
SCHEMA  ?= dq

.PHONY: help install lint test build validate deploy-dev deploy-prod clean

help:
	@echo ""
	@echo "dq-framework — available targets"
	@echo "────────────────────────────────────────────────────"
	@echo "  install       install package + dev deps (editable)"
	@echo "  lint          run ruff linter"
	@echo "  test          run pytest suite"
	@echo "  build         build wheel into dist/"
	@echo "  validate      validate DAB bundle (TARGET=dev|prod)"
	@echo "  deploy-dev    deploy bundle to dev workspace"
	@echo "  deploy-prod   deploy bundle to prod workspace"
	@echo "  clean         remove build artefacts"
	@echo ""
	@echo "Variables:"
	@echo "  TARGET=$(TARGET)   CATALOG=$(CATALOG)   SCHEMA=$(SCHEMA)"
	@echo ""

# ── Dev environment ────────────────────────────────────────────────────────────
install:
	pip install --upgrade pip
	pip install -e ".[dev]"

lint:
	ruff check src/ tests/

test:
	pytest tests/ -v --tb=short

# ── Build ──────────────────────────────────────────────────────────────────────
build:
	python -m build --wheel --outdir dist/
	@echo "Wheel built:"
	@ls -1 dist/*.whl

# ── DAB ───────────────────────────────────────────────────────────────────────
validate:
	databricks bundle validate --target $(TARGET)

deploy-dev:
	databricks bundle deploy --target dev \
	  --var catalog=$(CATALOG) \
	  --var schema=$(SCHEMA)
	@echo "Deployed to dev workspace."

deploy-prod:
	@echo "Deploying to PROD — confirm? [y/N] " && read ans && [ $${ans:-N} = y ]
	databricks bundle deploy --target prod \
	  --var catalog=$(CATALOG) \
	  --var schema=$(SCHEMA)
	@echo "Deployed to prod workspace."

# ── Housekeeping ───────────────────────────────────────────────────────────────
clean:
	rm -rf dist/ build/ src/*.egg-info __pycache__ .pytest_cache
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete
