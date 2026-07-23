# Developer entry points. Run `make help` to list targets.
# Everything runs through `uv run` so it uses the locked project environment.

.PHONY: help install lint format format-check typecheck test test-spark check hooks clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install: ## Create/sync the virtualenv from pyproject + uv.lock
	uv sync

lint: ## Lint with ruff
	uv run ruff check .

format: ## Auto-format with ruff
	uv run ruff format .

format-check: ## Check formatting without writing (CI-style)
	uv run ruff format --check .

typecheck: ## Static type check with mypy
	uv run mypy

test: ## Run the fast test suite (no JVM)
	uv run pytest

test-spark: ## Run the JVM-backed Spark transformation/DQ tests (needs Java 17)
	uv run --group spark pytest tests/spark

check: lint format-check typecheck test ## Run all quality gates (what CI runs)

hooks: ## Install pre-commit git hooks
	uv run pre-commit install

clean: ## Remove caches and build artifacts
	rm -rf .mypy_cache .ruff_cache .pytest_cache htmlcov .coverage coverage.xml
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
