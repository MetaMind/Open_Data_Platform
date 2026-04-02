# MetaMind Makefile

.PHONY: help install dev-install test-setup test lint format type-check quality build up down logs clean

# Default target
help:
	@echo "MetaMind Enterprise Query Intelligence Platform"
	@echo ""
	@echo "Available targets:"
	@echo "  install       - Install production dependencies"
	@echo "  dev-install   - Install development dependencies"
	@echo "  test          - Run all tests"
	@echo "  test-setup    - Create .venv and install test/dev dependencies"
	@echo "  test-unit     - Run unit tests only"
	@echo "  test-integration - Run integration tests (requires Docker)"
	@echo "  lint          - Run linters (ruff)"
	@echo "  format        - Format code (black)"
	@echo "  type-check    - Run type checker (mypy)"
	@echo "  quality       - Run all quality checks"
	@echo "  validate      - Validate imports and quality"
	@echo "  build         - Build Docker images"
	@echo "  up            - Start all services (Docker Compose)"
	@echo "  down          - Stop all services"
	@echo "  logs          - View service logs"
	@echo "  clean         - Clean up build artifacts"
	@echo "  migrate       - Run database migrations"
	@echo "  shell         - Start interactive shell"

# Installation
install:
	pip install -r requirements.txt

dev-install: install
	pip install -e ".[dev]"

# Testing
test-setup:
	./scripts/setup_test_env.sh

test:
	pytest tests/ -v --tb=short

test-unit:
	pytest tests/unit -v --tb=short

test-integration:
	pytest tests/integration -v --tb=short

test-cov:
	pytest --cov=metamind --cov-report=html --cov-report=term

# Code quality
lint:
	ruff check metamind/

format:
	black metamind/ tests/

type-check:
	mypy metamind/

quality: lint type-check
	python scripts/quality_check.py

validate: quality
	python scripts/validate_imports.py

# Docker
build:
	docker-compose build

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f

# Database
migrate:
	./scripts/apply_migrations.sh

# Development
shell:
	python -m metamind.cli.main shell

run:
	uvicorn metamind.api.server:app --host 0.0.0.0 --port 8000 --reload

# Cleanup
clean:
	rm -rf build/ dist/ *.egg-info/
	rm -rf .pytest_cache/ .mypy_cache/ .ruff_cache/
	rm -rf htmlcov/ .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# CI/CD
ci: validate test
	@echo "CI checks passed"

# Release
release: ci
	@echo "Building release..."
	docker build -t metamind:latest .
	@echo "Release built successfully"

# SDK generation (Task 09)
sdk-generate:
	python3 scripts/generate_sdk.py --lang all
