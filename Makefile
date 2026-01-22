.PHONY: build test clean fmt lint doc python python-dev python-release python-install python-test

# Build the project
build:
	cargo build

# Build in release mode
release:
	cargo build --release

# Run all tests
test:
	cargo test --all

# Run tests with verbose output
test-verbose:
	cargo test --all -- --nocapture

# Clean build artifacts
clean:
	cargo clean
	rm -rf target/wheels dist *.egg-info .pytest_cache

# Format code
fmt:
	cargo fmt
	black grism/ tests/
	ruff check --fix grism/ tests/

# Check formatting
fmt-check:
	cargo fmt -- --check
	black --check grism/ tests/
	ruff check grism/ tests/

# Run clippy linter
lint:
	cargo clippy --all-targets --all-features -- -D warnings

# Generate documentation
doc:
	cargo doc --no-deps --open

# ============================================================================
# Python Build Commands
# ============================================================================

# Build Python wheel (development mode - unoptimized)
python-dev:
	maturin develop --features python

# Build Python wheel (release mode - optimized)
python-release:
	maturin build --release --features python

# Build and install Python package in current environment
python-install:
	maturin develop --release --features python

# Build Python wheel for distribution
python-wheel:
	maturin build --release --features python --strip

# Run Python tests
python-test: python-dev
	pytest tests/ -v

# Run Python type checking
python-typecheck:
	mypy grism/

# Format Python code
python-fmt:
	black grism/ tests/
	ruff check --fix grism/ tests/

# Lint Python code
python-lint:
	ruff check grism/ tests/
	mypy grism/

# Build Python documentation
python-doc:
	cd docs && make html

# Build with Python support (cargo only, no maturin)
build-python:
	cargo build --features python

# ============================================================================
# Rust Crate Tests
# ============================================================================

test-core:
	cargo test -p grism-core

test-logical:
	cargo test -p grism-logical

test-engine:
	cargo test -p grism-engine

test-storage:
	cargo test -p grism-storage

test-distributed:
	cargo test -p grism-distributed

test-optimizer:
	cargo test -p grism-optimizer

# ============================================================================
# All Tests
# ============================================================================

# Run all Rust and Python tests
test-all: test python-test

# ============================================================================
# Development Workflow
# ============================================================================

# Full development build (Rust + Python)
dev: build python-dev

# Check everything before commit
check: fmt-check lint test python-lint

# Full CI check
ci: fmt-check lint test python-typecheck python-test

# ============================================================================
# Benchmarks
# ============================================================================

# Run E2E logical plan benchmark (validates all 200 queries can construct plans)
bench-e2e-logical: python-dev
	python bench/run_logical_benchmark.py

# Run E2E logical plan benchmark with verbose output
bench-e2e-logical-verbose: python-dev
	python bench/run_logical_benchmark.py --verbose
