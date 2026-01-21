.PHONY: build test clean fmt lint doc

# Build the project
build:
	cargo build

# Build in release mode
release:
	cargo build --release

# Run all tests
test:
	cargo test

# Run tests with verbose output
test-verbose:
	cargo test -- --nocapture

# Clean build artifacts
clean:
	cargo clean

# Format code
fmt:
	cargo fmt

# Check formatting
fmt-check:
	cargo fmt -- --check

# Run clippy linter
lint:
	cargo clippy --all-targets --all-features -- -D warnings

# Generate documentation
doc:
	cargo doc --no-deps --open

# Build with Python support
build-python:
	cargo build --features python

# Run specific crate tests
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
