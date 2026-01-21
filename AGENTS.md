# Resources

- Architecture: `specs/1_arch_design_v1.md` for the full design document
- RFCs: `specs/rfc-*.md` for design decisions and proposals

# Dev Workflow

1. [Once] Set up Rust toolchain: `rustup default stable`
2. Build the project: `cargo build`
3. Run tests: `cargo test`

# Project Structure

```
grism/
├── Cargo.toml              # Workspace root
├── src/
│   ├── lib.rs              # Main crate with PyO3 bindings
│   ├── python/             # Python bindings
│   ├── common/             # Shared utilities
│   │   ├── error/          # Error types
│   │   ├── display/        # Display utilities
│   │   ├── config/         # Configuration
│   │   └── runtime/        # Async runtime
│   ├── grism-core/         # Core data model
│   ├── grism-logical/      # Logical plan layer
│   ├── grism-optimizer/    # Query optimization
│   ├── grism-engine/       # Local execution
│   ├── grism-distributed/  # Ray distributed execution
│   └── grism-storage/      # Storage layer
```

# Testing

- `cargo test` runs all tests
- `cargo test -p grism-core` runs tests for a specific crate
- Tests are located alongside the code in `#[cfg(test)]` modules

# Code Style

- Follow Rust 2021 edition idioms
- Use `thiserror` for error types
- Use `serde` for serialization
- All public APIs should be documented
