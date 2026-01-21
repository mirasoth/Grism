# Grism Development Guide

## Specification Hierarchy

The following specifications define the Grism system. They must be followed in order of priority:

### 1. Core Standards (Must Comply)

- **`specs/1_arch_design_v1.md`** - Architecture design document, defines core concepts, data model, and system architecture
- **`specs/rfc-namings.md`** - Authoritative naming reference for all layers (logical, physical, storage)

### 2. RFC Specifications (Must Comply)

Each RFC defines specific system aspects. See `specs/rfc-index.md`.

### 3. API References (May Change)

- **`specs/2_python_api_v0.1.md`** - Python API contract for UX interfaces (subject to change)

### 4. Planning Documents

- **`specs/3_dev_schedule.md`** - Development schedule and milestones

---

## Key Design Principles

From the architecture and RFCs, these principles must be followed:

1. **Hypergraph-first semantics** - All relations are hyperedges; binary edges are projections only
2. **Logical ≠ Physical ≠ Storage** - Layer boundaries must be respected in naming and implementation
3. **One concept → one canonical name** - Aliases allowed only at public API boundaries
4. **Operators are semantic, not algorithmic** - Physical algorithms live in `*Exec` names only
5. **Hyperedges can reference other hyperedges** - Enabling meta-relations, provenance, and inference chains

---

## Resources

- Architecture: `specs/1_arch_design_v1.md` for the full design document
- RFCs: `specs/rfc-*.md` for design decisions and proposals
- Python API: `specs/2_python_api_v0.1.md` for Python interface contract
- Schedule: `specs/3_dev_schedule.md` for development planning

---

## Dev Workflow

1. [Once] Set up Rust toolchain: `rustup default stable`
2. Build the project: `cargo build`
3. Run tests: `cargo test`

---

## Recording Work Progress

AI agents should record work progress in the `_workdir/` directory for each development session.

### File Naming

Create progress files named: `progress-YYYY-MM-DD-NNN.md`
- `YYYY-MM-DD`: Date of the session
- `NNN`: Sequence number for multiple sessions on the same day (001, 002, etc.)

Example: `_workdir/progress-2026-01-21-001.md`

### Progress File Format

Use the template in `_workdir/_template.md` as a reference. Each progress file should include:

**Frontmatter (YAML):**
```yaml
---
date: YYYY-MM-DD
session: <unique-id>
objective: <one-line summary>
status: completed | in-progress | blocked
---
```

**Required Sections:**
- **Objective** - What was the goal of this session
- **Completed** - List of completed tasks
- **Files Changed** - List of modified files with brief descriptions
- **Tests** - Test results (total passed/failed, new tests added)
- **Notes** - Important observations, decisions, or context
- **Next Steps** - What comes next (even if "none")

### Guidelines

1. **Be concise** - Focus on outcomes, not process
2. **List files** - Always document which files were changed
3. **Note tests** - Include test results when code changes are made
4. **Link specs** - Reference relevant specs/RFCs when applicable
5. **Next steps** - Always indicate what comes next, even if "none"

See `_workdir/_template.md` for the complete template and field descriptions.

---

## Project Structure

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
│   ├── grism-core/         # Core data model (Hyperedge, Node, Schema, Types)
│   ├── grism-logical/      # Logical plan layer (LogicalOp, Expressions)
│   ├── grism-optimizer/    # Query optimization (Rewrite rules)
│   ├── grism-engine/       # Local execution
│   ├── grism-distributed/  # Ray distributed execution
│   └── grism-storage/      # Storage layer (Lance backend)
```

---

## Testing

- `cargo test` runs all tests
- `cargo test -p grism-core` runs tests for a specific crate
- Tests are located alongside the code in `#[cfg(test)]` modules

---

## Code Style

- Follow Rust 2021 edition idioms
- Use `thiserror` for error types
- Use `serde` for serialization
- All public APIs should be documented
- Follow naming conventions from `specs/rfc-namings.md`

---

## Core Data Model (grism-core)

Key types that must align with architecture:

| Concept | Type | Description |
|---------|------|-------------|
| Graph container | `Hypergraph` | Canonical user-facing container |
| Atomic entity | `Node` | Stable identity with labels and properties |
| N-ary relation | `Hyperedge` | Sole relational primitive with role bindings |
| Binary relation | `Edge` | View-only projection of arity=2 hyperedge |
| Entity reference | `EntityRef` | Reference to Node or Hyperedge |
| Role binding | `RoleBinding` | Associates role with target entity |

### API Conventions

Building hyperedges:
- `with_node(node_id, role)` - Add node binding
- `with_hyperedge(edge_id, role)` - Add hyperedge binding (meta-relations)
- `with_binding(entity_ref, role)` - Generic binding

Checking involvement:
- `involves_node(node_id)` - Check node involvement
- `involves_hyperedge(edge_id)` - Check hyperedge involvement
- `involves_entity(entity_ref)` - Generic check
