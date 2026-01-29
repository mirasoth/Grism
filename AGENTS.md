# Grism Development Guide for AI Agents

> **This document contains mandatory rules for AI agents working on this codebase.**
> Read carefully and follow all "Must Comply" sections.

---

## MANDATORY RULES (Must Comply)

**AI agents MUST complete ALL of the following before finishing any development session:**

### 1. Code Quality Gates

Before completing ANY code changes, run these commands and ensure they pass:

```bash
make test    # All tests must pass
make lint    # No clippy warnings allowed (fails on any warning)
```

**If either command fails:**
- Fix all issues before ending the session
- Do NOT leave the codebase in a broken state
- If unable to fix, document the issue clearly in the progress file

### 2. Progress Documentation

Record all work in `_worklog/progress-YYYY-MM-DD-NNN.md` (see [Recording Work Progress](#recording-work-progress-must-comply) section).

### 3. Specification Compliance

Follow the specification hierarchy (see [Specification Hierarchy](#specification-hierarchy) section).

### 4. RFC History Maintenance

**When modifying any RFC file (`specs/rfc-*.md`)**, AI agents MUST also update `specs/rfc-history.md` and `specs/rfc-index.md`:

- Add an entry under the current date
- Document: RFC number, type of change, brief description, author, rationale
- Follow the template format in rfc-history.md

This ensures all RFC changes are tracked chronologically for audit and reference.

### 5. Test Utilities Feature Gating

**Test-only code MUST be feature-gated, not in standalone crates:**

- Use `#[cfg(feature = "test-utils")]` for builders, fixtures, and test helpers
- Add `test-utils` feature to `Cargo.toml` features section
- Enable in `dev-dependencies` for tests: `crate-name = { path = ".", features = ["test-utils"] }`
- This follows industry standard (DataFusion, Polars) for:
  - Clean production binaries (zero test code in release builds)
  - Integration test access (works in `/tests` folder)
  - Downstream extensibility (users can enable for their tests)
  - Benchmark support (use in `/benches` folder)

**Do NOT use:**
- `#[cfg(test)]` - breaks integration tests
- Standalone `test-utils` crate - unnecessary workspace complexity

**Do NOT feature-gate:**
- Public API builders (e.g., `PlanBuilder` in grism-logical is user-facing)
- Production convenience utilities

---

## Quick Reference

| Action | Command |
|--------|---------|
| Build | `make build` |
| Run tests | `make test` |
| Run linter | `make lint` |
| Format code | `make fmt` |
| Check before commit | `make check` |
| Full CI validation | `make ci` |

---

## Specification Hierarchy

Specifications define the Grism system. Follow them in priority order:

### Priority 1: Core Standards (Must Comply)

- **`specs/rfc-0100.md`** - Architecture design document (core concepts, data model, system architecture)
- **`specs/rfc-namings.md`** - Authoritative naming reference for all layers

### Priority 2: RFC Specifications (Must Comply)

Each RFC defines specific system aspects. Index: `specs/rfc-index.md`

### Priority 3: API References (May Change)

- **`specs/rfc-0101.md`** - Python API contract (subject to change)

### Priority 4: Planning Documents

- **`references/`** - Development schedule and milestone documents

---

## Key Design Principles

These principles are derived from the architecture RFCs and must be followed:

| Principle | Description |
|-----------|-------------|
| Hypergraph-first | All relations are hyperedges; binary edges are projections only |
| Layer separation | Logical ≠ Physical ≠ Storage; respect boundaries in naming and implementation |
| One name per concept | Aliases allowed only at public API boundaries |
| Semantic operators | Operators define semantics; algorithms live in `*Exec` names only |
| Meta-relations | Hyperedges can reference other hyperedges (provenance, inference chains) |

---

## Development Workflow

### Setup (One-time)

```bash
rustup default stable
```

### Build & Test Cycle

```bash
make build        # Compile the project
make test         # Run all Rust tests
make lint         # Check for code issues (clippy)
make fmt          # Auto-format code
```

### Before Committing

```bash
make check        # Runs: fmt-check, lint, test, python-lint
```

### Testing Specific Crates

```bash
make test-core        # grism-core tests only
make test-logical     # grism-logical tests only
make test-engine      # grism-engine tests only
make test-storage     # grism-storage tests only
make test-optimizer   # grism-optimizer tests only
```

---

## Recording Work Progress (Must Comply)

**AI agents MUST record work progress in `_worklog/` before completing any session.**

### File Naming

Format: `progress-YYYY-MM-DD-NNN.md`
- `YYYY-MM-DD`: Session date
- `NNN`: Sequence number (001, 002, etc.)

Example: `_worklog/progress-2026-01-21-001.md`

### Required Format

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

| Section | Content |
|---------|---------|
| Objective | Goal of this session |
| Completed | List of completed tasks |
| Files Changed | Modified files with brief descriptions |
| Tests | `make test` results (pass/fail counts) |
| Lint | `make lint` results (pass/fail) |
| Notes | Important observations, decisions, context |
| Next Steps | What comes next (even if "none") |

### Session Completion Checklist

Before ending a session, AI agents MUST:

1. [ ] Run `make test` - all tests pass
2. [ ] Run `make lint` - no warnings
3. [ ] Create progress file in `_worklog/`
4. [ ] Document all files changed
5. [ ] Record test and lint results
6. [ ] Note next steps (even if "none")
7. [ ] If RFC files were modified, update `specs/rfc-history.md`

**Template:** `_worklog/_template.md`

---

## Project Structure

```
grism/
├── Cargo.toml              # Workspace root
├── Makefile                # Build commands (use these!)
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
│   ├── grism-engine/       # Local execution engine
│   ├── grism-distributed/  # Ray distributed execution
│   └── grism-storage/      # Storage layer (Lance backend)
├── specs/                  # Specifications and RFCs
├── references/            # Development milestones and schedules
├── tests/                  # Python integration tests
└── _worklog/               # AI agent progress files
```

---

## Code Style

| Requirement | Details |
|-------------|---------|
| Rust edition | 2021 |
| Error handling | Use `thiserror` |
| Serialization | Use `serde` |
| Documentation | All public APIs must be documented |
| Naming | Follow `specs/rfc-namings.md` |
| Formatting | Run `make fmt` before committing |

---

## Core Data Model (grism-core)

### Key Types

| Concept | Type | Description |
|---------|------|-------------|
| Graph container | `Hypergraph` | Canonical user-facing container |
| Atomic entity | `Node` | Stable identity with labels and properties |
| N-ary relation | `Hyperedge` | Sole relational primitive with role bindings |
| Binary relation | `Edge` | View-only projection of arity=2 hyperedge |
| Entity reference | `EntityRef` | Reference to Node or Hyperedge |
| Role binding | `RoleBinding` | Associates role with target entity |

### API Conventions

**Building hyperedges:**
- `with_node(node_id, role)` - Add node binding
- `with_hyperedge(edge_id, role)` - Add hyperedge binding (meta-relations)
- `with_binding(entity_ref, role)` - Generic binding

**Checking involvement:**
- `involves_node(node_id)` - Check node involvement
- `involves_hyperedge(edge_id)` - Check hyperedge involvement
- `involves_entity(entity_ref)` - Generic check

---

## Resources

| Resource | Location |
|----------|----------|
| Architecture | `specs/rfc-0100.md` |
| All RFCs | `specs/rfc-*.md` |
| RFC Index | `specs/rfc-index.md` |
| Python API | `specs/rfc-0101.md` |
| Milestones | `references/` |
| Progress template | `_worklog/_template.md` |
