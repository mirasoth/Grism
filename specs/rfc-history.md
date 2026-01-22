# RFC History

Chronological record of RFC lifecycle events: creation, status changes, and version updates.

---

## History Log

### 2026-01-22

**RFC Management System Established**
- Created rfc-index.md, rfc-specifications.md, rfc-history.md
- Standardized metadata across all RFCs
- Added `Created` field to all existing RFCs

**RFC Renaming**
- Renamed `1_arch_design_v1.md` → `RFC-0100.md` (Frozen)
- Renamed `2_python_api_v0.1.md` → `RFC-0101.md` (Draft)
- Updated all references across project

### 2026-01-21

**Initial RFC Batch Created**

**Core Standards (Frozen)**
- RFC-0001: Hypergraph Logical Model and Execution Architecture
- RFC-0002: Hypergraph Logical Algebra & Formal Semantics
- RFC-0003: Expression System & Type Model
- RFC-0100: Architecture Design (formerly 1_arch_design_v1.md)
- rfc-namings.md: Naming Alignment & Completion

**Draft RFCs - Planning & Optimization**
- RFC-0006: Logical Planning & Rewrite Rules
- RFC-0007: Cost Model & Execution Mode Selection

**Draft RFCs - Execution & Runtime**
- RFC-0008: Physical Plan & Operator Interfaces
- RFC-0009: Indexes, Adjacency & Access Paths
- RFC-0010: Distributed & Parallel Execution
- RFC-0011: Runtime, Scheduling & Backpressure

**Draft RFCs - Storage & Data Management**
- RFC-0012: Storage & Persistence Layer
- RFC-0015: Schema, Typing & Evolution
- RFC-0016: Constraints & Integrity
- RFC-0017: Transactions, Mutations & Write Semantics

**Draft RFCs - AI & Semantic Layer**
- RFC-0013: Semantic Reasoning & Neurosymbolic Layer
- RFC-0014: Multi-Modal Data Processing

**Draft RFCs - API & Interfaces**
- RFC-0101: Python API (formerly 2_python_api_v0.1.md)

**Reserved**
- RFC-0004, RFC-0005: Reserved for future use

---

## Version History

### RFC-0001 Versions
- Base: RFC-0001 (Frozen 2026-01-21)

### RFC-0002 Versions
- Base: RFC-0002 (Frozen 2026-01-21)

### RFC-0003 Versions
- Base: RFC-0003 (Frozen 2026-01-21)

### RFC-0100 Versions
- Base: RFC-0100 (Frozen 2026-01-22)

### rfc-namings.md Versions
- Base: rfc-namings.md (Frozen 2026-01-21)

---

## Recording New Events

### Template

```markdown
### YYYY-MM-DD

**RFC-NNNN: [Event Type]**
- [Brief description of change]
- Author: [Name]
- Rationale: [Why]
```

### Examples

**Status Change (Draft → Frozen)**
```markdown
**RFC-0006: Status Change**
- Changed from Draft to Frozen
- Author: Core Team
- Rationale: Implementation starting, need stable reference
```

**Version Update**
```markdown
**RFC-0001: Version Update**
- Created rfc-0001-001.md
- Changes: Updated Section 5 projection semantics
- Author: Grism Team
- Rationale: Clarified ambiguity found during implementation
```

**New RFC Created**
```markdown
**RFC-0018: Created**
- Title: Vector Indexing & Similarity Search
- Status: Draft
- Dependencies: RFC-0009, RFC-0012
- Author: AI Team
```

---

Last Updated: 2026-01-22
