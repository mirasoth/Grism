# RFC History

Chronological record of RFC lifecycle events: creation, status changes, and version updates.

---

## History Log

### 2026-01-23

**RFC-0103: Created**
- Title: Standalone Storage Architecture
- Status: Draft
- Dependencies: RFC-0012, RFC-0019, RFC-0020, RFC-0102
- Author: Grism Team
- Rationale: Define unified StorageProvider architecture for local engine with memory, Lance, and tiered storage modes. Production-ready implementation reference.

**rfc-index.md: Updated**
- Added RFC-0103 to API & Interfaces table
- Added RFC-0103 to dependency graph
- Added RFC-0103 to "RFC by Layer" section
- Author: Grism Team
- Rationale: Keep index synchronized with new RFC

---

**RFC-0019: Created**
- Title: Lance-Based Local Storage Backend
- Status: Draft
- Dependencies: RFC-0009, RFC-0012, RFC-0018, RFC-0102
- Author: Grism Team
- Rationale: Define concrete Lance-based implementation of storage abstractions for local filesystem

**RFC-0020: Created**
- Title: In-Memory Storage Backend
- Status: Draft
- Dependencies: RFC-0009, RFC-0012, RFC-0018, RFC-0102
- Author: Grism Team
- Rationale: Provide non-persistent, low-latency storage for testing and prototyping

**RFC-0021: Created**
- Title: Cloud / ObjectStore Lance Backend
- Status: Draft
- Dependencies: RFC-0009, RFC-0012, RFC-0018, RFC-0019, RFC-0102
- Author: Grism Team
- Rationale: Extend Lance backend for cloud object stores (S3, GCS, Azure)

**RFC-0022: Created**
- Title: Write & Mutation Semantics
- Status: Draft
- Dependencies: RFC-0012, RFC-0018, RFC-0019, RFC-0020, RFC-0021, RFC-0102
- Author: Grism Team
- Rationale: Define backend-agnostic, snapshot-oriented mutation model

**RFC-0023: Created**
- Title: Index Materialization Semantics
- Status: Draft
- Dependencies: RFC-0009, RFC-0012, RFC-0018, RFC-0022, RFC-0102
- Author: Grism Team
- Rationale: Specify index lifecycle and materialization timing during write path

**RFC-0024: Created & Updated**
- Title: Physical Planning Rules
- Status: Draft
- Dependencies: RFC-0009, RFC-0012, RFC-0018, RFC-0022, RFC-0023, RFC-0102
- Author: Grism Team
- Rationale: Define how logical plans become backend-aware physical plans
- Update: Aligned PhysicalPlan model with RFC-0102 (root/properties instead of nodes/edges)

**rfc-index.md: Updated**
- Added RFCs 0019-0024 to index tables
- Updated dependency graph with new RFC relationships
- Updated "RFC by Layer" section
- Author: Grism Team
- Rationale: Keep index synchronized with new RFCs

---

**RFC-0018: Created**
- Title: Persistent Storage & Adjacency Layout
- Status: Draft
- Dependencies: RFC-0008, RFC-0009, RFC-0012, RFC-0102
- Author: Grism Team
- Rationale: Formalize persistent storage layout for nodes, hyperedges, and adjacency structures; complete the storage foundation for graph-native execution

**Cross-references added:**
- RFC-0009 §6: Added reference to RFC-0018 for adjacency persistence
- RFC-0012 §5.2: Added reference to RFC-0018 for fragment layout specifications

---

**Cross-RFC Consistency Audit & Alignment**

Performed comprehensive consistency audit across RFC-0008, RFC-0009, RFC-0012, and RFC-0102. Resolved 14 consistency issues and applied polish edits for long-term stability.

**RFC-0008: Status Change & Major Updates**
- Status: Frozen → Review
- Updated operator interface from `open/next/close` lifecycle to `execute() → RecordBatchStream` (aligned with RFC-0102)
- Updated ExecutionContext to include `storage()`, `snapshot_id()` access
- Marked `MaterializeHyperedgeExec` as deferred (moved to Open Questions)
- Author: Grism Team
- Rationale: Align abstract contract with implementation reference (RFC-0102)

**RFC-0009: Status Change & Polish**
- Status: Draft → Review
- §4.1: Added clarification that access paths exclude distribution operators (ExchangeExec)
- §7.2: Updated snapshot consistency to reference RFC-0012 authority
- §8: Added note that index usage does not imply distinct physical operator
- Author: Grism Team
- Rationale: Cross-RFC terminology alignment and future-proofing

**RFC-0012: Cross-Reference Update**
- Updated non-goals to reference both RFC-0008 and RFC-0102
- Author: Grism Team
- Rationale: Correct authoritative references for physical operators

**RFC-0102: Capability Extension & Polish**
- Extended `OperatorCaps` with `scan_caps: Option<ScanCaps>` for pushdown capabilities
- §7.5: Clarified blocking operator reference to RFC-0008
- §9.1: Added note that adjacency partitioning is orthogonal to adjacency access paths
- §15: Added open question about distributed approximate operators (vector search top-K)
- Author: Grism Team
- Rationale: Reconcile capability models and clarify terminology boundaries

**rfc-index.md: Dependency & Status Corrections**
- Added "Review" status to RFC Status Legend
- Updated RFC-0008, RFC-0009, RFC-0012, RFC-0102 statuses to Review
- Fixed RFC-0009 dependencies: added RFC-0012, RFC-0102
- Fixed RFC-0012 dependencies: RFC-0002, RFC-0008, RFC-0010, RFC-0100, RFC-0102
- Corrected dependency graph edges for RFC-0009 and RFC-0012
- Author: Grism Team
- Rationale: Sync index with actual RFC documents

---

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

Last Updated: 2026-01-23
