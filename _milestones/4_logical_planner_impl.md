# Grism Logical Layer Implementation

**Module:** `grism-logical`
**Layer:** Canonical Logical IR + Rewrite System
**Status:** Week 2 Complete (grism-core), Week 3 In Progress (grism-logical)

---

## 1. Current Progress Status

### Week 2 Complete ✅ (grism-core)
- **Hypergraph data model**: Node, Hyperedge, Edge types with role bindings
- **Schema system**: PropertySchema, SchemaViolation, validation with strict/non-strict modes
- **Type system**: DataType, Value with Arrow integration
- **Property-based testing**: 13 proptests covering serialization, type coercion, invariants

### Week 3 Target (grism-logical)
By the end of Week 3, `grism-logical` MUST provide:

1. **Canonical Logical IR** ✅ *Partially implemented*
2. **Hypergraph-first operator semantics** ✅ *Partially implemented*
3. **Expression system (typed, deterministic)** ✅ *Partially implemented*
4. **Logical plan DAG** ✅ *Basic implementation*
5. **Rewrite framework + core rewrite rules** ⚠️ *Framework exists, rules partial*
6. **Explainability hooks** ✅ *Basic explain()*
7. **Python-facing logical surface (no execution)** ❌ *Not implemented*

### Out of Scope (Future Weeks)

* Physical execution (grism-engine, Week 4)
* Storage integration (grism-storage, Week 5)
* Cost model (Week 6)
* Distributed runtime (grism-distributed, Week 7)

---

## 2. Implementation Status by Module

### grism-core (Week 2 ✅ Complete)

```text
grism-core/src/
├─ lib.rs                    # Main exports, documentation
├─ hypergraph/               # Core data model
│  ├─ container.rs          # Hypergraph, Node, Hyperedge, Edge
│  ├─ node.rs               # Node implementation
│  ├─ hyperedge.rs          # Hyperedge, RoleBinding, HyperedgeBuilder
│  ├─ edge.rs               # Edge (binary hyperedge view)
│  ├─ properties.rs         # PropertyMap, PropertyKey
│  ├─ identifiers.rs        # NodeId, HyperedgeId, EdgeId
│  └─ mod.rs
├─ schema/                   # Schema validation system
│  ├─ schema.rs             # Schema, PropertySchema, SchemaViolation
│  ├─ column_ref.rs         # Column reference resolution
│  ├─ entity.rs             # EntityInfo, EntityKind
│  └─ mod.rs
├─ types/                    # Type system
│  ├─ data_type.rs          # DataType enum with Arrow integration
│  ├─ value.rs              # Value enum with serialization
│  ├─ spec.rs               # Type specifications
│  └─ mod.rs
└─ proptest_utils.rs        # Property-based testing (13 tests)
```

### grism-logical (Week 3 🔄 In Progress)

```text
grism-logical/src/
├─ lib.rs                    # Main exports ✅
├─ plan.rs                   # LogicalPlan struct ✅
├─ expr/                     # Expression system
│  ├─ mod.rs                # Exports ✅
│  ├─ expr.rs               # LogicalExpr, UnaryOp ✅
│  ├─ binary.rs             # BinaryOp ✅
│  ├─ agg.rs                # AggExpr, AggFunc ✅
│  └─ func.rs               # FuncExpr ✅
├─ ops/                      # Logical operators
│  ├─ mod.rs                # LogicalOp enum ✅
│  ├─ scan.rs               # ScanOp ✅
│  ├─ expand.rs             # ExpandOp ✅
│  ├─ filter.rs             # FilterOp ✅
│  ├─ project.rs            # ProjectOp ✅
│  ├─ aggregate.rs          # AggregateOp ✅
│  ├─ limit.rs              # LimitOp ✅
│  └─ infer.rs              # InferOp ✅
└─ error.rs                  # Error types (planned)
```

### grism-optimizer (Week 3 🔄 Partial)

```text
grism-optimizer/src/
├─ lib.rs                    # Main optimize() function ✅
└─ rules/
   ├─ mod.rs                # Exports ✅
   ├─ optimizer.rs          # Optimizer trait, default rules ✅
   ├─ predicate_pushdown.rs # PredicatePushdown rule ✅
   └─ projection_pushdown.rs # ProjectionPushdown rule ✅
   ├─ projection_prune.rs   # Planned
   ├─ expand_reorder.rs     # Planned
   ├─ filter_expand_fusion.rs # Planned
   └─ constant_folding.rs   # Planned
```

---

## 3. Foundational Types (From grism-core)

### Core Identifiers

```rust
// grism-core/src/hypergraph/identifiers.rs
pub type NodeId = u64;
pub type HyperedgeId = u64;
pub type EdgeId = (NodeId, HyperedgeId, NodeId); // (source, edge, target)
```

### Role and Label Types

```rust
// grism-core/src/hypergraph/mod.rs
pub struct Role(pub String);
pub type Label = String; // Node/Edge/Hyperedge labels

// Role constants (following RFC-namings)
pub const ROLE_SOURCE: &str = "source";
pub const ROLE_TARGET: &str = "target";
```

### Schema System

```rust
// grism-core/src/schema/schema.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropertySchema {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub required: bool,
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub properties: HashMap<String, PropertySchema>,
    pub entities: HashMap<String, EntityInfo>,
}
```

**Key Invariants:**
- `Schema` integrates **entity info** + **property schemas**
- Every logical operator carries a `Schema` for type checking
- Schema validation supports **strict mode** (reject undeclared properties) vs **permissive mode**

---

## 4. Expression System (RFC-0003 Compliant)

### Current Implementation

```rust
// grism-logical/src/expr/mod.rs
pub enum LogicalExpr {
    Literal(Value),
    Column(String),
    Binary { left: Box<LogicalExpr>, op: BinaryOp, right: Box<LogicalExpr> },
    Unary { op: UnaryOp, expr: Box<LogicalExpr> },
    Function(FuncExpr),
    Aggregate(AggExpr),
}
```

### Expression Types Implemented

#### Binary Operations
```rust
// grism-logical/src/expr/binary.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    // Arithmetic
    Add, Subtract, Multiply, Divide, Modulo,

    // Comparison
    Eq, NotEq, Lt, LtEq, Gt, GtEq,

    // Logical
    And, Or,

    // String
    Concat,
}
```

#### Aggregate Functions
```rust
// grism-logical/src/expr/agg.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Min,
    Max,
    Avg,
    CountDistinct,
}

pub struct AggExpr {
    pub func: AggFunc,
    pub expr: Box<LogicalExpr>,
    pub distinct: bool,
}
```

#### Function Calls
```rust
// grism-logical/src/expr/func.rs
pub struct FuncExpr {
    pub name: String,
    pub args: Vec<LogicalExpr>,
}
```

**Implementation Status:**
- ✅ **Typed**: All expressions integrate with `DataType` from grism-core
- ✅ **Deterministic**: Determinism tracking planned for future
- ⚠️ **No evaluation**: Expressions are logical-only, no runtime evaluation
- ❌ **Schema integration**: Column reference resolution needs `Schema` context

---

## 5. Frame System (Planned - Week 3 Late)

**Status**: Not yet implemented. Planned as user-facing logical views.

### Design Intent

Frames provide **hypergraph-first query surface**:

```rust
// Planned Python API (grism-python)
hg = Hypergraph()

# Node-centric queries
nodes = hg.nodes()
  .filter(col("year") > 2020)
  .select("name", "title")

# Hyperedge-centric queries
edges = hg.hyperedges()
  .filter(col("weight") > 0.5)
  .expand(role="author")

# Edge views (binary projections)
binary_edges = hg.edges()
  .filter(col("relationship") == "KNOWS")
```

### Implementation Plan

```rust
// Planned: src/frame/mod.rs
pub struct NodeFrame {
    pub plan: LogicalPlan,
}

pub struct HyperedgeFrame {
    pub plan: LogicalPlan,
}

pub struct EdgeFrame {
    pub plan: LogicalPlan, // Binary projection
}
```

**Key Points:**
- Frames are **logical only** - no execution
- Frames **compile to** `LogicalPlan` DAGs
- Python API builds frames, Rust gets plans

---

## 6. Logical Operators (Implemented)

### Operator Enum

```rust
// grism-logical/src/ops/mod.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalOp {
    Scan(ScanOp),
    Expand(ExpandOp),
    Filter(FilterOp),
    Project(ProjectOp),
    Aggregate(AggregateOp),
    Limit(LimitOp),
    Infer(InferOp),
}
```

---

### Scan (✅ Implemented)

```rust
// grism-logical/src/ops/scan.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScanKind {
    Node,
    Hyperedge,
    Edge,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanOp {
    pub kind: ScanKind,
    pub label: Option<String>,
    pub namespace: Option<String>,
}
```

---

### Expand (✅ Implemented - Graph Traversal)

```rust
// grism-logical/src/ops/expand.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Direction {
    Outgoing,
    Incoming,
    Undirected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpandOp {
    pub input: Box<LogicalOp>,
    pub edge_label: Option<String>,
    pub to_label: Option<String>,
    pub direction: Direction,
    pub hops: u32,
}
```

**Hypergraph Semantics:**
- Expand traverses **hyperedges**, not just binary edges
- Direction applies to **role bindings** (source/target generalize)
- No explicit Join - Expand is the composition primitive

---

### Filter (✅ Implemented)

```rust
// grism-logical/src/ops/filter.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterOp {
    pub input: Box<LogicalOp>,
    pub predicate: LogicalExpr,
}
```

---

### Project (✅ Implemented)

```rust
// grism-logical/src/ops/project.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectOp {
    pub input: Box<LogicalOp>,
    pub columns: Vec<String>,
}
```

---

### Aggregate (✅ Implemented)

```rust
// grism-logical/src/ops/aggregate.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateOp {
    pub input: Box<LogicalOp>,
    pub keys: Vec<String>,
    pub aggs: Vec<(String, AggExpr)>,
}
```

---

## 7. Logical Plan DAG (Basic Implementation)

### Current Structure

```rust
// grism-logical/src/plan.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalPlan {
    pub root: LogicalOp,
    pub schema: Option<Schema>,
}
```

### Key Methods

```rust
impl LogicalPlan {
    pub fn new(root: LogicalOp) -> Self;
    pub fn with_schema(root: LogicalOp, schema: Schema) -> Self;
    pub fn root(&self) -> &LogicalOp;
    pub fn schema(&self) -> Option<&Schema>;
    pub fn explain(&self) -> String; // Tree visualization
}
```

### Planned Invariants (Validation Layer)

**To be implemented:**
- ✅ DAG structure (acyclic - enforced by tree structure)
- ❌ Column scope correctness (needs schema integration)
- ❌ Type correctness (needs expression type checking)
- ❌ Determinism preservation (needs expression determinism)

**Current Limitations:**
- Plans are **trees, not DAGs** (single input per operator)
- Schema is **optional** (should be required)
- No validation layer yet

---

## 8. Rewrite Framework (Partial Implementation)

### Current Rule Interface

```rust
// grism-optimizer/src/rules/optimizer.rs
pub trait OptimizationRule: Send + Sync {
    fn name(&self) -> &'static str;
    fn apply(&self, plan: LogicalPlan) -> GrismResult<LogicalPlan>;
}
```

### Optimizer Implementation

```rust
// grism-optimizer/src/rules/optimizer.rs
pub struct Optimizer {
    rules: Vec<Box<dyn OptimizationRule>>,
    max_iterations: usize,
}

impl Optimizer {
    pub fn optimize(&mut self, plan: LogicalPlan) -> GrismResult<LogicalPlan> {
        // Fixed-point iteration until no changes
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new(vec![
            Box::new(PredicatePushdown),
            Box::new(ProjectionPushdown),
        ])
    }
}
```

### Implemented Rules

| Rule | Status | File | Description |
|------|--------|------|-------------|
| **PredicatePushdown** | ✅ | `rules/predicate_pushdown.rs` | Push filters down the plan tree |
| **ProjectionPushdown** | ✅ | `rules/projection_pushdown.rs` | Push projections down, prune unused columns |

### Planned Rules (Week 3 Target)

| Rule | Priority | Description |
|------|----------|-------------|
| **ProjectionPrune** | REQUIRED | Remove unused column projections |
| **ConstantFolding** | REQUIRED | Evaluate constant expressions at plan time |
| **ExpandReorder** | REQUIRED | Reorder independent expand operations |
| **FilterExpandFusion** | REQUIRED | Fuse filter conditions into expand operations |
| **LimitPushdown** | OPTIONAL | Push limit operations down the tree |

**Rule Requirements:**
- ✅ Prove semantic equivalence (logical correctness)
- ❌ Reject illegal rewrites (validation needed)
- ❌ Emit trace metadata (explainability planned)

---

## 9. Expand-Specific Semantics

**Current Implementation Notes:**

1. **Binary vs Role Expansion**: Current `ExpandOp` supports edge labels, not explicit role binding
2. **Reordering**: Independent expands can be reordered (commutative)
3. **Hyperedge Materialization**: Not yet implemented (binary edges only)
4. **Role Preservation**: Role semantics need deeper integration with grism-core

**Planned for Week 3:**
- Role-based expansion with `RoleBinding` integration
- Hyperedge materialization flags
- Expand reordering rules

---

## 10. Explainability (Basic Implementation)

### Current Plan Visualization

```rust
// grism-logical/src/plan.rs
impl LogicalPlan {
    pub fn explain(&self) -> String {
        self.root.explain(0) // Tree format
    }
}
```

**Sample Output:**
```
Scan(Node, label=Person)
└── Filter(predicate)
    └── Project(2 columns)
```

### Planned Trace System

```rust
// Planned: grism-optimizer integration
pub struct RewriteTrace {
    pub rule_name: String,
    pub before_plan: String,  // explain() output
    pub after_plan: String,
    pub applied_at: std::time::Instant,
}

pub struct OptimizedPlan {
    pub final_plan: LogicalPlan,
    pub trace: Vec<RewriteTrace>,
    pub iterations: usize,
}
```

**Status**: Basic explainability exists, full trace system planned.

---

## 11. Python Surface (Planned - Week 3 Late)

### Design Intent

Python API provides **fluent, hypergraph-first** query interface:

```python
# Planned: grism-python integration
from grism import Hypergraph

hg = Hypergraph()

# Node-centric queries
papers = (hg.nodes(label="Paper")
    .expand(edge_label="AUTHORED_BY", role="paper")
    .filter(col("year") > 2020)
    .select("title", "venue"))

# Hyperedge queries
authorships = (hg.hyperedges(label="AUTHORED_BY")
    .filter(col("confidence") > 0.8)
    .expand(role="author")
    .aggregate(avg("h_index"), by=["venue"]))

# Binary edge projections
knows_edges = (hg.edges(label="KNOWS")
    .filter(col("strength") > 0.5))
```

### Implementation Plan

1. **Python Bindings** (`grism-python/`): PyO3 wrappers for logical types
2. **Fluent API**: Chain operations build `LogicalPlan`
3. **Frame System**: `NodeFrame`, `HyperedgeFrame`, `EdgeFrame` classes
4. **No Execution**: Python only builds plans, execution in Rust

**Status**: Not yet implemented. Depends on grism-core Python bindings.

---

## 12. Week-3 Exit Criteria (Updated)

Week 3 is **complete** when grism-logical provides:

### ✅ Already Implemented
* ✅ **Basic LogicalPlan** structure with tree ops
* ✅ **Core operators**: Scan, Expand, Filter, Project, Aggregate, Limit, Infer
* ✅ **Expression system**: Binary ops, aggregates, functions, literals
* ✅ **Basic optimizer** with 2 rewrite rules (PredicatePushdown, ProjectionPushdown)
* ✅ **Plan explainability** (tree visualization)
* ✅ **Integration** with grism-core types and schema system

### 🔄 In Progress / Planned
* 🔄 **Schema integration**: All operators need Schema validation
* 🔄 **DAG support**: Convert tree structure to true DAG
* 🔄 **Type checking**: Expression type validation against Schema
* 🔄 **Missing rewrite rules**: 4 more rules needed (ProjectionPrune, ConstantFolding, etc.)
* 🔄 **Trace system**: Rewrite rule application logging
* 🔄 **Frame system**: Python-facing NodeFrame/HyperedgeFrame/EdgeFrame
* 🔄 **Python bindings**: Fluent API for logical plan construction

### ❌ Out of Scope
* ❌ **Physical execution** (Week 4: grism-engine)
* ❌ **Storage** (Week 5: grism-storage)
* ❌ **Cost model** (Week 6)
* ❌ **Distributed runtime** (Week 7: grism-distributed)

### Success Criteria
Week 3 passes when:
- All logical operators integrate with grism-core Schema
- Optimizer applies rules correctly without breaking semantics
- Python can construct logical plans (even if execution fails)
- All code follows RFC-namings and AGENTS.md layer boundaries
