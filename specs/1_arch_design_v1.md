# Grism – Full Architecture Design Document (Rust + Python + Ray)

> **Grism** is an **AI-native, neurosymbolic, hypergraph database system** designed for modern agentic and LLM-driven workflows. It treats graphs as **executable knowledge**, not just queryable data, and provides a **Python-first user experience** with a **Rust-native core** and **pluggable execution backends (local or Ray-distributed)**.

---

## 1. Design Goals

### 1.1 Primary Goals

* **AI-native first-class system** (memory, reasoning, planning)
* **Hypergraph-first data model** (property graphs as a subset)
* **Python DSL as the main user interface** (no mandatory query language)
* **Single logical engine, multiple execution backends**
* **Composable, lazy, explainable execution**
* **Rust safety + Arrow/Lance performance**

### 1.2 Explicit Non-Goals

* OLTP replacement
* Row-oriented storage engine
* SQL-first UX

---

## 2. High-Level Architecture

```
┌──────────────────────────────────────────┐
│          User / Agent Layer              │
│  Python KG DSL · LLMs · LangGraph        │
└───────────────▲─────────────────────────┘
                │
┌───────────────┴─────────────────────────┐
│        Graph Expression & Planning       │
│  Python Expr → Rust Logical Plan         │
└───────────────▲─────────────────────────┘
                │
┌───────────────┴─────────────────────────┐
│        Optimization & Physical Plan      │
│  Local Planner | Ray Planner             │
└───────────────▲─────────────────────────┘
                │
┌───────────────┴─────────────────────────┐
│        Execution Backends                │
│  Standalone Rust | Ray-Orchestrated      │
└───────────────▲─────────────────────────┘
                │
┌───────────────┴─────────────────────────┐
│        Storage & Index Layer             │
│  Lance · Arrow · Vector Index            │
└─────────────────────────────────────────┘
```

---

## 3. Data Model

### 3.1 Unified Hypergraph Model

```rust
struct HyperEdge {
    id: EdgeId,
    endpoints: Vec<NodeId>,
    roles: HashMap<NodeId, Role>,
    label: Symbol,
    properties: PropertyMap,
}
```

* Binary edges = property graph compatibility
* N-ary edges = events, predicates, experiments
* Reification is native

### 3.2 Values & Types

```rust
enum Value {
    Int(i64), Float(f64), Bool(bool),
    String(String), Binary(Vec<u8>),
    Vector(Embedding), Symbol(SymbolId),
}
```

---

## 4. Python-First KnowledgeGraph API

### 4.1 Core Object

```python
kg = KnowledgeGraph.connect("hyphadb://local")
```

Properties:

* Immutable
* Lazy
* Typed

### 4.2 NodeFrame / EdgeFrame

```python
papers = kg.nodes("Paper")
authors = kg.nodes("Author")
```

### 4.3 Core Operations

```python
(
  kg.nodes("Paper")
    .filter(col("year") >= 2022)
    .expand("AUTHORED_BY", to="Author")
    .filter(col("Author.affiliation") == "MIT")
    .select("Paper.title", "Author.name")
)
```

Graph primitives:

* `filter`
* `select`
* `expand`
* `groupby / agg`
* `infer`

---

## 5. Expression System

Expressions are composable trees:

```python
sim(col("embedding"), query_emb) > 0.8
```

Lowered to:

* GraphExpr IR (Python)
* LogicalExpr (Rust)

---

## 6. Logical Plan Layer (Rust Canonical)

```rust
enum LogicalOp {
    Scan,
    Expand,
    Filter,
    Project,
    Join,
    Aggregate,
    Infer,
}
```

Properties:

* Execution-agnostic
* Deterministic
* Serializable

---

## 7. Optimization

### 7.1 Rule-Based

* Predicate pushdown
* Expand reordering
* Projection pruning

### 7.2 Cost-Based

* Cardinality estimates
* Vector selectivity
* Ray shuffle cost

---

## 8. Execution Backends

### 8.1 Standalone Rust Engine

**Model**:

* Vectorized
* Async (Tokio)
* Arrow RecordBatch

```rust
trait ExecNode {
    async fn next(&mut self) -> Option<RecordBatch>;
}
```

Parallelism:

* Rayon (CPU)
* Arrow kernels

---

### 8.2 Ray Distributed Engine

**Principle**: Ray orchestrates, Rust executes.

#### Ray Flow

```
Logical Plan
 → Ray Physical Planner
 → Ray DAG (stages)
 → Rust Workers
 → Arrow Results
```

#### Ray Tasks

```python
@ray.remote
def execute_stage(fragment, inputs):
    return rust_worker.execute(fragment, inputs)
```

Data transport:

* Arrow IPC
* Ray Plasma store

---

## 9. Storage Layer

### 9.1 Lance Datasets

```
/datasets/
  nodes.lance
  edges.lance
  hyperedges.lance
  properties.lance
  embeddings.lance
```

Features:

* Append-only
* Snapshot isolation
* Time travel

---

## 10. Indexing

### 10.1 Structural

* Adjacency lists
* Role-based indexes
* Label bitmaps

### 10.2 Vector

* Lance ANN
* HNSW

---

## 11. Reasoning & Neurosymbolic Layer

### 11.1 Ontologies

* OWL / RDFS via `horned-owl`
* Type inference

### 11.2 Rule Engine

* Datalog-style rules
* Fixpoint execution
* Graph materialization

---

## 12. AI & Agent Integration

```rust
trait CognitiveStore {
    fn remember(&self, event: Thought);
    fn recall(&self, intent: Query) -> Subgraph;
}
```

Use cases:

* Long-term agent memory
* Planning state
* Tool grounding

---

## 13. APIs & Interfaces

* Python SDK (primary)
* gRPC / Arrow Flight
* Optional GQL / Cypher (debug / interop)

```python
kg.explain(mode="logical")
kg.explain(mode="gql")
```

---

## 14. Transactions & Versioning

* MVCC via Lance snapshots
* Branchable graph states
* Deterministic replay

---

## 15. Security & Governance

* Label-based access control
* Subgraph isolation
* Provenance tracking

---

## 16. Rust & Python Crate Layout

```
hyphadb/
├── hyphadb-core        # graph model, values
├── hyphadb-logical     # logical plan & algebra
├── hyphadb-optimizer
├── hyphadb-engine     # local execution
├── hyphadb-ray        # Ray planner & workers
├── hyphadb-storage    # Lance integration
├── hyphadb-reasoning  # logic & ontology
├── hyphadb-python     # Python DSL (pyo3)
```

---

## 17. Roadmap

1. Hypergraph core + Python DSL
2. Local execution engine
3. Ray backend
4. Reasoning & inference
5. Agent-native memory

---

## Phase 0 – Python API Contract (Frozen v0.1)

> This section defines the **exact Python class and method surface** for Grism. This API is considered the **user contract** and should remain backward-compatible within v0.x.

### 0.1 Core Entry Point

```python
class KnowledgeGraph:
    @staticmethod
    def connect(uri: str, *, executor: "Executor | str" = "local") -> "KnowledgeGraph":
        ...

    # namespace / logical graph
    def with_namespace(self, name: str) -> "KnowledgeGraph":
        ...

    # graph views
    def nodes(self, label: str | None = None) -> "NodeFrame":
        ...

    def edges(self, label: str | None = None) -> "EdgeFrame":
        ...

    def hyperedges(self, label: str | None = None) -> "HyperEdgeFrame":
        ...

    # execution
    def collect(self, *, executor: "Executor | str | None" = None):
        ...

    def explain(self, mode: str = "logical") -> str:
        ...
```

---

### 0.2 Frame Base Class

```python
class GraphFrame:
    # structural ops
    def filter(self, predicate: "Expr") -> "Self":
        ...

    def select(self, *columns: str | "Expr") -> "Self":
        ...

    def limit(self, n: int) -> "Self":
        ...

    # grouping
    def groupby(self, *keys: str | "Expr") -> "GroupedFrame":
        ...

    # execution
    def collect(self, *, executor: "Executor | str | None" = None):
        ...

    def explain(self, mode: str = "logical") -> str:
        ...
```

---

### 0.3 NodeFrame

```python
class NodeFrame(GraphFrame):
    label: str | None

    def expand(
        self,
        edge: str | None = None,
        *,
        to: str | None = None,
        direction: str = "out",  # in | out | both
        hops: int = 1,
        as_: str | None = None,
    ) -> "NodeFrame":
        ...
```

Semantics:

* No joins
* Expansion is the primitive
* Supports multi-hop traversal

---

### 0.4 EdgeFrame

```python
class EdgeFrame(GraphFrame):
    label: str | None

    def endpoints(self) -> "NodeFrame":
        ...
```

---

### 0.5 HyperEdgeFrame

```python
class HyperEdgeFrame(GraphFrame):
    label: str | None

    def where_role(self, role: str, value: str | "NodeFrame") -> "HyperEdgeFrame":
        ...
```

---

### 0.6 GroupedFrame

```python
class GroupedFrame:
    def agg(self, **aggregations: "AggExpr") -> "GraphFrame":
        ...
```

---

### 0.7 Expression System (Public API)

```python
class Expr:
    def __and__(self, other: "Expr") -> "Expr": ...
    def __or__(self, other: "Expr") -> "Expr": ...
    def __eq__(self, other) -> "Expr": ...
    def __gt__(self, other) -> "Expr": ...
    def __ge__(self, other) -> "Expr": ...
```

Helpers:

```python
def col(name: str) -> Expr: ...
def lit(value) -> Expr: ...
def sim(left: Expr, right) -> Expr: ...
```

---

### 0.8 Aggregations

```python
class AggExpr: ...

def count(expr: Expr | None = None) -> AggExpr: ...
def sum(expr: Expr) -> AggExpr: ...
def avg(expr: Expr) -> AggExpr: ...
def min(expr: Expr) -> AggExpr: ...
def max(expr: Expr) -> AggExpr: ...
```

---

### 0.9 Mutation API (Explicit)

```python
class KnowledgeGraph:
    def insert_node(self, label: str, properties: dict) -> None: ...
    def insert_edge(self, label: str, src, dst, properties: dict | None = None) -> None: ...
    def insert_hyperedge(self, label: str, roles: dict, properties: dict | None = None) -> None: ...
```

---

### 0.10 Execution Backends

```python
class Executor:
    name: str

class LocalExecutor(Executor): ...
class RayExecutor(Executor):
    def __init__(self, **ray_config): ...
```

---

### 0.11 Canonical Usage Example

```python
kg = KnowledgeGraph.connect("grism://local")

result = (
    kg.nodes("Paper")
      .filter(col("year") >= 2022)
      .expand("CITES")
      .filter(sim(col("embedding"), query_emb) > 0.8)
      .select("title")
      .limit(10)
      .collect()
)
```

---

### Phase 0 Completion Criteria

* Python API frozen
* 10 reference examples
* LogicalPlan lowering validated
* No execution logic in Python layer

---

## Phase 1.1 – Canonical Rust LogicalPlan (Frozen v0.1)

> This section defines the **canonical Rust logical representation** for Grism. All execution backends (local, Ray, future engines) must consume this plan **without semantic loss**.

---

### 1.1.1 Design Principles

* Execution-agnostic
* Deterministic & replayable
* Serializable (Serde)
* Graph-native (no SQL bias)
* Expression trees are immutable

---

### 1.1.2 Core Identifiers

```rust
pub type NodeId = u64;
pub type EdgeId = u64;
pub type Label = String;
pub type Role = String;
pub type Column = String;
```

---

### 1.1.3 LogicalPlan Root

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogicalPlan {
    pub root: LogicalOp,
}
```

---

### 1.1.4 Logical Operators

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
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

Each operator:

* Has exactly **one input** (except Scan)
* Forms a strict DAG (tree in v0.1)

---

### 1.1.5 ScanOp

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScanOp {
    pub kind: ScanKind,
    pub label: Option<Label>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ScanKind {
    Node,
    Edge,
    HyperEdge,
}
```

Semantics:

* Entry point of all plans
* No filtering here (pushdown happens later)

---

### 1.1.6 ExpandOp (Graph Primitive)

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExpandOp {
    pub input: Box<LogicalOp>,
    pub edge_label: Option<Label>,
    pub to_label: Option<Label>,
    pub direction: Direction,
    pub hops: u32,
    pub alias: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Direction {
    In,
    Out,
    Both,
}
```

Invariants:

* `Expand` replaces joins
* Multi-hop is explicit
* Alias introduces a new binding scope

---

### 1.1.7 FilterOp

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FilterOp {
    pub input: Box<LogicalOp>,
    pub predicate: LogicalExpr,
}
```

---

### 1.1.8 ProjectOp

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProjectOp {
    pub input: Box<LogicalOp>,
    pub columns: Vec<LogicalExpr>,
}
```

---

### 1.1.9 AggregateOp

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggregateOp {
    pub input: Box<LogicalOp>,
    pub keys: Vec<LogicalExpr>,
    pub aggs: Vec<AggExpr>,
}
```

---

### 1.1.10 LimitOp

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LimitOp {
    pub input: Box<LogicalOp>,
    pub limit: usize,
}
```

---

### 1.1.11 InferOp (Reasoning Placeholder)

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InferOp {
    pub input: Box<LogicalOp>,
    pub rule_set: String,
}
```

---

### 1.1.12 Expression System

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LogicalExpr {
    Column(ColumnRef),
    Literal(Value),
    Binary {
        left: Box<LogicalExpr>,
        op: BinaryOp,
        right: Box<LogicalExpr>,
    },
    Func(FuncExpr),
}
```

---

### 1.1.13 ColumnRef

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnRef {
    pub qualifier: Option<String>, // label or alias
    pub name: String,
}
```

---

### 1.1.14 BinaryOp

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BinaryOp {
    Eq, Neq, Gt, Gte, Lt, Lte,
    And, Or,
}
```

---

### 1.1.15 Function Expressions

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FuncExpr {
    pub name: String,
    pub args: Vec<LogicalExpr>,
}
```

Examples:

* `sim(a, b)`
* `contains(text, "LLM")`

---

### 1.1.16 Aggregation Expressions

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggExpr {
    pub func: AggFunc,
    pub expr: Option<LogicalExpr>,
    pub alias: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}
```

---

### 1.1.17 Serialization Guarantees

* `serde_json` for debugging
* `bincode` / Arrow IPC for execution
* Hash-stable for caching

---

### 1.1.18 Example Lowering (Python → Rust)

```python
kg.nodes("Paper") \
  .filter(col("year") >= 2022) \
  .expand("CITES") \
  .limit(10)
```

Lowered to:

```text
Limit
 └─ Expand(edge=CITES)
    └─ Filter(year >= 2022)
       └─ Scan(Node, Paper)
```

---

### Phase 1.1 Completion Criteria

* LogicalPlan fully specified
* Python lowering produces this plan
* Optimizer consumes this plan
* No backend-specific logic present

---

## 18. One-Sentence Summary

> **Grism exposes graphs as a lazy, typed Python object model whose only job is to express intent; all semantics live in the Rust logical engine.**
