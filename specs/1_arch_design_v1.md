# Grism – Full Architecture Design Document (Rust + Python + Ray)

> **Grism** is an **AI-native, neurosymbolic, hypergraph database system** designed for modern agentic and LLM-driven workflows. It treats graphs as **executable knowledge**, not just queryable data, and provides a **Python-first user experience** with a **Rust-native core** and **pluggable execution backends (local or Ray-distributed)**.

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

## 2. High-Level Architecture

```
┌──────────────────────────────────────────┐
│          User / Agent Layer              │
│  Python Hypergraph · LLMs · LangGraph   │
└───────────────▲─────────────────────────┘
                │
┌───────────────┴─────────────────────────┐
│        Graph Expression & Planning       │
│  Hypergraph → Rust Logical Plan          │
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

## 3. Data Model

## 3.1 Unified Hypergraph Data Model

### 3.1.1 Design Principle

Grism adopts a **single, canonical relational primitive: the *Hyperedge***. All relational structures—binary edges, n-ary relations, events, predicates, and meta-relations—are uniformly represented using this primitive.

A **Hyperedge** models an arbitrary n-ary relation whose participants are explicitly bound through **named roles**. Traditional binary edges are treated as a special case of hyperedges with arity equal to two. This approach ensures a uniform logical foundation while enabling multiple projection views (e.g., property graph, Cypher-compatible graphs) without altering core semantics.

This design establishes Grism as a **true hypergraph system**, while preserving efficient execution paths and compatibility with established graph query models.

### 3.1.2 Conceptual Entities

The Grism data model consists of exactly two first-class entity types:

| Entity        | Description                                                    |
| ------------- | -------------------------------------------------------------- |
| **Node**      | Atomic entities with stable identity and associated properties |
| **Hyperedge** | N-ary relations connecting entities via explicitly named roles |

There is no independent logical `Edge` abstraction. Instead:

> **Edge ≡ Hyperedge with arity = 2**, conventionally using the roles `source` and `target`.

This guarantees that all relational constructs are represented uniformly and can participate in higher-order relations.

### 3.1.3 Hyperedge Structure

Formally, a hyperedge is defined as a labeled relation with role bindings and properties:

```rust
struct Hyperedge {
    id: EdgeId,
    label: Label,
    roles: Vec<RoleBinding>,
    properties: PropertyMap,
}
```

Each role binding associates a semantic role with a target entity:

```rust
struct RoleBinding {
    role: Role,
    target: EntityRef,
}
```

Targets may reference either nodes or other hyperedges:

```rust
enum EntityRef {
    Node(NodeId),
    Hyperedge(EdgeId),
}
```

This recursive capability enables direct representation of reified relations, provenance chains, and inference traces.

### 3.1.4 Roles and Arity Semantics

Hyperedges must satisfy the following constraints:

* **Arity ≥ 2** (at least two role bindings)
* Roles are **explicitly named**, **unordered**, and **semantically meaningful**
* Role identity is independent of position

Example hyperedges:

```text
AUTHORED_BY(
  author → Person#123,
  paper  → Paper#456
)

EVENT(
  subject → AUTHORED_BY#77,
  time    → 2022-01-01,
  agent   → Model#gpt4
)
```

This structure natively supports:

* N-ary relations and events
* First-class reification
* Statements about relations
* Temporal, causal, and epistemic modeling

### 3.1.5 Binary Edge Projection

A binary edge is defined as a hyperedge that satisfies:

```text
|roles| = 2
roles = { source, target }
```

Example:

```text
CITES(
  source → Paper#1,
  target → Paper#2
)
```

Binary edges:

* Are logically indistinguishable from hyperedges
* May be physically optimized for adjacency traversal
* Are exposed to users via property-graph or `EdgeFrame` views

This separation of **logical semantics** and **physical layout** enables efficient traversal without compromising expressiveness.

### 3.1.6 Hyperedge-to-Hyperedge Relations

Because role targets are defined as `EntityRef`, hyperedges may directly reference other hyperedges. This allows the model to express higher-order and meta-relations such as:

* Provenance and justification
* Causal and temporal dependencies
* Rule application and inference confidence
* Planning and execution traces

Examples:

```text
INFERRED_BY(
  conclusion → Edge#42,
  rule       → Rule#R3,
  confidence → 0.91
)

CAUSES(
  cause  → Edge#17,
  effect → Edge#23
)
```

This capability is foundational for neurosymbolic reasoning, explainability, and agent memory.

### 3.1.7 Property Attachment Semantics

Properties may be attached to the following entities:

| Entity    | Property Semantics       |
| --------- | ------------------------ |
| Node      | Attributes of an entity  |
| Hyperedge | Attributes of a relation |

Properties are **not attached directly to roles**. Role-specific attributes must be modeled using:

* Dedicated hyperedges, or
* Structured property values (e.g., maps or nested objects)

This design preserves:

* Columnar storage efficiency
* Relational executability
* Clear separation between structure and metadata

### 3.1.8 Global Invariants

The following invariants are enforced across the system:

1. All relations are represented as hyperedges
2. Binary edges are a specialization, not a distinct primitive
3. Roles are explicit, named, and unordered
4. Hyperedges may reference other hyperedges
5. Physical optimizations never alter logical semantics

Together, these invariants provide a stable foundation for planning, optimization, reasoning, and distributed execution.

### 3.2 Values & Types

```rust
enum Value {
    Int(i64), Float(f64), Bool(bool),
    String(String), Binary(Vec<u8>),
    Vector(Embedding), Symbol(SymbolId),
    Null, // explicit null handling
    Timestamp(i64), // nanoseconds since epoch
    Date(i32), // days since epoch
}
```

### 3.3 Type System Semantics

* **Dynamic typing at Python DSL level**: Expressions are type-checked at execution time
* **Static typing at Rust logical level**: Type inference propagates through LogicalPlan
* **Type coercion rules**: Explicit conversions via `cast()` function; implicit coercion only for safe cases (e.g., `Int` → `Float`)
* **Null handling**: Three-valued logic (true, false, null) for comparisons; `is_null()` / `is_not_null()` predicates

## 3.2 Frames and Logical Views

### 3.2.1 Frame Abstraction

Grism exposes its data model to users through **Frames**, which are logical, immutable views over the underlying hypergraph. Frames provide a familiar, dataframe-like interface while preserving the full expressive power of the hyperedge model.

Frames are **not storage structures**. They are:

* Declarative
* Lazily evaluated
* Composable
* Backed by the logical planning system

All Frames ultimately compile into logical plans operating over Nodes and Hyperedges.

### 3.2.2 Core Frame Types

Grism defines the following canonical frame abstractions:

| Frame              | Description                                      |
| ------------------ | ------------------------------------------------ |
| **NodeFrame**      | A view over nodes matching a label and predicate |
| **HyperedgeFrame** | A view over hyperedges with arbitrary arity      |
| **EdgeFrame**      | A binary projection of `HyperedgeFrame`          |

Among these, **HyperedgeFrame is the primary relational view**. All other frame types are either projections or constrained specializations.

### 3.2.3 HyperedgeFrame

`HyperedgeFrame` represents a set of hyperedges selected by:

* Label
* Role constraints
* Property predicates
* Structural constraints (e.g. arity)

Conceptually, a HyperedgeFrame is equivalent to a relation with the following logical columns:

```text
(edge_id, label, role, entity_type, entity_id, properties...)
```

This representation is intentionally normalized to support:

* Relational optimization
* Predicate pushdown
* Join reordering
* Cost-based planning

### 3.2.4 EdgeFrame as a Projection

`EdgeFrame` is defined as a **restricted projection** of `HyperedgeFrame` satisfying:

```text
arity = 2
roles = { source, target }
```

Semantically:

> **EdgeFrame ≡ HyperedgeFrame WHERE arity = 2 AND roles ⊆ {source, target}**

EdgeFrame exists to:

* Preserve property-graph ergonomics
* Enable Cypher / GQL-style traversal
* Allow aggressive physical optimization

EdgeFrame does **not** introduce new semantics and cannot express relations beyond what is representable in HyperedgeFrame.

### 3.2.5 View Consistency Guarantees

The following guarantees hold:

1. Every EdgeFrame row corresponds to exactly one Hyperedge
2. Every Hyperedge with arity = 2 is representable in EdgeFrame
3. Transformations on EdgeFrame are lossless when lifted back to HyperedgeFrame

Thus, EdgeFrame is a **compatibility view**, not a semantic restriction.

## 3.3 Traversal and Expansion Semantics

### 3.3.1 Expand as a Logical Operation

Traversal in Grism is expressed via the **Expand** logical operator. Expand consumes a Frame and produces a new Frame by following role bindings in hyperedges.

Expand is defined over Hyperedges, not Nodes or Edges alone.

### 3.3.2 Binary Expansion (Property Graph Semantics)

For EdgeFrame, Expand follows conventional property-graph semantics:

```text
(NodeFrame) --Expand--> (NodeFrame)
```

Where:

* Expansion follows `source → target` or `target → source`
* Directionality is explicit
* Only binary hyperedges participate

This mode enables efficient adjacency-based traversal and is the default for Cypher-compatible queries.

### 3.3.3 Role-Qualified Expansion

For HyperedgeFrame, Expand may be **role-qualified**:

```text
Entity --Expand(role = r)--> Entity
```

This allows traversal across arbitrary roles and supports:

* N-ary relations
* Event-centric navigation
* Semantic joins

Example:

```text
(Person)
  --Expand(role = author)-->
(Paper)
```

### 3.3.4 Hyperedge Materialization

Expand may optionally materialize hyperedges as first-class outputs:

```text
Node --Expand(materialize = true)--> HyperedgeFrame
```

This enables:

* Inspection of relations as data
* Meta-reasoning over relations
* Provenance and explanation queries

Materialized hyperedges may subsequently participate in further expansions, including hyperedge-to-hyperedge traversal.

### 3.3.5 Execution Constraints and Optimization

To preserve performance and predictability:

* Binary Expand operations are preferentially mapped to adjacency indexes
* Role-qualified Expand operations may degrade to joins
* The planner may rewrite hyperedge expansions into binary projections when safe

These rewrites preserve logical semantics while selecting optimal physical execution strategies.

### 3.3.6 Summary

Traversal in Grism is unified under a single Expand abstraction operating over hyperedges. Property-graph traversal is a constrained, optimized case of this general mechanism, ensuring consistency between expressive power and execution efficiency.

## 4. Hypergraph: Canonical Abstraction

### 4.1 Definition and Scope

**`Hypergraph`** is the **canonical user-facing container** in Grism. It represents a **logical, executable view over a persistent hypergraph**, analogous to how `DataFrame` represents a logical view over tabular data.

Formally:

> A **Hypergraph** is a *hypergraph-backed, relationally executable, AI-native graph container* that exposes declarative operations while deferring execution to the planning and runtime layers.

The Hypergraph abstraction is:

* **Hypergraph-first** — n-ary relations (hyperedges) are native
* **Relation-centric** — operations compile to relational algebra over hyperedges
* **View-based** — immutable, lazy, and composable
* **Storage-agnostic** — backed by Lance via physical planning, not hard-wired

`Hypergraph` is **not** a storage engine, query language, or execution runtime. It is a **logical façade** over the Grism core.

### 4.2 Architectural Positioning

The Hypergraph abstraction occupies a precise position in the system stack:

| Layer           | Abstraction                                       |
| --------------- | ------------------------------------------------- |
| Python User API | `Hypergraph`                                      |
| Logical IR      | `LogicalPlan` over Nodes and Hyperedges           |
| Optimization    | Rule-based + Cost-based planners                  |
| Execution       | Standalone Rust Engine or Ray-Orchestrated Engine |
| Storage         | Lance / Arrow datasets and indexes                |

This separation ensures that:

* User-facing semantics remain stable
* Execution strategies can evolve independently
* Distributed and local execution share the same logical foundation

### 4.3 Mental Model: DataFrame Analogy

The Hypergraph API intentionally mirrors the **DataFrame mental model**, generalized from rows to relations:

| DataFrame Concept | Hypergraph Concept       |
| ----------------- | ------------------------ |
| Row               | Hyperedge                |
| Column            | Role or property         |
| Filter            | Sub-hypergraph selection |
| Join              | Hyperedge composition    |
| GroupBy           | Hyperedge projection     |

This analogy is conceptual rather than literal, but it provides a powerful intuition: **graphs are treated as executable relations**, not as pointer-based structures.

### 4.4 Views and Surface Projections

A Hypergraph may be projected into multiple **semantic views**, each exposing a constrained surface syntax while preserving the same underlying logical plan.

```python
hg = Hypergraph.connect("grism://local")

pg = hg.view("property")   # Property-graph projection
cg = hg.view("cypher")     # Cypher-compatible surface
nq = hg.view("ankql")      # Native hypergraph query surface
```

Key guarantees:

* Views are **pure projections**, not data copies
* No semantic information is lost; only expressiveness may be restricted
* All views compile into the same Hyperedge-based logical plans

Binary edges in property or Cypher views correspond to **arity-2 hyperedges** in the core model.

### 4.5 Core Hypergraph Operations

The Hypergraph object exposes a minimal, orthogonal set of operations that construct logical plans:

```python
class Hypergraph:
    def match(self, pattern): ...
    def nodes(self, label=None): ...
    def hyperedges(self, label=None): ...
    def filter(self, predicate): ...
    def project(self, *roles): ...
    def expand(self, *args, **kwargs): ...
    def view(self, mode: str): ...
    def collect(self): ...
```

All operations:

* Are **immutable** (return new objects)
* Are **lazy** (no execution on construction)
* Produce Frames (`NodeFrame`, `HyperedgeFrame`, or projections)

### 4.6 Relationship to Frames

Conceptually:

> **Hypergraph is the root context; Frames are scoped logical views derived from it.**

* `Hypergraph` represents *what data exists*
* `Frame` represents *which subgraph is currently addressed*

Operations such as `nodes()`, `hyperedges()`, and `match()` return Frames, which then participate in traversal, filtering, and projection.

Frames never outlive their Hypergraph context but may be freely composed and transformed within it.

### 4.7 Python-First Interface and Query-Language Agnosticism

Grism is designed as a **Python-first system**. The primary user interface for constructing, composing, and executing hypergraph queries is the **Python API**, not an embedded textual query language.

The Hypergraph abstraction is therefore **query-language agnostic** by design:

* All semantics are defined at the level of **Hypergraph, Frames, and Logical Plans**
* No core capability depends on Cypher, GQL, or any specific surface syntax
* Query languages, if supported, are treated strictly as **optional front-end projections**

This ensures that the hypergraph model remains stable, extensible, and suitable for programmatic and agent-driven workloads.

### 4.8 Property-Graph Semantics via Python Projections

Property-graph semantics are expressed **directly in Python**, without requiring Cypher or GQL syntax. Binary graph traversal is modeled as a constrained case of hyperedge expansion.

Mapping rules:

* Nodes map to `Node`
* Binary edges map to **arity-2 `Hyperedge`**
* Labels map to node or hyperedge labels
* Properties map to entity attributes

Example (property-graph style traversal expressed in Python):

```python
(
  hg.nodes("Person")
    .expand("WORKS_AT", to="Company")
    .select("Person", "Company")
)
```

This query is semantically equivalent to a property-graph pattern match, but is:

* Composable as Python code
* Type-aware and IDE-friendly
* Directly integrated with control flow, functions, and agents

### 4.9 Optional Query-Language Frontends

While Grism is Python-first, the architecture does **not preclude** support for textual query languages in the future.

Potential frontends include:

* Cypher-compatible syntax
* GQL-compatible syntax
* A native declarative hypergraph language

If enabled, such frontends:

* Parse user queries into the **same Hypergraph logical plans**
* Introduce no new semantics
* Remain strictly optional and non-authoritative

In all cases, Python remains the **reference interface** against which correctness and completeness are defined.

### 4.8 Naming and Semantic Guarantees

The following guarantees apply to the Hypergraph abstraction:

1. Hypergraph semantics are invariant across views
2. No operation bypasses the logical planning layer
3. Physical optimizations do not alter observable results
4. Hyperedge semantics always remain first-class

These guarantees ensure that Section 4 remains fully consistent with the data model and traversal semantics defined in Section 3.

### 4.9 Summary

The **Hypergraph** abstraction unifies graph, relational, and knowledge representations into a single executable model. It provides a stable, expressive, and optimizable foundation for AI-native workloads while preserving compatibility with existing graph query paradigms.

## 5. Python-First Hypergraph API

### 5.1 Design Principles

The Python API is the **authoritative user-facing interface** of Grism. All user interactions—whether programmatic, agent-driven, or generated by LLMs—are expressed in Python and compiled into the same canonical Rust logical plan.

Design principles:

* **Python-first semantics**: Python defines correctness; textual query languages are optional projections
* **Declarative & lazy**: API calls describe *what* to compute, not *how* to execute
* **Frame-centric**: All operations return immutable Frames (`NodeFrame`, `HyperedgeFrame`, or projections)
* **Explainable by construction**: Every API call maps to a deterministic logical operator

This ensures a stable, composable, and AI-friendly surface aligned with the hypergraph model defined in Sections 3–4.

### 5.2 Hypergraph Root Object

```python
hg = Hypergraph.connect("grism://local")
```

The `Hypergraph` object represents a **logical handle to a versioned hypergraph state**. It is not a session, cursor, or transaction, but a *root planning context*.

Key properties:

* **Immutable**: No in-place mutation; all methods return new logical objects
* **Snapshot-bound**: Each Hypergraph is associated with a read snapshot (MVCC)
* **Serializable**: Logical plans can be persisted, replayed, or shipped to workers
* **Storage-agnostic**: No dependency on physical layout or execution backend

### 5.3 Frames as First-Class Values

All data access in Grism happens through **Frames**, which are immutable, typed logical views over the hypergraph.

Canonical frame types:

| Frame            | Semantics                                         |
| ---------------- | ------------------------------------------------- |
| `NodeFrame`      | A set of nodes with optional label and predicates |
| `HyperedgeFrame` | A set of hyperedges of arbitrary arity            |
| `EdgeFrame`      | A binary projection of `HyperedgeFrame`           |

Frames are:

* **Value objects**: Equality is based on logical plan structure
* **Composable**: Frames can be freely chained and nested
* **Schema-aware**: Carry column and type metadata when available

### 5.4 Core Hypergraph Operations

```python
(
  hg.nodes("Paper")
    .filter(col("year") >= 2022)
    .expand("AUTHORED_BY", to="Author")
    .filter(col("Author.affiliation") == "MIT")
    .select("Paper.title", "Author.name")
)
```

The API exposes a minimal, orthogonal operator set:

| Operation                  | Logical Meaning                      |
| -------------------------- | ------------------------------------ |
| `nodes()` / `hyperedges()` | Base relation scan                   |
| `filter()`                 | Predicate selection                  |
| `expand()`                 | Hyperedge-based traversal            |
| `select()`                 | Projection and expression evaluation |
| `groupby()` / `agg()`      | Aggregation                          |
| `infer()`                  | Rule-based reasoning                 |

Each operation appends a node to the logical plan; no execution occurs at construction time.

### 5.5 Traversal Semantics in the API

Traversal is always expressed via `expand()`, which is a direct surface representation of the **Expand logical operator** defined in Section 3.3.

Supported modes:

* **Binary traversal** (property-graph compatible)
* **Role-qualified traversal** over n-ary hyperedges
* **Hyperedge materialization** for meta-reasoning

This guarantees that Python traversal semantics are *identical* to the hypergraph traversal model.

## 6. Expression System

### 6.1 Expression Model

Expressions in Grism are **pure, immutable expression trees** that are attached to logical operators such as `filter`, `select`, `expand`, and `agg`.

```python
sim(col("embedding"), query_emb) > 0.8
```

Key properties:

* **Side-effect free**
* **Lazily evaluated**
* **Serializable** (part of the logical plan)
* **Execution-backend agnostic**

Expressions never access data directly; they describe computations to be applied during execution.

### 6.2 Column References & Scope Resolution

Column references are expressed via `col(name)` and resolved against the **frame scope stack**.

Resolution rules:

1. **Qualified references** (`col("Author.name")`)

   * Resolved against explicit labels or aliases
   * Deterministic and unambiguous

2. **Unqualified references** (`col("name")`)

   * Resolved by most-recent scope first
   * Ambiguity results in a validation error

3. **Post-expand scope**

   * Original frame columns remain visible
   * Expanded entities are accessible via label or `as_` alias
   * Hyperedge properties are accessible via hyperedge label

4. **Post-select scope**

   * Only projected columns remain visible
   * Original columns are no longer addressable unless re-selected

These rules mirror relational scoping while respecting hypergraph expansion semantics.

### 6.3 Expression Categories

**Comparison & Logical**:

```python
(col("age") > 18) & (col("status") == "active")
```

**Function & Vector**:

```python
sim(col("embedding"), qvec)
contains(col("text"), "graph")
```

**Type & Null Handling**:

```python
cast(col("age"), "float")
col("email").is_null()
col("score").coalesce(0.0)
```

All functions are mapped to typed logical expressions and validated before execution.

### 6.4 Validation & Type Semantics

Validation occurs in two stages:

* **Plan construction**: Column existence, scoping, arity checks
* **Plan validation**: Type compatibility, null semantics, aggregation legality

Errors include full frame lineage and column suggestions, ensuring debuggability for both humans and agents.

## 7. Logical Plan Layer (Rust Canonical IR)

### 7.1 Role of the Logical Plan

The logical plan is the **single source of truth** for execution semantics in Grism. All frontends—Python DSL, Cypher, GQL, or agents—compile into this representation.

Logical plans are:

* **Deterministic**
* **Purely declarative**
* **Execution-backend independent**
* **Serializable and replayable**

### 7.2 Core Logical Operators

```rust
enum LogicalOp {
    Scan,        // Node / Hyperedge scan
    Expand,      // Hyperedge-based traversal
    Filter,      // Predicate selection
    Project,     // Projection & expressions
    Aggregate,   // Grouping & aggregation
    Infer,       // Rule-based derivation
}
```

Each operator consumes one or more input relations and produces a new relation over Nodes or Hyperedges.

### 7.3 Logical Operator Semantics

* **Scan**: Binds a base relation (nodes or hyperedges)
* **Expand**: Follows role bindings; may materialize hyperedges
* **Filter**: Applies boolean expressions with three-valued logic
* **Project**: Computes expressions and column aliases
* **Aggregate**: Groups by keys and applies aggregations
* **Infer**: Applies declarative rules to derive new hyperedges

All operators are **side-effect free** and composable.

### 7.4 Plan Construction & Lineage

Logical plans are constructed incrementally as a DAG:

* Each API call appends a new logical node
* Nodes reference their parent(s)
* Full lineage is retained for explanation and optimization

```text
Scan → Expand → Filter → Project → Aggregate
```

This lineage is surfaced via `hg.explain()` and drives both optimization and distributed execution.

### 7.5 Stability Guarantees

The logical plan layer guarantees that:

1. Hyperedge semantics are preserved across all rewrites
2. Physical optimizations cannot change observable results
3. Distributed and local execution share identical semantics

This makes the logical plan the cornerstone that unifies Grism’s hypergraph model, Python API, and execution engines.

## 8. Optimization

Optimization in Grism operates **entirely on the hyperedge-native logical plan**. Unlike traditional relational or property-graph engines, optimization decisions are driven primarily by **Expand semantics, hyperedge arity, and role selectivity**, rather than generic join heuristics.

The optimizer is deliberately split into **rule-based** and **cost-based** phases, both of which preserve the logical guarantees defined in Sections 3–7.

### 8.1 Optimization Objectives

The optimizer aims to:

* Minimize hyperedge materialization
* Prefer adjacency-based execution for binary expansions
* Delay or avoid n-ary joins when possible
* Reduce intermediate cardinality early
* Preserve explainability and determinism

Crucially, **no optimization may alter hyperedge semantics** or observable results.

### 8.2 Rule-Based Optimization (Logical Rewrites)

Rule-based optimization applies **semantics-preserving rewrites** to the logical plan DAG.

#### 8.2.1 Predicate Pushdown Across Expand

Filters that reference only pre-expand scope are pushed *before* Expand:

```text
Scan → Expand → Filter  ⟶  Scan → Filter → Expand
```

This is especially important for hypergraphs, as it reduces the number of hyperedges considered during expansion.

Constraints:

* Predicates referencing expanded roles or hyperedge properties cannot be pushed below Expand

#### 8.2.2 Expand Reordering

When multiple Expand operators are chained, the optimizer may reorder them if semantics permit.

Heuristics:

* Binary expands before n-ary expands
* High-selectivity roles before low-selectivity roles

Reordering is allowed only when role bindings are independent and hyperedge materialization semantics are unchanged.

#### 8.2.3 Projection Pruning

Unused roles, properties, and hyperedge columns are pruned early to reduce join width and memory pressure.

### 8.3 Cost-Based Optimization (Hyperedge-Aware)

Cost-based optimization selects physical execution strategies using estimates derived from hyperedge structure.

#### Core Cost Dimensions

* Hyperedge arity
* Role selectivity
* Expand execution mode (binary vs n-ary)
* Hyperedge materialization cost
* Cardinality growth
* Distributed shuffle cost (Ray)

#### 8.3.1 Expand Cost Model

Expand is the **dominant cost driver** in Grism.

**Binary Expand (Adjacency-Based)**

* Arity = 2 with `{source, target}` roles
* O(deg(node)) traversal
* Uses adjacency or role indexes

**N-ary Expand (Relational Join)**

* Arity > 2 or role-qualified traversal
* Join-like execution over role bindings
* Cost grows with arity and fan-out

The optimizer aggressively rewrites n-ary expands into binary projections when semantics allow.

### 8.4 Distributed Cost Modeling (Ray)

For Ray execution, the optimizer additionally considers:

* Data locality of Lance fragments
* Shuffle volume induced by Expand and Aggregate
* Stage fusion opportunities

### 8.5 Optimization Guarantees

The optimizer guarantees that:

1. Expand semantics are never violated
2. Hyperedge arity and role meaning are preserved
3. All rewrites are explainable via `hg.explain()`
4. Cost-based decisions do not affect correctness

## 9. Execution Backends

Grism supports multiple execution backends that share the **same logical plan semantics**. Execution backends differ only in *how* logical operators are executed, never in *what* they compute.

All backends execute **hyperedge-native logical plans**, with `Expand` as the dominant physical operator.

### 9.1 Standalone Rust Engine

The standalone engine is the **reference execution backend** for Grism. It executes logical plans locally with maximal performance and minimal orchestration overhead.

**Execution model**:

* Vectorized, columnar execution
* Async execution using Tokio
* Arrow `RecordBatch` as the physical data unit

```rust
trait ExecNode {
    async fn next(&mut self) -> Option<RecordBatch>;
}
```

Each logical operator is compiled into a corresponding physical operator implementing `ExecNode`.

**Parallelism**:

* CPU parallelism via Rayon
* SIMD-optimized Arrow kernels
* Operator-level pipelining for streaming execution

**Expand execution**:

* Binary expands preferentially use adjacency or role indexes
* N-ary expands execute as role-binding joins
* Hyperedge materialization is performed only when required by the logical plan

The standalone engine prioritizes **low latency, predictable performance**, and serves as the semantic baseline for all other backends.

### 9.2 Ray Distributed Engine

The Ray backend enables **distributed execution** of the same logical plans across a cluster.

**Principle**:

> **Ray orchestrates; Rust executes.**

Ray is responsible for task scheduling, data movement, and fault tolerance, while Rust workers perform actual query execution.

#### Ray Execution Flow

```
Logical Plan
 → Physical Plan (Ray-aware)
 → Ray DAG (stages)
 → Rust Worker Tasks
 → Arrow Results
```

The physical planner partitions the logical plan into **execution stages**, typically aligned with:

* Expand boundaries
* Aggregation boundaries
* Materialization points

#### Ray Tasks

```python
@ray.remote
def execute_stage(fragment, inputs):
    return rust_worker.execute(fragment, inputs)
```

Each task executes a fragment of the physical plan using the same Rust engine as the standalone backend.

**Data transport**:

* Arrow IPC for batch serialization
* Ray Plasma store for zero-copy sharing when possible

**Distributed considerations**:

* Expand-induced fan-out is the primary source of shuffle cost
* High-selectivity filters are pushed to workers early
* Hyperedge materialization across stage boundaries is avoided when possible

The Ray backend preserves **identical semantics** to local execution while enabling scale-out execution for large graphs and agent workloads.

## 10. Storage Layer

Grism’s storage layer is built on **Lance**, providing columnar, versioned persistence optimized for analytical and AI workloads.

### 10.1 Lance Dataset Layout

```
/datasets/
  nodes.lance
  hyperedges.lance
  properties.lance
  embeddings.lance
```

Logical separation is maintained between:

* **Structural data** (nodes, hyperedges, roles)
* **Attribute data** (properties)
* **Vector data** (embeddings)

This separation aligns with the hyperedge model and supports independent optimization of structure and content.

### 10.2 Storage Properties

* Append-only writes
* Snapshot isolation (MVCC)
* Time travel and reproducible queries
* Arrow-native zero-copy reads

Storage layout is **not exposed** to users or logical planning and may evolve without affecting semantics.

## 11. Indexing

Indexes are **physical accelerators** that do not change logical semantics. They exist solely to optimize Expand, Filter, and Scan operations.

### 11.1 Structural Indexes

* Adjacency indexes for binary hyperedges
* Role-based indexes for n-ary hyperedges
* Label and type bitmaps for fast filtering

These indexes primarily accelerate **Expand execution**, especially in adjacency-based traversal.

### 11.2 Vector Indexes

* Lance ANN indexes
* HNSW or equivalent structures

Vector indexes integrate directly with expression evaluation (e.g. `sim()`), enabling hybrid symbolic–vector queries.

## 12. Reasoning & Neurosymbolic Layer

The reasoning layer treats the hypergraph as **executable knowledge**, not static data.

### 12.1 Ontologies & Typing

* OWL / RDFS support via `horned-owl`
* Type inference over nodes and hyperedges
* Ontological constraints enforced during validation and inference

Types participate directly in logical planning and optimization.

### 12.2 Rule Engine

* Datalog-style declarative rules
* Fixpoint evaluation semantics
* Rules derive **new hyperedges**, not side effects

Rule execution is compiled into logical plans using the `Infer` operator, making reasoning explainable and replayable.

## 14. APIs & Interfaces

Grism is **Python-first**, but exposes multiple interfaces for interoperability.

* Python SDK (authoritative)
* gRPC / Arrow Flight (service integration)
* Optional GQL / Cypher frontends (debugging / interop only)

```python
hg.explain(mode="logical")
hg.explain(mode="physical")
```

All interfaces compile into the same logical plan representation.

## 15. Transactions & Versioning

* MVCC via Lance snapshots
* Branchable hypergraph states
* Deterministic replay of logical plans

Each query executes against a **stable snapshot**, ensuring consistency across distributed and concurrent execution.

## 16. Security & Governance

* Label- and role-based access control
* Subgraph-level isolation
* Provenance tracking via hyperedge-to-hyperedge relations

Security is enforced at the **logical plan level**, not as an afterthought in execution.

## 17. Rust & Python Crate Layout

```
grism/
├── grism-core        # hypergraph model, values, roles
├── grism-logical     # logical plan & operators
├── grism-optimizer  # rule-based & cost-based optimization
├── grism-engine     # standalone Rust execution engine
├── grism-ray        # Ray physical planner & workers
├── grism-storage    # Lance integration
├── grism-reasoning  # ontology & rule engine
├── grism-python     # Python DSL (pyo3 bindings)
```

Crate boundaries reflect **semantic layers**, not implementation convenience.

## 18. Design Details & Semantics

### 18.1 Lazy Evaluation Guarantees

**Frame immutability**:

* All operations return new Frames
* Original Frames are never modified
* Logical plans use structural sharing

**Execution triggering**:

* Execution occurs only on `.collect()` or iteration
* `.explain()` never triggers execution
* No implicit caching by default

### 18.2 Error Handling

Errors are categorized by phase:

1. **Construction errors**: invalid API usage
2. **Validation errors**: semantic or type violations
3. **Execution errors**: runtime or system failures

All errors include **logical plan lineage** for explainability.

### 18.3 Performance Considerations

* Streaming execution for large results
* Arrow `RecordBatch` batching
* Per-executor memory limits

Future features may include explicit optimization hints and plan caching.

### 18.4 Concurrency Model

**Reads**:

* Fully concurrent
* Snapshot isolation per query

**Writes**:

* Serialized commits
* Optimistic conflict detection

**Distributed execution**:

* Ray handles task scheduling and retries
* Physical planning considers data locality and Expand fan-out
* Shuffle is treated as a first-class cost
