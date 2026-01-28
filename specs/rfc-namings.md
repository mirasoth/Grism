# RFC Naming Alignment & Completion (Grism Core)

**Status**: Frozen
**Authors**: Grism Team
**Created**: 2026-01-21
**Last Updated**: 2026-01-28
**Depends on**: —
**Supersedes**: —

---

This RFC **polishes, aligns, and completes** the RFC naming scheme so it is **fully consistent with the Grism architecture design** (v1) and **complete across all layers**: logical model, planning, execution, storage, reasoning, and distributed runtime.

It **supersedes** earlier partial naming lists and should be treated as the **authoritative cross-RFC naming reference**.

---

## 0. Naming Principles (Normative)

1. **Hypergraph-first semantics**
   All relations are hyperedges; binary edges are projections only.

2. **Logical ≠ Physical ≠ Storage**
   Naming must reflect layer boundaries precisely.

3. **One concept → one canonical name**
   Aliases are allowed only at public API boundaries.

4. **Operators are semantic, not algorithmic**
   Physical algorithms live in `*Exec` names only.

5. **Python surface mirrors logical model**
   Python names map 1:1 to logical operators and frames.

---

## 1. Foundational Model (Frozen)

| Concept          | Canonical Name         | Notes                           |
| ---------------- | ---------------------- | ------------------------------- |
| Graph container  | **Hypergraph**         | Canonical user-facing container |
| Atomic entity    | **Node**               | Stable identity                 |
| Relation (n-ary) | **Hyperedge**          | Sole relational primitive       |
| Binary relation  | **Edge** *(view only)* | Arity=2 hyperedge projection    |

**Invariant:** There is no independent `Edge` primitive.

---

## 2. Identity & Structural Types

```text
NodeId
EdgeId
Label
Role
PropertyKey
```

```text
EntityRef
 ├─ Node(NodeId)
 └─ Hyperedge(EdgeId)
```

```text
Node
Hyperedge
RoleBinding
PropertyMap
```

---

## 3. Frame System (Logical Views)

Frames are **immutable, lazy, logical relations**.

| Frame              | Canonical Name     | Semantics                    |
| ------------------ | ------------------ | ---------------------------- |
| Node relation      | **NodeFrame**      | Nodes with predicates        |
| Hyperedge relation | **HyperedgeFrame** | Primary relational view      |
| Binary edge view   | **EdgeFrame**      | Projection of HyperedgeFrame |

**Rule:** All planning starts from `NodeFrame` or `HyperedgeFrame`.

---

## 4. Hypergraph Root API

```text
Hypergraph
```

| Method                | Semantics               |
| --------------------- | ----------------------- |
| `nodes()`             | NodeFrame scan          |
| `hyperedges()`        | HyperedgeFrame scan     |
| `match()`             | Pattern-based expansion |
| `expand()`            | Hyperedge traversal     |
| `filter()`            | Predicate selection     |
| `select()`            | Projection              |
| `groupby()` / `agg()` | Aggregation             |
| `infer()`             | Rule-based derivation   |
| `view()`              | Surface projection      |
| `collect()`           | Trigger execution       |

---

## 5. Expression System

```text
Expr
LogicalExpr
AggExpr
ColumnRef
Literal
FunctionExpr
```

Expression categories:

* BooleanExpr
* ComparisonExpr
* ArithmeticExpr
* VectorExpr
* TypeExpr
* NullExpr

---

## 6. Logical Plan (Canonical IR)

```text
LogicalPlan
LogicalOp
Schema
```

### 6.1 Core Logical Operators

| Operator  | Canonical Name | Notes             |
| --------- | -------------- | ----------------- |
| Scan      | **Scan**       | Node / Hyperedge  |
| Expand    | **Expand**     | Unified traversal |
| Filter    | **Filter**     | Predicate         |
| Project   | **Project**    | Expressions       |
| Aggregate | **Aggregate**  | Group + agg       |
| Limit     | **Limit**      | Cardinality bound |
| Infer     | **Infer**      | Rule evaluation   |

**Important:** There is **no Join operator**. Joins are expressed via `Expand`.

---

## 7. Expand Semantics (Critical Section)

| Concept               | Canonical Name                    |
| --------------------- | --------------------------------- |
| Traversal operator    | **Expand**                        |
| Binary expand mode    | **BinaryExpand** *(mode)*         |
| Role-qualified expand | **RoleExpand** *(mode)*           |
| Hyperedge output      | **MaterializeHyperedge** *(flag)* |

Binary traversal is an **optimization**, not a semantic distinction.

---

## 8. Optimization Layer

```text
Optimizer
RuleOptimizer
CostOptimizer
RewriteRule
CostModel
```

| Concept            | Canonical Name        |
| ------------------ | --------------------- |
| Predicate pushdown | **PredicatePushdown** |
| Expand reorder     | **ExpandReorder**     |
| Projection prune   | **ProjectionPrune**   |
| Expand cost        | **ExpandCostModel**   |

---

## 9. Physical Planning & Execution

### 9.1 Physical Plans

```text
PhysicalPlan
PhysicalOp
ExecNode
```

### 9.2 Physical Operators

| Operator        | Canonical Name            | Layer    | Notes                    |
| --------------- | ------------------------- | -------- | ------------------------ |
| Node scan       | **NodeScanExec**          | Physical | Scan nodes by label      |
| Hyperedge scan  | **HyperedgeScanExec**     | Physical | Scan hyperedges by label |
| Binary expand   | **AdjacencyExpandExec**   | Physical | Binary edge traversal    |
| N-ary expand    | **RoleExpandExec**        | Physical | N-ary hyperedge traversal|
| Filter          | **FilterExec**            | Physical | Apply predicate          |
| Project         | **ProjectExec**           | Physical | Compute expressions      |
| Rename          | **RenameExec**            | Physical | Rename columns           |
| Aggregate       | **AggregateExec**         | Physical | Generic aggregation      |
| Hash aggregate  | **HashAggregateExec**     | Physical | Hash-based aggregation   |
| Limit           | **LimitExec**             | Physical | Limit output rows        |
| Sort            | **SortExec**              | Physical | Multi-key sorting        |
| Union           | **UnionExec**             | Physical | Union of inputs          |
| Collect         | **CollectExec**           | Physical | Collect all results      |
| Empty           | **EmptyExec**             | Physical | Empty result source      |
| Exchange        | **ExchangeExec**          | Physical | Data repartitioning (distributed) |

---

## 10. Execution Layer

### 10.1 Runtimes

| Runtime     | Canonical Name     | Description                      |
| ----------- | ------------------ | -------------------------------- |
| Local       | **LocalRuntime**   | Single-machine pull-based execution |
| Distributed | **RayRuntime**     | Ray-orchestrated distributed execution |

### 10.2 Executors & Context

| Type               | Canonical Name           | Notes                    |
| ------------------ | ------------------------ | ------------------------ |
| Local executor     | **LocalExecutor**        | Drives local execution   |
| Ray executor       | **RayExecutor**          | Drives distributed execution |
| Context trait      | **ExecutionContext**     | Runtime-agnostic context |
| Local context      | **LocalExecutionContext**| Local runtime context    |

### 10.3 Distributed Concepts

| Concept            | Canonical Name         | Notes                          |
| ------------------ | ---------------------- | ------------------------------ |
| Execution stage    | **ExecutionStage**     | Connected sub-DAG of operators |
| Partitioning spec  | **PartitioningSpec**   | Data distribution strategy     |
| Physical planner   | **LocalPhysicalPlanner** / **DistributedPlanner** | Runtime-specific planners |

---

## 11. Storage Layer

```text
StorageEngine
LanceStorage
```

### 11.1 Datasets

```text
nodes.lance
hyperedges.lance
properties.lance
embeddings.lance
```

### 11.2 Indexes

| Index            | Canonical Name     |
| ---------------- | ------------------ |
| Binary adjacency | **AdjacencyIndex** |
| Role-based       | **RoleIndex**      |
| Vector           | **VectorIndex**    |

Indexes are **pure accelerators**.

---

## 12. Reasoning & Neurosymbolic Layer

| Concept     | Canonical Name  |
| ----------- | --------------- |
| Ontology    | **Ontology**    |
| Rule        | **Rule**        |
| Inference   | **Inference**   |
| Provenance  | **Provenance**  |
| Explanation | **Explanation** |
| Constraint  | **Constraint**  |

Rules always derive **new hyperedges**.

---

## 13. Interfaces & Frontends

| Interface | Canonical Name                   |
| --------- | -------------------------------- |
| Python    | **Python API** *(authoritative)* |
| gRPC      | **GrpcService**                  |
| Arrow     | **ArrowFlight**                  |
| Cypher    | **CypherFrontend** *(optional)*  |
| GQL       | **GqlFrontend** *(optional)*     |

---

## 14. Forbidden / Deprecated Names (Enforced)

```text
GraphFrame
HyperGraph
RelationEdge
Triple
Statement
Fact
Join
SQL
```

## 15. Final Invariant

> **Every name in this document corresponds to exactly one semantic concept in the Grism architecture.**

Any deviation requires an explicit RFC amendment.
