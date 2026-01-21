# Core Type Naming Freeze (Authoritative)

This document locks down **all core type names** for the Grism / Mizar architecture. These names are **canonical** and should be treated as stable across RFCs, codebases, APIs, and documentation.

---

## 1. Foundational Concepts

| Concept          | Canonical Name         | Notes                                    |
| ---------------- | ---------------------- | ---------------------------------------- |
| Graph model      | **Hypergraph**         | Standard graph-theoretic term            |
| Entity (atomic)  | **Node**               | First-class entity                       |
| Relation (n-ary) | **Hyperedge**          | Sole relational primitive                |
| Binary relation  | **Edge** *(view only)* | Projection of Hyperedge, not a primitive |

**Invariant:** All relations are hyperedges. Edge is a compatibility view.

---

## 2. Logical Data Model Types

### 2.1 Core IDs

```text
NodeId
EdgeId
Label
Role
```

---

### 2.2 Entity References

```text
EntityRef
  ├─ Node(NodeId)
  └─ Hyperedge(EdgeId)
```

---

### 2.3 Data Structures

```text
Node
Hyperedge
RoleBinding
PropertyMap
```

---

## 3. Frame Abstractions (User-Facing)

Frames are **logical views**, not storage structures.

| Frame            | Canonical Name     | Description                  |
| ---------------- | ------------------ | ---------------------------- |
| Node view        | **NodeFrame**      | Nodes matching predicates    |
| Hyperedge view   | **HyperedgeFrame** | Primary relational view      |
| Binary edge view | **EdgeFrame**      | Projection of HyperedgeFrame |

**Rule:** All query planning starts from NodeFrame or HyperedgeFrame.

---

## 4. Logical Plan & Operators

### 4.1 Logical Plan

```text
LogicalPlan
LogicalExpr
```

---

### 4.2 Core Operators

| Operator       | Canonical Name    | Notes                     |
| -------------- | ----------------- | ------------------------- |
| Scan nodes     | **ScanNode**      | NodeFrame source          |
| Scan relations | **ScanHyperedge** | HyperedgeFrame source     |
| Filter         | **Filter**        | Predicate filtering       |
| Project        | **Project**       | Column selection          |
| Expand         | **Expand**        | Traversal over hyperedges |
| Join           | **Join**          | Relational join           |
| Aggregate      | **Aggregate**     | Grouping                  |

**Explicitly avoided:** `ScanEdge` (use ScanHyperedge + projection).

---

## 5. Traversal & Expansion Semantics

| Concept                   | Canonical Name                    |
| ------------------------- | --------------------------------- |
| Traversal op              | **Expand**                        |
| Binary traversal          | **BinaryExpand** *(mode)*         |
| Role-based traversal      | **RoleExpand** *(mode)*           |
| Hyperedge materialization | **MaterializeHyperedge** *(flag)* |

---

## 6. Physical Planning & Execution

### 6.1 Plans

```text
PhysicalPlan
PhysicalOperator
```

---

### 6.2 Physical Operators

| Operator         | Canonical Name          |
| ---------------- | ----------------------- |
| Node scan        | **NodeScanExec**        |
| Hyperedge scan   | **HyperedgeScanExec**   |
| Adjacency expand | **AdjacencyExpandExec** |
| Role join expand | **RoleExpandExec**      |
| Filter           | **FilterExec**          |
| Projection       | **ProjectExec**         |

---

## 7. Storage Layer

| Concept         | Canonical Name     | Notes              |
| --------------- | ------------------ | ------------------ |
| Storage engine  | **StorageEngine**  | Abstract interface |
| Lance backend   | **LanceStorage**   | Default backend    |
| Index           | **Index**          | Generic            |
| Adjacency index | **AdjacencyIndex** | Binary hyperedges  |
| Role index      | **RoleIndex**      | Hyperedge roles    |

---

## 8. Query Language Layer

| Layer             | Canonical Name               |
| ----------------- | ---------------------------- |
| Cypher-compatible | **Cypher**                   |
| GQL-compatible    | **GQL**                      |
| Native hypergraph | **GrismQL** *(working name)* |

---

## 9. Reasoning & Neurosymbolic Layer

| Concept     | Canonical Name  |
| ----------- | --------------- |
| Rule        | **Rule**        |
| Inference   | **Inference**   |
| Provenance  | **Provenance**  |
| Explanation | **Explanation** |
| Constraint  | **Constraint**  |

---

## 10. Distributed & Runtime Layer

| Concept           | Canonical Name       |
| ----------------- | -------------------- |
| Runtime           | **Runtime**          |
| Scheduler         | **Scheduler**        |
| Task              | **Task**             |
| Execution context | **ExecutionContext** |

---

## 11. Explicitly Forbidden / Deprecated Names

These names should **not** appear in new code or docs:

```text
GraphFrame
HyperGraph
RelationEdge
Triple
Statement
Fact
```

Use the canonical names instead.

---

## 12. Naming Stability Policy

1. Names in this document are **stable**
2. Changes require an explicit RFC
3. Aliases may exist at API boundaries only
4. Internal code must use canonical names

---

## 13. One-Line Summary

> **Grism uses a hypergraph-first naming system: Nodes represent entities, Hyperedges represent all relations, and Frames expose logical views.**
