# Phase One Milestone — Logical Pipeline End-to-End

**Project:** Grism / Mizar Core Engine
**Phase:** Phase One (Milestone 1)
**Status:** Specification
**Scope:** Python API → Logical IR → Optimization → Explain & Validate

---

## 1. Milestone Goal

The Phase One milestone establishes the **first fully closed logical loop** of Grism:

> **Python API → Python Expr → Logical Plan → Basic Optimizer → Explain / Validate**

At the end of this milestone, Grism must be able to:

* Accept **multiple Python query construction patterns**
* Compile them into a **typed, canonical Logical Plan**
* Apply **basic but correct logical optimizations**
* **Display and validate** the resulting logical plan deterministically

This milestone intentionally **excludes physical execution**.

---

## 2. Non-Goals (Explicit)

To keep Phase One tight and verifiable, the following are **out of scope**:

* Physical execution (Rust executor, Arrow kernels)
* Storage engines (Lance, Parquet, etc.)
* Cost-based optimization
* Distributed planning
* Query languages (Cypher, GQL)

---

## 3. End-to-End Pipeline Definition

```
Python API
   ↓
Python Expression Objects
   ↓
Logical Operator Construction
   ↓
Logical Plan DAG
   ↓
Rule-Based Optimizer
   ↓
Explain / Validate / Debug Output
```

Each stage must be **individually testable** and **composable**.

---

## 4. Python API Layer

### 4.1 Design Objectives

* Expressive but minimal
* Chainable, fluent API
* Deterministic mapping to logical operators
* No execution side effects

### 4.2 Supported Entry Points (Multiple Cases)

#### Case A — Node-Centric Query

```python
hg.nodes()
```

#### Case B — Hyperedge-Centric Query

```python
hg.hyperedges()
```

#### Case C — Pre-Filtered Entry

```python
hg.nodes(label="Paper")
```

---

### 4.3 Core API Operators

| API Method  | Logical Meaning   |
| ----------- | ----------------- |
| `expand()`  | Logical Expand    |
| `filter()`  | Logical Filter    |
| `select()`  | Logical Project   |
| `groupby()` | Logical Aggregate |
| `limit()`   | Logical Limit     |

All API methods **only construct logical state**.

---

## 5. Python Expression Layer

### 5.1 Expression Objects

Python expressions are **symbolic**, not evaluative.

```python
col("year") > 2020
```

Becomes:

* `ColumnRefExpr("year")`
* `LiteralExpr(2020)`
* `BinaryFunctionExpr("gt")`

---

### 5.2 Expression Guarantees

* Typed (int, float, bool, string)
* Deterministic
* Serializable to Rust Logical Expr

---

## 6. Logical Plan Construction

### 6.1 Canonical Operators

Logical plans MUST use the canonical operator set:

* Scan
* Expand
* Filter
* Project
* Aggregate
* Limit

No joins, no shortcuts.

---

### 6.2 Logical Plan DAG Rules

* DAG only (no cycles)
* Explicit schema on every node
* Column scope validated at construction
* Expand is the only relational composition primitive

---

## 7. Basic Optimizer

### 7.1 Optimizer Nature

* Rule-based
* Deterministic
* Fixpoint iteration
* Semantics-preserving

---

### 7.2 Required Optimization Rules

| Rule                 | Description                 |
| -------------------- | --------------------------- |
| Predicate Pushdown   | Move filters earlier        |
| Projection Pruning   | Remove unused columns       |
| Constant Folding     | Simplify expressions        |
| Expand Reordering    | Reorder independent expands |
| Filter–Expand Fusion | Reduce expand width         |

Each rule must:

* Prove safety
* Preserve schema
* Emit rewrite trace

---

## 8. Explain & Display

### 8.1 Explain Plan Output

```text
LogicalPlan
 ├─ Project[name, paper]
 │   └─ Filter[year > 2020]
 │       └─ Expand[role=author]
 │           └─ Scan[Node]
```

Explain output must be:

* Deterministic
* Stable across runs
* Human-readable

---

### 8.2 Rewrite Trace Output

```text
Rule: PredicatePushdown
Before: Filter → Expand
After: Expand → Filter
```

---

## 9. Validation Layer

### 9.1 Structural Validation

* DAG validation
* Operator input arity
* Schema consistency

### 9.2 Semantic Validation

* Column reference validity
* Expression typing
* Role binding correctness

Validation failures must be **explicit and early**.

---

## 10. Testing Matrix

### 10.1 Python API Tests

* Multiple chaining orders
* Invalid API usage
* Expression construction

### 10.2 Logical Plan Tests

* Plan shape correctness
* Schema propagation
* Expand semantics

### 10.3 Optimizer Tests

* Rule-specific tests
* Rewrite safety tests
* Fixpoint convergence

---

## 11. Deliverables

By the end of Phase One:

* `grism-logical` crate complete
* Python logical API functional
* Optimizer passes all invariants
* Explain & validate usable for debugging

---

## 12. Phase-One Success Criteria

Phase One is **complete** when:

* ✅ Python → Logical Plan works end-to-end
* ✅ Multiple query forms compile correctly
* ✅ Optimizer produces stable plans
* ✅ Logical plans are explainable & validated
* ❌ No execution yet (by design)
