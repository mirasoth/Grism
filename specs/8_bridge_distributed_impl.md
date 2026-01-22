Below is a **Week-8 bridge design** that cleanly extends your Week-7 local engine into **distributed execution**, without changing semantics or operator contracts.

This is written as a **continuation of the Week-7 design**, not a rewrite.
Everything here is **additive** and explicitly aligned with **RFC-0008 (physical operators)** and **RFC-0010 (distributed execution)**.

---

# Design: Exchange Operator & Stage Splitting

*(Week-8 bridge: Local → Distributed Execution)*

---

## 0. Design Objective (Why this exists)

Week-7 gave us:

* A **fully executable local physical plan**
* Streaming, Arrow-native operators
* Blocking operators clearly marked

Week-8 must add:

* **Distributed execution**
* **Data repartitioning**
* **Parallel execution**
* **Failure isolation**

Without:

* Changing logical semantics
* Changing physical operator contracts
* Introducing implicit magic

> **Key principle**
> Distribution is achieved by **cutting** the physical plan into **stages**, connected by an explicit **Exchange** operator.

---

## 1. Core Concepts

### 1.1 Exchange is a Physical Operator

**Exchange is NOT a runtime trick.**
It is a **first-class physical operator** that:

* Repartitions data
* Introduces a synchronization boundary
* Separates execution stages

```text
Local Plan:
Scan → Filter → Expand → Aggregate → Collect

Distributed Plan:
Scan → Filter → Expand → Exchange → Aggregate → Collect
```

---

### 1.2 Stage = Executable Physical Subplan

A **Stage** is:

* A connected sub-DAG of physical operators
* Executed as a unit (locally or remotely)
* Has no internal Exchange operators

```rust
struct ExecutionStage {
    stage_id: StageId,
    plan: PhysicalSubPlan,
    input_partitioning: Option<PartitioningSpec>,
    output_partitioning: Option<PartitioningSpec>,
}
```

---

## 2. Exchange Operator Design

### 2.1 ExchangeExec (Physical Operator)

```rust
pub struct ExchangeExec {
    pub partitioning: PartitioningSpec,
    pub mode: ExchangeMode,
}
```

### 2.2 ExchangeMode

```rust
pub enum ExchangeMode {
    Local,        // no-op (Week 7 compatibility)
    Shuffle,      // repartition across workers
    Broadcast,    // replicate to all workers
    Gather,       // many → one
}
```

Only `Local` is used in Week-7.
All others activate in Week-8.

---

### 2.3 PartitioningSpec (Normative)

```rust
pub enum PartitioningSpec {
    Hash {
        keys: Vec<PhysicalExpr>,
        partitions: usize,
    },
    Range {
        key: PhysicalExpr,
        ranges: Vec<Range>,
    },
    Adjacency {
        entity: EntityType, // Node | Hyperedge
    },
    Single,
}
```

**Important**:
Partitioning is **explicit**, never inferred at runtime.

---

### 2.4 Exchange Semantics (Non-Negotiable)

| Property     | Guarantee                                |
| ------------ | ---------------------------------------- |
| Completeness | All rows forwarded                       |
| Disjointness | Each row appears once (except Broadcast) |
| Determinism  | Same input → same partition              |
| Barrier      | Downstream waits for upstream readiness  |

Exchange is a **semantic boundary**, not an optimization hint.

---

## 3. Why Exchange Is Explicit (Architectural Justification)

**Why not implicit shuffle?**

Because Grism guarantees:

> *Parallelism must not change meaning.*

Explicit Exchange ensures:

* Explainability (`EXPLAIN DISTRIBUTED`)
* Cost modeling
* Debuggability
* Operator capability checks
* Hypergraph adjacency correctness

---

## 4. Stage Splitting Algorithm

### 4.1 Inputs

* Fully planned **PhysicalPlan**
* Operator metadata:

  * `blocking`
  * `requires_global_view`
  * `parallelizable`
* Execution target: `Local | Distributed`

---

### 4.2 Stage Boundary Rules (Normative)

A **new stage MUST start** at:

1. Any `ExchangeExec`
2. Any **blocking operator** if distribution is enabled
3. Any operator that:

   * requires global state
   * cannot be parallelized

```text
[Stage A] → Exchange → [Stage B]
```

---

### 4.3 Stage Splitting Pseudocode

```rust
fn split_into_stages(plan: PhysicalPlan) -> Vec<ExecutionStage> {
    let mut stages = vec![];
    let mut current = StageBuilder::new();

    for node in plan.topo_order() {
        if node.is_exchange() || node.is_blocking() {
            stages.push(current.finish());
            current = StageBuilder::new();
        }
        current.add(node);
    }

    stages.push(current.finish());
    stages
}
```

---

## 5. Execution Model After Splitting

### 5.1 Distributed Execution Flow

```text
Stage 0 (parallel)
  ├─ Worker 1
  ├─ Worker 2
  └─ Worker 3
        ↓
     Exchange
        ↓
Stage 1 (parallel or single)
```

Each stage:

* Executes independently
* Produces Arrow batches
* Hands off via Exchange

---

## 6. Exchange Execution (Ray Backend)

### 6.1 Control Plane vs Data Plane

| Plane   | Responsibility          |
| ------- | ----------------------- |
| Control | Stage graph, scheduling |
| Data    | Arrow batch movement    |

Ray owns **control**, Rust owns **execution**.

---

### 6.2 Exchange Execution Steps

**Shuffle Exchange example**:

1. Upstream stage emits `RecordBatch`
2. Batch partitioned using `PartitioningSpec`
3. Sub-batches sent to Ray object store
4. Downstream tasks pull assigned partitions

No operator knows **where** data comes from.

---

## 7. Expand-Aware Partitioning (Critical)

### 7.1 Binary Expand (Adjacency-Aware)

**Preferred strategy**:

```rust
PartitioningSpec::Adjacency {
    entity: Node,
}
```

Guarantee:

* Most expands are local
* Cross-partition traversal is explicit

---

### 7.2 N-ary Expand (RoleExpand)

* Often forces shuffle
* Exchange inserted **before** Expand
* Planner decides based on cost

This preserves correctness for hyperedges.

---

## 8. Blocking Operators & Global Semantics

### 8.1 Aggregate

Two-phase aggregation pattern:

```text
Stage 0: PartialAggregate (parallel)
   ↓ Exchange(Hash by group key)
Stage 1: FinalAggregate
```

This is **planned explicitly**.

---

### 8.2 Sort

```text
Stage 0: LocalSort
   ↓ Exchange(Range)
Stage 1: MergeSort
```

No hidden coordination.

---

## 9. Operator Capability Contract (Extended)

Operators must declare:

```rust
struct OperatorCaps {
    streaming: bool,
    blocking: bool,
    parallel_safe: bool,
    requires_partitioning: Option<PartitioningSpec>,
}
```

Planner uses this to:

* Insert Exchange
* Split stages
* Reject illegal plans

---

## 10. Failure & Retry Semantics (Week-8 Scope)

Stage boundaries define **failure domains**.

| Failure                | Recovery              |
| ---------------------- | --------------------- |
| Worker crash           | Retry stage           |
| Exchange failure       | Re-run upstream stage |
| Blocking stage failure | Restart stage         |

No partial stage result is reused unless idempotent.

---

## 11. Explainability Guarantees

`EXPLAIN DISTRIBUTED` must show:

```text
Stage 0:
  NodeScan → Filter → Expand
  Parallelism: 8
  Output: HashPartition(node_id)

Stage 1:
  Aggregate → Collect
  Parallelism: 1
```

This is **non-optional**.

---

## 12. Integration with Week-7 Local Engine

### 12.1 Local Compatibility

When execution mode = `Local`:

* ExchangeExec becomes **no-op**
* Single stage only
* No scheduling overhead

Same code path, different backend.

---

## 13. Minimal Week-8 Implementation Checklist

✔ ExchangeExec struct
✔ PartitioningSpec implemented
✔ Stage splitter implemented
✔ Ray stage executor wrapper
✔ Partial/final aggregate variants
✔ Expand-aware partition rules
✔ Distributed EXPLAIN output

---

## 14. Architectural Payoff

This design ensures:

* **Zero semantic drift**
* **Local engine remains reference**
* **Distributed execution is explainable**
* **Hypergraph Expand remains correct**
* **Future extensions (GPU, spill, adaptive) slot in cleanly**

---

## Final Summary

> **Exchange is the only way data moves across stages.**
> **Stages are the only unit of distribution.**
> **Operators never know they are distributed.**

This is the exact architectural separation that allows Grism to scale **without ever compromising its hypergraph semantics**.