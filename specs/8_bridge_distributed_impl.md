# Design: Engine-Runtime Separation & Distributed Execution

*(Bridge from Local Engine to Ray Distributed Execution)*

---

## 0. Design Objective

Week-7 delivered a **fully executable local physical plan** with streaming, Arrow-native operators. This document extends that foundation to support **distributed execution on Ray** while maintaining a clean architecture separation.

### Goals

1. **Runtime Separation**: Extract common engine components that work across local and distributed execution
2. **Local Runtime**: Dedicated crate for single-machine execution with in-memory and Lance storage
3. **Ray Runtime**: Dedicated crate for distributed execution using Ray as orchestration layer
4. **Zero Semantic Drift**: Distribution MUST NOT change query results

### Key Principle

> **The engine defines what to compute. The runtime defines how to execute.**

---

## 1. Crate Architecture

### 1.1 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            Application Layer                                 │
│                     (Python API, gRPC, CLI)                                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          grism-engine (Common)                               │
│  ┌─────────────┐  ┌──────────────┐  ┌───────────────┐  ┌────────────────┐  │
│  │  Physical   │  │  Operators   │  │  Expression   │  │   Physical     │  │
│  │  Plan Model │  │  (Exec)      │  │  Evaluator    │  │   Schema       │  │
│  └─────────────┘  └──────────────┘  └───────────────┘  └────────────────┘  │
│  ┌─────────────┐  ┌──────────────┐  ┌───────────────┐                      │
│  │  Operator   │  │  Schema      │  │   Memory &    │                      │
│  │  Traits     │  │  Inference   │  │   Metrics     │                      │
│  └─────────────┘  └──────────────┘  └───────────────┘                      │
└─────────────────────────────────────────────────────────────────────────────┘
                │                                    │
                ▼                                    ▼
┌───────────────────────────────┐    ┌───────────────────────────────────────┐
│      grism-local (Runtime)    │    │         grism-ray (Runtime)           │
│  ┌────────────────────────┐   │    │  ┌─────────────────────────────────┐  │
│  │   LocalExecutor        │   │    │  │   RayExecutor                   │  │
│  │   - Pull-based         │   │    │  │   - Stage-based                 │  │
│  │   - Single-threaded    │   │    │  │   - Parallel workers            │  │
│  └────────────────────────┘   │    │  └─────────────────────────────────┘  │
│  ┌────────────────────────┐   │    │  ┌─────────────────────────────────┐  │
│  │   LocalPhysicalPlanner │   │    │  │   DistributedPlanner            │  │
│  │   - No Exchange ops    │   │    │  │   - Exchange insertion          │  │
│  │   - Direct execution   │   │    │  │   - Stage splitting             │  │
│  └────────────────────────┘   │    │  └─────────────────────────────────┘  │
│  ┌────────────────────────┐   │    │  ┌─────────────────────────────────┐  │
│  │   ExecutionContext     │   │    │  │   StageExecutor                 │  │
│  │   - InMemoryStorage    │   │    │  │   - Ray task management         │  │
│  │   - LanceStorage       │   │    │  │   - Arrow IPC transport         │  │
│  └────────────────────────┘   │    │  └─────────────────────────────────┘  │
└───────────────────────────────┘    └───────────────────────────────────────┘
                │                                    │
                └────────────────┬───────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           grism-storage                                      │
│                  (InMemoryStorage, LanceStorage, Catalog)                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Crate Responsibilities

| Crate | Responsibility | Dependencies |
|-------|----------------|--------------|
| **grism-engine** | Physical plan model, operators, expressions, schema inference | grism-core, grism-logical |
| **grism-local** | Local single-machine execution runtime | grism-engine, grism-storage |
| **grism-ray** | Distributed Ray-based execution runtime | grism-engine, grism-storage, ray |

---

## 2. grism-engine (Common Components)

The `grism-engine` crate contains all **runtime-agnostic** components that both local and distributed execution share.

### 2.1 Physical Plan Model

```rust
// Core plan structures (runtime-agnostic)
pub struct PhysicalPlan {
    pub root: Box<dyn PhysicalOperator>,
    pub properties: PlanProperties,
}

pub struct PlanProperties {
    pub execution_mode: ExecutionMode,
    pub partitioning: PartitioningSpec,
    pub blocking: bool,
}
```

### 2.2 Physical Operators

All physical operators implement the `PhysicalOperator` trait:

```rust
#[async_trait]
pub trait PhysicalOperator: Send + Sync {
    /// Execute this operator, pulling from children
    async fn execute(
        &self,
        ctx: &dyn ExecutionContextTrait,
    ) -> Result<Box<dyn RecordBatchStream>, GrismError>;

    /// Operator's output schema
    fn schema(&self) -> &PhysicalSchema;

    /// Operator capabilities
    fn capabilities(&self) -> OperatorCaps;

    /// Child operators
    fn children(&self) -> Vec<&dyn PhysicalOperator>;
}
```

**Operators in grism-engine:**

| Operator | Description | Blocking |
|----------|-------------|----------|
| `NodeScanExec` | Scan nodes by label | No |
| `HyperedgeScanExec` | Scan hyperedges by label | No |
| `FilterExec` | Filter rows by predicate | No |
| `ProjectExec` | Project/compute columns | No |
| `LimitExec` | Limit rows with offset | No |
| `SortExec` | Multi-key sorting | **Yes** |
| `HashAggregateExec` | Aggregation with GROUP BY | **Yes** |
| `AdjacencyExpandExec` | Binary edge traversal | No |
| `RoleExpandExec` | N-ary hyperedge traversal | No |
| `UnionExec` | Union of two inputs | No |
| `RenameExec` | Rename columns | No |
| `EmptyExec` | Empty result | No |

### 2.3 Expression Evaluator

The `ExprEvaluator` converts `LogicalExpr` to Arrow arrays:

```rust
pub struct ExprEvaluator;

impl ExprEvaluator {
    pub fn evaluate(
        expr: &LogicalExpr,
        batch: &RecordBatch,
    ) -> Result<ArrayRef, GrismError>;
}
```

**Supported expressions:**
- Literals: Int64, Float64, String, Bool, Null
- Column references: Direct and qualified
- Binary operations: +, -, *, /, %, =, <>, <, <=, >, >=, AND, OR
- Unary operations: NOT, IS NULL, IS NOT NULL, NEG
- CASE expressions, IN lists, BETWEEN

### 2.4 Schema Inference

```rust
pub mod schema_inference {
    pub fn infer_expr_type(expr: &LogicalExpr, input_schema: &Schema) -> DataType;
    pub fn build_project_schema(exprs: &[LogicalExpr], input: &Schema) -> Schema;
    pub fn build_aggregate_schema(aggs: &[AggExpr], groups: &[LogicalExpr]) -> Schema;
}
```

### 2.5 Operator Capabilities

```rust
pub struct OperatorCaps {
    pub streaming: bool,
    pub blocking: bool,
    pub parallel_safe: bool,
    pub requires_partitioning: Option<PartitioningSpec>,
}
```

### 2.6 Memory & Metrics

```rust
// Memory management trait
pub trait MemoryManager: Send + Sync {
    fn try_reserve(&self, bytes: usize) -> Result<MemoryReservation, GrismError>;
    fn current_usage(&self) -> usize;
}

// Metrics collection trait
pub trait MetricsSink: Send + Sync {
    fn record_rows(&self, operator: &str, rows: usize);
    fn record_batches(&self, operator: &str, count: usize);
    fn record_time(&self, operator: &str, duration: Duration);
}
```

### 2.7 Execution Context Trait

```rust
/// Trait for execution context - implemented by both local and ray runtimes
#[async_trait]
pub trait ExecutionContextTrait: Send + Sync {
    fn storage(&self) -> &dyn Storage;
    fn snapshot_id(&self) -> SnapshotId;
    fn memory_manager(&self) -> &dyn MemoryManager;
    fn metrics_sink(&self) -> Option<&dyn MetricsSink>;
    fn is_cancelled(&self) -> bool;
}
```

---

## 3. grism-local (Local Runtime)

The `grism-local` crate provides single-machine execution with support for both in-memory and persistent storage.

### 3.1 LocalExecutor

```rust
pub struct LocalExecutor {
    config: RuntimeConfig,
}

impl LocalExecutor {
    pub fn new() -> Self;
    pub fn with_config(config: RuntimeConfig) -> Self;

    pub async fn execute(
        &self,
        plan: PhysicalPlan,
        storage: Arc<dyn Storage>,
        snapshot: SnapshotId,
    ) -> Result<ExecutionResult, GrismError>;
}
```

### 3.2 LocalPhysicalPlanner

```rust
pub struct LocalPhysicalPlanner {
    config: PlannerConfig,
}

impl PhysicalPlanner for LocalPhysicalPlanner {
    fn plan(&self, logical: &LogicalPlan) -> Result<PhysicalPlan, GrismError>;
}
```

**Key characteristics:**
- No Exchange operators inserted
- Direct operator tree execution
- Pull-based batch streaming

### 3.3 LocalExecutionContext

```rust
pub struct LocalExecutionContext {
    storage: Arc<dyn Storage>,
    snapshot_id: SnapshotId,
    memory_manager: Arc<dyn MemoryManager>,
    metrics_sink: Option<Arc<dyn MetricsSink>>,
    cancellation: CancellationHandle,
}

impl ExecutionContextTrait for LocalExecutionContext {
    // Implement all trait methods
}
```

### 3.4 Storage Support

| Storage Backend | Description | Use Case |
|-----------------|-------------|----------|
| `InMemoryStorage` | Hash-map based storage | Testing, small datasets |
| `LanceStorage` | Lance format file storage | Production, large datasets |

```rust
// Example usage with InMemoryStorage
let storage = InMemoryStorage::new();
let executor = LocalExecutor::new();
let result = executor.execute(plan, Arc::new(storage), SnapshotId::default()).await?;

// Example usage with LanceStorage
let storage = LanceStorage::open("/data/graph.lance").await?;
let executor = LocalExecutor::new();
let result = executor.execute(plan, Arc::new(storage), SnapshotId::default()).await?;
```

---

## 4. grism-ray (Distributed Runtime)

The `grism-ray` crate provides distributed execution using Ray as the orchestration layer.

### 4.1 Core Concepts

#### Exchange Operator

Exchange is a **first-class physical operator** that:
- Repartitions data
- Introduces a synchronization boundary
- Separates execution stages

```rust
pub struct ExchangeExec {
    pub partitioning: PartitioningSpec,
    pub mode: ExchangeMode,
    pub child: Box<dyn PhysicalOperator>,
}

pub enum ExchangeMode {
    Shuffle,    // repartition across workers
    Broadcast,  // replicate to all workers
    Gather,     // many → one
}
```

#### Execution Stage

A **Stage** is a connected sub-DAG of physical operators executed as a unit:

```rust
pub struct ExecutionStage {
    pub stage_id: StageId,
    pub plan: PhysicalSubPlan,
    pub input_partitioning: Option<PartitioningSpec>,
    pub output_partitioning: Option<PartitioningSpec>,
}
```

### 4.2 DistributedPlanner

```rust
pub struct DistributedPlanner {
    config: DistributedPlannerConfig,
}

impl DistributedPlanner {
    /// Plan a logical plan into distributed stages
    pub fn plan(&self, logical: &LogicalPlan) -> Result<DistributedPlan, GrismError>;

    /// Split physical plan into stages
    pub fn split_into_stages(&self, plan: PhysicalPlan) -> Vec<ExecutionStage>;
}
```

### 4.3 Partitioning Specification

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
    RoundRobin {
        partitions: usize,
    },
}
```

### 4.4 RayExecutor

```rust
pub struct RayExecutor {
    config: RayExecutorConfig,
}

impl RayExecutor {
    pub async fn execute(
        &self,
        stages: Vec<ExecutionStage>,
        storage: Arc<dyn Storage>,
    ) -> Result<ExecutionResult, GrismError>;
}
```

### 4.5 Stage Execution Flow

```text
┌──────────────────────────────────────────────────────────────────────┐
│                        DistributedPlan                               │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Stage 0 (parallel)          Stage 1 (parallel)       Stage 2       │
│  ┌─────────────────┐         ┌─────────────────┐    ┌──────────┐    │
│  │ Scan → Filter   │───────▶ │ Expand → Agg    │───▶│ Collect  │    │
│  │ → Project       │ Exchange│ (partial)       │    │ (final)  │    │
│  └─────────────────┘ (Hash)  └─────────────────┘    └──────────┘    │
│         │                           │                    │           │
│         ▼                           ▼                    ▼           │
│  ┌─────────────┐            ┌─────────────┐       ┌──────────┐      │
│  │ Worker 1    │            │ Worker 1    │       │ Driver   │      │
│  │ Worker 2    │            │ Worker 2    │       │          │      │
│  │ Worker 3    │            │ Worker 3    │       │          │      │
│  └─────────────┘            └─────────────┘       └──────────┘      │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 5. Stage Splitting Algorithm

### 5.1 Stage Boundary Rules

A **new stage MUST start** at:

1. Any `ExchangeExec` operator
2. Any **blocking operator** when distribution is enabled
3. Any operator that requires global state

### 5.2 Splitting Algorithm

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

## 6. Exchange Semantics

### 6.1 Guarantees

| Property | Guarantee |
|----------|-----------|
| Completeness | All rows forwarded |
| Disjointness | Each row appears once (except Broadcast) |
| Determinism | Same input → same partition |
| Barrier | Downstream waits for upstream readiness |

### 6.2 Exchange Execution Steps (Shuffle)

1. Upstream stage emits `RecordBatch`
2. Batch partitioned using `PartitioningSpec`
3. Sub-batches sent to Ray object store via Arrow IPC
4. Downstream tasks pull assigned partitions

---

## 7. Blocking Operators in Distributed Context

### 7.1 Aggregate (Two-Phase)

```text
Stage 0: PartialAggregate (parallel)
   ↓ Exchange(Hash by group key)
Stage 1: FinalAggregate
```

### 7.2 Sort (Two-Phase)

```text
Stage 0: LocalSort (parallel)
   ↓ Exchange(Range)
Stage 1: MergeSort
```

---

## 8. Expand-Aware Partitioning

### 8.1 Adjacency Partitioning

For graph traversal workloads, adjacency-based partitioning keeps most expansions local:

```rust
PartitioningSpec::Adjacency {
    entity: EntityType::Node,
}
```

### 8.2 N-ary Expand

N-ary hyperedge expansion often requires shuffle:

```text
Stage 0: Scan nodes
   ↓ Exchange(Adjacency)
Stage 1: RoleExpand (hyperedges co-located with nodes)
```

---

## 9. Control Plane vs Data Plane

| Plane | Responsibility | Owner |
|-------|----------------|-------|
| Control | Stage graph, scheduling, progress | Ray |
| Data | Arrow batch movement, execution | Rust |

**Ray orchestrates, Rust executes.**

---

## 10. Failure & Retry Semantics

Stage boundaries define **failure domains**:

| Failure | Recovery |
|---------|----------|
| Worker crash | Retry stage |
| Exchange failure | Re-run upstream stage |
| Blocking stage failure | Restart stage |

No partial stage result is reused unless idempotent.

---

## 11. Explainability

### 11.1 EXPLAIN DISTRIBUTED

```text
Stage 0:
  NodeScan(Person) → Filter(age > 21) → Project(name, age)
  Parallelism: 8
  Output: HashPartition(node_id)

    ↓ Exchange(Shuffle, Hash(node_id))

Stage 1:
  Aggregate(COUNT(*), GROUP BY city)
  Parallelism: 4
  Output: HashPartition(city)

    ↓ Exchange(Gather)

Stage 2:
  Collect
  Parallelism: 1
```

---

## 12. Migration Path

### 12.1 Current State

```
grism-engine/      # Contains both engine AND local runtime
grism-distributed/ # Partial Ray integration
```

### 12.2 Target State

```
grism-engine/      # Common: operators, expressions, schemas, traits
grism-local/       # Runtime: LocalExecutor, LocalPhysicalPlanner
grism-ray/         # Runtime: RayExecutor, DistributedPlanner, Exchange
```

### 12.3 Migration Steps

1. **Extract common traits** from `grism-engine` into stable interfaces
2. **Create `grism-local`** by moving `LocalExecutor`, `LocalPhysicalPlanner`, `ExecutionContext`
3. **Rename `grism-distributed`** to `grism-ray`
4. **Implement Exchange operator** in `grism-ray`
5. **Implement stage splitting** in `grism-ray`
6. **Keep current local engine functionality unchanged** - users should see no difference

---

## 13. API Compatibility

### 13.1 Local Execution (Unchanged)

```rust
// Before and after: identical API
use grism_local::{LocalExecutor, LocalPhysicalPlanner};

let planner = LocalPhysicalPlanner::new();
let plan = planner.plan(&logical_plan)?;

let executor = LocalExecutor::new();
let result = executor.execute(plan, storage, snapshot).await?;
```

### 13.2 Distributed Execution (New)

```rust
use grism_ray::{RayExecutor, DistributedPlanner};

let planner = DistributedPlanner::new();
let stages = planner.plan(&logical_plan)?;

let executor = RayExecutor::connect("ray://cluster:10001")?;
let result = executor.execute(stages, storage).await?;
```

---

## 14. Implementation Checklist

### Phase 1: Engine Separation
- [ ] Define `ExecutionContextTrait` in grism-engine
- [ ] Define `PhysicalPlanner` trait in grism-engine
- [ ] Ensure all operators depend only on traits, not concrete types

### Phase 2: Create grism-local
- [ ] Move `LocalExecutor` to grism-local
- [ ] Move `LocalPhysicalPlanner` to grism-local
- [ ] Move `ExecutionContext` implementation to grism-local
- [ ] Add re-exports for backward compatibility

### Phase 3: Rename to grism-ray
- [ ] Rename `grism-distributed` to `grism-ray`
- [ ] Update all imports and references
- [ ] Implement `ExchangeExec` operator
- [ ] Implement `DistributedPlanner` with stage splitting
- [ ] Implement Ray task submission

### Phase 4: Integration
- [ ] Integrate with Ray object store for Arrow IPC
- [ ] Implement two-phase aggregation
- [ ] Implement range-partitioned sort
- [ ] Add distributed EXPLAIN support

---

## 15. Summary

| Crate | What It Contains | Key Types |
|-------|------------------|-----------|
| **grism-engine** | Runtime-agnostic physical layer | `PhysicalPlan`, `PhysicalOperator`, `ExprEvaluator`, `OperatorCaps` |
| **grism-local** | Single-machine execution | `LocalExecutor`, `LocalPhysicalPlanner`, `LocalExecutionContext` |
| **grism-ray** | Distributed Ray execution | `RayExecutor`, `DistributedPlanner`, `ExchangeExec`, `ExecutionStage` |

### Architectural Guarantees

1. **Zero semantic drift**: Local and distributed execution produce identical results
2. **Local engine unchanged**: Current local execution API remains stable
3. **Explicit distribution**: Exchange operators are visible in plans
4. **Hypergraph-correct**: Expand operators preserve adjacency semantics
5. **Explainable**: Distributed plans can be inspected and debugged

> **Exchange is the only way data moves across stages.**
> **Stages are the only unit of distribution.**
> **Operators never know they are distributed.**
