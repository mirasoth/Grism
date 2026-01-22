# Implementation: Engine-Runtime Separation

*(Implementation guide for RFC-0102: Execution Engine Architecture)*

---

## 1. Overview

This document provides implementation-specific details for the engine architecture defined in **RFC-0102**. It covers:

* Current code structure and status
* Migration path to target architecture
* Implementation considerations and technical decisions
* Concrete code examples and patterns

**For architectural design, see [RFC-0102: Execution Engine Architecture](rfc-0102.md).**

---

## 2. Current Code Status

### 2.1 Existing Crate Structure

```
src/
├── grism-engine/          # Currently contains BOTH engine AND local runtime
│   ├── executor/          # LocalExecutor, ExecutionContext
│   ├── expr/              # ExprEvaluator
│   ├── memory/            # MemoryManager implementations
│   ├── metrics/           # MetricsSink
│   ├── operators/         # All physical operators (*Exec)
│   ├── physical/          # PhysicalPlan, PhysicalSchema, PlanProperties
│   └── planner/           # LocalPhysicalPlanner, schema_inference
│
├── grism-distributed/     # Partial Ray integration (to be renamed grism-ray)
│   ├── planner/           # RayPlanner, Stage definitions
│   ├── transport/         # Arrow IPC transport
│   └── worker/            # Worker task definitions
```

### 2.2 What Works Today

| Component | Status | Notes |
|-----------|--------|-------|
| Physical Plan Model | ✅ Complete | `PhysicalPlan`, `PhysicalSchema`, `PlanProperties` |
| All Physical Operators | ✅ Complete | 12 operators fully implemented and tested |
| Expression Evaluator | ✅ Complete | Full support for all expression types |
| Schema Inference | ✅ Complete | Type inference for all operators |
| LocalExecutor | ✅ Complete | Pull-based pipeline execution |
| LocalPhysicalPlanner | ✅ Complete | Logical to physical conversion |
| Memory Management | ✅ Complete | `MemoryManager` trait with implementations |
| Metrics Collection | ✅ Complete | `MetricsSink` trait |
| InMemoryStorage | ✅ Complete | For testing |
| Ray Stage Definitions | ⚠️ Partial | Basic Stage and StageId types |
| Exchange Operator | ❌ Not Started | Required for distributed execution |
| DistributedPlanner | ❌ Not Started | Stage splitting not implemented |
| RayExecutor | ❌ Not Started | Ray task submission not implemented |

### 2.3 Test Coverage

```
grism-engine: 97 unit tests + 33 integration tests (all passing)
grism-distributed: 0 tests (skeleton only)
```

---

## 3. Migration Plan

### 3.1 Phase 1: Extract Common Traits

**Goal**: Define runtime-agnostic interfaces in `grism-engine`.

**Tasks**:

1. Define `ExecutionContextTrait` in `grism-engine`:

```rust
// src/grism-engine/src/executor/traits.rs

/// Runtime-agnostic execution context trait
#[async_trait]
pub trait ExecutionContextTrait: Send + Sync {
    /// Access to storage layer
    fn storage(&self) -> &dyn Storage;
    
    /// Current snapshot for consistent reads
    fn snapshot_id(&self) -> SnapshotId;
    
    /// Memory management interface
    fn memory_manager(&self) -> &dyn MemoryManager;
    
    /// Optional metrics collection
    fn metrics_sink(&self) -> Option<&dyn MetricsSink>;
    
    /// Check if execution has been cancelled
    fn is_cancelled(&self) -> bool;
}
```

2. Define `PhysicalPlanner` trait in `grism-engine`:

```rust
// src/grism-engine/src/planner/traits.rs

/// Runtime-agnostic physical planner trait
pub trait PhysicalPlanner {
    type Output;
    
    /// Convert logical plan to physical representation
    fn plan(&self, logical: &LogicalPlan) -> Result<Self::Output, GrismError>;
}
```

3. Update `PhysicalOperator` trait to use trait objects:

```rust
// Change from concrete ExecutionContext to trait
#[async_trait]
pub trait PhysicalOperator: Send + Sync {
    async fn execute(
        &self,
        ctx: &dyn ExecutionContextTrait,  // Changed from &ExecutionContext
    ) -> Result<Box<dyn RecordBatchStream>, GrismError>;
    
    // ... rest unchanged
}
```

**Files to Modify**:
- `src/grism-engine/src/executor/mod.rs` - Add traits module
- `src/grism-engine/src/operators/traits.rs` - Update operator trait
- All operator files - Update execute() signature

### 3.2 Phase 2: Create grism-local

**Goal**: Move local runtime components to dedicated crate.

**Tasks**:

1. Create new crate structure:

```
src/grism-local/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── executor.rs      # LocalExecutor (moved from grism-engine)
    ├── context.rs       # LocalExecutionContext (moved from grism-engine)
    ├── planner.rs       # LocalPhysicalPlanner (moved from grism-engine)
    └── result.rs        # ExecutionResult (moved from grism-engine)
```

2. Update `Cargo.toml` for grism-local:

```toml
[package]
name = "grism-local"
version = "0.1.0"
edition = "2021"

[dependencies]
grism-engine = { path = "../grism-engine" }
grism-storage = { path = "../grism-storage" }
grism-logical = { path = "../grism-logical" }
arrow = "53"
async-trait = "0.1"
tokio = { version = "1", features = ["rt"] }
```

3. Implement `LocalExecutionContext`:

```rust
// src/grism-local/src/context.rs

pub struct LocalExecutionContext {
    storage: Arc<dyn Storage>,
    snapshot_id: SnapshotId,
    memory_manager: Arc<dyn MemoryManager>,
    metrics_sink: Option<Arc<dyn MetricsSink>>,
    cancellation: CancellationHandle,
}

impl ExecutionContextTrait for LocalExecutionContext {
    fn storage(&self) -> &dyn Storage {
        self.storage.as_ref()
    }
    
    fn snapshot_id(&self) -> SnapshotId {
        self.snapshot_id
    }
    
    fn memory_manager(&self) -> &dyn MemoryManager {
        self.memory_manager.as_ref()
    }
    
    fn metrics_sink(&self) -> Option<&dyn MetricsSink> {
        self.metrics_sink.as_ref().map(|s| s.as_ref())
    }
    
    fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}
```

4. Add re-exports for backward compatibility in grism-engine:

```rust
// src/grism-engine/src/lib.rs

// Re-export from grism-local for backward compatibility
// TODO: Add deprecation warnings in future version
#[cfg(feature = "local-runtime")]
pub use grism_local::{LocalExecutor, LocalPhysicalPlanner, ExecutionContext};
```

### 3.3 Phase 3: Rename grism-distributed to grism-ray

**Goal**: Rename and restructure for Ray-specific implementation.

**Tasks**:

1. Rename directory:

```bash
git mv src/grism-distributed src/grism-ray
```

2. Update `Cargo.toml`:

```toml
[package]
name = "grism-ray"
version = "0.1.0"
edition = "2021"

[dependencies]
grism-engine = { path = "../grism-engine" }
grism-storage = { path = "../grism-storage" }
grism-logical = { path = "../grism-logical" }
arrow = "53"
arrow-ipc = "53"
async-trait = "0.1"
tokio = { version = "1", features = ["rt", "net"] }
# ray = "..." # Ray Rust bindings when available
```

3. Update workspace `Cargo.toml`:

```toml
[workspace]
members = [
    "src/grism-core",
    "src/grism-logical",
    "src/grism-optimizer",
    "src/grism-engine",
    "src/grism-local",      # New
    "src/grism-ray",        # Renamed from grism-distributed
    "src/grism-storage",
    # ...
]
```

4. Update all imports across the codebase.

### 3.4 Phase 4: Implement Exchange Operator

**Goal**: Add Exchange operator for data repartitioning.

**Tasks**:

1. Add Exchange types to grism-ray:

```rust
// src/grism-ray/src/operators/exchange.rs

/// Exchange modes for data repartitioning
#[derive(Debug, Clone)]
pub enum ExchangeMode {
    /// Repartition data by hash of keys
    Shuffle,
    /// Replicate data to all workers
    Broadcast,
    /// Collect all data to coordinator
    Gather,
}

/// Exchange operator for distributed data movement
pub struct ExchangeExec {
    pub child: Arc<dyn PhysicalOperator>,
    pub partitioning: PartitioningSpec,
    pub mode: ExchangeMode,
}

impl PhysicalOperator for ExchangeExec {
    async fn execute(
        &self,
        ctx: &dyn ExecutionContextTrait,
    ) -> Result<Box<dyn RecordBatchStream>, GrismError> {
        // In local execution, Exchange is a no-op passthrough
        // In distributed execution, this is handled by the stage executor
        self.child.execute(ctx).await
    }
    
    fn schema(&self) -> &PhysicalSchema {
        self.child.schema()
    }
    
    fn capabilities(&self) -> OperatorCaps {
        OperatorCaps {
            streaming: false,  // Exchange is a barrier
            blocking: true,
            parallel_safe: true,
            requires_partitioning: Some(self.partitioning.clone()),
        }
    }
    
    fn children(&self) -> Vec<&dyn PhysicalOperator> {
        vec![self.child.as_ref()]
    }
}
```

2. Add partitioning specification:

```rust
// src/grism-ray/src/planner/partitioning.rs

/// Specification for how data is partitioned
#[derive(Debug, Clone)]
pub enum PartitioningSpec {
    /// Hash partitioning by key columns
    Hash {
        keys: Vec<String>,
        num_partitions: usize,
    },
    /// Range partitioning by key
    Range {
        key: String,
        ranges: Vec<(Value, Value)>,
    },
    /// Partitioning by graph adjacency (keeps neighbors together)
    Adjacency {
        entity_type: EntityType,
    },
    /// Single partition (no distribution)
    Single,
    /// Round-robin distribution
    RoundRobin {
        num_partitions: usize,
    },
}

impl PartitioningSpec {
    /// Calculate partition for a given row
    pub fn partition_for(&self, batch: &RecordBatch, row: usize) -> usize {
        match self {
            Self::Hash { keys, num_partitions } => {
                let mut hasher = DefaultHasher::new();
                for key in keys {
                    // Hash the value at this column
                    let col = batch.column_by_name(key).unwrap();
                    hash_array_value(col, row, &mut hasher);
                }
                (hasher.finish() as usize) % num_partitions
            }
            Self::Single => 0,
            Self::RoundRobin { num_partitions } => row % num_partitions,
            // ... other cases
        }
    }
}
```

### 3.5 Phase 5: Implement Stage Splitting

**Goal**: Implement algorithm to split physical plans into stages.

**Tasks**:

1. Implement stage builder:

```rust
// src/grism-ray/src/planner/stage.rs

/// Unique identifier for an execution stage
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StageId(pub usize);

/// An execution stage containing a sub-plan
pub struct ExecutionStage {
    pub stage_id: StageId,
    pub root: Arc<dyn PhysicalOperator>,
    pub input_partitioning: Option<PartitioningSpec>,
    pub output_partitioning: Option<PartitioningSpec>,
}

/// Builder for constructing stages
pub struct StageBuilder {
    operators: Vec<Arc<dyn PhysicalOperator>>,
    input_partitioning: Option<PartitioningSpec>,
}

impl StageBuilder {
    pub fn new() -> Self {
        Self {
            operators: vec![],
            input_partitioning: None,
        }
    }
    
    pub fn with_input_partitioning(mut self, spec: PartitioningSpec) -> Self {
        self.input_partitioning = Some(spec);
        self
    }
    
    pub fn add(&mut self, op: Arc<dyn PhysicalOperator>) {
        self.operators.push(op);
    }
    
    pub fn finish(self, stage_id: StageId) -> ExecutionStage {
        // Build operator tree from collected operators
        let root = self.build_tree();
        ExecutionStage {
            stage_id,
            root,
            input_partitioning: self.input_partitioning,
            output_partitioning: None, // Set by planner
        }
    }
}
```

2. Implement stage splitting algorithm:

```rust
// src/grism-ray/src/planner/splitter.rs

pub struct StageSplitter;

impl StageSplitter {
    /// Split a physical plan into execution stages
    pub fn split(plan: &PhysicalPlan) -> Vec<ExecutionStage> {
        let mut stages = vec![];
        let mut current = StageBuilder::new();
        let mut stage_id = 0;
        
        // Topological traversal of operator tree
        for node in Self::topo_order(&plan.root) {
            if Self::is_stage_boundary(node.as_ref()) {
                // Finish current stage
                if !current.is_empty() {
                    stages.push(current.finish(StageId(stage_id)));
                    stage_id += 1;
                }
                
                // Handle Exchange specially
                if let Some(exchange) = node.as_any().downcast_ref::<ExchangeExec>() {
                    // Exchange defines output partitioning of previous stage
                    // and input partitioning of next stage
                    if let Some(last) = stages.last_mut() {
                        last.output_partitioning = Some(exchange.partitioning.clone());
                    }
                    current = StageBuilder::new()
                        .with_input_partitioning(exchange.partitioning.clone());
                } else {
                    current = StageBuilder::new();
                }
            }
            
            current.add(node.clone());
        }
        
        // Final stage
        if !current.is_empty() {
            stages.push(current.finish(StageId(stage_id)));
        }
        
        stages
    }
    
    fn is_stage_boundary(op: &dyn PhysicalOperator) -> bool {
        // Exchange always creates a boundary
        if op.as_any().is::<ExchangeExec>() {
            return true;
        }
        
        // Blocking operators create boundaries in distributed mode
        let caps = op.capabilities();
        caps.blocking
    }
}
```

### 3.6 Phase 6: Implement Distributed Planner

**Goal**: Create DistributedPlanner that inserts Exchange operators.

```rust
// src/grism-ray/src/planner/distributed.rs

pub struct DistributedPlanner {
    pub config: DistributedPlannerConfig,
}

pub struct DistributedPlannerConfig {
    pub default_parallelism: usize,
    pub prefer_adjacency_partitioning: bool,
}

impl DistributedPlanner {
    /// Plan a logical plan for distributed execution
    pub fn plan(&self, logical: &LogicalPlan) -> Result<DistributedPlan, GrismError> {
        // First, create a basic physical plan
        let physical = self.create_physical_plan(logical)?;
        
        // Insert Exchange operators where needed
        let with_exchanges = self.insert_exchanges(physical)?;
        
        // Split into stages
        let stages = StageSplitter::split(&with_exchanges);
        
        Ok(DistributedPlan { stages })
    }
    
    fn insert_exchanges(&self, plan: PhysicalPlan) -> Result<PhysicalPlan, GrismError> {
        // Walk the plan and insert Exchange operators at:
        // 1. Before blocking operators (Aggregate, Sort)
        // 2. Before Expand operators that need repartitioning
        // 3. Before the final Collect
        
        self.transform_plan(&plan.root, None)
    }
    
    fn transform_plan(
        &self,
        op: &Arc<dyn PhysicalOperator>,
        required_partitioning: Option<&PartitioningSpec>,
    ) -> Result<PhysicalPlan, GrismError> {
        // Check if we need to insert an Exchange
        let current_partitioning = self.infer_partitioning(op);
        
        let result = if let Some(required) = required_partitioning {
            if !self.partitioning_satisfies(&current_partitioning, required) {
                // Need Exchange
                Arc::new(ExchangeExec {
                    child: op.clone(),
                    partitioning: required.clone(),
                    mode: ExchangeMode::Shuffle,
                })
            } else {
                op.clone()
            }
        } else {
            op.clone()
        };
        
        // ... recursively transform children
        Ok(PhysicalPlan::new(result))
    }
}
```

---

## 4. Implementation Considerations

### 4.1 Backward Compatibility

To maintain backward compatibility during migration:

1. **Feature Flags**: Use Cargo features to control crate dependencies

```toml
# grism-engine/Cargo.toml
[features]
default = ["local-runtime"]
local-runtime = ["grism-local"]
```

2. **Re-exports**: Re-export moved types from original locations with deprecation warnings

3. **Version Pinning**: Maintain API compatibility within minor versions

### 4.2 Testing Strategy

1. **Unit Tests**: Each component has comprehensive unit tests
2. **Integration Tests**: End-to-end tests for both local and distributed execution
3. **Equivalence Tests**: Verify local and distributed produce identical results

```rust
#[test]
fn test_local_distributed_equivalence() {
    let logical_plan = create_test_plan();
    
    // Execute locally
    let local_result = LocalExecutor::new()
        .execute(local_plan, storage.clone())
        .await?;
    
    // Execute distributed (single worker for comparison)
    let ray_result = RayExecutor::new_local()
        .execute(distributed_plan, storage.clone())
        .await?;
    
    assert_batches_equal(&local_result.batches, &ray_result.batches);
}
```

### 4.3 Performance Considerations

1. **Zero-Copy Where Possible**: Use Arrow's zero-copy semantics
2. **Batch Size Tuning**: Configurable batch sizes for different workloads
3. **Memory Budgets**: Respect memory limits in blocking operators
4. **Metrics Collection**: Optional to avoid overhead in production

### 4.4 Error Handling

```rust
/// Execution-layer errors
#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    #[error("Memory limit exceeded: requested {requested}, limit {limit}")]
    MemoryExceeded { requested: usize, limit: usize },
    
    #[error("Execution cancelled")]
    Cancelled,
    
    #[error("Stage {0} failed: {1}")]
    StageFailed(StageId, String),
    
    #[error("Exchange failed: {0}")]
    ExchangeFailed(String),
    
    #[error("Worker {0} unreachable")]
    WorkerUnreachable(String),
}
```

---

## 5. File Changes Summary

### New Files to Create

| File | Description |
|------|-------------|
| `src/grism-local/Cargo.toml` | New crate manifest |
| `src/grism-local/src/lib.rs` | Crate entry point |
| `src/grism-local/src/executor.rs` | LocalExecutor |
| `src/grism-local/src/context.rs` | LocalExecutionContext |
| `src/grism-local/src/planner.rs` | LocalPhysicalPlanner |
| `src/grism-engine/src/executor/traits.rs` | ExecutionContextTrait |
| `src/grism-engine/src/planner/traits.rs` | PhysicalPlanner trait |
| `src/grism-ray/src/operators/exchange.rs` | ExchangeExec |
| `src/grism-ray/src/planner/distributed.rs` | DistributedPlanner |
| `src/grism-ray/src/planner/splitter.rs` | StageSplitter |

### Files to Modify

| File | Changes |
|------|---------|
| `Cargo.toml` (workspace) | Add new crate members |
| `src/grism-engine/src/lib.rs` | Export traits, add re-exports |
| `src/grism-engine/src/operators/*.rs` | Update execute() signatures |
| `src/grism-ray/Cargo.toml` | Rename from grism-distributed |
| `src/grism-ray/src/lib.rs` | Update exports |

### Files to Move

| From | To |
|------|-----|
| `src/grism-engine/src/executor/local.rs` | `src/grism-local/src/executor.rs` |
| `src/grism-engine/src/executor/context.rs` | `src/grism-local/src/context.rs` |
| `src/grism-engine/src/executor/result.rs` | `src/grism-local/src/result.rs` |
| `src/grism-engine/src/planner/local.rs` | `src/grism-local/src/planner.rs` |

---

## 6. Implementation Checklist

### Phase 1: Extract Common Traits
- [ ] Define `ExecutionContextTrait` in grism-engine
- [ ] Define `PhysicalPlanner` trait in grism-engine
- [ ] Update `PhysicalOperator` trait to use `dyn ExecutionContextTrait`
- [ ] Update all operator implementations
- [ ] Verify all tests pass

### Phase 2: Create grism-local
- [ ] Create `src/grism-local/` directory structure
- [ ] Create `Cargo.toml` with dependencies
- [ ] Move `LocalExecutor` from grism-engine
- [ ] Move `LocalPhysicalPlanner` from grism-engine
- [ ] Move `ExecutionContext` as `LocalExecutionContext`
- [ ] Implement `ExecutionContextTrait` for `LocalExecutionContext`
- [ ] Add re-exports to grism-engine for compatibility
- [ ] Update workspace Cargo.toml
- [ ] Verify all tests pass

### Phase 3: Rename to grism-ray
- [ ] Rename directory from grism-distributed to grism-ray
- [ ] Update package name in Cargo.toml
- [ ] Update workspace Cargo.toml
- [ ] Update all imports across codebase
- [ ] Verify compilation

### Phase 4: Implement Exchange
- [ ] Add `ExchangeMode` enum
- [ ] Add `PartitioningSpec` enum
- [ ] Implement `ExchangeExec` operator
- [ ] Add hash partitioning logic
- [ ] Add unit tests for Exchange

### Phase 5: Implement Stage Splitting
- [ ] Implement `StageBuilder`
- [ ] Implement `StageSplitter`
- [ ] Add unit tests for stage splitting
- [ ] Verify correct boundary detection

### Phase 6: Implement Distributed Planner
- [ ] Implement `DistributedPlanner`
- [ ] Implement Exchange insertion logic
- [ ] Implement partitioning inference
- [ ] Add integration tests

### Phase 7: Implement Ray Integration
- [ ] Add Ray task submission
- [ ] Implement Arrow IPC transport
- [ ] Implement stage execution on workers
- [ ] Add distributed execution tests

---

## 7. References

* **RFC-0102**: Execution Engine Architecture (authoritative design)
* **RFC-0008**: Physical Plan & Operator Interfaces
* **RFC-0010**: Distributed & Parallel Execution
* **RFC-0100**: Architecture Design Document
