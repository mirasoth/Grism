# Physical Planning & Local Execution Engine Implementation

**Status**: Implementation Spec  
**Phase**: 3 (Execution Engine)  
**Week**: 7  
**Depends on**: RFC-0002, RFC-0006, RFC-0007, RFC-0008, RFC-0009  
**Prepares for**: RFC-0010 (Distributed Execution)  

---

## 1. Overview

This document specifies the implementation of the **Physical Planning** and **Local Execution Engine** for Grism. It is the implementation guide for Phase 3, Week 7 of the development schedule.

### 1.1 Normative References

This implementation MUST comply with:

- **RFC-0008**: Physical Plan & Operator Interfaces (primary normative spec)
- **RFC-namings**: Canonical naming conventions
- **RFC-0100**: Architecture Design Document

### 1.2 Goals

Week 7 delivers:

- A **PhysicalPlan** representation distinct from LogicalPlan
- A **PhysicalPlanner** that compiles LogicalPlan to PhysicalPlan
- A **LocalExecutor** that executes PhysicalPlan on a single node
- **Arrow-native** data flow using `RecordBatch`
- **Expand-centric** graph execution with correct hyperedge semantics
- A **stable operator contract** that will not change when distribution is added

### 1.3 Non-Goals (Deferred to Week 8 / RFC-0010)

- Network shuffle and Exchange operator execution
- Cluster scheduling and distributed coordination
- Fault-tolerant retry and recovery
- Cross-node partitioning

These are **explicitly anticipated** but not implemented.

---

## 2. Architecture Overview

### 2.1 Planning and Execution Flow

```
LogicalPlan
    │
    ▼
┌─────────────────────┐
│  Optimizer          │  (grism-optimizer, already implemented)
│  (Rewrite Rules)    │
└─────────────────────┘
    │
    ▼
OptimizedLogicalPlan
    │
    ▼
┌─────────────────────┐
│  PhysicalPlanner    │  (grism-engine/physical/planner.rs)
│  (LocalPhysical     │
│   Planner)          │
└─────────────────────┘
    │
    ▼
PhysicalPlan
    │
    ▼
┌─────────────────────┐
│  LocalExecutor      │  (grism-engine/executor/local.rs)
│  (Pull-based        │
│   Pipeline)         │
└─────────────────────┘
    │
    ▼
Arrow RecordBatch Stream
```

### 2.2 Layer Responsibilities

| Layer | Responsibility | Output |
|-------|---------------|--------|
| LogicalPlan | What to compute | DAG of LogicalOp |
| PhysicalPlanner | How to compute locally | DAG of PhysicalOp |
| LocalExecutor | Execute the plan | Stream of RecordBatch |

---

## 3. Physical Plan Model

### 3.1 PhysicalPlan Structure

```rust
use std::sync::Arc;

/// Unique identifier for a node in the physical plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PhysicalNodeId(pub(crate) usize);

/// A physical execution plan.
///
/// Physical plans are DAGs of physical operators that can be directly executed.
/// Unlike logical plans, physical plans are fully resolved and backend-specific.
#[derive(Debug)]
pub struct PhysicalPlan {
    /// The root operator of this plan.
    root: Arc<dyn PhysicalOperator>,
    /// Plan-level properties.
    properties: PlanProperties,
    /// Output schema.
    schema: PhysicalSchema,
}

impl PhysicalPlan {
    /// Create a new physical plan with the given root operator.
    pub fn new(root: Arc<dyn PhysicalOperator>) -> Self {
        let schema = root.schema().clone();
        Self {
            root,
            properties: PlanProperties::default(),
            schema,
        }
    }

    /// Get the root operator.
    pub fn root(&self) -> &Arc<dyn PhysicalOperator> {
        &self.root
    }

    /// Get plan properties.
    pub fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    /// Get output schema.
    pub fn schema(&self) -> &PhysicalSchema {
        &self.schema
    }

    /// Generate EXPLAIN output.
    pub fn explain(&self) -> String {
        self.root.explain(0)
    }

    /// Generate verbose EXPLAIN output with schema.
    pub fn explain_verbose(&self) -> String {
        let mut output = self.explain();
        output.push_str("\n\nOutput Schema:\n");
        output.push_str(&format!("{}", self.schema));
        output
    }
}
```

### 3.2 Plan Properties

```rust
/// Execution mode for physical plans.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExecutionMode {
    /// Single-node local execution.
    #[default]
    Local,
    /// Distributed execution (future).
    Distributed,
}

/// Properties of a physical plan.
///
/// These properties inform the executor about plan characteristics
/// and enable future distributed execution.
#[derive(Debug, Clone, Default)]
pub struct PlanProperties {
    /// Execution mode.
    pub execution_mode: ExecutionMode,
    /// Whether the plan requires shuffle (always false for local).
    pub requires_shuffle: bool,
    /// Whether the plan contains blocking operators.
    pub contains_blocking: bool,
    /// Partitioning specification (placeholder for RFC-0010).
    pub partitioning: Option<PartitioningSpec>,
}

/// Partitioning specification (placeholder for RFC-0010).
///
/// This type exists to maintain forward compatibility with distributed
/// execution but is not used in Week 7.
#[derive(Debug, Clone)]
pub struct PartitioningSpec {
    /// Partitioning strategy.
    pub strategy: PartitioningStrategy,
    /// Number of partitions.
    pub num_partitions: usize,
}

/// Partitioning strategies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitioningStrategy {
    /// Single partition (local execution).
    Single,
    /// Hash partitioning by keys.
    Hash,
    /// Range partitioning.
    Range,
    /// Round-robin distribution.
    RoundRobin,
}
```

### 3.3 Physical Schema

Physical schemas are Arrow-based and include graph metadata.

```rust
use arrow::datatypes::{Schema as ArrowSchema, SchemaRef};
use std::collections::HashMap;

/// Physical schema for operator output.
///
/// Wraps Arrow schema with additional graph metadata.
#[derive(Debug, Clone)]
pub struct PhysicalSchema {
    /// Arrow schema.
    arrow_schema: SchemaRef,
    /// Entity qualifiers for columns (label or alias).
    qualifiers: HashMap<String, String>,
}

impl PhysicalSchema {
    /// Create from Arrow schema.
    pub fn new(arrow_schema: SchemaRef) -> Self {
        Self {
            arrow_schema,
            qualifiers: HashMap::new(),
        }
    }

    /// Create with qualifiers.
    pub fn with_qualifiers(
        arrow_schema: SchemaRef,
        qualifiers: HashMap<String, String>,
    ) -> Self {
        Self {
            arrow_schema,
            qualifiers,
        }
    }

    /// Get the Arrow schema.
    pub fn arrow_schema(&self) -> &SchemaRef {
        &self.arrow_schema
    }

    /// Get column qualifier.
    pub fn qualifier(&self, column: &str) -> Option<&str> {
        self.qualifiers.get(column).map(|s| s.as_str())
    }

    /// Get field by name.
    pub fn field(&self, name: &str) -> Option<&arrow::datatypes::Field> {
        self.arrow_schema.field_with_name(name).ok()
    }

    /// Number of columns.
    pub fn num_columns(&self) -> usize {
        self.arrow_schema.fields().len()
    }
}

impl std::fmt::Display for PhysicalSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for field in self.arrow_schema.fields() {
            let qualifier = self.qualifiers.get(field.name())
                .map(|q| format!("{}.", q))
                .unwrap_or_default();
            writeln!(
                f,
                "  {}{}: {} {}",
                qualifier,
                field.name(),
                field.data_type(),
                if field.is_nullable() { "(nullable)" } else { "" }
            )?;
        }
        Ok(())
    }
}
```

---

## 4. Physical Operator Interface

### 4.1 PhysicalOperator Trait

This is the **core execution contract** per RFC-0008 Section 7.1.

```rust
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use common_error::GrismResult;

/// Trait for physical operators in the execution plan.
///
/// Physical operators form a tree that processes data in a **pull-based** fashion.
/// Each call to `next()` returns the next batch of results.
///
/// # Lifecycle (RFC-0008, Section 5.2)
///
/// ```text
/// create → open → next* → close
/// ```
///
/// - `open()` initializes resources and child operators
/// - `next()` returns batches until exhausted (returns None)
/// - `close()` releases resources (MUST be idempotent)
///
/// # Contract
///
/// Operators MUST NOT:
/// - Mutate upstream data
/// - Perform side effects outside ExecutionContext
/// - Block indefinitely without yielding
#[async_trait]
pub trait PhysicalOperator: Send + Sync + std::fmt::Debug {
    /// Get the operator name for display.
    fn name(&self) -> &'static str;

    /// Get the output schema.
    fn schema(&self) -> &PhysicalSchema;

    /// Get operator capabilities.
    fn capabilities(&self) -> OperatorCaps;

    /// Get child operators.
    fn children(&self) -> Vec<&Arc<dyn PhysicalOperator>>;

    /// Initialize the operator and its children.
    ///
    /// Called once before the first `next()`. Opens resources,
    /// initializes state, and recursively opens children.
    async fn open(&mut self, ctx: &ExecutionContext) -> GrismResult<()>;

    /// Get the next batch of results.
    ///
    /// Returns `Ok(Some(batch))` while data is available.
    /// Returns `Ok(None)` when exhausted.
    /// Returns `Err(_)` on failure (aborts pipeline).
    async fn next(&mut self) -> GrismResult<Option<RecordBatch>>;

    /// Close the operator and release resources.
    ///
    /// MUST be called after execution completes or on error.
    /// MUST be idempotent (safe to call multiple times).
    async fn close(&mut self) -> GrismResult<()>;

    /// Generate EXPLAIN output at given indentation level.
    fn explain(&self, indent: usize) -> String {
        let prefix = "  ".repeat(indent);
        let mut output = format!("{}{}\n", prefix, self.display());
        for child in self.children() {
            output.push_str(&child.explain(indent + 1));
        }
        output
    }

    /// Display string for EXPLAIN.
    fn display(&self) -> String {
        self.name().to_string()
    }
}

/// Boxed physical operator for dynamic dispatch.
pub type BoxedPhysicalOperator = Arc<dyn PhysicalOperator>;
```

### 4.2 Operator Capabilities

```rust
/// Capabilities and properties of a physical operator.
#[derive(Debug, Clone, Default)]
pub struct OperatorCaps {
    /// Whether this operator is blocking (must consume all input before producing output).
    pub blocking: bool,
    /// Whether this operator requires a global view of data (cannot be parallelized).
    pub requires_global_view: bool,
    /// Whether this operator supports predicate pushdown.
    pub supports_predicate_pushdown: bool,
    /// Whether this operator supports projection pushdown.
    pub supports_projection_pushdown: bool,
    /// Whether this operator is stateless (can be restarted without loss).
    pub stateless: bool,
}

impl OperatorCaps {
    /// Create capabilities for a streaming (non-blocking) operator.
    pub fn streaming() -> Self {
        Self {
            blocking: false,
            requires_global_view: false,
            stateless: true,
            ..Default::default()
        }
    }

    /// Create capabilities for a blocking operator.
    pub fn blocking() -> Self {
        Self {
            blocking: true,
            requires_global_view: true,
            stateless: false,
            ..Default::default()
        }
    }

    /// Create capabilities for a source operator.
    pub fn source() -> Self {
        Self {
            blocking: false,
            requires_global_view: false,
            supports_predicate_pushdown: true,
            supports_projection_pushdown: true,
            stateless: true,
        }
    }
}
```

### 4.3 Operator Categories

| Category | Blocking | Distribution Impact | Examples |
|----------|----------|--------------------| ---------|
| Source | No | Partitionable | `NodeScanExec`, `HyperedgeScanExec` |
| Unary | No | Stateless | `FilterExec`, `ProjectExec`, `LimitExec` |
| Expand | No | Partition-sensitive | `AdjacencyExpandExec`, `RoleExpandExec` |
| Blocking | Yes | Barrier | `HashAggregateExec`, `SortExec` |
| Binary | No | Requires alignment | `UnionExec` |
| Sink | Yes | Terminal | `CollectExec` |

Blocking operators become **stage boundaries** in distributed execution (Week 8).

---

## 5. Execution Context

### 5.1 ExecutionContext Definition

```rust
use grism_storage::{Snapshot, SnapshotId, Storage};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Runtime configuration for execution.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Batch size for operator processing.
    pub batch_size: usize,
    /// Memory limit in bytes (0 = unlimited).
    pub memory_limit: usize,
    /// Enable metrics collection.
    pub collect_metrics: bool,
    /// Parallelism level (unused in Week 7).
    pub parallelism: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            memory_limit: 0,
            collect_metrics: true,
            parallelism: 1,
        }
    }
}

/// Execution context passed to all operators.
///
/// The context is **read-only** to operators and shared across the pipeline.
/// Per RFC-0008, Section 5.1.
#[derive(Clone)]
pub struct ExecutionContext {
    /// Snapshot for consistent reads.
    pub snapshot: SnapshotId,
    /// Storage backend handle.
    pub storage: Arc<dyn Storage>,
    /// Memory manager for accounting.
    pub memory: Arc<dyn MemoryManager>,
    /// Cancellation token for cooperative cancellation.
    pub cancel: CancellationToken,
    /// Metrics sink for operator statistics.
    pub metrics: MetricsSink,
    /// Runtime configuration.
    pub config: RuntimeConfig,
}

impl ExecutionContext {
    /// Create a new execution context.
    pub fn new(
        storage: Arc<dyn Storage>,
        snapshot: SnapshotId,
    ) -> Self {
        Self {
            snapshot,
            storage,
            memory: Arc::new(NoopMemoryManager),
            cancel: CancellationToken::new(),
            metrics: MetricsSink::new(),
            config: RuntimeConfig::default(),
        }
    }

    /// Create with custom configuration.
    pub fn with_config(mut self, config: RuntimeConfig) -> Self {
        self.config = config;
        self
    }

    /// Create with memory manager.
    pub fn with_memory(mut self, memory: Arc<dyn MemoryManager>) -> Self {
        self.memory = memory;
        self
    }

    /// Check if execution is cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Get batch size from config.
    pub fn batch_size(&self) -> usize {
        self.config.batch_size
    }
}
```

### 5.2 Memory Manager

```rust
use common_error::GrismResult;

/// Memory manager for tracking and limiting memory usage.
///
/// In Week 7, this provides **accounting only** (no spill-to-disk).
pub trait MemoryManager: Send + Sync {
    /// Reserve memory. Returns error if limit exceeded.
    fn reserve(&self, bytes: usize) -> GrismResult<()>;

    /// Release previously reserved memory.
    fn release(&self, bytes: usize);

    /// Get current memory usage.
    fn used(&self) -> usize;

    /// Get memory limit (0 = unlimited).
    fn limit(&self) -> usize;
}

/// No-op memory manager for unlimited memory.
#[derive(Debug, Default)]
pub struct NoopMemoryManager;

impl MemoryManager for NoopMemoryManager {
    fn reserve(&self, _bytes: usize) -> GrismResult<()> {
        Ok(())
    }

    fn release(&self, _bytes: usize) {}

    fn used(&self) -> usize {
        0
    }

    fn limit(&self) -> usize {
        0
    }
}

/// Tracking memory manager with limit enforcement.
#[derive(Debug)]
pub struct TrackingMemoryManager {
    used: std::sync::atomic::AtomicUsize,
    limit: usize,
}

impl TrackingMemoryManager {
    pub fn new(limit: usize) -> Self {
        Self {
            used: std::sync::atomic::AtomicUsize::new(0),
            limit,
        }
    }
}

impl MemoryManager for TrackingMemoryManager {
    fn reserve(&self, bytes: usize) -> GrismResult<()> {
        use std::sync::atomic::Ordering;
        
        let current = self.used.fetch_add(bytes, Ordering::SeqCst);
        if self.limit > 0 && current + bytes > self.limit {
            self.used.fetch_sub(bytes, Ordering::SeqCst);
            return Err(common_error::GrismError::resource_exhausted(
                format!("Memory limit exceeded: {} + {} > {}", current, bytes, self.limit)
            ));
        }
        Ok(())
    }

    fn release(&self, bytes: usize) {
        use std::sync::atomic::Ordering;
        self.used.fetch_sub(bytes, Ordering::SeqCst);
    }

    fn used(&self) -> usize {
        use std::sync::atomic::Ordering;
        self.used.load(Ordering::SeqCst)
    }

    fn limit(&self) -> usize {
        self.limit
    }
}
```

### 5.3 Metrics Sink

```rust
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// Metrics for a single operator execution.
#[derive(Debug, Clone, Default)]
pub struct OperatorMetrics {
    /// Number of input rows processed.
    pub rows_in: u64,
    /// Number of output rows produced.
    pub rows_out: u64,
    /// Total execution time.
    pub exec_time: Duration,
    /// Peak memory usage in bytes.
    pub memory_bytes: usize,
    /// Number of batches processed.
    pub batches: u64,
}

/// Sink for collecting operator metrics.
#[derive(Debug, Clone, Default)]
pub struct MetricsSink {
    metrics: Arc<RwLock<HashMap<String, OperatorMetrics>>>,
}

impl MetricsSink {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record metrics for an operator.
    pub fn record(&self, operator_id: &str, metrics: OperatorMetrics) {
        self.metrics
            .write()
            .unwrap()
            .insert(operator_id.to_string(), metrics);
    }

    /// Get metrics for an operator.
    pub fn get(&self, operator_id: &str) -> Option<OperatorMetrics> {
        self.metrics.read().unwrap().get(operator_id).cloned()
    }

    /// Get all metrics.
    pub fn all(&self) -> HashMap<String, OperatorMetrics> {
        self.metrics.read().unwrap().clone()
    }

    /// Format metrics for EXPLAIN ANALYZE.
    pub fn format_analyze(&self) -> String {
        let metrics = self.metrics.read().unwrap();
        let mut output = String::new();
        for (op, m) in metrics.iter() {
            output.push_str(&format!(
                "{}: rows_in={}, rows_out={}, time={:?}, memory={}B\n",
                op, m.rows_in, m.rows_out, m.exec_time, m.memory_bytes
            ));
        }
        output
    }
}
```

---

## 6. Physical Operators

### 6.1 Complete Operator Set

Per RFC-namings Section 9.2, all physical operators use the `*Exec` suffix.

| Logical Operator | Physical Operator | Module |
|-----------------|-------------------|--------|
| `Scan(Node)` | `NodeScanExec` | `operators/scan.rs` |
| `Scan(Hyperedge)` | `HyperedgeScanExec` | `operators/scan.rs` |
| `Filter` | `FilterExec` | `operators/filter.rs` |
| `Project` | `ProjectExec` | `operators/project.rs` |
| `Expand(Binary)` | `AdjacencyExpandExec` | `operators/expand.rs` |
| `Expand(Role)` | `RoleExpandExec` | `operators/expand.rs` |
| `Aggregate` | `HashAggregateExec` | `operators/aggregate.rs` |
| `Limit` | `LimitExec` | `operators/limit.rs` |
| `Sort` | `SortExec` | `operators/sort.rs` |
| `Union` | `UnionExec` | `operators/union.rs` |
| `Rename` | `RenameExec` | `operators/rename.rs` |
| (terminal) | `CollectExec` | `operators/collect.rs` |

### 6.2 Scan Operators

```rust
use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;

/// Node scan execution operator.
///
/// Reads nodes from storage and produces Arrow RecordBatch.
#[derive(Debug)]
pub struct NodeScanExec {
    /// Label filter (None = all nodes).
    label: Option<String>,
    /// Output schema.
    schema: PhysicalSchema,
    /// Columns to project (None = all).
    projection: Option<Vec<String>>,
    /// Execution state.
    state: ScanState,
}

#[derive(Debug, Default)]
enum ScanState {
    #[default]
    Uninitialized,
    Open {
        /// Buffered nodes to return.
        buffer: Vec<grism_core::hypergraph::Node>,
        /// Current position in buffer.
        position: usize,
    },
    Exhausted,
}

impl NodeScanExec {
    /// Create a node scan operator.
    pub fn new(label: Option<String>, schema: PhysicalSchema) -> Self {
        Self {
            label,
            schema,
            projection: None,
            state: ScanState::Uninitialized,
        }
    }

    /// Create with projection.
    pub fn with_projection(mut self, columns: Vec<String>) -> Self {
        self.projection = Some(columns);
        self
    }

    /// Convert nodes to RecordBatch.
    fn nodes_to_batch(
        &self,
        nodes: &[grism_core::hypergraph::Node],
        batch_size: usize,
    ) -> GrismResult<RecordBatch> {
        // Build Arrow arrays from node data
        let mut id_builder = Int64Array::builder(batch_size);
        let mut label_builder = StringBuilder::new();
        
        for node in nodes.iter().take(batch_size) {
            id_builder.append_value(node.id.0 as i64);
            let label_str = node.labels.first()
                .map(|l| l.as_str())
                .unwrap_or("");
            label_builder.append_value(label_str);
        }
        
        let schema = self.schema.arrow_schema().clone();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_builder.finish()) as ArrayRef,
                Arc::new(label_builder.finish()) as ArrayRef,
            ],
        ).map_err(|e| GrismError::execution(e.to_string()))
    }
}

#[async_trait]
impl PhysicalOperator for NodeScanExec {
    fn name(&self) -> &'static str {
        "NodeScanExec"
    }

    fn schema(&self) -> &PhysicalSchema {
        &self.schema
    }

    fn capabilities(&self) -> OperatorCaps {
        OperatorCaps::source()
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalOperator>> {
        vec![] // Source operator has no children
    }

    async fn open(&mut self, ctx: &ExecutionContext) -> GrismResult<()> {
        // Load nodes from storage
        let nodes = match &self.label {
            Some(label) => ctx.storage.get_nodes_by_label(label).await?,
            None => {
                // TODO: Implement get_all_nodes in storage
                vec![]
            }
        };

        self.state = ScanState::Open {
            buffer: nodes,
            position: 0,
        };

        Ok(())
    }

    async fn next(&mut self) -> GrismResult<Option<RecordBatch>> {
        // Check cancellation
        if ctx.is_cancelled() {
            return Err(GrismError::cancelled("Query cancelled"));
        }

        match &mut self.state {
            ScanState::Uninitialized => {
                Err(GrismError::execution("Operator not opened"))
            }
            ScanState::Open { buffer, position } => {
                if *position >= buffer.len() {
                    self.state = ScanState::Exhausted;
                    return Ok(None);
                }

                let end = (*position + ctx.batch_size()).min(buffer.len());
                let batch_nodes = &buffer[*position..end];
                *position = end;

                let batch = self.nodes_to_batch(batch_nodes, ctx.batch_size())?;
                Ok(Some(batch))
            }
            ScanState::Exhausted => Ok(None),
        }
    }

    async fn close(&mut self) -> GrismResult<()> {
        self.state = ScanState::Exhausted;
        Ok(())
    }

    fn display(&self) -> String {
        match &self.label {
            Some(label) => format!("NodeScanExec(label={})", label),
            None => "NodeScanExec(all)".to_string(),
        }
    }
}

/// Hyperedge scan execution operator.
#[derive(Debug)]
pub struct HyperedgeScanExec {
    label: Option<String>,
    schema: PhysicalSchema,
    state: ScanState,
}

// Implementation similar to NodeScanExec...
```

### 6.3 Filter Operator

```rust
use arrow::compute::filter_record_batch;
use grism_logical::LogicalExpr;

/// Filter execution operator.
///
/// Applies a predicate expression to each batch.
#[derive(Debug)]
pub struct FilterExec {
    /// Input operator.
    input: Arc<dyn PhysicalOperator>,
    /// Filter predicate.
    predicate: LogicalExpr,
    /// Output schema (same as input).
    schema: PhysicalSchema,
    /// Metrics.
    metrics: OperatorMetrics,
}

impl FilterExec {
    pub fn new(input: Arc<dyn PhysicalOperator>, predicate: LogicalExpr) -> Self {
        let schema = input.schema().clone();
        Self {
            input,
            predicate,
            schema,
            metrics: OperatorMetrics::default(),
        }
    }

    /// Evaluate predicate and filter batch.
    fn filter_batch(&self, batch: &RecordBatch) -> GrismResult<RecordBatch> {
        // Evaluate predicate to boolean array
        let predicate_array = evaluate_expr(&self.predicate, batch)?;
        
        // Apply filter
        filter_record_batch(batch, &predicate_array)
            .map_err(|e| GrismError::execution(e.to_string()))
    }
}

#[async_trait]
impl PhysicalOperator for FilterExec {
    fn name(&self) -> &'static str {
        "FilterExec"
    }

    fn schema(&self) -> &PhysicalSchema {
        &self.schema
    }

    fn capabilities(&self) -> OperatorCaps {
        OperatorCaps::streaming()
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalOperator>> {
        vec![&self.input]
    }

    async fn open(&mut self, ctx: &ExecutionContext) -> GrismResult<()> {
        Arc::get_mut(&mut self.input)
            .ok_or_else(|| GrismError::execution("Cannot get mutable input"))?
            .open(ctx)
            .await
    }

    async fn next(&mut self) -> GrismResult<Option<RecordBatch>> {
        loop {
            match Arc::get_mut(&mut self.input)
                .ok_or_else(|| GrismError::execution("Cannot get mutable input"))?
                .next()
                .await?
            {
                Some(batch) => {
                    self.metrics.rows_in += batch.num_rows() as u64;
                    let filtered = self.filter_batch(&batch)?;
                    
                    if filtered.num_rows() > 0 {
                        self.metrics.rows_out += filtered.num_rows() as u64;
                        return Ok(Some(filtered));
                    }
                    // Continue to next batch if all rows filtered
                }
                None => return Ok(None),
            }
        }
    }

    async fn close(&mut self) -> GrismResult<()> {
        if let Some(input) = Arc::get_mut(&mut self.input) {
            input.close().await?;
        }
        Ok(())
    }

    fn display(&self) -> String {
        format!("FilterExec({})", self.predicate)
    }
}
```

### 6.4 Project Operator

```rust
/// Project execution operator.
///
/// Evaluates projection expressions and produces output batch.
#[derive(Debug)]
pub struct ProjectExec {
    input: Arc<dyn PhysicalOperator>,
    /// Projection expressions with output names.
    projections: Vec<(LogicalExpr, String)>,
    schema: PhysicalSchema,
}

impl ProjectExec {
    pub fn new(
        input: Arc<dyn PhysicalOperator>,
        projections: Vec<(LogicalExpr, String)>,
        schema: PhysicalSchema,
    ) -> Self {
        Self {
            input,
            projections,
            schema,
        }
    }

    fn project_batch(&self, batch: &RecordBatch) -> GrismResult<RecordBatch> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.projections.len());
        
        for (expr, _name) in &self.projections {
            let array = evaluate_expr(expr, batch)?;
            columns.push(array);
        }
        
        RecordBatch::try_new(self.schema.arrow_schema().clone(), columns)
            .map_err(|e| GrismError::execution(e.to_string()))
    }
}

#[async_trait]
impl PhysicalOperator for ProjectExec {
    fn name(&self) -> &'static str {
        "ProjectExec"
    }

    fn schema(&self) -> &PhysicalSchema {
        &self.schema
    }

    fn capabilities(&self) -> OperatorCaps {
        OperatorCaps::streaming()
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalOperator>> {
        vec![&self.input]
    }

    async fn open(&mut self, ctx: &ExecutionContext) -> GrismResult<()> {
        Arc::get_mut(&mut self.input)
            .ok_or_else(|| GrismError::execution("Cannot get mutable input"))?
            .open(ctx)
            .await
    }

    async fn next(&mut self) -> GrismResult<Option<RecordBatch>> {
        match Arc::get_mut(&mut self.input)
            .ok_or_else(|| GrismError::execution("Cannot get mutable input"))?
            .next()
            .await?
        {
            Some(batch) => {
                let projected = self.project_batch(&batch)?;
                Ok(Some(projected))
            }
            None => Ok(None),
        }
    }

    async fn close(&mut self) -> GrismResult<()> {
        if let Some(input) = Arc::get_mut(&mut self.input) {
            input.close().await?;
        }
        Ok(())
    }

    fn display(&self) -> String {
        let cols: Vec<_> = self.projections.iter()
            .map(|(_, name)| name.as_str())
            .collect();
        format!("ProjectExec({})", cols.join(", "))
    }
}
```

### 6.5 Limit Operator

```rust
/// Limit execution operator.
///
/// Limits output to a maximum number of rows.
#[derive(Debug)]
pub struct LimitExec {
    input: Arc<dyn PhysicalOperator>,
    limit: usize,
    offset: usize,
    /// Rows returned so far.
    rows_returned: usize,
    /// Rows skipped so far.
    rows_skipped: usize,
    schema: PhysicalSchema,
}

impl LimitExec {
    pub fn new(input: Arc<dyn PhysicalOperator>, limit: usize) -> Self {
        let schema = input.schema().clone();
        Self {
            input,
            limit,
            offset: 0,
            rows_returned: 0,
            rows_skipped: 0,
            schema,
        }
    }

    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }
}

#[async_trait]
impl PhysicalOperator for LimitExec {
    fn name(&self) -> &'static str {
        "LimitExec"
    }

    fn schema(&self) -> &PhysicalSchema {
        &self.schema
    }

    fn capabilities(&self) -> OperatorCaps {
        OperatorCaps::streaming()
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalOperator>> {
        vec![&self.input]
    }

    async fn open(&mut self, ctx: &ExecutionContext) -> GrismResult<()> {
        self.rows_returned = 0;
        self.rows_skipped = 0;
        Arc::get_mut(&mut self.input)
            .ok_or_else(|| GrismError::execution("Cannot get mutable input"))?
            .open(ctx)
            .await
    }

    async fn next(&mut self) -> GrismResult<Option<RecordBatch>> {
        if self.rows_returned >= self.limit {
            return Ok(None);
        }

        while let Some(batch) = Arc::get_mut(&mut self.input)
            .ok_or_else(|| GrismError::execution("Cannot get mutable input"))?
            .next()
            .await?
        {
            let batch_rows = batch.num_rows();
            
            // Handle offset
            if self.rows_skipped < self.offset {
                let to_skip = (self.offset - self.rows_skipped).min(batch_rows);
                self.rows_skipped += to_skip;
                
                if to_skip == batch_rows {
                    continue; // Skip entire batch
                }
                
                // Slice batch to skip offset rows
                let batch = batch.slice(to_skip, batch_rows - to_skip);
            }
            
            // Handle limit
            let remaining = self.limit - self.rows_returned;
            let to_return = batch.num_rows().min(remaining);
            
            self.rows_returned += to_return;
            
            if to_return < batch.num_rows() {
                return Ok(Some(batch.slice(0, to_return)));
            }
            
            return Ok(Some(batch));
        }

        Ok(None)
    }

    async fn close(&mut self) -> GrismResult<()> {
        if let Some(input) = Arc::get_mut(&mut self.input) {
            input.close().await?;
        }
        Ok(())
    }

    fn display(&self) -> String {
        if self.offset > 0 {
            format!("LimitExec(limit={}, offset={})", self.limit, self.offset)
        } else {
            format!("LimitExec({})", self.limit)
        }
    }
}
```

### 6.6 Aggregate Operator

```rust
use std::collections::HashMap;

/// Hash aggregate execution operator.
///
/// This is a **blocking** operator that must consume all input
/// before producing any output.
#[derive(Debug)]
pub struct HashAggregateExec {
    input: Arc<dyn PhysicalOperator>,
    /// Group-by expressions.
    group_by: Vec<LogicalExpr>,
    /// Aggregate expressions.
    aggregates: Vec<AggExpr>,
    schema: PhysicalSchema,
    /// Aggregation state.
    state: AggregateState,
}

#[derive(Debug, Default)]
enum AggregateState {
    #[default]
    Accumulating,
    /// All input consumed, ready to emit.
    Ready(Vec<RecordBatch>),
    Exhausted,
}

impl HashAggregateExec {
    pub fn new(
        input: Arc<dyn PhysicalOperator>,
        group_by: Vec<LogicalExpr>,
        aggregates: Vec<AggExpr>,
        schema: PhysicalSchema,
    ) -> Self {
        Self {
            input,
            group_by,
            aggregates,
            schema,
            state: AggregateState::Accumulating,
        }
    }
}

#[async_trait]
impl PhysicalOperator for HashAggregateExec {
    fn name(&self) -> &'static str {
        "HashAggregateExec"
    }

    fn schema(&self) -> &PhysicalSchema {
        &self.schema
    }

    fn capabilities(&self) -> OperatorCaps {
        OperatorCaps::blocking()
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalOperator>> {
        vec![&self.input]
    }

    async fn open(&mut self, ctx: &ExecutionContext) -> GrismResult<()> {
        self.state = AggregateState::Accumulating;
        Arc::get_mut(&mut self.input)
            .ok_or_else(|| GrismError::execution("Cannot get mutable input"))?
            .open(ctx)
            .await
    }

    async fn next(&mut self) -> GrismResult<Option<RecordBatch>> {
        match &mut self.state {
            AggregateState::Accumulating => {
                // Consume all input
                let mut accumulators: HashMap<Vec<u8>, Vec<Box<dyn Accumulator>>> = HashMap::new();
                
                while let Some(batch) = Arc::get_mut(&mut self.input)
                    .ok_or_else(|| GrismError::execution("Cannot get mutable input"))?
                    .next()
                    .await?
                {
                    // Update accumulators for each row
                    self.accumulate_batch(&batch, &mut accumulators)?;
                }
                
                // Finalize and produce output
                let output = self.finalize_aggregates(accumulators)?;
                self.state = AggregateState::Ready(vec![output]);
                
                // Recurse to emit
                self.next().await
            }
            AggregateState::Ready(batches) => {
                if batches.is_empty() {
                    self.state = AggregateState::Exhausted;
                    Ok(None)
                } else {
                    Ok(Some(batches.remove(0)))
                }
            }
            AggregateState::Exhausted => Ok(None),
        }
    }

    async fn close(&mut self) -> GrismResult<()> {
        self.state = AggregateState::Exhausted;
        if let Some(input) = Arc::get_mut(&mut self.input) {
            input.close().await?;
        }
        Ok(())
    }

    fn display(&self) -> String {
        let groups: Vec<_> = self.group_by.iter()
            .map(|e| format!("{}", e))
            .collect();
        let aggs: Vec<_> = self.aggregates.iter()
            .map(|a| format!("{}", a))
            .collect();
        format!("HashAggregateExec(group_by=[{}], agg=[{}])", 
            groups.join(", "), aggs.join(", "))
    }
}
```

---

## 7. Expand Operators (Critical Section)

Expand is the **semantic core** of Grism graph execution. These operators traverse hyperedges and must correctly preserve hypergraph semantics.

### 7.1 AdjacencyExpandExec (Binary Hyperedges)

```rust
use grism_logical::{Direction, ExpandMode};

/// Adjacency expand for binary hyperedges (arity = 2).
///
/// Uses adjacency index for efficient traversal when:
/// - Arity = 2
/// - Roles = {source, target}
///
/// # Execution Strategy
///
/// 1. Input batch provides node IDs
/// 2. For each node, lookup adjacent nodes via storage
/// 3. Emit joined batches with original + target columns
///
/// # Properties
///
/// - **Streaming**: Does not buffer entire input
/// - **Fan-out**: May produce more rows than input
/// - **Non-blocking**: Produces output incrementally
#[derive(Debug)]
pub struct AdjacencyExpandExec {
    input: Arc<dyn PhysicalOperator>,
    /// Direction of traversal.
    direction: Direction,
    /// Edge label filter.
    edge_label: Option<String>,
    /// Target node label filter.
    to_label: Option<String>,
    /// Alias for target entity.
    target_alias: Option<String>,
    /// Output schema.
    schema: PhysicalSchema,
    /// Current input batch being expanded.
    current_batch: Option<RecordBatch>,
    /// Position in current batch.
    batch_position: usize,
    /// Pending output rows.
    pending_output: Vec<RecordBatch>,
}

impl AdjacencyExpandExec {
    pub fn new(
        input: Arc<dyn PhysicalOperator>,
        direction: Direction,
        schema: PhysicalSchema,
    ) -> Self {
        Self {
            input,
            direction,
            edge_label: None,
            to_label: None,
            target_alias: None,
            schema,
            current_batch: None,
            batch_position: 0,
            pending_output: Vec::new(),
        }
    }

    pub fn with_edge_label(mut self, label: String) -> Self {
        self.edge_label = Some(label);
        self
    }

    pub fn with_to_label(mut self, label: String) -> Self {
        self.to_label = Some(label);
        self
    }

    pub fn with_target_alias(mut self, alias: String) -> Self {
        self.target_alias = Some(alias);
        self
    }

    /// Expand a single input row to produce output rows.
    async fn expand_row(
        &self,
        ctx: &ExecutionContext,
        input_row: &RecordBatch,
        row_idx: usize,
    ) -> GrismResult<Vec<RecordBatch>> {
        // Extract node ID from input
        let id_col = input_row.column(0); // Assume first column is node ID
        let node_id = get_node_id(id_col, row_idx)?;
        
        // Get adjacent edges from storage
        let edges = ctx.storage.get_edges_for_node(node_id).await?;
        
        // Filter by label and direction
        let filtered_edges: Vec<_> = edges.into_iter()
            .filter(|e| {
                // Filter by edge label
                if let Some(ref label) = self.edge_label {
                    if !e.has_label(label) {
                        return false;
                    }
                }
                
                // Filter by direction
                match self.direction {
                    Direction::Outgoing => e.source == node_id,
                    Direction::Incoming => e.target == node_id,
                    Direction::Both => true,
                }
            })
            .collect();
        
        // Build output batches
        let mut output = Vec::new();
        for edge in filtered_edges {
            let target_id = match self.direction {
                Direction::Outgoing => edge.target,
                Direction::Incoming => edge.source,
                Direction::Both => {
                    if edge.source == node_id { edge.target } else { edge.source }
                }
            };
            
            // Filter by target label if specified
            if let Some(ref to_label) = self.to_label {
                let target_node = ctx.storage.get_node(target_id).await?;
                if let Some(node) = target_node {
                    if !node.has_label(to_label) {
                        continue;
                    }
                }
            }
            
            // Create output row combining input + target
            let output_batch = self.create_output_row(input_row, row_idx, &edge, target_id)?;
            output.push(output_batch);
        }
        
        Ok(output)
    }

    fn create_output_row(
        &self,
        input: &RecordBatch,
        row_idx: usize,
        edge: &grism_core::hypergraph::Edge,
        target_id: grism_core::hypergraph::NodeId,
    ) -> GrismResult<RecordBatch> {
        // Build output with input columns + target columns
        // Implementation depends on output schema structure
        todo!("Implement create_output_row")
    }
}

#[async_trait]
impl PhysicalOperator for AdjacencyExpandExec {
    fn name(&self) -> &'static str {
        "AdjacencyExpandExec"
    }

    fn schema(&self) -> &PhysicalSchema {
        &self.schema
    }

    fn capabilities(&self) -> OperatorCaps {
        OperatorCaps {
            blocking: false,
            requires_global_view: false,
            stateless: false, // Has internal state (current_batch)
            ..Default::default()
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalOperator>> {
        vec![&self.input]
    }

    async fn open(&mut self, ctx: &ExecutionContext) -> GrismResult<()> {
        self.current_batch = None;
        self.batch_position = 0;
        self.pending_output.clear();
        
        Arc::get_mut(&mut self.input)
            .ok_or_else(|| GrismError::execution("Cannot get mutable input"))?
            .open(ctx)
            .await
    }

    async fn next(&mut self) -> GrismResult<Option<RecordBatch>> {
        // Return pending output first
        if !self.pending_output.is_empty() {
            return Ok(Some(self.pending_output.remove(0)));
        }
        
        // Process current batch or fetch new one
        loop {
            // Need new batch?
            if self.current_batch.is_none() {
                match Arc::get_mut(&mut self.input)
                    .ok_or_else(|| GrismError::execution("Cannot get mutable input"))?
                    .next()
                    .await?
                {
                    Some(batch) => {
                        self.current_batch = Some(batch);
                        self.batch_position = 0;
                    }
                    None => return Ok(None), // Input exhausted
                }
            }
            
            let batch = self.current_batch.as_ref().unwrap();
            
            // Process next row in current batch
            if self.batch_position < batch.num_rows() {
                let ctx = todo!("Get context"); // Need to store in state
                let expanded = self.expand_row(&ctx, batch, self.batch_position).await?;
                self.batch_position += 1;
                
                if !expanded.is_empty() {
                    self.pending_output.extend(expanded.into_iter().skip(1));
                    return Ok(expanded.into_iter().next());
                }
                // No output for this row, continue
            } else {
                // Batch exhausted
                self.current_batch = None;
            }
        }
    }

    async fn close(&mut self) -> GrismResult<()> {
        self.current_batch = None;
        self.pending_output.clear();
        
        if let Some(input) = Arc::get_mut(&mut self.input) {
            input.close().await?;
        }
        Ok(())
    }

    fn display(&self) -> String {
        let mut parts = vec![format!("dir={}", self.direction)];
        if let Some(ref label) = self.edge_label {
            parts.push(format!("edge={}", label));
        }
        if let Some(ref label) = self.to_label {
            parts.push(format!("to={}", label));
        }
        format!("AdjacencyExpandExec({})", parts.join(", "))
    }
}
```

### 7.2 RoleExpandExec (N-ary Hyperedges)

```rust
/// Role-based expand for n-ary hyperedges.
///
/// Used when:
/// - Any n-ary hyperedge traversal
/// - Role-qualified traversal
/// - Hyperedge materialization required
///
/// # Execution Strategy
///
/// - Join-like expansion over role bindings
/// - Often batch-amplifying (many output rows per input)
/// - Still non-blocking (streaming)
///
/// # Distribution Sensitivity (RFC-0010)
///
/// This operator is **distribution-sensitive** - hyperedges may span
/// partitions, requiring shuffle in distributed execution.
#[derive(Debug)]
pub struct RoleExpandExec {
    input: Arc<dyn PhysicalOperator>,
    /// Source role for traversal.
    from_role: String,
    /// Target role for traversal.
    to_role: String,
    /// Hyperedge label filter.
    edge_label: Option<String>,
    /// Whether to materialize the hyperedge.
    materialize_edge: bool,
    schema: PhysicalSchema,
}

impl RoleExpandExec {
    pub fn new(
        input: Arc<dyn PhysicalOperator>,
        from_role: String,
        to_role: String,
        schema: PhysicalSchema,
    ) -> Self {
        Self {
            input,
            from_role,
            to_role,
            edge_label: None,
            materialize_edge: false,
            schema,
        }
    }

    pub fn with_edge_label(mut self, label: String) -> Self {
        self.edge_label = Some(label);
        self
    }

    pub fn with_materialize(mut self) -> Self {
        self.materialize_edge = true;
        self
    }
}

#[async_trait]
impl PhysicalOperator for RoleExpandExec {
    fn name(&self) -> &'static str {
        "RoleExpandExec"
    }

    fn schema(&self) -> &PhysicalSchema {
        &self.schema
    }

    fn capabilities(&self) -> OperatorCaps {
        OperatorCaps {
            blocking: false,
            requires_global_view: false, // But partition-sensitive in distributed
            stateless: false,
            ..Default::default()
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalOperator>> {
        vec![&self.input]
    }

    async fn open(&mut self, ctx: &ExecutionContext) -> GrismResult<()> {
        Arc::get_mut(&mut self.input)
            .ok_or_else(|| GrismError::execution("Cannot get mutable input"))?
            .open(ctx)
            .await
    }

    async fn next(&mut self) -> GrismResult<Option<RecordBatch>> {
        // Similar structure to AdjacencyExpandExec but using hyperedge role bindings
        todo!("Implement RoleExpandExec::next")
    }

    async fn close(&mut self) -> GrismResult<()> {
        if let Some(input) = Arc::get_mut(&mut self.input) {
            input.close().await?;
        }
        Ok(())
    }

    fn display(&self) -> String {
        let mut s = format!("RoleExpandExec({} -> {})", self.from_role, self.to_role);
        if let Some(ref label) = self.edge_label {
            s.push_str(&format!(", edge={}", label));
        }
        if self.materialize_edge {
            s.push_str(", materialize");
        }
        s
    }
}
```

---

## 8. Physical Planner

### 8.1 PhysicalPlanner Trait

```rust
use grism_logical::LogicalPlan;

/// Trait for physical planners.
///
/// A physical planner converts a logical plan into a physical plan
/// by selecting concrete operator implementations.
pub trait PhysicalPlanner: Send + Sync {
    /// Convert a logical plan to a physical plan.
    fn plan(&self, logical: &LogicalPlan) -> GrismResult<PhysicalPlan>;
}
```

### 8.2 LocalPhysicalPlanner

```rust
use grism_logical::{LogicalOp, ScanOp, ScanKind, ExpandOp, ExpandMode};

/// Physical planner for local (single-node) execution.
///
/// Selects operator implementations optimized for local execution
/// without considering distribution.
#[derive(Debug, Default)]
pub struct LocalPhysicalPlanner {
    /// Configuration options.
    config: PlannerConfig,
}

/// Configuration for the local physical planner.
#[derive(Debug, Clone, Default)]
pub struct PlannerConfig {
    /// Prefer adjacency-based expand when possible.
    pub prefer_adjacency: bool,
    /// Enable projection pushdown into scans.
    pub projection_pushdown: bool,
    /// Enable predicate pushdown into scans.
    pub predicate_pushdown: bool,
}

impl LocalPhysicalPlanner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_config(config: PlannerConfig) -> Self {
        Self { config }
    }

    /// Plan a single logical operator.
    fn plan_operator(&self, op: &LogicalOp) -> GrismResult<Arc<dyn PhysicalOperator>> {
        match op {
            LogicalOp::Scan(scan) => self.plan_scan(scan),
            
            LogicalOp::Filter { input, filter } => {
                let input_physical = self.plan_operator(input)?;
                Ok(Arc::new(FilterExec::new(input_physical, filter.predicate.clone())))
            }
            
            LogicalOp::Project { input, project } => {
                let input_physical = self.plan_operator(input)?;
                let schema = self.infer_project_schema(&input_physical, project)?;
                let projections = self.build_projections(project)?;
                Ok(Arc::new(ProjectExec::new(input_physical, projections, schema)))
            }
            
            LogicalOp::Expand { input, expand } => {
                let input_physical = self.plan_operator(input)?;
                self.plan_expand(input_physical, expand)
            }
            
            LogicalOp::Aggregate { input, aggregate } => {
                let input_physical = self.plan_operator(input)?;
                let schema = self.infer_aggregate_schema(&input_physical, aggregate)?;
                Ok(Arc::new(HashAggregateExec::new(
                    input_physical,
                    aggregate.group_by.clone(),
                    aggregate.aggregates.clone(),
                    schema,
                )))
            }
            
            LogicalOp::Limit { input, limit } => {
                let input_physical = self.plan_operator(input)?;
                let mut exec = LimitExec::new(input_physical, limit.limit);
                if let Some(offset) = limit.offset {
                    exec = exec.with_offset(offset);
                }
                Ok(Arc::new(exec))
            }
            
            LogicalOp::Sort { input, sort } => {
                let input_physical = self.plan_operator(input)?;
                Ok(Arc::new(SortExec::new(input_physical, sort.keys.clone())))
            }
            
            LogicalOp::Union { left, right, union } => {
                let left_physical = self.plan_operator(left)?;
                let right_physical = self.plan_operator(right)?;
                Ok(Arc::new(UnionExec::new(left_physical, right_physical, union.all)))
            }
            
            LogicalOp::Rename { input, rename } => {
                let input_physical = self.plan_operator(input)?;
                Ok(Arc::new(RenameExec::new(input_physical, rename.mappings.clone())))
            }
            
            LogicalOp::Infer { .. } => {
                Err(GrismError::not_implemented("Infer execution"))
            }
            
            LogicalOp::Empty => {
                Ok(Arc::new(EmptyExec::new()))
            }
        }
    }

    fn plan_scan(&self, scan: &ScanOp) -> GrismResult<Arc<dyn PhysicalOperator>> {
        let schema = self.infer_scan_schema(scan)?;
        
        match scan.kind {
            ScanKind::Node => {
                let mut exec = NodeScanExec::new(scan.label.clone(), schema);
                if let Some(ref cols) = scan.projection {
                    exec = exec.with_projection(cols.clone());
                }
                Ok(Arc::new(exec))
            }
            ScanKind::Hyperedge => {
                Ok(Arc::new(HyperedgeScanExec::new(scan.label.clone(), schema)))
            }
        }
    }

    fn plan_expand(
        &self,
        input: Arc<dyn PhysicalOperator>,
        expand: &ExpandOp,
    ) -> GrismResult<Arc<dyn PhysicalOperator>> {
        let schema = self.infer_expand_schema(&input, expand)?;
        
        match expand.mode {
            ExpandMode::Binary => {
                let mut exec = AdjacencyExpandExec::new(input, expand.direction, schema);
                
                if let Some(ref label) = expand.edge_label {
                    exec = exec.with_edge_label(label.clone());
                }
                if let Some(ref label) = expand.to_label {
                    exec = exec.with_to_label(label.clone());
                }
                if let Some(ref alias) = expand.target_alias {
                    exec = exec.with_target_alias(alias.clone());
                }
                
                Ok(Arc::new(exec))
            }
            ExpandMode::Role => {
                let from_role = expand.from_role.clone()
                    .ok_or_else(|| GrismError::planning("Role expand requires from_role"))?;
                let to_role = expand.to_role.clone()
                    .ok_or_else(|| GrismError::planning("Role expand requires to_role"))?;
                
                let mut exec = RoleExpandExec::new(input, from_role, to_role, schema);
                
                if let Some(ref label) = expand.edge_label {
                    exec = exec.with_edge_label(label.clone());
                }
                
                Ok(Arc::new(exec))
            }
            ExpandMode::MaterializeHyperedge => {
                let from_role = expand.from_role.clone().unwrap_or_default();
                let to_role = expand.to_role.clone().unwrap_or_default();
                
                let exec = RoleExpandExec::new(input, from_role, to_role, schema)
                    .with_materialize();
                
                Ok(Arc::new(exec))
            }
        }
    }

    // Schema inference helpers
    fn infer_scan_schema(&self, scan: &ScanOp) -> GrismResult<PhysicalSchema> {
        // Build Arrow schema based on scan type
        let fields = vec![
            Field::new("_id", DataType::Int64, false),
            Field::new("_label", DataType::Utf8, true),
            // Additional property columns would be added based on catalog
        ];
        
        let arrow_schema = Arc::new(ArrowSchema::new(fields));
        Ok(PhysicalSchema::new(arrow_schema))
    }

    fn infer_expand_schema(
        &self,
        input: &Arc<dyn PhysicalOperator>,
        expand: &ExpandOp,
    ) -> GrismResult<PhysicalSchema> {
        // Combine input schema with target entity columns
        let input_schema = input.schema();
        
        // Add target entity columns
        let mut fields: Vec<Field> = input_schema.arrow_schema()
            .fields()
            .iter()
            .cloned()
            .collect();
        
        // Add target node columns
        let target_prefix = expand.target_alias.as_deref().unwrap_or("_target");
        fields.push(Field::new(
            format!("{}._id", target_prefix),
            DataType::Int64,
            false,
        ));
        fields.push(Field::new(
            format!("{}._label", target_prefix),
            DataType::Utf8,
            true,
        ));
        
        // Add edge columns if materializing
        if expand.mode == ExpandMode::MaterializeHyperedge {
            let edge_prefix = expand.edge_alias.as_deref().unwrap_or("_edge");
            fields.push(Field::new(
                format!("{}._id", edge_prefix),
                DataType::Int64,
                false,
            ));
        }
        
        let arrow_schema = Arc::new(ArrowSchema::new(fields));
        Ok(PhysicalSchema::new(arrow_schema))
    }

    fn infer_project_schema(
        &self,
        input: &Arc<dyn PhysicalOperator>,
        project: &ProjectOp,
    ) -> GrismResult<PhysicalSchema> {
        // Build schema from projection expressions
        todo!("Implement infer_project_schema")
    }

    fn infer_aggregate_schema(
        &self,
        input: &Arc<dyn PhysicalOperator>,
        aggregate: &AggregateOp,
    ) -> GrismResult<PhysicalSchema> {
        // Build schema from group by + aggregate expressions
        todo!("Implement infer_aggregate_schema")
    }

    fn build_projections(
        &self,
        project: &ProjectOp,
    ) -> GrismResult<Vec<(LogicalExpr, String)>> {
        // Convert project op to expression list
        todo!("Implement build_projections")
    }
}

impl PhysicalPlanner for LocalPhysicalPlanner {
    fn plan(&self, logical: &LogicalPlan) -> GrismResult<PhysicalPlan> {
        let root = self.plan_operator(logical.root())?;
        Ok(PhysicalPlan::new(root))
    }
}
```

---

## 9. Local Executor

### 9.1 LocalExecutor Implementation

```rust
/// Local single-node executor.
///
/// Executes physical plans using a pull-based pipeline model.
/// This is the **reference execution backend** for Grism.
pub struct LocalExecutor {
    /// Execution configuration.
    config: RuntimeConfig,
}

impl LocalExecutor {
    /// Create a new local executor with default configuration.
    pub fn new() -> Self {
        Self {
            config: RuntimeConfig::default(),
        }
    }

    /// Create with custom configuration.
    pub fn with_config(config: RuntimeConfig) -> Self {
        Self { config }
    }

    /// Execute a physical plan.
    pub async fn execute(
        &self,
        plan: PhysicalPlan,
        storage: Arc<dyn Storage>,
        snapshot: SnapshotId,
    ) -> GrismResult<ExecutionResult> {
        // Create execution context
        let ctx = ExecutionContext::new(storage, snapshot)
            .with_config(self.config.clone());
        
        // Get mutable root
        let mut root = plan.root;
        
        // Open the pipeline
        Arc::get_mut(&mut root)
            .ok_or_else(|| GrismError::execution("Cannot get mutable root"))?
            .open(&ctx)
            .await?;
        
        // Collect results
        let mut batches = Vec::new();
        let start = std::time::Instant::now();
        
        loop {
            // Check cancellation
            if ctx.is_cancelled() {
                break;
            }
            
            match Arc::get_mut(&mut root)
                .ok_or_else(|| GrismError::execution("Cannot get mutable root"))?
                .next()
                .await?
            {
                Some(batch) => batches.push(batch),
                None => break,
            }
        }
        
        // Close the pipeline
        Arc::get_mut(&mut root)
            .ok_or_else(|| GrismError::execution("Cannot get mutable root"))?
            .close()
            .await?;
        
        let elapsed = start.elapsed();
        
        Ok(ExecutionResult {
            batches,
            metrics: ctx.metrics.clone(),
            elapsed,
        })
    }

    /// Execute synchronously (blocking).
    pub fn execute_sync(
        &self,
        plan: PhysicalPlan,
        storage: Arc<dyn Storage>,
        snapshot: SnapshotId,
    ) -> GrismResult<ExecutionResult> {
        common_runtime::block_on(self.execute(plan, storage, snapshot))?
    }

    /// Execute a logical plan (convenience method).
    pub async fn execute_logical(
        &self,
        logical: LogicalPlan,
        storage: Arc<dyn Storage>,
        snapshot: SnapshotId,
    ) -> GrismResult<ExecutionResult> {
        // Optimize
        let optimized = grism_optimizer::optimize(logical)?;
        
        // Physical plan
        let planner = LocalPhysicalPlanner::new();
        let physical = planner.plan(&optimized.plan)?;
        
        // Execute
        self.execute(physical, storage, snapshot).await
    }
}

impl Default for LocalExecutor {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of query execution.
#[derive(Debug)]
pub struct ExecutionResult {
    /// Output batches.
    pub batches: Vec<RecordBatch>,
    /// Execution metrics.
    pub metrics: MetricsSink,
    /// Total execution time.
    pub elapsed: std::time::Duration,
}

impl ExecutionResult {
    /// Get total row count.
    pub fn total_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Combine all batches into one.
    pub fn concat(&self) -> GrismResult<RecordBatch> {
        if self.batches.is_empty() {
            return Err(GrismError::execution("No batches to concat"));
        }
        
        arrow::compute::concat_batches(
            &self.batches[0].schema(),
            &self.batches,
        ).map_err(|e| GrismError::execution(e.to_string()))
    }

    /// Format as EXPLAIN ANALYZE output.
    pub fn explain_analyze(&self) -> String {
        let mut output = String::new();
        output.push_str(&format!("Execution Time: {:?}\n", self.elapsed));
        output.push_str(&format!("Total Rows: {}\n", self.total_rows()));
        output.push_str(&format!("Batches: {}\n", self.batches.len()));
        output.push_str("\nOperator Metrics:\n");
        output.push_str(&self.metrics.format_analyze());
        output
    }
}
```

---

## 10. Module Layout

### 10.1 Directory Structure

```
grism-engine/
├── Cargo.toml
└── src/
    ├── lib.rs                    # Public API exports
    │
    ├── physical/
    │   ├── mod.rs                # Physical plan module
    │   ├── plan.rs               # PhysicalPlan, PhysicalNodeId
    │   ├── planner.rs            # LocalPhysicalPlanner, PhysicalPlanner trait
    │   ├── properties.rs         # PlanProperties, PartitioningSpec
    │   └── schema.rs             # PhysicalSchema
    │
    ├── operators/
    │   ├── mod.rs                # Operator module, PhysicalOperator trait
    │   ├── traits.rs             # OperatorCaps, common traits
    │   ├── scan.rs               # NodeScanExec, HyperedgeScanExec
    │   ├── filter.rs             # FilterExec
    │   ├── project.rs            # ProjectExec
    │   ├── expand.rs             # AdjacencyExpandExec, RoleExpandExec
    │   ├── aggregate.rs          # HashAggregateExec
    │   ├── limit.rs              # LimitExec
    │   ├── sort.rs               # SortExec
    │   ├── union.rs              # UnionExec
    │   ├── rename.rs             # RenameExec
    │   ├── collect.rs            # CollectExec
    │   └── empty.rs              # EmptyExec
    │
    ├── executor/
    │   ├── mod.rs                # Executor module
    │   ├── local.rs              # LocalExecutor
    │   ├── context.rs            # ExecutionContext, RuntimeConfig
    │   └── result.rs             # ExecutionResult
    │
    ├── memory/
    │   ├── mod.rs                # Memory management
    │   └── manager.rs            # MemoryManager, TrackingMemoryManager
    │
    ├── metrics/
    │   ├── mod.rs                # Metrics collection
    │   └── sink.rs               # MetricsSink, OperatorMetrics
    │
    ├── expr/
    │   ├── mod.rs                # Expression evaluation
    │   └── evaluator.rs          # evaluate_expr function
    │
    └── python/                   # Python bindings (optional)
        └── mod.rs
```

### 10.2 Cargo.toml Dependencies

```toml
[package]
name = "grism-engine"
version = "0.1.0"
edition = "2021"

[dependencies]
# Internal crates
grism-core = { path = "../grism-core" }
grism-logical = { path = "../grism-logical" }
grism-optimizer = { path = "../grism-optimizer" }
grism-storage = { path = "../grism-storage" }
common-error = { path = "../common/error" }
common-runtime = { path = "../common/runtime" }

# Arrow ecosystem
arrow = { version = "53", features = ["compute"] }

# Async runtime
async-trait = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "sync"] }
tokio-util = "0.7"

# Utilities
thiserror = "1"
tracing = "0.1"

[dev-dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
proptest = "1"
```

---

## 11. RFC-0010 Extension Points

The following are **intentionally designed but not implemented** in Week 7.

### 11.1 Exchange Operator Placeholder

```rust
/// Exchange operator for distributed execution (RFC-0010).
///
/// NOT IMPLEMENTED in Week 7. This placeholder exists to ensure
/// the physical plan structure accommodates distributed execution.
#[derive(Debug)]
pub struct ExchangeExec {
    input: Arc<dyn PhysicalOperator>,
    partitioning: PartitioningSpec,
    schema: PhysicalSchema,
}

// Implementation deferred to Week 8
```

### 11.2 Fragmentability Invariant

Every `PhysicalOperator` MUST satisfy:

> **Fragmentability**: The operator can be executed independently given a subset of input rows.

This enables:
- Stage splitting for distributed execution
- Ray task parallelism
- Shuffle insertion at stage boundaries

### 11.3 Blocking Operators as Stage Boundaries

Operators declaring `blocking: true` in their capabilities become stage boundaries in distributed execution:

```rust
// In distributed planner (Week 8):
fn insert_stage_boundaries(plan: PhysicalPlan) -> StagedPlan {
    // Find blocking operators
    // Insert Exchange before each blocking operator
    // Create execution stages
}
```

---

## 12. Testing Strategy

### 12.1 Unit Tests

Each operator requires:
- Basic functionality test
- Empty input handling
- Error propagation test
- Metrics collection test

### 12.2 Integration Tests

```rust
#[tokio::test]
async fn test_scan_filter_project_pipeline() {
    let storage = Arc::new(InMemoryStorage::new());
    
    // Insert test data
    storage.insert_node(&Node::new().with_label("Person")).await.unwrap();
    
    // Build logical plan
    let logical = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
        .filter(FilterOp::new(col("age").gt(lit(18i64))))
        .project(ProjectOp::columns(["name"]))
        .build();
    
    // Execute
    let executor = LocalExecutor::new();
    let result = executor.execute_logical(
        logical,
        storage,
        SnapshotId::default(),
    ).await.unwrap();
    
    assert!(result.total_rows() >= 0);
}

#[tokio::test]
async fn test_expand_execution() {
    let storage = Arc::new(InMemoryStorage::new());
    
    // Insert graph data
    let person = Node::new().with_label("Person");
    let company = Node::new().with_label("Company");
    storage.insert_node(&person).await.unwrap();
    storage.insert_node(&company).await.unwrap();
    storage.insert_edge(&Edge::new(person.id, company.id).with_label("WORKS_AT")).await.unwrap();
    
    // Build expand plan
    let logical = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
        .expand(ExpandOp::binary()
            .with_edge_label("WORKS_AT")
            .with_to_label("Company"))
        .build();
    
    let executor = LocalExecutor::new();
    let result = executor.execute_logical(
        logical,
        storage,
        SnapshotId::default(),
    ).await.unwrap();
    
    assert_eq!(result.total_rows(), 1);
}
```

---

## 13. Acceptance Checklist

Per Phase 3, Week 7 deliverables from the development schedule:

- [ ] `PhysicalPlan` DAG structure implemented
- [ ] `PhysicalPlanner` trait and `LocalPhysicalPlanner` implemented
- [ ] All physical operators executable:
  - [ ] `NodeScanExec`, `HyperedgeScanExec`
  - [ ] `FilterExec`, `ProjectExec`, `LimitExec`
  - [ ] `AdjacencyExpandExec`, `RoleExpandExec`
  - [ ] `HashAggregateExec`, `SortExec`
  - [ ] `UnionExec`, `RenameExec`, `CollectExec`
- [ ] `LocalExecutor` streams Arrow `RecordBatch`
- [ ] Expand semantics correct (hyperedge-first)
- [ ] Blocking operators declare `blocking: true`
- [ ] `MemoryManager` accounting present
- [ ] `MetricsSink` collects operator metrics
- [ ] EXPLAIN and EXPLAIN ANALYZE hooks
- [ ] No distributed assumptions leaked
- [ ] RFC-0010 extension points present but unused
- [ ] Unit tests for all operators
- [ ] Integration tests for full pipelines
- [ ] All tests passing

---

## 14. Summary

This implementation spec defines:

1. **Physical plan model** distinct from logical plans, with Arrow-native schemas
2. **Pull-based operator interface** following RFC-0008 lifecycle
3. **Complete operator set** with `*Exec` naming per RFC-namings
4. **LocalExecutor** for single-node execution
5. **Storage integration** for scan operators
6. **Memory and metrics** infrastructure
7. **Extension points** for RFC-0010 distributed execution

The design ensures:
- **Semantic correctness**: Hyperedge-first graph execution
- **Execution efficiency**: Arrow-native, streaming pipeline
- **Future compatibility**: Zero rewrite needed for Week 8 distribution
- **Clean implementation**: Well-defined module boundaries
