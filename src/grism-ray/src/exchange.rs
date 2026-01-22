//! Exchange operator for distributed data movement.
//!
//! The `ExchangeExec` operator is a first-class physical operator that
//! repartitions data across workers. Per RFC-0102, Exchange:
//! - Introduces a synchronization boundary
//! - Separates execution stages
//! - Enables parallel execution across workers

use std::fmt::Debug;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use common_error::{GrismError, GrismResult};
use grism_engine::executor::ExecutionContext;
use grism_engine::operators::PhysicalOperator;
use grism_engine::physical::{OperatorCaps, PhysicalSchema};

use crate::partitioning::PartitioningSpec;

// ============================================================================
// Exchange Mode
// ============================================================================

/// Exchange modes for data repartitioning.
///
/// Per RFC-0102 Section 7.2, these modes determine how data flows between stages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExchangeMode {
    /// Repartition data by hash of key columns.
    /// Used for aggregation and join operations.
    Shuffle,

    /// Replicate data to all workers.
    /// Used for broadcast joins with small tables.
    Broadcast,

    /// Collect all data to a single coordinator.
    /// Used for final result collection.
    Gather,
}

impl Default for ExchangeMode {
    fn default() -> Self {
        Self::Shuffle
    }
}

impl std::fmt::Display for ExchangeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Shuffle => write!(f, "Shuffle"),
            Self::Broadcast => write!(f, "Broadcast"),
            Self::Gather => write!(f, "Gather"),
        }
    }
}

// ============================================================================
// Exchange Operator
// ============================================================================

/// Exchange operator for distributed data movement.
///
/// `ExchangeExec` is a physical operator that repartitions data according to
/// a specified partitioning scheme. In local execution, it acts as a passthrough.
/// In distributed execution, it coordinates data movement between stages.
///
/// # Behavior
///
/// - **Local execution**: Passthrough (no actual repartitioning)
/// - **Distributed execution**: Data is sent to appropriate workers based on
///   the partitioning scheme
///
/// # Example
///
/// ```text
/// Stage 0: NodeScan → Filter → Exchange(Hash by city)
///                                  │
///                                  ▼
/// Stage 1: Aggregate(GROUP BY city) → Collect
/// ```
pub struct ExchangeExec {
    /// Child operator to read from.
    child: Arc<dyn PhysicalOperator>,
    /// Partitioning specification for output.
    partitioning: PartitioningSpec,
    /// Exchange mode (shuffle, broadcast, gather).
    mode: ExchangeMode,
    /// Output schema (same as input).
    schema: PhysicalSchema,
}

impl ExchangeExec {
    /// Create a new exchange operator.
    pub fn new(
        child: Arc<dyn PhysicalOperator>,
        partitioning: PartitioningSpec,
        mode: ExchangeMode,
    ) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            partitioning,
            mode,
            schema,
        }
    }

    /// Create a shuffle exchange.
    pub fn shuffle(child: Arc<dyn PhysicalOperator>, keys: Vec<String>, num_partitions: usize) -> Self {
        Self::new(
            child,
            PartitioningSpec::hash(keys, num_partitions),
            ExchangeMode::Shuffle,
        )
    }

    /// Create a gather exchange (collect to single partition).
    pub fn gather(child: Arc<dyn PhysicalOperator>) -> Self {
        Self::new(child, PartitioningSpec::single(), ExchangeMode::Gather)
    }

    /// Create a broadcast exchange.
    pub fn broadcast(child: Arc<dyn PhysicalOperator>, num_partitions: usize) -> Self {
        Self::new(
            child,
            PartitioningSpec::round_robin(num_partitions),
            ExchangeMode::Broadcast,
        )
    }

    /// Get the partitioning specification.
    pub fn partitioning(&self) -> &PartitioningSpec {
        &self.partitioning
    }

    /// Get the exchange mode.
    pub fn mode(&self) -> ExchangeMode {
        self.mode
    }

    /// Get the child operator.
    pub fn child(&self) -> &Arc<dyn PhysicalOperator> {
        &self.child
    }
}

impl Debug for ExchangeExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExchangeExec")
            .field("mode", &self.mode)
            .field("partitioning", &self.partitioning)
            .field("schema", &self.schema)
            .finish()
    }
}

#[async_trait]
impl PhysicalOperator for ExchangeExec {
    fn name(&self) -> &'static str {
        "ExchangeExec"
    }

    fn schema(&self) -> &PhysicalSchema {
        &self.schema
    }

    fn capabilities(&self) -> OperatorCaps {
        OperatorCaps {
            blocking: true, // Exchange is a blocking barrier
            requires_global_view: false,
            supports_predicate_pushdown: false,
            supports_projection_pushdown: false,
            stateless: false,
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalOperator>> {
        vec![&self.child]
    }

    async fn open(&self, ctx: &ExecutionContext) -> GrismResult<()> {
        // In local execution, just open child
        // In distributed execution, this would set up network connections
        self.child.open(ctx).await
    }

    async fn next(&self) -> GrismResult<Option<RecordBatch>> {
        // In local execution, Exchange is a passthrough
        // The actual repartitioning happens in distributed execution
        // via the StageExecutor
        //
        // TODO: In distributed mode, this should:
        // 1. Read from upstream partition
        // 2. Route rows to downstream partitions
        // 3. Send via network transport
        self.child.next().await
    }

    async fn close(&self) -> GrismResult<()> {
        self.child.close().await
    }

    fn display(&self) -> String {
        format!(
            "ExchangeExec(mode={}, partitioning={})",
            self.mode, self.partitioning
        )
    }
}

// ============================================================================
// Exchange State (for distributed execution)
// ============================================================================

/// State for exchange operation in distributed execution.
///
/// This tracks the progress of data movement between stages.
#[derive(Debug, Clone, Default)]
pub struct ExchangeState {
    /// Rows sent per partition.
    pub rows_sent: Vec<u64>,
    /// Rows received per partition.
    pub rows_received: Vec<u64>,
    /// Bytes sent.
    pub bytes_sent: u64,
    /// Bytes received.
    pub bytes_received: u64,
    /// Whether exchange is complete.
    pub complete: bool,
}

impl ExchangeState {
    /// Create new state for given number of partitions.
    pub fn new(num_partitions: usize) -> Self {
        Self {
            rows_sent: vec![0; num_partitions],
            rows_received: vec![0; num_partitions],
            bytes_sent: 0,
            bytes_received: 0,
            complete: false,
        }
    }

    /// Record rows sent to a partition.
    pub fn record_sent(&mut self, partition: usize, rows: u64, bytes: u64) {
        if partition < self.rows_sent.len() {
            self.rows_sent[partition] += rows;
        }
        self.bytes_sent += bytes;
    }

    /// Record rows received from a partition.
    pub fn record_received(&mut self, partition: usize, rows: u64, bytes: u64) {
        if partition < self.rows_received.len() {
            self.rows_received[partition] += rows;
        }
        self.bytes_received += bytes;
    }

    /// Mark exchange as complete.
    pub fn mark_complete(&mut self) {
        self.complete = true;
    }

    /// Get total rows sent.
    pub fn total_sent(&self) -> u64 {
        self.rows_sent.iter().sum()
    }

    /// Get total rows received.
    pub fn total_received(&self) -> u64 {
        self.rows_received.iter().sum()
    }
}

// ============================================================================
// Exchange Builder
// ============================================================================

/// Builder for constructing Exchange operators.
pub struct ExchangeBuilder {
    child: Option<Arc<dyn PhysicalOperator>>,
    partitioning: PartitioningSpec,
    mode: ExchangeMode,
}

impl ExchangeBuilder {
    /// Create a new exchange builder.
    pub fn new() -> Self {
        Self {
            child: None,
            partitioning: PartitioningSpec::Unknown,
            mode: ExchangeMode::Shuffle,
        }
    }

    /// Set the child operator.
    pub fn child(mut self, child: Arc<dyn PhysicalOperator>) -> Self {
        self.child = Some(child);
        self
    }

    /// Set the partitioning specification.
    pub fn partitioning(mut self, spec: PartitioningSpec) -> Self {
        self.partitioning = spec;
        self
    }

    /// Set the exchange mode.
    pub fn mode(mut self, mode: ExchangeMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set up hash partitioning.
    pub fn hash_by(mut self, keys: Vec<String>, num_partitions: usize) -> Self {
        self.partitioning = PartitioningSpec::hash(keys, num_partitions);
        self.mode = ExchangeMode::Shuffle;
        self
    }

    /// Set up gather (collect to single partition).
    pub fn gather(mut self) -> Self {
        self.partitioning = PartitioningSpec::single();
        self.mode = ExchangeMode::Gather;
        self
    }

    /// Set up broadcast to all partitions.
    pub fn broadcast(mut self, num_partitions: usize) -> Self {
        self.partitioning = PartitioningSpec::round_robin(num_partitions);
        self.mode = ExchangeMode::Broadcast;
        self
    }

    /// Build the exchange operator.
    pub fn build(self) -> GrismResult<ExchangeExec> {
        let child = self.child.ok_or_else(|| {
            GrismError::value_error("Exchange requires a child operator")
        })?;

        Ok(ExchangeExec::new(child, self.partitioning, self.mode))
    }
}

impl Default for ExchangeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use grism_engine::operators::EmptyExec;

    #[test]
    fn test_exchange_mode_display() {
        assert_eq!(format!("{}", ExchangeMode::Shuffle), "Shuffle");
        assert_eq!(format!("{}", ExchangeMode::Broadcast), "Broadcast");
        assert_eq!(format!("{}", ExchangeMode::Gather), "Gather");
    }

    #[test]
    fn test_exchange_exec_creation() {
        let child = Arc::new(EmptyExec::new());
        let exchange = ExchangeExec::shuffle(child, vec!["id".to_string()], 4);

        assert_eq!(exchange.name(), "ExchangeExec");
        assert_eq!(exchange.mode(), ExchangeMode::Shuffle);
        assert!(exchange.capabilities().blocking);
    }

    #[test]
    fn test_exchange_builder() {
        let child = Arc::new(EmptyExec::new());
        let exchange = ExchangeBuilder::new()
            .child(child)
            .hash_by(vec!["key".to_string()], 8)
            .build()
            .unwrap();

        assert_eq!(exchange.partitioning().num_partitions(), 8);
    }

    #[test]
    fn test_exchange_state() {
        let mut state = ExchangeState::new(4);
        state.record_sent(0, 100, 1000);
        state.record_sent(1, 200, 2000);

        assert_eq!(state.total_sent(), 300);
        assert_eq!(state.bytes_sent, 3000);
    }
}
