//! Execution stage definition for distributed plans.
//!
//! An execution stage is a unit of parallel execution in a distributed plan.
//! Stages are separated by Exchange operators and execute as a unit
//! on one or more workers.

use serde::{Deserialize, Serialize};

use crate::exchange::ExchangeMode;

/// Stage identifier.
pub type StageId = u64;

/// An execution stage in the distributed plan.
///
/// Per RFC-0102 Section 7.4, a stage:
/// - Contains no internal Exchange operators
/// - Is executed as a unit on one or more workers
/// - Has explicit input and output partitioning
///
/// Stages store operator metadata for serialization rather than full operator trees.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionStage {
    /// Unique stage identifier.
    pub id: StageId,
    /// Number of partitions (parallelism).
    pub partitions: usize,
    /// Operator names in this stage (for serialization/display).
    pub operator_names: Vec<String>,
    /// Input exchange mode (how data arrives from upstream).
    pub input_exchange: Option<ExchangeMode>,
    /// Output exchange mode (how data leaves to downstream).
    pub output_exchange: Option<ExchangeMode>,
    /// Dependencies (input stage IDs).
    pub dependencies: Vec<StageId>,
    /// Shuffle keys for hash-based exchange.
    pub shuffle_keys: Vec<String>,
    /// Optional stage name for debugging.
    pub name: Option<String>,
}

impl ExecutionStage {
    /// Create a new execution stage.
    pub fn new(id: StageId) -> Self {
        Self {
            id,
            partitions: 1,
            operator_names: Vec::new(),
            input_exchange: None,
            output_exchange: None,
            dependencies: Vec::new(),
            shuffle_keys: Vec::new(),
            name: None,
        }
    }

    /// Set the number of partitions.
    pub fn with_partitions(mut self, partitions: usize) -> Self {
        self.partitions = partitions;
        self
    }

    /// Add an operator name to this stage.
    pub fn with_operator(mut self, op_name: impl Into<String>) -> Self {
        self.operator_names.push(op_name.into());
        self
    }

    /// Add an operator name (mutating version).
    pub fn add_operator(&mut self, op_name: impl Into<String>) {
        self.operator_names.push(op_name.into());
    }

    /// Set the input exchange mode.
    pub fn with_input_exchange(mut self, mode: ExchangeMode) -> Self {
        self.input_exchange = Some(mode);
        self
    }

    /// Set the output exchange mode.
    pub fn with_output_exchange(mut self, mode: ExchangeMode) -> Self {
        self.output_exchange = Some(mode);
        self
    }

    /// Add a dependency.
    pub fn with_dependency(mut self, stage_id: StageId) -> Self {
        self.dependencies.push(stage_id);
        self
    }

    /// Set shuffle keys.
    pub fn with_shuffle_keys(mut self, keys: Vec<String>) -> Self {
        self.shuffle_keys = keys;
        self
    }

    /// Set stage name.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Check if this stage has dependencies.
    pub fn has_dependencies(&self) -> bool {
        !self.dependencies.is_empty()
    }

    /// Check if this stage requires input exchange.
    pub fn requires_input_exchange(&self) -> bool {
        self.input_exchange.is_some()
    }

    /// Check if this stage requires output exchange.
    pub fn requires_output_exchange(&self) -> bool {
        self.output_exchange.is_some()
    }

    /// Check if this stage is a leaf (no dependencies).
    pub fn is_leaf(&self) -> bool {
        self.dependencies.is_empty()
    }

    /// Get the display name for this stage.
    pub fn display_name(&self) -> String {
        self.name
            .clone()
            .unwrap_or_else(|| format!("Stage-{}", self.id))
    }

    /// Get the number of operators in this stage.
    pub fn num_operators(&self) -> usize {
        self.operator_names.len()
    }
}

impl std::fmt::Display for ExecutionStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ExecutionStage[id={}, partitions={}, ops={}]",
            self.id,
            self.partitions,
            self.operator_names.len()
        )
    }
}

// ============================================================================
// ExecutionStage Builder
// ============================================================================

/// Builder for constructing execution stages.
#[derive(Debug, Default)]
pub struct ExecutionStageBuilder {
    id: StageId,
    partitions: usize,
    operator_names: Vec<String>,
    input_exchange: Option<ExchangeMode>,
    output_exchange: Option<ExchangeMode>,
    dependencies: Vec<StageId>,
    shuffle_keys: Vec<String>,
    name: Option<String>,
}

impl ExecutionStageBuilder {
    /// Create a new execution stage builder.
    pub fn new(id: StageId) -> Self {
        Self {
            id,
            partitions: 1,
            ..Default::default()
        }
    }

    /// Set the number of partitions.
    pub fn partitions(mut self, n: usize) -> Self {
        self.partitions = n;
        self
    }

    /// Add an operator name.
    pub fn operator(mut self, op_name: impl Into<String>) -> Self {
        self.operator_names.push(op_name.into());
        self
    }

    /// Set input exchange mode.
    pub fn input_exchange(mut self, mode: ExchangeMode) -> Self {
        self.input_exchange = Some(mode);
        self
    }

    /// Set output exchange mode.
    pub fn output_exchange(mut self, mode: ExchangeMode) -> Self {
        self.output_exchange = Some(mode);
        self
    }

    /// Add a dependency.
    pub fn depends_on(mut self, stage_id: StageId) -> Self {
        self.dependencies.push(stage_id);
        self
    }

    /// Set shuffle keys.
    pub fn shuffle_keys(mut self, keys: Vec<String>) -> Self {
        self.shuffle_keys = keys;
        self
    }

    /// Set stage name.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Build the execution stage.
    pub fn build(self) -> ExecutionStage {
        ExecutionStage {
            id: self.id,
            partitions: self.partitions,
            operator_names: self.operator_names,
            input_exchange: self.input_exchange,
            output_exchange: self.output_exchange,
            dependencies: self.dependencies,
            shuffle_keys: self.shuffle_keys,
            name: self.name,
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_stage_creation() {
        let stage = ExecutionStage::new(1)
            .with_partitions(4)
            .with_input_exchange(ExchangeMode::Shuffle);

        assert_eq!(stage.id, 1);
        assert_eq!(stage.partitions, 4);
        assert!(stage.requires_input_exchange());
    }

    #[test]
    fn test_execution_stage_operators() {
        let mut stage = ExecutionStage::new(1);
        stage.add_operator("NodeScanExec");
        stage.add_operator("FilterExec");

        assert_eq!(stage.num_operators(), 2);
    }

    #[test]
    fn test_execution_stage_builder() {
        let stage = ExecutionStageBuilder::new(42)
            .partitions(8)
            .input_exchange(ExchangeMode::Shuffle)
            .depends_on(10)
            .name("my-stage")
            .build();

        assert_eq!(stage.id, 42);
        assert_eq!(stage.partitions, 8);
        assert_eq!(stage.dependencies, vec![10]);
        assert_eq!(stage.name, Some("my-stage".to_string()));
    }

    #[test]
    fn test_execution_stage_display() {
        let stage = ExecutionStage::new(1).with_partitions(4);
        let display = format!("{}", stage);
        assert!(display.contains("id=1"));
        assert!(display.contains("partitions=4"));
    }

    #[test]
    fn test_exchange_mode_used() {
        let stage = ExecutionStage::new(1).with_output_exchange(ExchangeMode::Gather);

        assert!(stage.requires_output_exchange());
        assert!(!stage.requires_input_exchange());
    }
}
