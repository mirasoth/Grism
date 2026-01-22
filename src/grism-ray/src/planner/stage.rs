//! Execution stage definition for distributed plans.
//!
//! A stage is a unit of parallel execution in a distributed plan.
//! Stages are separated by Exchange operators and execute as a unit
//! on one or more workers.

use serde::{Deserialize, Serialize};

use grism_logical::LogicalOp;

/// Stage identifier.
pub type StageId = u64;

/// Shuffle strategy for data distribution.
///
/// Determines how data flows between stages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum ShuffleStrategy {
    /// No shuffle (preserve partitioning).
    #[default]
    None,
    /// Hash-based partitioning by key.
    Hash,
    /// Round-robin distribution.
    RoundRobin,
    /// Broadcast to all partitions.
    Broadcast,
    /// Single partition (collect/gather).
    Single,
}

impl std::fmt::Display for ShuffleStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::Hash => write!(f, "Hash"),
            Self::RoundRobin => write!(f, "RoundRobin"),
            Self::Broadcast => write!(f, "Broadcast"),
            Self::Single => write!(f, "Single"),
        }
    }
}

/// A stage in the distributed execution plan.
///
/// Per RFC-0102 Section 7.4, a stage:
/// - Contains no internal Exchange operators
/// - Is executed as a unit on one or more workers
/// - Has explicit input and output partitioning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stage {
    /// Unique stage identifier.
    pub id: StageId,
    /// Number of partitions (parallelism).
    pub partitions: usize,
    /// Operators in this stage (logical ops for serialization).
    pub operators: Vec<LogicalOp>,
    /// Input shuffle strategy.
    pub shuffle: ShuffleStrategy,
    /// Dependencies (input stage IDs).
    pub dependencies: Vec<StageId>,
    /// Output columns for shuffle key (if Hash shuffle).
    pub shuffle_keys: Vec<String>,
    /// Optional stage name for debugging.
    pub name: Option<String>,
}

impl Stage {
    /// Create a new stage.
    pub fn new(id: StageId) -> Self {
        Self {
            id,
            partitions: 1,
            operators: Vec::new(),
            shuffle: ShuffleStrategy::None,
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

    /// Add an operator to this stage.
    pub fn with_operator(mut self, op: LogicalOp) -> Self {
        self.operators.push(op);
        self
    }

    /// Add an operator (mutating version).
    pub fn add_operator(&mut self, op: LogicalOp) {
        self.operators.push(op);
    }

    /// Set the shuffle strategy.
    pub fn with_shuffle(mut self, shuffle: ShuffleStrategy) -> Self {
        self.shuffle = shuffle;
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

    /// Check if this stage requires shuffle.
    pub fn requires_shuffle(&self) -> bool {
        self.shuffle != ShuffleStrategy::None
    }

    /// Check if this stage is a leaf (no dependencies).
    pub fn is_leaf(&self) -> bool {
        self.dependencies.is_empty()
    }

    /// Get the display name for this stage.
    pub fn display_name(&self) -> String {
        self.name.clone().unwrap_or_else(|| format!("Stage-{}", self.id))
    }

    /// Estimate the computational cost of this stage.
    ///
    /// Returns a rough estimate based on operator types.
    pub fn estimated_cost(&self) -> f64 {
        let mut cost = 0.0;
        for op in &self.operators {
            cost += match op {
                LogicalOp::Scan(_) => 1.0,
                LogicalOp::Filter { .. } => 0.5,
                LogicalOp::Project { .. } => 0.3,
                LogicalOp::Aggregate { .. } => 2.0,
                LogicalOp::Sort { .. } => 3.0,
                LogicalOp::Expand { .. } => 2.0,
                LogicalOp::Limit { .. } => 0.1,
                LogicalOp::Union { .. } => 0.5,
                LogicalOp::Rename { .. } => 0.1,
                LogicalOp::Infer { .. } => 5.0,
                LogicalOp::Empty => 0.0,
            };
        }
        cost
    }
}

impl std::fmt::Display for Stage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Stage[id={}, partitions={}, ops={}, shuffle={}]",
            self.id,
            self.partitions,
            self.operators.len(),
            self.shuffle
        )
    }
}

// ============================================================================
// Stage Builder
// ============================================================================

/// Builder for constructing stages.
#[derive(Debug, Default)]
pub struct StageBuilder {
    id: StageId,
    partitions: usize,
    operators: Vec<LogicalOp>,
    shuffle: ShuffleStrategy,
    dependencies: Vec<StageId>,
    shuffle_keys: Vec<String>,
    name: Option<String>,
}

impl StageBuilder {
    /// Create a new stage builder.
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

    /// Add an operator.
    pub fn operator(mut self, op: LogicalOp) -> Self {
        self.operators.push(op);
        self
    }

    /// Set shuffle strategy.
    pub fn shuffle(mut self, strategy: ShuffleStrategy) -> Self {
        self.shuffle = strategy;
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

    /// Build the stage.
    pub fn build(self) -> Stage {
        Stage {
            id: self.id,
            partitions: self.partitions,
            operators: self.operators,
            shuffle: self.shuffle,
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
    use grism_logical::ScanOp;

    #[test]
    fn test_stage_creation() {
        let stage = Stage::new(1)
            .with_partitions(4)
            .with_shuffle(ShuffleStrategy::Hash);

        assert_eq!(stage.id, 1);
        assert_eq!(stage.partitions, 4);
        assert!(stage.requires_shuffle());
    }

    #[test]
    fn test_stage_operators() {
        let mut stage = Stage::new(1);
        stage.add_operator(LogicalOp::Scan(ScanOp::nodes_with_label("Person")));

        assert_eq!(stage.operators.len(), 1);
    }

    #[test]
    fn test_stage_builder() {
        let stage = StageBuilder::new(42)
            .partitions(8)
            .shuffle(ShuffleStrategy::Hash)
            .depends_on(10)
            .name("my-stage")
            .build();

        assert_eq!(stage.id, 42);
        assert_eq!(stage.partitions, 8);
        assert_eq!(stage.dependencies, vec![10]);
        assert_eq!(stage.name, Some("my-stage".to_string()));
    }

    #[test]
    fn test_stage_display() {
        let stage = Stage::new(1).with_partitions(4);
        let display = format!("{}", stage);
        assert!(display.contains("id=1"));
        assert!(display.contains("partitions=4"));
    }

    #[test]
    fn test_shuffle_strategy_display() {
        assert_eq!(ShuffleStrategy::Hash.to_string(), "Hash");
        assert_eq!(ShuffleStrategy::Single.to_string(), "Single");
    }
}
