//! Execution stage definition for distributed plans.

use serde::{Deserialize, Serialize};

use grism_logical::LogicalOp;

/// Stage identifier.
pub type StageId = u64;

/// Shuffle strategy for data distribution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShuffleStrategy {
    /// No shuffle (preserve partitioning).
    None,
    /// Hash-based partitioning by key.
    Hash,
    /// Round-robin distribution.
    RoundRobin,
    /// Broadcast to all partitions.
    Broadcast,
    /// Single partition (collect).
    Single,
}

/// A stage in the distributed execution plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stage {
    /// Unique stage identifier.
    pub id: StageId,
    /// Number of partitions.
    pub partitions: usize,
    /// Operators in this stage.
    pub operators: Vec<LogicalOp>,
    /// Input shuffle strategy.
    pub shuffle: ShuffleStrategy,
    /// Dependencies (input stage IDs).
    pub dependencies: Vec<StageId>,
    /// Output columns for shuffle key.
    pub shuffle_keys: Vec<String>,
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

    /// Check if this stage has dependencies.
    pub fn has_dependencies(&self) -> bool {
        !self.dependencies.is_empty()
    }

    /// Check if this stage requires shuffle.
    pub fn requires_shuffle(&self) -> bool {
        self.shuffle != ShuffleStrategy::None
    }
}

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
        stage.add_operator(LogicalOp::Scan(ScanOp::nodes(Some("Person"))));

        assert_eq!(stage.operators.len(), 1);
    }
}
