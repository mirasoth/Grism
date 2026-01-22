//! Physical plan properties and execution mode.

/// Execution mode for physical plans.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExecutionMode {
    /// Single-node local execution.
    #[default]
    Local,
    /// Distributed execution (future, RFC-0010).
    Distributed,
}

impl std::fmt::Display for ExecutionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local => write!(f, "Local"),
            Self::Distributed => write!(f, "Distributed"),
        }
    }
}

/// Properties of a physical plan.
///
/// These properties inform the executor about plan characteristics
/// and enable future distributed execution (RFC-0010).
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

impl PlanProperties {
    /// Create default local execution properties.
    pub const fn local() -> Self {
        Self {
            execution_mode: ExecutionMode::Local,
            requires_shuffle: false,
            contains_blocking: false,
            partitioning: None,
        }
    }

    /// Mark as containing blocking operators.
    pub fn with_blocking(mut self) -> Self {
        self.contains_blocking = true;
        self
    }

    /// Set partitioning spec.
    pub fn with_partitioning(mut self, spec: PartitioningSpec) -> Self {
        self.partitioning = Some(spec);
        self
    }
}

/// Partitioning specification (placeholder for RFC-0010).
///
/// This type exists to maintain forward compatibility with distributed
/// execution but is not fully used in local execution.
#[derive(Debug, Clone)]
pub struct PartitioningSpec {
    /// Partitioning strategy.
    pub strategy: PartitioningStrategy,
    /// Number of partitions.
    pub num_partitions: usize,
    /// Partition keys (column names for hash/range partitioning).
    pub partition_keys: Vec<String>,
}

impl PartitioningSpec {
    /// Create a single-partition spec (for local execution).
    pub fn single() -> Self {
        Self {
            strategy: PartitioningStrategy::Single,
            num_partitions: 1,
            partition_keys: vec![],
        }
    }

    /// Create a hash partitioning spec.
    pub fn hash(keys: Vec<String>, num_partitions: usize) -> Self {
        Self {
            strategy: PartitioningStrategy::Hash,
            num_partitions,
            partition_keys: keys,
        }
    }

    /// Create a range partitioning spec.
    pub fn range(keys: Vec<String>, num_partitions: usize) -> Self {
        Self {
            strategy: PartitioningStrategy::Range,
            num_partitions,
            partition_keys: keys,
        }
    }

    /// Create a round-robin partitioning spec.
    pub fn round_robin(num_partitions: usize) -> Self {
        Self {
            strategy: PartitioningStrategy::RoundRobin,
            num_partitions,
            partition_keys: vec![],
        }
    }

    /// Check if this is a single partition.
    pub const fn is_single(&self) -> bool {
        matches!(self.strategy, PartitioningStrategy::Single) || self.num_partitions == 1
    }
}

impl Default for PartitioningSpec {
    fn default() -> Self {
        Self::single()
    }
}

/// Partitioning strategies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PartitioningStrategy {
    /// Single partition (local execution).
    #[default]
    Single,
    /// Hash partitioning by keys.
    Hash,
    /// Range partitioning.
    Range,
    /// Round-robin distribution.
    RoundRobin,
}

impl std::fmt::Display for PartitioningStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Single => write!(f, "Single"),
            Self::Hash => write!(f, "Hash"),
            Self::Range => write!(f, "Range"),
            Self::RoundRobin => write!(f, "RoundRobin"),
        }
    }
}

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
    pub const fn streaming() -> Self {
        Self {
            blocking: false,
            requires_global_view: false,
            supports_predicate_pushdown: false,
            supports_projection_pushdown: false,
            stateless: true,
        }
    }

    /// Create capabilities for a blocking operator.
    pub const fn blocking() -> Self {
        Self {
            blocking: true,
            requires_global_view: true,
            supports_predicate_pushdown: false,
            supports_projection_pushdown: false,
            stateless: false,
        }
    }

    /// Create capabilities for a source operator.
    pub const fn source() -> Self {
        Self {
            blocking: false,
            requires_global_view: false,
            supports_predicate_pushdown: true,
            supports_projection_pushdown: true,
            stateless: true,
        }
    }

    /// Create capabilities for an expand operator.
    pub const fn expand() -> Self {
        Self {
            blocking: false,
            requires_global_view: false,
            supports_predicate_pushdown: false,
            supports_projection_pushdown: false,
            stateless: false, // Has internal state
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plan_properties_default() {
        let props = PlanProperties::default();
        assert_eq!(props.execution_mode, ExecutionMode::Local);
        assert!(!props.requires_shuffle);
        assert!(!props.contains_blocking);
    }

    #[test]
    fn test_partitioning_spec() {
        let single = PartitioningSpec::single();
        assert!(single.is_single());

        let hash = PartitioningSpec::hash(vec!["id".to_string()], 4);
        assert!(!hash.is_single());
        assert_eq!(hash.num_partitions, 4);
    }

    #[test]
    fn test_operator_caps() {
        let streaming = OperatorCaps::streaming();
        assert!(!streaming.blocking);
        assert!(streaming.stateless);

        let blocking = OperatorCaps::blocking();
        assert!(blocking.blocking);
        assert!(blocking.requires_global_view);
    }
}
