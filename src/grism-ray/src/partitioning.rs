//! Partitioning specifications for distributed execution.
//!
//! This module defines how data is partitioned across workers in a distributed
//! execution plan. Per RFC-0102, partitioning is explicit and determines
//! how data flows between stages.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use arrow_array::RecordBatch;
use serde::{Deserialize, Serialize};

// ============================================================================
// Partitioning Scheme
// ============================================================================

/// High-level partitioning scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PartitioningScheme {
    /// Data is not partitioned (single partition).
    Single,
    /// Data is hash-partitioned by key columns.
    Hash,
    /// Data is range-partitioned by key column.
    Range,
    /// Data is partitioned by graph adjacency.
    Adjacency,
    /// Data is distributed round-robin.
    RoundRobin,
    /// Unknown/unspecified partitioning.
    Unknown,
}

impl Default for PartitioningScheme {
    fn default() -> Self {
        Self::Unknown
    }
}

// ============================================================================
// Partitioning Specification
// ============================================================================

/// Detailed specification for how data is partitioned.
///
/// This type captures all the information needed to:
/// - Determine which partition a row belongs to
/// - Plan data movement between stages
/// - Optimize operator placement
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitioningSpec {
    /// Single partition (all data on one worker).
    Single,

    /// Hash partitioning by key columns.
    Hash {
        /// Column names to hash on.
        keys: Vec<String>,
        /// Number of partitions.
        num_partitions: usize,
    },

    /// Range partitioning by key column.
    Range {
        /// Column name to partition by.
        key: String,
        /// Partition boundaries (sorted).
        /// Each value represents the upper bound (exclusive) of a partition.
        boundaries: Vec<i64>,
    },

    /// Partitioning by graph adjacency.
    /// Keeps nodes and their neighbors together.
    Adjacency {
        /// Entity type being partitioned (node or hyperedge).
        entity_type: String,
        /// Number of partitions.
        num_partitions: usize,
    },

    /// Round-robin distribution.
    RoundRobin {
        /// Number of partitions.
        num_partitions: usize,
    },

    /// Unknown/unspecified partitioning.
    Unknown,
}

impl Default for PartitioningSpec {
    fn default() -> Self {
        Self::Unknown
    }
}

impl PartitioningSpec {
    /// Create a single-partition spec.
    pub const fn single() -> Self {
        Self::Single
    }

    /// Create a hash partitioning spec.
    pub fn hash(keys: Vec<String>, num_partitions: usize) -> Self {
        Self::Hash {
            keys,
            num_partitions,
        }
    }

    /// Create a round-robin partitioning spec.
    pub const fn round_robin(num_partitions: usize) -> Self {
        Self::RoundRobin { num_partitions }
    }

    /// Create an adjacency partitioning spec.
    pub fn adjacency(entity_type: impl Into<String>, num_partitions: usize) -> Self {
        Self::Adjacency {
            entity_type: entity_type.into(),
            num_partitions,
        }
    }

    /// Get the number of partitions.
    pub fn num_partitions(&self) -> usize {
        match self {
            Self::Single => 1,
            Self::Hash { num_partitions, .. }
            | Self::Adjacency { num_partitions, .. }
            | Self::RoundRobin { num_partitions } => *num_partitions,
            Self::Range { boundaries, .. } => boundaries.len() + 1,
            Self::Unknown => 1,
        }
    }

    /// Get the partitioning scheme.
    pub fn scheme(&self) -> PartitioningScheme {
        match self {
            Self::Single => PartitioningScheme::Single,
            Self::Hash { .. } => PartitioningScheme::Hash,
            Self::Range { .. } => PartitioningScheme::Range,
            Self::Adjacency { .. } => PartitioningScheme::Adjacency,
            Self::RoundRobin { .. } => PartitioningScheme::RoundRobin,
            Self::Unknown => PartitioningScheme::Unknown,
        }
    }

    /// Check if this partitioning satisfies the required partitioning.
    ///
    /// Returns true if data partitioned by `self` can be used directly
    /// without repartitioning for an operator that requires `required`.
    pub fn satisfies(&self, required: &Self) -> bool {
        match (self, required) {
            // Single partitioning satisfies anything (it's the most restrictive)
            (Self::Single, _) => true,

            // Unknown satisfies nothing except unknown
            (Self::Unknown, Self::Unknown) => true,
            (Self::Unknown, _) => false,

            // Same partitioning with same params
            (
                Self::Hash {
                    keys: k1,
                    num_partitions: n1,
                },
                Self::Hash {
                    keys: k2,
                    num_partitions: n2,
                },
            ) => k1 == k2 && n1 >= n2,

            (Self::RoundRobin { num_partitions: n1 }, Self::RoundRobin { num_partitions: n2 }) => {
                n1 == n2
            }

            // Range partitioning with matching key
            (Self::Range { key: k1, .. }, Self::Range { key: k2, .. }) => k1 == k2,

            // Adjacency with matching entity type
            (
                Self::Adjacency {
                    entity_type: e1, ..
                },
                Self::Adjacency {
                    entity_type: e2, ..
                },
            ) => e1 == e2,

            // Different schemes don't satisfy each other
            _ => false,
        }
    }

    /// Calculate which partition a row belongs to.
    ///
    /// This is used during exchange operations to route rows to the
    /// correct downstream partition.
    pub fn partition_for_row(&self, batch: &RecordBatch, row: usize) -> usize {
        match self {
            Self::Single => 0,

            Self::Hash {
                keys,
                num_partitions,
            } => {
                let mut hasher = DefaultHasher::new();
                for key in keys {
                    if let Some(col) = batch.column_by_name(key) {
                        // Hash the array element at the given row
                        // For simplicity, we hash the debug representation
                        // In production, we'd use proper Arrow hash kernels
                        let value = format!("{:?}", col.slice(row, 1));
                        value.hash(&mut hasher);
                    }
                }
                (hasher.finish() as usize) % num_partitions
            }

            Self::Range { key, boundaries } => {
                // TODO: Extract value and binary search in boundaries
                // For now, return 0 as placeholder
                let _ = (key, boundaries);
                0
            }

            Self::Adjacency { num_partitions, .. } => {
                // TODO: Use graph-aware partitioning
                // For now, use simple hash of node ID
                row % num_partitions
            }

            Self::RoundRobin { num_partitions } => row % num_partitions,

            Self::Unknown => 0,
        }
    }

    /// Partition a batch into multiple batches, one per partition.
    ///
    /// Returns a vector of (`partition_id`, batch) pairs.
    pub fn partition_batch(&self, batch: &RecordBatch) -> Vec<(usize, RecordBatch)> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return vec![];
        }

        let num_partitions = self.num_partitions();
        if num_partitions == 1 {
            return vec![(0, batch.clone())];
        }

        // Group rows by partition
        let mut partition_rows: Vec<Vec<usize>> = vec![vec![]; num_partitions];
        for row in 0..num_rows {
            let partition = self.partition_for_row(batch, row);
            partition_rows[partition].push(row);
        }

        // Create batches for each partition
        let mut result = Vec::with_capacity(num_partitions);
        for (partition_id, rows) in partition_rows.into_iter().enumerate() {
            if rows.is_empty() {
                continue;
            }

            // Use Arrow's take kernel to extract rows
            // For now, we'll create a simple filtered batch
            // TODO: Use proper take kernel for efficiency
            let indices =
                arrow_array::UInt32Array::from_iter_values(rows.iter().map(|&r| r as u32));
            let columns: Vec<_> = batch
                .columns()
                .iter()
                .map(|col| arrow::compute::take(col, &indices, None).unwrap())
                .collect();

            if let Ok(new_batch) = RecordBatch::try_new(batch.schema(), columns) {
                result.push((partition_id, new_batch));
            }
        }

        result
    }
}

impl std::fmt::Display for PartitioningSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Single => write!(f, "Single"),
            Self::Hash {
                keys,
                num_partitions,
            } => write!(f, "Hash({}, {})", keys.join(", "), num_partitions),
            Self::Range { key, boundaries } => {
                write!(f, "Range({}, {} partitions)", key, boundaries.len() + 1)
            }
            Self::Adjacency {
                entity_type,
                num_partitions,
            } => write!(f, "Adjacency({entity_type}, {num_partitions})"),
            Self::RoundRobin { num_partitions } => write!(f, "RoundRobin({num_partitions})"),
            Self::Unknown => write!(f, "Unknown"),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let id_array = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec![
            Some("Alice"),
            Some("Bob"),
            Some("Charlie"),
            Some("Diana"),
            Some("Eve"),
        ]);

        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap()
    }

    #[test]
    fn test_partitioning_spec_single() {
        let spec = PartitioningSpec::single();
        assert_eq!(spec.num_partitions(), 1);
        assert_eq!(spec.scheme(), PartitioningScheme::Single);
    }

    #[test]
    fn test_partitioning_spec_hash() {
        let spec = PartitioningSpec::hash(vec!["id".to_string()], 4);
        assert_eq!(spec.num_partitions(), 4);
        assert_eq!(spec.scheme(), PartitioningScheme::Hash);
    }

    #[test]
    fn test_partitioning_satisfies() {
        let single = PartitioningSpec::single();
        let hash1 = PartitioningSpec::hash(vec!["id".to_string()], 4);
        let hash2 = PartitioningSpec::hash(vec!["id".to_string()], 4);
        let hash3 = PartitioningSpec::hash(vec!["name".to_string()], 4);

        // Single satisfies anything
        assert!(single.satisfies(&hash1));

        // Same hash specs satisfy each other
        assert!(hash1.satisfies(&hash2));

        // Different keys don't satisfy
        assert!(!hash1.satisfies(&hash3));
    }

    #[test]
    fn test_partition_batch() {
        let batch = create_test_batch();
        let spec = PartitioningSpec::round_robin(2);

        let partitions = spec.partition_batch(&batch);
        assert!(!partitions.is_empty());

        let total_rows: usize = partitions.iter().map(|(_, b)| b.num_rows()).sum();
        assert_eq!(total_rows, 5);
    }
}
