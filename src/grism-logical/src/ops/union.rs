//! Union operator for logical planning (RFC-0002 compliant).
//!
//! Union combines two hypergraphs into one.

use serde::{Deserialize, Serialize};

/// Union operator - multiset union.
///
/// # Semantics (RFC-0002, Section 6.6)
///
/// - Multiset union (preserves duplicates)
/// - Schemas MUST be compatible
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct UnionOp {
    /// Whether to remove duplicates (UNION vs UNION ALL).
    pub distinct: bool,
}

impl UnionOp {
    /// Create a UNION ALL (preserves duplicates).
    pub fn all() -> Self {
        Self { distinct: false }
    }

    /// Create a UNION DISTINCT (removes duplicates).
    pub fn distinct() -> Self {
        Self { distinct: true }
    }
}

impl std::fmt::Display for UnionOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.distinct {
            write!(f, "Union(DISTINCT)")
        } else {
            write!(f, "Union(ALL)")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_union_creation() {
        let union_all = UnionOp::all();
        assert!(!union_all.distinct);

        let union_distinct = UnionOp::distinct();
        assert!(union_distinct.distinct);
    }

    #[test]
    fn test_union_display() {
        assert_eq!(UnionOp::all().to_string(), "Union(ALL)");
        assert_eq!(UnionOp::distinct().to_string(), "Union(DISTINCT)");
    }
}
