//! Limit operator for logical planning (RFC-0002 compliant).
//!
//! Limit constrains the number of output rows.

use serde::{Deserialize, Serialize};

/// Limit operator - row count restriction.
///
/// # Semantics (RFC-0002, Section 6.9)
///
/// - Purely logical constraint
/// - Execution order is unspecified unless combined with Sort
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LimitOp {
    /// Maximum number of rows to return.
    pub limit: usize,

    /// Number of rows to skip before returning.
    pub offset: usize,
}

impl LimitOp {
    /// Create a limit with no offset.
    pub fn new(limit: usize) -> Self {
        Self { limit, offset: 0 }
    }

    /// Create a limit with offset.
    pub fn with_offset(limit: usize, offset: usize) -> Self {
        Self { limit, offset }
    }

    /// Check if this limit has an offset.
    pub fn has_offset(&self) -> bool {
        self.offset > 0
    }
}

impl std::fmt::Display for LimitOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.offset > 0 {
            write!(f, "Limit({}, offset={})", self.limit, self.offset)
        } else {
            write!(f, "Limit({})", self.limit)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_limit_creation() {
        let limit = LimitOp::new(10);
        assert_eq!(limit.limit, 10);
        assert_eq!(limit.offset, 0);
        assert!(!limit.has_offset());
    }

    #[test]
    fn test_limit_with_offset() {
        let limit = LimitOp::with_offset(10, 20);
        assert!(limit.has_offset());
        assert_eq!(limit.to_string(), "Limit(10, offset=20)");
    }
}
