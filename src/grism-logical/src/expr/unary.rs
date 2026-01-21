//! Unary operators for logical expressions (RFC-0003 compliant).

use grism_core::DataType;
use serde::{Deserialize, Serialize};

/// Unary operators for logical expressions.
///
/// These operators represent unary computations following RFC-0003.
/// All operators are deterministic and have well-defined type semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnaryOp {
    /// Logical NOT
    Not,
    /// Arithmetic negation (-)
    Neg,
    /// NULL check (IS NULL)
    IsNull,
    /// NOT NULL check (IS NOT NULL)
    IsNotNull,
    /// TRUE check (IS TRUE) - for three-valued logic
    IsTrue,
    /// FALSE check (IS FALSE)
    IsFalse,
    /// UNKNOWN check (IS UNKNOWN)
    IsUnknown,
}

impl UnaryOp {
    /// Get the result type of this operator given the input type.
    ///
    /// Returns `None` if the operation is not valid for the given type.
    pub fn result_type(&self, input: &DataType) -> Option<DataType> {
        match self {
            // Logical NOT requires boolean input
            Self::Not => {
                if matches!(input, DataType::Bool) {
                    Some(DataType::Bool)
                } else {
                    None
                }
            }

            // Arithmetic negation requires numeric input
            Self::Neg => match input {
                DataType::Int64 => Some(DataType::Int64),
                DataType::Float64 => Some(DataType::Float64),
                _ => None,
            },

            // NULL checks work on any type, return Bool
            Self::IsNull | Self::IsNotNull => Some(DataType::Bool),

            // Truth checks require boolean input, return Bool
            Self::IsTrue | Self::IsFalse | Self::IsUnknown => {
                if matches!(input, DataType::Bool) {
                    Some(DataType::Bool)
                } else {
                    None
                }
            }
        }
    }

    /// Get the operator name for display.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Not => "NOT",
            Self::Neg => "-",
            Self::IsNull => "IS NULL",
            Self::IsNotNull => "IS NOT NULL",
            Self::IsTrue => "IS TRUE",
            Self::IsFalse => "IS FALSE",
            Self::IsUnknown => "IS UNKNOWN",
        }
    }

    /// Check if this operator is null-safe (always returns non-null).
    pub fn is_null_safe(&self) -> bool {
        matches!(
            self,
            Self::IsNull | Self::IsNotNull | Self::IsTrue | Self::IsFalse | Self::IsUnknown
        )
    }
}

impl std::fmt::Display for UnaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_type_inference() {
        assert_eq!(
            UnaryOp::Not.result_type(&DataType::Bool),
            Some(DataType::Bool)
        );
        assert_eq!(UnaryOp::Not.result_type(&DataType::Int64), None);
    }

    #[test]
    fn test_neg_type_inference() {
        assert_eq!(
            UnaryOp::Neg.result_type(&DataType::Int64),
            Some(DataType::Int64)
        );
        assert_eq!(
            UnaryOp::Neg.result_type(&DataType::Float64),
            Some(DataType::Float64)
        );
        assert_eq!(UnaryOp::Neg.result_type(&DataType::String), None);
    }

    #[test]
    fn test_is_null_type_inference() {
        assert_eq!(
            UnaryOp::IsNull.result_type(&DataType::Int64),
            Some(DataType::Bool)
        );
        assert_eq!(
            UnaryOp::IsNull.result_type(&DataType::String),
            Some(DataType::Bool)
        );
    }

    #[test]
    fn test_null_safety() {
        assert!(UnaryOp::IsNull.is_null_safe());
        assert!(UnaryOp::IsNotNull.is_null_safe());
        assert!(!UnaryOp::Not.is_null_safe());
        assert!(!UnaryOp::Neg.is_null_safe());
    }
}
