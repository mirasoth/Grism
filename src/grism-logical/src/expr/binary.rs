//! Binary operators for logical expressions (RFC-0003 compliant).

use grism_core::DataType;
use serde::{Deserialize, Serialize};

/// Binary operators for logical expressions.
///
/// These operators represent binary computations following RFC-0003.
/// All operators are deterministic and have well-defined type semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BinaryOp {
    // Arithmetic operators
    /// Addition (+)
    Add,
    /// Subtraction (-)
    Subtract,
    /// Multiplication (*)
    Multiply,
    /// Division (/)
    Divide,
    /// Modulo (%)
    Modulo,

    // Comparison operators
    /// Equality (=)
    Eq,
    /// Inequality (<>)
    NotEq,
    /// Less than (<)
    Lt,
    /// Less than or equal (<=)
    LtEq,
    /// Greater than (>)
    Gt,
    /// Greater than or equal (>=)
    GtEq,

    // Logical operators (three-valued logic per RFC-0002)
    /// Logical AND
    And,
    /// Logical OR
    Or,

    // String operators
    /// String concatenation
    Concat,

    // Special operators
    /// IS DISTINCT FROM (null-safe inequality)
    IsDistinctFrom,
    /// IS NOT DISTINCT FROM (null-safe equality)
    IsNotDistinctFrom,
}

impl BinaryOp {
    /// Check if this is an arithmetic operator.
    pub const fn is_arithmetic(&self) -> bool {
        matches!(
            self,
            Self::Add | Self::Subtract | Self::Multiply | Self::Divide | Self::Modulo
        )
    }

    /// Check if this is a comparison operator.
    pub const fn is_comparison(&self) -> bool {
        matches!(
            self,
            Self::Eq
                | Self::NotEq
                | Self::Lt
                | Self::LtEq
                | Self::Gt
                | Self::GtEq
                | Self::IsDistinctFrom
                | Self::IsNotDistinctFrom
        )
    }

    /// Check if this is a logical operator.
    pub const fn is_logical(&self) -> bool {
        matches!(self, Self::And | Self::Or)
    }

    /// Get the result type of this operator given input types.
    ///
    /// Returns `None` if the operation is not valid for the given types.
    pub fn result_type(&self, left: &DataType, right: &DataType) -> Option<DataType> {
        match self {
            // Arithmetic: numeric types, result is widest type
            Self::Add | Self::Subtract | Self::Multiply | Self::Divide | Self::Modulo => {
                match (left, right) {
                    (DataType::Int64, DataType::Int64) => Some(DataType::Int64),
                    (DataType::Float64, DataType::Float64) => Some(DataType::Float64),
                    (DataType::Int64, DataType::Float64) | (DataType::Float64, DataType::Int64) => {
                        Some(DataType::Float64)
                    }
                    _ => None,
                }
            }

            // Comparison: any comparable types, result is Bool
            Self::Eq
            | Self::NotEq
            | Self::Lt
            | Self::LtEq
            | Self::Gt
            | Self::GtEq
            | Self::IsDistinctFrom
            | Self::IsNotDistinctFrom => {
                // Types must be compatible for comparison
                if left == right || left.can_coerce_to(right) || right.can_coerce_to(left) {
                    Some(DataType::Bool)
                } else {
                    None
                }
            }

            // Logical: both must be Bool
            Self::And | Self::Or => {
                if matches!(left, DataType::Bool) && matches!(right, DataType::Bool) {
                    Some(DataType::Bool)
                } else {
                    None
                }
            }

            // String concatenation
            Self::Concat => {
                if left.is_string() && right.is_string() {
                    Some(DataType::String)
                } else {
                    None
                }
            }
        }
    }

    /// Get the operator symbol for display.
    pub const fn symbol(&self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Subtract => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::Modulo => "%",
            Self::Eq => "=",
            Self::NotEq => "<>",
            Self::Lt => "<",
            Self::LtEq => "<=",
            Self::Gt => ">",
            Self::GtEq => ">=",
            Self::And => "AND",
            Self::Or => "OR",
            Self::Concat => "||",
            Self::IsDistinctFrom => "IS DISTINCT FROM",
            Self::IsNotDistinctFrom => "IS NOT DISTINCT FROM",
        }
    }
}

impl std::fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.symbol())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arithmetic_type_inference() {
        assert_eq!(
            BinaryOp::Add.result_type(&DataType::Int64, &DataType::Int64),
            Some(DataType::Int64)
        );
        assert_eq!(
            BinaryOp::Add.result_type(&DataType::Int64, &DataType::Float64),
            Some(DataType::Float64)
        );
        assert_eq!(
            BinaryOp::Add.result_type(&DataType::String, &DataType::Int64),
            None
        );
    }

    #[test]
    fn test_comparison_type_inference() {
        assert_eq!(
            BinaryOp::Eq.result_type(&DataType::Int64, &DataType::Int64),
            Some(DataType::Bool)
        );
        assert_eq!(
            BinaryOp::Lt.result_type(&DataType::String, &DataType::String),
            Some(DataType::Bool)
        );
    }

    #[test]
    fn test_logical_type_inference() {
        assert_eq!(
            BinaryOp::And.result_type(&DataType::Bool, &DataType::Bool),
            Some(DataType::Bool)
        );
        assert_eq!(
            BinaryOp::Or.result_type(&DataType::Int64, &DataType::Bool),
            None
        );
    }

    #[test]
    fn test_operator_classification() {
        assert!(BinaryOp::Add.is_arithmetic());
        assert!(!BinaryOp::Add.is_comparison());

        assert!(BinaryOp::Eq.is_comparison());
        assert!(!BinaryOp::Eq.is_arithmetic());

        assert!(BinaryOp::And.is_logical());
        assert!(!BinaryOp::And.is_arithmetic());
    }
}
