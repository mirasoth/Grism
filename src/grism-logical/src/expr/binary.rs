//! Binary operators for expressions.

use serde::{Deserialize, Serialize};

/// Binary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryOp {
    // Comparison
    /// Equality (==).
    Eq,
    /// Inequality (!=).
    Neq,
    /// Greater than (>).
    Gt,
    /// Greater than or equal (>=).
    Gte,
    /// Less than (<).
    Lt,
    /// Less than or equal (<=).
    Lte,

    // Logical
    /// Logical AND.
    And,
    /// Logical OR.
    Or,

    // Arithmetic
    /// Addition.
    Add,
    /// Subtraction.
    Sub,
    /// Multiplication.
    Mul,
    /// Division.
    Div,
    /// Modulo.
    Mod,

    // String
    /// Pattern matching (LIKE).
    Like,

    // Vector
    /// Vector similarity.
    Similarity,
}

impl BinaryOp {
    /// Get the operator symbol for display.
    pub fn symbol(&self) -> &'static str {
        match self {
            Self::Eq => "==",
            Self::Neq => "!=",
            Self::Gt => ">",
            Self::Gte => ">=",
            Self::Lt => "<",
            Self::Lte => "<=",
            Self::And => "&&",
            Self::Or => "||",
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::Mod => "%",
            Self::Like => "LIKE",
            Self::Similarity => "~",
        }
    }

    /// Check if this is a comparison operator.
    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            Self::Eq | Self::Neq | Self::Gt | Self::Gte | Self::Lt | Self::Lte
        )
    }

    /// Check if this is a logical operator.
    pub fn is_logical(&self) -> bool {
        matches!(self, Self::And | Self::Or)
    }

    /// Check if this is an arithmetic operator.
    pub fn is_arithmetic(&self) -> bool {
        matches!(
            self,
            Self::Add | Self::Sub | Self::Mul | Self::Div | Self::Mod
        )
    }
}

impl std::fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.symbol())
    }
}
