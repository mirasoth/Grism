//! Logical expression tree.

use serde::{Deserialize, Serialize};

use grism_core::{schema::ColumnRef, types::DataType, types::Value};

use super::{BinaryOp, FuncExpr};

/// Unary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnaryOp {
    /// Logical NOT.
    Not,
    /// Numeric negation.
    Neg,
    /// Is null check.
    IsNull,
    /// Is not null check.
    IsNotNull,
}

impl std::fmt::Display for UnaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Not => write!(f, "NOT"),
            Self::Neg => write!(f, "-"),
            Self::IsNull => write!(f, "IS NULL"),
            Self::IsNotNull => write!(f, "IS NOT NULL"),
        }
    }
}

/// Logical expression in a query plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalExpr {
    /// Column reference.
    Column(ColumnRef),
    /// Literal value.
    Literal(Value),
    /// Binary operation.
    Binary {
        left: Box<LogicalExpr>,
        op: BinaryOp,
        right: Box<LogicalExpr>,
    },
    /// Unary operation.
    Unary { op: UnaryOp, expr: Box<LogicalExpr> },
    /// Function call.
    Func(FuncExpr),
    /// Type cast.
    Cast {
        expr: Box<LogicalExpr>,
        target_type: DataType,
    },
}

impl LogicalExpr {
    /// Create a column reference expression.
    pub fn column(name: impl Into<String>) -> Self {
        Self::Column(ColumnRef::parse(&name.into()))
    }

    /// Create a qualified column reference.
    pub fn qualified_column(qualifier: impl Into<String>, name: impl Into<String>) -> Self {
        Self::Column(ColumnRef::qualified(qualifier, name))
    }

    /// Create a literal expression.
    pub fn literal(value: impl Into<Value>) -> Self {
        Self::Literal(value.into())
    }

    /// Create a binary expression.
    pub fn binary(left: LogicalExpr, op: BinaryOp, right: LogicalExpr) -> Self {
        Self::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    /// Create a unary expression.
    pub fn unary(op: UnaryOp, expr: LogicalExpr) -> Self {
        Self::Unary {
            op,
            expr: Box::new(expr),
        }
    }

    /// Create a cast expression.
    pub fn cast(expr: LogicalExpr, target_type: DataType) -> Self {
        Self::Cast {
            expr: Box::new(expr),
            target_type,
        }
    }

    // Comparison operators

    /// Equality comparison.
    pub fn eq(self, other: LogicalExpr) -> Self {
        Self::binary(self, BinaryOp::Eq, other)
    }

    /// Inequality comparison.
    pub fn neq(self, other: LogicalExpr) -> Self {
        Self::binary(self, BinaryOp::Neq, other)
    }

    /// Greater than comparison.
    pub fn gt(self, other: LogicalExpr) -> Self {
        Self::binary(self, BinaryOp::Gt, other)
    }

    /// Greater than or equal comparison.
    pub fn gte(self, other: LogicalExpr) -> Self {
        Self::binary(self, BinaryOp::Gte, other)
    }

    /// Less than comparison.
    pub fn lt(self, other: LogicalExpr) -> Self {
        Self::binary(self, BinaryOp::Lt, other)
    }

    /// Less than or equal comparison.
    pub fn lte(self, other: LogicalExpr) -> Self {
        Self::binary(self, BinaryOp::Lte, other)
    }

    // Logical operators

    /// Logical AND.
    pub fn and(self, other: LogicalExpr) -> Self {
        Self::binary(self, BinaryOp::And, other)
    }

    /// Logical OR.
    pub fn or(self, other: LogicalExpr) -> Self {
        Self::binary(self, BinaryOp::Or, other)
    }

    /// Logical NOT.
    pub fn not(self) -> Self {
        Self::unary(UnaryOp::Not, self)
    }

    // Null checks

    /// Is null check.
    pub fn is_null(self) -> Self {
        Self::unary(UnaryOp::IsNull, self)
    }

    /// Is not null check.
    pub fn is_not_null(self) -> Self {
        Self::unary(UnaryOp::IsNotNull, self)
    }

    // Arithmetic operators

    /// Addition.
    pub fn add(self, other: LogicalExpr) -> Self {
        Self::binary(self, BinaryOp::Add, other)
    }

    /// Subtraction.
    pub fn sub(self, other: LogicalExpr) -> Self {
        Self::binary(self, BinaryOp::Sub, other)
    }

    /// Multiplication.
    pub fn mul(self, other: LogicalExpr) -> Self {
        Self::binary(self, BinaryOp::Mul, other)
    }

    /// Division.
    pub fn div(self, other: LogicalExpr) -> Self {
        Self::binary(self, BinaryOp::Div, other)
    }
}

impl std::fmt::Display for LogicalExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Column(col) => write!(f, "{}", col),
            Self::Literal(val) => write!(f, "{:?}", val),
            Self::Binary { left, op, right } => write!(f, "({} {} {})", left, op, right),
            Self::Unary { op, expr } => write!(f, "{} {}", op, expr),
            Self::Func(func) => write!(f, "{}({} args)", func.name, func.args.len()),
            Self::Cast { expr, target_type } => write!(f, "CAST({} AS {})", expr, target_type),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expression_building() {
        let expr = LogicalExpr::column("year").gte(LogicalExpr::literal(2022i64));

        assert!(matches!(
            expr,
            LogicalExpr::Binary {
                op: BinaryOp::Gte,
                ..
            }
        ));
    }

    #[test]
    fn test_compound_expression() {
        let expr = LogicalExpr::column("year")
            .gte(LogicalExpr::literal(2020i64))
            .and(LogicalExpr::column("year").lte(LogicalExpr::literal(2023i64)));

        assert!(matches!(
            expr,
            LogicalExpr::Binary {
                op: BinaryOp::And,
                ..
            }
        ));
    }
}
