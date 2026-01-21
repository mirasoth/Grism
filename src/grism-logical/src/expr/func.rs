//! Function expressions.

use serde::{Deserialize, Serialize};

use grism_core::types::DataType;

use super::LogicalExpr;

/// Function expression.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuncExpr {
    /// Function name.
    pub name: String,
    /// Function arguments.
    pub args: Vec<LogicalExpr>,
    /// Return type (if known).
    pub return_type: Option<DataType>,
}

impl FuncExpr {
    /// Create a new function expression.
    pub fn new(name: impl Into<String>, args: Vec<LogicalExpr>) -> Self {
        Self {
            name: name.into(),
            args,
            return_type: None,
        }
    }

    /// Set the return type.
    pub fn with_return_type(mut self, return_type: DataType) -> Self {
        self.return_type = Some(return_type);
        self
    }
}

/// Built-in function names.
pub mod builtin {
    /// Similarity function for vectors.
    pub const SIM: &str = "sim";
    /// Contains function for strings.
    pub const CONTAINS: &str = "contains";
    /// Length function for arrays/strings.
    pub const LEN: &str = "len";
    /// Coalesce function (first non-null).
    pub const COALESCE: &str = "coalesce";
}

impl LogicalExpr {
    /// Create a similarity function call.
    pub fn sim(left: LogicalExpr, right: LogicalExpr) -> Self {
        Self::Func(FuncExpr::new(builtin::SIM, vec![left, right]))
    }

    /// Create a contains function call.
    pub fn contains(expr: LogicalExpr, substring: LogicalExpr) -> Self {
        Self::Func(FuncExpr::new(builtin::CONTAINS, vec![expr, substring]))
    }

    /// Create a len function call.
    pub fn len(expr: LogicalExpr) -> Self {
        Self::Func(FuncExpr::new(builtin::LEN, vec![expr]))
    }

    /// Create a coalesce function call.
    pub fn coalesce(exprs: Vec<LogicalExpr>) -> Self {
        Self::Func(FuncExpr::new(builtin::COALESCE, exprs))
    }
}
