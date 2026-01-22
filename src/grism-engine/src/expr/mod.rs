//! Expression evaluation module for physical execution.
//!
//! This module provides expression evaluation that converts `LogicalExpr` to
//! Arrow compute operations for use in physical operators like Filter and Project.
//!
//! # Supported Expressions
//!
//! - `Literal` - Constant values
//! - `Column` / `QualifiedColumn` - Column references
//! - `Binary` - Arithmetic, comparison, and logical operations
//! - `Unary` - NOT, IS NULL, IS NOT NULL
//! - `Case` - CASE WHEN expressions
//! - `InList` - IN expressions
//!
//! # Example
//!
//! ```rust,ignore
//! use grism_engine::expr::ExprEvaluator;
//! use grism_logical::expr::col;
//!
//! let expr = col("age").gt(lit(18));
//! let evaluator = ExprEvaluator::new();
//! let result = evaluator.evaluate(&expr, &batch)?;
//! ```

mod evaluator;

pub use evaluator::ExprEvaluator;
