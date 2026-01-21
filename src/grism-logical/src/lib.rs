//! Logical plan layer for Grism query execution.
//!
//! This crate provides:
//! - `LogicalOp` enum for logical operators
//! - `LogicalPlan` as the root of a query plan
//! - `LogicalExpr` for expression trees

pub mod expr;
pub mod ops;
pub mod plan;

// Re-export main types
pub use expr::{AggExpr, AggFunc, BinaryOp, LogicalExpr, UnaryOp};
pub use ops::{
    AggregateOp, Direction, ExpandOp, FilterOp, InferOp, LimitOp, LogicalOp, ProjectOp, Projection,
    ScanKind, ScanOp,
};
pub use plan::LogicalPlan;
