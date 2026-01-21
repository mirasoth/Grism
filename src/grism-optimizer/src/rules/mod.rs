//! Optimization rules for Grism query plans (RFC-0006 compliant).
//!
//! This module provides the rewrite rules that transform logical plans
//! while preserving semantic equivalence.
//!
//! # Rule Categories (RFC-0006, Section 6)
//!
//! - **Predicate Pushdown**: Move filters closer to data sources
//! - **Projection Pruning**: Remove unused columns early
//! - **Constant Folding**: Evaluate constant expressions at plan time
//! - **Expand Reordering**: Reorder independent expand operations (future)
//! - **Filter-Expand Fusion**: Fuse traversal predicates into expansion (future)
//! - **Limit Pushdown**: Move limit operations upstream (future)
//!
//! # Rewrite Safety (RFC-0006, Section 5.2)
//!
//! A rewrite is **legal** if and only if all of the following hold:
//!
//! 1. **Hyperedge Preservation**: The multiset of logical hyperedges produced is identical
//! 2. **Column Semantics Preservation**: Column values are identical for all rows
//! 3. **NULL Semantics Preservation**: Three-valued logic behavior is unchanged
//! 4. **Determinism Preservation**: No volatile expressions are reordered
//! 5. **Scope Preservation**: No column or role shadowing is introduced

mod constant_folding;
mod optimizer;
mod predicate_pushdown;
mod projection_pruning;
mod rule;

pub use constant_folding::ConstantFolding;
pub use optimizer::{Optimizer, OptimizerConfig};
pub use predicate_pushdown::PredicatePushdown;
pub use projection_pruning::ProjectionPruning;
pub use rule::{OptimizationRule, OptimizedPlan, RuleTrace, Transformed};
