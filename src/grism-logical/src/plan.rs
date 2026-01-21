//! Logical plan structure for Grism (RFC-0006 compliant).
//!
//! A LogicalPlan is a DAG of logical operators that represents a query.

use grism_core::Schema;
use serde::{Deserialize, Serialize};

use crate::ops::LogicalOp;

/// A logical plan representing a query.
///
/// # Definition (RFC-0006, Section 3.1)
///
/// A **Logical Plan** is a **directed acyclic graph (DAG)** of logical operators.
///
/// # Properties
///
/// - Nodes are operators
/// - Edges represent dataflow
/// - Plan MUST be acyclic
/// - Plan MUST be closed (all inputs resolved)
///
/// # Plan Invariants (RFC-0006, Section 3.2)
///
/// A valid logical plan MUST satisfy:
///
/// 1. **Semantic correctness**: The plan corresponds to a valid Hypergraph algebra expression.
/// 2. **Scope consistency**: All column references are resolvable at every node.
/// 3. **Type consistency**: All expressions are well-typed (RFC-0003).
/// 4. **Determinism preservation**: No rewrite may introduce non-determinism.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogicalPlan {
    /// The root operator of the plan.
    pub root: LogicalOp,

    /// The output schema of this plan (optional until type checking).
    pub schema: Option<Schema>,
}

impl LogicalPlan {
    /// Create a new logical plan with the given root operator.
    pub fn new(root: LogicalOp) -> Self {
        Self { root, schema: None }
    }

    /// Create a logical plan with a schema.
    pub fn with_schema(root: LogicalOp, schema: Schema) -> Self {
        Self {
            root,
            schema: Some(schema),
        }
    }

    /// Get a reference to the root operator.
    pub fn root(&self) -> &LogicalOp {
        &self.root
    }

    /// Get a mutable reference to the root operator.
    pub fn root_mut(&mut self) -> &mut LogicalOp {
        &mut self.root
    }

    /// Get the output schema if available.
    pub fn schema(&self) -> Option<&Schema> {
        self.schema.as_ref()
    }

    /// Set the output schema.
    pub fn set_schema(&mut self, schema: Schema) {
        self.schema = Some(schema);
    }

    /// Generate a tree-formatted explanation of the plan.
    ///
    /// This is a mandatory requirement per RFC-0006, Section 10.
    pub fn explain(&self) -> String {
        let mut output = String::new();
        output.push_str("Logical Plan:\n");
        output.push_str(&self.root.explain(1));
        output
    }

    /// Generate a verbose explanation with schema information.
    pub fn explain_verbose(&self) -> String {
        let mut output = self.explain();

        if let Some(ref schema) = self.schema {
            output.push_str("\nOutput Schema:\n");
            for col in &schema.columns {
                output.push_str(&format!(
                    "  - {}: {} {}\n",
                    col.qualified_name(),
                    col.data_type,
                    if col.nullable { "(nullable)" } else { "" }
                ));
            }
        }

        output
    }

    /// Count the number of operators in the plan.
    pub fn operator_count(&self) -> usize {
        fn count(op: &LogicalOp) -> usize {
            1 + op.inputs().iter().map(|i| count(i)).sum::<usize>()
        }
        count(&self.root)
    }

    /// Get the maximum depth of the plan tree.
    pub fn depth(&self) -> usize {
        fn max_depth(op: &LogicalOp) -> usize {
            let child_depths: Vec<_> = op.inputs().iter().map(|i| max_depth(i)).collect();
            1 + child_depths.into_iter().max().unwrap_or(0)
        }
        max_depth(&self.root)
    }

    /// Check if the plan contains a specific operator type.
    pub fn contains_op<F>(&self, predicate: F) -> bool
    where
        F: Fn(&LogicalOp) -> bool,
    {
        fn check<F>(op: &LogicalOp, predicate: &F) -> bool
        where
            F: Fn(&LogicalOp) -> bool,
        {
            if predicate(op) {
                return true;
            }
            op.inputs().iter().any(|i| check(i, predicate))
        }
        check(&self.root, &predicate)
    }

    /// Transform the plan by applying a function to each operator (bottom-up).
    pub fn transform<F>(self, f: F) -> Self
    where
        F: Fn(LogicalOp) -> LogicalOp + Clone,
    {
        fn transform_op<F>(op: LogicalOp, f: &F) -> LogicalOp
        where
            F: Fn(LogicalOp) -> LogicalOp + Clone,
        {
            // First transform children
            let transformed_children = op.map_children(|child| transform_op(child, f));
            // Then transform this node
            f(transformed_children)
        }

        Self {
            root: transform_op(self.root, &f),
            schema: self.schema,
        }
    }

    /// Transform the plan by applying a fallible function to each operator.
    pub fn try_transform<F, E>(self, f: F) -> Result<Self, E>
    where
        F: Fn(LogicalOp) -> Result<LogicalOp, E> + Clone,
    {
        fn transform_op<F, E>(op: LogicalOp, f: &F) -> Result<LogicalOp, E>
        where
            F: Fn(LogicalOp) -> Result<LogicalOp, E> + Clone,
        {
            // First transform children
            let transformed = match op {
                LogicalOp::Scan(_) | LogicalOp::Empty => op,
                LogicalOp::Expand { input, expand } => LogicalOp::Expand {
                    input: Box::new(transform_op(*input, f)?),
                    expand,
                },
                LogicalOp::Filter { input, filter } => LogicalOp::Filter {
                    input: Box::new(transform_op(*input, f)?),
                    filter,
                },
                LogicalOp::Project { input, project } => LogicalOp::Project {
                    input: Box::new(transform_op(*input, f)?),
                    project,
                },
                LogicalOp::Aggregate { input, aggregate } => LogicalOp::Aggregate {
                    input: Box::new(transform_op(*input, f)?),
                    aggregate,
                },
                LogicalOp::Limit { input, limit } => LogicalOp::Limit {
                    input: Box::new(transform_op(*input, f)?),
                    limit,
                },
                LogicalOp::Sort { input, sort } => LogicalOp::Sort {
                    input: Box::new(transform_op(*input, f)?),
                    sort,
                },
                LogicalOp::Union { left, right, union } => LogicalOp::Union {
                    left: Box::new(transform_op(*left, f)?),
                    right: Box::new(transform_op(*right, f)?),
                    union,
                },
                LogicalOp::Rename { input, rename } => LogicalOp::Rename {
                    input: Box::new(transform_op(*input, f)?),
                    rename,
                },
                LogicalOp::Infer { input, infer } => LogicalOp::Infer {
                    input: Box::new(transform_op(*input, f)?),
                    infer,
                },
            };
            f(transformed)
        }

        Ok(Self {
            root: transform_op(self.root, &f)?,
            schema: self.schema,
        })
    }
}

impl std::fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.root)
    }
}

impl From<LogicalOp> for LogicalPlan {
    fn from(op: LogicalOp) -> Self {
        Self::new(op)
    }
}

/// Builder for constructing logical plans fluently.
#[derive(Debug, Clone)]
pub struct PlanBuilder {
    op: LogicalOp,
}

impl PlanBuilder {
    /// Start building from a scan.
    pub fn scan(scan: crate::ops::ScanOp) -> Self {
        Self {
            op: LogicalOp::scan(scan),
        }
    }

    /// Add a filter.
    pub fn filter(self, filter: crate::ops::FilterOp) -> Self {
        Self {
            op: LogicalOp::filter(self.op, filter),
        }
    }

    /// Add an expand.
    pub fn expand(self, expand: crate::ops::ExpandOp) -> Self {
        Self {
            op: LogicalOp::expand(self.op, expand),
        }
    }

    /// Add a project.
    pub fn project(self, project: crate::ops::ProjectOp) -> Self {
        Self {
            op: LogicalOp::project(self.op, project),
        }
    }

    /// Add an aggregate.
    pub fn aggregate(self, aggregate: crate::ops::AggregateOp) -> Self {
        Self {
            op: LogicalOp::aggregate(self.op, aggregate),
        }
    }

    /// Add a limit.
    pub fn limit(self, limit: crate::ops::LimitOp) -> Self {
        Self {
            op: LogicalOp::limit(self.op, limit),
        }
    }

    /// Add a sort.
    pub fn sort(self, sort: crate::ops::SortOp) -> Self {
        Self {
            op: LogicalOp::sort(self.op, sort),
        }
    }

    /// Add a rename.
    pub fn rename(self, rename: crate::ops::RenameOp) -> Self {
        Self {
            op: LogicalOp::rename(self.op, rename),
        }
    }

    /// Build the final plan.
    pub fn build(self) -> LogicalPlan {
        LogicalPlan::new(self.op)
    }

    /// Build with a schema.
    pub fn build_with_schema(self, schema: Schema) -> LogicalPlan {
        LogicalPlan::with_schema(self.op, schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::{col, lit};
    use crate::ops::{FilterOp, ProjectOp, ScanOp};

    #[test]
    fn test_plan_creation() {
        let plan = LogicalPlan::new(LogicalOp::scan(ScanOp::nodes_with_label("Person")));

        assert!(plan.schema().is_none());
        assert_eq!(plan.operator_count(), 1);
        assert_eq!(plan.depth(), 1);
    }

    #[test]
    fn test_plan_builder() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .filter(FilterOp::new(col("age").gt(lit(18i64))))
            .project(ProjectOp::columns(["name", "city"]))
            .limit(crate::ops::LimitOp::new(10))
            .build();

        assert_eq!(plan.operator_count(), 4);
        assert_eq!(plan.depth(), 4);
    }

    #[test]
    fn test_plan_explain() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .filter(FilterOp::new(col("active").eq(lit(true))))
            .build();

        let explain = plan.explain();
        assert!(explain.contains("Logical Plan"));
        assert!(explain.contains("Filter"));
        assert!(explain.contains("Scan"));
    }

    #[test]
    fn test_plan_contains_op() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .filter(FilterOp::new(col("x").gt(lit(0i64))))
            .build();

        assert!(plan.contains_op(|op| matches!(op, LogicalOp::Filter { .. })));
        assert!(plan.contains_op(|op| matches!(op, LogicalOp::Scan(_))));
        assert!(!plan.contains_op(|op| matches!(op, LogicalOp::Aggregate { .. })));
    }

    #[test]
    fn test_plan_transform() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .filter(FilterOp::new(col("x").gt(lit(0i64))))
            .build();

        // Transform that adds a limit to all scans
        let transformed = plan.transform(|op| {
            if matches!(op, LogicalOp::Scan(_)) {
                LogicalOp::limit(op, crate::ops::LimitOp::new(1000))
            } else {
                op
            }
        });

        assert!(transformed.contains_op(|op| matches!(op, LogicalOp::Limit { .. })));
    }
}
