//! Logical operators for query plans.

mod aggregate;
mod expand;
mod filter;
mod infer;
mod limit;
mod project;
mod scan;

pub use aggregate::AggregateOp;
pub use expand::{Direction, ExpandOp};
pub use filter::FilterOp;
pub use infer::InferOp;
pub use limit::LimitOp;
pub use project::{ProjectOp, Projection};
pub use scan::{ScanKind, ScanOp};

use serde::{Deserialize, Serialize};

/// Logical operator in a query plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalOp {
    /// Scan nodes, edges, or hyperedges.
    Scan(ScanOp),
    /// Expand to adjacent nodes via edges.
    Expand(ExpandOp),
    /// Filter rows based on a predicate.
    Filter(FilterOp),
    /// Project columns.
    Project(ProjectOp),
    /// Aggregate rows.
    Aggregate(AggregateOp),
    /// Limit number of rows.
    Limit(LimitOp),
    /// Apply inference rules.
    Infer(InferOp),
}

impl LogicalOp {
    /// Get the input operator, if any.
    pub fn input(&self) -> Option<&LogicalOp> {
        match self {
            Self::Scan(_) => None,
            Self::Expand(op) => Some(&op.input),
            Self::Filter(op) => Some(&op.input),
            Self::Project(op) => Some(&op.input),
            Self::Aggregate(op) => Some(&op.input),
            Self::Limit(op) => Some(&op.input),
            Self::Infer(op) => Some(&op.input),
        }
    }

    /// Get the name of this operator.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Scan(_) => "Scan",
            Self::Expand(_) => "Expand",
            Self::Filter(_) => "Filter",
            Self::Project(_) => "Project",
            Self::Aggregate(_) => "Aggregate",
            Self::Limit(_) => "Limit",
            Self::Infer(_) => "Infer",
        }
    }

    /// Explain this operator as a string.
    pub fn explain(&self, indent: usize) -> String {
        let prefix = "  ".repeat(indent);
        let mut result = format!("{}{}", prefix, self.explain_self());

        if let Some(input) = self.input() {
            result.push('\n');
            result.push_str(&input.explain(indent + 1));
        }

        result
    }

    fn explain_self(&self) -> String {
        match self {
            Self::Scan(op) => {
                let label = op.label.as_deref().unwrap_or("*");
                format!("Scan({:?}, label={})", op.kind, label)
            }
            Self::Expand(op) => {
                let edge = op.edge_label.as_deref().unwrap_or("*");
                let to = op.to_label.as_deref().unwrap_or("*");
                format!(
                    "Expand(edge={}, to={}, dir={:?}, hops={})",
                    edge, to, op.direction, op.hops
                )
            }
            Self::Filter(_) => "Filter(predicate)".to_string(),
            Self::Project(op) => {
                format!("Project({} columns)", op.columns.len())
            }
            Self::Aggregate(op) => {
                format!("Aggregate(keys={}, aggs={})", op.keys.len(), op.aggs.len())
            }
            Self::Limit(op) => format!("Limit({})", op.limit),
            Self::Infer(op) => format!("Infer(rule_set={})", op.rule_set),
        }
    }
}
