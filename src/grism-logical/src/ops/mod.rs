//! Logical operators for Grism query planning (RFC-0002 compliant).
//!
//! This module provides the canonical set of logical operators for Hypergraph.
//! These operators define *what* a query does, not *how* it is executed.
//!
//! # Operator Categories (RFC-0002, Section 5.1)
//!
//! | Category    | Operators                    |
//! | ----------- | ---------------------------- |
//! | Source      | `Scan`                       |
//! | Navigation  | `Expand`                     |
//! | Restriction | `Filter`                     |
//! | Shape       | `Project`, `Rename`          |
//! | Combination | `Union`                      |
//! | Aggregation | `Aggregate`                  |
//! | Derivation  | `Infer`                      |
//! | Control     | `Limit`, `Sort`              |
//!
//! # Important Design Notes
//!
//! - There is **no Join operator**. Relational composition is expressed via `Expand`.
//! - All operators are deterministic and side-effect free.
//! - Operators form a closed algebra: every operator consumes and produces Hypergraphs.

mod aggregate;
mod expand;
mod filter;
mod infer;
mod limit;
mod project;
mod rename;
mod scan;
mod sort;
mod union;

pub use aggregate::AggregateOp;
pub use expand::{Direction, ExpandMode, ExpandOp, HopRange};
pub use filter::FilterOp;
pub use infer::{InferMode, InferOp};
pub use limit::LimitOp;
pub use project::ProjectOp;
pub use rename::RenameOp;
pub use scan::{ScanKind, ScanOp};
pub use sort::{SortKey, SortOp};
pub use union::UnionOp;

use serde::{Deserialize, Serialize};

/// A logical operator in the Grism query plan.
///
/// This enum represents all possible logical operators in the Hypergraph algebra.
/// Each variant wraps a specific operator type with its parameters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LogicalOp {
    /// Scan - source of data.
    Scan(ScanOp),

    /// Expand - traversal over hyperedges.
    Expand {
        /// Input plan.
        input: Box<LogicalOp>,
        /// Expand operation parameters.
        expand: ExpandOp,
    },

    /// Filter - restriction by predicate.
    Filter {
        /// Input plan.
        input: Box<LogicalOp>,
        /// Filter operation parameters.
        filter: FilterOp,
    },

    /// Project - column selection.
    Project {
        /// Input plan.
        input: Box<LogicalOp>,
        /// Project operation parameters.
        project: ProjectOp,
    },

    /// Aggregate - grouping and aggregation.
    Aggregate {
        /// Input plan.
        input: Box<LogicalOp>,
        /// Aggregate operation parameters.
        aggregate: AggregateOp,
    },

    /// Limit - row count restriction.
    Limit {
        /// Input plan.
        input: Box<LogicalOp>,
        /// Limit operation parameters.
        limit: LimitOp,
    },

    /// Sort - row ordering.
    Sort {
        /// Input plan.
        input: Box<LogicalOp>,
        /// Sort operation parameters.
        sort: SortOp,
    },

    /// Union - combine two plans.
    Union {
        /// Left input plan.
        left: Box<LogicalOp>,
        /// Right input plan.
        right: Box<LogicalOp>,
        /// Union operation parameters.
        union: UnionOp,
    },

    /// Rename - column renaming.
    Rename {
        /// Input plan.
        input: Box<LogicalOp>,
        /// Rename operation parameters.
        rename: RenameOp,
    },

    /// Infer - declarative rule application.
    Infer {
        /// Input plan.
        input: Box<LogicalOp>,
        /// Infer operation parameters.
        infer: InferOp,
    },

    /// Empty relation (zero rows).
    Empty,
}

impl LogicalOp {
    // ========== Constructors ==========

    /// Create a scan operator.
    pub fn scan(scan: ScanOp) -> Self {
        Self::Scan(scan)
    }

    /// Create an expand operator.
    pub fn expand(input: LogicalOp, expand: ExpandOp) -> Self {
        Self::Expand {
            input: Box::new(input),
            expand,
        }
    }

    /// Create a filter operator.
    pub fn filter(input: LogicalOp, filter: FilterOp) -> Self {
        Self::Filter {
            input: Box::new(input),
            filter,
        }
    }

    /// Create a project operator.
    pub fn project(input: LogicalOp, project: ProjectOp) -> Self {
        Self::Project {
            input: Box::new(input),
            project,
        }
    }

    /// Create an aggregate operator.
    pub fn aggregate(input: LogicalOp, aggregate: AggregateOp) -> Self {
        Self::Aggregate {
            input: Box::new(input),
            aggregate,
        }
    }

    /// Create a limit operator.
    pub fn limit(input: LogicalOp, limit: LimitOp) -> Self {
        Self::Limit {
            input: Box::new(input),
            limit,
        }
    }

    /// Create a sort operator.
    pub fn sort(input: LogicalOp, sort: SortOp) -> Self {
        Self::Sort {
            input: Box::new(input),
            sort,
        }
    }

    /// Create a union operator.
    pub fn union(left: LogicalOp, right: LogicalOp, union: UnionOp) -> Self {
        Self::Union {
            left: Box::new(left),
            right: Box::new(right),
            union,
        }
    }

    /// Create a rename operator.
    pub fn rename(input: LogicalOp, rename: RenameOp) -> Self {
        Self::Rename {
            input: Box::new(input),
            rename,
        }
    }

    /// Create an infer operator.
    pub fn infer(input: LogicalOp, infer: InferOp) -> Self {
        Self::Infer {
            input: Box::new(input),
            infer,
        }
    }

    /// Create an empty relation.
    pub fn empty() -> Self {
        Self::Empty
    }

    // ========== Analysis methods ==========

    /// Get the operator name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Scan(_) => "Scan",
            Self::Expand { .. } => "Expand",
            Self::Filter { .. } => "Filter",
            Self::Project { .. } => "Project",
            Self::Aggregate { .. } => "Aggregate",
            Self::Limit { .. } => "Limit",
            Self::Sort { .. } => "Sort",
            Self::Union { .. } => "Union",
            Self::Rename { .. } => "Rename",
            Self::Infer { .. } => "Infer",
            Self::Empty => "Empty",
        }
    }

    /// Get the number of inputs to this operator.
    pub fn input_count(&self) -> usize {
        match self {
            Self::Scan(_) | Self::Empty => 0,
            Self::Union { .. } => 2,
            _ => 1,
        }
    }

    /// Check if this is a leaf operator (no inputs).
    pub fn is_leaf(&self) -> bool {
        self.input_count() == 0
    }

    /// Get the input operators.
    pub fn inputs(&self) -> Vec<&LogicalOp> {
        match self {
            Self::Scan(_) | Self::Empty => vec![],
            Self::Expand { input, .. }
            | Self::Filter { input, .. }
            | Self::Project { input, .. }
            | Self::Aggregate { input, .. }
            | Self::Limit { input, .. }
            | Self::Sort { input, .. }
            | Self::Rename { input, .. }
            | Self::Infer { input, .. } => vec![input.as_ref()],
            Self::Union { left, right, .. } => vec![left.as_ref(), right.as_ref()],
        }
    }

    /// Get mutable references to input operators.
    pub fn inputs_mut(&mut self) -> Vec<&mut LogicalOp> {
        match self {
            Self::Scan(_) | Self::Empty => vec![],
            Self::Expand { input, .. }
            | Self::Filter { input, .. }
            | Self::Project { input, .. }
            | Self::Aggregate { input, .. }
            | Self::Limit { input, .. }
            | Self::Sort { input, .. }
            | Self::Rename { input, .. }
            | Self::Infer { input, .. } => vec![input.as_mut()],
            Self::Union { left, right, .. } => vec![left.as_mut(), right.as_mut()],
        }
    }

    /// Map over children, replacing them with transformed versions.
    pub fn map_children<F>(self, mut f: F) -> Self
    where
        F: FnMut(LogicalOp) -> LogicalOp,
    {
        match self {
            Self::Scan(_) | Self::Empty => self,
            Self::Expand { input, expand } => Self::Expand {
                input: Box::new(f(*input)),
                expand,
            },
            Self::Filter { input, filter } => Self::Filter {
                input: Box::new(f(*input)),
                filter,
            },
            Self::Project { input, project } => Self::Project {
                input: Box::new(f(*input)),
                project,
            },
            Self::Aggregate { input, aggregate } => Self::Aggregate {
                input: Box::new(f(*input)),
                aggregate,
            },
            Self::Limit { input, limit } => Self::Limit {
                input: Box::new(f(*input)),
                limit,
            },
            Self::Sort { input, sort } => Self::Sort {
                input: Box::new(f(*input)),
                sort,
            },
            Self::Union { left, right, union } => Self::Union {
                left: Box::new(f(*left)),
                right: Box::new(f(*right)),
                union,
            },
            Self::Rename { input, rename } => Self::Rename {
                input: Box::new(f(*input)),
                rename,
            },
            Self::Infer { input, infer } => Self::Infer {
                input: Box::new(f(*input)),
                infer,
            },
        }
    }

    /// Format as a tree string with indentation.
    pub fn explain(&self, indent: usize) -> String {
        let prefix = "  ".repeat(indent);
        let mut result = format!("{}{}\n", prefix, self);

        for input in self.inputs() {
            result.push_str(&input.explain(indent + 1));
        }

        result
    }
}

impl std::fmt::Display for LogicalOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scan(scan) => write!(f, "{}", scan),
            Self::Expand { expand, .. } => write!(f, "{}", expand),
            Self::Filter { filter, .. } => write!(f, "{}", filter),
            Self::Project { project, .. } => write!(f, "{}", project),
            Self::Aggregate { aggregate, .. } => write!(f, "{}", aggregate),
            Self::Limit { limit, .. } => write!(f, "{}", limit),
            Self::Sort { sort, .. } => write!(f, "{}", sort),
            Self::Union { union, .. } => write!(f, "{}", union),
            Self::Rename { rename, .. } => write!(f, "{}", rename),
            Self::Infer { infer, .. } => write!(f, "{}", infer),
            Self::Empty => write!(f, "Empty"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::{col, lit};

    #[test]
    fn test_operator_chain() {
        // Build: Scan(Person) -> Filter(age > 18) -> Project(name, city)
        let plan = LogicalOp::project(
            LogicalOp::filter(
                LogicalOp::scan(ScanOp::nodes_with_label("Person")),
                FilterOp::new(col("age").gt(lit(18i64))),
            ),
            ProjectOp::columns(["name", "city"]),
        );

        assert_eq!(plan.name(), "Project");
        assert!(!plan.is_leaf());
    }

    #[test]
    fn test_operator_inputs() {
        let scan = LogicalOp::scan(ScanOp::nodes());
        assert_eq!(scan.input_count(), 0);
        assert!(scan.is_leaf());

        let filter = LogicalOp::filter(scan.clone(), FilterOp::new(col("x").gt(lit(0i64))));
        assert_eq!(filter.input_count(), 1);

        let union = LogicalOp::union(scan.clone(), scan.clone(), UnionOp::all());
        assert_eq!(union.input_count(), 2);
    }

    #[test]
    fn test_explain() {
        let plan = LogicalOp::filter(
            LogicalOp::scan(ScanOp::nodes_with_label("Person")),
            FilterOp::new(col("age").gt(lit(18i64))),
        );

        let explain = plan.explain(0);
        assert!(explain.contains("Filter"));
        assert!(explain.contains("Scan"));
    }

    #[test]
    fn test_map_children() {
        let plan = LogicalOp::filter(
            LogicalOp::scan(ScanOp::nodes_with_label("Person")),
            FilterOp::new(col("active").eq(lit(true))),
        );

        // Add a limit to the child
        let transformed = plan.map_children(|child| LogicalOp::limit(child, LimitOp::new(100)));

        // The transformed plan should have Limit as its input
        if let LogicalOp::Filter { input, .. } = transformed {
            assert_eq!(input.name(), "Limit");
        } else {
            panic!("Expected Filter");
        }
    }
}
