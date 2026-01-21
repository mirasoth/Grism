//! Predicate pushdown optimization rule (RFC-0006, Section 6.1).
//!
//! Move Filter operators as close as possible to data sources.

use common_error::GrismResult;
use grism_logical::{FilterOp, LogicalOp, LogicalPlan};

use super::rule::{OptimizationRule, Transformed};

/// Predicate pushdown rule.
///
/// # Description (RFC-0006, Section 6.1)
///
/// Move `Filter` operators as close as possible to data sources.
///
/// # Legal When
///
/// - Predicate references only columns available below
/// - Predicate is deterministic
/// - Predicate does not depend on aggregation output
///
/// # Example
///
/// Before:
/// ```text
/// Filter(p)
///   └─ Expand
///        └─ Scan
/// ```
///
/// After:
/// ```text
/// Expand
///   └─ Filter(p)
///        └─ Scan
/// ```
pub struct PredicatePushdown;

impl OptimizationRule for PredicatePushdown {
    fn name(&self) -> &'static str {
        "PredicatePushdown"
    }

    fn description(&self) -> &'static str {
        "Push filter predicates closer to data sources"
    }

    fn apply(&self, plan: LogicalPlan) -> GrismResult<Transformed> {
        let (new_root, changed) = push_down_predicates(plan.root);

        if changed {
            Ok(Transformed::yes(LogicalPlan {
                root: new_root,
                schema: plan.schema,
            }))
        } else {
            Ok(Transformed::no(LogicalPlan {
                root: new_root,
                schema: plan.schema,
            }))
        }
    }
}

/// Recursively push predicates down the plan tree.
fn push_down_predicates(op: LogicalOp) -> (LogicalOp, bool) {
    match op {
        // Filter on top of Expand: try to push through
        LogicalOp::Filter { input, filter } => {
            match *input {
                LogicalOp::Expand { input: expand_input, expand } => {
                    // Check if predicate can be pushed past expand
                    if filter.is_deterministic() && !filter.column_refs().is_empty() {
                        // Push the filter below the expand
                        let (pushed_input, _) = push_down_predicates(*expand_input);
                        let new_input = LogicalOp::filter(pushed_input, filter);

                        (
                            LogicalOp::Expand {
                                input: Box::new(new_input),
                                expand,
                            },
                            true,
                        )
                    } else {
                        // Can't push, just recurse
                        let (new_input, child_changed) = push_down_predicates(LogicalOp::Expand {
                            input: expand_input,
                            expand,
                        });

                        (
                            LogicalOp::Filter {
                                input: Box::new(new_input),
                                filter,
                            },
                            child_changed,
                        )
                    }
                }

                // Filter on top of Project: can push through if predicate uses only projected columns
                LogicalOp::Project { input: project_input, project } => {
                    let predicate_refs = filter.column_refs();
                    let projected_cols: std::collections::HashSet<_> =
                        project.output_names().into_iter().collect();

                    // Check if all predicate columns are in the projection
                    let can_push = predicate_refs.iter().all(|r| projected_cols.contains(r));

                    if can_push && filter.is_deterministic() {
                        // Push filter below project
                        let (pushed_input, _) = push_down_predicates(*project_input);
                        let new_input = LogicalOp::filter(pushed_input, filter);

                        (
                            LogicalOp::Project {
                                input: Box::new(new_input),
                                project,
                            },
                            true,
                        )
                    } else {
                        // Can't push, just recurse
                        let (new_input, child_changed) = push_down_predicates(LogicalOp::Project {
                            input: project_input,
                            project,
                        });

                        (
                            LogicalOp::Filter {
                                input: Box::new(new_input),
                                filter,
                            },
                            child_changed,
                        )
                    }
                }

                // Filter on top of Sort: can push through
                LogicalOp::Sort { input: sort_input, sort } => {
                    if filter.is_deterministic() {
                        let (pushed_input, _) = push_down_predicates(*sort_input);
                        let new_input = LogicalOp::filter(pushed_input, filter);

                        (
                            LogicalOp::Sort {
                                input: Box::new(new_input),
                                sort,
                            },
                            true,
                        )
                    } else {
                        let (new_input, child_changed) = push_down_predicates(LogicalOp::Sort {
                            input: sort_input,
                            sort,
                        });

                        (
                            LogicalOp::Filter {
                                input: Box::new(new_input),
                                filter,
                            },
                            child_changed,
                        )
                    }
                }

                // Filter on top of Limit: don't push through (changes semantics)
                LogicalOp::Limit { input: limit_input, limit } => {
                    let (new_input, child_changed) = push_down_predicates(LogicalOp::Limit {
                        input: limit_input,
                        limit,
                    });

                    (
                        LogicalOp::Filter {
                            input: Box::new(new_input),
                            filter,
                        },
                        child_changed,
                    )
                }

                // Combine adjacent filters
                LogicalOp::Filter { input: inner_input, filter: inner_filter } => {
                    let combined = FilterOp::new(inner_filter.predicate.and(filter.predicate));
                    let (new_input, _) = push_down_predicates(*inner_input);

                    (
                        LogicalOp::Filter {
                            input: Box::new(new_input),
                            filter: combined,
                        },
                        true,
                    )
                }

                // For other cases, just recurse into the child
                other => {
                    let (new_input, changed) = push_down_predicates(other);
                    (
                        LogicalOp::Filter {
                            input: Box::new(new_input),
                            filter,
                        },
                        changed,
                    )
                }
            }
        }

        // For other operators, just recurse into children
        LogicalOp::Expand { input, expand } => {
            let (new_input, changed) = push_down_predicates(*input);
            (
                LogicalOp::Expand {
                    input: Box::new(new_input),
                    expand,
                },
                changed,
            )
        }
        LogicalOp::Project { input, project } => {
            let (new_input, changed) = push_down_predicates(*input);
            (
                LogicalOp::Project {
                    input: Box::new(new_input),
                    project,
                },
                changed,
            )
        }
        LogicalOp::Aggregate { input, aggregate } => {
            // Don't push predicates below aggregates
            let (new_input, changed) = push_down_predicates(*input);
            (
                LogicalOp::Aggregate {
                    input: Box::new(new_input),
                    aggregate,
                },
                changed,
            )
        }
        LogicalOp::Limit { input, limit } => {
            let (new_input, changed) = push_down_predicates(*input);
            (
                LogicalOp::Limit {
                    input: Box::new(new_input),
                    limit,
                },
                changed,
            )
        }
        LogicalOp::Sort { input, sort } => {
            let (new_input, changed) = push_down_predicates(*input);
            (
                LogicalOp::Sort {
                    input: Box::new(new_input),
                    sort,
                },
                changed,
            )
        }
        LogicalOp::Union { left, right, union } => {
            let (new_left, left_changed) = push_down_predicates(*left);
            let (new_right, right_changed) = push_down_predicates(*right);
            (
                LogicalOp::Union {
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                    union,
                },
                left_changed || right_changed,
            )
        }
        LogicalOp::Rename { input, rename } => {
            let (new_input, changed) = push_down_predicates(*input);
            (
                LogicalOp::Rename {
                    input: Box::new(new_input),
                    rename,
                },
                changed,
            )
        }
        LogicalOp::Infer { input, infer } => {
            let (new_input, changed) = push_down_predicates(*input);
            (
                LogicalOp::Infer {
                    input: Box::new(new_input),
                    infer,
                },
                changed,
            )
        }

        // Leaf nodes - no change
        LogicalOp::Scan(_) | LogicalOp::Empty => (op, false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grism_logical::{col, lit, PlanBuilder, ProjectOp, ScanOp};

    #[test]
    fn test_push_filter_through_project() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .project(ProjectOp::columns(["name", "age"]))
            .filter(FilterOp::new(col("age").gt(lit(18i64))))
            .build();

        let result = PredicatePushdown.apply(plan).unwrap();

        assert!(result.changed);
        // Filter should now be below Project
        if let LogicalOp::Project { input, .. } = result.plan.root() {
            assert!(matches!(input.as_ref(), LogicalOp::Filter { .. }));
        } else {
            panic!("Expected Project at root");
        }
    }

    #[test]
    fn test_combine_adjacent_filters() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .filter(FilterOp::new(col("age").gt(lit(18i64))))
            .filter(FilterOp::new(col("active").eq(lit(true))))
            .build();

        let result = PredicatePushdown.apply(plan).unwrap();

        assert!(result.changed);
        // Should have only one filter now
        fn count_filters(op: &LogicalOp) -> usize {
            let self_count = if matches!(op, LogicalOp::Filter { .. }) { 1 } else { 0 };
            self_count + op.inputs().iter().map(|i| count_filters(i)).sum::<usize>()
        }
        let filter_count = count_filters(result.plan.root());
        assert_eq!(filter_count, 1);
    }

    #[test]
    fn test_no_push_through_aggregate() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Order"))
            .aggregate(
                grism_logical::AggregateOp::group_by(["customer_id"])
                    .with_agg(grism_logical::AggExpr::sum(col("amount"))),
            )
            .filter(FilterOp::new(col("customer_id").eq(lit(42i64))))
            .build();

        let result = PredicatePushdown.apply(plan).unwrap();

        // Filter should still be above Aggregate
        assert!(matches!(result.plan.root(), LogicalOp::Filter { .. }));
    }
}
