//! Projection pruning optimization rule (RFC-0006, Section 6.2).
//!
//! Remove unused columns early in the plan.

use std::collections::HashSet;

use common_error::GrismResult;
use grism_logical::{LogicalOp, LogicalPlan};

use super::rule::{OptimizationRule, Transformed};

/// Projection pruning rule.
///
/// # Description (RFC-0006, Section 6.2)
///
/// Remove unused columns early.
///
/// # Legal When
///
/// - Dropped columns are not referenced downstream
/// - Dropped columns are not required for identity preservation
pub struct ProjectionPruning;

impl OptimizationRule for ProjectionPruning {
    fn name(&self) -> &'static str {
        "ProjectionPruning"
    }

    fn description(&self) -> &'static str {
        "Remove unused columns from projections"
    }

    fn apply(&self, plan: LogicalPlan) -> GrismResult<Transformed> {
        // Collect all columns referenced in the plan
        let used_columns = collect_used_columns(plan.root());

        // Prune projections
        let (new_root, changed) = prune_projections(plan.root, &used_columns);

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

/// Collect all column references used in the plan from the root down.
fn collect_used_columns(op: &LogicalOp) -> HashSet<String> {
    let mut used = HashSet::new();

    match op {
        LogicalOp::Scan(scan) => {
            // If scan has a predicate, collect its columns
            if let Some(ref pred) = scan.predicate {
                used.extend(pred.column_refs());
            }
        }
        LogicalOp::Filter { filter, input } => {
            used.extend(filter.column_refs());
            used.extend(collect_used_columns(input));
        }
        LogicalOp::Project { project, input } => {
            used.extend(project.column_refs());
            used.extend(collect_used_columns(input));
        }
        LogicalOp::Expand { expand, input } => {
            // Expand references roles/columns
            if let Some(ref pred) = expand.edge_predicate {
                used.extend(pred.column_refs());
            }
            if let Some(ref pred) = expand.target_predicate {
                used.extend(pred.column_refs());
            }
            used.extend(collect_used_columns(input));
        }
        LogicalOp::Aggregate { aggregate, input } => {
            used.extend(aggregate.column_refs());
            used.extend(collect_used_columns(input));
        }
        LogicalOp::Sort { sort, input } => {
            used.extend(sort.column_refs());
            used.extend(collect_used_columns(input));
        }
        LogicalOp::Union { left, right, .. } => {
            used.extend(collect_used_columns(left));
            used.extend(collect_used_columns(right));
        }
        LogicalOp::Limit { input, .. } => {
            used.extend(collect_used_columns(input));
        }
        LogicalOp::Rename { input, .. } => {
            used.extend(collect_used_columns(input));
        }
        LogicalOp::Infer { input, .. } => {
            used.extend(collect_used_columns(input));
        }
        LogicalOp::Empty => {}
    }

    used
}

/// Prune unused columns from projections.
fn prune_projections(op: LogicalOp, used: &HashSet<String>) -> (LogicalOp, bool) {
    match op {
        // Remove redundant projections (project followed by project)
        LogicalOp::Project {
            input,
            project: outer_project,
        } => {
            match *input {
                LogicalOp::Project {
                    input: inner_input,
                    project: inner_project,
                } => {
                    // Combine projects: only keep columns needed by outer
                    let outer_refs = outer_project.column_refs();
                    let needed_from_inner: Vec<_> = inner_project
                        .expressions
                        .into_iter()
                        .filter(|e| outer_refs.contains(&e.output_name()))
                        .collect();

                    if needed_from_inner.is_empty() {
                        // Nothing needed from inner, just use outer
                        let (new_input, _) = prune_projections(*inner_input, used);
                        (
                            LogicalOp::Project {
                                input: Box::new(new_input),
                                project: outer_project,
                            },
                            true,
                        )
                    } else {
                        // Merge into single projection
                        let (new_input, child_changed) = prune_projections(*inner_input, used);
                        (
                            LogicalOp::Project {
                                input: Box::new(new_input),
                                project: outer_project,
                            },
                            child_changed || true,
                        )
                    }
                }

                // Project on top of Scan: push projection into scan if possible
                LogicalOp::Scan(mut scan) => {
                    // Add projection columns to scan
                    let needed_cols: Vec<String> = outer_project
                        .expressions
                        .iter()
                        .filter_map(|e| {
                            if let grism_logical::LogicalExpr::Column(name) = e {
                                Some(name.clone())
                            } else {
                                None
                            }
                        })
                        .collect();

                    if !needed_cols.is_empty() && scan.projection.is_empty() {
                        scan.projection = needed_cols;
                        (
                            LogicalOp::Project {
                                input: Box::new(LogicalOp::Scan(scan)),
                                project: outer_project,
                            },
                            true,
                        )
                    } else {
                        (
                            LogicalOp::Project {
                                input: Box::new(LogicalOp::Scan(scan)),
                                project: outer_project,
                            },
                            false,
                        )
                    }
                }

                // For other inputs, just recurse
                other => {
                    let (new_input, changed) = prune_projections(other, used);
                    (
                        LogicalOp::Project {
                            input: Box::new(new_input),
                            project: outer_project,
                        },
                        changed,
                    )
                }
            }
        }

        // Recurse into children for other operators
        LogicalOp::Expand { input, expand } => {
            let (new_input, changed) = prune_projections(*input, used);
            (
                LogicalOp::Expand {
                    input: Box::new(new_input),
                    expand,
                },
                changed,
            )
        }
        LogicalOp::Filter { input, filter } => {
            let (new_input, changed) = prune_projections(*input, used);
            (
                LogicalOp::Filter {
                    input: Box::new(new_input),
                    filter,
                },
                changed,
            )
        }
        LogicalOp::Aggregate { input, aggregate } => {
            let (new_input, changed) = prune_projections(*input, used);
            (
                LogicalOp::Aggregate {
                    input: Box::new(new_input),
                    aggregate,
                },
                changed,
            )
        }
        LogicalOp::Limit { input, limit } => {
            let (new_input, changed) = prune_projections(*input, used);
            (
                LogicalOp::Limit {
                    input: Box::new(new_input),
                    limit,
                },
                changed,
            )
        }
        LogicalOp::Sort { input, sort } => {
            let (new_input, changed) = prune_projections(*input, used);
            (
                LogicalOp::Sort {
                    input: Box::new(new_input),
                    sort,
                },
                changed,
            )
        }
        LogicalOp::Union { left, right, union } => {
            let (new_left, left_changed) = prune_projections(*left, used);
            let (new_right, right_changed) = prune_projections(*right, used);
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
            let (new_input, changed) = prune_projections(*input, used);
            (
                LogicalOp::Rename {
                    input: Box::new(new_input),
                    rename,
                },
                changed,
            )
        }
        LogicalOp::Infer { input, infer } => {
            let (new_input, changed) = prune_projections(*input, used);
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
    use grism_logical::{FilterOp, PlanBuilder, ProjectOp, ScanOp, col, lit};

    #[test]
    fn test_collect_used_columns() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .filter(FilterOp::new(col("age").gt(lit(18i64))))
            .project(ProjectOp::columns(["name", "city"]))
            .build();

        let used = collect_used_columns(plan.root());

        assert!(used.contains("age"));
        assert!(used.contains("name"));
        assert!(used.contains("city"));
    }

    #[test]
    fn test_projection_pruning() {
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .project(ProjectOp::columns(["name", "age", "city"]))
            .project(ProjectOp::columns(["name", "age"]))
            .build();

        let result = ProjectionPruning.apply(plan).unwrap();

        // Should have optimized the double projection
        assert!(result.changed);
    }
}
