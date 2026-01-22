//! Expand reordering optimization rule (RFC-0006, Section 6.4).
//!
//! Reorder independent expand operations for optimal execution.

use common_error::GrismResult;
use grism_logical::{ExpandOp, LogicalOp, LogicalPlan};

use super::rule::{OptimizationRule, Transformed};

/// Expand reordering rule.
///
/// # Description (RFC-0006, Section 6.4)
///
/// Reorder independent `Expand` operators for optimal traversal.
/// Two expands are independent if they don't share column dependencies.
///
/// # Legal When
///
/// - Expands are consecutive (no operators between them)
/// - Expands are independent (disjoint introduced columns)
/// - Predicates don't reference each other's introduced columns
///
/// # Selectivity Heuristics
///
/// Prefer to execute first:
/// 1. Expands with more restrictive edge labels
/// 2. Expands with target label filters
/// 3. Expands with inline predicates
///
/// # Example
///
/// Before:
/// ```text
/// Expand(AUTHORED_BY) AS author
///   └─ Expand(KNOWS) AS friend
///        └─ Scan[Person]
/// ```
///
/// After (if KNOWS is more selective):
/// ```text
/// Expand(KNOWS) AS friend
///   └─ Expand(AUTHORED_BY) AS author
///        └─ Scan[Person]
/// ```
pub struct ExpandReordering;

impl OptimizationRule for ExpandReordering {
    fn name(&self) -> &'static str {
        "ExpandReordering"
    }

    fn description(&self) -> &'static str {
        "Reorder independent expand operations for optimal traversal"
    }

    fn apply(&self, plan: LogicalPlan) -> GrismResult<Transformed> {
        let (new_root, changed) = reorder_expands(plan.root);

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

/// Recursively reorder expand operations in the plan tree.
#[allow(clippy::too_many_lines)]
fn reorder_expands(op: LogicalOp) -> (LogicalOp, bool) {
    match op {
        // Expand on top of Expand: check if we can reorder
        LogicalOp::Expand {
            input,
            expand: outer_expand,
        } => {
            match *input {
                LogicalOp::Expand {
                    input: inner_input,
                    expand: inner_expand,
                } => {
                    // Check if the expands are independent
                    if are_expands_independent(&outer_expand, &inner_expand) {
                        // Compare selectivity to decide order
                        let outer_score = selectivity_score(&outer_expand);
                        let inner_score = selectivity_score(&inner_expand);

                        if inner_score > outer_score {
                            // Inner is more selective, swap them so inner becomes outer
                            // Before: outer(inner(input))
                            // After: inner(outer(input))
                            // This moves the more selective expand up in the tree
                            let (processed_input, _) = reorder_expands(*inner_input);

                            let new_plan = LogicalOp::Expand {
                                input: Box::new(LogicalOp::Expand {
                                    input: Box::new(processed_input),
                                    expand: outer_expand,
                                }),
                                expand: inner_expand,
                            };

                            return (new_plan, true);
                        }
                    }

                    // Can't reorder or no benefit - just recurse
                    let (new_inner, inner_changed) = reorder_expands(LogicalOp::Expand {
                        input: inner_input,
                        expand: inner_expand,
                    });

                    (
                        LogicalOp::Expand {
                            input: Box::new(new_inner),
                            expand: outer_expand,
                        },
                        inner_changed,
                    )
                }

                // Not an Expand below - just recurse
                other => {
                    let (new_input, changed) = reorder_expands(other);
                    (
                        LogicalOp::Expand {
                            input: Box::new(new_input),
                            expand: outer_expand,
                        },
                        changed,
                    )
                }
            }
        }

        // For other operators, recurse into children
        LogicalOp::Filter { input, filter } => {
            let (new_input, changed) = reorder_expands(*input);
            (
                LogicalOp::Filter {
                    input: Box::new(new_input),
                    filter,
                },
                changed,
            )
        }
        LogicalOp::Project { input, project } => {
            let (new_input, changed) = reorder_expands(*input);
            (
                LogicalOp::Project {
                    input: Box::new(new_input),
                    project,
                },
                changed,
            )
        }
        LogicalOp::Aggregate { input, aggregate } => {
            let (new_input, changed) = reorder_expands(*input);
            (
                LogicalOp::Aggregate {
                    input: Box::new(new_input),
                    aggregate,
                },
                changed,
            )
        }
        LogicalOp::Limit { input, limit } => {
            let (new_input, changed) = reorder_expands(*input);
            (
                LogicalOp::Limit {
                    input: Box::new(new_input),
                    limit,
                },
                changed,
            )
        }
        LogicalOp::Sort { input, sort } => {
            let (new_input, changed) = reorder_expands(*input);
            (
                LogicalOp::Sort {
                    input: Box::new(new_input),
                    sort,
                },
                changed,
            )
        }
        LogicalOp::Union { left, right, union } => {
            let (new_left, left_changed) = reorder_expands(*left);
            let (new_right, right_changed) = reorder_expands(*right);
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
            let (new_input, changed) = reorder_expands(*input);
            (
                LogicalOp::Rename {
                    input: Box::new(new_input),
                    rename,
                },
                changed,
            )
        }
        LogicalOp::Infer { input, infer } => {
            let (new_input, changed) = reorder_expands(*input);
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

/// Check if two expands are independent and can be safely reordered.
///
/// Two expands are independent if:
/// 1. They don't have conflicting aliases
/// 2. The outer expand's predicates don't reference the inner expand's introduced columns
fn are_expands_independent(outer: &ExpandOp, inner: &ExpandOp) -> bool {
    // Check for alias conflicts
    if let (Some(outer_target), Some(inner_target)) = (&outer.target_alias, &inner.target_alias)
        && outer_target == inner_target
    {
        return false;
    }

    if let (Some(outer_edge), Some(inner_edge)) = (&outer.edge_alias, &inner.edge_alias)
        && outer_edge == inner_edge
    {
        return false;
    }

    // Check if outer's predicates reference inner's introduced columns
    if let Some(ref pred) = outer.edge_predicate {
        let refs = pred.column_refs();
        if let Some(ref alias) = inner.target_alias
            && refs.iter().any(|r| r.starts_with(alias))
        {
            return false;
        }
        if let Some(ref alias) = inner.edge_alias
            && refs.iter().any(|r| r.starts_with(alias))
        {
            return false;
        }
    }

    if let Some(ref pred) = outer.target_predicate {
        let refs = pred.column_refs();
        if let Some(ref alias) = inner.target_alias
            && refs.iter().any(|r| r.starts_with(alias))
        {
            return false;
        }
        if let Some(ref alias) = inner.edge_alias
            && refs.iter().any(|r| r.starts_with(alias))
        {
            return false;
        }
    }

    true
}

/// Calculate a selectivity score for an expand operation.
///
/// Higher score = more selective = should be executed first.
///
/// Scoring:
/// - Has edge label: +10
/// - Has target label: +10
/// - Has edge predicate: +20
/// - Has target predicate: +20
/// - Single hop (vs variable): +5
const fn selectivity_score(expand: &ExpandOp) -> i32 {
    let mut score = 0;

    if expand.edge_label.is_some() {
        score += 10;
    }

    if expand.to_label.is_some() {
        score += 10;
    }

    if expand.edge_predicate.is_some() {
        score += 20;
    }

    if expand.target_predicate.is_some() {
        score += 20;
    }

    if expand.hops.is_single() {
        score += 5;
    }

    score
}

#[cfg(test)]
mod tests {
    use super::*;
    use grism_logical::{PlanBuilder, ScanOp, col, lit};

    #[test]
    fn test_independent_expands() {
        let expand1 = ExpandOp::binary()
            .with_edge_label("KNOWS")
            .with_target_alias("friend");

        let expand2 = ExpandOp::binary()
            .with_edge_label("WORKS_AT")
            .with_target_alias("company");

        assert!(are_expands_independent(&expand1, &expand2));
    }

    #[test]
    fn test_dependent_expands_same_alias() {
        let expand1 = ExpandOp::binary()
            .with_edge_label("KNOWS")
            .with_target_alias("target");

        let expand2 = ExpandOp::binary()
            .with_edge_label("WORKS_AT")
            .with_target_alias("target");

        assert!(!are_expands_independent(&expand1, &expand2));
    }

    #[test]
    fn test_selectivity_scoring() {
        // No filters - low score
        let expand1 = ExpandOp::binary();
        assert_eq!(selectivity_score(&expand1), 5); // Just single hop

        // With edge label
        let expand2 = ExpandOp::binary().with_edge_label("KNOWS");
        assert_eq!(selectivity_score(&expand2), 15); // edge_label + single hop

        // With edge label and target label
        let expand3 = ExpandOp::binary()
            .with_edge_label("KNOWS")
            .with_to_label("Person");
        assert_eq!(selectivity_score(&expand3), 25);

        // With predicate - highest
        let expand4 = ExpandOp::binary()
            .with_edge_label("KNOWS")
            .with_target_predicate(col("age").gt(lit(18i64)));
        assert_eq!(selectivity_score(&expand4), 35);
    }

    #[test]
    fn test_reorder_expands() {
        // Create: Expand(no_filter) -> Expand(with_filter) -> Scan
        // The more selective expand should move to the top
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .expand(
                ExpandOp::binary()
                    .with_edge_label("KNOWS")
                    .with_to_label("Person")
                    .with_target_alias("friend"),
            )
            .expand(ExpandOp::binary().with_target_alias("other"))
            .build();

        let result = ExpandReordering.apply(plan).unwrap();

        // The more selective expand (with KNOWS label and to_label) should now be on top
        assert!(result.changed);

        // Verify the structure: the top expand should be the one with KNOWS
        if let LogicalOp::Expand { expand, .. } = result.plan.root() {
            assert_eq!(expand.edge_label, Some("KNOWS".to_string()));
        } else {
            panic!("Expected Expand at root");
        }
    }

    #[test]
    fn test_no_reorder_when_optimal() {
        // Create: Expand(with_filter) -> Expand(no_filter) -> Scan
        // Already optimal, should not reorder
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .expand(ExpandOp::binary().with_target_alias("other"))
            .expand(
                ExpandOp::binary()
                    .with_edge_label("KNOWS")
                    .with_to_label("Person")
                    .with_target_alias("friend"),
            )
            .build();

        let result = ExpandReordering.apply(plan).unwrap();

        // Should not change - already optimal
        assert!(!result.changed);
    }

    #[test]
    fn test_no_reorder_dependent_expands() {
        // Create expands with same alias - should not reorder
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .expand(
                ExpandOp::binary()
                    .with_edge_label("KNOWS")
                    .with_target_alias("target"),
            )
            .expand(ExpandOp::binary().with_target_alias("target"))
            .build();

        let result = ExpandReordering.apply(plan).unwrap();

        // Should not change due to alias conflict
        assert!(!result.changed);
    }
}
