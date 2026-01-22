//! Filter-Expand fusion optimization rule (RFC-0006, Section 6.3).
//!
//! Fuse filter predicates into expand operations to reduce traversal width.

use std::collections::HashSet;

use common_error::GrismResult;
use grism_logical::{FilterOp, LogicalExpr, LogicalOp, LogicalPlan};

use super::rule::{OptimizationRule, Transformed};

/// Filter-Expand fusion rule.
///
/// # Description (RFC-0006, Section 6.3)
///
/// Fuse `Filter` predicates into `Expand` operations to reduce traversal width.
/// This allows filtering during graph traversal rather than after.
///
/// # Legal When
///
/// - Filter is directly above Expand
/// - Predicate applies to target node or edge of the expand
/// - Predicate is deterministic
///
/// # Example
///
/// Before:
/// ```text
/// Filter[friend.age > 18]
///   └─ Expand(KNOWS) AS friend
///        └─ Scan[Person]
/// ```
///
/// After:
/// ```text
/// Expand(KNOWS, target_pred=age > 18) AS friend
///   └─ Scan[Person]
/// ```
pub struct FilterExpandFusion;

impl OptimizationRule for FilterExpandFusion {
    fn name(&self) -> &'static str {
        "FilterExpandFusion"
    }

    fn description(&self) -> &'static str {
        "Fuse filter predicates into expand operations"
    }

    fn apply(&self, plan: LogicalPlan) -> GrismResult<Transformed> {
        let (new_root, changed) = fuse_filter_expand(plan.root);

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

/// Recursively fuse filter predicates into expand operations.
#[allow(clippy::too_many_lines)]
fn fuse_filter_expand(op: LogicalOp) -> (LogicalOp, bool) {
    match op {
        // Filter on top of Expand: try to fuse
        LogicalOp::Filter { input, filter } => {
            match *input {
                LogicalOp::Expand {
                    input: expand_input,
                    mut expand,
                } => {
                    // Analyze the predicate to see if it can be fused
                    let predicate = filter.predicate.clone();

                    // Check if predicate references the target alias
                    let target_alias = expand.target_alias.clone();
                    let edge_alias = expand.edge_alias.clone();

                    let (target_pred, edge_pred, remaining_pred) = partition_predicate(
                        &predicate,
                        target_alias.as_deref(),
                        edge_alias.as_deref(),
                    );

                    #[allow(clippy::useless_let_if_seq)]
                    let mut changed = false;

                    // Fuse target predicate if possible
                    if let Some(tp) = target_pred
                        && filter.is_deterministic()
                    {
                        expand.target_predicate =
                            Some(merge_predicates(expand.target_predicate, tp));
                        changed = true;
                    }

                    // Fuse edge predicate if possible
                    if let Some(ep) = edge_pred
                        && filter.is_deterministic()
                    {
                        expand.edge_predicate = Some(merge_predicates(expand.edge_predicate, ep));
                        changed = true;
                    }

                    // Recurse into expand input
                    let (new_expand_input, expand_input_changed) =
                        fuse_filter_expand(*expand_input);

                    let new_expand = LogicalOp::Expand {
                        input: Box::new(new_expand_input),
                        expand,
                    };

                    // If there's a remaining predicate, keep the filter
                    if let Some(remaining) = remaining_pred {
                        (
                            LogicalOp::Filter {
                                input: Box::new(new_expand),
                                filter: FilterOp::new(remaining),
                            },
                            changed || expand_input_changed,
                        )
                    } else if changed {
                        // All predicates were fused, remove the filter
                        (new_expand, true)
                    } else {
                        // No fusion happened, keep original structure
                        (
                            LogicalOp::Filter {
                                input: Box::new(new_expand),
                                filter,
                            },
                            expand_input_changed,
                        )
                    }
                }

                // Not an Expand below - just recurse
                other => {
                    let (new_input, child_changed) = fuse_filter_expand(other);
                    (
                        LogicalOp::Filter {
                            input: Box::new(new_input),
                            filter,
                        },
                        child_changed,
                    )
                }
            }
        }

        // For other operators, recurse into children
        LogicalOp::Expand { input, expand } => {
            let (new_input, changed) = fuse_filter_expand(*input);
            (
                LogicalOp::Expand {
                    input: Box::new(new_input),
                    expand,
                },
                changed,
            )
        }
        LogicalOp::Project { input, project } => {
            let (new_input, changed) = fuse_filter_expand(*input);
            (
                LogicalOp::Project {
                    input: Box::new(new_input),
                    project,
                },
                changed,
            )
        }
        LogicalOp::Aggregate { input, aggregate } => {
            let (new_input, changed) = fuse_filter_expand(*input);
            (
                LogicalOp::Aggregate {
                    input: Box::new(new_input),
                    aggregate,
                },
                changed,
            )
        }
        LogicalOp::Limit { input, limit } => {
            let (new_input, changed) = fuse_filter_expand(*input);
            (
                LogicalOp::Limit {
                    input: Box::new(new_input),
                    limit,
                },
                changed,
            )
        }
        LogicalOp::Sort { input, sort } => {
            let (new_input, changed) = fuse_filter_expand(*input);
            (
                LogicalOp::Sort {
                    input: Box::new(new_input),
                    sort,
                },
                changed,
            )
        }
        LogicalOp::Union { left, right, union } => {
            let (new_left, left_changed) = fuse_filter_expand(*left);
            let (new_right, right_changed) = fuse_filter_expand(*right);
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
            let (new_input, changed) = fuse_filter_expand(*input);
            (
                LogicalOp::Rename {
                    input: Box::new(new_input),
                    rename,
                },
                changed,
            )
        }
        LogicalOp::Infer { input, infer } => {
            let (new_input, changed) = fuse_filter_expand(*input);
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

/// Partition a predicate into target, edge, and remaining components.
///
/// Returns (`target_predicate`, `edge_predicate`, `remaining_predicate`)
#[allow(clippy::too_many_lines)]
fn partition_predicate(
    predicate: &LogicalExpr,
    target_alias: Option<&str>,
    edge_alias: Option<&str>,
) -> (
    Option<LogicalExpr>,
    Option<LogicalExpr>,
    Option<LogicalExpr>,
) {
    // For AND expressions, we can potentially split them
    if let LogicalExpr::Binary {
        left,
        op: grism_logical::BinaryOp::And,
        right,
    } = predicate
    {
        let (left_target, left_edge, left_remaining) =
            partition_predicate(left, target_alias, edge_alias);
        let (right_target, right_edge, right_remaining) =
            partition_predicate(right, target_alias, edge_alias);

        let target = merge_optional_predicates(left_target, right_target);
        let edge = merge_optional_predicates(left_edge, right_edge);
        let remaining = merge_optional_predicates(left_remaining, right_remaining);

        return (target, edge, remaining);
    }

    // For a single predicate, check what it references
    let refs = predicate.column_refs();

    // Check if all refs are target-qualified
    if let Some(alias) = target_alias
        && refs_all_match_alias(&refs, alias)
    {
        // Rewrite the predicate to remove the alias prefix
        let rewritten = rewrite_predicate_remove_alias(predicate.clone(), alias);
        return (Some(rewritten), None, None);
    }

    // Check if all refs are edge-qualified
    if let Some(alias) = edge_alias
        && refs_all_match_alias(&refs, alias)
    {
        let rewritten = rewrite_predicate_remove_alias(predicate.clone(), alias);
        return (None, Some(rewritten), None);
    }

    // Can't partition - keep as remaining
    (None, None, Some(predicate.clone()))
}

/// Check if all column references match the given alias prefix.
fn refs_all_match_alias(refs: &HashSet<String>, alias: &str) -> bool {
    if refs.is_empty() {
        return false;
    }
    let prefix = format!("{alias}.");
    refs.iter().all(|r| r.starts_with(&prefix))
}

/// Rewrite a predicate to remove the alias prefix from column references.
fn rewrite_predicate_remove_alias(expr: LogicalExpr, alias: &str) -> LogicalExpr {
    let prefix = format!("{alias}.");
    match expr {
        LogicalExpr::Column(name) => name.strip_prefix(&prefix).map_or_else(
            || LogicalExpr::Column(name.clone()),
            |stripped| LogicalExpr::Column(stripped.to_string()),
        ),
        LogicalExpr::QualifiedColumn { qualifier, name } => {
            if qualifier == alias {
                LogicalExpr::Column(name)
            } else {
                LogicalExpr::QualifiedColumn { qualifier, name }
            }
        }
        LogicalExpr::Binary { left, op, right } => LogicalExpr::Binary {
            left: Box::new(rewrite_predicate_remove_alias(*left, alias)),
            op,
            right: Box::new(rewrite_predicate_remove_alias(*right, alias)),
        },
        LogicalExpr::Unary { op, expr } => LogicalExpr::Unary {
            op,
            expr: Box::new(rewrite_predicate_remove_alias(*expr, alias)),
        },
        LogicalExpr::Function(func) => LogicalExpr::Function(grism_logical::FuncExpr {
            func: func.func,
            args: func
                .args
                .into_iter()
                .map(|a| rewrite_predicate_remove_alias(a, alias))
                .collect(),
        }),
        LogicalExpr::Alias { expr, alias: a } => LogicalExpr::Alias {
            expr: Box::new(rewrite_predicate_remove_alias(*expr, alias)),
            alias: a,
        },
        // Other expressions pass through unchanged
        other => other,
    }
}

/// Merge two optional predicates with AND.
fn merge_optional_predicates(
    a: Option<LogicalExpr>,
    b: Option<LogicalExpr>,
) -> Option<LogicalExpr> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.and(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

/// Merge a new predicate with an existing optional predicate.
fn merge_predicates(existing: Option<LogicalExpr>, new: LogicalExpr) -> LogicalExpr {
    match existing {
        Some(e) => e.and(new),
        None => new,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grism_logical::{ExpandOp, PlanBuilder, ScanOp, col, lit};

    #[test]
    fn test_fuse_target_predicate() {
        // Create: Filter(friend.age > 18) -> Expand(KNOWS) AS friend -> Scan
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .expand(
                ExpandOp::binary()
                    .with_edge_label("KNOWS")
                    .with_target_alias("friend"),
            )
            .filter(FilterOp::new(col("friend.age").gt(lit(18i64))))
            .build();

        let result = FilterExpandFusion.apply(plan).unwrap();

        // Filter should be fused into expand
        assert!(result.changed);

        // Root should now be Expand (filter removed)
        if let LogicalOp::Expand { expand, .. } = result.plan.root() {
            assert!(expand.target_predicate.is_some());
        } else {
            panic!("Expected Expand at root after fusion");
        }
    }

    #[test]
    fn test_no_fuse_unrelated_predicate() {
        // Create: Filter(name = 'Alice') -> Expand(KNOWS) AS friend -> Scan
        // The predicate doesn't reference the expand alias, so can't fuse
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .expand(
                ExpandOp::binary()
                    .with_edge_label("KNOWS")
                    .with_target_alias("friend"),
            )
            .filter(FilterOp::new(col("name").eq(lit("Alice"))))
            .build();

        let result = FilterExpandFusion.apply(plan).unwrap();

        // Should not fuse - predicate doesn't reference friend alias
        assert!(!result.changed);

        // Root should still be Filter
        assert!(matches!(result.plan.root(), LogicalOp::Filter { .. }));
    }

    #[test]
    fn test_partial_fuse_and_predicate() {
        // Create: Filter(friend.age > 18 AND name = 'Alice') -> Expand(KNOWS) AS friend -> Scan
        // Only the friend.age part should be fused
        let plan = PlanBuilder::scan(ScanOp::nodes_with_label("Person"))
            .expand(
                ExpandOp::binary()
                    .with_edge_label("KNOWS")
                    .with_target_alias("friend"),
            )
            .filter(FilterOp::new(
                col("friend.age")
                    .gt(lit(18i64))
                    .and(col("name").eq(lit("Alice"))),
            ))
            .build();

        let result = FilterExpandFusion.apply(plan).unwrap();

        // Should fuse the friend.age part
        assert!(result.changed);

        // Root should still be Filter (with remaining predicate)
        if let LogicalOp::Filter { input, filter } = result.plan.root() {
            // The filter should have the remaining predicate
            let remaining_refs = filter.predicate.column_refs();
            assert!(remaining_refs.contains("name"));

            // The expand should have the fused predicate
            if let LogicalOp::Expand { expand, .. } = input.as_ref() {
                assert!(expand.target_predicate.is_some());
            } else {
                panic!("Expected Expand under Filter");
            }
        } else {
            panic!("Expected Filter at root after partial fusion");
        }
    }

    #[test]
    fn test_refs_all_match_alias() {
        let mut refs = HashSet::new();
        refs.insert("friend.age".to_string());
        refs.insert("friend.name".to_string());

        assert!(refs_all_match_alias(&refs, "friend"));
        assert!(!refs_all_match_alias(&refs, "other"));

        // Mixed refs should return false
        refs.insert("other.id".to_string());
        assert!(!refs_all_match_alias(&refs, "friend"));
    }

    #[test]
    fn test_rewrite_predicate_remove_alias() {
        // Column reference
        let expr = col("friend.age");
        let rewritten = rewrite_predicate_remove_alias(expr, "friend");
        assert_eq!(rewritten.output_name(), "age");

        // Binary expression
        let expr = col("friend.age").gt(lit(18i64));
        let rewritten = rewrite_predicate_remove_alias(expr, "friend");
        let refs = rewritten.column_refs();
        assert!(refs.contains("age"));
        assert!(!refs.contains("friend.age"));
    }
}
