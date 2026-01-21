//! Constant folding optimization rule (RFC-0006, Section 6.5).
//!
//! Evaluate constant expressions at plan time.

use common_error::GrismResult;
use grism_core::Value;
use grism_logical::{BinaryOp, FilterOp, LogicalExpr, LogicalOp, LogicalPlan, ProjectOp};

use super::rule::{OptimizationRule, Transformed};

/// Constant folding rule.
///
/// # Description (RFC-0006, Section 6.5)
///
/// Evaluate constant expressions at plan time.
///
/// # Legal When
///
/// - Expression is fully literal
/// - Expression is deterministic
pub struct ConstantFolding;

impl OptimizationRule for ConstantFolding {
    fn name(&self) -> &'static str {
        "ConstantFolding"
    }

    fn description(&self) -> &'static str {
        "Evaluate constant expressions at plan time"
    }

    fn apply(&self, plan: LogicalPlan) -> GrismResult<Transformed> {
        let (new_root, changed) = fold_constants_in_op(plan.root);

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

/// Fold constants in an operator and its children.
fn fold_constants_in_op(op: LogicalOp) -> (LogicalOp, bool) {
    match op {
        LogicalOp::Filter { input, filter } => {
            let (new_input, input_changed) = fold_constants_in_op(*input);
            let (new_predicate, pred_changed) = fold_constants(filter.predicate);

            // Check if the predicate is now a constant true/false
            if let LogicalExpr::Literal(Value::Bool(true)) = &new_predicate {
                // Filter is always true, remove it
                return (new_input, true);
            }

            if let LogicalExpr::Literal(Value::Bool(false)) = &new_predicate {
                // Filter is always false, replace with empty
                return (LogicalOp::Empty, true);
            }

            let changed = input_changed || pred_changed;
            (
                LogicalOp::Filter {
                    input: Box::new(new_input),
                    filter: FilterOp::new(new_predicate),
                },
                changed,
            )
        }

        LogicalOp::Project { input, project } => {
            let (new_input, input_changed) = fold_constants_in_op(*input);

            // Fold constants in projection expressions
            let mut exprs_changed = false;
            let new_expressions: Vec<_> = project
                .expressions
                .into_iter()
                .map(|e| {
                    let (folded, changed) = fold_constants(e);
                    exprs_changed = exprs_changed || changed;
                    folded
                })
                .collect();

            let changed = input_changed || exprs_changed;
            (
                LogicalOp::Project {
                    input: Box::new(new_input),
                    project: ProjectOp::new(new_expressions),
                },
                changed,
            )
        }

        LogicalOp::Expand { input, expand } => {
            let (new_input, changed) = fold_constants_in_op(*input);
            (
                LogicalOp::Expand {
                    input: Box::new(new_input),
                    expand,
                },
                changed,
            )
        }

        LogicalOp::Aggregate { input, aggregate } => {
            let (new_input, changed) = fold_constants_in_op(*input);
            (
                LogicalOp::Aggregate {
                    input: Box::new(new_input),
                    aggregate,
                },
                changed,
            )
        }

        LogicalOp::Limit { input, limit } => {
            let (new_input, changed) = fold_constants_in_op(*input);
            (
                LogicalOp::Limit {
                    input: Box::new(new_input),
                    limit,
                },
                changed,
            )
        }

        LogicalOp::Sort { input, sort } => {
            let (new_input, changed) = fold_constants_in_op(*input);
            (
                LogicalOp::Sort {
                    input: Box::new(new_input),
                    sort,
                },
                changed,
            )
        }

        LogicalOp::Union { left, right, union } => {
            let (new_left, left_changed) = fold_constants_in_op(*left);
            let (new_right, right_changed) = fold_constants_in_op(*right);
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
            let (new_input, changed) = fold_constants_in_op(*input);
            (
                LogicalOp::Rename {
                    input: Box::new(new_input),
                    rename,
                },
                changed,
            )
        }

        LogicalOp::Infer { input, infer } => {
            let (new_input, changed) = fold_constants_in_op(*input);
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

/// Fold constants in an expression.
fn fold_constants(expr: LogicalExpr) -> (LogicalExpr, bool) {
    match expr {
        // Already a literal - nothing to fold
        LogicalExpr::Literal(_) => (expr, false),

        // Binary expression with two literals - evaluate
        LogicalExpr::Binary { left, op, right } => {
            let (folded_left, left_changed) = fold_constants(*left);
            let (folded_right, right_changed) = fold_constants(*right);

            // Try to evaluate if both sides are literals
            if let (LogicalExpr::Literal(l), LogicalExpr::Literal(r)) = (&folded_left, &folded_right)
            {
                if let Some(result) = evaluate_binary(l, op, r) {
                    return (LogicalExpr::Literal(result), true);
                }
            }

            // Special case: x AND true = x, x AND false = false
            if op == BinaryOp::And {
                if matches!(&folded_right, LogicalExpr::Literal(Value::Bool(true))) {
                    return (folded_left, true);
                }
                if matches!(&folded_left, LogicalExpr::Literal(Value::Bool(true))) {
                    return (folded_right, true);
                }
                if matches!(&folded_right, LogicalExpr::Literal(Value::Bool(false)))
                    || matches!(&folded_left, LogicalExpr::Literal(Value::Bool(false)))
                {
                    return (LogicalExpr::Literal(Value::Bool(false)), true);
                }
            }

            // Special case: x OR true = true, x OR false = x
            if op == BinaryOp::Or {
                if matches!(&folded_right, LogicalExpr::Literal(Value::Bool(true)))
                    || matches!(&folded_left, LogicalExpr::Literal(Value::Bool(true)))
                {
                    return (LogicalExpr::Literal(Value::Bool(true)), true);
                }
                if matches!(&folded_right, LogicalExpr::Literal(Value::Bool(false))) {
                    return (folded_left, true);
                }
                if matches!(&folded_left, LogicalExpr::Literal(Value::Bool(false))) {
                    return (folded_right, true);
                }
            }

            let changed = left_changed || right_changed;
            (
                LogicalExpr::Binary {
                    left: Box::new(folded_left),
                    op,
                    right: Box::new(folded_right),
                },
                changed,
            )
        }

        // Unary expression with literal - evaluate
        LogicalExpr::Unary { op, expr } => {
            let (folded, child_changed) = fold_constants(*expr);

            if let LogicalExpr::Literal(v) = &folded {
                if let Some(result) = evaluate_unary(op, v) {
                    return (LogicalExpr::Literal(result), true);
                }
            }

            (
                LogicalExpr::Unary {
                    op,
                    expr: Box::new(folded),
                },
                child_changed,
            )
        }

        // Function calls - check for constant arguments
        LogicalExpr::Function(func) => {
            let mut changed = false;
            let new_args: Vec<_> = func
                .args
                .into_iter()
                .map(|a| {
                    let (folded, arg_changed) = fold_constants(a);
                    changed = changed || arg_changed;
                    folded
                })
                .collect();

            (
                LogicalExpr::Function(grism_logical::FuncExpr {
                    func: func.func,
                    args: new_args,
                }),
                changed,
            )
        }

        // Alias - fold the inner expression
        LogicalExpr::Alias { expr, alias } => {
            let (folded, changed) = fold_constants(*expr);
            (
                LogicalExpr::Alias {
                    expr: Box::new(folded),
                    alias,
                },
                changed,
            )
        }

        // Case expression - fold all branches
        LogicalExpr::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            let mut changed = false;

            let new_operand = operand.map(|op| {
                let (folded, c) = fold_constants(*op);
                changed = changed || c;
                Box::new(folded)
            });

            let new_when: Vec<_> = when_clauses
                .into_iter()
                .map(|(cond, result)| {
                    let (folded_cond, c1) = fold_constants(cond);
                    let (folded_result, c2) = fold_constants(result);
                    changed = changed || c1 || c2;
                    (folded_cond, folded_result)
                })
                .collect();

            let new_else = else_result.map(|e| {
                let (folded, c) = fold_constants(*e);
                changed = changed || c;
                Box::new(folded)
            });

            (
                LogicalExpr::Case {
                    operand: new_operand,
                    when_clauses: new_when,
                    else_result: new_else,
                },
                changed,
            )
        }

        // Other expressions - no folding for now
        _ => (expr, false),
    }
}

/// Evaluate a binary operation on two literal values.
fn evaluate_binary(left: &Value, op: BinaryOp, right: &Value) -> Option<Value> {
    match (left, right, op) {
        // Integer arithmetic
        (Value::Int64(l), Value::Int64(r), BinaryOp::Add) => Some(Value::Int64(l + r)),
        (Value::Int64(l), Value::Int64(r), BinaryOp::Subtract) => Some(Value::Int64(l - r)),
        (Value::Int64(l), Value::Int64(r), BinaryOp::Multiply) => Some(Value::Int64(l * r)),
        (Value::Int64(l), Value::Int64(r), BinaryOp::Divide) if *r != 0 => {
            Some(Value::Int64(l / r))
        }
        (Value::Int64(l), Value::Int64(r), BinaryOp::Modulo) if *r != 0 => {
            Some(Value::Int64(l % r))
        }

        // Float arithmetic
        (Value::Float64(l), Value::Float64(r), BinaryOp::Add) => Some(Value::Float64(l + r)),
        (Value::Float64(l), Value::Float64(r), BinaryOp::Subtract) => Some(Value::Float64(l - r)),
        (Value::Float64(l), Value::Float64(r), BinaryOp::Multiply) => Some(Value::Float64(l * r)),
        (Value::Float64(l), Value::Float64(r), BinaryOp::Divide) if *r != 0.0 => {
            Some(Value::Float64(l / r))
        }

        // Integer comparisons
        (Value::Int64(l), Value::Int64(r), BinaryOp::Eq) => Some(Value::Bool(l == r)),
        (Value::Int64(l), Value::Int64(r), BinaryOp::NotEq) => Some(Value::Bool(l != r)),
        (Value::Int64(l), Value::Int64(r), BinaryOp::Lt) => Some(Value::Bool(l < r)),
        (Value::Int64(l), Value::Int64(r), BinaryOp::LtEq) => Some(Value::Bool(l <= r)),
        (Value::Int64(l), Value::Int64(r), BinaryOp::Gt) => Some(Value::Bool(l > r)),
        (Value::Int64(l), Value::Int64(r), BinaryOp::GtEq) => Some(Value::Bool(l >= r)),

        // Boolean logic
        (Value::Bool(l), Value::Bool(r), BinaryOp::And) => Some(Value::Bool(*l && *r)),
        (Value::Bool(l), Value::Bool(r), BinaryOp::Or) => Some(Value::Bool(*l || *r)),

        // String comparisons
        (Value::String(l), Value::String(r), BinaryOp::Eq) => Some(Value::Bool(l == r)),
        (Value::String(l), Value::String(r), BinaryOp::NotEq) => Some(Value::Bool(l != r)),

        // String concatenation
        (Value::String(l), Value::String(r), BinaryOp::Concat) => {
            Some(Value::String(format!("{}{}", l, r)))
        }

        _ => None,
    }
}

/// Evaluate a unary operation on a literal value.
fn evaluate_unary(op: grism_logical::UnaryOp, value: &Value) -> Option<Value> {
    match (op, value) {
        (grism_logical::UnaryOp::Not, Value::Bool(b)) => Some(Value::Bool(!b)),
        (grism_logical::UnaryOp::Neg, Value::Int64(i)) => Some(Value::Int64(-i)),
        (grism_logical::UnaryOp::Neg, Value::Float64(f)) => Some(Value::Float64(-f)),
        (grism_logical::UnaryOp::IsNull, Value::Null) => Some(Value::Bool(true)),
        (grism_logical::UnaryOp::IsNull, _) => Some(Value::Bool(false)),
        (grism_logical::UnaryOp::IsNotNull, Value::Null) => Some(Value::Bool(false)),
        (grism_logical::UnaryOp::IsNotNull, _) => Some(Value::Bool(true)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grism_logical::{col, lit, PlanBuilder, ScanOp};

    #[test]
    fn test_fold_arithmetic() {
        let expr = lit(2i64).add(lit(3i64));
        let (folded, changed) = fold_constants(expr);

        assert!(changed);
        assert_eq!(folded, LogicalExpr::Literal(Value::Int64(5)));
    }

    #[test]
    fn test_fold_comparison() {
        let expr = lit(5i64).gt(lit(3i64));
        let (folded, changed) = fold_constants(expr);

        assert!(changed);
        assert_eq!(folded, LogicalExpr::Literal(Value::Bool(true)));
    }

    #[test]
    fn test_fold_boolean_simplification() {
        // x AND true = x
        let expr = col("x").and(lit(true));
        let (folded, changed) = fold_constants(expr);

        assert!(changed);
        assert_eq!(folded, LogicalExpr::Column("x".to_string()));
    }

    #[test]
    fn test_fold_in_filter() {
        let plan = PlanBuilder::scan(ScanOp::nodes())
            .filter(FilterOp::new(lit(1i64).lt(lit(2i64))))
            .build();

        let result = ConstantFolding.apply(plan).unwrap();

        // Filter should be removed (constant true)
        assert!(result.changed);
        assert!(matches!(result.plan.root(), LogicalOp::Scan(_)));
    }

    #[test]
    fn test_fold_false_filter() {
        let plan = PlanBuilder::scan(ScanOp::nodes())
            .filter(FilterOp::new(lit(false)))
            .build();

        let result = ConstantFolding.apply(plan).unwrap();

        // Should become Empty
        assert!(result.changed);
        assert!(matches!(result.plan.root(), LogicalOp::Empty));
    }
}
