//! Predicate pushdown optimization rule.

use common_error::GrismResult;
use grism_logical::LogicalPlan;

use super::OptimizationRule;

/// Predicate pushdown optimization.
///
/// Pushes filter predicates closer to the data source to reduce
/// the amount of data processed in later stages.
pub struct PredicatePushdown;

impl OptimizationRule for PredicatePushdown {
    fn name(&self) -> &'static str {
        "PredicatePushdown"
    }

    fn apply(&self, plan: LogicalPlan) -> GrismResult<LogicalPlan> {
        // TODO: Implement predicate pushdown logic
        // For now, return the plan unchanged
        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grism_logical::{FilterOp, LogicalExpr, LogicalOp, ScanKind, ScanOp};

    #[test]
    fn test_predicate_pushdown_noop() {
        let scan = LogicalOp::Scan(ScanOp::nodes(Some("Person")));
        let filter = LogicalOp::Filter(FilterOp::new(
            scan,
            LogicalExpr::column("age").gte(LogicalExpr::literal(18i64)),
        ));
        let plan = LogicalPlan::new(filter);

        let rule = PredicatePushdown;
        let result = rule.apply(plan).unwrap();

        // For now, plan should be unchanged
        assert!(matches!(result.root(), LogicalOp::Filter(_)));
    }
}
