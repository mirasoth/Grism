//! Projection pushdown optimization rule.

use common_error::GrismResult;
use grism_logical::LogicalPlan;

use super::OptimizationRule;

/// Projection pushdown optimization.
///
/// Pushes column projections closer to the data source to reduce
/// the number of columns read and processed.
pub struct ProjectionPushdown;

impl OptimizationRule for ProjectionPushdown {
    fn name(&self) -> &'static str {
        "ProjectionPushdown"
    }

    fn apply(&self, plan: LogicalPlan) -> GrismResult<LogicalPlan> {
        // TODO: Implement projection pushdown logic
        // For now, return the plan unchanged
        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grism_logical::{LogicalExpr, LogicalOp, ProjectOp, Projection, ScanOp};

    #[test]
    fn test_projection_pushdown_noop() {
        let scan = LogicalOp::Scan(ScanOp::nodes(Some("Person")));
        let project = LogicalOp::Project(ProjectOp::new(
            scan,
            vec![Projection::new(LogicalExpr::column("name"))],
        ));
        let plan = LogicalPlan::new(project);

        let rule = ProjectionPushdown;
        let result = rule.apply(plan).unwrap();

        // For now, plan should be unchanged
        assert!(matches!(result.root(), LogicalOp::Project(_)));
    }
}
