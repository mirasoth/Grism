//! Local physical planner implementation.
//!
//! Converts logical plans to physical plans for local execution.

use std::sync::Arc;

use common_error::{GrismError, GrismResult};
use grism_logical::LogicalPlan;
use grism_logical::ops::{ExpandMode, ExpandOp, LogicalOp, ScanKind, ScanOp};

use crate::operators::{
    AdjacencyExpandExec, EmptyExec, FilterExec, HashAggregateExec, HyperedgeScanExec, LimitExec,
    NodeScanExec, PhysicalOperator, ProjectExec, RenameExec, RoleExpandExec, SortExec, UnionExec,
};
use crate::physical::{PhysicalPlan, PlanProperties};

use super::schema_inference::{build_aggregate_schema, build_project_schema};

/// Physical planner trait.
///
/// Responsible for converting logical plans to physical plans.
pub trait PhysicalPlanner: Send + Sync {
    /// Plan a logical plan into a physical plan.
    fn plan(&self, logical: &LogicalPlan) -> GrismResult<PhysicalPlan>;
}

/// Local physical planner for single-node execution.
///
/// Converts logical operators to their local physical counterparts.
#[derive(Debug, Default)]
pub struct LocalPhysicalPlanner {
    /// Configuration options.
    config: PlannerConfig,
}

/// Configuration for the local planner.
#[derive(Debug, Clone, Default)]
pub struct PlannerConfig {
    /// Default batch size for operators.
    pub batch_size: Option<usize>,
    /// Whether to enable predicate pushdown during planning.
    pub enable_predicate_pushdown: bool,
    /// Whether to enable projection pushdown during planning.
    pub enable_projection_pushdown: bool,
}

impl LocalPhysicalPlanner {
    /// Create a new local physical planner.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with configuration.
    pub fn with_config(config: PlannerConfig) -> Self {
        Self { config }
    }

    /// Get the planner configuration.
    pub fn config(&self) -> &PlannerConfig {
        &self.config
    }

    /// Plan a logical operator into a physical operator.
    fn plan_operator(&self, op: &LogicalOp) -> GrismResult<Arc<dyn PhysicalOperator>> {
        match op {
            LogicalOp::Scan(scan) => self.plan_scan(scan),

            LogicalOp::Filter { input, filter } => {
                let input_op = self.plan_operator(input)?;
                Ok(Arc::new(FilterExec::new(
                    input_op,
                    filter.predicate.clone(),
                )))
            }

            LogicalOp::Project { input, project } => {
                let input_op = self.plan_operator(input)?;
                // Build projections from expressions with their output names
                let projections: Vec<_> = project
                    .expressions
                    .iter()
                    .map(|expr| (expr.clone(), expr.output_name()))
                    .collect();
                // Use schema inference to properly type computed expressions
                let schema = build_project_schema(&input_op.schema(), &projections);
                Ok(Arc::new(ProjectExec::new(input_op, projections, schema)))
            }

            LogicalOp::Expand { input, expand } => {
                let input_op = self.plan_operator(input)?;
                self.plan_expand(input_op, expand)
            }

            LogicalOp::Aggregate { input, aggregate } => {
                let input_op = self.plan_operator(input)?;
                // Use schema inference to properly type aggregation results
                let schema = build_aggregate_schema(&input_op.schema(), aggregate);
                Ok(Arc::new(HashAggregateExec::new(
                    input_op,
                    aggregate.group_keys.clone(),
                    aggregate.aggregates.clone(),
                    schema,
                )))
            }

            LogicalOp::Limit { input, limit } => {
                let input_op = self.plan_operator(input)?;
                let mut exec = LimitExec::new(input_op, limit.limit);
                if limit.offset > 0 {
                    exec = exec.with_offset(limit.offset);
                }
                Ok(Arc::new(exec))
            }

            LogicalOp::Sort { input, sort } => {
                let input_op = self.plan_operator(input)?;
                Ok(Arc::new(SortExec::new(input_op, sort.keys.clone())))
            }

            LogicalOp::Union { left, right, union } => {
                let left_op = self.plan_operator(left)?;
                let right_op = self.plan_operator(right)?;
                // UnionExec takes `all: bool`, which is the opposite of `distinct`
                Ok(Arc::new(UnionExec::new(left_op, right_op, !union.distinct)))
            }

            LogicalOp::Rename { input, rename } => {
                let input_op = self.plan_operator(input)?;
                Ok(Arc::new(RenameExec::new(input_op, rename.mapping.clone())))
            }

            LogicalOp::Infer { .. } => Err(GrismError::not_implemented(
                "Infer operator physical planning",
            )),

            LogicalOp::Empty => Ok(Arc::new(EmptyExec::new())),
        }
    }

    /// Plan a scan operator.
    fn plan_scan(&self, scan: &ScanOp) -> GrismResult<Arc<dyn PhysicalOperator>> {
        match scan.kind {
            ScanKind::Node => {
                let mut exec = NodeScanExec::new(scan.label.clone(), scan.alias.clone());
                if let Some(ref alias) = scan.alias {
                    exec = exec.with_alias(alias);
                }
                Ok(Arc::new(exec))
            }
            ScanKind::Hyperedge => {
                let mut exec = HyperedgeScanExec::new(scan.label.clone(), scan.alias.clone());
                if let Some(ref alias) = scan.alias {
                    exec = exec.with_alias(alias);
                }
                Ok(Arc::new(exec))
            }
            ScanKind::Edge => {
                // Edge scan is a projection of hyperedges with arity=2
                // For now, treat as hyperedge scan
                let exec = HyperedgeScanExec::new(scan.label.clone(), scan.alias.clone());
                Ok(Arc::new(exec))
            }
        }
    }

    /// Plan an expand operator.
    fn plan_expand(
        &self,
        input: Arc<dyn PhysicalOperator>,
        expand: &ExpandOp,
    ) -> GrismResult<Arc<dyn PhysicalOperator>> {
        match expand.mode {
            ExpandMode::Binary | ExpandMode::MaterializeHyperedge => {
                // Binary/adjacency expansion for arity=2 hyperedges
                let mut exec = AdjacencyExpandExec::new(input, expand.direction);
                if let Some(ref label) = expand.edge_label {
                    exec = exec.with_edge_label(label);
                }
                if let Some(ref label) = expand.to_label {
                    exec = exec.with_to_label(label);
                }
                if let Some(ref alias) = expand.target_alias {
                    exec = exec.with_target_alias(alias);
                }
                Ok(Arc::new(exec))
            }
            ExpandMode::Role => {
                // Role-based expansion for n-ary hyperedges
                let from_role = expand.from_role.clone().unwrap_or_default();
                let to_role = expand.to_role.clone().unwrap_or_default();
                let mut exec = RoleExpandExec::new(input, &from_role, &to_role);
                if let Some(ref label) = expand.edge_label {
                    exec = exec.with_edge_label(label);
                }
                if expand.mode == ExpandMode::MaterializeHyperedge {
                    exec = exec.with_materialize();
                }
                Ok(Arc::new(exec))
            }
        }
    }
}

impl PhysicalPlanner for LocalPhysicalPlanner {
    fn plan(&self, logical: &LogicalPlan) -> GrismResult<PhysicalPlan> {
        let root = self.plan_operator(logical.root())?;
        let properties = PlanProperties::local();

        if root.capabilities().blocking {
            Ok(PhysicalPlan::with_properties(
                root,
                properties.with_blocking(),
            ))
        } else {
            Ok(PhysicalPlan::with_properties(root, properties))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grism_logical::expr::{col, lit};
    use grism_logical::ops::{Direction, FilterOp, LimitOp};

    #[test]
    fn test_plan_scan() {
        let planner = LocalPhysicalPlanner::new();

        let scan = ScanOp::nodes_with_label("Person").with_alias("p");
        let logical = LogicalPlan::new(LogicalOp::scan(scan));

        let physical = planner.plan(&logical).unwrap();
        assert_eq!(physical.root().name(), "NodeScanExec");
    }

    #[test]
    fn test_plan_filter() {
        let planner = LocalPhysicalPlanner::new();

        let logical = LogicalPlan::new(LogicalOp::filter(
            LogicalOp::scan(ScanOp::nodes_with_label("Person")),
            FilterOp::new(col("age").gt(lit(18i64))),
        ));

        let physical = planner.plan(&logical).unwrap();
        assert_eq!(physical.root().name(), "FilterExec");
    }

    #[test]
    fn test_plan_limit() {
        let planner = LocalPhysicalPlanner::new();

        let logical = LogicalPlan::new(LogicalOp::limit(
            LogicalOp::scan(ScanOp::nodes()),
            LimitOp::new(10),
        ));

        let physical = planner.plan(&logical).unwrap();
        assert_eq!(physical.root().name(), "LimitExec");
    }

    #[test]
    fn test_plan_empty() {
        let planner = LocalPhysicalPlanner::new();

        let logical = LogicalPlan::new(LogicalOp::Empty);

        let physical = planner.plan(&logical).unwrap();
        assert_eq!(physical.root().name(), "EmptyExec");
    }

    #[test]
    fn test_plan_expand() {
        let planner = LocalPhysicalPlanner::new();

        let expand = ExpandOp::binary()
            .with_direction(Direction::Outgoing)
            .with_edge_label("KNOWS")
            .with_target_alias("friend");

        let logical = LogicalPlan::new(LogicalOp::expand(
            LogicalOp::scan(ScanOp::nodes_with_label("Person")),
            expand,
        ));

        let physical = planner.plan(&logical).unwrap();
        assert_eq!(physical.root().name(), "AdjacencyExpandExec");
    }
}
