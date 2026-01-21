//! Ray physical planner for distributed execution.

mod stage;

pub use stage::{ShuffleStrategy, Stage, StageId};

use serde::{Deserialize, Serialize};

use common_error::{GrismError, GrismResult};
use grism_logical::{LogicalOp, LogicalPlan};

/// Ray physical planner.
///
/// Converts logical plans into distributed execution stages (Ray DAGs).
pub struct RayPlanner {
    /// Configuration for the planner.
    config: PlannerConfig,
}

/// Planner configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannerConfig {
    /// Target number of partitions.
    pub num_partitions: usize,
    /// Maximum stage size (number of operators).
    pub max_stage_size: usize,
    /// Enable stage fusion optimization.
    pub enable_fusion: bool,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            num_partitions: 4,
            max_stage_size: 10,
            enable_fusion: true,
        }
    }
}

impl RayPlanner {
    /// Create a new Ray planner.
    pub fn new() -> Self {
        Self {
            config: PlannerConfig::default(),
        }
    }

    /// Create with custom configuration.
    pub fn with_config(config: PlannerConfig) -> Self {
        Self { config }
    }

    /// Plan a logical plan into distributed stages.
    pub fn plan(&self, logical_plan: &LogicalPlan) -> GrismResult<Vec<Stage>> {
        let mut stages = Vec::new();
        self.plan_recursive(logical_plan.root(), &mut stages, 0)?;
        Ok(stages)
    }

    fn plan_recursive(
        &self,
        op: &LogicalOp,
        stages: &mut Vec<Stage>,
        current_stage_id: StageId,
    ) -> GrismResult<StageId> {
        match op {
            LogicalOp::Scan(_scan) => {
                // Scan creates a new parallel stage
                let stage = Stage::new(current_stage_id)
                    .with_partitions(self.config.num_partitions)
                    .with_operator(op.clone());
                stages.push(stage);
                Ok(current_stage_id)
            }

            LogicalOp::Filter { input, filter: _ } => {
                // Filter can be fused with input stage
                let input_stage = self.plan_recursive(input, stages, current_stage_id)?;

                if let Some(stage) = stages.iter_mut().find(|s| s.id == input_stage) {
                    stage.add_operator(op.clone());
                }
                Ok(input_stage)
            }

            LogicalOp::Expand { .. } => {
                // Expand may require shuffle
                Err(GrismError::not_implemented("Distributed expand planning"))
            }

            LogicalOp::Project { input, project: _ } => {
                // Project can be fused with input stage
                let input_stage = self.plan_recursive(input, stages, current_stage_id)?;

                if let Some(stage) = stages.iter_mut().find(|s| s.id == input_stage) {
                    stage.add_operator(op.clone());
                }
                Ok(input_stage)
            }

            LogicalOp::Aggregate { .. } => {
                // Aggregate typically requires shuffle
                Err(GrismError::not_implemented(
                    "Distributed aggregate planning",
                ))
            }

            LogicalOp::Limit { input, limit: _ } => {
                // Limit can be partially pushed down
                let input_stage = self.plan_recursive(input, stages, current_stage_id)?;

                // Create a new stage for final limit
                let final_stage = Stage::new(current_stage_id + 1)
                    .with_partitions(1) // Single partition for final limit
                    .with_operator(op.clone())
                    .with_dependency(input_stage);
                stages.push(final_stage);

                Ok(current_stage_id + 1)
            }

            LogicalOp::Sort { .. } => Err(GrismError::not_implemented("Distributed sort planning")),
            LogicalOp::Union { .. } => {
                Err(GrismError::not_implemented("Distributed union planning"))
            }
            LogicalOp::Rename { .. } => {
                Err(GrismError::not_implemented("Distributed rename planning"))
            }
            LogicalOp::Infer { .. } => {
                Err(GrismError::not_implemented("Distributed infer planning"))
            }
            LogicalOp::Empty => Err(GrismError::not_implemented("Distributed empty planning")),
        }
    }

    /// Get the planner configuration.
    pub fn config(&self) -> &PlannerConfig {
        &self.config
    }
}

impl Default for RayPlanner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grism_logical::{FilterOp, ScanOp, col, lit};

    #[test]
    fn test_plan_simple_scan() {
        let planner = RayPlanner::new();
        let scan = LogicalOp::Scan(ScanOp::nodes_with_label("Person"));
        let plan = LogicalPlan::new(scan);

        let stages = planner.plan(&plan).unwrap();
        assert_eq!(stages.len(), 1);
        assert_eq!(stages[0].partitions, 4);
    }

    #[test]
    fn test_plan_scan_filter() {
        let planner = RayPlanner::new();
        let scan = LogicalOp::Scan(ScanOp::nodes_with_label("Person"));
        let filter = LogicalOp::filter(scan, FilterOp::new(col("age").gt_eq(lit(18i64))));
        let plan = LogicalPlan::new(filter);

        let stages = planner.plan(&plan).unwrap();
        assert_eq!(stages.len(), 1); // Filter fused with scan
    }
}
