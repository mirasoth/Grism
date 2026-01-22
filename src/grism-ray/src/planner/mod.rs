//! Distributed planning for Ray execution.
//!
//! This module provides planners for converting logical plans to distributed
//! execution plans with stage-based parallelism.

mod stage;

pub use stage::{ShuffleStrategy, Stage, StageId};

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use common_error::{GrismError, GrismResult};
use grism_engine::operators::PhysicalOperator;
use grism_engine::physical::PhysicalPlan;
use grism_engine::planner::{LocalPhysicalPlanner, PhysicalPlanner};
use grism_logical::{LogicalOp, LogicalPlan};

use crate::exchange::ExchangeMode;
use crate::executor::DistributedPlan;
use crate::partitioning::PartitioningSpec;

// ============================================================================
// Distributed Planner Configuration
// ============================================================================

/// Configuration for the distributed planner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedPlannerConfig {
    /// Default number of partitions.
    pub default_parallelism: usize,
    /// Maximum stage size (number of operators).
    pub max_stage_size: usize,
    /// Enable stage fusion optimization.
    pub enable_fusion: bool,
    /// Prefer adjacency-based partitioning for graph operations.
    pub prefer_adjacency_partitioning: bool,
    /// Target batch size.
    pub batch_size: usize,
}

impl Default for DistributedPlannerConfig {
    fn default() -> Self {
        Self {
            default_parallelism: 4,
            max_stage_size: 10,
            enable_fusion: true,
            prefer_adjacency_partitioning: true,
            batch_size: 8192,
        }
    }
}

impl DistributedPlannerConfig {
    /// Set the default parallelism.
    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.default_parallelism = parallelism;
        self
    }

    /// Enable or disable stage fusion.
    pub fn with_fusion(mut self, enabled: bool) -> Self {
        self.enable_fusion = enabled;
        self
    }
}

// ============================================================================
// Distributed Planner
// ============================================================================

/// Distributed planner for Ray execution.
///
/// Converts logical plans into distributed execution plans by:
/// 1. Creating a physical plan
/// 2. Inserting Exchange operators where needed
/// 3. Splitting the plan into execution stages
///
/// # Stage Boundaries (RFC-0102, Section 7.5)
///
/// A new stage MUST start at:
/// - Any Exchange operator
/// - Any blocking operator in distributed mode
/// - Any operator requiring global state
pub struct DistributedPlanner {
    /// Planner configuration.
    config: DistributedPlannerConfig,
    /// Local planner for physical planning.
    local_planner: LocalPhysicalPlanner,
}

impl DistributedPlanner {
    /// Create a new distributed planner.
    pub fn new() -> Self {
        Self {
            config: DistributedPlannerConfig::default(),
            local_planner: LocalPhysicalPlanner::new(),
        }
    }

    /// Create with configuration.
    pub fn with_config(config: DistributedPlannerConfig) -> Self {
        Self {
            config,
            local_planner: LocalPhysicalPlanner::new(),
        }
    }

    /// Get the planner configuration.
    pub fn config(&self) -> &DistributedPlannerConfig {
        &self.config
    }

    /// Plan a logical plan for distributed execution.
    pub fn plan(&self, logical_plan: &LogicalPlan) -> GrismResult<DistributedPlan> {
        // Step 1: Create physical plan using local planner
        let physical_plan = self.local_planner.plan(logical_plan)?;

        // Step 2: Insert exchanges and split into stages
        let stages = self.split_into_stages(&physical_plan)?;

        // Step 3: Build distributed plan
        Ok(DistributedPlan::new(stages, physical_plan.schema().clone()))
    }

    /// Split a physical plan into execution stages.
    ///
    /// This is the core algorithm for distributed planning. It traverses
    /// the physical plan and creates stage boundaries at:
    /// - Exchange operators
    /// - Blocking operators (Sort, Aggregate)
    fn split_into_stages(&self, physical_plan: &PhysicalPlan) -> GrismResult<Vec<Stage>> {
        let mut stages = Vec::new();
        let mut current_stage = Stage::new(0).with_partitions(self.config.default_parallelism);

        // Walk the operator tree
        self.split_recursive(
            physical_plan.root(),
            &mut current_stage,
            &mut stages,
            0,
        )?;

        // Add the final stage if non-empty
        if !current_stage.operators.is_empty() {
            stages.push(current_stage);
        }

        // If no stages were created, create an empty one
        if stages.is_empty() {
            stages.push(Stage::new(0).with_partitions(1));
        }

        Ok(stages)
    }

    fn split_recursive(
        &self,
        op: &Arc<dyn PhysicalOperator>,
        current_stage: &mut Stage,
        stages: &mut Vec<Stage>,
        depth: usize,
    ) -> GrismResult<()> {
        let caps = op.capabilities();
        let name = op.name();

        // Check if this operator is a stage boundary
        let is_boundary = caps.blocking || name == "ExchangeExec";

        if is_boundary && !current_stage.operators.is_empty() {
            // Finish current stage and start a new one
            let finished_stage = std::mem::replace(
                current_stage,
                Stage::new((stages.len() + 1) as u64)
                    .with_partitions(self.config.default_parallelism),
            );

            // Add dependency from new stage to finished stage
            current_stage.dependencies.push(finished_stage.id);

            // If blocking, add exchange between stages
            if caps.blocking {
                current_stage.shuffle = ShuffleStrategy::Single;
            }

            stages.push(finished_stage);
        }

        // Add operator info to stage (we store logical ops for serialization)
        // In a full implementation, we'd store physical operator metadata
        // For now, just track operator names for debugging

        // Process children first (for proper ordering)
        for child in op.children() {
            self.split_recursive(child, current_stage, stages, depth + 1)?;
        }

        Ok(())
    }

    /// Determine where to insert Exchange operators.
    ///
    /// Exchanges are needed:
    /// - Before aggregation (to partition by group keys)
    /// - Before sort (to partition by sort key)
    /// - Before final collection (gather)
    pub fn determine_exchanges(&self, _plan: &PhysicalPlan) -> Vec<ExchangeInsertPoint> {
        // TODO: Implement exchange insertion logic
        // This would analyze the plan and determine:
        // 1. Which operators need repartitioning
        // 2. What partitioning scheme to use
        // 3. What exchange mode (shuffle/broadcast/gather)
        vec![]
    }
}

impl Default for DistributedPlanner {
    fn default() -> Self {
        Self::new()
    }
}

/// Point where an Exchange should be inserted.
#[derive(Debug, Clone)]
pub struct ExchangeInsertPoint {
    /// Operator ID to insert exchange before.
    pub before_operator: String,
    /// Partitioning specification.
    pub partitioning: PartitioningSpec,
    /// Exchange mode.
    pub mode: ExchangeMode,
}

// ============================================================================
// Legacy RayPlanner (kept for backward compatibility)
// ============================================================================

/// Legacy Ray planner (deprecated, use DistributedPlanner).
#[deprecated(note = "Use DistributedPlanner instead")]
pub type RayPlanner = LegacyRayPlanner;

/// Legacy planner configuration.
pub type PlannerConfig = DistributedPlannerConfig;

/// Legacy Ray planner implementation.
pub struct LegacyRayPlanner {
    config: DistributedPlannerConfig,
}

impl LegacyRayPlanner {
    /// Create a new legacy Ray planner.
    pub fn new() -> Self {
        Self {
            config: DistributedPlannerConfig::default(),
        }
    }

    /// Create with configuration.
    pub fn with_config(config: DistributedPlannerConfig) -> Self {
        Self { config }
    }

    /// Plan a logical plan into stages (legacy API).
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
                let stage = Stage::new(current_stage_id)
                    .with_partitions(self.config.default_parallelism)
                    .with_operator(op.clone());
                stages.push(stage);
                Ok(current_stage_id)
            }

            LogicalOp::Filter { input, filter: _ } => {
                let input_stage = self.plan_recursive(input, stages, current_stage_id)?;
                if let Some(stage) = stages.iter_mut().find(|s| s.id == input_stage) {
                    stage.add_operator(op.clone());
                }
                Ok(input_stage)
            }

            LogicalOp::Project { input, project: _ } => {
                let input_stage = self.plan_recursive(input, stages, current_stage_id)?;
                if let Some(stage) = stages.iter_mut().find(|s| s.id == input_stage) {
                    stage.add_operator(op.clone());
                }
                Ok(input_stage)
            }

            LogicalOp::Limit { input, limit: _ } => {
                let input_stage = self.plan_recursive(input, stages, current_stage_id)?;
                let final_stage = Stage::new(current_stage_id + 1)
                    .with_partitions(1)
                    .with_operator(op.clone())
                    .with_dependency(input_stage);
                stages.push(final_stage);
                Ok(current_stage_id + 1)
            }

            // Mark unimplemented operations clearly
            LogicalOp::Expand { .. } => {
                Err(GrismError::not_implemented("Distributed expand planning"))
            }
            LogicalOp::Aggregate { .. } => {
                Err(GrismError::not_implemented("Distributed aggregate planning"))
            }
            LogicalOp::Sort { .. } => {
                Err(GrismError::not_implemented("Distributed sort planning"))
            }
            LogicalOp::Union { .. } => {
                Err(GrismError::not_implemented("Distributed union planning"))
            }
            LogicalOp::Rename { .. } => {
                Err(GrismError::not_implemented("Distributed rename planning"))
            }
            LogicalOp::Infer { .. } => {
                Err(GrismError::not_implemented("Distributed infer planning"))
            }
            LogicalOp::Empty => {
                Err(GrismError::not_implemented("Distributed empty planning"))
            }
        }
    }

    /// Get planner configuration.
    pub fn config(&self) -> &DistributedPlannerConfig {
        &self.config
    }
}

impl Default for LegacyRayPlanner {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use grism_logical::{FilterOp, ScanOp, col, lit};

    #[test]
    fn test_distributed_planner_creation() {
        let planner = DistributedPlanner::new();
        assert_eq!(planner.config().default_parallelism, 4);
    }

    #[test]
    fn test_legacy_plan_simple_scan() {
        #[allow(deprecated)]
        let planner = LegacyRayPlanner::new();
        let scan = LogicalOp::Scan(ScanOp::nodes_with_label("Person"));
        let plan = LogicalPlan::new(scan);

        let stages = planner.plan(&plan).unwrap();
        assert_eq!(stages.len(), 1);
        assert_eq!(stages[0].partitions, 4);
    }

    #[test]
    fn test_legacy_plan_scan_filter() {
        #[allow(deprecated)]
        let planner = LegacyRayPlanner::new();
        let scan = LogicalOp::Scan(ScanOp::nodes_with_label("Person"));
        let filter = LogicalOp::filter(scan, FilterOp::new(col("age").gt_eq(lit(18i64))));
        let plan = LogicalPlan::new(filter);

        let stages = planner.plan(&plan).unwrap();
        assert_eq!(stages.len(), 1);
    }

    #[test]
    fn test_distributed_planner_config() {
        let config = DistributedPlannerConfig::default()
            .with_parallelism(8)
            .with_fusion(false);

        assert_eq!(config.default_parallelism, 8);
        assert!(!config.enable_fusion);
    }
}
