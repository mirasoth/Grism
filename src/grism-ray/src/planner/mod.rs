//! Distributed planning for Ray execution.
//!
//! This module provides planners for converting logical plans to distributed
//! execution plans with stage-based parallelism.

mod stage;

pub use stage::{ExecutionStage, ExecutionStageBuilder, StageId};

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use common_error::GrismResult;
use grism_engine::operators::PhysicalOperator;
use grism_engine::physical::{PhysicalPlan, PhysicalSchema};
use grism_engine::planner::{LocalPhysicalPlanner, PhysicalPlanner};
use grism_logical::LogicalPlan;

use crate::exchange::ExchangeMode;
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
    fn split_into_stages(&self, physical_plan: &PhysicalPlan) -> GrismResult<Vec<ExecutionStage>> {
        let mut stages = Vec::new();
        let mut current_stage =
            ExecutionStage::new(0).with_partitions(self.config.default_parallelism);

        // Walk the operator tree
        self.split_recursive(physical_plan.root(), &mut current_stage, &mut stages)?;

        // Add the final stage if non-empty
        if !current_stage.operator_names.is_empty() {
            stages.push(current_stage);
        }

        // If no stages were created, create an empty one
        if stages.is_empty() {
            stages.push(ExecutionStage::new(0).with_partitions(1));
        }

        Ok(stages)
    }

    fn split_recursive(
        &self,
        op: &Arc<dyn PhysicalOperator>,
        current_stage: &mut ExecutionStage,
        stages: &mut Vec<ExecutionStage>,
    ) -> GrismResult<()> {
        let caps = op.capabilities();
        let name = op.name();

        // Check if this operator is a stage boundary
        let is_boundary = caps.blocking || name == "ExchangeExec";

        if is_boundary && !current_stage.operator_names.is_empty() {
            // Finish current stage and start a new one
            let finished_stage = std::mem::replace(
                current_stage,
                ExecutionStage::new((stages.len() + 1) as u64)
                    .with_partitions(self.config.default_parallelism),
            );

            // Add dependency from new stage to finished stage
            current_stage.dependencies.push(finished_stage.id);

            // If blocking, add gather exchange between stages
            if caps.blocking {
                current_stage.input_exchange = Some(ExchangeMode::Gather);
            }

            stages.push(finished_stage);
        }

        // Add operator name to stage for tracking
        current_stage.add_operator(name);

        // Process children (depth-first traversal)
        for child in op.children() {
            self.split_recursive(child, current_stage, stages)?;
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

// ============================================================================
// Distributed Plan
// ============================================================================

/// A distributed execution plan consisting of stages.
///
/// The plan represents a DAG of stages, where each stage can be executed
/// in parallel and stages are connected by exchanges.
#[derive(Debug, Clone)]
pub struct DistributedPlan {
    /// Execution stages.
    pub stages: Vec<ExecutionStage>,
    /// Output schema (from final stage).
    pub schema: PhysicalSchema,
    /// Stage dependencies (stage_id -> [dependency_stage_ids]).
    pub dependencies: HashMap<StageId, Vec<StageId>>,
}

impl DistributedPlan {
    /// Create a new distributed plan.
    pub fn new(stages: Vec<ExecutionStage>, schema: PhysicalSchema) -> Self {
        // Build dependency graph
        let mut dependencies = HashMap::new();
        for stage in &stages {
            dependencies.insert(stage.id, stage.dependencies.clone());
        }

        Self {
            stages,
            schema,
            dependencies,
        }
    }

    /// Get stages in topological order (dependencies first).
    pub fn topological_order(&self) -> Vec<&ExecutionStage> {
        let mut result = Vec::new();
        let mut visited = std::collections::HashSet::new();

        fn visit<'a>(
            stage_id: StageId,
            stages: &'a [ExecutionStage],
            deps: &HashMap<StageId, Vec<StageId>>,
            visited: &mut std::collections::HashSet<StageId>,
            result: &mut Vec<&'a ExecutionStage>,
        ) {
            if visited.contains(&stage_id) {
                return;
            }
            visited.insert(stage_id);

            if let Some(dep_ids) = deps.get(&stage_id) {
                for &dep_id in dep_ids {
                    visit(dep_id, stages, deps, visited, result);
                }
            }

            if let Some(stage) = stages.iter().find(|s| s.id == stage_id) {
                result.push(stage);
            }
        }

        for stage in &self.stages {
            visit(
                stage.id,
                &self.stages,
                &self.dependencies,
                &mut visited,
                &mut result,
            );
        }

        result
    }

    /// Get the number of stages.
    pub fn num_stages(&self) -> usize {
        self.stages.len()
    }

    /// Get a stage by ID.
    pub fn get_stage(&self, id: StageId) -> Option<&ExecutionStage> {
        self.stages.iter().find(|s| s.id == id)
    }

    /// Get the root stages (no dependents).
    pub fn root_stages(&self) -> Vec<&ExecutionStage> {
        let has_dependents: std::collections::HashSet<_> = self
            .dependencies
            .values()
            .flat_map(|deps| deps.iter())
            .copied()
            .collect();

        self.stages
            .iter()
            .filter(|s| !has_dependents.contains(&s.id))
            .collect()
    }

    /// Format plan for display.
    pub fn explain(&self) -> String {
        let mut output = String::new();
        output.push_str("Distributed Plan:\n");

        for stage in self.topological_order() {
            output.push_str(&format!(
                "\nStage {} (parallelism={}):\n",
                stage.id, stage.partitions
            ));

            for (i, op_name) in stage.operator_names.iter().enumerate() {
                let prefix = if i == stage.operator_names.len() - 1 {
                    "└── "
                } else {
                    "├── "
                };
                output.push_str(&format!("  {}{}\n", prefix, op_name));
            }

            if !stage.dependencies.is_empty() {
                output.push_str(&format!("  Dependencies: {:?}\n", stage.dependencies));
            }

            if let Some(mode) = &stage.input_exchange {
                output.push_str(&format!("  Input Exchange: {:?}\n", mode));
            }
        }

        output
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
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use grism_engine::physical::PhysicalSchemaBuilder;

    #[test]
    fn test_distributed_planner_creation() {
        let planner = DistributedPlanner::new();
        assert_eq!(planner.config().default_parallelism, 4);
    }

    #[test]
    fn test_distributed_planner_config() {
        let config = DistributedPlannerConfig::default()
            .with_parallelism(8)
            .with_fusion(false);

        assert_eq!(config.default_parallelism, 8);
        assert!(!config.enable_fusion);
    }

    #[test]
    fn test_execution_stage_builder() {
        let stage = ExecutionStageBuilder::new(1)
            .partitions(4)
            .operator("NodeScanExec")
            .operator("FilterExec")
            .input_exchange(ExchangeMode::Shuffle)
            .build();

        assert_eq!(stage.id, 1);
        assert_eq!(stage.partitions, 4);
        assert_eq!(stage.num_operators(), 2);
        assert!(stage.requires_input_exchange());
    }

    #[test]
    fn test_distributed_plan_creation() {
        let schema = PhysicalSchemaBuilder::new().build();
        let stages = vec![
            ExecutionStage::new(0)
                .with_partitions(4)
                .with_operator("NodeScanExec")
                .with_operator("FilterExec"),
            ExecutionStage::new(1)
                .with_partitions(2)
                .with_dependency(0)
                .with_input_exchange(ExchangeMode::Shuffle)
                .with_operator("HashAggregateExec"),
        ];

        let plan = DistributedPlan::new(stages, schema);

        assert_eq!(plan.num_stages(), 2);
        assert!(plan.get_stage(0).is_some());
        assert!(plan.get_stage(1).is_some());
        assert!(plan.get_stage(99).is_none());
    }

    #[test]
    fn test_distributed_plan_topological_order() {
        let schema = PhysicalSchemaBuilder::new().build();
        let stages = vec![
            ExecutionStage::new(0).with_partitions(4),
            ExecutionStage::new(1).with_partitions(2).with_dependency(0),
            ExecutionStage::new(2).with_partitions(1).with_dependency(1),
        ];

        let plan = DistributedPlan::new(stages, schema);
        let order = plan.topological_order();

        // Dependencies should come first
        assert_eq!(order.len(), 3);
        assert_eq!(order[0].id, 0);
        assert_eq!(order[1].id, 1);
        assert_eq!(order[2].id, 2);
    }

    #[test]
    fn test_distributed_plan_root_stages() {
        let schema = PhysicalSchemaBuilder::new().build();
        let stages = vec![
            ExecutionStage::new(0).with_partitions(4),
            ExecutionStage::new(1).with_partitions(2).with_dependency(0),
        ];

        let plan = DistributedPlan::new(stages, schema);
        let roots = plan.root_stages();

        // Stage 1 depends on Stage 0, so Stage 1 is the root (final stage)
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].id, 1);
    }

    #[test]
    fn test_distributed_plan_explain() {
        let schema = PhysicalSchemaBuilder::new().build();
        let stages = vec![
            ExecutionStage::new(0)
                .with_partitions(4)
                .with_operator("NodeScanExec")
                .with_operator("FilterExec"),
            ExecutionStage::new(1)
                .with_partitions(1)
                .with_dependency(0)
                .with_input_exchange(ExchangeMode::Gather)
                .with_operator("CollectExec"),
        ];

        let plan = DistributedPlan::new(stages, schema);
        let explain = plan.explain();

        assert!(explain.contains("Distributed Plan"));
        assert!(explain.contains("Stage 0"));
        assert!(explain.contains("Stage 1"));
        assert!(explain.contains("NodeScanExec"));
        assert!(explain.contains("FilterExec"));
        assert!(explain.contains("CollectExec"));
        assert!(explain.contains("Input Exchange: Gather"));
    }

    #[test]
    fn test_execution_stage_with_exchange_modes() {
        let stage = ExecutionStage::new(0)
            .with_partitions(8)
            .with_input_exchange(ExchangeMode::Shuffle)
            .with_output_exchange(ExchangeMode::Gather)
            .with_shuffle_keys(vec!["city".to_string()]);

        assert!(stage.requires_input_exchange());
        assert!(stage.requires_output_exchange());
        assert_eq!(stage.shuffle_keys, vec!["city"]);
    }
}
