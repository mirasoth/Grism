//! Ray distributed execution backend for Grism.
//!
//! This crate provides distributed query execution using Ray as the orchestration layer.
//! The core principle is: **Ray orchestrates, Rust executes.**
//!
//! # Architecture (RFC-0102)
//!
//! The Ray runtime provides distributed execution using a stage-based model:
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────────────┐
//! │                        Distributed Plan                              │
//! ├──────────────────────────────────────────────────────────────────────┤
//! │                                                                      │
//! │  Stage 0 (parallel)       Exchange        Stage 1 (parallel)        │
//! │  ┌─────────────────┐    ┌─────────┐     ┌─────────────────┐         │
//! │  │ Scan → Filter   │───▶│ Shuffle │────▶│ Agg → Collect   │         │
//! │  │ → Project       │    │ (Hash)  │     │                 │         │
//! │  └─────────────────┘    └─────────┘     └─────────────────┘         │
//! │         │                                      │                     │
//! │  ┌──────┴──────┐                        ┌──────┴──────┐             │
//! │  │ Worker 1-N  │                        │ Worker 1-M  │             │
//! │  └─────────────┘                        └─────────────┘             │
//! │                                                                      │
//! └──────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Key Components
//!
//! - [`DistributedPlanner`]: Converts logical plans to distributed execution plans
//! - [`DistributedPlan`]: A DAG of execution stages
//! - [`RayExecutor`]: Orchestrates distributed execution (preview)
//! - [`ExchangeExec`]: Repartitions data across workers
//! - [`ExecutionStage`]: Execution unit containing operators and partitioning info
//!
//! # Status: Preview
//!
//! This crate is in preview status. Core functionality is implemented but
//! actual Ray integration requires the Ray Python/Rust bindings.
//! Unimplemented parts are marked with `TODO` comments or return
//! `GrismError::NotImplemented`.

#![allow(clippy::missing_const_for_fn)]
#![allow(clippy::return_self_not_must_use)]
#![allow(clippy::unused_async)]
#![allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#![allow(clippy::match_same_arms)] // Some match arms intentionally have same body
#![allow(clippy::only_used_in_recursion)] // Some recursive params are for future use
#![allow(clippy::doc_markdown)] // Allow doc without backticks in some cases
#![allow(clippy::cast_possible_truncation)] // Some casts are intentional
#![allow(clippy::collection_is_never_read)] // Some collections are for future use
#![allow(clippy::uninlined_format_args)] // Format args are sometimes clearer non-inline
#![allow(clippy::missing_fields_in_debug)] // Some Debug impls skip internal fields
#![allow(clippy::derivable_impls)] // Some manual Default impls are clearer
#![allow(clippy::items_after_statements)] // Local functions after statements are sometimes clearer
#![allow(clippy::format_push_string)] // format! + push_str is sometimes clearer
#![allow(dead_code)] // Preview code may have unused items

pub mod exchange;
pub mod executor;
pub mod partitioning;
pub mod planner;
pub mod transport;
pub mod worker;

// Re-export key types from planner
pub use planner::{
    DistributedPlan, DistributedPlanner, DistributedPlannerConfig, ExecutionStage,
    ExecutionStageBuilder, StageId,
};

// Re-export exchange and partitioning types
pub use exchange::{ExchangeExec, ExchangeMode};
pub use partitioning::{PartitioningScheme, PartitioningSpec};

// Re-export executor types
pub use executor::{RayExecutor, RayExecutorConfig};

// Re-export transport types
pub use transport::{ArrowTransport, TransportConfig};

// Re-export worker types
pub use worker::{Worker, WorkerConfig, WorkerTask};
