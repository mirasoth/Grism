//! Ray distributed execution backend for Grism.
//!
//! Provides distributed query execution using Ray as the orchestration layer.
//! Pattern: Ray orchestrates, Rust executes.

#![allow(clippy::missing_const_for_fn)] // Builder patterns often can't be const
#![allow(clippy::return_self_not_must_use)] // Builder patterns don't always need must_use
#![allow(clippy::unused_async)] // Some async functions are for API consistency
#![allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)] // Some closures are clearer

pub mod planner;
pub mod transport;
pub mod worker;

pub use planner::{RayPlanner, Stage, StageId};
pub use transport::{ArrowTransport, TransportConfig};
pub use worker::{Worker, WorkerConfig, WorkerTask};
