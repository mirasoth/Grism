//! Ray distributed execution backend for Grism.
//!
//! Provides distributed query execution using Ray as the orchestration layer.
//! Pattern: Ray orchestrates, Rust executes.

pub mod planner;
pub mod transport;
pub mod worker;

pub use planner::{RayPlanner, Stage, StageId};
pub use transport::{ArrowTransport, TransportConfig};
pub use worker::{Worker, WorkerConfig, WorkerTask};
