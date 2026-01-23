//! Error types and result aliases for Grism.
//!
//! This module provides the core error handling infrastructure

mod error;
#[cfg(feature = "python")]
pub mod python;

pub use error::{GrismError, GrismResult};
