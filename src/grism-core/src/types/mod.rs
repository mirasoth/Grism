//! Type system for Grism values.
//!
//! This module defines the `Value` enum for runtime values and
//! `DataType` for type information, following the pattern from
//! Daft's parquet2 spec.rs for invariant checking.

mod data_type;
mod spec;
mod value;

pub use data_type::DataType;
pub use spec::{check_type_invariants, check_vector_invariants};
pub use value::Value;
