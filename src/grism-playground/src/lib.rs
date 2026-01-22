//! Grism Playground - Experiments and Examples
//!
//! This crate provides executable apps for experimenting with Grism's
//! hypergraph database capabilities.
//!
//! # Available Binaries
//!
//! - **`hypergraph-demo`**: End-to-end demo reading hypergraph data and running queries
//! - **`query-runner`**: Interactive query runner for testing
//!
//! # Usage
//!
//! ```bash
//! # Run the hypergraph demo
//! cargo run --package grism-playground --bin hypergraph-demo
//!
//! # Run the query runner
//! cargo run --package grism-playground --bin query-runner
//! ```

pub mod data;
pub mod utils;

pub use data::{create_sample_hypergraph, create_social_network};
pub use utils::{format_batch, print_divider, print_header, print_results};
