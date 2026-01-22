//! Memory management for execution.

mod manager;

pub use manager::{MemoryManager, MemoryReservation, NoopMemoryManager, TrackingMemoryManager};
