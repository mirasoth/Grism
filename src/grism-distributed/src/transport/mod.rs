//! Data transport layer for distributed execution.

mod ipc;

pub use ipc::ArrowTransport;

use serde::{Deserialize, Serialize};

/// Transport configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransportConfig {
    /// Maximum message size in bytes.
    pub max_message_size: usize,
    /// Compression enabled.
    pub compression: bool,
    /// Compression level (1-9).
    pub compression_level: u32,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            max_message_size: 64 * 1024 * 1024, // 64 MB
            compression: true,
            compression_level: 3,
        }
    }
}
