//! `StorageProvider` - unified entry point for storage (RFC-0103).
//!
//! This module provides the `StorageProvider` abstraction that serves as
//! the single entry point for all storage operations in the local engine.
//!
//! # Modes
//!
//! - `Memory`: Pure in-memory, no persistence (RFC-0020)
//! - `Lance`: Pure Lance, all data on disk (RFC-0019)
//!
//! # Usage
//!
//! ```rust,ignore
//! use grism_storage::{StorageProvider, StorageConfig, StorageMode};
//!
//! // Memory mode for testing
//! let provider = StorageProvider::new(StorageConfig::memory())?;
//!
//! // Lance mode for production
//! let provider = StorageProvider::new(StorageConfig::lance("./data"))?;
//!
//! // Get storage trait object
//! let storage = provider.storage();
//! ```

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Mutex;

use common_error::GrismResult;

use crate::lance::LanceStorage;
use crate::memory::MemoryStorage;
use crate::storage::Storage;
use crate::types::StorageCaps;

// ============================================================================
// StorageMode
// ============================================================================

/// Storage mode selection per RFC-0103.
#[derive(Debug, Clone)]
pub enum StorageMode {
    /// Pure in-memory storage, no persistence.
    Memory,
    /// Lance-based persistent storage on local filesystem.
    Lance {
        /// Path to storage directory.
        path: PathBuf,
    },
}

impl StorageMode {
    /// Create a memory mode.
    pub fn memory() -> Self {
        Self::Memory
    }

    /// Create a Lance mode with the given path.
    pub fn lance(path: impl Into<PathBuf>) -> Self {
        Self::Lance { path: path.into() }
    }
}

// ============================================================================
// StorageConfig
// ============================================================================

/// Storage configuration per RFC-0103.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Storage mode (Memory or Lance).
    pub mode: StorageMode,
    /// Memory configuration.
    pub memory: MemoryConfig,
}

/// Memory-related configuration.
#[derive(Debug, Clone)]
pub struct MemoryConfig {
    /// Maximum memory for write buffers (bytes). 0 = unlimited.
    pub write_buffer_limit: usize,
    /// Maximum memory for read cache (bytes). 0 = disabled.
    pub read_cache_limit: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            mode: StorageMode::Memory,
            memory: MemoryConfig::default(),
        }
    }
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            write_buffer_limit: 256 * 1024 * 1024, // 256 MB
            read_cache_limit: 128 * 1024 * 1024,   // 128 MB
        }
    }
}

impl StorageConfig {
    /// Create a memory-only configuration.
    pub fn memory() -> Self {
        Self {
            mode: StorageMode::Memory,
            ..Default::default()
        }
    }

    /// Create a Lance configuration.
    pub fn lance(path: impl Into<PathBuf>) -> Self {
        Self {
            mode: StorageMode::Lance { path: path.into() },
            ..Default::default()
        }
    }

    /// Set the storage mode.
    pub fn with_mode(mut self, mode: StorageMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set memory configuration.
    pub fn with_memory_config(mut self, config: MemoryConfig) -> Self {
        self.memory = config;
        self
    }
}

// ============================================================================
// ProviderState
// ============================================================================

/// Internal state tracking for the provider.
#[derive(Debug)]
struct ProviderState {
    /// Current memory usage in bytes.
    memory_usage: AtomicUsize,
    /// Active snapshot reference count (reserved for future use).
    #[allow(dead_code)]
    active_snapshots: AtomicUsize,
    /// Provider lifecycle state.
    lifecycle: Mutex<LifecycleState>,
}

/// Provider lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LifecycleState {
    /// Not yet initialized.
    Uninitialized,
    /// Ready for use.
    Ready,
    /// Closing/flushing.
    Closing,
    /// Fully closed.
    Closed,
}

impl Default for ProviderState {
    fn default() -> Self {
        Self {
            memory_usage: AtomicUsize::new(0),
            active_snapshots: AtomicUsize::new(0),
            lifecycle: Mutex::new(LifecycleState::Uninitialized),
        }
    }
}

// ============================================================================
// StorageProvider
// ============================================================================

/// Unified storage provider per RFC-0103.
///
/// The `StorageProvider` is the **sole entry point** for all storage
/// operations in the local engine. It manages the underlying storage
/// backend and provides a unified interface.
///
/// # Thread Safety
///
/// `StorageProvider` is thread-safe and can be shared across async tasks.
///
/// # Example
///
/// ```rust,ignore
/// // Create provider
/// let provider = StorageProvider::new(StorageConfig::memory())?;
///
/// // Get storage for execution
/// let storage = provider.storage();
///
/// // Create snapshot
/// let snapshot = provider.create_snapshot().await?;
///
/// // Close when done
/// provider.close().await?;
/// ```
pub struct StorageProvider {
    /// Storage mode.
    mode: StorageMode,
    /// Inner storage implementation.
    inner: Arc<dyn Storage>,
    /// Configuration.
    config: StorageConfig,
    /// Internal state.
    state: ProviderState,
}

impl std::fmt::Debug for StorageProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageProvider")
            .field("mode", &self.mode)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl StorageProvider {
    /// Create a new storage provider with the given configuration.
    ///
    /// For Lance mode, this will create a new storage if it doesn't exist,
    /// or open existing storage if it does.
    pub async fn new(config: StorageConfig) -> GrismResult<Self> {
        let inner: Arc<dyn Storage> = match &config.mode {
            StorageMode::Memory => Arc::new(MemoryStorage::new()),
            StorageMode::Lance { path } => Arc::new(LanceStorage::open(path).await?),
        };

        let provider = Self {
            mode: config.mode.clone(),
            inner,
            config,
            state: ProviderState::default(),
        };

        // Mark as ready
        *provider.state.lifecycle.lock().await = LifecycleState::Ready;

        Ok(provider)
    }

    /// Create a new storage provider synchronously (blocking).
    ///
    /// This is a convenience method for synchronous contexts.
    pub fn new_sync(config: StorageConfig) -> GrismResult<Self> {
        common_runtime::block_on(Self::new(config))?
    }

    /// Get the storage mode.
    pub fn mode(&self) -> &StorageMode {
        &self.mode
    }

    /// Get the configuration.
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Get the storage trait object.
    ///
    /// This is the primary way to access storage functionality.
    pub fn storage(&self) -> Arc<dyn Storage> {
        Arc::clone(&self.inner)
    }

    /// Get storage capabilities.
    pub fn capabilities(&self) -> StorageCaps {
        self.inner.capabilities()
    }

    /// Get current memory usage (approximate).
    pub fn memory_usage(&self) -> usize {
        self.state.memory_usage.load(Ordering::Relaxed)
    }

    /// Check if the provider is ready.
    pub async fn is_ready(&self) -> bool {
        *self.state.lifecycle.lock().await == LifecycleState::Ready
    }

    /// Check if the provider is closed.
    pub async fn is_closed(&self) -> bool {
        *self.state.lifecycle.lock().await == LifecycleState::Closed
    }

    /// Close the provider, releasing all resources.
    ///
    /// For Lance mode, this will flush any pending writes.
    pub async fn close(&self) -> GrismResult<()> {
        // Mark as closing
        {
            let mut lifecycle = self.state.lifecycle.lock().await;
            if *lifecycle == LifecycleState::Closed {
                return Ok(());
            }
            *lifecycle = LifecycleState::Closing;
        }

        // Close inner storage
        self.inner.close().await?;

        // Mark as closed
        *self.state.lifecycle.lock().await = LifecycleState::Closed;

        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::NodeBatchBuilder;

    #[tokio::test]
    async fn test_provider_memory_mode() {
        let provider = StorageProvider::new(StorageConfig::memory()).await.unwrap();

        assert!(matches!(provider.mode(), StorageMode::Memory));
        assert!(provider.is_ready().await);

        let caps = provider.capabilities();
        assert!(!caps.predicate_pushdown);
        assert!(caps.projection_pushdown);
    }

    #[tokio::test]
    async fn test_provider_lance_mode() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let provider = StorageProvider::new(StorageConfig::lance(tmp_dir.path()))
            .await
            .unwrap();

        assert!(matches!(provider.mode(), StorageMode::Lance { .. }));
        assert!(provider.is_ready().await);

        let caps = provider.capabilities();
        assert!(caps.predicate_pushdown);
        assert!(caps.projection_pushdown);

        provider.close().await.unwrap();
        assert!(provider.is_closed().await);
    }

    #[tokio::test]
    async fn test_provider_storage_access() {
        let provider = StorageProvider::new(StorageConfig::memory()).await.unwrap();
        let storage = provider.storage();

        // Test we can build data (for use with the storage)
        let mut builder = NodeBatchBuilder::new();
        builder.add(1, Some("Person"));
        let _batch = builder.build().unwrap();

        // Access storage capabilities
        let storage_ref = storage.as_ref();
        assert!(storage_ref.capabilities().projection_pushdown);
    }

    #[tokio::test]
    async fn test_config_builders() {
        let memory_config = StorageConfig::memory();
        assert!(matches!(memory_config.mode, StorageMode::Memory));

        let lance_config = StorageConfig::lance("./data");
        assert!(matches!(lance_config.mode, StorageMode::Lance { .. }));
    }

    #[tokio::test]
    async fn test_provider_close_idempotent() {
        let provider = StorageProvider::new(StorageConfig::memory()).await.unwrap();

        provider.close().await.unwrap();
        assert!(provider.is_closed().await);

        // Closing again should be fine
        provider.close().await.unwrap();
        assert!(provider.is_closed().await);
    }
}
