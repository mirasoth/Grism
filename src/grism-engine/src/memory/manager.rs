//! Memory manager for tracking and limiting memory usage.

use std::sync::atomic::{AtomicUsize, Ordering};

use common_error::{GrismError, GrismResult};

/// Memory manager for tracking and limiting memory usage.
///
/// In the current implementation, this provides **accounting only** (no spill-to-disk).
/// Future implementations may add spill support.
pub trait MemoryManager: Send + Sync + std::fmt::Debug {
    /// Reserve memory. Returns error if limit exceeded.
    fn reserve(&self, bytes: usize) -> GrismResult<()>;

    /// Release previously reserved memory.
    fn release(&self, bytes: usize);

    /// Get current memory usage.
    fn used(&self) -> usize;

    /// Get memory limit (0 = unlimited).
    fn limit(&self) -> usize;

    /// Get available memory (limit - used, or usize::MAX if unlimited).
    fn available(&self) -> usize {
        let limit = self.limit();
        if limit == 0 {
            usize::MAX
        } else {
            limit.saturating_sub(self.used())
        }
    }

    /// Check if a reservation would succeed without actually reserving.
    fn can_reserve(&self, bytes: usize) -> bool {
        let limit = self.limit();
        if limit == 0 {
            true
        } else {
            self.used() + bytes <= limit
        }
    }
}

/// No-op memory manager for unlimited memory.
#[derive(Debug, Default)]
pub struct NoopMemoryManager;

impl NoopMemoryManager {
    /// Create a new no-op memory manager.
    pub fn new() -> Self {
        Self
    }
}

impl MemoryManager for NoopMemoryManager {
    fn reserve(&self, _bytes: usize) -> GrismResult<()> {
        Ok(())
    }

    fn release(&self, _bytes: usize) {}

    fn used(&self) -> usize {
        0
    }

    fn limit(&self) -> usize {
        0
    }
}

/// Tracking memory manager with limit enforcement.
#[derive(Debug)]
pub struct TrackingMemoryManager {
    /// Current memory usage.
    used: AtomicUsize,
    /// Memory limit (0 = unlimited).
    limit: usize,
}

impl TrackingMemoryManager {
    /// Create a new tracking memory manager.
    ///
    /// # Arguments
    ///
    /// * `limit` - Memory limit in bytes. 0 means unlimited.
    pub fn new(limit: usize) -> Self {
        Self {
            used: AtomicUsize::new(0),
            limit,
        }
    }

    /// Create an unlimited tracking memory manager (for accounting only).
    pub fn unlimited() -> Self {
        Self::new(0)
    }

    /// Get usage percentage (0.0 - 1.0, or 0.0 if unlimited).
    pub fn usage_ratio(&self) -> f64 {
        if self.limit == 0 {
            0.0
        } else {
            self.used() as f64 / self.limit as f64
        }
    }
}

impl Default for TrackingMemoryManager {
    fn default() -> Self {
        Self::unlimited()
    }
}

impl MemoryManager for TrackingMemoryManager {
    fn reserve(&self, bytes: usize) -> GrismResult<()> {
        if bytes == 0 {
            return Ok(());
        }

        // Try to reserve atomically
        let mut current = self.used.load(Ordering::Relaxed);
        loop {
            let new = current.saturating_add(bytes);

            // Check limit
            if self.limit > 0 && new > self.limit {
                return Err(GrismError::resource_exhausted(format!(
                    "Memory limit exceeded: {} + {} > {} bytes",
                    current, bytes, self.limit
                )));
            }

            match self
                .used
                .compare_exchange_weak(current, new, Ordering::SeqCst, Ordering::Relaxed)
            {
                Ok(_) => return Ok(()),
                Err(actual) => current = actual,
            }
        }
    }

    fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }

        // Saturating subtraction to handle potential underflow
        let mut current = self.used.load(Ordering::Relaxed);
        loop {
            let new = current.saturating_sub(bytes);
            match self
                .used
                .compare_exchange_weak(current, new, Ordering::SeqCst, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    fn used(&self) -> usize {
        self.used.load(Ordering::SeqCst)
    }

    fn limit(&self) -> usize {
        self.limit
    }
}

/// RAII guard for memory reservations.
///
/// Automatically releases memory when dropped.
pub struct MemoryReservation {
    manager: std::sync::Arc<dyn MemoryManager>,
    bytes: usize,
}

impl MemoryReservation {
    /// Create a new memory reservation.
    pub fn try_new(manager: std::sync::Arc<dyn MemoryManager>, bytes: usize) -> GrismResult<Self> {
        manager.reserve(bytes)?;
        Ok(Self { manager, bytes })
    }

    /// Get the reserved size.
    pub fn size(&self) -> usize {
        self.bytes
    }

    /// Grow the reservation by additional bytes.
    pub fn grow(&mut self, additional: usize) -> GrismResult<()> {
        self.manager.reserve(additional)?;
        self.bytes += additional;
        Ok(())
    }

    /// Shrink the reservation.
    pub fn shrink(&mut self, amount: usize) {
        let release = amount.min(self.bytes);
        self.manager.release(release);
        self.bytes -= release;
    }

    /// Release all reserved memory without dropping.
    pub fn free(mut self) {
        self.manager.release(self.bytes);
        self.bytes = 0;
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.manager.release(self.bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_noop_manager() {
        let manager = NoopMemoryManager::new();
        assert!(manager.reserve(1000).is_ok());
        manager.release(1000);
        assert_eq!(manager.used(), 0);
        assert_eq!(manager.limit(), 0);
    }

    #[test]
    fn test_tracking_manager() {
        let manager = TrackingMemoryManager::new(1000);

        assert!(manager.reserve(500).is_ok());
        assert_eq!(manager.used(), 500);

        assert!(manager.reserve(400).is_ok());
        assert_eq!(manager.used(), 900);

        // Should fail - over limit
        assert!(manager.reserve(200).is_err());
        assert_eq!(manager.used(), 900);

        manager.release(400);
        assert_eq!(manager.used(), 500);
    }

    #[test]
    fn test_tracking_manager_unlimited() {
        let manager = TrackingMemoryManager::unlimited();

        assert!(manager.reserve(1_000_000).is_ok());
        assert_eq!(manager.used(), 1_000_000);
        assert_eq!(manager.limit(), 0);
    }

    #[test]
    fn test_memory_reservation() {
        let manager: Arc<dyn MemoryManager> = Arc::new(TrackingMemoryManager::new(1000));

        {
            let reservation = MemoryReservation::try_new(Arc::clone(&manager), 500).unwrap();
            assert_eq!(reservation.size(), 500);
            assert_eq!(manager.used(), 500);
        }

        // Memory released on drop
        assert_eq!(manager.used(), 0);
    }

    #[test]
    fn test_memory_reservation_grow() {
        let manager: Arc<dyn MemoryManager> = Arc::new(TrackingMemoryManager::new(1000));

        let mut reservation = MemoryReservation::try_new(Arc::clone(&manager), 500).unwrap();
        assert_eq!(manager.used(), 500);

        reservation.grow(200).unwrap();
        assert_eq!(reservation.size(), 700);
        assert_eq!(manager.used(), 700);
    }

    #[test]
    fn test_can_reserve() {
        let manager = TrackingMemoryManager::new(1000);
        assert!(manager.can_reserve(500));
        manager.reserve(800).unwrap();
        assert!(!manager.can_reserve(300));
        assert!(manager.can_reserve(200));
    }
}
