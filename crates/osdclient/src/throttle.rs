//! Operation throttling
//!
//! This module implements request throttling to prevent resource exhaustion,
//! matching the behavior of C++ Objecter's op_throttle_bytes and op_throttle_ops.
//!
//! Reference: ~/dev/ceph/src/osdc/Objecter.h lines 2693-2698

use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Semaphore;

/// Operation throttle
///
/// Limits both the number of concurrent operations and the total bytes in-flight.
/// This prevents the client from overwhelming itself or the OSDs.
///
/// Based on C++ Objecter throttles:
/// - op_throttle_ops: Limits concurrent operation count
/// - op_throttle_bytes: Limits total bytes in requests/responses
pub struct Throttle {
    /// Maximum concurrent operations
    max_ops: usize,

    /// Maximum bytes in-flight
    max_bytes: usize,

    /// Semaphore for operation count
    ops_sem: Semaphore,

    /// Semaphore for byte budget
    bytes_sem: Semaphore,

    /// Current operations (for monitoring)
    current_ops: AtomicUsize,

    /// Current bytes (for monitoring)
    current_bytes: AtomicUsize,
}

impl Throttle {
    /// Create a new throttle
    ///
    /// # Arguments
    /// * `max_ops` - Maximum concurrent operations (default: 1024)
    /// * `max_bytes` - Maximum bytes in-flight (default: 100 MB)
    pub fn new(max_ops: usize, max_bytes: usize) -> Self {
        Self {
            max_ops,
            max_bytes,
            ops_sem: Semaphore::new(max_ops),
            bytes_sem: Semaphore::new(max_bytes),
            current_ops: AtomicUsize::new(0),
            current_bytes: AtomicUsize::new(0),
        }
    }

    /// Create throttle with default limits
    ///
    /// Matches Ceph defaults:
    /// - objecter_inflight_ops: 1024
    /// - objecter_inflight_op_bytes: 100 MB
    pub fn default_limits() -> Self {
        const DEFAULT_MAX_OPS: usize = 1024;
        const DEFAULT_MAX_BYTES: usize = 100 * 1024 * 1024; // 100 MB

        Self::new(DEFAULT_MAX_OPS, DEFAULT_MAX_BYTES)
    }

    /// Acquire budget for an operation
    ///
    /// This will block if the throttle limits are exceeded, providing backpressure.
    ///
    /// # Arguments
    /// * `bytes` - Number of bytes this operation will use
    ///
    /// # Returns
    /// A permit that releases the budget when dropped
    pub async fn acquire(&self, bytes: usize) -> ThrottlePermit<'_> {
        // Acquire operation slot (blocks if at limit)
        let op_permit = self
            .ops_sem
            .acquire()
            .await
            .expect("Semaphore should not be closed");

        // Acquire byte budget (blocks if at limit)
        let bytes_permit = if bytes > 0 {
            Some(
                self.bytes_sem
                    .acquire_many(bytes as u32)
                    .await
                    .expect("Semaphore should not be closed"),
            )
        } else {
            None
        };

        // Update monitoring counters
        self.current_ops.fetch_add(1, Ordering::Relaxed);
        if bytes > 0 {
            self.current_bytes.fetch_add(bytes, Ordering::Relaxed);
        }

        ThrottlePermit {
            throttle: self,
            op_permit,
            bytes_permit,
            bytes,
        }
    }

    /// Try to acquire budget without blocking
    ///
    /// Returns None if the budget is not available.
    pub fn try_acquire(&self, bytes: usize) -> Option<ThrottlePermit<'_>> {
        // Try to acquire operation slot
        let op_permit = self.ops_sem.try_acquire().ok()?;

        // Try to acquire byte budget
        let bytes_permit = if bytes > 0 {
            Some(self.bytes_sem.try_acquire_many(bytes as u32).ok()?)
        } else {
            None
        };

        // Update monitoring counters
        self.current_ops.fetch_add(1, Ordering::Relaxed);
        if bytes > 0 {
            self.current_bytes.fetch_add(bytes, Ordering::Relaxed);
        }

        Some(ThrottlePermit {
            throttle: self,
            op_permit,
            bytes_permit,
            bytes,
        })
    }

    /// Get current number of operations
    pub fn current_ops(&self) -> usize {
        self.current_ops.load(Ordering::Relaxed)
    }

    /// Get current bytes in-flight
    pub fn current_bytes(&self) -> usize {
        self.current_bytes.load(Ordering::Relaxed)
    }

    /// Get maximum operations
    pub fn max_ops(&self) -> usize {
        self.max_ops
    }

    /// Get maximum bytes
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }
}

/// RAII guard that releases throttle budget when dropped
pub struct ThrottlePermit<'a> {
    throttle: &'a Throttle,
    #[allow(dead_code)]
    op_permit: tokio::sync::SemaphorePermit<'a>,
    #[allow(dead_code)]
    bytes_permit: Option<tokio::sync::SemaphorePermit<'a>>,
    bytes: usize,
}

impl<'a> Drop for ThrottlePermit<'a> {
    fn drop(&mut self) {
        // Update monitoring counters
        self.throttle.current_ops.fetch_sub(1, Ordering::Relaxed);
        if self.bytes > 0 {
            self.throttle
                .current_bytes
                .fetch_sub(self.bytes, Ordering::Relaxed);
        }
        // Semaphore permits are automatically released when dropped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_throttle_basic() {
        let throttle = Throttle::new(2, 1000);

        // Acquire first operation
        let permit1 = throttle.acquire(100).await;
        assert_eq!(throttle.current_ops(), 1);
        assert_eq!(throttle.current_bytes(), 100);

        // Acquire second operation
        let permit2 = throttle.acquire(200).await;
        assert_eq!(throttle.current_ops(), 2);
        assert_eq!(throttle.current_bytes(), 300);

        // Drop first permit
        drop(permit1);
        assert_eq!(throttle.current_ops(), 1);
        assert_eq!(throttle.current_bytes(), 200);

        // Drop second permit
        drop(permit2);
        assert_eq!(throttle.current_ops(), 0);
        assert_eq!(throttle.current_bytes(), 0);
    }

    #[tokio::test]
    async fn test_throttle_ops_limit() {
        let throttle = Throttle::new(2, 1000);

        // Acquire two operations (at limit)
        let _permit1 = throttle.acquire(100).await;
        let _permit2 = throttle.acquire(100).await;

        // Try to acquire third - should fail
        let result = throttle.try_acquire(100);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_throttle_bytes_limit() {
        let throttle = Throttle::new(10, 1000);

        // Acquire 900 bytes
        let _permit1 = throttle.acquire(900).await;
        assert_eq!(throttle.current_bytes(), 900);

        // Try to acquire 200 bytes (would exceed limit) - should fail
        let result = throttle.try_acquire(200);
        assert!(result.is_none());

        // Try to acquire 100 bytes (within limit) - should succeed
        let permit2 = throttle.try_acquire(100);
        assert!(permit2.is_some());
        assert_eq!(throttle.current_bytes(), 1000);
    }

    #[tokio::test]
    async fn test_throttle_zero_bytes() {
        let throttle = Throttle::new(10, 1000);

        // Operations with zero bytes should not consume byte budget
        let _permit = throttle.acquire(0).await;
        assert_eq!(throttle.current_ops(), 1);
        assert_eq!(throttle.current_bytes(), 0);
    }
}
