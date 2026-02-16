//! Operation builders for type-safe operation construction
//!
//! This module provides ergonomic builders for constructing OSD operations.
//! The main entry point is [`OpBuilder`], which allows chaining multiple
//! operations together with optional flags and settings.
//!
//! # Example
//!
//! ```ignore
//! use osdclient::operation::OpBuilder;
//!
//! // Simple read
//! let op = OpBuilder::new().read(0, 4096).build();
//!
//! // Write with flags
//! let op = OpBuilder::new()
//!     .write(0, data)
//!     .priority(10)
//!     .build();
//!
//! // Multiple operations in one request
//! let op = OpBuilder::new()
//!     .stat()
//!     .read(0, 4096)
//!     .balance_reads()
//!     .build();
//! ```

use crate::types::{OSDOp, OsdOpFlags};
use bytes::Bytes;
use std::time::Duration;

/// A built operation ready for execution
#[derive(Debug, Clone)]
pub struct BuiltOp {
    /// The operations to execute
    pub ops: Vec<OSDOp>,
    /// Operation flags
    pub flags: OsdOpFlags,
    /// Priority (-1 to use default)
    pub priority: i32,
    /// Optional timeout override
    pub timeout: Option<Duration>,
}

impl BuiltOp {
    /// Check if this is a read operation
    pub fn is_read(&self) -> bool {
        self.flags.contains(OsdOpFlags::READ)
    }

    /// Check if this is a write operation
    pub fn is_write(&self) -> bool {
        self.flags.contains(OsdOpFlags::WRITE)
    }

    /// Get the operations
    pub fn ops(&self) -> &[OSDOp] {
        &self.ops
    }

    /// Consume and return the operations
    pub fn into_ops(self) -> Vec<OSDOp> {
        self.ops
    }
}

/// Builder for constructing OSD operations
///
/// Provides a fluent API for building single or compound operations
/// with optional flags and settings.
///
/// # Example
///
/// ```ignore
/// // Read with balance_reads flag
/// let op = OpBuilder::new()
///     .read(0, 4096)
///     .balance_reads()
///     .build();
///
/// // Compound operation: stat + read
/// let op = OpBuilder::new()
///     .stat()
///     .read(0, 4096)
///     .build();
/// ```
#[derive(Debug, Clone, Default)]
pub struct OpBuilder {
    ops: Vec<OSDOp>,
    flags: OsdOpFlags,
    priority: i32,
    timeout: Option<Duration>,
}

impl OpBuilder {
    /// Create a new operation builder
    pub fn new() -> Self {
        Self {
            ops: Vec::new(),
            flags: OsdOpFlags::empty(),
            priority: -1, // Use default priority
            timeout: None,
        }
    }

    /// Add a read operation
    ///
    /// # Arguments
    /// * `offset` - Starting offset in the object
    /// * `length` - Number of bytes to read
    pub fn read(mut self, offset: u64, length: u64) -> Self {
        self.ops.push(OSDOp::read(offset, length));
        self.flags |= OsdOpFlags::READ;
        self
    }

    /// Add a write operation
    ///
    /// # Arguments
    /// * `offset` - Starting offset in the object
    /// * `data` - Data to write
    pub fn write(mut self, offset: u64, data: impl Into<Bytes>) -> Self {
        self.ops.push(OSDOp::write(offset, data.into()));
        self.flags |= OsdOpFlags::WRITE;
        self
    }

    /// Add a write_full operation (overwrites entire object)
    ///
    /// # Arguments
    /// * `data` - Data to write
    pub fn write_full(mut self, data: impl Into<Bytes>) -> Self {
        self.ops.push(OSDOp::write_full(data.into()));
        self.flags |= OsdOpFlags::WRITE;
        self
    }

    /// Add a stat operation
    pub fn stat(mut self) -> Self {
        self.ops.push(OSDOp::stat());
        self.flags |= OsdOpFlags::READ;
        self
    }

    /// Add a delete operation
    pub fn delete(mut self) -> Self {
        self.ops.push(OSDOp::delete());
        self.flags |= OsdOpFlags::WRITE;
        self
    }

    /// Add a sparse read operation
    ///
    /// # Arguments
    /// * `offset` - Starting offset in the object
    /// * `length` - Maximum number of bytes to read
    pub fn sparse_read(mut self, offset: u64, length: u64) -> Self {
        self.ops.push(OSDOp::sparse_read(offset, length));
        self.flags |= OsdOpFlags::READ;
        self
    }

    /// Enable balance_reads flag (distribute reads among replicas)
    pub fn balance_reads(mut self) -> Self {
        self.flags |= OsdOpFlags::BALANCE_READS;
        self
    }

    /// Enable localize_reads flag (prefer nearby replicas)
    pub fn localize_reads(mut self) -> Self {
        self.flags |= OsdOpFlags::LOCALIZE_READS;
        self
    }

    /// Ignore cache tier logic
    pub fn ignore_cache(mut self) -> Self {
        self.flags |= OsdOpFlags::IGNORE_CACHE;
        self
    }

    /// Ignore pool overlay
    pub fn ignore_overlay(mut self) -> Self {
        self.flags |= OsdOpFlags::IGNORE_OVERLAY;
        self
    }

    /// Set operation priority
    ///
    /// Higher values = higher priority. Default is -1 (use system default).
    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Set operation timeout
    ///
    /// Overrides the default client timeout for this operation.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Add custom flags
    pub fn flags(mut self, flags: OsdOpFlags) -> Self {
        self.flags |= flags;
        self
    }

    /// Add a raw OSDOp
    ///
    /// For advanced use cases where you need to add a pre-constructed operation.
    pub fn op(mut self, op: OSDOp) -> Self {
        // Infer flags from operation
        if op.op.is_read() {
            self.flags |= OsdOpFlags::READ;
        }
        if op.op.is_write() {
            self.flags |= OsdOpFlags::WRITE;
        }
        self.ops.push(op);
        self
    }

    /// Build the operation
    pub fn build(self) -> BuiltOp {
        BuiltOp {
            ops: self.ops,
            flags: self.flags,
            priority: self.priority,
            timeout: self.timeout,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_opbuilder_read() {
        let op = OpBuilder::new().read(0, 4096).build();

        assert_eq!(op.ops.len(), 1);
        assert!(op.is_read());
        assert!(!op.is_write());
        assert_eq!(op.priority, -1);
        assert!(op.timeout.is_none());
    }

    #[test]
    fn test_opbuilder_write() {
        let data = Bytes::from(vec![1, 2, 3, 4]);
        let op = OpBuilder::new().write(0, data).build();

        assert_eq!(op.ops.len(), 1);
        assert!(!op.is_read());
        assert!(op.is_write());
    }

    #[test]
    fn test_opbuilder_write_full() {
        let data = Bytes::from(vec![1, 2, 3, 4]);
        let op = OpBuilder::new().write_full(data).build();

        assert_eq!(op.ops.len(), 1);
        assert!(op.is_write());
    }

    #[test]
    fn test_opbuilder_stat() {
        let op = OpBuilder::new().stat().build();

        assert_eq!(op.ops.len(), 1);
        assert!(op.is_read());
    }

    #[test]
    fn test_opbuilder_delete() {
        let op = OpBuilder::new().delete().build();

        assert_eq!(op.ops.len(), 1);
        assert!(op.is_write());
    }

    #[test]
    fn test_opbuilder_sparse_read() {
        let op = OpBuilder::new().sparse_read(1024, 4096).build();

        assert_eq!(op.ops.len(), 1);
        assert!(op.is_read());
    }

    #[test]
    fn test_opbuilder_compound() {
        let op = OpBuilder::new().stat().read(0, 4096).build();

        assert_eq!(op.ops.len(), 2);
        assert!(op.is_read());
    }

    #[test]
    fn test_opbuilder_balance_reads() {
        let op = OpBuilder::new().read(0, 4096).balance_reads().build();

        assert!(op.flags.contains(OsdOpFlags::BALANCE_READS));
        assert!(op.flags.contains(OsdOpFlags::READ));
    }

    #[test]
    fn test_opbuilder_localize_reads() {
        let op = OpBuilder::new().read(0, 4096).localize_reads().build();

        assert!(op.flags.contains(OsdOpFlags::LOCALIZE_READS));
    }

    #[test]
    fn test_opbuilder_priority() {
        let op = OpBuilder::new().read(0, 4096).priority(10).build();

        assert_eq!(op.priority, 10);
    }

    #[test]
    fn test_opbuilder_timeout() {
        let op = OpBuilder::new()
            .read(0, 4096)
            .timeout(Duration::from_secs(30))
            .build();

        assert_eq!(op.timeout, Some(Duration::from_secs(30)));
    }

    #[test]
    fn test_opbuilder_chained() {
        let data = Bytes::from(vec![1, 2, 3, 4]);
        let op = OpBuilder::new()
            .write(0, data)
            .priority(5)
            .timeout(Duration::from_secs(10))
            .ignore_cache()
            .build();

        assert!(op.is_write());
        assert_eq!(op.priority, 5);
        assert_eq!(op.timeout, Some(Duration::from_secs(10)));
        assert!(op.flags.contains(OsdOpFlags::IGNORE_CACHE));
    }

    #[test]
    fn test_opbuilder_into_ops() {
        let op = OpBuilder::new().read(0, 4096).stat().build();

        let ops = op.into_ops();
        assert_eq!(ops.len(), 2);
    }

    #[test]
    fn test_opbuilder_custom_op() {
        let custom = OSDOp::read(100, 200);
        let op = OpBuilder::new().op(custom).build();

        assert_eq!(op.ops.len(), 1);
        assert!(op.is_read());
    }
}
