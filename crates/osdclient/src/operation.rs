//! Operation builders for type-safe operation construction

use crate::types::OSDOp;
use bytes::Bytes;

/// Builder for read operations
pub struct ReadOp {
    offset: u64,
    length: u64,
}

impl ReadOp {
    pub fn new(offset: u64, length: u64) -> Self {
        Self { offset, length }
    }

    pub fn build(self) -> OSDOp {
        OSDOp::read(self.offset, self.length)
    }
}

/// Builder for write operations
pub struct WriteOp {
    offset: u64,
    data: Bytes,
}

impl WriteOp {
    pub fn new(offset: u64, data: Bytes) -> Self {
        Self { offset, data }
    }

    pub fn build(self) -> OSDOp {
        OSDOp::write(self.offset, self.data)
    }
}

/// Builder for stat operations
pub struct StatOp;

impl StatOp {
    pub fn new() -> Self {
        Self
    }

    pub fn build(self) -> OSDOp {
        OSDOp::stat()
    }
}

impl Default for StatOp {
    fn default() -> Self {
        Self::new()
    }
}
