//! Common types used throughout the MonClient

use bytes::Bytes;

/// Result of a monitor command
#[derive(Debug, Clone)]
pub struct CommandResult {
    /// Return code (0 = success)
    pub retval: i32,
    /// String output
    pub outs: String,
    /// Binary output
    pub outbl: Bytes,
}

impl CommandResult {
    pub fn new(retval: i32, outs: String, outbl: Bytes) -> Self {
        Self {
            retval,
            outs,
            outbl,
        }
    }
}
