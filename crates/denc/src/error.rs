use thiserror::Error;

/// Error types for RADOS operations
#[derive(Debug, Error)]
pub enum RadosError {
    /// I/O error occurred during encoding/decoding
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Buffer too short for operation
    #[error("Buffer too short: expected at least {expected} bytes, got {actual}")]
    BufferTooShort { expected: usize, actual: usize },

    /// Invalid data format
    #[error("Invalid data format: {0}")]
    InvalidFormat(String),

    /// Version mismatch
    #[error("Version mismatch: expected {expected}, got {actual}")]
    VersionMismatch { expected: u8, actual: u8 },
}
