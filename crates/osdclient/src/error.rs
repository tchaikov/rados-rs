//! Error types for OSD client operations

use std::time::Duration;
use thiserror::Error;

// Standard errno constants (as negative values, matching Ceph conventions)
pub const ENOENT: i32 = -2; // No such file or directory
pub const EAGAIN: i32 = -11; // Try again
pub const ENOSPC: i32 = -28; // No space left on device

/// Errors that can occur during OSD client operations
#[derive(Debug, Error)]
pub enum OSDClientError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("OSD error {code}: {message}")]
    OSDError { code: i32, message: String },

    #[error("Operation timeout after {0:?}")]
    Timeout(Duration),

    #[error("Object not found: {0}")]
    ObjectNotFound(String),

    #[error("Pool not found: {0}")]
    PoolNotFound(u64),

    #[error("No OSDs available")]
    NoOSDs,

    #[error("{0}")]
    Other(String),

    #[error("Msgr2 error: {0}")]
    Msgr2(#[from] msgr2::Msgr2Error),

    #[error("Denc error: {0}")]
    Denc(#[from] denc::RadosError),

    #[error("Authentication error: {0}")]
    Auth(String),

    #[error("Encoding error: {0}")]
    Encoding(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("MonClient error: {0}")]
    MonClient(#[from] monclient::MonClientError),

    #[error("CRUSH error: {0}")]
    Crush(String),

    #[error("OSD backoff: {0}")]
    Backoff(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

/// Result type alias for OSD client operations
pub type Result<T> = std::result::Result<T, OSDClientError>;

impl From<OSDClientError> for denc::RadosError {
    fn from(e: OSDClientError) -> Self {
        match e {
            OSDClientError::Denc(error) => error,
            OSDClientError::MonClient(error) => error.into(),
            other => denc::RadosError::Protocol(format!("OSDClient error: {}", other)),
        }
    }
}

/// Error category for retry decision making
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    /// Can retry immediately
    Transient,
    /// Should not retry
    Permanent,
    /// Wait for new OSDMap
    NeedsMapUpdate,
    /// Retry after delay
    RetriableWithBackoff,
}

impl OSDClientError {
    /// Categorize error for retry decisions
    pub fn category(&self) -> ErrorCategory {
        match self {
            Self::Timeout(_) | Self::Connection(_) => ErrorCategory::Transient,
            Self::ObjectNotFound(_) | Self::PoolNotFound(_) | Self::Auth(_) => {
                ErrorCategory::Permanent
            }
            Self::Backoff(_) => ErrorCategory::RetriableWithBackoff,
            Self::OSDError { code, .. } => match *code {
                ENOENT => ErrorCategory::Permanent,
                EAGAIN => ErrorCategory::NeedsMapUpdate,
                ENOSPC => ErrorCategory::RetriableWithBackoff,
                _ => ErrorCategory::Transient,
            },
            _ => ErrorCategory::Permanent,
        }
    }

    /// Check if error is retriable
    pub fn is_retriable(&self) -> bool {
        !matches!(self.category(), ErrorCategory::Permanent)
    }
}

#[cfg(test)]
mod tests {
    use super::OSDClientError;

    #[test]
    fn converting_osdclient_denc_error_preserves_variant() {
        let error = OSDClientError::Denc(denc::RadosError::InvalidData("bad pg".into()));

        let converted: denc::RadosError = error.into();

        assert!(matches!(converted, denc::RadosError::InvalidData(message) if message == "bad pg"));
    }

    #[test]
    fn converting_osdclient_monclient_error_preserves_nested_rados_error() {
        let error = OSDClientError::MonClient(monclient::MonClientError::RadosError(
            denc::RadosError::InvalidData("bad mon command".into()),
        ));

        let converted: denc::RadosError = error.into();

        assert!(
            matches!(converted, denc::RadosError::InvalidData(message) if message == "bad mon command")
        );
    }
}
