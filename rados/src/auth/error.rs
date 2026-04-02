//! Error types for CephX authentication

use thiserror::Error;

/// CephX authentication errors
#[derive(Error, Debug, Clone)]
pub enum CephXError {
    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("Invalid key format: {0}")]
    InvalidKey(String),

    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("Cryptographic error: {0}")]
    CryptographicError(String),

    #[error("Encoding error: {0}")]
    EncodingError(String),

    #[error("Time error: {0}")]
    TimeError(String),
}

/// Result type for CephX operations
pub type Result<T> = std::result::Result<T, CephXError>;

impl From<crate::RadosError> for CephXError {
    fn from(err: crate::RadosError) -> Self {
        CephXError::EncodingError(err.to_string())
    }
}
