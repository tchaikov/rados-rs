//! Error types for OSD client operations

use std::time::Duration;
use thiserror::Error;

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
    Msgr2(#[from] msgr2::Error),

    #[error("Denc error: {0}")]
    Denc(#[from] denc::RadosError),

    #[error("Authentication error: {0}")]
    Auth(String),

    #[error("Encoding error: {0}")]
    Encoding(String),

    #[error("Decoding error: {0}")]
    Decoding(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("MonClient error: {0}")]
    MonClient(String),

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
        denc::RadosError::Protocol(format!("OSDClient error: {}", e))
    }
}
