//! Error types used by the msgr2 transport, framing, and handshake layers.
//!
//! This module defines the shared `Msgr2Error` enum returned by msgr2 components.
//! The variants preserve distinctions between wire-format issues, authentication
//! failures, I/O problems, and serialization errors so callers can react with
//! more context than a generic protocol string would provide.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Msgr2Error {
    #[error("Protocol error: {0}")]
    Protocol(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Denc error: {0}")]
    Denc(#[from] crate::RadosError),
    #[error("Authentication error: {0}")]
    Auth(String),
    #[error("CephX error: {0}")]
    CephX(#[from] crate::auth::error::CephXError),
    #[error("Crypto error: {0}")]
    Crypto(#[from] crate::msgr2::crypto::CryptoError),
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("Timeout error")]
    Timeout,
    #[error("Invalid data: {0}")]
    InvalidData(String),
    #[error("Serialization error")]
    Serialization,
    #[error("Deserialization error: {0}")]
    Deserialization(String),
    #[error("Compression error: {0}")]
    Compression(String),
    #[error("Configuration error: {0}")]
    ConfigError(String),
}

impl Msgr2Error {
    pub fn protocol_error(msg: &str) -> Self {
        Self::Protocol(msg.to_string())
    }

    pub fn auth_error(msg: &str) -> Self {
        Self::Auth(msg.to_string())
    }

    pub fn connection_error(msg: &str) -> Self {
        Self::Connection(msg.to_string())
    }

    pub fn invalid_data(msg: &str) -> Self {
        Self::InvalidData(msg.to_string())
    }

    pub fn compression_error(msg: &str) -> Self {
        Self::Compression(msg.to_string())
    }

    pub fn config_error(msg: &str) -> Self {
        Self::ConfigError(msg.to_string())
    }

    /// Check if this error represents a recoverable connection issue
    ///
    /// Recoverable errors are transient network issues that might succeed on retry,
    /// such as connection resets, broken pipes, or timeouts.
    pub fn is_recoverable(&self) -> bool {
        match self {
            // Network errors that can be retried
            Msgr2Error::Io(io_err) => matches!(
                io_err.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::Interrupted
            ),
            // Timeouts can be retried
            Msgr2Error::Timeout => true,
            // Connection errors might be retryable (depends on context)
            Msgr2Error::Connection(_) => true,
            // All other errors are not recoverable
            _ => false,
        }
    }

    /// Check if this is a fatal error that should not be retried
    ///
    /// Fatal errors include protocol violations, authentication failures,
    /// and configuration errors that won't be fixed by retrying.
    pub fn is_fatal(&self) -> bool {
        !self.is_recoverable()
    }

    /// Get a human-readable category for this error
    pub fn category(&self) -> &'static str {
        match self {
            Msgr2Error::Protocol(_) => "Protocol",
            Msgr2Error::Io(_) => "I/O",
            Msgr2Error::Denc(_) => "Encoding",
            Msgr2Error::Auth(_) | Msgr2Error::CephX(_) => "Authentication",
            Msgr2Error::Crypto(_) => "Crypto",
            Msgr2Error::Connection(_) => "Connection",
            Msgr2Error::Timeout => "Timeout",
            Msgr2Error::InvalidData(_) => "InvalidData",
            Msgr2Error::Serialization | Msgr2Error::Deserialization(_) => "Serialization",
            Msgr2Error::Compression(_) => "Compression",
            Msgr2Error::ConfigError(_) => "Configuration",
        }
    }
}

pub type Result<T> = std::result::Result<T, Msgr2Error>;
