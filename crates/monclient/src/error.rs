//! Error types for MonClient

use thiserror::Error;

pub type Result<T> = std::result::Result<T, MonClientError>;

#[derive(Debug, Error)]
pub enum MonClientError {
    #[error("Not connected to any monitor")]
    NotConnected,

    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("Timeout waiting for authentication to complete")]
    AuthenticationTimeout,

    #[error("Command failed with code {code}: {message}")]
    CommandFailed { code: i32, message: String },

    #[error("Timeout waiting for response")]
    Timeout,

    #[error("Monitor unavailable")]
    MonitorUnavailable,

    #[error("Invalid monmap: {0}")]
    InvalidMonMap(String),

    #[error("Invalid monitor rank: {0}")]
    InvalidMonitorRank(usize),

    #[error("Already initialized")]
    AlreadyInitialized,

    #[error("Not initialized")]
    NotInitialized,

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Shutting down")]
    ShuttingDown,

    #[error("Message error: {0}")]
    MessageError(#[from] msgr2::error::Error),

    #[error("Encoding error")]
    EncodingError,

    #[error("Decoding error: {0}")]
    DecodingError(String),

    #[error("Authentication error: {0}")]
    AuthError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Rados error: {0}")]
    RadosError(#[from] denc::error::RadosError),

    #[error("Other error: {0}")]
    Other(String),
}

impl From<String> for MonClientError {
    fn from(s: String) -> Self {
        MonClientError::Other(s)
    }
}

impl From<&str> for MonClientError {
    fn from(s: &str) -> Self {
        MonClientError::Other(s.to_string())
    }
}

impl From<MonClientError> for denc::RadosError {
    fn from(e: MonClientError) -> Self {
        match e {
            MonClientError::RadosError(error) => error,
            MonClientError::IoError(error) => error.into(),
            other => denc::RadosError::Protocol(format!("MonClient error: {}", other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MonClientError;

    #[test]
    fn converting_monclient_rados_error_preserves_variant() {
        let error = MonClientError::RadosError(denc::RadosError::InvalidData("bad monmap".into()));

        let converted: denc::RadosError = error.into();

        assert!(
            matches!(converted, denc::RadosError::InvalidData(message) if message == "bad monmap")
        );
    }

    #[test]
    fn converting_monclient_io_error_preserves_variant() {
        let error = MonClientError::IoError(std::io::Error::other("socket closed"));

        let converted: denc::RadosError = error.into();

        assert!(
            matches!(converted, denc::RadosError::Io(io_error) if io_error.to_string() == "socket closed")
        );
    }
}
