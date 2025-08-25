use thiserror::Error;
use std::io;

pub type Result<T> = std::result::Result<T, Error>;
pub type RadosError = Error; // Type alias for compatibility

#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    
    #[error("Connection error: {0}")]
    Connection(String),
    
    #[error("Protocol error: {0}")]
    Protocol(String),
    
    #[error("Authentication error: {0}")]
    Authentication(String),
    
    #[error("Serialization error")]
    Serialization,
    
    #[error("Deserialization error: {0}")]
    Deserialization(String),
    
    #[error("Timeout error")]
    Timeout,
    
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
    
    #[error("Feature not supported: {0}")]
    Unsupported(String),
    
    #[error("Buffer too small, need at least {needed} bytes")]
    BufferTooSmall { needed: usize },
    
    #[error("Invalid entity address: {0}")]
    InvalidEntityAddr(String),
    
    #[error("Invalid message type: {0}")]
    InvalidMessageType(u16),
    
    #[error("Handshake failed: {0}")]
    HandshakeFailed(String),
}

impl Error {
    pub fn protocol_error(msg: impl Into<String>) -> Self {
        Self::Protocol(msg.into())
    }
    
    pub fn connection_error(msg: impl Into<String>) -> Self {
        Self::Connection(msg.into())
    }
    
    pub fn auth_error(msg: impl Into<String>) -> Self {
        Self::Authentication(msg.into())
    }
}

// For backwards compatibility
impl Error {
    pub fn ProtocolError(msg: String) -> Self {
        Self::Protocol(msg)
    }
}