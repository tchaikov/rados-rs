use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Protocol error: {0}")]
    Protocol(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Denc error: {0}")]
    Denc(#[from] denc::RadosError),
    #[error("Authentication error: {0}")]
    Auth(String),
    #[error("CephX error: {0}")]
    CephX(#[from] auth::error::CephXError),
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
    #[error("Encode error: {0}")]
    Encode(#[from] denc::zerocopy::EncodeError),
    #[error("Decode error: {0}")]
    Decode(#[from] denc::zerocopy::DecodeError),
    #[error("Compression error: {0}")]
    Compression(String),
}

impl Error {
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
}

pub type Result<T> = std::result::Result<T, Error>;
