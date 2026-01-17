use thiserror::Error;

#[derive(Error, Debug)]
pub enum RadosError {
    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid data: {0}")]
    InvalidData(String),

    #[error("Encoding/Decoding error: {0}")]
    Denc(String),
}
