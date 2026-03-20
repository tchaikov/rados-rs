//! Shared error types for DENC encoding, decoding, and protocol validation.
//!
//! This module defines the common `RadosError` enum used across the workspace's
//! DENC implementations. The variants cover wire-format problems, invalid data,
//! I/O failures, and higher-level protocol issues so callers can propagate
//! structured failures without flattening them into strings prematurely.

use crate::denc::CodecError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RadosError {
    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Compression error: {0}")]
    Compression(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid data: {0}")]
    InvalidData(String),

    #[error("Encoding/Decoding error: {0}")]
    Denc(String),

    /// A wire-format encode or decode failure from a [`Denc`] implementation.
    #[error(transparent)]
    Codec(#[from] CodecError),
}
