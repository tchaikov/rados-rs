//! Wire-format encode/decode error type.
//!
//! `CodecError` covers every failure that can occur inside a [`Denc`] implementation:
//! buffer underruns, unsupported struct versions, unrecognised discriminants, and
//! invalid UTF-8.  It is distinct from [`crate::denc::RadosError`] so that high-level
//! callers see a focused type for codec failures while the top-level error enum
//! stays lean.

use thiserror::Error;

/// Errors produced by [`crate::Denc`] encode and decode implementations.
#[derive(Error, Debug)]
pub enum CodecError {
    /// The input buffer contained fewer bytes than needed to decode the value.
    #[error("Insufficient data: need {needed} bytes, have {available}")]
    InsufficientData { needed: usize, available: usize },

    /// The encoded struct version is older than the minimum this decoder supports.
    #[error("{type_name} version {got} not supported (minimum: {min}, requires {ceph_release})")]
    VersionTooOld {
        got: u8,
        min: u8,
        type_name: &'static str,
        ceph_release: &'static str,
    },

    /// An enum discriminant or tag value was not recognised by this decoder.
    #[error("Unknown {type_name} value: {value}")]
    UnknownValue { type_name: &'static str, value: u64 },

    /// A byte sequence could not be decoded as valid UTF-8.
    #[error("Invalid UTF-8: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),

    /// A fixed-size array could not be constructed because the element count was wrong.
    #[error("Array size mismatch: expected {expected}, got {got}")]
    ArraySizeMismatch { expected: usize, got: usize },

    /// The encoded struct version exceeds the maximum this decoder supports.
    #[error("{type_name} struct version {got} too new (max supported: {max})")]
    VersionTooNew {
        got: u8,
        max: u8,
        type_name: &'static str,
    },

    /// The compat version field is greater than the struct version, which is invalid.
    #[error("{type_name} invalid header: compat {compat} > version {version}")]
    InvalidVersionHeader {
        type_name: &'static str,
        compat: u8,
        version: u8,
    },
}
