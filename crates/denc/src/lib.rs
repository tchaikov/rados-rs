//! Denc: Encoding and decoding library for Ceph data structures
//! 
//! This crate provides the core traits and implementations for encoding
//! and decoding Ceph protocol data structures.

mod error;
mod traits;

pub use error::RadosError;
pub use traits::{Denc, FixedSize, VersionedEncode};

pub type Result<T> = std::result::Result<T, RadosError>;
