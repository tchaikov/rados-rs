//! Ceph DENC encoding and decoding primitives for modern Rust monitor and OSD types.
//!
//! This crate provides the core [`Denc`] trait plus supporting types for encoding,
//! decoding, and version-gating Ceph wire structures. Most users consume the
//! re-exported traits, derive macros, and common Ceph data types directly from
//! this crate rather than importing individual modules.
//!
//! The main entry points are:
//! - [`Denc`] for general encode/decode support
//! - [`VersionedEncode`] for versioned wire formats
//! - derive macros such as [`Denc`] and [`ZeroCopyDencode`] for concise implementations
//!
//! The crate also re-exports common Ceph structures such as monitor maps,
//! entity addresses, object identifiers, and feature flags.

pub mod constants;
pub mod denc;
pub mod encoding_metadata;
pub mod entity_addr;
pub mod error;
pub mod features;
pub mod hobject;
pub mod ids;
pub mod macros;
pub mod monmap;
pub mod padding;
pub mod pg_nls_response;
pub mod types;
pub mod zero_copy;

pub use denc::{encode_with_capacity, Denc, FixedSize, VersionedEncode};
pub use encoding_metadata::*;
pub use entity_addr::*;
pub use error::*;
pub use features::*;
pub use hobject::*;
pub use monmap::*;
pub use padding::*;
pub use pg_nls_response::*;
pub use types::*;
pub use zero_copy::*;

// Re-export zerocopy crate for use in derived code
pub use zerocopy;

// Re-export derive macros
pub use denc_macros::{Denc, DencMut, StructVDenc, VersionedDenc, ZeroCopyDencode};
