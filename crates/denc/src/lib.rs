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

pub mod codec;
pub mod codec_error;
pub mod constants;
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

// Core encode/decode traits
pub use codec::{encode_with_capacity, Denc, FixedSize, VersionedEncode};

// Error type
pub use codec_error::CodecError;
pub use error::RadosError;

// Common Ceph wire types
pub use entity_addr::{EntityAddr, EntityAddrType, EntityAddrvec};
pub use hobject::{HObject, SNAP_DIR, SNAP_HEAD};
pub use ids::{Epoch, GlobalId, OsdId, PoolId};
pub use monmap::{ElectionStrategy, MonCephRelease, MonFeature, MonInfo, MonMap};
pub use pg_nls_response::{ListObjectImpl, PgNlsResponse};
pub use types::{EVersion, EntityName, EntityType, FsId, UTime, UuidD, Version};

// Feature flags
pub use features::{
    get_significant_features, has_feature, has_significant_feature, CephFeatures,
    SIGNIFICANT_FEATURES,
};

// Encoding metadata helpers
pub use encoding_metadata::{EncodingMetadata, HasEncodingMetadata};
pub use padding::Padding;
pub use zero_copy::ZeroCopyDencode;

// Re-export zerocopy crate for use in derived code
pub use zerocopy;

// Re-export derive macros
pub use denc_macros::{Denc, DencMut, StructVDenc, VersionedDenc, ZeroCopyDencode};
