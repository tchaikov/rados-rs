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
pub mod types;
pub mod zero_copy;

// Core encode/decode traits
pub use codec::{Denc, FixedSize, VersionedEncode, encode_with_capacity};

// Error type
pub use codec_error::CodecError;
pub use error::RadosError;

// Common Ceph wire types
pub use entity_addr::{EntityAddr, EntityAddrType, EntityAddrvec};
pub use hobject::{HObject, SNAP_DIR, SNAP_HEAD};
pub use ids::{Epoch, GlobalId, OsdId, PoolId};
pub use monmap::{ElectionStrategy, MonCephRelease, MonFeature, MonInfo, MonMap};
pub use types::{EVersion, EntityName, EntityType, FsId, UTime, UuidD, Version};

// Feature flags
pub use features::{CephFeatures, SIGNIFICANT_FEATURES, has_significant_feature};

// Encoding metadata helpers
pub use encoding_metadata::{EncodingMetadata, HasEncodingMetadata};
pub use zero_copy::ZeroCopyDencode;

// Re-export zerocopy crate for use in derived code
pub use zerocopy;

// Re-export derive macros
pub use rados_denc_macros::{Denc, StructVDenc, VersionedDenc, ZeroCopyDencode};

pub use crate::{
    check_min_version, decode_if_version, impl_denc_for_versioned, impl_denc_u8_enum,
    mark_feature_dependent_encoding, mark_simple_encoding, mark_versioned_encoding,
};
