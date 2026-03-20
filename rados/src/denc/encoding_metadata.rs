//! Compile-time encoding metadata system
//!
//! This module provides macros to declaratively mark types with their encoding properties:
//! - Whether they use versioned encoding (ENCODE_START in C++)
//! - Whether their encoding depends on feature flags (WRITE_CLASS_ENCODER_FEATURES in C++)
//!
//! # Examples
//!
//! ```rust
//! use crate::denc::{
//!     mark_feature_dependent_encoding, mark_simple_encoding, mark_versioned_encoding,
//! };
//!
//! struct PgId;
//! struct PoolSnapInfo;
//! struct PgPool;
//! struct EntityAddr;
//!
//! // Simple type: no versioning, no feature dependency
//! mark_simple_encoding!(PgId);
//!
//! // Versioned type: uses ENCODE_START/DECODE_START, but encoding doesn't depend on features
//! mark_versioned_encoding!(PoolSnapInfo);
//!
//! // Feature-dependent type: uses versioning AND encoding depends on features
//! mark_feature_dependent_encoding!(PgPool);
//!
//! // Versioned type with a fixed modern encode contract
//! mark_versioned_encoding!(EntityAddr);
//! ```

/// Compile-time encoding properties of a type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncodingMetadata {
    /// Does this type use ENCODE_START/DECODE_START wrapping?
    pub uses_versioning: bool,

    /// Does the encoding format change based on feature flags?
    /// Corresponds to WRITE_CLASS_ENCODER_FEATURES in C++
    pub feature_dependent: bool,
}

impl EncodingMetadata {
    pub const SIMPLE: Self = Self {
        uses_versioning: false,
        feature_dependent: false,
    };

    pub const VERSIONED: Self = Self {
        uses_versioning: true,
        feature_dependent: false,
    };

    pub const FEATURE_DEPENDENT: Self = Self {
        uses_versioning: true,
        feature_dependent: true,
    };
}

/// Trait for types that can report their encoding metadata at compile time
pub trait HasEncodingMetadata {
    /// Get the encoding metadata for this type
    fn encoding_metadata() -> EncodingMetadata;
}

// ============= Declarative Macros =============

/// Mark a type as using simple encoding (no versioning, no feature dependency)
///
/// # Example
/// ```rust
/// use crate::denc::mark_simple_encoding;
///
/// struct PgId;
/// struct UTime;
/// struct UuidD;
///
/// mark_simple_encoding!(PgId);
/// mark_simple_encoding!(UTime);
/// mark_simple_encoding!(UuidD);
/// ```
#[macro_export]
macro_rules! mark_simple_encoding {
    ($type:ty) => {
        impl $crate::encoding_metadata::HasEncodingMetadata for $type {
            fn encoding_metadata() -> $crate::encoding_metadata::EncodingMetadata {
                $crate::encoding_metadata::EncodingMetadata::SIMPLE
            }
        }
    };
}

/// Mark a type as using versioned encoding (ENCODE_START/DECODE_START)
/// but encoding does NOT depend on features
///
/// # Example
/// ```rust
/// use crate::denc::mark_versioned_encoding;
///
/// struct PoolSnapInfo;
/// struct PgMergeMeta;
/// struct OsdInfo;
///
/// mark_versioned_encoding!(PoolSnapInfo);
/// mark_versioned_encoding!(PgMergeMeta);
/// mark_versioned_encoding!(OsdInfo);
/// ```
#[macro_export]
macro_rules! mark_versioned_encoding {
    ($type:ty) => {
        impl $crate::encoding_metadata::HasEncodingMetadata for $type {
            fn encoding_metadata() -> $crate::encoding_metadata::EncodingMetadata {
                $crate::encoding_metadata::EncodingMetadata::VERSIONED
            }
        }
    };
}

/// Mark a type as feature-dependent: uses versioned encoding AND
/// the encoding format/version changes based on feature flags
///
/// Corresponds to types marked with WRITE_CLASS_ENCODER_FEATURES in C++
///
/// # Example
/// ```rust
/// use crate::denc::mark_feature_dependent_encoding;
///
/// struct PgPool;
/// struct OSDMap;
///
/// mark_feature_dependent_encoding!(PgPool);    // Version depends on SERVER_TENTACLE, etc.
/// mark_feature_dependent_encoding!(OSDMap);    // Version depends on multiple server feature masks
/// ```
#[macro_export]
macro_rules! mark_feature_dependent_encoding {
    ($type:ty) => {
        impl $crate::encoding_metadata::HasEncodingMetadata for $type {
            fn encoding_metadata() -> $crate::encoding_metadata::EncodingMetadata {
                $crate::encoding_metadata::EncodingMetadata::FEATURE_DEPENDENT
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    struct SimpleType;
    struct VersionedType;
    struct FeatureDependentType;

    mark_simple_encoding!(SimpleType);
    mark_versioned_encoding!(VersionedType);
    mark_feature_dependent_encoding!(FeatureDependentType);

    #[test]
    fn test_simple_encoding_metadata() {
        let meta = SimpleType::encoding_metadata();
        assert!(!meta.uses_versioning);
        assert!(!meta.feature_dependent);
    }

    #[test]
    fn test_versioned_encoding_metadata() {
        let meta = VersionedType::encoding_metadata();
        assert!(meta.uses_versioning);
        assert!(!meta.feature_dependent);
    }

    #[test]
    fn test_feature_dependent_encoding_metadata() {
        let meta = FeatureDependentType::encoding_metadata();
        assert!(meta.uses_versioning);
        assert!(meta.feature_dependent);
    }
}
