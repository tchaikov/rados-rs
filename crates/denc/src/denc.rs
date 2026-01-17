use crate::error::RadosError;
use bytes::{Buf, BufMut, Bytes, BytesMut};

// ============= Core Encoding Traits =============

/// Context for encoding/decoding operations
#[derive(Clone, Copy, Debug, Default)]
pub struct EncodingContext {
    pub features: u64,
}

impl EncodingContext {
    pub fn new() -> Self {
        Self { features: 0 }
    }

    pub fn with_features(features: u64) -> Self {
        Self { features }
    }
}

// ============= Base Trait =============

/// Core trait that all encodable types must implement
///
/// # Compile-Time Properties
/// - `USES_VERSIONING`: Does this type use ENCODE_START/DECODE_START wrapping?
/// - `FEATURE_DEPENDENT`: Does the encoding format change based on feature flags?
///
/// # Examples
/// - `PgId`: USES_VERSIONING=true (version byte), FEATURE_DEPENDENT=false
/// - `PoolSnapInfo`: USES_VERSIONING=true, FEATURE_DEPENDENT=false (always version 2)
/// - `PgPool`: USES_VERSIONING=true, FEATURE_DEPENDENT=true (version depends on features)
/// - `EntityAddr`: USES_VERSIONING=true, FEATURE_DEPENDENT=true (MSG_ADDR2 feature)
pub trait Denc: Sized {
    /// Does this type use versioned encoding? (ENCODE_START in C++)
    const USES_VERSIONING: bool = false;

    /// Does encoding format depend on feature flags? (WRITE_CLASS_ENCODER_FEATURES in C++)
    const FEATURE_DEPENDENT: bool = false;

    /// Encode with optional features
    fn encode(&self, features: u64) -> Result<Bytes, RadosError>;

    /// Decode from bytes
    fn decode(bytes: &mut Bytes) -> Result<Self, RadosError>;

    /// Optional: provide encoded size hint
    fn encoded_size(&self) -> Option<usize> {
        None
    }
}

// ============= Versioned Encoding =============

/// Trait for types that add version metadata during encoding
/// Follows Ceph's ENCODE_START/DECODE_START pattern
///
/// # C++ Equivalents
/// - `ENCODE_START(version, compat, bl)` → `encode_versioned()`
/// - `DECODE_START(version, bl)` → `decode_versioned()`
/// - `WRITE_CLASS_ENCODER` → version fixed at compile time
/// - `WRITE_CLASS_ENCODER_FEATURES` → version may depend on features
///
/// # Compile-Time Properties
/// Types implementing this trait automatically have `USES_VERSIONING = true`.
/// Set `FEATURE_DEPENDENT = true` if version/encoding depends on features.
pub trait VersionedEncode: Sized {
    /// Does encoding format depend on feature flags?
    /// Override this if the version or encoding changes based on features.
    /// Example: PgPool (version depends on SERVER_TENTACLE, etc.)
    const FEATURE_DEPENDENT: bool = false;

    /// Get the current version to encode with (may depend on features)
    fn encoding_version(&self, features: u64) -> u8;

    /// Get the minimum compatible version
    fn compat_version(&self, features: u64) -> u8;

    /// Encode the actual content (without version wrapper)
    fn encode_content(&self, features: u64, version: u8) -> Result<Bytes, RadosError>;

    /// Decode content for a specific version
    fn decode_content(
        bytes: &mut Bytes,
        version: u8,
        compat_version: u8,
    ) -> Result<Self, RadosError>;

    /// Encode with version metadata (ENCODE_START pattern)
    fn encode_versioned(&self, features: u64) -> Result<Bytes, RadosError> {
        let version = self.encoding_version(features);
        let compat = self.compat_version(features);
        let content = self.encode_content(features, version)?;

        let mut buf = BytesMut::with_capacity(6 + content.len());
        buf.put_u8(version);
        buf.put_u8(compat);
        buf.put_u32_le(content.len() as u32);
        buf.extend_from_slice(&content);

        Ok(buf.freeze())
    }

    /// Decode with version handling (DECODE_START pattern)
    fn decode_versioned(bytes: &mut Bytes) -> Result<Self, RadosError> {
        if bytes.len() < 6 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for version header".to_string(),
            ));
        }

        let struct_v = bytes.get_u8();
        let struct_compat = bytes.get_u8();
        let struct_len = bytes.get_u32_le() as usize;

        if bytes.len() < struct_len {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes: need {}, have {}",
                struct_len,
                bytes.len()
            )));
        }

        // Extract content with exact length
        let mut content = bytes.split_to(struct_len);

        // Decode based on version
        let result = Self::decode_content(&mut content, struct_v, struct_compat)?;

        // DECODE_FINISH: consume any remaining bytes (forward compatibility)
        // This allows newer encoders to add fields that older decoders ignore
        if content.remaining() > 0 {
            // Skip remaining bytes for forward compatibility
        }

        Ok(result)
    }
}

// Default implementation for VersionedEncode types
impl<T: VersionedEncode> Denc for T {
    // Override compile-time properties
    const USES_VERSIONING: bool = true;
    const FEATURE_DEPENDENT: bool = T::FEATURE_DEPENDENT;

    fn encode(&self, features: u64) -> Result<Bytes, RadosError> {
        self.encode_versioned(features)
    }

    fn decode(bytes: &mut Bytes) -> Result<Self, RadosError> {
        Self::decode_versioned(bytes)
    }
}

// ============= Simple Types (No versioning) =============

/// Trait for types that don't need versioning metadata
pub trait SimpleEncode {
    fn encode_simple(&self) -> Result<Bytes, RadosError>;
    fn decode_simple(bytes: &mut Bytes) -> Result<Self, RadosError>
    where
        Self: Sized;
}

// ============= Feature-Dependent Types =============

/// Trait for types whose encoding format changes based on features
/// but don't add version metadata
pub trait FeatureEncode: Denc {
    fn encode_with_features(&self, features: u64) -> Result<Bytes, RadosError>;
    fn decode_with_features(bytes: &mut Bytes, features: u64) -> Result<Self, RadosError>;
}

// ============= Basic Type Implementations =============

macro_rules! impl_simple_encode {
    ($type:ty, $put_method:ident, $get_method:ident, $size:expr) => {
        impl SimpleEncode for $type {
            fn encode_simple(&self) -> Result<Bytes, RadosError> {
                let mut buf = BytesMut::with_capacity($size);
                buf.$put_method(*self);
                Ok(buf.freeze())
            }

            fn decode_simple(bytes: &mut Bytes) -> Result<Self, RadosError> {
                if bytes.len() < $size {
                    return Err(RadosError::Protocol(format!(
                        "Not enough bytes for {}",
                        stringify!($type)
                    )));
                }
                Ok(bytes.$get_method())
            }
        }

        impl Denc for $type {
            fn encode(&self, _features: u64) -> Result<Bytes, RadosError> {
                SimpleEncode::encode_simple(self)
            }

            fn decode(bytes: &mut Bytes) -> Result<Self, RadosError> {
                Self::decode_simple(bytes)
            }

            fn encoded_size(&self) -> Option<usize> {
                Some($size)
            }
        }
    };
}

impl_simple_encode!(u8, put_u8, get_u8, 1);
impl_simple_encode!(u16, put_u16_le, get_u16_le, 2);
impl_simple_encode!(u32, put_u32_le, get_u32_le, 4);
impl_simple_encode!(u64, put_u64_le, get_u64_le, 8);
impl_simple_encode!(i32, put_i32_le, get_i32_le, 4);
impl_simple_encode!(i64, put_i64_le, get_i64_le, 8);

// bool is encoded as u8 (0 or 1) in C++
impl Denc for bool {
    fn encode(&self, _features: u64) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::with_capacity(1);
        buf.put_u8(if *self { 1 } else { 0 });
        Ok(buf.freeze())
    }

    fn decode(bytes: &mut Bytes) -> Result<Self, RadosError> {
        if bytes.is_empty() {
            return Err(RadosError::Protocol(
                "Not enough bytes for bool".to_string(),
            ));
        }
        Ok(bytes.get_u8() != 0)
    }

    fn encoded_size(&self) -> Option<usize> {
        Some(1)
    }
}

// Implement FeatureEncode for primitive types (needed for frame encoding)
impl FeatureEncode for u8 {
    fn encode_with_features(&self, features: u64) -> Result<Bytes, RadosError> {
        self.encode(features)
    }

    fn decode_with_features(bytes: &mut Bytes, _features: u64) -> Result<Self, RadosError> {
        Self::decode(bytes)
    }
}

// Vec<T> implementation
impl<T: Denc> Denc for Vec<T> {
    fn encode(&self, features: u64) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::new();

        // Encode length
        let len = (self.len() as u32).encode(features)?;
        buf.extend_from_slice(&len);

        // Encode each element
        for item in self {
            let encoded = item.encode(features)?;
            buf.extend_from_slice(&encoded);
        }

        Ok(buf.freeze())
    }

    fn decode(bytes: &mut Bytes) -> Result<Self, RadosError> {
        let len = u32::decode(bytes)? as usize;
        let mut vec = Vec::with_capacity(len);

        for _ in 0..len {
            vec.push(T::decode(bytes)?);
        }

        Ok(vec)
    }
}

// ============= Unified Interface =============

/// Generic encoding function that dispatches based on type capabilities
pub fn denc_encode<T>(obj: &T, features: u64) -> Result<Bytes, RadosError>
where
    T: Denc,
{
    obj.encode(features)
}

/// Generic decoding function that dispatches based on type capabilities  
pub fn denc_decode<T>(bytes: &mut Bytes) -> Result<T, RadosError>
where
    T: Denc,
{
    T::decode(bytes)
}

/// Helper functions for common encoding patterns
pub trait DencExt: Denc {
    /// Encode without features (uses features=0)
    fn encode_no_features(&self) -> Result<Bytes, RadosError> {
        self.encode(0)
    }
}

impl<T: Denc> DencExt for T {}

// Bytes implementation - matches Ceph's buffer::ptr encoding (denc.h line 786+)
// Encode: uint32_t length + raw bytes content
impl Denc for Bytes {
    fn encode(&self, _features: u64) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::with_capacity(4 + self.len());
        buf.put_u32_le(self.len() as u32);
        buf.extend_from_slice(self);
        Ok(buf.freeze())
    }

    fn decode(bytes: &mut Bytes) -> Result<Self, RadosError> {
        if bytes.len() < 4 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for Bytes length".to_string(),
            ));
        }
        let len = bytes.get_u32_le() as usize;
        if bytes.len() < len {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes for Bytes content: need {}, have {}",
                len,
                bytes.len()
            )));
        }
        Ok(bytes.split_to(len))
    }
}
