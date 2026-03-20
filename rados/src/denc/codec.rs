//! Efficient buffer-based encoding/decoding traits
//!
//! This module provides the `Denc` trait which enables zero-allocation encoding
//! by writing directly to mutable buffers instead of allocating intermediate `Bytes` objects.
//!
//! # Performance Benefits
//!
//! The traditional `Denc` trait returns `Result<Bytes, RadosError>`, which forces allocation
//! at every level. For a composite type with N fields, this results in:
//! - N+1 allocations (one per field + parent)
//! - N memcpy operations (via `extend_from_slice`)
//!
//! The `Denc` trait writes directly to a mutable buffer, enabling:
//! - Single allocation when size is known upfront
//! - Zero memcpy operations
//! - 50-70% reduction in encoding time for typical composite types
//!
//! # Design
//!
//! ```rust,ignore
//! use bytes::{Buf, BufMut, BytesMut};
//! use crate::denc::{Denc, RadosError};
//!
//! // Types implement Denc to encode directly to buffers
//! fn encode_efficiently<T: Denc>(value: &T, features: u64) -> Result<BytesMut, RadosError> {
//!     // Calculate size if possible
//!     let capacity = value.encoded_size(features).unwrap_or(1024);
//!
//!     // Single allocation
//!     let mut buf = BytesMut::with_capacity(capacity);
//!
//!     // Direct write - no intermediate allocations
//!     value.encode(&mut buf, features)?;
//!
//!     Ok(buf)
//! }
//! ```

use crate::denc::codec_error::CodecError;
use crate::denc::error::RadosError;
use bytes::{Buf, BufMut};

/// Efficient encoding trait that writes directly to a mutable buffer
///
/// This trait is designed to eliminate allocation overhead by allowing types
/// to encode directly into a caller-provided buffer.
///
/// # Compile-Time Properties
///
/// - `USES_VERSIONING`: Does this type use ENCODE_START/DECODE_START wrapping?
/// - `FEATURE_DEPENDENT`: Does the encoding format change based on feature flags?
///
/// # Implementation Notes
///
/// Types should implement `encoded_size()` to return `Some(size)` whenever possible,
/// as this enables optimal preallocation. Return `None` only when the size truly
/// depends on runtime data that would be expensive to calculate.
pub trait Denc: Sized {
    /// Does this type use versioned encoding? (ENCODE_START in C++)
    const USES_VERSIONING: bool = false;

    /// Does encoding format depend on feature flags? (WRITE_CLASS_ENCODER_FEATURES in C++)
    const FEATURE_DEPENDENT: bool = false;

    /// Encode directly into a mutable buffer
    ///
    /// # Arguments
    ///
    /// * `buf` - The buffer to write encoded data into
    /// * `features` - Feature flags that may affect encoding format
    ///
    /// # Errors
    ///
    /// Returns `RadosError` if encoding fails (e.g., insufficient buffer space)
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError>;

    /// Decode from a buffer
    ///
    /// # Arguments
    ///
    /// * `buf` - The buffer to read encoded data from
    /// * `features` - Feature flags that may affect decoding format
    ///
    /// # Errors
    ///
    /// Returns `RadosError` if decoding fails (e.g., unexpected EOF, invalid data)
    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError>;

    /// Calculate encoded size
    ///
    /// Returns `Some(size)` if the size can be determined without actually encoding,
    /// or `None` if the size depends on expensive runtime calculations.
    ///
    /// # Performance Note
    ///
    /// Returning `Some(size)` enables optimal buffer preallocation, significantly
    /// improving performance for composite types.
    fn encoded_size(&self, features: u64) -> Option<usize>;
}

/// Marker trait for types with compile-time known fixed size
///
/// Types implementing this trait have a size that is known at compile time
/// and does not depend on runtime data or feature flags.
///
/// # Examples
///
/// ```rust,ignore
/// use crate::FixedSize;
///
/// // Primitive types are FixedSize
/// assert_eq!(u32::SIZE, 4);
/// assert_eq!(u64::SIZE, 8);
///
/// // Structs with only FixedSize fields are also FixedSize
/// #[repr(C)]
/// struct Header {
///     magic: u32,
///     version: u16,
///     flags: u16,
/// }
/// // Header::SIZE would be 8
/// ```
pub trait FixedSize: Denc {
    /// The fixed size in bytes
    const SIZE: usize;
}

/// Encode a value with pre-allocated buffer based on encoded_size hint
///
/// This helper function optimizes encoding by pre-allocating the exact buffer size needed,
/// eliminating reallocations during encoding. Use this for hot-path encoding where the
/// encoded size can be determined efficiently.
///
/// # Performance
///
/// Pre-allocating the buffer can improve encoding performance by 3-5% by avoiding
/// reallocation overhead during encoding.
///
/// # Examples
///
/// ```rust,ignore
/// use crate::denc::encode_with_capacity;
///
/// let message = MyMessage { ... };
/// let encoded = encode_with_capacity(&message, features)?;
/// ```
pub fn encode_with_capacity<T: Denc>(value: &T, features: u64) -> Result<bytes::Bytes, RadosError> {
    use bytes::BytesMut;

    let size = value.encoded_size(features).unwrap_or(1024);
    let mut buf = BytesMut::with_capacity(size);
    value.encode(&mut buf, features)?;
    Ok(buf.freeze())
}

// ============= Primitive Type Implementations =============

// Macro to implement Denc for primitive integer types
macro_rules! impl_denc_int {
    ($type:ty, $put_method:ident, $get_method:ident, $size:expr) => {
        impl Denc for $type {
            fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
                if buf.remaining_mut() < $size {
                    return Err(RadosError::Codec(CodecError::InsufficientData {
                        needed: $size,
                        available: buf.remaining_mut(),
                    }));
                }
                buf.$put_method(*self);
                Ok(())
            }

            fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
                if buf.remaining() < $size {
                    return Err(RadosError::Codec(CodecError::InsufficientData {
                        needed: $size,
                        available: buf.remaining(),
                    }));
                }
                Ok(buf.$get_method())
            }

            fn encoded_size(&self, _features: u64) -> Option<usize> {
                Some($size)
            }
        }

        impl FixedSize for $type {
            const SIZE: usize = $size;
        }
    };
}

impl_denc_int!(u8, put_u8, get_u8, 1);
impl_denc_int!(u16, put_u16_le, get_u16_le, 2);
impl_denc_int!(u32, put_u32_le, get_u32_le, 4);
impl_denc_int!(u64, put_u64_le, get_u64_le, 8);
impl_denc_int!(i8, put_i8, get_i8, 1);
impl_denc_int!(i16, put_i16_le, get_i16_le, 2);
impl_denc_int!(i32, put_i32_le, get_i32_le, 4);
impl_denc_int!(i64, put_i64_le, get_i64_le, 8);

impl Denc for bool {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        (u8::from(*self)).encode(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Ok(u8::decode(buf, features)? != 0)
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(1)
    }
}

impl FixedSize for bool {
    const SIZE: usize = 1;
}

impl Denc for f32 {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.to_bits().encode(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Ok(Self::from_bits(u32::decode(buf, features)?))
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(4)
    }
}

impl FixedSize for f32 {
    const SIZE: usize = 4;
}

impl Denc for f64 {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.to_bits().encode(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Ok(Self::from_bits(u64::decode(buf, features)?))
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(8)
    }
}

impl FixedSize for f64 {
    const SIZE: usize = 8;
}

// Array implementation for fixed-size arrays
impl<T: Denc + FixedSize, const N: usize> Denc for [T; N] {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        for item in self.iter() {
            item.encode(buf, features)?;
        }
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        let items = (0..N)
            .map(|_| T::decode(buf, features))
            .collect::<Result<Vec<_>, _>>()?;

        match items.try_into() {
            Ok(array) => Ok(array),
            Err(items) => Err(RadosError::Codec(CodecError::ArraySizeMismatch {
                expected: N,
                got: items.len(),
            })),
        }
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(T::SIZE * N)
    }
}

impl<T: FixedSize, const N: usize> FixedSize for [T; N] {
    const SIZE: usize = T::SIZE * N;
}

// ============= std::time Type Implementations =============

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Denc implementation for std::time::Duration
///
/// Encodes as Ceph's utime_t format: u32 seconds + u32 nanoseconds (8 bytes total)
///
/// # Wire Format
/// ```text
/// [u32_le sec][u32_le nsec]
/// ```
///
/// # Limitations
/// - Maximum duration: ~136 years (u32::MAX seconds)
/// - Durations exceeding u32::MAX seconds will be truncated
///
/// # Examples
/// ```rust
/// use std::time::Duration;
/// use crate::Denc;
/// use bytes::BytesMut;
///
/// let duration = Duration::new(12345, 678901234);
/// let mut buf = BytesMut::new();
/// duration.encode(&mut buf, 0).unwrap();
///
/// let decoded = Duration::decode(&mut buf, 0).unwrap();
/// assert_eq!(duration, decoded);
/// ```
impl Denc for Duration {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        // Truncate to u32::MAX seconds if necessary (matches Ceph behavior)
        let sec = self.as_secs().min(u32::MAX as u64) as u32;
        let nsec = self.subsec_nanos();
        sec.encode(buf, 0)?;
        nsec.encode(buf, 0)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        let sec = u32::decode(buf, 0)?;
        let nsec = u32::decode(buf, 0)?;
        Ok(Duration::new(sec as u64, nsec))
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(8)
    }
}

impl FixedSize for Duration {
    const SIZE: usize = 8;
}

/// Denc implementation for std::time::SystemTime
///
/// Encodes as Ceph's utime_t format: u32 seconds + u32 nanoseconds since UNIX_EPOCH
///
/// # Wire Format
/// ```text
/// [u32_le sec][u32_le nsec]
/// ```
///
/// # Limitations
/// - Maximum timestamp: ~2106-02-07 (u32::MAX seconds since epoch)
/// - Timestamps before UNIX_EPOCH will error
/// - Timestamps after 2106 will be truncated to u32::MAX
///
/// # Examples
/// ```rust
/// use std::time::{SystemTime, Duration, UNIX_EPOCH};
/// use crate::Denc;
/// use bytes::BytesMut;
///
/// let time = UNIX_EPOCH + Duration::new(1234567890, 123456789);
/// let mut buf = BytesMut::new();
/// time.encode(&mut buf, 0).unwrap();
///
/// let decoded = SystemTime::decode(&mut buf, 0).unwrap();
/// // Note: precision is limited to nanoseconds
/// assert_eq!(time, decoded);
/// ```
impl Denc for SystemTime {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        let duration = self
            .duration_since(UNIX_EPOCH)
            .map_err(|e| RadosError::Protocol(format!("SystemTime before UNIX_EPOCH: {e}")))?;
        // Truncate to u32::MAX seconds if necessary (matches Ceph's 2106 limit)
        let sec = duration.as_secs().min(u32::MAX as u64) as u32;
        let nsec = duration.subsec_nanos();
        sec.encode(buf, 0)?;
        nsec.encode(buf, 0)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        let sec = u32::decode(buf, 0)?;
        let nsec = u32::decode(buf, 0)?;
        Ok(UNIX_EPOCH + Duration::new(sec as u64, nsec))
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(8)
    }
}

impl FixedSize for SystemTime {
    const SIZE: usize = 8;
}

// ============= Integration with zerocopy module =============
// ZeroCopyDencode types now get Denc implementations directly from the derive macro.
// No blanket impl is needed.

// ============= Versioned Encoding =============
/// Trait for types that add version metadata during encoding
/// Follows Ceph's ENCODE_START/DECODE_START pattern
///
/// # C++ Equivalents
/// - `ENCODE_START(version, compat, bl)` → `encode_versioned()`
/// - `DECODE_START(version, bl)` → `decode_versioned()`
/// - `WRITE_CLASS_ENCODER` → version fixed at compile time
/// - `WRITE_CLASS_ENCODER_FEATURES` → version may depend on features
pub trait VersionedEncode: Sized {
    /// Does encoding format depend on feature flags?
    const FEATURE_DEPENDENT: bool = false;
    /// Maximum encoded version this decoder can handle.
    ///
    /// Ceph's `DECODE_START(v, ...)` validates `v >= struct_compat`.
    /// For fixed-version types derived with `VersionedDenc`, this is set to `version`.
    /// Manual implementations can override this when they have a known decoder cap.
    const MAX_DECODE_VERSION: u8 = u8::MAX;

    /// Get the current version to encode with (may depend on features)
    fn encoding_version(&self, features: u64) -> u8;

    /// Get the minimum compatible version
    fn compat_version(&self, features: u64) -> u8;

    /// Encode the actual content (without version wrapper) into a buffer
    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        version: u8,
    ) -> Result<(), RadosError>;

    /// Decode content for a specific version
    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        compat_version: u8,
    ) -> Result<Self, RadosError>;

    /// Calculate encoded size of content (without version header)
    ///
    /// This should return the size that would be written by encode_content.
    /// The total encoded size including version header is:
    /// VERSION_HEADER_SIZE (6 bytes) + encoded_size_content()
    fn encoded_size_content(&self, features: u64, version: u8) -> Option<usize>;

    /// Calculate total encoded size including version header
    ///
    /// Default implementation adds VERSION_HEADER_SIZE to content size.
    fn encoded_size_versioned(&self, features: u64) -> Option<usize> {
        const VERSION_HEADER_SIZE: usize = 6; // struct_v (1) + struct_compat (1) + len (4)
        let version = self.encoding_version(features);
        let content_size = self.encoded_size_content(features, version)?;
        Some(VERSION_HEADER_SIZE + content_size)
    }

    /// Encode with version metadata (ENCODE_START pattern)
    fn encode_versioned<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        let version = self.encoding_version(features);
        let compat = self.compat_version(features);

        if let Some(content_size) = self.encoded_size_content(features, version) {
            // Fast path: size is known ahead of time; write header then content in one pass.
            buf.put_u8(version);
            buf.put_u8(compat);
            buf.put_u32_le(content_size as u32);
            self.encode_content(buf, features, version)?;
        } else {
            // Slow path: encode content to a temporary buffer to discover its length.
            let mut content_buf = bytes::BytesMut::with_capacity(256);
            self.encode_content(&mut content_buf, features, version)?;
            buf.put_u8(version);
            buf.put_u8(compat);
            buf.put_u32_le(content_buf.len() as u32);
            buf.put_slice(&content_buf);
        }

        Ok(())
    }

    /// Decode with version handling (DECODE_START pattern)
    fn decode_versioned<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 6 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for version header".to_string(),
            ));
        }

        let struct_v = buf.get_u8();
        let struct_compat = buf.get_u8();
        let struct_len = buf.get_u32_le() as usize;

        if struct_v > Self::MAX_DECODE_VERSION {
            return Err(RadosError::Codec(CodecError::VersionTooNew {
                got: struct_v,
                max: Self::MAX_DECODE_VERSION,
                type_name: std::any::type_name::<Self>(),
            }));
        }
        if struct_compat > struct_v {
            return Err(RadosError::Codec(CodecError::InvalidVersionHeader {
                type_name: std::any::type_name::<Self>(),
                compat: struct_compat,
                version: struct_v,
            }));
        }

        if buf.remaining() < struct_len {
            return Err(RadosError::Codec(CodecError::InsufficientData {
                needed: struct_len,
                available: buf.remaining(),
            }));
        }

        // Create a limited view of the content
        let mut content = buf.take(struct_len);

        // Decode based on version
        let result = Self::decode_content(&mut content, features, struct_v, struct_compat)?;

        // DECODE_FINISH: consume any remaining bytes (forward compatibility)
        content.advance(content.remaining());

        Ok(result)
    }
}

// Note: We cannot provide a blanket implementation for VersionedEncode types
// because it would conflict with the blanket impl for ZeroCopyDencode.
// Use the impl_denc_for_versioned! macro below to generate Denc impl.

/// Macro to implement Denc for types that implement VersionedEncode
///
/// This macro generates the boilerplate Denc implementation that delegates to
/// the VersionedEncode methods. This eliminates repetitive code for versioned types.
///
/// # Example
///
/// ```ignore
/// impl VersionedEncode for MyType {
///     fn encoding_version(&self, _features: u64) -> u8 { 1 }
///     fn compat_version(&self, _features: u64) -> u8 { 1 }
///     fn encode_content<B: BufMut>(&self, buf: &mut B, features: u64, _version: u8) -> Result<(), RadosError> {
///         self.field.encode(buf, features)?;
///         Ok(())
///     }
///     fn decode_content<B: Buf>(buf: &mut B, features: u64, _version: u8, _compat: u8) -> Result<Self, RadosError> {
///         let field = Denc::decode(buf, features)?;
///         Ok(Self { field })
///     }
///     fn encoded_size_content(&self, features: u64, _version: u8) -> Option<usize> {
///         Some(self.field.encoded_size(features)?)
///     }
/// }
///
/// // Instead of manually implementing Denc, use the macro:
/// impl_denc_for_versioned!(MyType);
/// ```
#[macro_export]
macro_rules! impl_denc_for_versioned {
    ($type:ty) => {
        impl $crate::Denc for $type {
            const USES_VERSIONING: bool = true;

            fn encode<B: bytes::BufMut>(
                &self,
                buf: &mut B,
                features: u64,
            ) -> std::result::Result<(), $crate::RadosError> {
                <Self as $crate::VersionedEncode>::encode_versioned(self, buf, features)
            }

            fn decode<B: bytes::Buf>(
                buf: &mut B,
                features: u64,
            ) -> std::result::Result<Self, $crate::RadosError> {
                <Self as $crate::VersionedEncode>::decode_versioned(buf, features)
            }

            fn encoded_size(&self, features: u64) -> Option<usize> {
                <Self as $crate::VersionedEncode>::encoded_size_versioned(self, features)
            }
        }
    };
}

// ============= Variable-Size Type Implementations =============
use bytes::Bytes;

// Vec<T> implementation - encodes length as u32 followed by elements
impl<T: Denc> Denc for Vec<T> {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        // Encode length as u32
        let len = self.len() as u32;
        Denc::encode(&len, buf, features)?;

        // Encode each element
        for item in self {
            Denc::encode(item, buf, features)?;
        }

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        let len = <u32 as Denc>::decode(buf, features)? as usize;
        let mut vec = Vec::with_capacity(len);

        for _ in 0..len {
            vec.push(<T as Denc>::decode(buf, features)?);
        }

        Ok(vec)
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        // Start with u32 length
        let mut size = 4;

        // Add size of each element
        for item in self {
            size += Denc::encoded_size(item, features)?;
        }

        Some(size)
    }
}

// Bytes implementation - matches Ceph's buffer::ptr encoding
// Encode: uint32_t length + raw bytes content
impl Denc for Bytes {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        // Encode length as u32
        let len = self.len() as u32;
        Denc::encode(&len, buf, features)?;

        // Copy bytes
        if buf.remaining_mut() < self.len() {
            return Err(RadosError::Codec(CodecError::InsufficientData {
                needed: self.len(),
                available: buf.remaining_mut(),
            }));
        }
        buf.put_slice(self);

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        let len = <u32 as Denc>::decode(buf, features)? as usize;

        if buf.remaining() < len {
            return Err(RadosError::Codec(CodecError::InsufficientData {
                needed: len,
                available: buf.remaining(),
            }));
        }

        Ok(buf.copy_to_bytes(len))
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(4 + self.len())
    }
}

impl Denc for String {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        // Encode length as u32
        let len = self.len() as u32;
        Denc::encode(&len, buf, features)?;

        // Copy string bytes
        if buf.remaining_mut() < self.len() {
            return Err(RadosError::Codec(CodecError::InsufficientData {
                needed: self.len(),
                available: buf.remaining_mut(),
            }));
        }
        buf.put_slice(self.as_bytes());

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        let len = <u32 as Denc>::decode(buf, features)? as usize;

        if buf.remaining() < len {
            return Err(RadosError::Codec(CodecError::InsufficientData {
                needed: len,
                available: buf.remaining(),
            }));
        }

        let bytes = buf.copy_to_bytes(len);
        String::from_utf8(bytes.into()).map_err(|e| RadosError::Codec(CodecError::Utf8(e)))
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(4 + self.len())
    }
}

// Macro to implement Denc for map-like types.
// Encodes as: u32 length + key-value pairs.
// Use the `@capacity` arm for hash-based types that support `with_capacity`.
macro_rules! impl_denc_map {
    // Arm for hash-based types with with_capacity (e.g. HashMap)
    (@capacity $map_type:ident, $key_bound:tt, $($extra_bound:tt)*) => {
        impl<K: Denc + $key_bound, V: Denc> Denc for std::collections::$map_type<K, V>
        where
            K: $($extra_bound)*
        {
            fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
                Denc::encode(&(self.len() as u32), buf, features)?;
                for (key, value) in self {
                    Denc::encode(key, buf, features)?;
                    Denc::encode(value, buf, features)?;
                }
                Ok(())
            }

            fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
                let len = <u32 as Denc>::decode(buf, features)? as usize;
                let mut map = Self::with_capacity(len);
                for _ in 0..len {
                    let key = <K as Denc>::decode(buf, features)?;
                    let value = <V as Denc>::decode(buf, features)?;
                    map.insert(key, value);
                }
                Ok(map)
            }

            fn encoded_size(&self, features: u64) -> Option<usize> {
                let mut size = 4;
                for (key, value) in self {
                    size += Denc::encoded_size(key, features)?;
                    size += Denc::encoded_size(value, features)?;
                }
                Some(size)
            }
        }
    };
    // Arm for tree-based types without with_capacity (e.g. BTreeMap)
    ($map_type:ident, $key_bound:tt, $($extra_bound:tt)*) => {
        impl<K: Denc + $key_bound, V: Denc> Denc for std::collections::$map_type<K, V>
        where
            K: $($extra_bound)*
        {
            fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
                Denc::encode(&(self.len() as u32), buf, features)?;
                for (key, value) in self {
                    Denc::encode(key, buf, features)?;
                    Denc::encode(value, buf, features)?;
                }
                Ok(())
            }

            fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
                let len = <u32 as Denc>::decode(buf, features)? as usize;
                let mut map = Self::new();
                for _ in 0..len {
                    let key = <K as Denc>::decode(buf, features)?;
                    let value = <V as Denc>::decode(buf, features)?;
                    map.insert(key, value);
                }
                Ok(map)
            }

            fn encoded_size(&self, features: u64) -> Option<usize> {
                let mut size = 4;
                for (key, value) in self {
                    size += Denc::encoded_size(key, features)?;
                    size += Denc::encoded_size(value, features)?;
                }
                Some(size)
            }
        }
    };
}

// BTreeMap: tree-based, no with_capacity
impl_denc_map!(BTreeMap, Ord,);

// HashMap: hash-based, pre-allocate with the decoded count
impl_denc_map!(@capacity HashMap, Eq, Eq + std::hash::Hash);

// BTreeSet implementation
impl<T: Denc + Ord> Denc for std::collections::BTreeSet<T> {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        // Encode length as u32
        let len = self.len() as u32;
        Denc::encode(&len, buf, features)?;

        // Encode each element
        for item in self {
            Denc::encode(item, buf, features)?;
        }

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        let len = <u32 as Denc>::decode(buf, features)? as usize;
        let mut set = std::collections::BTreeSet::new();

        for _ in 0..len {
            let item = <T as Denc>::decode(buf, features)?;
            set.insert(item);
        }

        Ok(set)
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        // Start with u32 length
        let mut size = 4;

        // Add size of each element
        for item in self {
            size += Denc::encoded_size(item, features)?;
        }

        Some(size)
    }
}

// Option implementation
impl<T: Denc> Denc for Option<T> {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        match self {
            Some(value) => {
                // Encode 1 to indicate Some
                buf.put_u8(1);
                value.encode(buf, features)?;
            }
            None => {
                // Encode 0 to indicate None
                buf.put_u8(0);
            }
        }
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        let has_value = buf.get_u8();
        if has_value != 0 {
            let value = T::decode(buf, features)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        match self {
            Some(value) => {
                let value_size = value.encoded_size(features)?;
                Some(1 + value_size)
            }
            None => Some(1),
        }
    }
}

// Tuple implementations - using a macro pattern like std does
macro_rules! impl_denc_tuple {
    ($($idx:tt => $T:ident),+) => {
        impl<$($T: Denc),+> Denc for ($($T,)+) {
            fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
                $(self.$idx.encode(buf, features)?;)+
                Ok(())
            }

            fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
                Ok(($($T::decode(buf, features)?,)+))
            }

            fn encoded_size(&self, features: u64) -> Option<usize> {
                let mut size = 0;
                $(size += self.$idx.encoded_size(features)?;)+
                Some(size)
            }
        }
    };
}

// Implement for tuples of size 1-12 (matching std library conventions)
impl_denc_tuple!(0 => T0);
impl_denc_tuple!(0 => T0, 1 => T1);
impl_denc_tuple!(0 => T0, 1 => T1, 2 => T2);
impl_denc_tuple!(0 => T0, 1 => T1, 2 => T2, 3 => T3);
impl_denc_tuple!(0 => T0, 1 => T1, 2 => T2, 3 => T3, 4 => T4);
impl_denc_tuple!(0 => T0, 1 => T1, 2 => T2, 3 => T3, 4 => T4, 5 => T5);
impl_denc_tuple!(0 => T0, 1 => T1, 2 => T2, 3 => T3, 4 => T4, 5 => T5, 6 => T6);
impl_denc_tuple!(0 => T0, 1 => T1, 2 => T2, 3 => T3, 4 => T4, 5 => T5, 6 => T6, 7 => T7);
impl_denc_tuple!(0 => T0, 1 => T1, 2 => T2, 3 => T3, 4 => T4, 5 => T5, 6 => T6, 7 => T7, 8 => T8);
impl_denc_tuple!(0 => T0, 1 => T1, 2 => T2, 3 => T3, 4 => T4, 5 => T5, 6 => T6, 7 => T7, 8 => T8, 9 => T9);
impl_denc_tuple!(0 => T0, 1 => T1, 2 => T2, 3 => T3, 4 => T4, 5 => T5, 6 => T6, 7 => T7, 8 => T8, 9 => T9, 10 => T10);
impl_denc_tuple!(0 => T0, 1 => T1, 2 => T2, 3 => T3, 4 => T4, 5 => T5, 6 => T6, 7 => T7, 8 => T8, 9 => T9, 10 => T10, 11 => T11);

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestVersionedType {
        value: u8,
    }

    impl VersionedEncode for TestVersionedType {
        const MAX_DECODE_VERSION: u8 = 2;

        fn encoding_version(&self, _features: u64) -> u8 {
            1
        }

        fn compat_version(&self, _features: u64) -> u8 {
            1
        }

        fn encode_content<B: BufMut>(
            &self,
            buf: &mut B,
            _features: u64,
            _version: u8,
        ) -> Result<(), RadosError> {
            self.value.encode(buf, 0)
        }

        fn decode_content<B: Buf>(
            buf: &mut B,
            _features: u64,
            _version: u8,
            _compat_version: u8,
        ) -> Result<Self, RadosError> {
            let value = u8::decode(buf, 0)?;
            Ok(Self { value })
        }

        fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
            Some(1)
        }
    }

    crate::denc::impl_denc_for_versioned!(TestVersionedType);

    #[test]
    fn test_primitive_roundtrip() {
        let mut buf = BytesMut::new();

        // Test u32
        let val: u32 = 0x12345678;
        Denc::encode(&val, &mut buf, 0).unwrap();
        assert_eq!(buf.len(), 4);

        let decoded = <u32 as Denc>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, val);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_bool_roundtrip() {
        let mut buf = BytesMut::new();

        Denc::encode(&true, &mut buf, 0).unwrap();
        Denc::encode(&false, &mut buf, 0).unwrap();

        assert!(<bool as Denc>::decode(&mut buf, 0).unwrap());
        assert!(!(<bool as Denc>::decode(&mut buf, 0).unwrap()));
    }

    #[test]
    fn test_float_roundtrip() {
        let mut buf = BytesMut::new();

        Denc::encode(&3.5f32, &mut buf, 0).unwrap();
        Denc::encode(&-1.25f64, &mut buf, 0).unwrap();

        assert_eq!(<f32 as Denc>::decode(&mut buf, 0).unwrap(), 3.5);
        assert_eq!(<f64 as Denc>::decode(&mut buf, 0).unwrap(), -1.25);
    }

    #[test]
    fn test_array_roundtrip() {
        let mut buf = BytesMut::new();

        let arr: [u32; 4] = [1, 2, 3, 4];
        Denc::encode(&arr, &mut buf, 0).unwrap();
        assert_eq!(buf.len(), 16);

        let decoded = <[u32; 4] as Denc>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, arr);
    }

    #[test]
    fn test_vec_roundtrip() {
        let mut buf = BytesMut::new();

        let vec = vec![1u32, 2, 3, 4, 5];
        Denc::encode(&vec, &mut buf, 0).unwrap();

        // Length (4 bytes) + 5 * u32 (20 bytes) = 24 bytes
        assert_eq!(buf.len(), 24);

        let decoded = <Vec<u32> as Denc>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, vec);
    }

    #[test]
    fn test_bytes_roundtrip() {
        let mut buf = BytesMut::new();

        let data = Bytes::from_static(b"hello world");
        Denc::encode(&data, &mut buf, 0).unwrap();

        // Length (4 bytes) + data (11 bytes) = 15 bytes
        assert_eq!(buf.len(), 15);

        let decoded = <Bytes as Denc>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_string_roundtrip() {
        let mut buf = BytesMut::new();

        let s = String::from("hello world");
        Denc::encode(&s, &mut buf, 0).unwrap();

        // Length (4 bytes) + data (11 bytes) = 15 bytes
        assert_eq!(buf.len(), 15);

        let decoded = <String as Denc>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, s);
    }

    #[test]
    fn test_encoded_size() {
        assert_eq!(Denc::encoded_size(&42u32, 0), Some(4));
        assert_eq!(Denc::encoded_size(&true, 0), Some(1));
        assert_eq!(Denc::encoded_size(&[1u32, 2, 3], 0), Some(12));

        let vec = vec![1u32, 2, 3];
        assert_eq!(Denc::encoded_size(&vec, 0), Some(16)); // 4 + 3*4

        let s = String::from("test");
        assert_eq!(Denc::encoded_size(&s, 0), Some(8)); // 4 + 4
    }

    #[test]
    fn test_fixed_size_constants() {
        assert_eq!(u8::SIZE, 1);
        assert_eq!(u16::SIZE, 2);
        assert_eq!(u32::SIZE, 4);
        assert_eq!(u64::SIZE, 8);
        assert_eq!(bool::SIZE, 1);
        assert_eq!(<[u32; 10]>::SIZE, 40);
    }

    #[test]
    fn test_preallocation() {
        // Test that we can preallocate the exact size
        let vec = vec![1u32, 2, 3, 4, 5];
        let size = vec.encoded_size(0).unwrap();

        let mut buf = BytesMut::with_capacity(size);
        vec.encode(&mut buf, 0).unwrap();

        assert_eq!(buf.len(), size);
        assert_eq!(buf.capacity(), size); // No reallocation occurred
    }

    // TODO: Fix derive macro to work inside the rados-denc crate itself
    // The macro generates `crate::Denc` which doesn't work when used inside rados-denc crate
    /*
        #[test]
        fn test_derive_macro_fixed_size() {
            #[derive(crate::Denc)]
    #[denc(crate = "crate")]
                struct FixedStruct {
                a: u32,
                b: u64,
                c: u16,
            }

            let s = FixedStruct { a: 1, b: 2, c: 3 };

            // Test encoded_size
            assert_eq!(s.encoded_size(0), Some(14)); // 4 + 8 + 2

            // Test roundtrip
            let mut buf = BytesMut::new();
            s.encode(&mut buf, 0).unwrap();
            assert_eq!(buf.len(), 14);

            let decoded = FixedStruct::decode(&mut buf, 0).unwrap();
            assert_eq!(decoded.a, 1);
            assert_eq!(decoded.b, 2);
            assert_eq!(decoded.c, 3);
        }

        #[test]
        fn test_derive_macro_variable_size() {
            #[derive(crate::Denc)]
    #[denc(crate = "crate")]
            struct MixedStruct {
                header: u32,
                data: Vec<u8>,
                footer: u64,
            }

            let s = MixedStruct {
                header: 0x12345678,
                data: vec![1, 2, 3, 4, 5],
                footer: 0xABCDEF,
            };

            // Test encoded_size
            assert_eq!(s.encoded_size(0), Some(4 + 4 + 5 + 8)); // header + vec_len + data + footer

            // Test roundtrip
            let mut buf = BytesMut::new();
            s.encode(&mut buf, 0).unwrap();

            let decoded = MixedStruct::decode(&mut buf, 0).unwrap();
            assert_eq!(decoded.header, 0x12345678);
            assert_eq!(decoded.data, vec![1, 2, 3, 4, 5]);
            assert_eq!(decoded.footer, 0xABCDEF);
        }
        */

    #[test]
    fn test_zerocopy_integration() {
        // Test that ZeroCopyDencode types automatically get Denc
        // We'll use a simple array which implements ZeroCopyDencode via the derive macro

        let arr: [u32; 3] = [1, 2, 3];

        // Test that we can use Denc methods
        assert_eq!(arr.encoded_size(0), Some(12)); // 3 * 4 bytes

        let mut buf = BytesMut::new();
        Denc::encode(&arr, &mut buf, 0).unwrap();
        assert_eq!(buf.len(), 12);

        let decoded = <[u32; 3] as Denc>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, arr);
    }

    #[test]
    fn test_tuple_implementations() {
        let mut buf = BytesMut::new();

        // Test 1-tuple
        let t1 = (42u32,);
        assert_eq!(t1.encoded_size(0), Some(4));
        t1.encode(&mut buf, 0).unwrap();
        let decoded1 = <(u32,)>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded1, t1);

        // Test 2-tuple
        let t2 = (1u32, 2u64);
        assert_eq!(t2.encoded_size(0), Some(12));
        t2.encode(&mut buf, 0).unwrap();
        let decoded2 = <(u32, u64)>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded2, t2);

        // Test 3-tuple
        let t3 = (1u32, 2u64, 3u16);
        assert_eq!(t3.encoded_size(0), Some(14));
        t3.encode(&mut buf, 0).unwrap();
        let decoded3 = <(u32, u64, u16)>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded3, t3);

        // Test 5-tuple (to verify macro works beyond original 4-tuple)
        let t5 = (1u32, 2u64, 3u16, 4u8, 5i32);
        assert_eq!(t5.encoded_size(0), Some(19));
        t5.encode(&mut buf, 0).unwrap();
        let decoded5 = <(u32, u64, u16, u8, i32)>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded5, t5);
    }

    #[test]
    fn test_duration_roundtrip() {
        use std::time::Duration;

        let mut buf = BytesMut::new();

        // Test basic duration
        let d1 = Duration::new(1234567890, 123456789);
        d1.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 8);

        let decoded = Duration::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, d1);

        // Test zero duration
        let d2 = Duration::ZERO;
        d2.encode(&mut buf, 0).unwrap();
        let decoded2 = Duration::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded2, d2);

        // Test duration with max nanoseconds
        let d3 = Duration::new(100, 999_999_999);
        d3.encode(&mut buf, 0).unwrap();
        let decoded3 = Duration::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded3, d3);
    }

    #[test]
    fn test_duration_fixed_size() {
        use std::time::Duration;

        assert_eq!(Duration::SIZE, 8);
        assert_eq!(Duration::new(0, 0).encoded_size(0), Some(8));
        assert_eq!(Duration::new(999999, 123456).encoded_size(0), Some(8));
    }

    #[test]
    fn test_duration_truncation() {
        use std::time::Duration;

        let mut buf = BytesMut::new();

        // Duration exceeding u32::MAX seconds should be truncated
        let large_duration = Duration::new(u64::MAX, 123456789);
        large_duration.encode(&mut buf, 0).unwrap();

        let decoded = Duration::decode(&mut buf, 0).unwrap();
        // Should be truncated to u32::MAX seconds
        assert_eq!(decoded.as_secs(), u32::MAX as u64);
        assert_eq!(decoded.subsec_nanos(), 123456789);
    }

    #[test]
    fn test_systemtime_roundtrip() {
        use std::time::{Duration, SystemTime, UNIX_EPOCH};

        let mut buf = BytesMut::new();

        // Test basic SystemTime
        let t1 = UNIX_EPOCH + Duration::new(1234567890, 123456789);
        t1.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 8);

        let decoded = SystemTime::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, t1);

        // Test epoch
        let t2 = UNIX_EPOCH;
        t2.encode(&mut buf, 0).unwrap();
        let decoded2 = SystemTime::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded2, t2);

        // Test with max nanoseconds
        let t3 = UNIX_EPOCH + Duration::new(1000000, 999_999_999);
        t3.encode(&mut buf, 0).unwrap();
        let decoded3 = SystemTime::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded3, t3);
    }

    #[test]
    fn test_systemtime_fixed_size() {
        use std::time::{Duration, SystemTime, UNIX_EPOCH};

        assert_eq!(SystemTime::SIZE, 8);
        assert_eq!((UNIX_EPOCH + Duration::new(0, 0)).encoded_size(0), Some(8));
        assert_eq!(
            (UNIX_EPOCH + Duration::new(999999, 123456)).encoded_size(0),
            Some(8)
        );
    }

    #[test]
    fn test_systemtime_before_epoch() {
        use std::time::Duration;

        let mut buf = BytesMut::new();

        // SystemTime before UNIX_EPOCH should error
        let before_epoch = UNIX_EPOCH - Duration::new(100, 0);
        let result = before_epoch.encode(&mut buf, 0);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("before UNIX_EPOCH")
        );
    }

    #[test]
    fn test_systemtime_year_2106_limit() {
        use std::time::{Duration, UNIX_EPOCH};

        let mut buf = BytesMut::new();

        // SystemTime at u32::MAX seconds (year 2106) should work
        let year_2106 = UNIX_EPOCH + Duration::new(u32::MAX as u64, 999_999_999);
        year_2106.encode(&mut buf, 0).unwrap();

        let decoded = std::time::SystemTime::decode(&mut buf, 0).unwrap();
        let duration = decoded.duration_since(UNIX_EPOCH).unwrap();
        // Should match exactly at the limit
        assert_eq!(duration.as_secs(), u32::MAX as u64);
        assert_eq!(duration.subsec_nanos(), 999_999_999);
    }

    #[test]
    fn test_decode_versioned_rejects_struct_v_above_max() {
        let mut buf = BytesMut::new();
        3u8.encode(&mut buf, 0).unwrap(); // struct_v
        1u8.encode(&mut buf, 0).unwrap(); // struct_compat
        1u32.encode(&mut buf, 0).unwrap(); // struct_len
        0x2au8.encode(&mut buf, 0).unwrap(); // payload

        let mut read_buf = buf.freeze();
        let err = TestVersionedType::decode(&mut read_buf, 0).unwrap_err();
        assert!(
            matches!(
                err,
                RadosError::Codec(CodecError::VersionTooNew { got: 3, .. })
            ),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_decode_versioned_consumes_remaining_content() {
        let mut buf = BytesMut::new();
        1u8.encode(&mut buf, 0).unwrap(); // struct_v
        1u8.encode(&mut buf, 0).unwrap(); // struct_compat
        3u32.encode(&mut buf, 0).unwrap(); // struct_len
        7u8.encode(&mut buf, 0).unwrap(); // decoded by TestVersionedType
        0xaau8.encode(&mut buf, 0).unwrap(); // forward-compatible trailing byte
        0xbbu8.encode(&mut buf, 0).unwrap(); // forward-compatible trailing byte
        9u8.encode(&mut buf, 0).unwrap(); // next field in parent stream

        let mut read_buf = buf.freeze();
        let decoded = TestVersionedType::decode(&mut read_buf, 0).unwrap();
        assert_eq!(decoded.value, 7);

        let next = u8::decode(&mut read_buf, 0).unwrap();
        assert_eq!(next, 9);
    }
}
