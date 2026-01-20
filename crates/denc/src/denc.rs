//! Core encoding/decoding traits for Ceph DENC protocol
//!
//! This module provides the fundamental traits for encoding and decoding
//! Ceph data structures.

use crate::error::RadosError;
use bytes::{Buf, BufMut};

/// Core encoding/decoding trait for Ceph types
///
/// This trait provides methods for encoding types to and decoding from
/// binary format compatible with Ceph's DENC protocol.
///
/// # Compile-Time Properties
///
/// - `USES_VERSIONING`: Does this type use ENCODE_START/DECODE_START wrapping?
/// - `FEATURE_DEPENDENT`: Does the encoding format change based on feature flags?
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
    /// Returns `RadosError` if encoding fails
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
    /// Returns `RadosError` if decoding fails
    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError>;

    /// Calculate encoded size
    ///
    /// Returns `Some(size)` if the size can be determined without actually encoding,
    /// or `None` if the size depends on expensive runtime calculations.
    fn encoded_size(&self, features: u64) -> Option<usize>;
}

/// Marker trait for types with compile-time known fixed size
///
/// Types implementing this trait have a size that is known at compile time
/// and does not depend on runtime data or feature flags.
pub trait FixedSize: Denc {
    /// The fixed size in bytes
    const SIZE: usize;
}

/// Trait for types that add version metadata during encoding
///
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

    /// Encode with version metadata (ENCODE_START pattern)
    fn encode_versioned<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        let version = self.encoding_version(features);
        let compat = self.compat_version(features);

        // Encode content to temporary buffer to get size
        let mut content_buf = bytes::BytesMut::new();
        self.encode_content(&mut content_buf, features, version)?;

        // Write version header
        buf.put_u8(version);
        buf.put_u8(compat);
        buf.put_u32_le(content_buf.len() as u32);
        buf.put_slice(&content_buf);

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

        if buf.remaining() < struct_len {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes: need {}, have {}",
                struct_len,
                buf.remaining()
            )));
        }

        // Create a limited view of the content
        let mut content = buf.take(struct_len);

        // Decode based on version
        let result = Self::decode_content(&mut content, features, struct_v, struct_compat)?;

        // DECODE_FINISH: consume any remaining bytes (forward compatibility)
        if content.remaining() > 0 {
            // Skip remaining bytes for forward compatibility
        }

        Ok(result)
    }
}

// ============= Primitive Type Implementations =============

// Macro to implement Denc for primitive integer types
macro_rules! impl_denc_int {
    ($type:ty, $put_method:ident, $get_method:ident, $size:expr) => {
        impl Denc for $type {
            fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
                if buf.remaining_mut() < $size {
                    return Err(RadosError::Protocol(format!(
                        "Insufficient buffer space: need {} bytes for {}, have {}",
                        $size,
                        stringify!($type),
                        buf.remaining_mut()
                    )));
                }
                buf.$put_method(*self);
                Ok(())
            }

            fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
                if buf.remaining() < $size {
                    return Err(RadosError::Protocol(format!(
                        "Insufficient bytes: need {} for {}, have {}",
                        $size,
                        stringify!($type),
                        buf.remaining()
                    )));
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

// bool is encoded as u8 (0 or 1) in C++
impl Denc for bool {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        if buf.remaining_mut() < 1 {
            return Err(RadosError::Protocol(format!(
                "Insufficient buffer space: need 1 byte for bool, have {}",
                buf.remaining_mut()
            )));
        }
        buf.put_u8(if *self { 1 } else { 0 });
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 1 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for bool".to_string(),
            ));
        }
        Ok(buf.get_u8() != 0)
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(1)
    }
}

impl FixedSize for bool {
    const SIZE: usize = 1;
}

// f32 and f64 implementations
impl Denc for f32 {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        if buf.remaining_mut() < 4 {
            return Err(RadosError::Protocol(format!(
                "Insufficient buffer space: need 4 bytes for f32, have {}",
                buf.remaining_mut()
            )));
        }
        buf.put_f32_le(*self);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 4 {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes: need 4 for f32, have {}",
                buf.remaining()
            )));
        }
        Ok(buf.get_f32_le())
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(4)
    }
}

impl FixedSize for f32 {
    const SIZE: usize = 4;
}

impl Denc for f64 {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        if buf.remaining_mut() < 8 {
            return Err(RadosError::Protocol(format!(
                "Insufficient buffer space: need 8 bytes for f64, have {}",
                buf.remaining_mut()
            )));
        }
        buf.put_f64_le(*self);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 8 {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes: need 8 for f64, have {}",
                buf.remaining()
            )));
        }
        Ok(buf.get_f64_le())
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(8)
    }
}

impl FixedSize for f64 {
    const SIZE: usize = 8;
}

// ============= Variable-Size Collection Type Implementations =============

// String implementation - encoded as UTF-8 bytes with length prefix
impl Denc for String {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        // Encode length as u32
        let len = self.len() as u32;
        len.encode(buf, features)?;

        // Copy string bytes
        if buf.remaining_mut() < self.len() {
            return Err(RadosError::Protocol(format!(
                "Insufficient buffer space: need {} bytes, have {}",
                self.len(),
                buf.remaining_mut()
            )));
        }
        buf.put_slice(self.as_bytes());

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        let len = u32::decode(buf, features)? as usize;

        if buf.remaining() < len {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes for String content: need {}, have {}",
                len,
                buf.remaining()
            )));
        }

        let mut bytes = vec![0u8; len];
        buf.copy_to_slice(&mut bytes);

        String::from_utf8(bytes).map_err(|e| RadosError::Protocol(format!("Invalid UTF-8: {}", e)))
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(4 + self.len())
    }
}

// Vec<T> implementation - encodes length as u32 followed by elements
impl<T: Denc> Denc for Vec<T> {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        // Encode length as u32
        let len = self.len() as u32;
        len.encode(buf, features)?;

        // Encode each element
        for item in self {
            item.encode(buf, features)?;
        }

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        let len = u32::decode(buf, features)? as usize;
        let mut vec = Vec::with_capacity(len);

        for _ in 0..len {
            vec.push(T::decode(buf, features)?);
        }

        Ok(vec)
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        // Start with u32 length
        let mut size = 4;

        // Add size of each element
        for item in self {
            size += item.encoded_size(features)?;
        }

        Some(size)
    }
}

// Option<T> implementation - encodes as bool (present) followed by value if Some
impl<T: Denc> Denc for Option<T> {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        match self {
            Some(value) => {
                true.encode(buf, features)?;
                value.encode(buf, features)?;
            }
            None => {
                false.encode(buf, features)?;
            }
        }
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        let present = bool::decode(buf, features)?;
        if present {
            Ok(Some(T::decode(buf, features)?))
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

// ============= Complex Collection Type Implementations =============

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

// BTreeMap implementation - encodes length as u32 followed by key-value pairs
impl<K: Denc + Ord, V: Denc> Denc for BTreeMap<K, V> {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        // Encode length as u32
        let len = self.len() as u32;
        len.encode(buf, features)?;

        // Encode each key-value pair
        for (key, value) in self {
            key.encode(buf, features)?;
            value.encode(buf, features)?;
        }

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        let len = u32::decode(buf, features)? as usize;
        let mut map = BTreeMap::new();

        for _ in 0..len {
            let key = K::decode(buf, features)?;
            let value = V::decode(buf, features)?;
            map.insert(key, value);
        }

        Ok(map)
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        // Start with u32 length
        let mut size = 4;

        // Add size of each key-value pair
        for (key, value) in self {
            size += key.encoded_size(features)?;
            size += value.encoded_size(features)?;
        }

        Some(size)
    }
}

// HashMap implementation - encodes length as u32 followed by key-value pairs
impl<K: Denc + Eq + Hash, V: Denc> Denc for HashMap<K, V> {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        // Encode length as u32
        let len = self.len() as u32;
        len.encode(buf, features)?;

        // Encode each key-value pair
        for (key, value) in self {
            key.encode(buf, features)?;
            value.encode(buf, features)?;
        }

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        let len = u32::decode(buf, features)? as usize;
        let mut map = HashMap::with_capacity(len);

        for _ in 0..len {
            let key = K::decode(buf, features)?;
            let value = V::decode(buf, features)?;
            map.insert(key, value);
        }

        Ok(map)
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        // Start with u32 length
        let mut size = 4;

        // Add size of each key-value pair
        for (key, value) in self {
            size += key.encoded_size(features)?;
            size += value.encoded_size(features)?;
        }

        Some(size)
    }
}

// Tuple implementations for pairs (2-element tuples)
impl<T1: Denc, T2: Denc> Denc for (T1, T2) {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.0.encode(buf, features)?;
        self.1.encode(buf, features)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        let first = T1::decode(buf, features)?;
        let second = T2::decode(buf, features)?;
        Ok((first, second))
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        let size1 = self.0.encoded_size(features)?;
        let size2 = self.1.encoded_size(features)?;
        Some(size1 + size2)
    }
}

// 3-element tuple
impl<T1: Denc, T2: Denc, T3: Denc> Denc for (T1, T2, T3) {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.0.encode(buf, features)?;
        self.1.encode(buf, features)?;
        self.2.encode(buf, features)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Ok((
            T1::decode(buf, features)?,
            T2::decode(buf, features)?,
            T3::decode(buf, features)?,
        ))
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        Some(
            self.0.encoded_size(features)?
                + self.1.encoded_size(features)?
                + self.2.encoded_size(features)?,
        )
    }
}

// 4-element tuple
impl<T1: Denc, T2: Denc, T3: Denc, T4: Denc> Denc for (T1, T2, T3, T4) {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.0.encode(buf, features)?;
        self.1.encode(buf, features)?;
        self.2.encode(buf, features)?;
        self.3.encode(buf, features)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Ok((
            T1::decode(buf, features)?,
            T2::decode(buf, features)?,
            T3::decode(buf, features)?,
            T4::decode(buf, features)?,
        ))
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        Some(
            self.0.encoded_size(features)?
                + self.1.encoded_size(features)?
                + self.2.encoded_size(features)?
                + self.3.encoded_size(features)?,
        )
    }
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
        // Use MaybeUninit for safety
        use std::mem::MaybeUninit;

        let mut array: [MaybeUninit<T>; N] = unsafe { MaybeUninit::uninit().assume_init() };

        for elem in &mut array {
            *elem = MaybeUninit::new(T::decode(buf, features)?);
        }

        // SAFETY: All elements have been initialized
        Ok(unsafe { std::mem::transmute_copy::<_, [T; N]>(&array) })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(T::SIZE * N)
    }
}

impl<T: FixedSize, const N: usize> FixedSize for [T; N] {
    const SIZE: usize = T::SIZE * N;
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test that the traits are properly defined and can be used
    #[test]
    fn test_trait_definitions() {
        // This test verifies that the trait definitions compile
        // and have the expected associated constants

        // We can't instantiate the traits directly, but we can verify
        // they exist and have the right structure through type checking
        fn _assert_denc_trait<T: Denc>() {
            // Verify the trait has the expected methods
            let _: fn(&T, &mut bytes::BytesMut, u64) -> Result<(), RadosError> = T::encode;
            let _: fn(&mut bytes::Bytes, u64) -> Result<T, RadosError> = T::decode;
            let _: fn(&T, u64) -> Option<usize> = T::encoded_size;
        }

        fn _assert_versioned_encode_trait<T: VersionedEncode>() {
            // Verify the trait has the expected methods
            let _: fn(&T, u64) -> u8 = T::encoding_version;
            let _: fn(&T, u64) -> u8 = T::compat_version;
        }
    }

    #[test]
    fn test_versioned_encode_constants() {
        // Test that we can check the FEATURE_DEPENDENT constant
        // through a generic function
        fn check_feature_dependent<T: VersionedEncode>() -> bool {
            T::FEATURE_DEPENDENT
        }

        // This just verifies the constant exists and is accessible
        // We'll test actual implementations in later commits
        let _ = check_feature_dependent::<DummyVersioned>;
    }

    // Dummy type for testing trait bounds
    struct DummyVersioned;

    impl VersionedEncode for DummyVersioned {
        fn encoding_version(&self, _features: u64) -> u8 {
            1
        }

        fn compat_version(&self, _features: u64) -> u8 {
            1
        }

        fn encode_content<B: BufMut>(
            &self,
            _buf: &mut B,
            _features: u64,
            _version: u8,
        ) -> Result<(), RadosError> {
            Ok(())
        }

        fn decode_content<B: Buf>(
            _buf: &mut B,
            _features: u64,
            _version: u8,
            _compat_version: u8,
        ) -> Result<Self, RadosError> {
            Ok(DummyVersioned)
        }
    }

    // ============= Primitive Type Tests =============

    #[test]
    fn test_u8_roundtrip() {
        let mut buf = bytes::BytesMut::new();
        let val: u8 = 42;
        val.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 1);
        let decoded = u8::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn test_u16_roundtrip() {
        let mut buf = bytes::BytesMut::new();
        let val: u16 = 0x1234;
        val.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 2);
        // Verify little-endian encoding
        assert_eq!(&buf[..], &[0x34, 0x12]);
        let decoded = u16::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn test_u32_roundtrip() {
        let mut buf = bytes::BytesMut::new();
        let val: u32 = 0x12345678;
        val.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 4);
        // Verify little-endian encoding
        assert_eq!(&buf[..], &[0x78, 0x56, 0x34, 0x12]);
        let decoded = u32::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn test_u64_roundtrip() {
        let mut buf = bytes::BytesMut::new();
        let val: u64 = 0x123456789ABCDEF0;
        val.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 8);
        let decoded = u64::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn test_i8_roundtrip() {
        let mut buf = bytes::BytesMut::new();
        let val: i8 = -42;
        val.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 1);
        let decoded = i8::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn test_i16_roundtrip() {
        let mut buf = bytes::BytesMut::new();
        let val: i16 = -1234;
        val.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 2);
        let decoded = i16::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn test_i32_roundtrip() {
        let mut buf = bytes::BytesMut::new();
        let val: i32 = -123456;
        val.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 4);
        let decoded = i32::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn test_i64_roundtrip() {
        let mut buf = bytes::BytesMut::new();
        let val: i64 = -123456789012345;
        val.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 8);
        let decoded = i64::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, val);
    }

    #[test]
    fn test_bool_roundtrip() {
        let mut buf = bytes::BytesMut::new();
        true.encode(&mut buf, 0).unwrap();
        false.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 2);
        assert!(bool::decode(&mut buf, 0).unwrap());
        assert!(!bool::decode(&mut buf, 0).unwrap());
    }

    #[test]
    fn test_f32_roundtrip() {
        let mut buf = bytes::BytesMut::new();
        let val: f32 = 3.14159;
        val.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 4);
        let decoded = f32::decode(&mut buf, 0).unwrap();
        assert!((decoded - val).abs() < 0.00001);
    }

    #[test]
    fn test_f64_roundtrip() {
        let mut buf = bytes::BytesMut::new();
        let val: f64 = 3.141592653589793;
        val.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 8);
        let decoded = f64::decode(&mut buf, 0).unwrap();
        assert!((decoded - val).abs() < 0.0000000000001);
    }

    #[test]
    fn test_fixed_size_constants() {
        assert_eq!(u8::SIZE, 1);
        assert_eq!(u16::SIZE, 2);
        assert_eq!(u32::SIZE, 4);
        assert_eq!(u64::SIZE, 8);
        assert_eq!(i8::SIZE, 1);
        assert_eq!(i16::SIZE, 2);
        assert_eq!(i32::SIZE, 4);
        assert_eq!(i64::SIZE, 8);
        assert_eq!(bool::SIZE, 1);
        assert_eq!(f32::SIZE, 4);
        assert_eq!(f64::SIZE, 8);
    }

    // ============= Collection Type Tests =============

    #[test]
    fn test_string_roundtrip() {
        let mut buf = bytes::BytesMut::new();
        let s = String::from("hello world");
        s.encode(&mut buf, 0).unwrap();
        // Length (4 bytes) + data (11 bytes) = 15 bytes
        assert_eq!(buf.len(), 15);
        let decoded = String::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, s);
    }

    #[test]
    fn test_string_empty() {
        let mut buf = bytes::BytesMut::new();
        let s = String::from("");
        s.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 4); // Just the length field
        let decoded = String::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, s);
    }

    #[test]
    fn test_string_unicode() {
        let mut buf = bytes::BytesMut::new();
        let s = String::from("Hello 世界 🌍");
        s.encode(&mut buf, 0).unwrap();
        let decoded = String::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, s);
    }

    #[test]
    fn test_vec_u32_roundtrip() {
        let mut buf = bytes::BytesMut::new();
        let vec = vec![1u32, 2, 3, 4, 5];
        vec.encode(&mut buf, 0).unwrap();
        // Length (4 bytes) + 5 * u32 (20 bytes) = 24 bytes
        assert_eq!(buf.len(), 24);
        let decoded = Vec::<u32>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, vec);
    }

    #[test]
    fn test_vec_empty() {
        let mut buf = bytes::BytesMut::new();
        let vec: Vec<u32> = vec![];
        vec.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 4); // Just the length field
        let decoded = Vec::<u32>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, vec);
    }

    #[test]
    fn test_vec_string_roundtrip() {
        let mut buf = bytes::BytesMut::new();
        let vec = vec![
            String::from("hello"),
            String::from("world"),
            String::from("test"),
        ];
        vec.encode(&mut buf, 0).unwrap();
        let decoded = Vec::<String>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, vec);
    }

    #[test]
    fn test_nested_vec() {
        let mut buf = bytes::BytesMut::new();
        let vec = vec![vec![1u32, 2], vec![3, 4, 5], vec![6]];
        vec.encode(&mut buf, 0).unwrap();
        let decoded = Vec::<Vec<u32>>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, vec);
    }

    #[test]
    fn test_option_some() {
        let mut buf = bytes::BytesMut::new();
        let opt = Some(42u32);
        opt.encode(&mut buf, 0).unwrap();
        // bool (1 byte) + u32 (4 bytes) = 5 bytes
        assert_eq!(buf.len(), 5);
        let decoded = Option::<u32>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, opt);
    }

    #[test]
    fn test_option_none() {
        let mut buf = bytes::BytesMut::new();
        let opt: Option<u32> = None;
        opt.encode(&mut buf, 0).unwrap();
        // Just bool (1 byte)
        assert_eq!(buf.len(), 1);
        let decoded = Option::<u32>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, opt);
    }

    #[test]
    fn test_option_string() {
        let mut buf = bytes::BytesMut::new();
        let opt = Some(String::from("test"));
        opt.encode(&mut buf, 0).unwrap();
        let decoded = Option::<String>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, opt);
    }

    #[test]
    fn test_option_vec() {
        let mut buf = bytes::BytesMut::new();
        let opt = Some(vec![1u32, 2, 3]);
        opt.encode(&mut buf, 0).unwrap();
        let decoded = Option::<Vec<u32>>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, opt);
    }

    #[test]
    fn test_vec_option() {
        let mut buf = bytes::BytesMut::new();
        let vec = vec![Some(1u32), None, Some(3), Some(4)];
        vec.encode(&mut buf, 0).unwrap();
        let decoded = Vec::<Option<u32>>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, vec);
    }

    #[test]
    fn test_complex_nested() {
        let mut buf = bytes::BytesMut::new();
        let data = vec![
            Some(vec![String::from("a"), String::from("b")]),
            None,
            Some(vec![String::from("c")]),
        ];
        data.encode(&mut buf, 0).unwrap();
        let decoded = Vec::<Option<Vec<String>>>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_string_encoded_size() {
        let s = String::from("test");
        assert_eq!(s.encoded_size(0), Some(8)); // 4 + 4
    }

    #[test]
    fn test_vec_encoded_size() {
        let vec = vec![1u32, 2, 3];
        assert_eq!(vec.encoded_size(0), Some(16)); // 4 + 3*4
    }

    #[test]
    fn test_option_encoded_size() {
        let some_val = Some(42u32);
        assert_eq!(some_val.encoded_size(0), Some(5)); // 1 + 4

        let none_val: Option<u32> = None;
        assert_eq!(none_val.encoded_size(0), Some(1)); // 1
    }

    #[test]
    fn test_vec_large() {
        let mut buf = bytes::BytesMut::new();
        let vec: Vec<u32> = (0..1000).collect();
        vec.encode(&mut buf, 0).unwrap();
        let decoded = Vec::<u32>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, vec);
    }

    #[test]
    fn test_string_long() {
        let mut buf = bytes::BytesMut::new();
        let s = "a".repeat(10000);
        s.encode(&mut buf, 0).unwrap();
        let decoded = String::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, s);
    }

    // ============= Complex Collection Type Tests =============

    #[test]
    fn test_btreemap_roundtrip() {
        let mut buf = bytes::BytesMut::new();
        let mut map = BTreeMap::new();
        map.insert(1u32, String::from("one"));
        map.insert(2u32, String::from("two"));
        map.insert(3u32, String::from("three"));
        map.encode(&mut buf, 0).unwrap();
        let decoded = BTreeMap::<u32, String>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, map);
    }

    #[test]
    fn test_hashmap_roundtrip() {
        let mut buf = bytes::BytesMut::new();
        let mut map = HashMap::new();
        map.insert(String::from("a"), 1u32);
        map.insert(String::from("b"), 2u32);
        map.insert(String::from("c"), 3u32);
        map.encode(&mut buf, 0).unwrap();
        let decoded = HashMap::<String, u32>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, map);
    }

    #[test]
    fn test_btreemap_empty() {
        let mut buf = bytes::BytesMut::new();
        let map: BTreeMap<u32, String> = BTreeMap::new();
        map.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 4); // Just the length field
        let decoded = BTreeMap::<u32, String>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, map);
    }

    #[test]
    fn test_tuple2_roundtrip() {
        let mut buf = bytes::BytesMut::new();
        let tuple = (42u32, String::from("test"));
        tuple.encode(&mut buf, 0).unwrap();
        let decoded = <(u32, String)>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, tuple);
    }

    #[test]
    fn test_tuple3_roundtrip() {
        let mut buf = bytes::BytesMut::new();
        let tuple = (1u32, 2u64, true);
        tuple.encode(&mut buf, 0).unwrap();
        let decoded = <(u32, u64, bool)>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, tuple);
    }

    #[test]
    fn test_tuple4_roundtrip() {
        let mut buf = bytes::BytesMut::new();
        let tuple = (1u8, 2u16, 3u32, 4u64);
        tuple.encode(&mut buf, 0).unwrap();
        let decoded = <(u8, u16, u32, u64)>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, tuple);
    }

    #[test]
    fn test_array_u32_roundtrip() {
        let mut buf = bytes::BytesMut::new();
        let arr: [u32; 5] = [1, 2, 3, 4, 5];
        arr.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 20); // 5 * 4 bytes
        let decoded = <[u32; 5]>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, arr);
    }

    #[test]
    fn test_array_bool_roundtrip() {
        let mut buf = bytes::BytesMut::new();
        let arr: [bool; 3] = [true, false, true];
        arr.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 3);
        let decoded = <[bool; 3]>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, arr);
    }

    #[test]
    fn test_array_fixed_size() {
        assert_eq!(<[u32; 10]>::SIZE, 40);
        assert_eq!(<[u8; 16]>::SIZE, 16);
        assert_eq!(<[bool; 5]>::SIZE, 5);
    }

    #[test]
    fn test_nested_map() {
        let mut buf = bytes::BytesMut::new();
        let mut outer = BTreeMap::new();
        let mut inner1 = BTreeMap::new();
        inner1.insert(1u32, String::from("a"));
        let mut inner2 = BTreeMap::new();
        inner2.insert(2u32, String::from("b"));
        outer.insert(String::from("first"), inner1);
        outer.insert(String::from("second"), inner2);
        outer.encode(&mut buf, 0).unwrap();
        let decoded = BTreeMap::<String, BTreeMap<u32, String>>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, outer);
    }

    #[test]
    fn test_map_with_vec_values() {
        let mut buf = bytes::BytesMut::new();
        let mut map = BTreeMap::new();
        map.insert(1u32, vec![1u32, 2, 3]);
        map.insert(2u32, vec![4, 5]);
        map.encode(&mut buf, 0).unwrap();
        let decoded = BTreeMap::<u32, Vec<u32>>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, map);
    }

    #[test]
    fn test_tuple_with_collections() {
        let mut buf = bytes::BytesMut::new();
        let tuple = (vec![1u32, 2, 3], String::from("test"));
        tuple.encode(&mut buf, 0).unwrap();
        let decoded = <(Vec<u32>, String)>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, tuple);
    }

    #[test]
    fn test_btreemap_encoded_size() {
        let mut map = BTreeMap::new();
        map.insert(1u32, 2u32);
        map.insert(3u32, 4u32);
        // 4 (length) + 2 * (4 + 4) = 20
        assert_eq!(map.encoded_size(0), Some(20));
    }

    #[test]
    fn test_tuple_encoded_size() {
        let tuple = (42u32, true);
        assert_eq!(tuple.encoded_size(0), Some(5)); // 4 + 1
    }

    #[test]
    fn test_array_encoded_size() {
        let arr: [u32; 3] = [1, 2, 3];
        assert_eq!(arr.encoded_size(0), Some(12)); // 3 * 4
    }
}
