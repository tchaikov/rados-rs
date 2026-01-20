//! Efficient buffer-based encoding/decoding traits
//!
//! This module provides the core `Denc` trait for Ceph's DENC encoding protocol.

use crate::error::RadosError;
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

/// Trait for versioned encoding (ENCODE_START/DECODE_START in C++)
pub trait VersionedEncode: Denc {
    /// Encoding version number
    const ENCODING_VERSION: u8;
    
    /// Minimum compatible version for decoding
    const MIN_COMPAT_VERSION: u8;
}

/// Marker trait for types with compile-time known fixed size
pub trait FixedSize: Denc {
    /// The fixed size in bytes
    const SIZE: usize;
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

// Implement for all integer types
impl_denc_int!(u8, put_u8, get_u8, 1);
impl_denc_int!(u16, put_u16_le, get_u16_le, 2);
impl_denc_int!(u32, put_u32_le, get_u32_le, 4);
impl_denc_int!(u64, put_u64_le, get_u64_le, 8);
impl_denc_int!(i8, put_i8, get_i8, 1);
impl_denc_int!(i16, put_i16_le, get_i16_le, 2);
impl_denc_int!(i32, put_i32_le, get_i32_le, 4);
impl_denc_int!(i64, put_i64_le, get_i64_le, 8);

// bool is encoded as u8 (0 or 1) following C++ convention
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

// Floating point types
impl Denc for f32 {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        if buf.remaining_mut() < 4 {
            return Err(RadosError::Protocol(
                "Insufficient buffer space for f32".to_string(),
            ));
        }
        buf.put_f32_le(*self);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 4 {
            return Err(RadosError::Protocol("Insufficient bytes for f32".to_string()));
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
            return Err(RadosError::Protocol(
                "Insufficient buffer space for f64".to_string(),
            ));
        }
        buf.put_f64_le(*self);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 8 {
            return Err(RadosError::Protocol("Insufficient bytes for f64".to_string()));
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    // Test roundtrip for integers
    #[test]
    fn test_u8_roundtrip() {
        let value: u8 = 42;
        let mut buf = BytesMut::new();
        value.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 1);
        let decoded = u8::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_u16_roundtrip() {
        let value: u16 = 1234;
        let mut buf = BytesMut::new();
        value.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 2);
        let decoded = u16::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_u32_roundtrip() {
        let value: u32 = 123456;
        let mut buf = BytesMut::new();
        value.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 4);
        let decoded = u32::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_u64_roundtrip() {
        let value: u64 = 9876543210;
        let mut buf = BytesMut::new();
        value.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 8);
        let decoded = u64::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_i32_roundtrip() {
        let value: i32 = -12345;
        let mut buf = BytesMut::new();
        value.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 4);
        let decoded = i32::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_i64_roundtrip() {
        let value: i64 = -9876543210;
        let mut buf = BytesMut::new();
        value.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 8);
        let decoded = i64::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_bool_roundtrip() {
        // Test true
        let mut buf = BytesMut::new();
        true.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], 1);
        let decoded = bool::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, true);

        // Test false
        let mut buf = BytesMut::new();
        false.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], 0);
        let decoded = bool::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, false);
    }

    #[test]
    fn test_f32_roundtrip() {
        let value: f32 = 3.14159;
        let mut buf = BytesMut::new();
        value.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 4);
        let decoded = f32::decode(&mut buf, 0).unwrap();
        assert!((decoded - value).abs() < 0.0001);
    }

    #[test]
    fn test_f64_roundtrip() {
        let value: f64 = 3.141592653589793;
        let mut buf = BytesMut::new();
        value.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 8);
        let decoded = f64::decode(&mut buf, 0).unwrap();
        assert!((decoded - value).abs() < 0.0000001);
    }

    #[test]
    fn test_encoded_size() {
        assert_eq!(42u8.encoded_size(0), Some(1));
        assert_eq!(1234u16.encoded_size(0), Some(2));
        assert_eq!(123456u32.encoded_size(0), Some(4));
        assert_eq!(9876543210u64.encoded_size(0), Some(8));
        assert_eq!(true.encoded_size(0), Some(1));
        assert_eq!(3.14f32.encoded_size(0), Some(4));
        assert_eq!(3.14f64.encoded_size(0), Some(8));
    }

    #[test]
    fn test_fixed_size_constants() {
        assert_eq!(u8::SIZE, 1);
        assert_eq!(u16::SIZE, 2);
        assert_eq!(u32::SIZE, 4);
        assert_eq!(u64::SIZE, 8);
        assert_eq!(bool::SIZE, 1);
        assert_eq!(f32::SIZE, 4);
        assert_eq!(f64::SIZE, 8);
    }
}
