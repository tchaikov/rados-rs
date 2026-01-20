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

// ============= Collection Type Implementations =============

// String implementation - encoded as UTF-8 bytes with u32 length prefix
impl Denc for String {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        // Encode length as u32
        let len = self.len() as u32;
        Denc::encode(&len, buf, features)?;

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
        let len = <u32 as Denc>::decode(buf, features)? as usize;

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

// Option<T> implementation - encodes as bool (present) + value if present
impl<T: Denc> Denc for Option<T> {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        match self {
            Some(value) => {
                // Encode true (1) to indicate value is present
                Denc::encode(&true, buf, features)?;
                Denc::encode(value, buf, features)?;
            }
            None => {
                // Encode false (0) to indicate no value
                Denc::encode(&false, buf, features)?;
            }
        }
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        let present = <bool as Denc>::decode(buf, features)?;
        if present {
            Ok(Some(<T as Denc>::decode(buf, features)?))
        } else {
            Ok(None)
        }
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        match self {
            Some(value) => {
                let value_size = Denc::encoded_size(value, features)?;
                Some(1 + value_size) // 1 byte for bool + value size
            }
            None => Some(1), // Just the bool indicating None
        }
    }
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

    #[test]
    fn test_string_roundtrip() {
        let value = "Hello, Ceph!".to_string();
        let mut buf = BytesMut::new();
        value.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 4 + value.len()); // u32 length + content
        let decoded = String::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_string_empty() {
        let value = String::new();
        let mut buf = BytesMut::new();
        value.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 4); // Just the length field
        let decoded = String::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_vec_u32_roundtrip() {
        let vec = vec![1u32, 2, 3, 4, 5];
        let mut buf = BytesMut::new();
        vec.encode(&mut buf, 0).unwrap();
        // u32 length (4 bytes) + 5 * u32 (20 bytes) = 24 bytes
        assert_eq!(buf.len(), 24);
        let decoded = Vec::<u32>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, vec);
    }

    #[test]
    fn test_vec_string_roundtrip() {
        let vec = vec!["hello".to_string(), "world".to_string()];
        let mut buf = BytesMut::new();
        vec.encode(&mut buf, 0).unwrap();
        let decoded = Vec::<String>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, vec);
    }

    #[test]
    fn test_vec_empty() {
        let vec: Vec<u32> = Vec::new();
        let mut buf = BytesMut::new();
        vec.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 4); // Just the length field
        let decoded = Vec::<u32>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, vec);
    }

    #[test]
    fn test_option_some_roundtrip() {
        let value = Some(42u32);
        let mut buf = BytesMut::new();
        value.encode(&mut buf, 0).unwrap();
        // 1 byte bool + 4 bytes u32 = 5 bytes
        assert_eq!(buf.len(), 5);
        let decoded = Option::<u32>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_option_none_roundtrip() {
        let value: Option<u32> = None;
        let mut buf = BytesMut::new();
        value.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 1); // Just the bool
        let decoded = Option::<u32>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_option_string_roundtrip() {
        let value = Some("test".to_string());
        let mut buf = BytesMut::new();
        value.encode(&mut buf, 0).unwrap();
        let decoded = Option::<String>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_nested_collections() {
        // Vec of Vecs
        let value = vec![vec![1u32, 2], vec![3, 4, 5]];
        let mut buf = BytesMut::new();
        value.encode(&mut buf, 0).unwrap();
        let decoded = Vec::<Vec<u32>>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, value);
    }
}
