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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    // Test type to validate trait definitions
    struct TestType(u32);

    impl Denc for TestType {
        fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
            buf.put_u32_le(self.0);
            Ok(())
        }

        fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
            if buf.remaining() < 4 {
                return Err(RadosError::Denc("Insufficient data".to_string()));
            }
            Ok(TestType(buf.get_u32_le()))
        }

        fn encoded_size(&self, _features: u64) -> Option<usize> {
            Some(4)
        }
    }

    #[test]
    fn test_trait_implementation() {
        let value = TestType(42);
        let mut buf = BytesMut::new();
        
        value.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 4);
        
        let decoded = TestType::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded.0, 42);
    }

    #[test]
    fn test_encoded_size() {
        let value = TestType(123);
        assert_eq!(value.encoded_size(0), Some(4));
    }
}
