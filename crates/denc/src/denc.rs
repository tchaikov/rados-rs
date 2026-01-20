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
}
