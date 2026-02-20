//! Zero-copy encoding/decoding optimizations for network protocol structures
//!
//! This module provides a marker trait for types that are safe for zero-copy
//! encoding/decoding operations. Types marked with ZeroCopyDencode can use
//! direct memory copy for optimal performance using the zerocopy crate.
//!
//! The derive macro generates Denc implementations that use zerocopy when possible.

use zerocopy::little_endian::{I16, I32, I64, U16, U32, U64};

/// Marker trait for types that are safe for zero-copy encoding/decoding
///
/// This trait marks types that can be safely transmitted using direct memory
/// copy via the zerocopy crate. Types must implement FromBytes + IntoBytes
/// from the zerocopy crate.
///
/// Types marked with this trait should:
/// - Be `#[repr(C)]` for predictable memory layout
/// - Contain only zerocopy-compatible types (u8, [u8; N], or zerocopy LE types)
/// - Match the wire format exactly (including padding)
///
/// Use the `#[derive(ZeroCopyDencode)]` macro to automatically implement this trait.
///
/// # Compile-time Safety
///
/// The derive macro enforces that all fields implement `ZeroCopyDencode`, catching
/// mistakes at compile time:
///
/// ```compile_fail
/// # use denc::ZeroCopyDencode;
/// #[derive(ZeroCopyDencode)]
/// struct BadStruct {
///     vec: Vec<u8>,  // Error: Vec doesn't implement ZeroCopyDencode
/// }
/// ```
///
/// Valid nested structures work correctly:
///
/// ```
/// # use denc::ZeroCopyDencode;
/// # use zerocopy::{FromBytes, IntoBytes, KnownLayout, Immutable};
/// # use zerocopy::little_endian::U32;
/// #[derive(ZeroCopyDencode, FromBytes, IntoBytes, KnownLayout, Immutable)]
/// #[repr(C)]
/// struct Inner {
///     a: U32,
///     b: u8,
/// }
///
/// #[derive(ZeroCopyDencode, FromBytes, IntoBytes, KnownLayout, Immutable)]
/// #[repr(C)]
/// struct Outer {
///     inner: Inner,  // ✓ Works! Inner implements ZeroCopyDencode
///     c: U16,
/// }
/// ```
pub trait ZeroCopyDencode:
    zerocopy::FromBytes + zerocopy::IntoBytes + zerocopy::KnownLayout + zerocopy::Immutable
{
    /// Returns true if this type can use zero-copy optimization
    fn can_use_zerocopy() -> bool {
        true // Always true — zerocopy handles endianness
    }
}

// Implement ZeroCopyDencode for u8 and byte arrays
impl ZeroCopyDencode for u8 {}
impl<const N: usize> ZeroCopyDencode for [u8; N] {}

// Implement ZeroCopyDencode for zerocopy little-endian types
impl ZeroCopyDencode for U16 {}
impl ZeroCopyDencode for U32 {}
impl ZeroCopyDencode for U64 {}
impl ZeroCopyDencode for I16 {}
impl ZeroCopyDencode for I32 {}
impl ZeroCopyDencode for I64 {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u8_is_zerocopy() {
        assert!(u8::can_use_zerocopy());
    }

    #[test]
    fn test_arrays_are_zerocopy() {
        assert!(<[u8; 16]>::can_use_zerocopy());
    }

    #[test]
    fn test_le_types_are_zerocopy() {
        assert!(U16::can_use_zerocopy());
        assert!(U32::can_use_zerocopy());
        assert!(U64::can_use_zerocopy());
        assert!(I16::can_use_zerocopy());
        assert!(I32::can_use_zerocopy());
        assert!(I64::can_use_zerocopy());
    }
}
