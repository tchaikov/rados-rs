//! Zero-copy encoding/decoding optimizations for network protocol structures
//!
//! This module provides a marker trait for types that are safe for zero-copy
//! encoding/decoding operations. Types marked with ZeroCopyDencode can use
//! direct memory copy on little-endian systems for optimal performance.
//!
//! The derive macro generates Denc implementations that use zero-copy when possible.

/// Marker trait for types that are safe for zero-copy encoding/decoding
///
/// This trait marks types that can be safely transmitted using direct memory
/// copy on little-endian systems (like x86_64, ARM LE). For big-endian systems,
/// field-by-field encoding is used instead.
///
/// Types marked with this trait should:
/// - Be `#[repr(C, packed)]` for predictable memory layout (for packed structs)
/// - Contain only primitive types or other ZeroCopyDencode types
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
/// #[derive(ZeroCopyDencode)]
/// #[repr(C, packed)]
/// struct Inner {
///     a: u32,
///     b: u64,
/// }
///
/// #[derive(ZeroCopyDencode)]
/// #[repr(C, packed)]
/// struct Outer {
///     inner: Inner,  // ✓ Works! Inner implements ZeroCopyDencode
///     c: u16,
/// }
/// ```
pub trait ZeroCopyDencode {
    /// Returns true if this type can use zero-copy optimization on the current platform
    fn can_use_zerocopy() -> bool {
        cfg!(target_endian = "little")
    }
}

// Implement ZeroCopyDencode for all primitive types
impl ZeroCopyDencode for u8 {}
impl ZeroCopyDencode for u16 {}
impl ZeroCopyDencode for u32 {}
impl ZeroCopyDencode for u64 {}
impl ZeroCopyDencode for i8 {}
impl ZeroCopyDencode for i16 {}
impl ZeroCopyDencode for i32 {}
impl ZeroCopyDencode for i64 {}

// Implement ZeroCopyDencode for fixed-size byte arrays
impl<const N: usize> ZeroCopyDencode for [u8; N] {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zerocopy_marker() {
        // Primitives don't implement ZeroCopyDencode directly,
        // but we can test that the trait method exists
        struct TestStruct;
        impl ZeroCopyDencode for TestStruct {}
        assert_eq!(
            TestStruct::can_use_zerocopy(),
            cfg!(target_endian = "little")
        );
    }

    #[test]
    fn test_primitives_are_zerocopy() {
        // Test that primitives implement the marker trait
        assert_eq!(u8::can_use_zerocopy(), cfg!(target_endian = "little"));
        assert_eq!(u16::can_use_zerocopy(), cfg!(target_endian = "little"));
        assert_eq!(u32::can_use_zerocopy(), cfg!(target_endian = "little"));
        assert_eq!(u64::can_use_zerocopy(), cfg!(target_endian = "little"));
        assert_eq!(i8::can_use_zerocopy(), cfg!(target_endian = "little"));
        assert_eq!(i16::can_use_zerocopy(), cfg!(target_endian = "little"));
        assert_eq!(i32::can_use_zerocopy(), cfg!(target_endian = "little"));
        assert_eq!(i64::can_use_zerocopy(), cfg!(target_endian = "little"));
    }

    #[test]
    fn test_arrays_are_zerocopy() {
        // Test that byte arrays implement the marker trait
        assert_eq!(
            <[u8; 16]>::can_use_zerocopy(),
            cfg!(target_endian = "little")
        );
    }
}
