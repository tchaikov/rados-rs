//! Zero-copy encoding/decoding optimizations for network protocol structures
//!
//! This module provides traits and implementations for efficient encoding and decoding
//! of network protocol structures using the bytes crate's Buf/BufMut traits.
//!
//! The design is inspired by protocol-codec derive macros but implemented as manual
//! implementations for better control and integration with existing Denc trait.

use bytes::{Buf, BufMut};

/// Error type for zerocopy encoding/decoding operations
#[derive(Debug, Clone, PartialEq)]
pub enum EncodeError {
    /// Not enough space in the buffer
    InsufficientSpace { required: usize, available: usize },
    /// Custom error message
    Custom(String),
}

impl std::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EncodeError::InsufficientSpace {
                required,
                available,
            } => {
                write!(
                    f,
                    "Insufficient buffer space: need {} bytes, have {}",
                    required, available
                )
            }
            EncodeError::Custom(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for EncodeError {}

/// Error type for zerocopy decoding operations
#[derive(Debug, Clone, PartialEq)]
pub enum DecodeError {
    /// Unexpected end of input
    UnexpectedEof { expected: usize, available: usize },
    /// Invalid data encountered
    InvalidData(String),
    /// Custom error message
    Custom(String),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::UnexpectedEof {
                expected,
                available,
            } => {
                write!(
                    f,
                    "Unexpected EOF: expected {} bytes, have {}",
                    expected, available
                )
            }
            DecodeError::InvalidData(msg) => write!(f, "Invalid data: {}", msg),
            DecodeError::Custom(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for DecodeError {}

/// Trait for encoding types to network byte order (little-endian)
pub trait Encode {
    /// Encode this value into the provided buffer
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError>;

    /// Return the size of this value when encoded
    fn encoded_size(&self) -> usize;
}

/// Trait for decoding types from network byte order (little-endian)
pub trait Decode: Sized {
    /// Decode a value from the provided buffer
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError>;
}

/// Combined trait for zero-copy encoding and decoding of POD (Plain Old Data) types
///
/// This trait is designed for types that can be safely transmitted as-is on little-endian
/// systems (like x86_64, ARM LE) using direct memory copy, while providing field-by-field
/// encoding on big-endian systems.
///
/// Types implementing this trait should:
/// - Be `#[repr(C, packed)]` for predictable memory layout
/// - Contain only primitive types or other ZeroCopyDencode types
/// - Match the wire format exactly (including padding)
pub trait ZeroCopyDencode: Encode + Decode {
    /// Returns true if this type can use zero-copy optimization on the current platform
    fn can_use_zerocopy() -> bool {
        cfg!(target_endian = "little")
    }
}

// Implementations for primitive types (little-endian encoding)

impl Encode for u8 {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        if buf.remaining_mut() < 1 {
            return Err(EncodeError::InsufficientSpace {
                required: 1,
                available: buf.remaining_mut(),
            });
        }
        buf.put_u8(*self);
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        1
    }
}

impl Decode for u8 {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        if buf.remaining() < 1 {
            return Err(DecodeError::UnexpectedEof {
                expected: 1,
                available: buf.remaining(),
            });
        }
        Ok(buf.get_u8())
    }
}

impl Encode for u16 {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        if buf.remaining_mut() < 2 {
            return Err(EncodeError::InsufficientSpace {
                required: 2,
                available: buf.remaining_mut(),
            });
        }
        buf.put_u16_le(*self);
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        2
    }
}

impl Decode for u16 {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        if buf.remaining() < 2 {
            return Err(DecodeError::UnexpectedEof {
                expected: 2,
                available: buf.remaining(),
            });
        }
        Ok(buf.get_u16_le())
    }
}

impl Encode for u32 {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        if buf.remaining_mut() < 4 {
            return Err(EncodeError::InsufficientSpace {
                required: 4,
                available: buf.remaining_mut(),
            });
        }
        buf.put_u32_le(*self);
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        4
    }
}

impl Decode for u32 {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        if buf.remaining() < 4 {
            return Err(DecodeError::UnexpectedEof {
                expected: 4,
                available: buf.remaining(),
            });
        }
        Ok(buf.get_u32_le())
    }
}

impl Encode for u64 {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        if buf.remaining_mut() < 8 {
            return Err(EncodeError::InsufficientSpace {
                required: 8,
                available: buf.remaining_mut(),
            });
        }
        buf.put_u64_le(*self);
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        8
    }
}

impl Decode for u64 {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        if buf.remaining() < 8 {
            return Err(DecodeError::UnexpectedEof {
                expected: 8,
                available: buf.remaining(),
            });
        }
        Ok(buf.get_u64_le())
    }
}

// Array implementations for fixed-size byte arrays (common in protocol headers)
impl<const N: usize> Encode for [u8; N] {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        if buf.remaining_mut() < N {
            return Err(EncodeError::InsufficientSpace {
                required: N,
                available: buf.remaining_mut(),
            });
        }
        buf.put_slice(self);
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        N
    }
}

impl<const N: usize> Decode for [u8; N] {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        if buf.remaining() < N {
            return Err(DecodeError::UnexpectedEof {
                expected: N,
                available: buf.remaining(),
            });
        }
        let mut arr = [0u8; N];
        buf.copy_to_slice(&mut arr);
        Ok(arr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_u8_roundtrip() {
        let mut buf = BytesMut::new();
        let value: u8 = 42;
        value.encode(&mut buf).unwrap();
        assert_eq!(value.encoded_size(), 1);
        let decoded = u8::decode(&mut buf.as_ref()).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_u16_roundtrip() {
        let mut buf = BytesMut::new();
        let value: u16 = 0x1234;
        value.encode(&mut buf).unwrap();
        assert_eq!(value.encoded_size(), 2);
        // Verify little-endian encoding
        assert_eq!(&buf[..], &[0x34, 0x12]);
        let decoded = u16::decode(&mut buf.as_ref()).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_u32_roundtrip() {
        let mut buf = BytesMut::new();
        let value: u32 = 0x12345678;
        value.encode(&mut buf).unwrap();
        assert_eq!(value.encoded_size(), 4);
        // Verify little-endian encoding
        assert_eq!(&buf[..], &[0x78, 0x56, 0x34, 0x12]);
        let decoded = u32::decode(&mut buf.as_ref()).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_u64_roundtrip() {
        let mut buf = BytesMut::new();
        let value: u64 = 0x123456789ABCDEF0;
        value.encode(&mut buf).unwrap();
        assert_eq!(value.encoded_size(), 8);
        let decoded = u64::decode(&mut buf.as_ref()).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_array_roundtrip() {
        let mut buf = BytesMut::new();
        let value: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        value.encode(&mut buf).unwrap();
        assert_eq!(value.encoded_size(), 16);
        let decoded = <[u8; 16]>::decode(&mut buf.as_ref()).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_insufficient_space_error() {
        // Use a fixed-size buffer that won't grow
        let mut backing = [0u8; 1];
        let mut buf = &mut backing[..];
        let value: u32 = 0x12345678;
        let result = value.encode(&mut buf);
        assert!(result.is_err());
        match result {
            Err(EncodeError::InsufficientSpace {
                required,
                available,
            }) => {
                assert_eq!(required, 4);
                assert_eq!(available, 1);
            }
            _ => panic!("Expected InsufficientSpace error"),
        }
    }

    #[test]
    fn test_unexpected_eof_error() {
        let buf = &mut &[0x12, 0x34][..];
        let result = u32::decode(buf);
        assert!(result.is_err());
        match result {
            Err(DecodeError::UnexpectedEof {
                expected,
                available,
            }) => {
                assert_eq!(expected, 4);
                assert_eq!(available, 2);
            }
            _ => panic!("Expected UnexpectedEof error"),
        }
    }
}
