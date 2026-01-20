use bytes::{Buf, BufMut};
use std::io;

/// Core encoding/decoding trait for Ceph data structures
/// 
/// This trait provides methods for encoding values to and decoding values from
/// byte buffers, following Ceph's binary encoding format.
pub trait Denc: Sized {
    /// Encode the value into a buffer
    /// 
    /// # Arguments
    /// * `buf` - The buffer to write encoded data into
    /// 
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(io::Error)` if encoding fails
    fn encode<B: BufMut>(&self, buf: &mut B) -> io::Result<()>;
    
    /// Decode a value from a buffer
    /// 
    /// # Arguments
    /// * `buf` - The buffer to read encoded data from
    /// 
    /// # Returns
    /// * `Ok(Self)` containing the decoded value on success
    /// * `Err(io::Error)` if decoding fails or buffer is too short
    fn decode<B: Buf>(buf: &mut B) -> io::Result<Self>;
    
    /// Get the encoded size (if known at compile time)
    /// 
    /// # Returns
    /// * `Some(size)` for fixed-size types
    /// * `None` for variable-size types
    fn encoded_size(&self) -> Option<usize> {
        None
    }
}

/// Marker trait for fixed-size types
/// 
/// Types implementing this trait have a known size at compile time.
/// This allows for optimizations in encoding/decoding.
pub trait FixedSize: Denc {
    /// The size of the encoded representation in bytes
    const SIZE: usize;
}

/// Trait for versioned encoding
/// 
/// Some Ceph data structures include version information in their
/// encoding to support backwards compatibility.
pub trait VersionedEncode: Denc {
    /// Current encoding version
    const VERSION: u8;
    
    /// Minimum compatible version for decoding
    const COMPAT_VERSION: u8;
}
