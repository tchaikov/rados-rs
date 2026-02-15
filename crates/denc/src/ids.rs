//! Type-safe ID wrappers for Ceph identifiers
//!
//! This module provides newtype wrappers around primitive types to prevent
//! accidental mixing of different ID types (e.g., OSD IDs vs Pool IDs).

use crate::denc::Denc;
use crate::error::RadosError;
use bytes::{Buf, BufMut};
use serde::Serialize;

/// OSD identifier (newtype wrapper around i32)
///
/// Provides type safety to prevent mixing OSD IDs with other integer types.
/// Negative values have special meanings (e.g., -1 = NONE).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize)]
pub struct OsdId(pub i32);

impl OsdId {
    /// Special value indicating no OSD
    pub const NONE: OsdId = OsdId(-1);

    /// Create a new OSD ID
    pub const fn new(id: i32) -> Self {
        OsdId(id)
    }

    /// Get the raw i32 value
    pub const fn as_i32(self) -> i32 {
        self.0
    }

    /// Convert to usize if non-negative, None otherwise
    pub const fn as_usize(self) -> Option<usize> {
        if self.0 >= 0 {
            Some(self.0 as usize)
        } else {
            None
        }
    }

    /// Check if this is a valid OSD ID (>= 0)
    pub const fn is_valid(self) -> bool {
        self.0 >= 0
    }
}

impl From<i32> for OsdId {
    fn from(id: i32) -> Self {
        OsdId(id)
    }
}

impl From<OsdId> for i32 {
    fn from(id: OsdId) -> Self {
        id.0
    }
}

impl std::fmt::Display for OsdId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Denc for OsdId {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        buf.put_i32_le(self.0);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 4 {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes for OsdId: need 4, have {}",
                buf.remaining()
            )));
        }
        Ok(OsdId(buf.get_i32_le()))
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(4)
    }
}

/// Pool identifier (newtype wrapper around u64)
///
/// Provides type safety to prevent mixing pool IDs with other integer types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize)]
pub struct PoolId(pub u64);

impl PoolId {
    /// Create a new pool ID
    pub const fn new(id: u64) -> Self {
        PoolId(id)
    }

    /// Get the raw u64 value
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Convert to i64 (for compatibility with signed pool IDs in some contexts)
    pub const fn as_i64(self) -> i64 {
        self.0 as i64
    }
}

impl From<u64> for PoolId {
    fn from(id: u64) -> Self {
        PoolId(id)
    }
}

impl From<PoolId> for u64 {
    fn from(id: PoolId) -> Self {
        id.0
    }
}

impl From<i64> for PoolId {
    fn from(id: i64) -> Self {
        PoolId(id as u64)
    }
}

impl std::fmt::Display for PoolId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Denc for PoolId {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        buf.put_u64_le(self.0);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 8 {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes for PoolId: need 8, have {}",
                buf.remaining()
            )));
        }
        Ok(PoolId(buf.get_u64_le()))
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(8)
    }
}

/// Epoch number for versioning cluster state (newtype wrapper around u32)
///
/// Provides type safety to prevent mixing epochs with other integer types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize)]
pub struct Epoch(pub u32);

impl Epoch {
    /// Create a new epoch
    pub const fn new(epoch: u32) -> Self {
        Epoch(epoch)
    }

    /// Get the raw u32 value
    pub const fn as_u32(self) -> u32 {
        self.0
    }

    /// Get the next epoch
    pub const fn next(self) -> Self {
        Epoch(self.0 + 1)
    }
}

impl From<u32> for Epoch {
    fn from(epoch: u32) -> Self {
        Epoch(epoch)
    }
}

impl From<Epoch> for u32 {
    fn from(epoch: Epoch) -> Self {
        epoch.0
    }
}

impl std::fmt::Display for Epoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Denc for Epoch {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        buf.put_u32_le(self.0);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 4 {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes for Epoch: need 4, have {}",
                buf.remaining()
            )));
        }
        Ok(Epoch(buf.get_u32_le()))
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(4)
    }
}

/// Global ID assigned during authentication (newtype wrapper around u64)
///
/// Provides type safety to prevent mixing global IDs with other integer types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize)]
pub struct GlobalId(pub u64);

impl GlobalId {
    /// Create a new global ID
    pub const fn new(id: u64) -> Self {
        GlobalId(id)
    }

    /// Get the raw u64 value
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Check if this is a valid global ID (> 0)
    pub const fn is_valid(self) -> bool {
        self.0 > 0
    }
}

impl From<u64> for GlobalId {
    fn from(id: u64) -> Self {
        GlobalId(id)
    }
}

impl From<GlobalId> for u64 {
    fn from(id: GlobalId) -> Self {
        id.0
    }
}

impl std::fmt::Display for GlobalId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Denc for GlobalId {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        buf.put_u64_le(self.0);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 8 {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes for GlobalId: need 8, have {}",
                buf.remaining()
            )));
        }
        Ok(GlobalId(buf.get_u64_le()))
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_osd_id() {
        let id = OsdId::new(5);
        assert_eq!(id.as_i32(), 5);
        assert_eq!(id.as_usize(), Some(5));
        assert!(id.is_valid());

        let none = OsdId::NONE;
        assert_eq!(none.as_i32(), -1);
        assert_eq!(none.as_usize(), None);
        assert!(!none.is_valid());
    }

    #[test]
    fn test_pool_id() {
        let id = PoolId::new(42);
        assert_eq!(id.as_u64(), 42);
        assert_eq!(id.as_i64(), 42);
    }

    #[test]
    fn test_epoch() {
        let epoch = Epoch::new(10);
        assert_eq!(epoch.as_u32(), 10);
        assert_eq!(epoch.next().as_u32(), 11);
    }

    #[test]
    fn test_global_id() {
        let id = GlobalId::new(12345);
        assert_eq!(id.as_u64(), 12345);
        assert!(id.is_valid());

        let zero = GlobalId::new(0);
        assert!(!zero.is_valid());
    }

    #[test]
    fn test_osd_id_roundtrip() {
        use bytes::BytesMut;
        let mut buf = BytesMut::new();
        let id = OsdId::new(123);
        id.encode(&mut buf, 0).unwrap();
        let decoded = OsdId::decode(&mut buf, 0).unwrap();
        assert_eq!(id, decoded);
    }

    #[test]
    fn test_pool_id_roundtrip() {
        use bytes::BytesMut;
        let mut buf = BytesMut::new();
        let id = PoolId::new(456);
        id.encode(&mut buf, 0).unwrap();
        let decoded = PoolId::decode(&mut buf, 0).unwrap();
        assert_eq!(id, decoded);
    }

    #[test]
    fn test_epoch_roundtrip() {
        use bytes::BytesMut;
        let mut buf = BytesMut::new();
        let epoch = Epoch::new(789);
        epoch.encode(&mut buf, 0).unwrap();
        let decoded = Epoch::decode(&mut buf, 0).unwrap();
        assert_eq!(epoch, decoded);
    }

    #[test]
    fn test_global_id_roundtrip() {
        use bytes::BytesMut;
        let mut buf = BytesMut::new();
        let id = GlobalId::new(999);
        id.encode(&mut buf, 0).unwrap();
        let decoded = GlobalId::decode(&mut buf, 0).unwrap();
        assert_eq!(id, decoded);
    }
}
