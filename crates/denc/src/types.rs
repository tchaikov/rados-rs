//! Reusable Ceph wire types shared across DENC implementations and clients.
//!
//! This module gathers small but widely reused types such as UUID wrappers,
//! epochs, versions, timestamps, and other identifiers that appear throughout
//! monitor, OSD, and messenger payloads. Keeping them here avoids duplicate
//! definitions across crates and centralizes their DENC behavior.

// Re-export types from other modules for consistency
pub use crate::entity_addr::{EntityAddr, EntityAddrType};
pub use crate::ids::{Epoch, GlobalId, OsdId, PoolId};

use crate::denc::Denc;
use crate::error::RadosError;
use bytes::{Buf, BufMut};
use serde::Serialize;
use std::fmt;
use std::str::FromStr;

// ============= Basic types used across multiple modules =============

/// Filesystem ID - 16-byte UUID
pub type FsId = [u8; 16];

/// Generic version number
pub type Version = u64;

/// Version with epoch (eversion_t in C++)
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, crate::Denc,
)]
#[denc(crate = "crate")]
pub struct EVersion {
    pub version: Version,
    pub epoch: Epoch,
}

impl EVersion {
    pub fn new(version: Version, epoch: Epoch) -> Self {
        EVersion { version, epoch }
    }
}

/// Universal Time structure (utime_t in C++)
///
/// Represents time with second and nanosecond precision.
/// Wire format: u32 sec + u32 nsec
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, crate::Denc,
)]
#[denc(crate = "crate")]
pub struct UTime {
    #[serde(rename = "seconds")]
    pub sec: u32,
    #[serde(rename = "nanoseconds")]
    pub nsec: u32,
}

impl UTime {
    /// Create a new UTime with zero time
    pub const fn zero() -> Self {
        Self { sec: 0, nsec: 0 }
    }

    /// Create a new UTime from seconds and nanoseconds
    pub const fn new(sec: u32, nsec: u32) -> Self {
        Self { sec, nsec }
    }
}

/// UUID structure (uuid_d in C++)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, crate::Denc)]
#[denc(crate = "crate")]
pub struct UuidD {
    pub bytes: [u8; 16],
}

// Custom Serialize implementation to match ceph-dencoder format
impl Serialize for UuidD {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("UuidD", 1)?;
        // Format as UUID string to match ceph-dencoder
        let uuid_str = format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            self.bytes[0], self.bytes[1], self.bytes[2], self.bytes[3],
            self.bytes[4], self.bytes[5], self.bytes[6], self.bytes[7],
            self.bytes[8], self.bytes[9], self.bytes[10], self.bytes[11],
            self.bytes[12], self.bytes[13], self.bytes[14], self.bytes[15]
        );
        state.serialize_field("uuid", &uuid_str)?;
        state.end()
    }
}

impl UuidD {
    pub fn new() -> Self {
        Self { bytes: [0; 16] }
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self { bytes }
    }

    pub fn is_zero(&self) -> bool {
        self.bytes.iter().all(|&b| b == 0)
    }
}

impl std::fmt::Display for UuidD {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            self.bytes[0], self.bytes[1], self.bytes[2], self.bytes[3],
            self.bytes[4], self.bytes[5], self.bytes[6], self.bytes[7],
            self.bytes[8], self.bytes[9], self.bytes[10], self.bytes[11],
            self.bytes[12], self.bytes[13], self.bytes[14], self.bytes[15]
        )
    }
}

impl crate::denc::FixedSize for UuidD {
    const SIZE: usize = 16;
}

impl crate::denc::FixedSize for EVersion {
    const SIZE: usize = 12;
}

impl crate::denc::FixedSize for UTime {
    const SIZE: usize = 8;
}

bitflags::bitflags! {
    /// Ceph entity type flags from `src/include/msgr.h`
    ///
    /// Used both as individual service identifiers (ticket HashMap keys) and
    /// as bitmasks (e.g. `want_keys = EntityType::MON | EntityType::OSD`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct EntityType: u32 {
        const MON    = 0x01;
        const MDS    = 0x02;
        const OSD    = 0x04;
        const CLIENT = 0x08;
        const MGR    = 0x10;
        const AUTH   = 0x20;
    }
}

impl serde::Serialize for EntityType {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u32(self.bits())
    }
}

impl fmt::Display for EntityType {
    /// Formats a single-bit EntityType as its Ceph name (e.g. "mon", "osd").
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                EntityType::MON => "mon",
                EntityType::MDS => "mds",
                EntityType::OSD => "osd",
                EntityType::CLIENT => "client",
                EntityType::MGR => "mgr",
                EntityType::AUTH => "auth",
                _ => "unknown",
            }
        )
    }
}

impl FromStr for EntityType {
    type Err = RadosError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "mon" => Ok(EntityType::MON),
            "mds" => Ok(EntityType::MDS),
            "osd" => Ok(EntityType::OSD),
            "client" => Ok(EntityType::CLIENT),
            "mgr" => Ok(EntityType::MGR),
            "auth" => Ok(EntityType::AUTH),
            _ => Err(RadosError::Protocol(format!("Unknown entity type: {}", s))),
        }
    }
}

impl Denc for EntityType {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.bits().encode(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Ok(EntityType::from_bits_retain(u32::decode(buf, features)?))
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        self.bits().encoded_size(features)
    }
}

/// Canonical entity name (entity_name_t in C++)
///
/// Wire format: u32 (entity_type) + u32 (id_len) + bytes (id)
/// Matches the C++ `entity_name_t` struct in `src/include/entity_name.h`.
///
/// This is the single canonical definition used across the codebase.
/// The `auth` and `monclient` crates import this type directly.
///
/// For the OSD zero-copy packed format (9 bytes: u8 type + u64 num),
/// see `osdclient::types::PackedEntityName`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, crate::Denc)]
#[denc(crate = "crate")]
pub struct EntityName {
    pub entity_type: EntityType,
    pub id: String,
}

impl EntityName {
    pub fn new(entity_type: EntityType, id: impl Into<String>) -> Self {
        Self {
            entity_type,
            id: id.into(),
        }
    }

    pub fn client(id: impl Into<String>) -> Self {
        Self::new(EntityType::CLIENT, id)
    }

    pub fn osd(id: impl Into<String>) -> Self {
        Self::new(EntityType::OSD, id)
    }

    pub fn mon(id: impl Into<String>) -> Self {
        Self::new(EntityType::MON, id)
    }
}

impl fmt::Display for EntityName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.entity_type, self.id)
    }
}

impl FromStr for EntityName {
    type Err = RadosError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (type_str, id) = s
            .split_once('.')
            .ok_or_else(|| RadosError::Protocol(format!("Invalid entity name format: {}", s)))?;
        let entity_type = type_str.parse()?;
        Ok(Self::new(entity_type, id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_utime_serializes_with_ceph_field_names() {
        let value = serde_json::to_value(UTime::new(7, 9)).expect("UTime JSON should serialize");

        assert_eq!(
            value,
            json!({
                "seconds": 7,
                "nanoseconds": 9
            })
        );
    }
}
