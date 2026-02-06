// Re-export types from other modules for consistency
pub use crate::entity_addr::{EntityAddr, EntityAddrType};

use crate::denc::Denc;
use crate::error::RadosError;
use crate::mark_simple_encoding;
use bytes::{Buf, BufMut};
use serde::Serialize;
use std::convert::TryFrom;
use std::fmt;

// ============= Basic types used across multiple modules =============

/// Filesystem ID - 16-byte UUID
pub type FsId = [u8; 16];

/// Epoch number for versioning cluster state
pub type Epoch = u32;

/// Generic version number
pub type Version = u64;

/// Version with epoch (eversion_t in C++)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct EVersion {
    pub version: Version,
    pub epoch: Epoch,
}

impl EVersion {
    pub fn new(version: Version, epoch: Epoch) -> Self {
        EVersion { version, epoch }
    }
}

impl Denc for EVersion {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.version.encode(buf, features)?;
        self.epoch.encode(buf, features)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        let version = Version::decode(buf, features)?;
        let epoch = Epoch::decode(buf, features)?;
        Ok(EVersion { version, epoch })
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        Some(self.version.encoded_size(features)? + self.epoch.encoded_size(features)?)
    }
}

/// Universal Time structure (utime_t in C++)
///
/// Represents time with second and nanosecond precision.
/// Wire format: u32 sec + u32 nsec
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UTime {
    pub sec: u32,
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

impl Denc for UTime {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        buf.put_u32_le(self.sec);
        buf.put_u32_le(self.nsec);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 8 {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes for UTime: need 8, have {}",
                buf.remaining()
            )));
        }
        let sec = buf.get_u32_le();
        let nsec = buf.get_u32_le();
        Ok(UTime { sec, nsec })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(8)
    }
}

// Custom Serialize implementation to match ceph-dencoder format
impl Serialize for UTime {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("UTime", 2)?;
        state.serialize_field("seconds", &self.sec)?;
        state.serialize_field("nanoseconds", &self.nsec)?;
        state.end()
    }
}

/// UUID structure (uuid_d in C++)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
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

impl Denc for UuidD {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        buf.put_slice(&self.bytes);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 16 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for UuidD".to_string(),
            ));
        }
        let mut uuid_bytes = [0u8; 16];
        buf.copy_to_slice(&mut uuid_bytes);
        Ok(UuidD { bytes: uuid_bytes })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(16)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub struct EntityType(u32);

impl EntityType {
    pub const TYPE_MON: Self = Self(0x01);
    pub const TYPE_MDS: Self = Self(0x02);
    pub const TYPE_OSD: Self = Self(0x04);
    pub const TYPE_CLIENT: Self = Self(0x08);
    pub const TYPE_MGR: Self = Self(0x10);

    pub fn new(value: u32) -> Self {
        Self(value)
    }

    pub fn value(&self) -> u32 {
        self.0
    }
}

impl TryFrom<u8> for EntityType {
    type Error = RadosError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::TYPE_MON),
            0x02 => Ok(Self::TYPE_MDS),
            0x04 => Ok(Self::TYPE_OSD),
            0x08 => Ok(Self::TYPE_CLIENT),
            0x10 => Ok(Self::TYPE_MGR),
            _ => Err(RadosError::Protocol(format!(
                "Unknown entity type: {}",
                value
            ))),
        }
    }
}

impl fmt::Display for EntityType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::TYPE_MON => write!(f, "mon"),
            Self::TYPE_MDS => write!(f, "mds"),
            Self::TYPE_OSD => write!(f, "osd"),
            Self::TYPE_CLIENT => write!(f, "client"),
            Self::TYPE_MGR => write!(f, "mgr"),
            _ => write!(f, "unknown({})", self.0),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub struct EntityName {
    pub entity_type: EntityType,
    pub num: u64,
}

impl EntityName {
    pub fn new(entity_type: EntityType, num: u64) -> Self {
        Self { entity_type, num }
    }

    pub fn client(num: u64) -> Self {
        Self::new(EntityType::TYPE_CLIENT, num)
    }

    pub fn osd(num: u64) -> Self {
        Self::new(EntityType::TYPE_OSD, num)
    }

    pub fn mon(num: u64) -> Self {
        Self::new(EntityType::TYPE_MON, num)
    }
}

impl fmt::Display for EntityName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.entity_type, self.num)
    }
}

// Denc implementations for EntityType and EntityName

// ============= Encoding Metadata Registration =============

// Both EntityType and EntityName are simple types (no versioning, no feature dependency)
mark_simple_encoding!(EntityType);
mark_simple_encoding!(EntityName);
