// Re-export types from other modules for consistency
pub use crate::entity_addr::{EntityAddr, EntityAddrType};

use crate::denc::Denc;
use crate::error::RadosError;
use crate::mark_simple_encoding;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::Serialize;
use std::convert::TryFrom;
use std::fmt;

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

impl Denc for EntityType {
    fn decode(bytes: &mut Bytes) -> Result<Self, RadosError> {
        if bytes.remaining() < 1 {
            return Err(RadosError::InvalidData(
                "Insufficient bytes for EntityType".into(),
            ));
        }
        let val = bytes.get_u8();
        EntityType::try_from(val)
    }

    fn encode(&self, _features: u64) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::new();
        buf.put_u8(self.0 as u8);
        Ok(buf.freeze())
    }
}

impl Denc for EntityName {
    fn decode(bytes: &mut Bytes) -> Result<Self, RadosError> {
        if bytes.remaining() < 9 {
            // 1 byte for type + 8 bytes for num
            return Err(RadosError::InvalidData(
                "Insufficient bytes for EntityName".into(),
            ));
        }
        let entity_type = EntityType::decode(bytes)?;
        let num = bytes.get_u64_le();
        Ok(Self::new(entity_type, num))
    }

    fn encode(&self, features: u64) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::new();
        let type_bytes = self.entity_type.encode(features)?;
        buf.extend_from_slice(&type_bytes);
        buf.put_u64_le(self.num);
        Ok(buf.freeze())
    }
}

// ============= Encoding Metadata Registration =============

// Both EntityType and EntityName are simple types (no versioning, no feature dependency)
mark_simple_encoding!(EntityType);
mark_simple_encoding!(EntityName);
