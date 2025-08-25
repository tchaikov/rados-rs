use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::SocketAddr;
use crate::error::RadosError;
use bytes::{Bytes, BytesMut, BufMut, Buf};
use std::convert::TryFrom;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
            _ => Err(RadosError::ProtocolError(format!("Unknown entity type: {}", value))),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

mod entity_addr;

pub use entity_addr::{EntityAddr, EntityAddrType};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntityInst {
    pub name: EntityName,
    pub addr: EntityAddr,
}

impl EntityInst {
    pub fn new(name: EntityName, addr: EntityAddr) -> Self {
        Self { name, addr }
    }
}

impl fmt::Display for EntityInst {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.addr)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectionState {
    Accepting,
    Connecting,
    Connected,
    Standby,
    Wait,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FeatureSet(u64);

impl FeatureSet {
    pub const EMPTY: Self = Self(0);
    
    pub const MSGR2: Self = Self(1 << 0);
    pub const CRUSH_TUNABLES2: Self = Self(1 << 1);
    pub const CRUSH_TUNABLES3: Self = Self(1 << 2);
    pub const CRUSH_TUNABLES5: Self = Self(1 << 3);
    pub const CRUSH_TUNABLES_HAMMER: Self = Self(1 << 4);
    pub const SERVER_JEWEL: Self = Self(1 << 5);
    pub const SERVER_KRAKEN: Self = Self(1 << 6);
    pub const SERVER_LUMINOUS: Self = Self(1 << 7);
    pub const SERVER_MIMIC: Self = Self(1 << 8);
    pub const SERVER_NAUTILUS: Self = Self(1 << 9);
    pub const SERVER_OCTOPUS: Self = Self(1 << 10);
    pub const SERVER_PACIFIC: Self = Self(1 << 11);
    pub const SERVER_QUINCY: Self = Self(1 << 12);
    pub const COMPRESSION: Self = Self(1 << 13);
    
    pub fn new(value: u64) -> Self {
        Self(value)
    }
    
    pub fn value(&self) -> u64 {
        self.0
    }
    
    pub fn has_feature(&self, feature: FeatureSet) -> bool {
        (self.0 & feature.0) != 0
    }
    
    pub fn union(self, other: FeatureSet) -> Self {
        Self(self.0 | other.0)
    }
    
    pub fn intersection(self, other: FeatureSet) -> Self {
        Self(self.0 & other.0)
    }
    
    pub fn has_compression(&self) -> bool {
        self.has_feature(Self::COMPRESSION)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionInfo {
    pub peer_addr: EntityAddr,
    pub peer_name: Option<EntityName>,
    pub features: FeatureSet,
    pub lossy: bool,
}

pub type GlobalId = u64;