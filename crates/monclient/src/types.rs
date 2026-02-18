//! Common types used throughout the MonClient

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Re-export `auth::types::EntityName` as the canonical entity name type.
///
/// This replaces the previous `monclient`-specific `EntityName` (which used
/// `String`-based fields) with the canonical `auth::types::EntityName`
/// (which uses a numeric `u32` entity type and a `String` ID).
///
/// The `auth::EntityName` already provides all the functionality needed by
/// `monclient`: `FromStr` parsing (e.g., "client.admin"), `Display`, `Debug`,
/// and `Clone`.
pub use auth::types::EntityName;

/// Result of a monitor command
#[derive(Debug, Clone)]
pub struct CommandResult {
    /// Return code (0 = success)
    pub retval: i32,
    /// String output
    pub outs: String,
    /// Binary output
    pub outbl: Bytes,
}

impl CommandResult {
    pub fn new(retval: i32, outs: String, outbl: Bytes) -> Self {
        Self {
            retval,
            outs,
            outbl,
        }
    }

    pub fn is_success(&self) -> bool {
        self.retval == 0
    }
}

/// Entity address vector (supports multiple protocol versions)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntityAddrVec {
    pub addrs: Vec<EntityAddr>,
}

impl EntityAddrVec {
    pub fn new() -> Self {
        Self { addrs: Vec::new() }
    }

    pub fn with_addr(addr: EntityAddr) -> Self {
        Self { addrs: vec![addr] }
    }

    pub fn has_msgr2(&self) -> bool {
        self.addrs.iter().any(|a| a.is_msgr2())
    }

    pub fn get_msgr2(&self) -> Option<&EntityAddr> {
        self.addrs.iter().find(|a| a.is_msgr2())
    }
}

impl Default for EntityAddrVec {
    fn default() -> Self {
        Self::new()
    }
}

/// Entity address (IP:port with protocol version)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntityAddr {
    pub addr_type: AddrType,
    pub nonce: u32,
    pub addr: std::net::SocketAddr,
}

impl EntityAddr {
    pub fn new(addr_type: AddrType, addr: std::net::SocketAddr) -> Self {
        Self {
            addr_type,
            nonce: 0,
            addr,
        }
    }

    pub fn is_msgr2(&self) -> bool {
        matches!(self.addr_type, AddrType::Msgr2)
    }

    pub fn is_legacy(&self) -> bool {
        matches!(self.addr_type, AddrType::Legacy)
    }
}

impl std::fmt::Display for EntityAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.addr_type {
            AddrType::Legacy => write!(f, "v1:{}", self.addr),
            AddrType::Msgr2 => write!(f, "v2:{}", self.addr),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AddrType {
    Legacy,
    Msgr2,
}
