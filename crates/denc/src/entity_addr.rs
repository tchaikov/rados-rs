//! EntityAddr implementation using Denc for better performance
//!
//! This is a migration from the old Denc trait to the new Denc trait,
//! which provides zero-allocation encoding by writing directly to buffers.

use crate::constants::sockaddr::{AF_INET, AF_INET6, LEGACY_ENTITY_ADDR_SIZE, STORAGE_SIZE};
use crate::denc::Denc;
use crate::error::RadosError;
use crate::features::CephFeatures;
use bytes::{Buf, BufMut};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, PartialOrd, Ord)]
pub enum EntityAddrType {
    #[default]
    None = 0,
    Legacy = 1,
    Msgr2 = 2,
    Any = 3,
    Cidr = 4,
}

impl TryFrom<u32> for EntityAddrType {
    type Error = RadosError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(EntityAddrType::None),
            1 => Ok(EntityAddrType::Legacy),
            2 => Ok(EntityAddrType::Msgr2),
            3 => Ok(EntityAddrType::Any),
            4 => Ok(EntityAddrType::Cidr),
            _ => Err(RadosError::InvalidData(format!(
                "Invalid EntityAddrType value: {}",
                value
            ))),
        }
    }
}

// EntityAddrType is just a u32 enum, so we can implement Denc for it
impl Denc for EntityAddrType {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        let val = *self as u32;
        Denc::encode(&val, buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        let val = <u32 as Denc>::decode(buf, features)?;
        EntityAddrType::try_from(val)
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(4)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EntityAddr {
    pub addr_type: EntityAddrType,
    pub nonce: u32,
    pub sockaddr_data: Vec<u8>,
}

impl EntityAddr {
    pub fn new() -> Self {
        Self {
            addr_type: EntityAddrType::None,
            nonce: 0,
            sockaddr_data: Vec::new(),
        }
    }

    /// Format sockaddr_data as IP:port string (matching ceph-dencoder output)
    fn format_addr(&self) -> String {
        if self.sockaddr_data.len() < 8 {
            return "(unrecognized address family 0)".to_string();
        }

        // Parse sockaddr structure
        // Bytes [0-1]: address family (little-endian u16)
        let af = u16::from_le_bytes([self.sockaddr_data[0], self.sockaddr_data[1]]);

        match af {
            AF_INET => {
                // AF_INET (IPv4)
                // Bytes [2-3]: port (big-endian)
                let port = u16::from_be_bytes([self.sockaddr_data[2], self.sockaddr_data[3]]);
                // Bytes [4-7]: IPv4 address
                if self.sockaddr_data.len() >= 8 {
                    format!(
                        "{}.{}.{}.{}:{}",
                        self.sockaddr_data[4],
                        self.sockaddr_data[5],
                        self.sockaddr_data[6],
                        self.sockaddr_data[7],
                        port
                    )
                } else {
                    "(unrecognized address family 0)".to_string()
                }
            }
            AF_INET6 => {
                // AF_INET6 (IPv6)
                // Bytes [2-3]: port (big-endian)
                let port = u16::from_be_bytes([self.sockaddr_data[2], self.sockaddr_data[3]]);
                // Bytes [8-23]: IPv6 address (16 bytes)
                if self.sockaddr_data.len() >= 24 {
                    let addr_bytes = &self.sockaddr_data[8..24];
                    format!("[{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}]:{}",
                        addr_bytes[0], addr_bytes[1], addr_bytes[2], addr_bytes[3],
                        addr_bytes[4], addr_bytes[5], addr_bytes[6], addr_bytes[7],
                        addr_bytes[8], addr_bytes[9], addr_bytes[10], addr_bytes[11],
                        addr_bytes[12], addr_bytes[13], addr_bytes[14], addr_bytes[15],
                        port
                    )
                } else {
                    "(unrecognized address family 0)".to_string()
                }
            }
            _ => "(unrecognized address family 0)".to_string(),
        }
    }

    /// Convert sockaddr_data to SocketAddr
    pub fn to_socket_addr(&self) -> Option<std::net::SocketAddr> {
        use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

        if self.sockaddr_data.len() < 8 {
            return None;
        }

        let af = u16::from_le_bytes([self.sockaddr_data[0], self.sockaddr_data[1]]);

        match af {
            AF_INET => {
                // AF_INET (IPv4)
                let port = u16::from_be_bytes([self.sockaddr_data[2], self.sockaddr_data[3]]);
                if self.sockaddr_data.len() >= 8 {
                    let ip = Ipv4Addr::new(
                        self.sockaddr_data[4],
                        self.sockaddr_data[5],
                        self.sockaddr_data[6],
                        self.sockaddr_data[7],
                    );
                    Some(SocketAddr::new(IpAddr::V4(ip), port))
                } else {
                    None
                }
            }
            AF_INET6 => {
                // AF_INET6 (IPv6)
                let port = u16::from_be_bytes([self.sockaddr_data[2], self.sockaddr_data[3]]);
                if self.sockaddr_data.len() >= 24 {
                    let addr_bytes: [u8; 16] = self.sockaddr_data[8..24].try_into().ok()?;
                    let ip = Ipv6Addr::from(addr_bytes);
                    Some(SocketAddr::new(IpAddr::V6(ip), port))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Returns true if this address is a msgr2 (v2) address.
    pub fn is_msgr2(&self) -> bool {
        matches!(self.addr_type, EntityAddrType::Msgr2)
    }

    /// Returns true if this address is a legacy (v1) address.
    pub fn is_legacy(&self) -> bool {
        matches!(self.addr_type, EntityAddrType::Legacy)
    }

    /// Create from SocketAddr
    pub fn from_socket_addr(addr_type: EntityAddrType, addr: std::net::SocketAddr) -> Self {
        use std::net::IpAddr;

        let mut sockaddr_data = vec![0u8; STORAGE_SIZE];

        match addr.ip() {
            IpAddr::V4(ip) => {
                // AF_INET (little-endian)
                sockaddr_data[0] = AF_INET as u8;
                sockaddr_data[1] = (AF_INET >> 8) as u8;
                // Port (big-endian)
                let port_bytes = addr.port().to_be_bytes();
                sockaddr_data[2] = port_bytes[0];
                sockaddr_data[3] = port_bytes[1];
                // IPv4 address
                let octets = ip.octets();
                sockaddr_data[4..8].copy_from_slice(&octets);
            }
            IpAddr::V6(ip) => {
                // AF_INET6 (little-endian)
                sockaddr_data[0] = AF_INET6 as u8;
                sockaddr_data[1] = (AF_INET6 >> 8) as u8;
                // Port (big-endian)
                let port_bytes = addr.port().to_be_bytes();
                sockaddr_data[2] = port_bytes[0];
                sockaddr_data[3] = port_bytes[1];
                // IPv6 address (starting at offset 8)
                let octets = ip.octets();
                sockaddr_data[8..24].copy_from_slice(&octets);
            }
        }

        Self {
            addr_type,
            nonce: 0,
            sockaddr_data,
        }
    }

    /// Decode legacy format (marker byte already consumed)
    fn decode_legacy<B: Buf>(buf: &mut B) -> Result<Self, RadosError> {
        // The marker is a u32 (4 bytes), but the first byte (0x00) was already consumed
        // We need to skip the remaining 3 bytes
        if buf.remaining() < 3 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for legacy EntityAddr marker".to_string(),
            ));
        }
        buf.advance(3); // Skip remaining 3 bytes of the u32 marker

        if buf.remaining() < 4 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for legacy EntityAddr nonce".to_string(),
            ));
        }

        let nonce = buf.get_u32_le();

        // Read sockaddr_storage (STORAGE_SIZE bytes)
        if buf.remaining() < STORAGE_SIZE {
            return Err(RadosError::Protocol(
                "Insufficient bytes for sockaddr_storage".to_string(),
            ));
        }

        let mut sockaddr_data = vec![0u8; STORAGE_SIZE];
        buf.copy_to_slice(&mut sockaddr_data);

        Ok(Self {
            addr_type: EntityAddrType::Legacy,
            nonce,
            sockaddr_data,
        })
    }

    /// Decode MSG_ADDR2 format (marker byte already consumed)
    fn decode_msgr2<B: Buf>(buf: &mut B) -> Result<Self, RadosError> {
        // Read version header (DECODE_START pattern)
        if buf.remaining() < 6 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for version header".to_string(),
            ));
        }

        let _struct_v = buf.get_u8();
        let _struct_compat = buf.get_u8();
        let struct_len = buf.get_u32_le() as usize;

        if buf.remaining() < struct_len {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes for struct: need {}, have {}",
                struct_len,
                buf.remaining()
            )));
        }

        // Create a limited buffer for the struct content
        let mut content = buf.take(struct_len);

        // Decode content
        let addr_type = <EntityAddrType as Denc>::decode(&mut content, 0)?;
        let nonce = <u32 as Denc>::decode(&mut content, 0)?;
        let elen = <u32 as Denc>::decode(&mut content, 0)? as usize;

        let mut sockaddr_data = Vec::new();
        if elen > 0 {
            if content.remaining() < elen {
                return Err(RadosError::Protocol(
                    "Insufficient sockaddr data".to_string(),
                ));
            }
            sockaddr_data = vec![0u8; elen];
            content.copy_to_slice(&mut sockaddr_data);
        }

        // Note: take() already consumed the bytes from buf, no need to advance

        Ok(Self {
            addr_type,
            nonce,
            sockaddr_data,
        })
    }

    /// Encode in legacy format
    fn encode_legacy<B: BufMut>(&self, buf: &mut B) -> Result<(), RadosError> {
        // Check buffer space: marker (4) + nonce (4) + sockaddr (STORAGE_SIZE) = LEGACY_ENTITY_ADDR_SIZE
        if buf.remaining_mut() < LEGACY_ENTITY_ADDR_SIZE {
            return Err(RadosError::Protocol(format!(
                "Insufficient buffer space for legacy EntityAddr: need {}, have {}",
                LEGACY_ENTITY_ADDR_SIZE,
                buf.remaining_mut()
            )));
        }

        buf.put_u32_le(0); // marker
        buf.put_u32_le(self.nonce);

        // Pad sockaddr_storage to STORAGE_SIZE bytes
        let mut sockaddr = self.sockaddr_data.clone();
        sockaddr.resize(STORAGE_SIZE, 0);
        buf.put_slice(&sockaddr);

        Ok(())
    }

    /// Encode in MSG_ADDR2 format
    fn encode_msgr2<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        // Calculate content size
        let content_size = 4 + 4 + 4 + self.sockaddr_data.len(); // addr_type + nonce + len + data

        // Check buffer space: 1 (marker) + 1 (version) + 1 (compat) + 4 (len) + content
        let total_size = 1 + 1 + 1 + 4 + content_size;
        if buf.remaining_mut() < total_size {
            return Err(RadosError::Protocol(format!(
                "Insufficient buffer space for MSG_ADDR2 EntityAddr: need {}, have {}",
                total_size,
                buf.remaining_mut()
            )));
        }

        buf.put_u8(1); // marker

        // ENCODE_START(1, 1, bl)
        buf.put_u8(1); // version
        buf.put_u8(1); // compat version
        buf.put_u32_le(content_size as u32); // struct length

        // Encode content
        Denc::encode(&self.addr_type, buf, features)?;
        Denc::encode(&self.nonce, buf, features)?;
        Denc::encode(&(self.sockaddr_data.len() as u32), buf, features)?;

        if !self.sockaddr_data.is_empty() {
            buf.put_slice(&self.sockaddr_data);
        }

        Ok(())
    }
}

// Custom Serialize implementation to match ceph-dencoder format
impl Serialize for EntityAddr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("EntityAddr", 3)?;

        // Serialize type as lowercase string
        let type_str = match self.addr_type {
            EntityAddrType::None => "none",
            EntityAddrType::Legacy => "v1",
            EntityAddrType::Msgr2 => "v2",
            EntityAddrType::Any => "any",
            EntityAddrType::Cidr => "cidr",
        };
        state.serialize_field("type", type_str)?;
        state.serialize_field("addr", &self.format_addr())?;
        state.serialize_field("nonce", &self.nonce)?;
        state.end()
    }
}

impl std::fmt::Display for EntityAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let prefix = match self.addr_type {
            EntityAddrType::Msgr2 => "v2:",
            EntityAddrType::Legacy => "v1:",
            _ => "",
        };
        write!(f, "{}{}", prefix, self.format_addr())
    }
}

impl<'de> serde::Deserialize<'de> for EntityAddr {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::{self, MapAccess, Visitor};

        struct EntityAddrVisitor;

        impl<'de> Visitor<'de> for EntityAddrVisitor {
            type Value = EntityAddr;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an EntityAddr map with type, addr, and nonce fields")
            }

            fn visit_map<V: MapAccess<'de>>(self, mut map: V) -> Result<EntityAddr, V::Error> {
                let mut addr_type = EntityAddrType::None;
                let mut addr_str = String::new();
                let mut nonce = 0u32;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "type" => {
                            let s: String = map.next_value()?;
                            addr_type = match s.as_str() {
                                "none" => EntityAddrType::None,
                                "v1" => EntityAddrType::Legacy,
                                "v2" => EntityAddrType::Msgr2,
                                "any" => EntityAddrType::Any,
                                "cidr" => EntityAddrType::Cidr,
                                _ => {
                                    return Err(de::Error::custom(format!(
                                        "unknown addr type: {s}"
                                    )))
                                }
                            };
                        }
                        "addr" => {
                            addr_str = map.next_value()?;
                        }
                        "nonce" => {
                            nonce = map.next_value()?;
                        }
                        _ => {
                            let _ = map.next_value::<de::IgnoredAny>()?;
                        }
                    }
                }

                let sockaddr_data =
                    if let Ok(socket_addr) = addr_str.parse::<std::net::SocketAddr>() {
                        EntityAddr::from_socket_addr(addr_type, socket_addr).sockaddr_data
                    } else {
                        Vec::new()
                    };

                Ok(EntityAddr {
                    addr_type,
                    nonce,
                    sockaddr_data,
                })
            }
        }

        deserializer.deserialize_map(EntityAddrVisitor)
    }
}

impl Denc for EntityAddr {
    const FEATURE_DEPENDENT: bool = true;

    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        if (features & CephFeatures::MSG_ADDR2.bits()) == 0 {
            self.encode_legacy(buf)
        } else {
            self.encode_msgr2(buf, features)
        }
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 1 {
            return Err(RadosError::Protocol("Empty EntityAddr".to_string()));
        }

        let marker = buf.get_u8();
        if marker == 0 {
            // Legacy format
            Self::decode_legacy(buf)
        } else if marker == 1 {
            // MSG_ADDR2 format
            Self::decode_msgr2(buf)
        } else {
            Err(RadosError::Protocol(format!(
                "Unknown EntityAddr marker: {}",
                marker
            )))
        }
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        if (features & CephFeatures::MSG_ADDR2.bits()) == 0 {
            // Legacy: marker (4) + nonce (4) + sockaddr (STORAGE_SIZE) = LEGACY_ENTITY_ADDR_SIZE
            Some(LEGACY_ENTITY_ADDR_SIZE)
        } else {
            // MSG_ADDR2: marker (1) + version (1) + compat (1) + len (4) + content
            // Content: addr_type (4) + nonce (4) + len (4) + sockaddr_data
            Some(1 + 1 + 1 + 4 + 4 + 4 + 4 + self.sockaddr_data.len())
        }
    }
}

/// EntityAddrvec - a vector of EntityAddr (entity_addrvec_t in C++)
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntityAddrvec {
    pub addrs: Vec<EntityAddr>,
}

impl EntityAddrvec {
    pub fn new() -> Self {
        Self { addrs: Vec::new() }
    }

    pub fn with_addr(addr: EntityAddr) -> Self {
        Self { addrs: vec![addr] }
    }

    /// Returns true if any address is a msgr2 (v2) address.
    pub fn has_msgr2(&self) -> bool {
        self.addrs.iter().any(|a| a.is_msgr2())
    }

    /// Returns the first msgr2 (v2) address, if any.
    pub fn get_msgr2(&self) -> Option<&EntityAddr> {
        self.addrs.iter().find(|a| a.is_msgr2())
    }
}

// Manual Denc implementation for EntityAddrvec
impl crate::denc::Denc for EntityAddrvec {
    const FEATURE_DEPENDENT: bool = true;

    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        if (features & CephFeatures::MSG_ADDR2.bits()) == 0 {
            // Legacy format: encode a single legacy entity_addr_t
            if let Some(legacy_addr) = self.addrs.first() {
                legacy_addr.encode(buf, 0)?;
            }
        } else {
            // MSG_ADDR2 format: marker byte + vector
            buf.put_u8(2); // Marker byte to indicate MSG_ADDR2 format

            // Encode the number of addresses
            buf.put_u32_le(self.addrs.len() as u32);

            // Encode each address
            for addr in &self.addrs {
                addr.encode(buf, features)?;
            }
        }

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 1 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for EntityAddrvec marker".to_string(),
            ));
        }

        // Read the marker byte
        let marker = buf.get_u8();

        match marker {
            0 | 1 => {
                // Legacy format - single address
                // Put the marker byte back by creating a new buffer with it prepended
                let mut temp = bytes::BytesMut::with_capacity(1 + buf.remaining());
                temp.put_u8(marker);
                temp.put(buf.chunk());
                buf.advance(buf.remaining());

                let addr = EntityAddr::decode(&mut temp, 0)?;
                Ok(EntityAddrvec { addrs: vec![addr] })
            }
            2 => {
                // MSG_ADDR2 format - vector of addresses
                if buf.remaining() < 4 {
                    return Err(RadosError::Protocol(
                        "Insufficient bytes for EntityAddrvec count".to_string(),
                    ));
                }

                let count = buf.get_u32_le() as usize;
                let mut addrs = Vec::with_capacity(count);

                for _ in 0..count {
                    addrs.push(EntityAddr::decode(buf, features)?);
                }

                Ok(EntityAddrvec { addrs })
            }
            _ => Err(RadosError::Protocol(format!(
                "Invalid EntityAddrvec marker: {}",
                marker
            ))),
        }
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        if (features & CephFeatures::MSG_ADDR2.bits()) == 0 {
            // Legacy format
            self.addrs.first().and_then(|addr| addr.encoded_size(0))
        } else {
            // MSG_ADDR2 format: marker (1) + count (4) + addresses
            let mut size = 5;
            for addr in &self.addrs {
                size += addr.encoded_size(features)?;
            }
            Some(size)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_entity_addr_legacy_roundtrip() {
        let mut buf = BytesMut::new();

        let addr = EntityAddr {
            addr_type: EntityAddrType::Legacy,
            nonce: 0x12345678,
            sockaddr_data: vec![1, 2, 3, 4],
        };

        // Encode with legacy features (no MSG_ADDR2)
        Denc::encode(&addr, &mut buf, 0).unwrap();

        // Should be LEGACY_ENTITY_ADDR_SIZE bytes (4 marker + 4 nonce + STORAGE_SIZE sockaddr)
        assert_eq!(buf.len(), LEGACY_ENTITY_ADDR_SIZE);

        // Decode
        let decoded = <EntityAddr as Denc>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded.addr_type, EntityAddrType::Legacy);
        assert_eq!(decoded.nonce, 0x12345678);
        assert_eq!(decoded.sockaddr_data.len(), STORAGE_SIZE);
        assert_eq!(&decoded.sockaddr_data[0..4], &[1, 2, 3, 4]);
    }

    #[test]
    fn test_entity_addr_msgr2_roundtrip() {
        let mut buf = BytesMut::new();

        let addr = EntityAddr {
            addr_type: EntityAddrType::Msgr2,
            nonce: 0xABCDEF01,
            sockaddr_data: vec![10, 20, 30, 40, 50],
        };

        // Encode with MSG_ADDR2 feature
        Denc::encode(&addr, &mut buf, CephFeatures::MSG_ADDR2.bits()).unwrap();

        // Decode
        let decoded = <EntityAddr as Denc>::decode(&mut buf, CephFeatures::MSG_ADDR2.bits()).unwrap();
        assert_eq!(decoded.addr_type, EntityAddrType::Msgr2);
        assert_eq!(decoded.nonce, 0xABCDEF01);
        assert_eq!(decoded.sockaddr_data, vec![10, 20, 30, 40, 50]);
    }

    #[test]
    fn test_entity_addr_encoded_size() {
        let addr = EntityAddr {
            addr_type: EntityAddrType::None,
            nonce: 0,
            sockaddr_data: vec![1, 2, 3],
        };

        // Legacy size: 4 + 4 + STORAGE_SIZE = LEGACY_ENTITY_ADDR_SIZE
        assert_eq!(
            <EntityAddr as Denc>::encoded_size(&addr, 0),
            Some(LEGACY_ENTITY_ADDR_SIZE)
        );

        // MSG_ADDR2 size: 1 + 1 + 1 + 4 + 4 + 4 + 4 + 3 = 22
        assert_eq!(
            <EntityAddr as Denc>::encoded_size(&addr, CephFeatures::MSG_ADDR2.bits()),
            Some(22)
        );
    }
}
