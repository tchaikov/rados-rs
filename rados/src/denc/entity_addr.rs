//! EntityAddr and EntityAddrvec wire-format encoding/decoding.
//!
//! Supports both legacy (v1) and modern MSG_ADDR2 (v2) address formats.

use crate::denc::codec::Denc;
use crate::denc::constants::sockaddr::{AF_INET, AF_INET6, STORAGE_SIZE};
use crate::denc::error::RadosError;
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
                "Invalid EntityAddrType value: {value}"
            ))),
        }
    }
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EntityAddr {
    pub addr_type: EntityAddrType,
    pub nonce: u32,
    /// Raw `sockaddr_storage` bytes (128 bytes, same size as C++ `sockaddr_storage`).
    /// Bytes beyond the actual address family size are always zeroed.
    pub sockaddr_data: [u8; STORAGE_SIZE],
}

impl Default for EntityAddr {
    fn default() -> Self {
        Self {
            addr_type: EntityAddrType::default(),
            nonce: 0,
            sockaddr_data: [0u8; STORAGE_SIZE],
        }
    }
}

impl EntityAddr {
    /// Parse address family and port from sockaddr_data.
    fn parse_af_port(&self) -> Option<(u16, u16)> {
        let af = u16::from_le_bytes([self.sockaddr_data[0], self.sockaddr_data[1]]);
        if af == 0 {
            return None;
        }
        let port = u16::from_be_bytes([self.sockaddr_data[2], self.sockaddr_data[3]]);
        Some((af, port))
    }

    /// Format sockaddr_data as IP:port string (matching ceph-dencoder output)
    fn format_addr(&self) -> String {
        const UNRECOGNIZED: &str = "(unrecognized address family 0)";

        let Some((af, port)) = self.parse_af_port() else {
            return UNRECOGNIZED.to_string();
        };

        match af {
            AF_INET => {
                format!(
                    "{}.{}.{}.{}:{}",
                    self.sockaddr_data[4],
                    self.sockaddr_data[5],
                    self.sockaddr_data[6],
                    self.sockaddr_data[7],
                    port
                )
            }
            AF_INET6 => {
                let b = &self.sockaddr_data[8..24];
                format!(
                    "[{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}]:{}",
                    b[0],
                    b[1],
                    b[2],
                    b[3],
                    b[4],
                    b[5],
                    b[6],
                    b[7],
                    b[8],
                    b[9],
                    b[10],
                    b[11],
                    b[12],
                    b[13],
                    b[14],
                    b[15],
                    port
                )
            }
            _ => UNRECOGNIZED.to_string(),
        }
    }

    /// Convert sockaddr_data to SocketAddr
    pub fn to_socket_addr(&self) -> Option<std::net::SocketAddr> {
        use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

        let (af, port) = self.parse_af_port()?;

        match af {
            AF_INET => {
                let ip = Ipv4Addr::new(
                    self.sockaddr_data[4],
                    self.sockaddr_data[5],
                    self.sockaddr_data[6],
                    self.sockaddr_data[7],
                );
                Some(SocketAddr::new(IpAddr::V4(ip), port))
            }
            AF_INET6 => {
                let addr_bytes: [u8; 16] = self.sockaddr_data[8..24].try_into().ok()?;
                let ip = Ipv6Addr::from(addr_bytes);
                Some(SocketAddr::new(IpAddr::V6(ip), port))
            }
            _ => None,
        }
    }

    /// The address as C++ `entity_addr_t::get_legacy_str` prints it, which
    /// is how `cls_lock` dumps a locker: `a.b.c.d:port/nonce`,
    /// `[ipv6]:port/nonce` with glibc's `inet_ntop` compression, or
    /// `(unrecognized address family N)/nonce`. The address type is not
    /// printed.
    pub fn legacy_str(&self) -> String {
        use std::net::{Ipv4Addr, Ipv6Addr};

        let d = &self.sockaddr_data;
        let af = u16::from_le_bytes([d[0], d[1]]);
        let port = u16::from_be_bytes([d[2], d[3]]);
        let sockaddr = match af {
            AF_INET => format!("{}:{port}", Ipv4Addr::new(d[4], d[5], d[6], d[7])),
            AF_INET6 => {
                let mut octets = [0u8; 16];
                octets.copy_from_slice(&d[8..24]);
                let ip = Ipv6Addr::from(octets);
                let seg = ip.segments();
                // glibc prints an IPv4-compatible address (`::a.b.c.d`) in
                // dotted form, which Rust's Display compresses as hex.
                let text = if seg[..6].iter().all(|&s| s == 0) && seg[6] != 0 {
                    format!("::{}", Ipv4Addr::new(d[20], d[21], d[22], d[23]))
                } else {
                    ip.to_string()
                };
                format!("[{text}]:{port}")
            }
            _ => format!("(unrecognized address family {af})"),
        };
        format!("{sockaddr}/{}", self.nonce)
    }

    /// Returns true if this address is a msgr2 (v2) address.
    pub fn is_msgr2(&self) -> bool {
        matches!(self.addr_type, EntityAddrType::Msgr2)
    }

    /// Returns true if this address is a legacy (v1) address.
    pub fn is_legacy(&self) -> bool {
        matches!(self.addr_type, EntityAddrType::Legacy)
    }

    /// Return a copy normalised to TYPE_ANY, as Nautilus+ stores all blocklist
    /// entries with type ANY regardless of the original connection type.
    pub fn as_type_any(&self) -> Self {
        Self {
            addr_type: EntityAddrType::Any,
            ..*self
        }
    }

    /// Return a copy with port and nonce zeroed out — used for whole-IP
    /// blocklist checks (`ceph osd blocklist add <ip>:0/0`).
    pub fn as_ip_only(&self) -> Self {
        let mut data = self.sockaddr_data;
        // bytes 2-3 are the port in network byte order; zero them out
        data[2] = 0;
        data[3] = 0;
        Self {
            addr_type: EntityAddrType::Any,
            nonce: 0,
            sockaddr_data: data,
        }
    }

    /// Number of `sockaddr_data` bytes on the wire, as C++
    /// `entity_addr_t::get_sockaddr_len`: any family other than `AF_INET`
    /// and `AF_INET6`, `AF_UNSPEC` included, gets `sizeof(u)`, 28.
    fn sockaddr_len(&self) -> usize {
        let af = u16::from_le_bytes([self.sockaddr_data[0], self.sockaddr_data[1]]);
        match af {
            AF_INET => 16,
            _ => 28,
        }
    }

    /// Create from SocketAddr
    pub fn from_socket_addr(addr_type: EntityAddrType, addr: std::net::SocketAddr) -> Self {
        use std::net::IpAddr;

        let mut sockaddr_data = [0u8; STORAGE_SIZE];

        // Write address family and port (common to both V4 and V6)
        let af = match addr.ip() {
            IpAddr::V4(_) => AF_INET,
            IpAddr::V6(_) => AF_INET6,
        };
        sockaddr_data[0..2].copy_from_slice(&af.to_le_bytes());
        sockaddr_data[2..4].copy_from_slice(&addr.port().to_be_bytes());

        // Write IP address bytes
        match addr.ip() {
            IpAddr::V4(ip) => sockaddr_data[4..8].copy_from_slice(&ip.octets()),
            IpAddr::V6(ip) => sockaddr_data[8..24].copy_from_slice(&ip.octets()),
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

        let nonce = <u32 as Denc>::decode(buf, 0)?;

        // Read sockaddr_storage (STORAGE_SIZE bytes)
        if buf.remaining() < STORAGE_SIZE {
            return Err(RadosError::Protocol(
                "Insufficient bytes for sockaddr_storage".to_string(),
            ));
        }

        let mut sockaddr_data = [0u8; STORAGE_SIZE];
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

        let struct_v = buf.get_u8();
        let struct_compat = buf.get_u8();
        let struct_len = buf.get_u32_le() as usize;

        // Version / compat validation (mirrors VersionedEncode::decode_versioned)
        const MAX_VERSION: u8 = 1;
        if struct_compat > MAX_VERSION {
            return Err(RadosError::Codec(crate::denc::CodecError::VersionTooNew {
                got: struct_compat,
                max: MAX_VERSION,
                type_name: "EntityAddr (MSG_ADDR2)",
            }));
        }
        if struct_compat > struct_v {
            return Err(RadosError::Codec(
                crate::denc::CodecError::InvalidVersionHeader {
                    type_name: "EntityAddr (MSG_ADDR2)",
                    compat: struct_compat,
                    version: struct_v,
                },
            ));
        }

        // DoS protection: entity_addr is small; reject anything over 64 KiB
        const MAX_STRUCT_LEN: usize = 64 << 10;
        if struct_len > MAX_STRUCT_LEN {
            return Err(RadosError::InvalidData(format!(
                "EntityAddr MSG_ADDR2 struct_len {struct_len} exceeds maximum {MAX_STRUCT_LEN}",
            )));
        }

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

        if content.remaining() < elen {
            return Err(RadosError::Protocol(
                "Insufficient sockaddr data".to_string(),
            ));
        }
        if elen > STORAGE_SIZE {
            return Err(RadosError::InvalidData(format!(
                "EntityAddr sockaddr elen {elen} exceeds storage size {STORAGE_SIZE}",
            )));
        }
        let mut sockaddr_data = [0u8; STORAGE_SIZE];
        content.copy_to_slice(&mut sockaddr_data[..elen]);

        // DECODE_FINISH: skip any trailing bytes for forward compatibility
        // (take() shares the buffer; unconsumed bytes must be advanced past)
        content.advance(content.remaining());

        Ok(Self {
            addr_type,
            nonce,
            sockaddr_data,
        })
    }

    /// Encode in MSG_ADDR2 format
    fn encode_msgr2<B: BufMut>(&self, buf: &mut B) -> Result<(), RadosError> {
        let elen = self.sockaddr_len();
        let content_size = 4 + 4 + 4 + elen; // addr_type + nonce + len + data

        buf.put_u8(1); // marker

        // ENCODE_START(1, 1, bl)
        buf.put_u8(1); // version
        buf.put_u8(1); // compat version
        buf.put_u32_le(content_size as u32); // struct length

        // Encode content
        Denc::encode(&self.addr_type, buf, 0)?;
        Denc::encode(&self.nonce, buf, 0)?;
        Denc::encode(&(elen as u32), buf, 0)?;
        buf.put_slice(&self.sockaddr_data[..elen]);

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
                                    )));
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
                        [0u8; STORAGE_SIZE]
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
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        self.encode_msgr2(buf)
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 1 {
            return Err(RadosError::Protocol("Empty EntityAddr".to_string()));
        }

        match buf.get_u8() {
            0 => Self::decode_legacy(buf),
            1 => Self::decode_msgr2(buf),
            marker => Err(RadosError::Protocol(format!(
                "Unknown EntityAddr marker: {marker}"
            ))),
        }
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        // MSG_ADDR2: marker (1) + version (1) + compat (1) + len (4) + content
        // Content: addr_type (4) + nonce (4) + len (4) + sockaddr bytes (family-specific)
        Some(1 + 1 + 1 + 4 + 4 + 4 + 4 + self.sockaddr_len())
    }
}

/// EntityAddrvec - a vector of EntityAddr (entity_addrvec_t in C++)
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntityAddrvec {
    pub addrs: Vec<EntityAddr>,
}

impl EntityAddrvec {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_addr(addr: EntityAddr) -> Self {
        Self { addrs: vec![addr] }
    }

    /// Returns the first msgr2 (v2) address, if any.
    pub fn get_msgr2(&self) -> Option<&EntityAddr> {
        self.addrs.iter().find(|a| a.is_msgr2())
    }

    /// Returns true if this addrvec contains `addr` (exact match including
    /// type, nonce, and sockaddr bytes).
    ///
    /// Mirrors `entity_addrvec_t::contains` in Ceph's `src/msg/msg_types.h`.
    /// Used during msgr2 session establishment to validate that the peer's
    /// advertised addresses include the one we actually dialed — a
    /// defense-in-depth check against misrouting during DNS / topology
    /// changes. Both Ceph's C++ `handle_server_ident` and the Linux kernel
    /// `process_server_ident` perform this check and fault the connection
    /// on mismatch.
    pub fn contains(&self, addr: &EntityAddr) -> bool {
        self.addrs.iter().any(|a| a == addr)
    }
}

impl Denc for EntityAddrvec {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        // Quincy+ peers always use the addrvec marker on encode.
        buf.put_u8(2);
        Denc::encode(&(self.addrs.len() as u32), buf, 0)?;

        for addr in &self.addrs {
            addr.encode(buf, 0)?;
        }

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 1 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for EntityAddrvec marker".to_string(),
            ));
        }

        let marker = buf.get_u8();

        match marker {
            // Single-address legacy or msgr2 format
            0 => Ok(EntityAddrvec {
                addrs: vec![EntityAddr::decode_legacy(buf)?],
            }),
            1 => Ok(EntityAddrvec {
                addrs: vec![EntityAddr::decode_msgr2(buf)?],
            }),
            // MSG_ADDR2 format - vector of addresses
            2 => {
                let count = <u32 as Denc>::decode(buf, 0)? as usize;
                let mut addrs = Vec::with_capacity(count.min(32));

                for _ in 0..count {
                    addrs.push(EntityAddr::decode(buf, features)?);
                }

                Ok(EntityAddrvec { addrs })
            }
            _ => Err(RadosError::Protocol(format!(
                "Invalid EntityAddrvec marker: {marker}"
            ))),
        }
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        // MSG_ADDR2 format: marker (1) + Vec<EntityAddr> (4-byte count + addresses)
        Some(1 + self.addrs.encoded_size(features)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_entity_addr_legacy_decode_compatibility() {
        let mut buf = BytesMut::new();
        let mut sockaddr_data = vec![0u8; STORAGE_SIZE];
        sockaddr_data[..4].copy_from_slice(&[1, 2, 3, 4]);

        buf.put_u32_le(0);
        buf.put_u32_le(0x12345678);
        buf.put_slice(&sockaddr_data);

        let mut bytes = buf.freeze();
        let decoded = <EntityAddr as Denc>::decode(&mut bytes, 0).unwrap();
        assert_eq!(decoded.addr_type, EntityAddrType::Legacy);
        assert_eq!(decoded.nonce, 0x12345678);
        assert_eq!(decoded.sockaddr_data.len(), STORAGE_SIZE);
        assert_eq!(&decoded.sockaddr_data[0..4], &[1, 2, 3, 4]);
    }

    #[test]
    fn test_entity_addr_msgr2_roundtrip() {
        let addr =
            EntityAddr::from_socket_addr(EntityAddrType::Msgr2, "10.20.30.40:50".parse().unwrap());
        let addr = EntityAddr {
            nonce: 0xABCDEF01,
            ..addr
        };

        let mut buf = BytesMut::new();
        Denc::encode(&addr, &mut buf, 0).unwrap();

        let decoded = <EntityAddr as Denc>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, addr);
    }

    #[test]
    fn test_entity_addr_encode_ignores_features() {
        let addr =
            EntityAddr::from_socket_addr(EntityAddrType::Msgr2, "10.20.30.40:50".parse().unwrap());

        let mut modern = BytesMut::new();
        let mut with_msg_addr2 = BytesMut::new();
        Denc::encode(&addr, &mut modern, 0).unwrap();
        Denc::encode(&addr, &mut with_msg_addr2, u64::MAX).unwrap();

        assert_eq!(modern, with_msg_addr2);
        assert_eq!(modern[0], 1);
    }

    #[test]
    fn test_entity_addr_encoded_size() {
        // IPv4 addr: elen=16 → MSG_ADDR2 size: 1+1+1+4+4+4+4+16 = 35
        let ipv4 = EntityAddr::from_socket_addr(EntityAddrType::None, "1.2.3.4:0".parse().unwrap());
        assert_eq!(<EntityAddr as Denc>::encoded_size(&ipv4, 0), Some(35));

        // Unset addr (zeros): elen=28 → size = 1+1+1+4+4+4+4+28 = 47
        let unset = EntityAddr::default();
        assert_eq!(<EntityAddr as Denc>::encoded_size(&unset, 0), Some(47));
    }

    fn unhex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    fn assert_wire(addr: &EntityAddr, hex: &str) {
        let wire = unhex(hex);
        let mut buf = BytesMut::new();
        Denc::encode(addr, &mut buf, 0).unwrap();
        assert_eq!(&buf[..], &wire[..]);
        assert_eq!(
            <EntityAddr as Denc>::encoded_size(addr, 0),
            Some(wire.len())
        );
        let decoded = <EntityAddr as Denc>::decode(&mut &wire[..], 0).unwrap();
        assert_eq!(&decoded, addr);
    }

    // ceph-dencoder v19.2.2 `entity_addr_t` instances 1 and 2 (unset, nonce 0
    // and 1) and a Legacy 127.0.1.2:2 nonce 5.
    #[test]
    fn entity_addr_matches_ceph_dencoder() {
        let zeros = "0".repeat(56);
        assert_wire(
            &EntityAddr::default(),
            &format!("0101012800000000000000000000001c000000{zeros}"),
        );
        assert_wire(
            &EntityAddr {
                nonce: 1,
                ..EntityAddr::default()
            },
            &format!("0101012800000000000000010000001c000000{zeros}"),
        );
        assert_wire(
            &EntityAddr {
                nonce: 5,
                ..EntityAddr::from_socket_addr(
                    EntityAddrType::Legacy,
                    "127.0.1.2:2".parse().unwrap(),
                )
            },
            "0101011c000000010000000500000010000000020000027f0001020000000000000000",
        );
    }

    fn legacy(addr: &str, nonce: u32) -> EntityAddr {
        EntityAddr {
            nonce,
            ..EntityAddr::from_socket_addr(EntityAddrType::Legacy, addr.parse().unwrap())
        }
    }

    #[test]
    fn legacy_str_matches_get_legacy_str() {
        assert_eq!(legacy("127.0.1.2:2", 1).legacy_str(), "127.0.1.2:2/1");
        assert_eq!(
            EntityAddr::default().legacy_str(),
            "(unrecognized address family 0)/0"
        );
        assert_eq!(legacy("127.0.1.2:20", 10).legacy_str(), "127.0.1.2:20/10");
        assert_eq!(
            legacy("172.21.5.153:0", 1725310796).legacy_str(),
            "172.21.5.153:0/1725310796"
        );
        assert_eq!(legacy("[::1]:6789", 7).legacy_str(), "[::1]:6789/7");
        assert_eq!(
            legacy("[2001:db8::1]:6800", 42).legacy_str(),
            "[2001:db8::1]:6800/42"
        );
        assert_eq!(legacy("[::1.2.3.4]:1", 0).legacy_str(), "[::1.2.3.4]:1/0");
        assert_eq!(
            legacy("[::ffff:10.0.0.1]:3", 0).legacy_str(),
            "[::ffff:10.0.0.1]:3/0"
        );
        let mut family_1 = EntityAddr {
            addr_type: EntityAddrType::Legacy,
            ..EntityAddr::default()
        };
        family_1.sockaddr_data[0] = 1;
        assert_eq!(family_1.legacy_str(), "(unrecognized address family 1)/0");
    }

    // The encodings ceph-dencoder v19.2.2 dumped as these strings.
    #[test]
    fn legacy_str_of_ceph_dencoder_encodings() {
        for (hex, expected) in [
            (
                "0101012800000001000000000000001c0000000a000001000000000000000000000000000000000102030400000000",
                "[::1.2.3.4]:1/0",
            ),
            (
                "0101012800000001000000000000001c0000000a0000030000000000000000000000000000ffff0a00000100000000",
                "[::ffff:10.0.0.1]:3/0",
            ),
            (
                "0101012800000001000000070000001c0000000a001a85000000000000000000000000000000000000000100000000",
                "[::1]:6789/7",
            ),
            (
                "01010128000000010000002a0000001c0000000a001a900000000020010db800000000000000000000000100000000",
                "[2001:db8::1]:6800/42",
            ),
            (
                "0101012800000001000000000000001c00000001000000000000000000000000000000000000000000000000000000",
                "(unrecognized address family 1)/0",
            ),
        ] {
            let addr = <EntityAddr as Denc>::decode(&mut &unhex(hex)[..], 0).unwrap();
            assert_eq!(addr.legacy_str(), expected);
        }
    }

    #[test]
    fn test_entity_addrvec_legacy_decode_compatibility() {
        let mut buf = BytesMut::new();
        let mut sockaddr_data = vec![0u8; STORAGE_SIZE];
        sockaddr_data[..4].copy_from_slice(&[9, 8, 7, 6]);

        buf.put_u32_le(0);
        buf.put_u32_le(0x01020304);
        buf.put_slice(&sockaddr_data);

        let mut bytes = buf.freeze();
        let decoded = <EntityAddrvec as Denc>::decode(&mut bytes, 0).unwrap();
        assert_eq!(decoded.addrs.len(), 1);
        assert_eq!(decoded.addrs[0].addr_type, EntityAddrType::Legacy);
        assert_eq!(decoded.addrs[0].nonce, 0x01020304);
        assert_eq!(&decoded.addrs[0].sockaddr_data[0..4], &[9, 8, 7, 6]);
    }

    #[test]
    fn test_entity_addrvec_encode_ignores_features() {
        let addr = EntityAddr {
            nonce: 1,
            ..EntityAddr::from_socket_addr(EntityAddrType::Msgr2, "10.20.30.40:0".parse().unwrap())
        };
        let addrvec = EntityAddrvec::with_addr(addr);

        let mut modern = BytesMut::new();
        let mut with_msg_addr2 = BytesMut::new();
        Denc::encode(&addrvec, &mut modern, 0).unwrap();
        Denc::encode(&addrvec, &mut with_msg_addr2, u64::MAX).unwrap();

        assert_eq!(modern, with_msg_addr2);
        assert_eq!(modern[0], 2);
    }
}
