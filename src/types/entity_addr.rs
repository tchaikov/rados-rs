use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::error::{Error, Result};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::fmt;
use std::hash::{Hash, Hasher};
use serde::{Deserialize, Serialize};
use rand;

/// Address types matching Ceph's entity_addr_t
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u32)]
pub enum EntityAddrType {
    None = 0,
    Legacy = 1,  // legacy msgr1 protocol (ceph jewel and older)
    Msgr2 = 2,   // msgr2 protocol (new in ceph kraken)
    Any = 3,     // ambiguous
    Cidr = 4,
}

impl Default for EntityAddrType {
    fn default() -> Self {
        EntityAddrType::Msgr2
    }
}

impl TryFrom<u32> for EntityAddrType {
    type Error = Error;

    fn try_from(value: u32) -> Result<Self> {
        match value {
            0 => Ok(EntityAddrType::None),
            1 => Ok(EntityAddrType::Legacy),
            2 => Ok(EntityAddrType::Msgr2),
            3 => Ok(EntityAddrType::Any),
            4 => Ok(EntityAddrType::Cidr),
            _ => Err(Error::Protocol(format!("Invalid entity address type: {}", value))),
        }
    }
}

/// Ceph entity address structure matching entity_addr_t
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityAddr {
    pub addr_type: EntityAddrType,
    pub nonce: u32,
    pub addr: SocketAddr,
}

impl EntityAddr {
    /// Size of encoded EntityAddr for IPv4 (type + nonce + sockaddr_len + sockaddr_in)
    pub const ENCODED_SIZE_IPV4: usize = 4 + 4 + 4 + 16; // = 28 bytes
    
    /// Size of encoded EntityAddr for IPv6 (type + nonce + sockaddr_len + sockaddr_in6)
    pub const ENCODED_SIZE_IPV6: usize = 4 + 4 + 4 + 28; // = 40 bytes
    
    /// Backward compatibility - conservative estimate
    pub const ENCODED_SIZE: usize = Self::ENCODED_SIZE_IPV6;
    
    /// AF_INET (IPv4)
    pub const AF_INET: u8 = 2;
    
    /// AF_INET6 (IPv6)
    pub const AF_INET6: u8 = 10;

    pub fn new(addr: SocketAddr, nonce: u32) -> Self {
        Self {
            addr_type: EntityAddrType::default(),
            nonce,
            addr,
        }
    }

    pub fn new_with_type(addr_type: EntityAddrType, nonce: u32, addr: SocketAddr) -> Self {
        Self {
            addr_type,
            nonce,
            addr,
        }
    }

    pub fn new_with_random_nonce(addr: SocketAddr) -> Self {
        Self::new(addr, rand::random::<u32>())
    }

    /// Encode entity address to bytes following Ceph protocol
    pub fn encode(&self) -> Result<Bytes> {
        let mut buf = BytesMut::new();
        
        // Type (4 bytes, little-endian)
        buf.put_u32_le(self.addr_type as u32);
        
        // Nonce (4 bytes, little-endian)
        buf.put_u32_le(self.nonce);
        
        // Calculate sockaddr length and encode it
        let sockaddr_len = match self.addr.ip() {
            IpAddr::V4(_) => 16u32, // sizeof(sockaddr_in) = 16
            IpAddr::V6(_) => 28u32, // sizeof(sockaddr_in6) = 28  
        };
        
        // Sockaddr length (4 bytes, little-endian)
        buf.put_u32_le(sockaddr_len);
        
        // Address family and data in sockaddr format
        match self.addr.ip() {
            IpAddr::V4(ipv4) => {
                // Family (2 bytes, little-endian)
                buf.put_u16_le(Self::AF_INET as u16);
                
                // Port (2 bytes, big-endian for network order)
                buf.put_u16(self.addr.port());
                
                // IPv4 address (4 bytes, network order)
                buf.extend_from_slice(&ipv4.octets());
                
                // Padding for sockaddr_in (8 bytes of zeros)
                buf.extend_from_slice(&[0u8; 8]);
            }
            IpAddr::V6(ipv6) => {
                // Family (2 bytes, little-endian)
                buf.put_u16_le(Self::AF_INET6 as u16);
                
                // Port (2 bytes, big-endian for network order)
                buf.put_u16(self.addr.port());
                
                // Flow info (4 bytes, zero for now)
                buf.put_u32_le(0);
                
                // IPv6 address (16 bytes, network order)
                buf.extend_from_slice(&ipv6.octets());
                
                // Scope id (4 bytes, zero for now)
                buf.put_u32_le(0);
            }
        }
        
        Ok(buf.freeze())
    }

    /// Decode entity address from bytes following Ceph protocol
    pub fn decode(mut src: Bytes) -> Result<Self> {
        if src.len() < 4 + 4 + 4 { // type + nonce + sockaddr_len minimum
            return Err(Error::Deserialization("EntityAddr too short".into()));
        }

        // Type (4 bytes, little-endian)
        let addr_type = EntityAddrType::try_from(src.get_u32_le())?;
        
        // Nonce (4 bytes, little-endian)
        let nonce = src.get_u32_le();
        
        // Sockaddr length (4 bytes, little-endian)
        let sockaddr_len = src.get_u32_le();
        
        if (sockaddr_len as usize) > src.len() {
            return Err(Error::Deserialization("Sockaddr data too short".into()));
        }
        
        if sockaddr_len == 0 {
            return Err(Error::Deserialization("Empty sockaddr".into()));
        }
        
        // Family (2 bytes, little-endian)
        let family = src.get_u16_le();
        
        let (ip, port) = match family {
            2 => { // AF_INET
                if sockaddr_len < 16 {
                    return Err(Error::Deserialization("IPv4 sockaddr too short".into()));
                }
                
                // Port (2 bytes, big-endian)
                let port = src.get_u16();
                
                // IPv4 address (4 bytes, network order)
                if src.len() < 4 {
                    return Err(Error::Deserialization("IPv4 address too short".into()));
                }
                let mut octets = [0u8; 4];
                src.copy_to_slice(&mut octets);
                let ip = IpAddr::V4(Ipv4Addr::from(octets));
                
                // Skip padding bytes
                let padding_len = sockaddr_len as usize - 8; // 2 + 2 + 4 = 8
                if src.len() >= padding_len {
                    src.advance(padding_len);
                }
                
                (ip, port)
            }
            10 => { // AF_INET6
                if sockaddr_len < 28 {
                    return Err(Error::Deserialization("IPv6 sockaddr too short".into()));
                }
                
                // Port (2 bytes, big-endian)
                let port = src.get_u16();
                
                // Flow info (4 bytes, skip)
                src.advance(4);
                
                // IPv6 address (16 bytes, network order)
                if src.len() < 16 {
                    return Err(Error::Deserialization("IPv6 address too short".into()));
                }
                let mut octets = [0u8; 16];
                src.copy_to_slice(&mut octets);
                let ip = IpAddr::V6(Ipv6Addr::from(octets));
                
                // Skip scope id (4 bytes)
                if src.len() >= 4 {
                    src.advance(4);
                }
                
                (ip, port)
            }
            _ => {
                return Err(Error::Protocol(format!("Unsupported address family: {}", family)));
            }
        };

        let addr = SocketAddr::new(ip, port);

        Ok(Self {
            addr_type,
            nonce,
            addr,
        })
    }

    /// Get the encoded size for this address
    pub fn encoded_size(&self) -> usize {
        match self.addr.ip() {
            IpAddr::V4(_) => Self::ENCODED_SIZE_IPV4,
            IpAddr::V6(_) => Self::ENCODED_SIZE_IPV6,
        }
    }
}

impl Hash for EntityAddr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.addr_type.hash(state);
        self.nonce.hash(state);
        self.addr.hash(state);
    }
}

impl fmt::Display for EntityAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{}", self.addr, self.nonce)
    }
}

impl From<SocketAddr> for EntityAddr {
    fn from(addr: SocketAddr) -> Self {
        Self::new_with_random_nonce(addr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr};

    #[test]
    fn test_entity_addr_encode_decode_ipv4() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6789);
        let entity_addr = EntityAddr::new(EntityAddrType::Msgr2, 12345, addr);
        
        let encoded = entity_addr.encode().unwrap();
        let decoded = EntityAddr::decode(encoded).unwrap();
        
        assert_eq!(entity_addr, decoded);
    }

    #[test]
    fn test_entity_addr_display() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 80);
        let entity_addr = EntityAddr::new(EntityAddrType::Msgr2, 42, addr);
        
        assert_eq!(format!("{}", entity_addr), "192.168.1.1:80#42");
    }
}