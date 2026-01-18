//! EntityAddr implementation using DencMut for better performance
//!
//! This is a migration from the old Denc trait to the new DencMut trait,
//! which provides zero-allocation encoding by writing directly to buffers.

use crate::denc_mut::DencMut;
use crate::error::RadosError;
use crate::features::CEPH_FEATURE_MSG_ADDR2;
use bytes::{Buf, BufMut};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, PartialOrd, Ord)]
pub enum EntityAddrType {
    #[default]
    None = 0,
    Legacy = 1,
    Msgr2 = 2,
    Any = 3,
    Cidr = 4,
}

impl From<u32> for EntityAddrType {
    fn from(value: u32) -> Self {
        match value {
            0 => EntityAddrType::None,
            1 => EntityAddrType::Legacy,
            2 => EntityAddrType::Msgr2,
            3 => EntityAddrType::Any,
            4 => EntityAddrType::Cidr,
            _ => EntityAddrType::None,
        }
    }
}

// EntityAddrType is just a u32 enum, so we can implement DencMut for it
impl DencMut for EntityAddrType {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        let val = *self as u32;
        DencMut::encode(&val, buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        let val = <u32 as DencMut>::decode(buf, features)?;
        Ok(EntityAddrType::from(val))
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(4)
    }
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq, PartialOrd, Ord)]
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

        // Read sockaddr_storage (128 bytes)
        if buf.remaining() < 128 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for sockaddr_storage".to_string(),
            ));
        }

        let mut sockaddr_data = vec![0u8; 128];
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
        let addr_type = <EntityAddrType as DencMut>::decode(&mut content, 0)?;
        let nonce = <u32 as DencMut>::decode(&mut content, 0)?;
        let elen = <u32 as DencMut>::decode(&mut content, 0)? as usize;

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
        // Check buffer space: marker (4) + nonce (4) + sockaddr (128) = 136
        if buf.remaining_mut() < 136 {
            return Err(RadosError::Protocol(format!(
                "Insufficient buffer space for legacy EntityAddr: need 136, have {}",
                buf.remaining_mut()
            )));
        }

        buf.put_u32_le(0); // marker
        buf.put_u32_le(self.nonce);

        // Pad sockaddr_storage to 128 bytes
        let mut sockaddr = self.sockaddr_data.clone();
        sockaddr.resize(128, 0);
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
        DencMut::encode(&self.addr_type, buf, features)?;
        DencMut::encode(&self.nonce, buf, features)?;
        DencMut::encode(&(self.sockaddr_data.len() as u32), buf, features)?;

        if !self.sockaddr_data.is_empty() {
            buf.put_slice(&self.sockaddr_data);
        }

        Ok(())
    }
}

impl DencMut for EntityAddr {
    const FEATURE_DEPENDENT: bool = true;

    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        if (features & CEPH_FEATURE_MSG_ADDR2) == 0 {
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
        if (features & CEPH_FEATURE_MSG_ADDR2) == 0 {
            // Legacy: marker (4) + nonce (4) + sockaddr (128) = 136
            Some(136)
        } else {
            // MSG_ADDR2: marker (1) + version (1) + compat (1) + len (4) + content
            // Content: addr_type (4) + nonce (4) + len (4) + sockaddr_data
            Some(1 + 1 + 1 + 4 + 4 + 4 + 4 + self.sockaddr_data.len())
        }
    }
}

// ============= Backward Compatibility: Denc trait implementation =============

use crate::denc::Denc;
use bytes::{Bytes, BytesMut};

impl Denc for EntityAddr {
    const FEATURE_DEPENDENT: bool = true;

    fn encode(&self, features: u64) -> Result<Bytes, RadosError> {
        let size = <Self as DencMut>::encoded_size(self, features).unwrap_or(256);
        let mut buf = BytesMut::with_capacity(size);
        <Self as DencMut>::encode(self, &mut buf, features)?;
        Ok(buf.freeze())
    }

    fn decode(bytes: &mut Bytes) -> Result<Self, RadosError> {
        <Self as DencMut>::decode(bytes, 0)
    }
}

impl Denc for EntityAddrType {
    fn encode(&self, features: u64) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::with_capacity(4);
        <Self as DencMut>::encode(self, &mut buf, features)?;
        Ok(buf.freeze())
    }

    fn decode(bytes: &mut Bytes) -> Result<Self, RadosError> {
        <Self as DencMut>::decode(bytes, 0)
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
        DencMut::encode(&addr, &mut buf, 0).unwrap();

        // Should be 136 bytes (4 marker + 4 nonce + 128 sockaddr)
        assert_eq!(buf.len(), 136);

        // Decode
        let decoded = <EntityAddr as DencMut>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded.addr_type, EntityAddrType::Legacy);
        assert_eq!(decoded.nonce, 0x12345678);
        assert_eq!(decoded.sockaddr_data.len(), 128);
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
        DencMut::encode(&addr, &mut buf, CEPH_FEATURE_MSG_ADDR2).unwrap();

        // Decode
        let decoded = <EntityAddr as DencMut>::decode(&mut buf, CEPH_FEATURE_MSG_ADDR2).unwrap();
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

        // Legacy size: 4 + 4 + 128 = 136
        assert_eq!(<EntityAddr as DencMut>::encoded_size(&addr, 0), Some(136));

        // MSG_ADDR2 size: 1 + 1 + 1 + 4 + 4 + 4 + 4 + 3 = 22
        assert_eq!(
            <EntityAddr as DencMut>::encoded_size(&addr, CEPH_FEATURE_MSG_ADDR2),
            Some(22)
        );
    }
}
