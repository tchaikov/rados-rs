use crate::denc::{Denc, FeatureEncode};
use crate::error::RadosError;
use crate::features::CEPH_FEATURE_MSG_ADDR2;
use crate::{mark_feature_dependent_encoding, mark_simple_encoding};
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

    pub fn decode_legacy(bytes: &mut bytes::Bytes) -> Result<Self, RadosError> {
        use bytes::Buf;

        if bytes.remaining() < 8 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for legacy EntityAddr".to_string(),
            ));
        }

        // Skip marker (already consumed)
        let nonce = bytes.get_u32_le();

        // Read sockaddr_storage (128 bytes)
        if bytes.remaining() < 128 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for sockaddr_storage".to_string(),
            ));
        }

        let mut sockaddr_data = vec![0u8; 128];
        bytes.copy_to_slice(&mut sockaddr_data);

        Ok(Self {
            addr_type: EntityAddrType::Legacy,
            nonce,
            sockaddr_data,
        })
    }

    pub fn decode_msgr2(bytes: &mut bytes::Bytes) -> Result<Self, RadosError> {
        use bytes::Buf;

        if bytes.remaining() < 1 {
            return Err(RadosError::Protocol("Empty EntityAddr msgr2".to_string()));
        }

        // Skip marker (already consumed)

        // Read version header (DECODE_START pattern)
        if bytes.remaining() < 6 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for version header".to_string(),
            ));
        }

        let _struct_v = bytes.get_u8();
        let _struct_compat = bytes.get_u8();
        let struct_len = bytes.get_u32_le() as usize;

        if bytes.remaining() < struct_len {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes for struct: need {}, have {}",
                struct_len,
                bytes.remaining()
            )));
        }

        let mut content = bytes.split_to(struct_len);

        // Decode content based on version
        let addr_type = EntityAddrType::from(content.get_u32_le());
        let nonce = content.get_u32_le();

        let elen = content.get_u32_le() as usize;
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

        Ok(Self {
            addr_type,
            nonce,
            sockaddr_data,
        })
    }
}

impl FeatureEncode for EntityAddr {
    fn encode_with_features(&self, features: u64) -> Result<bytes::Bytes, RadosError> {
        use bytes::{BufMut, BytesMut};

        let mut buf = BytesMut::new();

        if (features & CEPH_FEATURE_MSG_ADDR2) == 0 {
            // Legacy encoding
            buf.put_u32_le(0); // marker
            buf.put_u32_le(self.nonce);

            // Pad sockaddr_storage to 128 bytes
            let mut sockaddr = self.sockaddr_data.clone();
            sockaddr.resize(128, 0);
            buf.extend_from_slice(&sockaddr);
        } else {
            // MSG_ADDR2 encoding
            buf.put_u8(1); // marker

            // ENCODE_START(1, 1, bl)
            buf.put_u8(1); // version
            buf.put_u8(1); // compat version

            let mut content = BytesMut::new();
            content.put_u32_le(self.addr_type as u32);
            content.put_u32_le(self.nonce);
            content.put_u32_le(self.sockaddr_data.len() as u32);

            if !self.sockaddr_data.is_empty() {
                content.extend_from_slice(&self.sockaddr_data);
            }

            buf.put_u32_le(content.len() as u32); // struct length
            buf.extend_from_slice(&content);
        }

        Ok(buf.freeze())
    }

    fn decode_with_features(bytes: &mut bytes::Bytes, _features: u64) -> Result<Self, RadosError> {
        use bytes::Buf;

        if bytes.is_empty() {
            return Err(RadosError::Protocol("Empty EntityAddr".to_string()));
        }

        let marker = bytes.get_u8();
        if marker == 0 {
            // Legacy format
            Self::decode_legacy(bytes)
        } else if marker == 1 {
            // MSG_ADDR2 format
            Self::decode_msgr2(bytes)
        } else {
            Err(RadosError::Protocol(format!(
                "Unknown EntityAddr marker: {}",
                marker
            )))
        }
    }
}

impl Denc for EntityAddr {
    fn encode(&self, features: u64) -> Result<bytes::Bytes, RadosError> {
        self.encode_with_features(features)
    }

    fn decode(bytes: &mut bytes::Bytes) -> Result<Self, RadosError> {
        Self::decode_with_features(bytes, 0)
    }
}

/// EntityAddrvec - a vector of EntityAddr (entity_addrvec_t in C++)
#[derive(Debug, Clone, Default, Serialize)]
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
}

impl Denc for EntityAddrvec {
    fn encode(&self, features: u64) -> Result<bytes::Bytes, RadosError> {
        use bytes::{BufMut, BytesMut};

        let mut buf = BytesMut::new();

        const CEPH_FEATURE_MSG_ADDR2: u64 = 1 << 59;

        // Check if MSG_ADDR2 feature is supported
        if (features & CEPH_FEATURE_MSG_ADDR2) == 0 {
            // Legacy format: encode a single legacy entity_addr_t
            if let Some(legacy_addr) = self.addrs.first() {
                let addr_bytes = legacy_addr.encode(0)?;
                buf.extend_from_slice(&addr_bytes);
            }
        } else {
            // MSG_ADDR2 format: marker byte + vector
            buf.put_u8(2); // Marker byte to indicate MSG_ADDR2 format

            // Encode the number of addresses
            buf.put_u32_le(self.addrs.len() as u32);

            // Encode each address
            for addr in &self.addrs {
                let addr_bytes = addr.encode(features)?;
                buf.extend_from_slice(&addr_bytes);
            }
        }

        Ok(buf.freeze())
    }

    fn decode(bytes: &mut bytes::Bytes) -> Result<Self, RadosError> {
        use bytes::Buf;

        if bytes.remaining() < 1 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for EntityAddrvec marker".to_string(),
            ));
        }

        // Read the marker byte
        let marker = bytes.get_u8();

        match marker {
            0 => {
                // Legacy format: single legacy entity_addr_t (marker already consumed)
                let addr = EntityAddr::decode_legacy(bytes)?;
                Ok(EntityAddrvec { addrs: vec![addr] })
            }
            1 => {
                // MSG_ADDR2 format: single address
                let addr = EntityAddr::decode(bytes)?;
                Ok(EntityAddrvec { addrs: vec![addr] })
            }
            2 => {
                // MSG_ADDR2 format: vector of addresses
                if bytes.remaining() < 4 {
                    return Err(RadosError::Protocol(
                        "Insufficient bytes for EntityAddrvec length".to_string(),
                    ));
                }

                let len = bytes.get_u32_le() as usize;
                let mut addrs = Vec::with_capacity(len);

                for _ in 0..len {
                    addrs.push(EntityAddr::decode(bytes)?);
                }

                Ok(EntityAddrvec { addrs })
            }
            _ => Err(RadosError::Protocol(format!(
                "Invalid EntityAddrvec marker: {}",
                marker
            ))),
        }
    }
}

// ============= Encoding Metadata Registration =============

// EntityAddr is feature-dependent: encoding format changes with MSG_ADDR2 feature
mark_feature_dependent_encoding!(EntityAddr);
mark_feature_dependent_encoding!(EntityAddrvec);

// EntityAddrType is simple (just a u32 enum)
mark_simple_encoding!(EntityAddrType);
