//! EntityAddr - Entity address type for Ceph
//!
//! This module re-exports the DencMut-based implementation for backward compatibility.

// Re-export the new DencMut-based implementation
pub use crate::entity_addr_dencmut::{EntityAddr, EntityAddrType};

// EntityAddrvec - keep old implementation for now (TODO: migrate to DencMut)
use crate::denc::Denc;
use crate::error::RadosError;
use crate::features::CEPH_FEATURE_MSG_ADDR2;
use bytes::{BufMut, BytesMut};
use serde::Serialize;

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
                // Need to put the marker back for EntityAddr::decode
                let mut temp = BytesMut::with_capacity(1 + bytes.len());
                temp.put_u8(0);
                temp.extend_from_slice(bytes);
                let mut temp_bytes = temp.freeze();
                let addr = EntityAddr::decode(&mut temp_bytes)?;
                *bytes = temp_bytes;
                Ok(EntityAddrvec { addrs: vec![addr] })
            }
            1 => {
                // MSG_ADDR2 format: single address
                let mut temp = BytesMut::with_capacity(1 + bytes.len());
                temp.put_u8(1);
                temp.extend_from_slice(bytes);
                let mut temp_bytes = temp.freeze();
                let addr = EntityAddr::decode(&mut temp_bytes)?;
                *bytes = temp_bytes;
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
