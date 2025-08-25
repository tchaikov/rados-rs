use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::error::{Error, Result};
use crate::types::FeatureSet;
use std::fmt;

pub const CEPH_BANNER: &[u8] = b"ceph v2";
pub const CEPH_BANNER_LEN: usize = CEPH_BANNER.len();

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Banner {
    pub banner: Bytes,
    pub supported_features: FeatureSet,
    pub required_features: FeatureSet,
}

impl Banner {
    pub fn new() -> Self {
        Self {
            banner: Bytes::from_static(CEPH_BANNER),
            supported_features: FeatureSet::MSGR2,
            required_features: FeatureSet::EMPTY,
        }
    }
    
    pub fn new_with_features(supported_features: FeatureSet, required_features: FeatureSet) -> Self {
        Self {
            banner: Bytes::from_static(CEPH_BANNER),
            supported_features,
            required_features,
        }
    }

    pub fn encode(&self, dst: &mut BytesMut) {
        // Send banner prefix "ceph v2"
        dst.extend_from_slice(&self.banner);
        
        // Send newline
        dst.put_u8(b'\n');
        
        // Prepare banner payload: entity_addr_t + supported_features + required_features
        let mut payload = BytesMut::new();
        
        // Minimal entity_addr_t: type(1) + family(1) + port(2) + addr(4/16) + nonce(4)
        // Using IPv4: 1 + 1 + 2 + 4 + 4 = 12 bytes
        payload.put_u8(1); // legacy type
        payload.put_u8(2); // AF_INET
        payload.put_u16(0); // port 0 (will be filled later)
        payload.put_u32(0x00000000); // 0.0.0.0 
        payload.put_u32(0); // nonce 0
        
        // Supported features (8 bytes, little-endian)
        payload.put_u64_le(self.supported_features.value());
        
        // Required features (8 bytes, little-endian)
        payload.put_u64_le(self.required_features.value());
        
        // Send payload size first (uint16_t, 2 bytes)
        dst.put_u16_le(payload.len() as u16);
        
        // Then send the payload itself
        dst.extend_from_slice(&payload);
    }

    pub fn decode(src: &mut impl Buf) -> Result<Self> {
        if src.remaining() < CEPH_BANNER_LEN + 1 { // +1 for newline
            return Err(Error::Deserialization("Incomplete banner prefix".into()));
        }

        let mut banner_bytes = vec![0u8; CEPH_BANNER_LEN];
        src.copy_to_slice(&mut banner_bytes);
        
        // Check if banner starts with "ceph v"
        if !banner_bytes.starts_with(b"ceph v") {
            return Err(Error::Protocol(format!(
                "Invalid banner prefix: expected 'ceph v', got {:?}",
                String::from_utf8_lossy(&banner_bytes)
            )));
        }
        
        // Read newline
        let newline = src.get_u8();
        if newline != b'\n' {
            return Err(Error::Protocol(format!(
                "Expected newline after banner, got: {}", newline
            )));
        }
        
        // Read payload size (uint16_t)
        if src.remaining() < 2 {
            return Err(Error::Deserialization("Missing payload size".into()));
        }
        let payload_size = src.get_u16_le() as usize;
        
        // Read payload
        if src.remaining() < payload_size {
            return Err(Error::Deserialization("Incomplete banner payload".into()));
        }
        
        // Minimum required payload should be at least 16 bytes for features
        if payload_size < 16 {
            return Err(Error::Deserialization("Banner payload too short".into()));
        }
        
        // The payload format varies based on size. For smaller payloads (16 bytes),
        // it might just be features without entity_addr_t, or a simpler entity_addr_t
        
        let mut entity_addr_size = 0;
        if payload_size > 16 {
            // Larger payload likely has entity_addr_t + features
            // Skip entity_addr_t for now (assume 12 bytes minimum for IPv4)
            // TODO: Properly parse entity_addr_t
            let _addr_type = src.get_u8();
            let _family = src.get_u8(); 
            let _port = src.get_u16();
            let _ipv4 = src.get_u32();
            let _nonce = src.get_u32();
            entity_addr_size = 12;
        }
        
        // Read features - the remaining bytes should be features
        let remaining_bytes = payload_size - entity_addr_size;
        if remaining_bytes >= 16 {
            // Both supported and required features (8 bytes each)
            let supported_features = FeatureSet::new(src.get_u64_le());
            let required_features = FeatureSet::new(src.get_u64_le());
            Ok(Self {
                banner: Bytes::from(banner_bytes),
                supported_features,
                required_features,
            })
        } else if remaining_bytes >= 8 {
            // Only supported features
            let supported_features = FeatureSet::new(src.get_u64_le());
            let required_features = FeatureSet::EMPTY;
            Ok(Self {
                banner: Bytes::from(banner_bytes),
                supported_features,
                required_features,
            })
        } else {
            // Not enough data for features - use defaults
            Ok(Self {
                banner: Bytes::from(banner_bytes),
                supported_features: FeatureSet::EMPTY,
                required_features: FeatureSet::EMPTY,
            })
        }
    }
}

impl Default for Banner {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectMessage {
    pub features: FeatureSet,
    pub host_type: u32,
    pub global_seq: u32,
    pub connect_seq: u32,
    pub protocol_version: u32,
    pub authorizer_protocol: u32,
    pub authorizer_len: u32,
    pub flags: u8,
}

impl ConnectMessage {
    pub const LENGTH: usize = 36;  // 8+4+4+4+4+4+4+1+3 = 36 bytes
    pub const PROTOCOL_VERSION: u32 = 2; // msgr2

    pub fn new(features: FeatureSet, host_type: u32) -> Self {
        Self {
            features,
            host_type,
            global_seq: 0,
            connect_seq: 0,
            protocol_version: Self::PROTOCOL_VERSION,
            authorizer_protocol: 0,
            authorizer_len: 0,
            flags: 0,
        }
    }

    pub fn with_auth(mut self, auth_protocol: u32, authorizer_len: u32) -> Self {
        self.authorizer_protocol = auth_protocol;
        self.authorizer_len = authorizer_len;
        self
    }

    pub fn encode(&self, dst: &mut impl BufMut) -> Result<()> {
        if dst.remaining_mut() < Self::LENGTH {
            return Err(Error::Serialization);
        }

        dst.put_u64_le(self.features.value());
        dst.put_u32_le(self.host_type);
        dst.put_u32_le(self.global_seq);
        dst.put_u32_le(self.connect_seq);
        dst.put_u32_le(self.protocol_version);
        dst.put_u32_le(self.authorizer_protocol);
        dst.put_u32_le(self.authorizer_len);
        dst.put_u8(self.flags);
        dst.put_u8(0); // padding
        dst.put_u8(0); // padding
        dst.put_u8(0); // padding

        Ok(())
    }

    pub fn decode(src: &mut impl Buf) -> Result<Self> {
        if src.remaining() < Self::LENGTH {
            return Err(Error::Deserialization("Incomplete connect message".into()));
        }

        let features = FeatureSet::new(src.get_u64_le());
        let host_type = src.get_u32_le();
        let global_seq = src.get_u32_le();
        let connect_seq = src.get_u32_le();
        let protocol_version = src.get_u32_le();
        let authorizer_protocol = src.get_u32_le();
        let authorizer_len = src.get_u32_le();
        let flags = src.get_u8();
        
        // Skip padding
        src.advance(3);

        Ok(Self {
            features,
            host_type,
            global_seq,
            connect_seq,
            protocol_version,
            authorizer_protocol,
            authorizer_len,
            flags,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectReplyMessage {
    pub tag: u8,
    pub features: FeatureSet,
    pub global_seq: u32,
    pub connect_seq: u32,
    pub protocol_version: u32,
    pub authorizer_len: u32,
    pub flags: u8,
}

impl ConnectReplyMessage {
    pub const LENGTH: usize = 31;
    pub const REPLY_TAG_READY: u8 = 1;
    pub const REPLY_TAG_RESETSESSION: u8 = 2;
    pub const REPLY_TAG_WAIT: u8 = 3;
    pub const REPLY_TAG_RETRY_SESSION: u8 = 4;
    pub const REPLY_TAG_RETRY_GLOBAL: u8 = 5;
    pub const REPLY_TAG_BADPROTOVER: u8 = 6;
    pub const REPLY_TAG_BADAUTHORIZER: u8 = 7;
    pub const REPLY_TAG_FEATURES: u8 = 8;
    pub const REPLY_TAG_SEQ: u8 = 9;

    pub fn ready(features: FeatureSet, global_seq: u32, connect_seq: u32) -> Self {
        Self {
            tag: Self::REPLY_TAG_READY,
            features,
            global_seq,
            connect_seq,
            protocol_version: ConnectMessage::PROTOCOL_VERSION,
            authorizer_len: 0,
            flags: 0,
        }
    }

    pub fn encode(&self, dst: &mut impl BufMut) -> Result<()> {
        if dst.remaining_mut() < Self::LENGTH {
            return Err(Error::Serialization);
        }

        dst.put_u8(self.tag);
        dst.put_u64_le(self.features.value());
        dst.put_u32_le(self.global_seq);
        dst.put_u32_le(self.connect_seq);
        dst.put_u32_le(self.protocol_version);
        dst.put_u32_le(self.authorizer_len);
        dst.put_u8(self.flags);
        dst.put_u8(0); // padding
        dst.put_u8(0); // padding

        Ok(())
    }

    pub fn decode(src: &mut impl Buf) -> Result<Self> {
        if src.remaining() < Self::LENGTH {
            return Err(Error::Deserialization("Incomplete connect reply message".into()));
        }

        let tag = src.get_u8();
        let features = FeatureSet::new(src.get_u64_le());
        let global_seq = src.get_u32_le();
        let connect_seq = src.get_u32_le();
        let protocol_version = src.get_u32_le();
        let authorizer_len = src.get_u32_le();
        let flags = src.get_u8();
        
        // Skip padding
        src.advance(2);

        Ok(Self {
            tag,
            features,
            global_seq,
            connect_seq,
            protocol_version,
            authorizer_len,
            flags,
        })
    }

    pub fn is_ready(&self) -> bool {
        self.tag == Self::REPLY_TAG_READY
    }

    pub fn is_retry(&self) -> bool {
        matches!(self.tag, Self::REPLY_TAG_RETRY_SESSION | Self::REPLY_TAG_RETRY_GLOBAL)
    }

    pub fn is_error(&self) -> bool {
        matches!(
            self.tag,
            Self::REPLY_TAG_BADPROTOVER | Self::REPLY_TAG_BADAUTHORIZER | Self::REPLY_TAG_FEATURES
        )
    }
}

impl fmt::Display for ConnectReplyMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tag_name = match self.tag {
            Self::REPLY_TAG_READY => "READY",
            Self::REPLY_TAG_RESETSESSION => "RESETSESSION",
            Self::REPLY_TAG_WAIT => "WAIT",
            Self::REPLY_TAG_RETRY_SESSION => "RETRY_SESSION",
            Self::REPLY_TAG_RETRY_GLOBAL => "RETRY_GLOBAL",
            Self::REPLY_TAG_BADPROTOVER => "BADPROTOVER",
            Self::REPLY_TAG_BADAUTHORIZER => "BADAUTHORIZER",
            Self::REPLY_TAG_FEATURES => "FEATURES",
            Self::REPLY_TAG_SEQ => "SEQ",
            _ => "UNKNOWN",
        };
        
        write!(f, "ConnectReply(tag={}, features={:x})", tag_name, self.features.value())
    }
}