use crate::error::RadosError;
use crate::types::{EntityAddr, EntityType};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::debug;
use std::convert::TryFrom;

// Frame tags based on official Ceph msgr2 protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Tag {
    Hello = 1,
    AuthRequest = 2,
    AuthBadMethod = 3,
    AuthReplyMore = 4,
    AuthRequestMore = 5,
    AuthDone = 6,
    AuthSignature = 7,
    ClientIdent = 8,
    ServerIdent = 9,
    IdentMissingFeatures = 10,
    SessionReconnect = 11,
    SessionReset = 12,
    SessionRetry = 13,
    SessionRetryGlobal = 14,
    SessionReconnectOk = 15,
    Wait = 16,
    Message = 17,
    Keepalive2 = 18,
    Keepalive2Ack = 19,
    Ack = 20,
    CompressionRequest = 21,
    CompressionDone = 22,
}

impl TryFrom<u8> for Tag {
    type Error = RadosError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Tag::Hello),
            2 => Ok(Tag::AuthRequest),
            3 => Ok(Tag::AuthBadMethod),
            4 => Ok(Tag::AuthReplyMore),
            5 => Ok(Tag::AuthRequestMore),
            6 => Ok(Tag::AuthDone),
            7 => Ok(Tag::AuthSignature),
            8 => Ok(Tag::ClientIdent),
            9 => Ok(Tag::ServerIdent),
            10 => Ok(Tag::IdentMissingFeatures),
            11 => Ok(Tag::SessionReconnect),
            12 => Ok(Tag::SessionReset),
            13 => Ok(Tag::SessionRetry),
            14 => Ok(Tag::SessionRetryGlobal),
            15 => Ok(Tag::SessionReconnectOk),
            16 => Ok(Tag::Wait),
            17 => Ok(Tag::Message),
            18 => Ok(Tag::Keepalive2),
            19 => Ok(Tag::Keepalive2Ack),
            20 => Ok(Tag::Ack),
            21 => Ok(Tag::CompressionRequest),
            22 => Ok(Tag::CompressionDone),
            _ => Err(RadosError::ProtocolError(format!("Unknown tag: {}", value))),
        }
    }
}

// Segment descriptor
#[derive(Debug, Clone)]
pub struct Segment {
    pub length: u32,
    pub alignment: u16,
}

// Frame preamble (32 bytes)
#[derive(Debug, Clone)]
pub struct Preamble {
    pub tag: Tag,
    pub num_segments: u8,
    pub segments: [Segment; 4], // Max 4 segments
    pub flags: u8,
    pub reserved: u8,
    pub crc: u32,
}

impl Preamble {
    const SIZE: usize = 32;
    const DEFAULT_ALIGNMENT: u16 = 8; // sizeof(void*)

    pub fn new(tag: Tag, payload_size: u32) -> Self {
        let mut segments = [
            Segment { length: 0, alignment: 0 },
            Segment { length: 0, alignment: 0 },
            Segment { length: 0, alignment: 0 },
            Segment { length: 0, alignment: 0 },
        ];

        // Single segment for payload
        segments[0] = Segment {
            length: payload_size,
            alignment: Self::DEFAULT_ALIGNMENT,
        };

        Self {
            tag,
            num_segments: 1,
            segments,
            flags: 0,
            reserved: 0,
            crc: 0, // Will be calculated when encoding
        }
    }

    pub fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(Self::SIZE);
        
        // Tag
        buf.put_u8(self.tag as u8);
        // Number of segments  
        buf.put_u8(self.num_segments);
        
        // 4 segments (6 bytes each: 4 for length + 2 for alignment)
        for segment in &self.segments {
            buf.put_u32_le(segment.length);
            buf.put_u16_le(segment.alignment);
        }
        
        // Flags and reserved
        buf.put_u8(self.flags);
        buf.put_u8(self.reserved);
        
        // Calculate CRC32-C over first 28 bytes
        let crc = crc32c::crc32c(&buf[..28]);
        buf.put_u32_le(crc);
        
        buf
    }

    pub fn decode(mut buf: Bytes) -> Result<Self, RadosError> {
        if buf.len() < Self::SIZE {
            return Err(RadosError::ProtocolError(format!(
                "Preamble too short: {} < {}",
                buf.len(),
                Self::SIZE
            )));
        }

        let tag = Tag::try_from(buf.get_u8())?;
        let num_segments = buf.get_u8();
        
        let mut segments = [
            Segment { length: 0, alignment: 0 },
            Segment { length: 0, alignment: 0 },
            Segment { length: 0, alignment: 0 },
            Segment { length: 0, alignment: 0 },
        ];
        
        for segment in segments.iter_mut() {
            segment.length = buf.get_u32_le();
            segment.alignment = buf.get_u16_le();
        }
        
        let flags = buf.get_u8();
        let reserved = buf.get_u8();
        let crc = buf.get_u32_le();
        
        // Verify CRC
        let preamble_data = &buf.as_ref()[buf.len() - Self::SIZE..buf.len() - 4];
        let calculated_crc = crc32c::crc32c(&preamble_data[..28]);
        if calculated_crc != crc {
            return Err(RadosError::ProtocolError(format!(
                "Preamble CRC mismatch: expected {}, got {}",
                crc, calculated_crc
            )));
        }

        Ok(Self {
            tag,
            num_segments,
            segments,
            flags,
            reserved,
            crc,
        })
    }
}

// Complete frame structure
#[derive(Debug, Clone)]
pub struct MsgFrame {
    pub preamble: Preamble,
    pub payload: Bytes,
}

impl MsgFrame {
    pub fn new(tag: Tag, payload: Bytes) -> Self {
        let preamble = Preamble::new(tag, payload.len() as u32);
        Self { preamble, payload }
    }

    pub fn encode(&self) -> BytesMut {
        let preamble_data = self.preamble.encode();
        let total_size = preamble_data.len() + self.payload.len();
        let mut buf = BytesMut::with_capacity(total_size);
        
        buf.extend_from_slice(&preamble_data);
        buf.extend_from_slice(&self.payload);
        
        debug!(
            "Encoded frame: tag={:?}, payload_size={}, total_size={}",
            self.preamble.tag,
            self.payload.len(),
            total_size
        );
        
        buf
    }

    pub fn decode(mut buf: Bytes) -> Result<Self, RadosError> {
        let preamble = Preamble::decode(buf.slice(..Preamble::SIZE))?;
        buf.advance(Preamble::SIZE);
        
        // Get payload based on first segment length
        let payload_len = preamble.segments[0].length as usize;
        if buf.len() < payload_len {
            return Err(RadosError::ProtocolError(format!(
                "Frame payload too short: {} < {}",
                buf.len(),
                payload_len
            )));
        }
        
        let payload = buf.slice(..payload_len);
        
        debug!(
            "Decoded frame: tag={:?}, payload_size={}",
            preamble.tag,
            payload.len()
        );
        
        Ok(Self { preamble, payload })
    }
}

// Hello frame payload
#[derive(Debug, Clone)]
pub struct HelloFrame {
    pub entity_type: EntityType,
    pub peer_addr: EntityAddr,
}

impl HelloFrame {
    pub fn new(entity_type: EntityType, peer_addr: EntityAddr) -> Self {
        Self {
            entity_type,
            peer_addr,
        }
    }

    pub fn encode(&self) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::new();
        
        // Entity type (1 byte)
        buf.put_u8(self.entity_type.value() as u8);
        
        // Entity address
        let addr_data = self.peer_addr.encode()?;
        buf.extend_from_slice(&addr_data);
        
        Ok(buf.freeze())
    }

    pub fn decode(mut buf: Bytes) -> Result<Self, RadosError> {
        if buf.is_empty() {
            return Err(RadosError::ProtocolError(
                "Hello frame payload is empty".to_string(),
            ));
        }

        let entity_type = EntityType::try_from(buf.get_u8())?;
        let peer_addr = EntityAddr::decode(buf)?;

        Ok(Self {
            entity_type,
            peer_addr,
        })
    }

    pub fn to_frame(&self) -> Result<MsgFrame, RadosError> {
        let payload = self.encode()?;
        Ok(MsgFrame::new(Tag::Hello, payload))
    }
}

// Auth request frame payload
#[derive(Debug, Clone)]
pub struct AuthRequestFrame {
    pub method: u32,
    pub preferred_modes: Vec<u32>,
    pub auth_payload: Bytes,
}

impl AuthRequestFrame {
    pub fn new(method: u32, preferred_modes: Vec<u32>, auth_payload: Bytes) -> Self {
        Self {
            method,
            preferred_modes,
            auth_payload,
        }
    }

    pub fn encode(&self) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::new();
        
        // Method (4 bytes)
        buf.put_u32_le(self.method);
        
        // Number of preferred modes (4 bytes)
        buf.put_u32_le(self.preferred_modes.len() as u32);
        
        // Preferred modes
        for mode in &self.preferred_modes {
            buf.put_u32_le(*mode);
        }
        
        // Auth payload
        buf.extend_from_slice(&self.auth_payload);
        
        Ok(buf.freeze())
    }

    pub fn decode(mut buf: Bytes) -> Result<Self, RadosError> {
        if buf.len() < 8 {
            return Err(RadosError::ProtocolError(
                "Auth request frame too short".to_string(),
            ));
        }

        let method = buf.get_u32_le();
        let num_modes = buf.get_u32_le();
        
        let mut preferred_modes = Vec::new();
        for _ in 0..num_modes {
            if buf.len() < 4 {
                return Err(RadosError::ProtocolError(
                    "Not enough data for preferred modes".to_string(),
                ));
            }
            preferred_modes.push(buf.get_u32_le());
        }
        
        let auth_payload = buf;

        Ok(Self {
            method,
            preferred_modes,
            auth_payload,
        })
    }

    pub fn to_frame(&self) -> Result<MsgFrame, RadosError> {
        let payload = self.encode()?;
        Ok(MsgFrame::new(Tag::AuthRequest, payload))
    }
}

// Auth done frame payload
#[derive(Debug, Clone)]
pub struct AuthDoneFrame {
    pub global_id: u64,
    pub con_mode: u32,
    pub auth_payload: Bytes,
}

impl AuthDoneFrame {
    pub fn decode(mut buf: Bytes) -> Result<Self, RadosError> {
        if buf.len() < 12 {
            return Err(RadosError::ProtocolError(
                "Auth done frame too short".to_string(),
            ));
        }

        let global_id = buf.get_u64_le();
        let con_mode = buf.get_u32_le();
        let auth_payload = buf;

        Ok(Self {
            global_id,
            con_mode,
            auth_payload,
        })
    }
}

// Client ident frame payload
#[derive(Debug, Clone)]
pub struct ClientIdentFrame {
    pub addrs: Vec<EntityAddr>,
    pub target_addr: EntityAddr,
    pub gid: i64,
    pub global_seq: u64,
    pub supported_features: u64,
    pub required_features: u64,
    pub flags: u64,
    pub cookie: u64,
}

impl ClientIdentFrame {
    pub fn new(
        addrs: Vec<EntityAddr>,
        target_addr: EntityAddr,
        gid: i64,
        global_seq: u64,
        supported_features: u64,
        required_features: u64,
        flags: u64,
        cookie: u64,
    ) -> Self {
        Self {
            addrs,
            target_addr,
            gid,
            global_seq,
            supported_features,
            required_features,
            flags,
            cookie,
        }
    }

    pub fn encode(&self) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::new();
        
        // Number of addresses
        buf.put_u32_le(self.addrs.len() as u32);
        
        // Addresses
        for addr in &self.addrs {
            let addr_data = addr.encode()?;
            buf.extend_from_slice(&addr_data);
        }
        
        // Target address
        let target_data = self.target_addr.encode()?;
        buf.extend_from_slice(&target_data);
        
        // GID
        buf.put_i64_le(self.gid);
        
        // Global sequence
        buf.put_u64_le(self.global_seq);
        
        // Features
        buf.put_u64_le(self.supported_features);
        buf.put_u64_le(self.required_features);
        
        // Flags
        buf.put_u64_le(self.flags);
        
        // Cookie
        buf.put_u64_le(self.cookie);
        
        Ok(buf.freeze())
    }

    pub fn to_frame(&self) -> Result<MsgFrame, RadosError> {
        let payload = self.encode()?;
        Ok(MsgFrame::new(Tag::ClientIdent, payload))
    }
}

// Server ident frame payload
#[derive(Debug, Clone)]
pub struct ServerIdentFrame {
    pub addrs: Vec<EntityAddr>,
    pub gid: i64,
    pub global_seq: u64,
    pub supported_features: u64,
    pub required_features: u64,
    pub flags: u64,
    pub cookie: u64,
}

impl ServerIdentFrame {
    pub fn decode(mut buf: Bytes) -> Result<Self, RadosError> {
        if buf.len() < 4 {
            return Err(RadosError::ProtocolError(
                "Server ident frame too short".to_string(),
            ));
        }

        let num_addrs = buf.get_u32_le();
        let mut addrs = Vec::new();
        
        for _ in 0..num_addrs {
            let addr = EntityAddr::decode(buf.clone())?;
            // Need to advance buf by the size of EntityAddr
            // For now, assume fixed size - this should be properly implemented
            buf.advance(EntityAddr::ENCODED_SIZE);
            addrs.push(addr);
        }
        
        if buf.len() < 48 {
            return Err(RadosError::ProtocolError(
                "Not enough data for server ident fields".to_string(),
            ));
        }

        let gid = buf.get_i64_le();
        let global_seq = buf.get_u64_le();
        let supported_features = buf.get_u64_le();
        let required_features = buf.get_u64_le();
        let flags = buf.get_u64_le();
        let cookie = buf.get_u64_le();

        Ok(Self {
            addrs,
            gid,
            global_seq,
            supported_features,
            required_features,
            flags,
            cookie,
        })
    }
}