//! Unified Ceph message encoding/decoding framework
//!
//! This module provides a consolidated approach for encoding/decoding Ceph messages
//! following the "sandwich" structure:
//! 1. Header (ceph_msg_header)
//! 2. Old Footer (ceph_msg_footer_old) - for wire compatibility
//! 3. Payload (front) - main message content
//! 4. Middle - optional middle section
//! 5. Data - optional data section
//!
//! Reference: ~/dev/ceph/src/msg/Message.h and ~/dev/ceph/src/msg/Message.cc

use crate::error::{Error, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32c::crc32c;

// Re-export message type constants
pub use crate::message::{CEPH_MSG_AUTH, CEPH_MSG_AUTH_REPLY, CEPH_MSG_PING, CEPH_MSG_PING_ACK};

/// Old footer format for wire compatibility
/// Reference: ~/dev/linux/include/linux/ceph/msgr.h
#[derive(Debug, Clone, Default)]
pub struct CephMsgFooterOld {
    pub front_crc: u32,
    pub middle_crc: u32,
    pub data_crc: u32,
    pub flags: u8,
}

impl CephMsgFooterOld {
    pub const LENGTH: usize = 13; // 3 * u32 + u8

    pub const FLAG_COMPLETE: u8 = 1;
    pub const FLAG_NOCRC: u8 = 2;

    pub fn encode(&self, dst: &mut impl BufMut) -> Result<()> {
        if dst.remaining_mut() < Self::LENGTH {
            return Err(Error::Serialization);
        }

        dst.put_u32_le(self.front_crc);
        dst.put_u32_le(self.middle_crc);
        dst.put_u32_le(self.data_crc);
        dst.put_u8(self.flags);

        Ok(())
    }

    pub fn decode(src: &mut impl Buf) -> Result<Self> {
        if src.remaining() < Self::LENGTH {
            return Err(Error::Deserialization("Incomplete footer".into()));
        }

        Ok(Self {
            front_crc: src.get_u32_le(),
            middle_crc: src.get_u32_le(),
            data_crc: src.get_u32_le(),
            flags: src.get_u8(),
        })
    }
}

/// Ceph message header
/// Reference: ~/dev/linux/include/linux/ceph/msgr.h struct ceph_msg_header
#[derive(Debug, Clone)]
pub struct CephMsgHeader {
    pub seq: u64,
    pub tid: u64,
    pub msg_type: u16,
    pub priority: u16,
    pub version: u16,
    pub front_len: u32,
    pub middle_len: u32,
    pub data_len: u32,
    pub data_off: u16,
    pub src_type: u8,
    pub src_num: u64,
    pub compat_version: u16,
    pub reserved: u16,
    pub crc: u32,
}

impl CephMsgHeader {
    pub const LENGTH: usize = 53; // Total size of packed struct

    pub fn new(msg_type: u16, version: u16) -> Self {
        Self {
            seq: 0,
            tid: 0,
            msg_type,
            priority: 0,
            version,
            front_len: 0,
            middle_len: 0,
            data_len: 0,
            data_off: 0,
            src_type: 0,
            src_num: 0,
            compat_version: version,
            reserved: 0,
            crc: 0,
        }
    }

    pub fn encode(&self, dst: &mut impl BufMut) -> Result<()> {
        if dst.remaining_mut() < Self::LENGTH {
            return Err(Error::Serialization);
        }

        dst.put_u64_le(self.seq);
        dst.put_u64_le(self.tid);
        dst.put_u16_le(self.msg_type);
        dst.put_u16_le(self.priority);
        dst.put_u16_le(self.version);
        dst.put_u32_le(self.front_len);
        dst.put_u32_le(self.middle_len);
        dst.put_u32_le(self.data_len);
        dst.put_u16_le(self.data_off);
        // ceph_entity_name: type (u8) + num (u64)
        dst.put_u8(self.src_type);
        dst.put_u64_le(self.src_num);
        dst.put_u16_le(self.compat_version);
        dst.put_u16_le(self.reserved);
        dst.put_u32_le(self.crc);

        Ok(())
    }

    pub fn decode(src: &mut impl Buf) -> Result<Self> {
        if src.remaining() < Self::LENGTH {
            return Err(Error::Deserialization("Incomplete header".into()));
        }

        Ok(Self {
            seq: src.get_u64_le(),
            tid: src.get_u64_le(),
            msg_type: src.get_u16_le(),
            priority: src.get_u16_le(),
            version: src.get_u16_le(),
            front_len: src.get_u32_le(),
            middle_len: src.get_u32_le(),
            data_len: src.get_u32_le(),
            data_off: src.get_u16_le(),
            src_type: src.get_u8(),
            src_num: src.get_u64_le(),
            compat_version: src.get_u16_le(),
            reserved: src.get_u16_le(),
            crc: src.get_u32_le(),
        })
    }

    /// Calculate header CRC (excluding the crc field itself)
    pub fn calc_crc(&self) -> u32 {
        let mut buf = BytesMut::with_capacity(Self::LENGTH - 4);
        buf.put_u64_le(self.seq);
        buf.put_u64_le(self.tid);
        buf.put_u16_le(self.msg_type);
        buf.put_u16_le(self.priority);
        buf.put_u16_le(self.version);
        buf.put_u32_le(self.front_len);
        buf.put_u32_le(self.middle_len);
        buf.put_u32_le(self.data_len);
        buf.put_u16_le(self.data_off);
        buf.put_u8(self.src_type);
        buf.put_u64_le(self.src_num);
        buf.put_u16_le(self.compat_version);
        buf.put_u16_le(self.reserved);
        crc32c(&buf)
    }
}

/// Trait for message types that can encode/decode their payload
pub trait CephMessagePayload: Sized {
    /// Get the message type constant
    fn msg_type() -> u16;

    /// Get the message version
    fn msg_version() -> u16;

    /// Encode the payload (front section) of the message
    fn encode_payload(&self, features: u64) -> Result<Bytes>;

    /// Encode the middle section (optional, default is empty)
    fn encode_middle(&self, _features: u64) -> Result<Bytes> {
        Ok(Bytes::new())
    }

    /// Encode the data section (optional, default is empty)
    fn encode_data(&self, _features: u64) -> Result<Bytes> {
        Ok(Bytes::new())
    }

    /// Decode the payload from front, middle, and data sections
    fn decode_payload(
        header: &CephMsgHeader,
        front: &[u8],
        middle: &[u8],
        data: &[u8],
    ) -> Result<Self>;
}

/// Complete Ceph message with header, footer, and payload sections
#[derive(Debug, Clone)]
pub struct CephMessage {
    pub header: CephMsgHeader,
    pub footer: CephMsgFooterOld,
    pub front: Bytes,
    pub middle: Bytes,
    pub data: Bytes,
}

impl CephMessage {
    /// Create a new message from a payload type
    pub fn from_payload<T: CephMessagePayload>(
        payload: &T,
        features: u64,
        _crc_flags: CrcFlags,
    ) -> Result<Self> {
        // Encode payload sections
        let front = payload.encode_payload(features)?;
        let middle = payload.encode_middle(features)?;
        let data = payload.encode_data(features)?;

        // Create header
        let mut header = CephMsgHeader::new(T::msg_type(), T::msg_version());
        header.front_len = front.len() as u32;
        header.middle_len = middle.len() as u32;
        header.data_len = data.len() as u32;

        // Create footer with CRCs
        let mut footer = CephMsgFooterOld {
            flags: CephMsgFooterOld::FLAG_COMPLETE,
            ..Default::default()
        };

        if _crc_flags.contains(CrcFlags::DATA) {
            footer.front_crc = crc32c(&front);
            footer.middle_crc = crc32c(&middle);
            footer.data_crc = crc32c(&data);
        } else {
            footer.flags |= CephMsgFooterOld::FLAG_NOCRC;
        }

        if _crc_flags.contains(CrcFlags::HEADER) {
            header.crc = header.calc_crc();
        }

        Ok(Self {
            header,
            footer,
            front,
            middle,
            data,
        })
    }

    /// Encode the complete message to bytes (sandwich format)
    /// Format: header + old_footer + front + middle + data
    pub fn encode(&self) -> Result<Bytes> {
        let total_len = CephMsgHeader::LENGTH
            + CephMsgFooterOld::LENGTH
            + self.front.len()
            + self.middle.len()
            + self.data.len();

        let mut buf = BytesMut::with_capacity(total_len);

        // 1. Encode header
        self.header.encode(&mut buf)?;

        // 2. Encode old footer
        self.footer.encode(&mut buf)?;

        // 3. Encode payload sections
        buf.extend_from_slice(&self.front);
        buf.extend_from_slice(&self.middle);
        buf.extend_from_slice(&self.data);

        Ok(buf.freeze())
    }

    /// Decode a complete message from bytes
    pub fn decode(src: &mut impl Buf) -> Result<Self> {
        // 1. Decode header
        let header = CephMsgHeader::decode(src)?;

        // 2. Decode old footer
        let footer = CephMsgFooterOld::decode(src)?;

        // 3. Decode payload sections
        if src.remaining() < (header.front_len + header.middle_len + header.data_len) as usize {
            return Err(Error::Deserialization("Incomplete message payload".into()));
        }

        let mut front = vec![0u8; header.front_len as usize];
        src.copy_to_slice(&mut front);

        let mut middle = vec![0u8; header.middle_len as usize];
        src.copy_to_slice(&mut middle);

        let mut data = vec![0u8; header.data_len as usize];
        src.copy_to_slice(&mut data);

        // Verify CRCs if not disabled
        if footer.flags & CephMsgFooterOld::FLAG_NOCRC == 0 {
            let front_crc = crc32c(&front);
            if front_crc != footer.front_crc {
                return Err(Error::Deserialization(format!(
                    "Front CRC mismatch: got {}, expected {}",
                    front_crc, footer.front_crc
                )));
            }

            let middle_crc = crc32c(&middle);
            if middle_crc != footer.middle_crc {
                return Err(Error::Deserialization(format!(
                    "Middle CRC mismatch: got {}, expected {}",
                    middle_crc, footer.middle_crc
                )));
            }

            let data_crc = crc32c(&data);
            if data_crc != footer.data_crc {
                return Err(Error::Deserialization(format!(
                    "Data CRC mismatch: got {}, expected {}",
                    data_crc, footer.data_crc
                )));
            }
        }

        Ok(Self {
            header,
            footer,
            front: Bytes::from(front),
            middle: Bytes::from(middle),
            data: Bytes::from(data),
        })
    }

    /// Decode the message payload into a specific type
    pub fn decode_payload<T: CephMessagePayload>(&self) -> Result<T> {
        T::decode_payload(&self.header, &self.front, &self.middle, &self.data)
    }
}

bitflags::bitflags! {
    /// CRC calculation flags
    pub struct CrcFlags: u32 {
        const DATA = 1 << 0;
        const HEADER = 1 << 1;
        const ALL = Self::DATA.bits() | Self::HEADER.bits();
    }
}

/// Simple PING message (no payload)
#[derive(Debug, Clone, Default)]
pub struct MPing;

impl CephMessagePayload for MPing {
    fn msg_type() -> u16 {
        CEPH_MSG_PING
    }

    fn msg_version() -> u16 {
        1
    }

    fn encode_payload(&self, _features: u64) -> Result<Bytes> {
        Ok(Bytes::new())
    }

    fn decode_payload(
        _header: &CephMsgHeader,
        _front: &[u8],
        _middle: &[u8],
        _data: &[u8],
    ) -> Result<Self> {
        Ok(MPing)
    }
}

/// Simple PING_ACK message (no payload)
#[derive(Debug, Clone, Default)]
pub struct MPingAck;

impl CephMessagePayload for MPingAck {
    fn msg_type() -> u16 {
        CEPH_MSG_PING_ACK
    }

    fn msg_version() -> u16 {
        1
    }

    fn encode_payload(&self, _features: u64) -> Result<Bytes> {
        Ok(Bytes::new())
    }

    fn decode_payload(
        _header: &CephMsgHeader,
        _front: &[u8],
        _middle: &[u8],
        _data: &[u8],
    ) -> Result<Self> {
        Ok(MPingAck)
    }
}

/// MAuth - Authentication request message
/// Note: This is for CEPH_MSG_AUTH (0x0011), which is different from
/// the msgr2 AUTH_REQUEST frame. This message type is used in the legacy
/// messenger protocol and some monitor communications.
#[derive(Debug, Clone)]
pub struct MAuth {
    pub protocol: u32,
    pub auth_payload: Bytes,
}

impl MAuth {
    pub fn new(protocol: u32, auth_payload: Bytes) -> Self {
        Self {
            protocol,
            auth_payload,
        }
    }
}

impl CephMessagePayload for MAuth {
    fn msg_type() -> u16 {
        CEPH_MSG_AUTH
    }

    fn msg_version() -> u16 {
        1
    }

    fn encode_payload(&self, _features: u64) -> Result<Bytes> {
        let mut buf = BytesMut::new();
        buf.put_u32_le(self.protocol);
        buf.put_u32_le(self.auth_payload.len() as u32);
        buf.put_slice(&self.auth_payload);
        Ok(buf.freeze())
    }

    fn decode_payload(
        _header: &CephMsgHeader,
        front: &[u8],
        _middle: &[u8],
        _data: &[u8],
    ) -> Result<Self> {
        if front.len() < 8 {
            return Err(Error::Deserialization("MAuth too short".into()));
        }

        let mut buf = front;
        let protocol = buf.get_u32_le();
        let payload_len = buf.get_u32_le() as usize;

        if buf.remaining() < payload_len {
            return Err(Error::Deserialization("MAuth payload incomplete".into()));
        }

        let auth_payload = Bytes::copy_from_slice(&buf[..payload_len]);

        Ok(Self {
            protocol,
            auth_payload,
        })
    }
}

/// MAuthReply - Authentication reply message
#[derive(Debug, Clone)]
pub struct MAuthReply {
    pub protocol: u32,
    pub result: i32,
    pub global_id: u64,
    pub auth_payload: Bytes,
}

impl MAuthReply {
    pub fn new(protocol: u32, result: i32, global_id: u64, auth_payload: Bytes) -> Self {
        Self {
            protocol,
            result,
            global_id,
            auth_payload,
        }
    }
}

impl CephMessagePayload for MAuthReply {
    fn msg_type() -> u16 {
        CEPH_MSG_AUTH_REPLY
    }

    fn msg_version() -> u16 {
        1
    }

    fn encode_payload(&self, _features: u64) -> Result<Bytes> {
        let mut buf = BytesMut::new();
        buf.put_u32_le(self.protocol);
        buf.put_i32_le(self.result);
        buf.put_u64_le(self.global_id);
        buf.put_u32_le(self.auth_payload.len() as u32);
        buf.put_slice(&self.auth_payload);
        Ok(buf.freeze())
    }

    fn decode_payload(
        _header: &CephMsgHeader,
        front: &[u8],
        _middle: &[u8],
        _data: &[u8],
    ) -> Result<Self> {
        if front.len() < 20 {
            return Err(Error::Deserialization("MAuthReply too short".into()));
        }

        let mut buf = front;
        let protocol = buf.get_u32_le();
        let result = buf.get_i32_le();
        let global_id = buf.get_u64_le();
        let payload_len = buf.get_u32_le() as usize;

        if buf.remaining() < payload_len {
            return Err(Error::Deserialization(
                "MAuthReply payload incomplete".into(),
            ));
        }

        let auth_payload = Bytes::copy_from_slice(&buf[..payload_len]);

        Ok(Self {
            protocol,
            result,
            global_id,
            auth_payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ping_message_encoding() {
        let ping = MPing;
        let msg = CephMessage::from_payload(&ping, 0, CrcFlags::ALL).unwrap();

        assert_eq!(msg.header.msg_type, CEPH_MSG_PING);
        assert_eq!(msg.header.version, 1);
        assert_eq!(msg.front.len(), 0);
        assert_eq!(msg.middle.len(), 0);
        assert_eq!(msg.data.len(), 0);

        // Encode to bytes
        let encoded = msg.encode().unwrap();
        assert!(encoded.len() > 0);
    }

    #[test]
    fn test_ping_ack_message_encoding() {
        let ping_ack = MPingAck;
        let msg = CephMessage::from_payload(&ping_ack, 0, CrcFlags::ALL).unwrap();

        assert_eq!(msg.header.msg_type, CEPH_MSG_PING_ACK);
        assert_eq!(msg.header.version, 1);
        assert_eq!(msg.front.len(), 0);
        assert_eq!(msg.middle.len(), 0);
        assert_eq!(msg.data.len(), 0);

        // Encode to bytes
        let encoded = msg.encode().unwrap();
        assert!(encoded.len() > 0);
    }

    #[test]
    fn test_mauth_message_encoding() {
        let auth_payload = Bytes::from(vec![1, 2, 3, 4]);
        let mauth = MAuth::new(2, auth_payload.clone()); // protocol 2 = CEPHX

        let msg = CephMessage::from_payload(&mauth, 0, CrcFlags::ALL).unwrap();

        assert_eq!(msg.header.msg_type, CEPH_MSG_AUTH);
        assert_eq!(msg.header.version, 1);
        assert!(msg.front.len() > 0);

        // Decode and verify
        let decoded: MAuth = msg.decode_payload().unwrap();
        assert_eq!(decoded.protocol, 2);
        assert_eq!(decoded.auth_payload, auth_payload);
    }

    #[test]
    fn test_mauth_reply_message_encoding() {
        let auth_payload = Bytes::from(vec![5, 6, 7, 8]);
        let reply = MAuthReply::new(2, 0, 12345, auth_payload.clone());

        let msg = CephMessage::from_payload(&reply, 0, CrcFlags::ALL).unwrap();

        assert_eq!(msg.header.msg_type, CEPH_MSG_AUTH_REPLY);
        assert_eq!(msg.header.version, 1);
        assert!(msg.front.len() > 0);

        // Decode and verify
        let decoded: MAuthReply = msg.decode_payload().unwrap();
        assert_eq!(decoded.protocol, 2);
        assert_eq!(decoded.result, 0);
        assert_eq!(decoded.global_id, 12345);
        assert_eq!(decoded.auth_payload, auth_payload);
    }
}
