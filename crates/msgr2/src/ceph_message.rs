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

bitflags::bitflags! {
    /// Message footer flags
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct MsgFooterFlags: u8 {
        /// Message is complete
        const COMPLETE = 1;
        /// No CRC check needed
        const NOCRC = 2;
    }
}

impl CephMsgFooterOld {
    pub const LENGTH: usize = 13; // 3 * u32 + u8

    pub fn encode(&self, dst: &mut impl BufMut) -> Result<()> {
        use denc::Denc;
        self.front_crc.encode(dst, 0)?;
        self.middle_crc.encode(dst, 0)?;
        self.data_crc.encode(dst, 0)?;
        self.flags.encode(dst, 0)?;
        Ok(())
    }

    pub fn decode(src: &mut impl Buf) -> Result<Self> {
        use denc::Denc;
        Ok(Self {
            front_crc: u32::decode(src, 0)?,
            middle_crc: u32::decode(src, 0)?,
            data_crc: u32::decode(src, 0)?,
            flags: u8::decode(src, 0)?,
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
        use denc::Denc;
        self.seq.encode(dst, 0)?;
        self.tid.encode(dst, 0)?;
        self.msg_type.encode(dst, 0)?;
        self.priority.encode(dst, 0)?;
        self.version.encode(dst, 0)?;
        self.front_len.encode(dst, 0)?;
        self.middle_len.encode(dst, 0)?;
        self.data_len.encode(dst, 0)?;
        self.data_off.encode(dst, 0)?;
        // ceph_entity_name: type (u8) + num (u64)
        self.src_type.encode(dst, 0)?;
        self.src_num.encode(dst, 0)?;
        self.compat_version.encode(dst, 0)?;
        self.reserved.encode(dst, 0)?;
        self.crc.encode(dst, 0)?;
        Ok(())
    }

    pub fn decode(src: &mut impl Buf) -> Result<Self> {
        use denc::Denc;
        Ok(Self {
            seq: u64::decode(src, 0)?,
            tid: u64::decode(src, 0)?,
            msg_type: u16::decode(src, 0)?,
            priority: u16::decode(src, 0)?,
            version: u16::decode(src, 0)?,
            front_len: u32::decode(src, 0)?,
            middle_len: u32::decode(src, 0)?,
            data_len: u32::decode(src, 0)?,
            data_off: u16::decode(src, 0)?,
            src_type: u8::decode(src, 0)?,
            src_num: u64::decode(src, 0)?,
            compat_version: u16::decode(src, 0)?,
            reserved: u16::decode(src, 0)?,
            crc: u32::decode(src, 0)?,
        })
    }

    /// Calculate header CRC (excluding the crc field itself)
    pub fn calc_crc(&self) -> u32 {
        use denc::Denc;
        let mut buf = BytesMut::with_capacity(Self::LENGTH - 4);
        // Use Denc for all fields (CRC calculation is special case, keep manual encoding here is fine
        // but using Denc is more consistent)
        self.seq.encode(&mut buf, 0).unwrap();
        self.tid.encode(&mut buf, 0).unwrap();
        self.msg_type.encode(&mut buf, 0).unwrap();
        self.priority.encode(&mut buf, 0).unwrap();
        self.version.encode(&mut buf, 0).unwrap();
        self.front_len.encode(&mut buf, 0).unwrap();
        self.middle_len.encode(&mut buf, 0).unwrap();
        self.data_len.encode(&mut buf, 0).unwrap();
        self.data_off.encode(&mut buf, 0).unwrap();
        self.src_type.encode(&mut buf, 0).unwrap();
        self.src_num.encode(&mut buf, 0).unwrap();
        self.compat_version.encode(&mut buf, 0).unwrap();
        self.reserved.encode(&mut buf, 0).unwrap();
        crc32c(&buf)
    }
}

/// Trait for message types that can encode/decode their payload
pub trait CephMessagePayload: Sized {
    /// Get the message type constant
    fn msg_type() -> u16;

    /// Get the message version
    fn msg_version() -> u16;

    /// Get the compat version (defaults to msg_version if not overridden)
    fn msg_compat_version() -> u16 {
        Self::msg_version()
    }

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

        // Create header with proper version and compat_version
        let mut header = CephMsgHeader::new(T::msg_type(), T::msg_version());
        header.compat_version = T::msg_compat_version();
        header.front_len = front.len() as u32;
        header.middle_len = middle.len() as u32;
        header.data_len = data.len() as u32;

        // Create footer with CRCs
        let mut footer = CephMsgFooterOld {
            flags: MsgFooterFlags::COMPLETE.bits(),
            ..Default::default()
        };

        if _crc_flags.contains(CrcFlags::DATA) {
            footer.front_crc = crc32c(&front);
            footer.middle_crc = crc32c(&middle);
            footer.data_crc = crc32c(&data);
        } else {
            footer.flags |= MsgFooterFlags::NOCRC.bits();
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
        if footer.flags & MsgFooterFlags::NOCRC.bits() == 0 {
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
        assert!(!encoded.is_empty());
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
        assert!(!encoded.is_empty());
    }
}
