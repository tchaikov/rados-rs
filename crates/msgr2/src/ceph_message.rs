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
#[repr(C, packed)]
#[derive(Debug, Clone, Default, denc::ZeroCopyDencode)]
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
}

/// Ceph message header
/// Reference: ~/dev/linux/include/linux/ceph/msgr.h struct ceph_msg_header
#[repr(C, packed)]
#[derive(Debug, Clone, denc::ZeroCopyDencode)]
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

    /// Calculate header CRC (excluding the crc field itself)
    pub fn calc_crc(&self) -> u32 {
        use denc::Denc;
        let mut buf = BytesMut::with_capacity(Self::LENGTH - 4);
        // SAFETY: Using read_unaligned to safely read from packed struct fields
        unsafe {
            std::ptr::addr_of!(self.seq)
                .read_unaligned()
                .encode(&mut buf, 0)
                .unwrap();
            std::ptr::addr_of!(self.tid)
                .read_unaligned()
                .encode(&mut buf, 0)
                .unwrap();
            std::ptr::addr_of!(self.msg_type)
                .read_unaligned()
                .encode(&mut buf, 0)
                .unwrap();
            std::ptr::addr_of!(self.priority)
                .read_unaligned()
                .encode(&mut buf, 0)
                .unwrap();
            std::ptr::addr_of!(self.version)
                .read_unaligned()
                .encode(&mut buf, 0)
                .unwrap();
            std::ptr::addr_of!(self.front_len)
                .read_unaligned()
                .encode(&mut buf, 0)
                .unwrap();
            std::ptr::addr_of!(self.middle_len)
                .read_unaligned()
                .encode(&mut buf, 0)
                .unwrap();
            std::ptr::addr_of!(self.data_len)
                .read_unaligned()
                .encode(&mut buf, 0)
                .unwrap();
            std::ptr::addr_of!(self.data_off)
                .read_unaligned()
                .encode(&mut buf, 0)
                .unwrap();
            std::ptr::addr_of!(self.src_type)
                .read_unaligned()
                .encode(&mut buf, 0)
                .unwrap();
            std::ptr::addr_of!(self.src_num)
                .read_unaligned()
                .encode(&mut buf, 0)
                .unwrap();
            std::ptr::addr_of!(self.compat_version)
                .read_unaligned()
                .encode(&mut buf, 0)
                .unwrap();
            std::ptr::addr_of!(self.reserved)
                .read_unaligned()
                .encode(&mut buf, 0)
                .unwrap();
        }
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
        use denc::Denc;

        let total_len = CephMsgHeader::LENGTH
            + CephMsgFooterOld::LENGTH
            + self.front.len()
            + self.middle.len()
            + self.data.len();

        let mut buf = BytesMut::with_capacity(total_len);

        // 1. Encode header
        Denc::encode(&self.header, &mut buf, 0)?;

        // 2. Encode old footer
        Denc::encode(&self.footer, &mut buf, 0)?;

        // 3. Encode payload sections
        buf.extend_from_slice(&self.front);
        buf.extend_from_slice(&self.middle);
        buf.extend_from_slice(&self.data);

        Ok(buf.freeze())
    }

    /// Decode a complete message from bytes
    pub fn decode(src: &mut impl Buf) -> Result<Self> {
        use denc::Denc;

        // 1. Decode header
        let header = CephMsgHeader::decode(src, 0)?;

        // 2. Decode old footer
        let footer = CephMsgFooterOld::decode(src, 0)?;

        // 3. Decode payload sections
        // SAFETY: Reading from packed struct fields using read_unaligned
        let front_len = unsafe { std::ptr::addr_of!(header.front_len).read_unaligned() };
        let middle_len = unsafe { std::ptr::addr_of!(header.middle_len).read_unaligned() };
        let data_len = unsafe { std::ptr::addr_of!(header.data_len).read_unaligned() };

        if src.remaining() < (front_len + middle_len + data_len) as usize {
            return Err(Error::Deserialization("Incomplete message payload".into()));
        }

        let mut front = vec![0u8; front_len as usize];
        src.copy_to_slice(&mut front);

        let mut middle = vec![0u8; middle_len as usize];
        src.copy_to_slice(&mut middle);

        let mut data = vec![0u8; data_len as usize];
        src.copy_to_slice(&mut data);

        // Verify CRCs if not disabled
        // SAFETY: Reading from packed struct fields using read_unaligned
        let flags = unsafe { std::ptr::addr_of!(footer.flags).read_unaligned() };
        if flags & MsgFooterFlags::NOCRC.bits() == 0 {
            let front_crc_expected =
                unsafe { std::ptr::addr_of!(footer.front_crc).read_unaligned() };
            let middle_crc_expected =
                unsafe { std::ptr::addr_of!(footer.middle_crc).read_unaligned() };
            let data_crc_expected = unsafe { std::ptr::addr_of!(footer.data_crc).read_unaligned() };

            let front_crc = crc32c(&front);
            if front_crc != front_crc_expected {
                return Err(Error::Deserialization(format!(
                    "Front CRC mismatch: got {}, expected {}",
                    front_crc, front_crc_expected
                )));
            }

            let middle_crc = crc32c(&middle);
            if middle_crc != middle_crc_expected {
                return Err(Error::Deserialization(format!(
                    "Middle CRC mismatch: got {}, expected {}",
                    middle_crc, middle_crc_expected
                )));
            }

            let data_crc = crc32c(&data);
            if data_crc != data_crc_expected {
                return Err(Error::Deserialization(format!(
                    "Data CRC mismatch: got {}, expected {}",
                    data_crc, data_crc_expected
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

        // Copy packed struct fields to local variables to avoid unaligned references
        let msg_type = msg.header.msg_type;
        let version = msg.header.version;

        assert_eq!(msg_type, CEPH_MSG_PING);
        assert_eq!(version, 1);
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

        // Copy packed struct fields to local variables to avoid unaligned references
        let msg_type = msg.header.msg_type;
        let version = msg.header.version;

        assert_eq!(msg_type, CEPH_MSG_PING_ACK);
        assert_eq!(version, 1);
        assert_eq!(msg.front.len(), 0);
        assert_eq!(msg.middle.len(), 0);
        assert_eq!(msg.data.len(), 0);

        // Encode to bytes
        let encoded = msg.encode().unwrap();
        assert!(!encoded.is_empty());
    }
}
