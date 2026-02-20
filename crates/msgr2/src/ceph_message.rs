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
use bytes::{Buf, Bytes, BytesMut};
use crc32c::crc32c;

// Re-export message type constants
pub use crate::message::{CEPH_MSG_AUTH, CEPH_MSG_AUTH_REPLY, CEPH_MSG_PING, CEPH_MSG_PING_ACK};

/// Old footer format for wire compatibility
/// Reference: ~/dev/linux/include/linux/ceph/msgr.h
#[repr(C)]
#[derive(
    Debug,
    Clone,
    Default,
    denc::ZeroCopyDencode,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
)]
pub struct CephMsgFooterOld {
    pub front_crc: denc::zerocopy::little_endian::U32,
    pub middle_crc: denc::zerocopy::little_endian::U32,
    pub data_crc: denc::zerocopy::little_endian::U32,
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
#[repr(C)]
#[derive(
    Debug,
    Clone,
    denc::ZeroCopyDencode,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
)]
pub struct CephMsgHeader {
    pub seq: denc::zerocopy::little_endian::U64,
    pub tid: denc::zerocopy::little_endian::U64,
    pub msg_type: denc::zerocopy::little_endian::U16,
    pub priority: denc::zerocopy::little_endian::U16,
    pub version: denc::zerocopy::little_endian::U16,
    pub front_len: denc::zerocopy::little_endian::U32,
    pub middle_len: denc::zerocopy::little_endian::U32,
    pub data_len: denc::zerocopy::little_endian::U32,
    pub data_off: denc::zerocopy::little_endian::U16,
    pub src_type: u8,
    pub src_num: denc::zerocopy::little_endian::U64,
    pub compat_version: denc::zerocopy::little_endian::U16,
    pub reserved: denc::zerocopy::little_endian::U16,
    pub crc: denc::zerocopy::little_endian::U32,
}

impl CephMsgHeader {
    pub const LENGTH: usize = 53; // Total size of packed struct

    pub fn new(msg_type: u16, version: u16) -> Self {
        use denc::zerocopy::little_endian::{U16, U32, U64};
        Self {
            seq: U64::new(0),
            tid: U64::new(0),
            msg_type: U16::new(msg_type),
            priority: U16::new(0),
            version: U16::new(version),
            front_len: U32::new(0),
            middle_len: U32::new(0),
            data_len: U32::new(0),
            data_off: U16::new(0),
            src_type: 0,
            src_num: U64::new(0),
            compat_version: U16::new(version),
            reserved: U16::new(0),
            crc: U32::new(0),
        }
    }

    /// Calculate header CRC (excluding the crc field itself)
    pub fn calc_crc(&self) -> u32 {
        use denc::zerocopy::IntoBytes;
        // CRC covers all fields except the last 4 bytes (the crc field itself)
        let bytes = self.as_bytes();
        let crc_bytes = &bytes[..bytes.len() - 4];
        crc32c(crc_bytes)
    }
}

/// Trait for message types that can encode/decode their payload
pub trait CephMessagePayload: Sized {
    /// Get the message type constant
    fn msg_type() -> u16;

    /// Get the message version (may depend on negotiated features)
    fn msg_version(features: u64) -> u16;

    /// Get the compat version (defaults to msg_version if not overridden)
    fn msg_compat_version(features: u64) -> u16 {
        Self::msg_version(features)
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
        let mut header = CephMsgHeader::new(T::msg_type(), T::msg_version(features));
        header.compat_version =
            denc::zerocopy::little_endian::U16::new(T::msg_compat_version(features));
        header.front_len = denc::zerocopy::little_endian::U32::new(front.len() as u32);
        header.middle_len = denc::zerocopy::little_endian::U32::new(middle.len() as u32);
        header.data_len = denc::zerocopy::little_endian::U32::new(data.len() as u32);

        // Create footer with CRCs
        let mut footer = CephMsgFooterOld {
            flags: MsgFooterFlags::COMPLETE.bits(),
            ..Default::default()
        };

        if _crc_flags.contains(CrcFlags::DATA) {
            footer.front_crc = denc::zerocopy::little_endian::U32::new(crc32c(&front));
            footer.middle_crc = denc::zerocopy::little_endian::U32::new(crc32c(&middle));
            footer.data_crc = denc::zerocopy::little_endian::U32::new(crc32c(&data));
        } else {
            footer.flags |= MsgFooterFlags::NOCRC.bits();
        }

        if _crc_flags.contains(CrcFlags::HEADER) {
            header.crc = denc::zerocopy::little_endian::U32::new(header.calc_crc());
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
        let front_len = header.front_len.get();
        let middle_len = header.middle_len.get();
        let data_len = header.data_len.get();

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
        let flags = footer.flags;
        if flags & MsgFooterFlags::NOCRC.bits() == 0 {
            let front_crc_expected = footer.front_crc.get();
            let middle_crc_expected = footer.middle_crc.get();
            let data_crc_expected = footer.data_crc.get();

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

    fn msg_version(_features: u64) -> u16 {
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

    fn msg_version(_features: u64) -> u16 {
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

        // Get field values
        let msg_type = msg.header.msg_type.get();
        let version = msg.header.version.get();

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

        // Get field values
        let msg_type = msg.header.msg_type.get();
        let version = msg.header.version.get();

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
