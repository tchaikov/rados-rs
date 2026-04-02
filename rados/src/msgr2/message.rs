//! Message wrappers and priorities for transporting Ceph payloads over msgr2.
//!
//! This module defines the message envelope types used by the transport layer,
//! including queue priority metadata and helpers for converting payloads into
//! framed msgr2 messages. It bridges higher-level Ceph payload structs and the
//! lower-level frame and header code.

use crate::msgr2::error::{Msgr2Error as Error, Result};
use crate::msgr2::header::MsgHeader;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::fmt;

/// Message priority levels for priority-based queueing
/// Matches Ceph's priority scheme where higher values are sent first
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    num_enum::FromPrimitive,
    num_enum::IntoPrimitive,
)]
#[repr(u16)]
pub enum MessagePriority {
    Low = 0,
    Normal = 1,
    /// Values >= 2 map to High, 1 maps to Normal, 0 maps to Low
    #[num_enum(default)]
    High = 2,
}

/// Known Ceph message type identifiers used by this crate.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, num_enum::TryFromPrimitive, num_enum::IntoPrimitive,
)]
#[repr(u16)]
pub enum MessageType {
    Ping = 0x0001,
    PingAck = 0x0002,
    MonMap = 0x0004,
    Auth = 0x0011,
    AuthReply = 0x0012,
    PoolOpReply = 48,
    PoolOp = 49,
    MonCommand = 50,
    MonCommandAck = 51,
    Config = 62,
    OsdMap = 0x0029,
}

pub const CEPH_MSG_PING: u16 = MessageType::Ping as u16;
pub const CEPH_MSG_PING_ACK: u16 = MessageType::PingAck as u16;
pub const CEPH_MSG_MON_MAP: u16 = MessageType::MonMap as u16;
pub const CEPH_MSG_OSD_MAP: u16 = MessageType::OsdMap as u16; // 41 decimal - Verified from Ceph src/include/ceph_fs.h:174
pub const CEPH_MSG_MON_COMMAND: u16 = MessageType::MonCommand as u16;
pub const CEPH_MSG_MON_COMMAND_ACK: u16 = MessageType::MonCommandAck as u16;
pub const CEPH_MSG_POOLOP_REPLY: u16 = MessageType::PoolOpReply as u16;
pub const CEPH_MSG_POOLOP: u16 = MessageType::PoolOp as u16;
pub const CEPH_MSG_CONFIG: u16 = MessageType::Config as u16;
pub const CEPH_MSG_AUTH: u16 = MessageType::Auth as u16;
pub const CEPH_MSG_AUTH_REPLY: u16 = MessageType::AuthReply as u16;

#[derive(Debug, Clone)]
pub struct Message {
    pub header: MsgHeader,
    pub front: Bytes,
    pub middle: Bytes,
    pub data: Bytes,
    pub footer: Option<MsgFooter>,
}

impl Message {
    pub fn new(msg_type: u16, front: Bytes) -> Self {
        let header = MsgHeader::new_default(msg_type, 0);
        Self {
            header,
            front,
            middle: Bytes::new(),
            data: Bytes::new(),
            footer: None,
        }
    }

    pub fn new_typed(msg_type: MessageType, front: Bytes) -> Self {
        Self::new(msg_type.into(), front)
    }

    pub fn ping() -> Self {
        Self::new_typed(MessageType::Ping, Bytes::new()).with_priority(MessagePriority::High)
    }

    pub fn ping_ack() -> Self {
        Self::new_typed(MessageType::PingAck, Bytes::new()).with_priority(MessagePriority::High)
    }

    pub fn with_seq(mut self, seq: u64) -> Self {
        self.header.set_seq(seq);
        self
    }

    pub fn with_tid(mut self, tid: u64) -> Self {
        self.header.set_tid(tid);
        self
    }

    pub fn with_priority(mut self, priority: MessagePriority) -> Self {
        self.header.priority.set(priority.into());
        self
    }

    /// Create a Message from a CephMessage
    pub fn from_ceph_message(ceph_msg: crate::msgr2::ceph_message::CephMessage) -> Self {
        let mut header = MsgHeader::new_default(
            ceph_msg.header.msg_type.get(),
            ceph_msg.header.priority.get(),
        );
        header.set_tid(ceph_msg.header.tid.get());
        header.version = ceph_msg.header.version; // Message version, not protocol version
        header.compat_version = ceph_msg.header.compat_version;
        header.data_off = ceph_msg.header.data_off;

        Self {
            header,
            front: ceph_msg.front,
            middle: ceph_msg.middle,
            data: ceph_msg.data,
            footer: None,
        }
    }

    pub fn with_version(mut self, version: u16) -> Self {
        self.header.version = version.into();
        self
    }

    pub fn encode(&self, dst: &mut BytesMut) -> Result<()> {
        let front_len = self.front.len() as u32;
        let middle_len = self.middle.len() as u32;
        let data_len = self.data.len() as u32;
        let total_len =
            MsgHeader::LENGTH + front_len as usize + middle_len as usize + data_len as usize;

        if dst.remaining_mut() < total_len {
            return Err(Error::Serialization);
        }

        let mut header = self.header;
        header
            .data_off
            .set((MsgHeader::LENGTH + front_len as usize) as u16);
        header.encode(dst)?;

        dst.extend_from_slice(&self.front);
        dst.extend_from_slice(&self.middle);
        dst.extend_from_slice(&self.data);

        Ok(())
    }

    /// Decode a flattened message payload when only `data_off` is known.
    ///
    /// The legacy message header only carries the end of the front segment, so
    /// callers without frame segment descriptors cannot distinguish `middle`
    /// from `data`. This helper preserves the front segment and flattens the
    /// remaining payload into `data`.
    pub fn decode_flattened(src: &mut impl Buf) -> Result<Self> {
        let header = MsgHeader::decode(src)?;

        // data_off = MsgHeader::LENGTH + front_len
        let data_off = header.get_data_off() as usize;
        if data_off < MsgHeader::LENGTH {
            return Err(Error::protocol_error("Invalid data_off in message header"));
        }

        let front_len = data_off - MsgHeader::LENGTH;

        if src.remaining() < front_len {
            return Err(Error::protocol_error("Insufficient data for front segment"));
        }
        let mut front = vec![0u8; front_len];
        if front_len > 0 {
            src.copy_to_slice(&mut front);
        }

        // Read remaining bytes as data segment
        // Note: Without explicit segment lengths, we cannot separate middle from data.
        // All remaining payload goes into data segment, middle is left empty.
        let remaining_len = src.remaining();
        let mut data = vec![0u8; remaining_len];
        if remaining_len > 0 {
            src.copy_to_slice(&mut data);
        }

        Ok(Self {
            header,
            front: Bytes::from(front),
            middle: Bytes::new(),
            data: Bytes::from(data),
            footer: None,
        })
    }

    /// Decode message with explicit segment lengths (used by frame protocol).
    ///
    /// Unlike [`Self::decode_flattened`], this preserves front/middle/data
    /// segment boundaries and should be used for msgr2 MESSAGE frames.
    pub fn decode_segments(
        src: &mut impl Buf,
        front_len: usize,
        middle_len: usize,
        data_len: usize,
    ) -> Result<Self> {
        let header = MsgHeader::decode(src)?;
        let total_payload = front_len + middle_len + data_len;
        if src.remaining() < total_payload {
            return Err(Error::protocol_error(
                "Insufficient data for message segments",
            ));
        }

        let mut front = vec![0u8; front_len];
        if front_len > 0 {
            src.copy_to_slice(&mut front);
        }

        let mut middle = vec![0u8; middle_len];
        if middle_len > 0 {
            src.copy_to_slice(&mut middle);
        }

        let mut data = vec![0u8; data_len];
        if data_len > 0 {
            src.copy_to_slice(&mut data);
        }

        Ok(Self {
            header,
            front: Bytes::from(front),
            middle: Bytes::from(middle),
            data: Bytes::from(data),
            footer: None,
        })
    }

    pub fn msg_type(&self) -> u16 {
        self.header.msg_type.get()
    }

    pub fn msg_type_enum(
        &self,
    ) -> std::result::Result<MessageType, num_enum::TryFromPrimitiveError<MessageType>> {
        MessageType::try_from(self.msg_type())
    }

    pub fn seq(&self) -> u64 {
        self.header.get_seq()
    }

    pub fn tid(&self) -> u64 {
        self.header.get_tid()
    }

    /// Get the message priority as MessagePriority enum
    pub fn priority(&self) -> MessagePriority {
        MessagePriority::from(self.header.get_priority())
    }

    pub fn total_len(&self) -> usize {
        MsgHeader::LENGTH + self.front.len() + self.middle.len() + self.data.len()
    }

    /// Build a `Message` from raw frame segments (header, front, middle, data).
    ///
    /// This is the canonical way to convert a received msgr2 `Frame` with
    /// `Tag::Message` into a `Message`.  Both the unsplit `Connection` and the
    /// split I/O task use this to avoid duplicating the extraction logic.
    pub fn from_frame_segments(segments: &[Bytes]) -> Result<Self> {
        if segments.is_empty() {
            return Err(Error::protocol_error(
                "Message frame missing header segment",
            ));
        }

        let mut header_buf = segments[0].as_ref();
        let header = MsgHeader::decode(&mut header_buf)?;

        let front = segments.get(1).cloned().unwrap_or_default();
        let middle = segments.get(2).cloned().unwrap_or_default();
        let data = segments.get(3).cloned().unwrap_or_default();

        Ok(Self {
            header,
            front,
            middle,
            data,
            footer: None,
        })
    }
}

impl crate::msgr2::priority_queue::Prioritized for Message {
    fn priority(&self) -> MessagePriority {
        self.priority()
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let type_name = match self.msg_type_enum() {
            Ok(MessageType::Ping) => "PING",
            Ok(MessageType::PingAck) => "PING_ACK",
            Ok(MessageType::OsdMap) => "OSD_MAP",
            Ok(MessageType::MonMap) => "MON_MAP",
            Ok(MessageType::MonCommand) => "MON_COMMAND",
            Ok(MessageType::MonCommandAck) => "MON_COMMAND_ACK",
            Ok(MessageType::Config) => "CONFIG",
            Ok(MessageType::Auth) => "AUTH",
            Ok(MessageType::AuthReply) => "AUTH_REPLY",
            Ok(MessageType::PoolOpReply) => "POOLOP_REPLY",
            Ok(MessageType::PoolOp) => "POOLOP",
            Err(_) => {
                return write!(
                    f,
                    "Message(type={}, seq={}, len={})",
                    self.msg_type(),
                    self.seq(),
                    self.total_len()
                );
            }
        };

        write!(
            f,
            "Message({}[{}], seq={}, len={})",
            type_name,
            self.msg_type(),
            self.seq(),
            self.total_len()
        )
    }
}

#[derive(Debug, Clone, Default)]
pub struct MsgFooter {
    pub front_crc: u32,
    pub middle_crc: u32,
    pub data_crc: u32,
    pub sig: u64,
    pub flags: u8,
}

impl MsgFooter {
    pub const LENGTH: usize = 21;

    pub fn encode(&self, dst: &mut impl BufMut) -> Result<()> {
        use crate::Denc;
        self.front_crc.encode(dst, 0)?;
        self.middle_crc.encode(dst, 0)?;
        self.data_crc.encode(dst, 0)?;
        self.sig.encode(dst, 0)?;
        self.flags.encode(dst, 0)?;
        Ok(())
    }

    pub fn decode(src: &mut impl Buf) -> Result<Self> {
        use crate::Denc;
        Ok(Self {
            front_crc: u32::decode(src, 0)?,
            middle_crc: u32::decode(src, 0)?,
            data_crc: u32::decode(src, 0)?,
            sig: u64::decode(src, 0)?,
            flags: u8::decode(src, 0)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_roundtrip_front_only() {
        let front = Bytes::from("Hello, Ceph!");
        let msg = Message::new(CEPH_MSG_PING, front.clone());

        // Encode
        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();

        // Decode
        let mut read_buf = buf.freeze();
        let decoded = Message::decode_flattened(&mut read_buf).unwrap();

        assert_eq!(decoded.msg_type(), CEPH_MSG_PING);
        assert_eq!(decoded.front, front);
        assert_eq!(decoded.middle.len(), 0);
        assert_eq!(decoded.data.len(), 0);
    }

    #[test]
    fn test_message_roundtrip_with_middle_and_data() {
        let front = Bytes::from("front");
        let middle = Bytes::from("middle");
        let data = Bytes::from("data");

        let msg = Message {
            header: MsgHeader::new_default(CEPH_MSG_AUTH, 10),
            front: front.clone(),
            middle: middle.clone(),
            data: data.clone(),
            footer: None,
        };

        // Encode
        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();

        // Decode using basic decode() - will combine middle+data
        let mut read_buf = buf.freeze();
        let decoded = Message::decode_flattened(&mut read_buf).unwrap();

        assert_eq!(decoded.msg_type(), CEPH_MSG_AUTH);
        assert_eq!(decoded.front, front);
        // middle and data are combined in basic decode
        assert_eq!(decoded.middle.len(), 0);
        let mut expected_data = BytesMut::new();
        expected_data.extend_from_slice(&middle);
        expected_data.extend_from_slice(&data);
        assert_eq!(decoded.data, expected_data.freeze());
    }

    #[test]
    fn test_message_decode_segments() {
        let front = Bytes::from("front payload");
        let middle = Bytes::from("middle payload");
        let data = Bytes::from("data payload");

        let msg = Message {
            header: MsgHeader::new_default(CEPH_MSG_OSD_MAP, 5),
            front: front.clone(),
            middle: middle.clone(),
            data: data.clone(),
            footer: None,
        };

        // Encode
        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();

        // Decode with explicit segment lengths
        let mut read_buf = buf.freeze();
        let decoded =
            Message::decode_segments(&mut read_buf, front.len(), middle.len(), data.len()).unwrap();

        assert_eq!(decoded.msg_type(), CEPH_MSG_OSD_MAP);
        assert_eq!(decoded.front, front);
        assert_eq!(decoded.middle, middle);
        assert_eq!(decoded.data, data);
    }

    #[test]
    fn test_message_decode_empty_payload() {
        let msg = Message::ping();

        // Encode
        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();

        // Decode
        let mut read_buf = buf.freeze();
        let decoded = Message::decode_flattened(&mut read_buf).unwrap();

        assert_eq!(decoded.msg_type(), CEPH_MSG_PING);
        assert_eq!(decoded.front.len(), 0);
        assert_eq!(decoded.middle.len(), 0);
        assert_eq!(decoded.data.len(), 0);
    }

    #[test]
    fn test_message_decode_segments_empty() {
        let msg = Message::ping_ack();

        // Encode
        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();

        // Decode with explicit lengths (all zero)
        let mut read_buf = buf.freeze();
        let decoded = Message::decode_segments(&mut read_buf, 0, 0, 0).unwrap();

        assert_eq!(decoded.msg_type(), CEPH_MSG_PING_ACK);
        assert_eq!(decoded.front.len(), 0);
        assert_eq!(decoded.middle.len(), 0);
        assert_eq!(decoded.data.len(), 0);
    }

    #[test]
    fn test_message_type_helpers() {
        let msg = Message::new_typed(MessageType::Auth, Bytes::new());
        assert_eq!(msg.msg_type(), CEPH_MSG_AUTH);
        assert_eq!(msg.msg_type_enum().unwrap(), MessageType::Auth);
    }

    #[test]
    fn test_message_total_len() {
        let front = Bytes::from("12345");
        let middle = Bytes::from("67");
        let data = Bytes::from("890");

        let msg = Message {
            header: MsgHeader::new_default(CEPH_MSG_AUTH, 0),
            front,
            middle,
            data,
            footer: None,
        };

        // total_len = header + front + middle + data
        // = MsgHeader::LENGTH + 5 + 2 + 3
        assert_eq!(msg.total_len(), MsgHeader::LENGTH + 10);
    }
}
