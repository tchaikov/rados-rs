use crate::error::{Error, Result};
use crate::header::MsgHeader;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::fmt;

/// Message priority levels for priority-based queueing
/// Matches Ceph's priority scheme where higher values are sent first
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u16)]
pub enum MessagePriority {
    Low = 0,
    Normal = 1,
    High = 2,
}

impl MessagePriority {
    /// Convert a u16 priority value to MessagePriority
    /// Values >= 2 map to High, 1 maps to Normal, 0 maps to Low
    pub fn from_u16(priority: u16) -> Self {
        match priority {
            0 => MessagePriority::Low,
            1 => MessagePriority::Normal,
            _ => MessagePriority::High, // 2 and above -> High
        }
    }

    /// Convert MessagePriority to u16 for message header
    pub fn to_u16(self) -> u16 {
        self as u16
    }
}

pub const CEPH_MSG_PING: u16 = 0x0001;
pub const CEPH_MSG_PING_ACK: u16 = 0x0002;
pub const CEPH_MSG_MON_MAP: u16 = 0x0004;
pub const CEPH_MSG_OSD_MAP: u16 = 0x0029; // 41 decimal - Verified from Ceph src/include/ceph_fs.h:174
pub const CEPH_MSG_MON_COMMAND: u16 = 50;
pub const CEPH_MSG_MON_COMMAND_ACK: u16 = 51;
pub const CEPH_MSG_POOLOP_REPLY: u16 = 48;
pub const CEPH_MSG_POOLOP: u16 = 49;
pub const CEPH_MSG_CONFIG: u16 = 62;
pub const CEPH_MSG_AUTH: u16 = 0x0011;
pub const CEPH_MSG_AUTH_REPLY: u16 = 0x0012;

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

    pub fn ping() -> Self {
        Self::new(CEPH_MSG_PING, Bytes::new())
            .with_priority(MessagePriority::High.to_u16())
    }

    pub fn ping_ack() -> Self {
        Self::new(CEPH_MSG_PING_ACK, Bytes::new())
            .with_priority(MessagePriority::High.to_u16())
    }

    pub fn with_seq(mut self, seq: u64) -> Self {
        self.header.seq = seq;
        self
    }

    pub fn with_tid(mut self, tid: u64) -> Self {
        self.header.tid = tid;
        self
    }

    pub fn with_priority(mut self, priority: u16) -> Self {
        self.header.priority = priority;
        self
    }

    /// Create a Message from a CephMessage
    pub fn from_ceph_message(ceph_msg: crate::ceph_message::CephMessage) -> Self {
        let mut header = MsgHeader::new_default(ceph_msg.header.msg_type, ceph_msg.header.priority);
        header.tid = ceph_msg.header.tid;
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
        self.header.version = version;
        self
    }

    pub fn encode(&self, dst: &mut BytesMut) -> Result<()> {
        // Calculate total message size
        let front_len = self.front.len() as u32;
        let middle_len = self.middle.len() as u32;
        let data_len = self.data.len() as u32;

        let total_len =
            MsgHeader::LENGTH + front_len as usize + middle_len as usize + data_len as usize;

        if dst.remaining_mut() < total_len {
            return Err(Error::Serialization);
        }

        // Update header with lengths
        let mut header = self.header.clone();
        header.data_off = (MsgHeader::LENGTH + front_len as usize) as u16;

        // Encode header
        header.encode(dst)?;

        // Encode payload sections
        dst.extend_from_slice(&self.front);
        dst.extend_from_slice(&self.middle);
        dst.extend_from_slice(&self.data);

        Ok(())
    }

    pub fn decode(src: &mut impl Buf) -> Result<Self> {
        // Decode header
        let header = MsgHeader::decode(src)?;

        // Calculate front length from data_off field
        // data_off = MsgHeader::LENGTH + front_len
        let data_off = header.get_data_off() as usize;
        if data_off < MsgHeader::LENGTH {
            return Err(Error::protocol_error("Invalid data_off in message header"));
        }

        let front_len = data_off - MsgHeader::LENGTH;

        // Decode front segment
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

    /// Decode message with explicit segment lengths (used by frame protocol)
    pub fn decode_segments(
        src: &mut impl Buf,
        front_len: usize,
        middle_len: usize,
        data_len: usize,
    ) -> Result<Self> {
        // Decode header
        let header = MsgHeader::decode(src)?;

        // Verify we have enough data
        let total_payload = front_len + middle_len + data_len;
        if src.remaining() < total_payload {
            return Err(Error::protocol_error(
                "Insufficient data for message segments",
            ));
        }

        // Decode front segment
        let mut front = vec![0u8; front_len];
        if front_len > 0 {
            src.copy_to_slice(&mut front);
        }

        // Decode middle segment
        let mut middle = vec![0u8; middle_len];
        if middle_len > 0 {
            src.copy_to_slice(&mut middle);
        }

        // Decode data segment
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
        self.header.msg_type
    }

    pub fn seq(&self) -> u64 {
        self.header.seq
    }

    pub fn tid(&self) -> u64 {
        self.header.tid
    }

    /// Get the message priority as MessagePriority enum
    pub fn priority(&self) -> MessagePriority {
        MessagePriority::from_u16(self.header.get_priority())
    }

    pub fn total_len(&self) -> usize {
        MsgHeader::LENGTH + self.front.len() + self.middle.len() + self.data.len()
    }

    /// Calculate total message size for throttling purposes
    /// This includes header + all payload segments
    pub fn total_size(&self) -> u64 {
        self.total_len() as u64
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let type_name = match self.msg_type() {
            CEPH_MSG_PING => "PING",
            CEPH_MSG_PING_ACK => "PING_ACK",
            CEPH_MSG_OSD_MAP => "OSD_MAP",
            CEPH_MSG_MON_MAP => "MON_MAP",
            CEPH_MSG_MON_COMMAND => "MON_COMMAND",
            CEPH_MSG_MON_COMMAND_ACK => "MON_COMMAND_ACK",
            CEPH_MSG_CONFIG => "CONFIG",
            CEPH_MSG_AUTH => "AUTH",
            CEPH_MSG_AUTH_REPLY => "AUTH_REPLY",
            t => {
                return write!(
                    f,
                    "Message(type={}, seq={}, len={})",
                    t,
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

#[derive(Debug, Clone)]
pub struct MsgFooter {
    pub front_crc: u32,
    pub middle_crc: u32,
    pub data_crc: u32,
    pub sig: u64,
    pub flags: u8,
}

impl MsgFooter {
    pub const LENGTH: usize = 21;

    pub fn new() -> Self {
        Self {
            front_crc: 0,
            middle_crc: 0,
            data_crc: 0,
            sig: 0,
            flags: 0,
        }
    }

    pub fn encode(&self, dst: &mut impl BufMut) -> Result<()> {
        use denc::Denc;
        self.front_crc.encode(dst, 0)?;
        self.middle_crc.encode(dst, 0)?;
        self.data_crc.encode(dst, 0)?;
        self.sig.encode(dst, 0)?;
        self.flags.encode(dst, 0)?;
        Ok(())
    }

    pub fn decode(src: &mut impl Buf) -> Result<Self> {
        use denc::Denc;
        Ok(Self {
            front_crc: u32::decode(src, 0)?,
            middle_crc: u32::decode(src, 0)?,
            data_crc: u32::decode(src, 0)?,
            sig: u64::decode(src, 0)?,
            flags: u8::decode(src, 0)?,
        })
    }
}

impl Default for MsgFooter {
    fn default() -> Self {
        Self::new()
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
        let decoded = Message::decode(&mut read_buf).unwrap();

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
        let decoded = Message::decode(&mut read_buf).unwrap();

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
        let decoded = Message::decode(&mut read_buf).unwrap();

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

#[cfg(test)]
mod priority_tests {
    use super::*;

    #[test]
    fn test_ping_has_high_priority() {
        let ping = Message::ping();
        assert_eq!(ping.priority(), MessagePriority::High);
        assert_eq!(ping.header.get_priority(), MessagePriority::High.to_u16());
    }

    #[test]
    fn test_ping_ack_has_high_priority() {
        let ping_ack = Message::ping_ack();
        assert_eq!(ping_ack.priority(), MessagePriority::High);
        assert_eq!(ping_ack.header.get_priority(), MessagePriority::High.to_u16());
    }

    #[test]
    fn test_message_priority_from_u16() {
        assert_eq!(MessagePriority::from_u16(0), MessagePriority::Low);
        assert_eq!(MessagePriority::from_u16(1), MessagePriority::Normal);
        assert_eq!(MessagePriority::from_u16(2), MessagePriority::High);
        assert_eq!(MessagePriority::from_u16(5), MessagePriority::High); // Values >= 2 map to High
    }

    #[test]
    fn test_message_priority_to_u16() {
        assert_eq!(MessagePriority::Low.to_u16(), 0);
        assert_eq!(MessagePriority::Normal.to_u16(), 1);
        assert_eq!(MessagePriority::High.to_u16(), 2);
    }

    #[test]
    fn test_message_with_priority() {
        let msg = Message::new(CEPH_MSG_MON_MAP, Bytes::from("test"))
            .with_priority(MessagePriority::High.to_u16());
        assert_eq!(msg.priority(), MessagePriority::High);
    }
}
