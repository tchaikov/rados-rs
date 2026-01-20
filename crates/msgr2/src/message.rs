use crate::error::{Error, Result};
use crate::header::MsgHeader;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::fmt;

pub const CEPH_MSG_PING: u16 = 0x0001;
pub const CEPH_MSG_PING_ACK: u16 = 0x0002;
pub const CEPH_MSG_OSD_MAP: u16 = 0x0041;
pub const CEPH_MSG_MON_MAP: u16 = 0x0042;
pub const CEPH_MSG_MON_COMMAND: u16 = 0x0050;
pub const CEPH_MSG_MON_COMMAND_ACK: u16 = 0x0051;
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
    }

    pub fn ping_ack() -> Self {
        Self::new(CEPH_MSG_PING_ACK, Bytes::new())
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

        // Calculate payload lengths (simplified for now)
        let front_len = src.remaining().min(1024); // TODO: proper length calculation
        let mut front = vec![0u8; front_len];
        if front_len > 0 && src.remaining() >= front_len {
            src.copy_to_slice(&mut front);
        }

        Ok(Self {
            header,
            front: Bytes::from(front),
            middle: Bytes::new(),
            data: Bytes::new(),
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

    pub fn total_len(&self) -> usize {
        MsgHeader::LENGTH + self.front.len() + self.middle.len() + self.data.len()
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
        if dst.remaining_mut() < Self::LENGTH {
            return Err(Error::Serialization);
        }

        dst.put_u32_le(self.front_crc);
        dst.put_u32_le(self.middle_crc);
        dst.put_u32_le(self.data_crc);
        dst.put_u64_le(self.sig);
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
            sig: src.get_u64_le(),
            flags: src.get_u8(),
        })
    }
}

impl Default for MsgFooter {
    fn default() -> Self {
        Self::new()
    }
}
