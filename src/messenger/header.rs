use bytes::{Buf, BufMut};
use crate::error::{Result, Error};
use std::mem::size_of;

//
// ceph:src/include/msgr.h:ceph_msg_header2
//
#[repr(C, packed)]
#[derive(Debug, Clone, PartialEq)]
pub struct MsgHeader {
    pub seq: u64,
    pub tid: u64,
    pub msg_type: u16,
    pub priority: u16,
    pub version: u16,
    pub data_pre_padding_len: u32,
    pub data_off: u16,
    pub ack_seq: u64,
    pub flags: u8,
    pub compat_version: u16,
    pub reserved: u16,
}

impl MsgHeader {
    pub const LENGTH: usize = size_of::<Self>();

    pub fn encode(&self, dst: &mut impl BufMut) -> Result<()> {
        if dst.remaining_mut() < Self::LENGTH {
            return Err(Error::Serialization);
        }

        // could use ptr::copy_nonoverlapping() for better performance
        dst.put_u64_le(self.seq);
        dst.put_u64_le(self.tid);
        dst.put_u16_le(self.msg_type);
        dst.put_u16_le(self.priority);
        dst.put_u16_le(self.version);
        dst.put_u32_le(self.data_pre_padding_len);
        dst.put_u16_le(self.data_off);
        dst.put_u64_le(self.ack_seq);
        dst.put_u8(self.flags);
        dst.put_u16_le(self.compat_version);
        dst.put_u16_le(self.reserved);

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
            data_pre_padding_len: src.get_u32_le(),
            data_off: src.get_u16_le(),
            ack_seq: src.get_u64_le(),
            flags: src.get_u8(),
            compat_version: src.get_u16_le(),
            reserved: src.get_u16_le(),
        })
    }

    /// 根据Crimson的create_message_header逻辑创建默认头
    pub fn new_default(msg_type: u16, priority: u16) -> Self {
        Self {
            seq: 0,
            tid: 0,
            msg_type,
            priority,
            version: 2, // Protocol V2
            data_pre_padding_len: 0,
            data_off: 0,
            ack_seq: 0,
            flags: 0,
            compat_version: 1, // 最小兼容版本
            reserved: 0,
        }
    }
}
