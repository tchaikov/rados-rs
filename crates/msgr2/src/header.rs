use crate::error::Result;
use bytes::{Buf, BufMut};
use std::mem::size_of;

//
// ceph:src/include/msgr.h:ceph_msg_header2
//
#[repr(C, packed)]
#[derive(Debug, denc::ZeroCopyDencode)]
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

impl Clone for MsgHeader {
    fn clone(&self) -> Self {
        // Use read_unaligned to safely copy from packed struct
        unsafe { std::ptr::addr_of!(*self).read_unaligned() }
    }
}

impl MsgHeader {
    pub const LENGTH: usize = size_of::<Self>();

    pub fn encode(&self, dst: &mut impl BufMut) -> Result<()> {
        <Self as denc::zerocopy::Encode>::encode(self, dst)?;
        Ok(())
    }

    pub fn decode(src: &mut impl Buf) -> Result<Self> {
        Ok(<Self as denc::zerocopy::Decode>::decode(src)?)
    }

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
            compat_version: 1,
            reserved: 0,
        }
    }
}
