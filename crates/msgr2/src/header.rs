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
        <Self as denc::Denc>::encode(self, dst, 0)?;
        Ok(())
    }

    pub fn decode(src: &mut impl Buf) -> Result<Self> {
        Ok(<Self as denc::Denc>::decode(src, 0)?)
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

    // Safe accessors for packed fields
    // These methods safely copy values from the packed struct without taking references,
    // avoiding undefined behavior from unaligned field access.

    /// Get sequence number (safe copy from packed field)
    #[inline]
    pub fn get_seq(&self) -> u64 {
        // SAFETY: Reading a copy of a packed field is safe
        unsafe { std::ptr::addr_of!(self.seq).read_unaligned() }
    }

    /// Get transaction ID (safe copy from packed field)
    #[inline]
    pub fn get_tid(&self) -> u64 {
        unsafe { std::ptr::addr_of!(self.tid).read_unaligned() }
    }

    /// Get message type (safe copy from packed field)
    #[inline]
    pub fn get_msg_type(&self) -> u16 {
        unsafe { std::ptr::addr_of!(self.msg_type).read_unaligned() }
    }

    /// Get priority (safe copy from packed field)
    #[inline]
    pub fn get_priority(&self) -> u16 {
        unsafe { std::ptr::addr_of!(self.priority).read_unaligned() }
    }

    /// Get version (safe copy from packed field)
    #[inline]
    pub fn get_version(&self) -> u16 {
        unsafe { std::ptr::addr_of!(self.version).read_unaligned() }
    }

    /// Get data pre-padding length (safe copy from packed field)
    #[inline]
    pub fn get_data_pre_padding_len(&self) -> u32 {
        unsafe { std::ptr::addr_of!(self.data_pre_padding_len).read_unaligned() }
    }

    /// Get data offset (safe copy from packed field)
    #[inline]
    pub fn get_data_off(&self) -> u16 {
        unsafe { std::ptr::addr_of!(self.data_off).read_unaligned() }
    }

    /// Get acknowledgment sequence (safe copy from packed field)
    #[inline]
    pub fn get_ack_seq(&self) -> u64 {
        unsafe { std::ptr::addr_of!(self.ack_seq).read_unaligned() }
    }

    /// Get flags (safe copy from packed field)
    #[inline]
    pub fn get_flags(&self) -> u8 {
        unsafe { std::ptr::addr_of!(self.flags).read_unaligned() }
    }

    /// Get compatible version (safe copy from packed field)
    #[inline]
    pub fn get_compat_version(&self) -> u16 {
        unsafe { std::ptr::addr_of!(self.compat_version).read_unaligned() }
    }

    /// Get reserved field (safe copy from packed field)
    #[inline]
    pub fn get_reserved(&self) -> u16 {
        unsafe { std::ptr::addr_of!(self.reserved).read_unaligned() }
    }

    // Safe setters for packed fields
    // These methods safely write values to the packed struct without taking mutable references.

    /// Set sequence number (safe write to packed field)
    #[inline]
    pub fn set_seq(&mut self, value: u64) {
        unsafe { std::ptr::addr_of_mut!(self.seq).write_unaligned(value) }
    }

    /// Set transaction ID (safe write to packed field)
    #[inline]
    pub fn set_tid(&mut self, value: u64) {
        unsafe { std::ptr::addr_of_mut!(self.tid).write_unaligned(value) }
    }

    /// Set acknowledgment sequence (safe write to packed field)
    #[inline]
    pub fn set_ack_seq(&mut self, value: u64) {
        unsafe { std::ptr::addr_of_mut!(self.ack_seq).write_unaligned(value) }
    }

    /// Set flags (safe write to packed field)
    #[inline]
    pub fn set_flags(&mut self, value: u8) {
        unsafe { std::ptr::addr_of_mut!(self.flags).write_unaligned(value) }
    }
}
