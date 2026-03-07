//! Message-header structures used inside msgr2 control and data frames.
//!
//! This module wraps Ceph's msgr2 header layouts in zero-copy Rust types so
//! frame encoding and decoding can operate efficiently on the wire format.
//! These headers sit beneath higher-level Ceph messages and above the frame
//! preamble and segment metadata.

use crate::error::Result;
use bytes::{Buf, BufMut};
use std::mem::size_of;

//
// ceph:src/include/msgr.h:ceph_msg_header2
//
#[repr(C)]
#[derive(
    Debug,
    Copy,
    Clone,
    denc::ZeroCopyDencode,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
)]
pub struct MsgHeader {
    seq: denc::zerocopy::little_endian::U64,
    tid: denc::zerocopy::little_endian::U64,
    pub msg_type: denc::zerocopy::little_endian::U16,
    pub priority: denc::zerocopy::little_endian::U16,
    pub version: denc::zerocopy::little_endian::U16,
    pub data_pre_padding_len: denc::zerocopy::little_endian::U32,
    pub data_off: denc::zerocopy::little_endian::U16,
    pub ack_seq: denc::zerocopy::little_endian::U64,
    pub flags: u8,
    pub compat_version: denc::zerocopy::little_endian::U16,
    pub reserved: denc::zerocopy::little_endian::U16,
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
        use denc::zerocopy::little_endian::{U16, U32, U64};
        Self {
            seq: U64::new(0),
            tid: U64::new(0),
            msg_type: U16::new(msg_type),
            priority: U16::new(priority),
            version: U16::new(2), // Protocol V2
            data_pre_padding_len: U32::new(0),
            data_off: U16::new(0),
            ack_seq: U64::new(0),
            flags: 0,
            compat_version: U16::new(1),
            reserved: U16::new(0),
        }
    }

    // Safe accessors for fields
    // These methods get values from the zerocopy LE types

    /// Get sequence number
    #[inline]
    pub fn get_seq(&self) -> u64 {
        self.seq.get()
    }

    /// Get transaction ID
    #[inline]
    pub fn get_tid(&self) -> u64 {
        self.tid.get()
    }

    /// Get message type
    #[inline]
    pub fn get_msg_type(&self) -> u16 {
        self.msg_type.get()
    }

    /// Get priority
    #[inline]
    pub fn get_priority(&self) -> u16 {
        self.priority.get()
    }

    /// Get version
    #[inline]
    pub fn get_version(&self) -> u16 {
        self.version.get()
    }

    /// Get data pre-padding length
    #[inline]
    pub fn get_data_pre_padding_len(&self) -> u32 {
        self.data_pre_padding_len.get()
    }

    /// Get data offset
    #[inline]
    pub fn get_data_off(&self) -> u16 {
        self.data_off.get()
    }

    /// Get acknowledgment sequence
    #[inline]
    pub fn get_ack_seq(&self) -> u64 {
        self.ack_seq.get()
    }

    /// Get flags
    #[inline]
    pub fn get_flags(&self) -> u8 {
        self.flags
    }

    /// Get compatible version
    #[inline]
    pub fn get_compat_version(&self) -> u16 {
        self.compat_version.get()
    }

    /// Get reserved field
    #[inline]
    pub fn get_reserved(&self) -> u16 {
        self.reserved.get()
    }

    // Safe setters for fields
    // These methods set values using the zerocopy LE types

    /// Set sequence number
    #[inline]
    pub fn set_seq(&mut self, value: u64) {
        self.seq = denc::zerocopy::little_endian::U64::new(value);
    }

    /// Set transaction ID
    #[inline]
    pub fn set_tid(&mut self, value: u64) {
        self.tid = denc::zerocopy::little_endian::U64::new(value);
    }

    /// Set acknowledgment sequence
    #[inline]
    pub fn set_ack_seq(&mut self, value: u64) {
        self.ack_seq = denc::zerocopy::little_endian::U64::new(value);
    }

    /// Set flags
    #[inline]
    pub fn set_flags(&mut self, value: u8) {
        self.flags = value;
    }
}
