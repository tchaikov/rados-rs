use crate::error::Result;
use bytes::{Buf, BufMut};
use std::mem::size_of;

//
// ceph:src/include/msgr.h:ceph_msg_header2
//
#[repr(C, packed)]
#[derive(Debug)]
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

// Manual implementation to avoid issues with packed struct references
impl denc::zerocopy::Encode for MsgHeader {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
    ) -> std::result::Result<(), denc::zerocopy::EncodeError> {
        let size = std::mem::size_of::<Self>();
        if buf.remaining_mut() < size {
            return Err(denc::zerocopy::EncodeError::InsufficientSpace {
                required: size,
                available: buf.remaining_mut(),
            });
        }

        // Zero-copy for little-endian systems
        #[cfg(target_endian = "little")]
        {
            unsafe {
                let src_ptr = self as *const Self as *const u8;
                let src_slice = std::slice::from_raw_parts(src_ptr, size);
                buf.put_slice(src_slice);
            }
        }

        // Field-by-field for big-endian
        #[cfg(not(target_endian = "little"))]
        {
            unsafe {
                use denc::zerocopy::Encode;
                let seq = std::ptr::addr_of!(self.seq).read_unaligned();
                seq.encode(buf)?;
                let tid = std::ptr::addr_of!(self.tid).read_unaligned();
                tid.encode(buf)?;
                let msg_type = std::ptr::addr_of!(self.msg_type).read_unaligned();
                msg_type.encode(buf)?;
                let priority = std::ptr::addr_of!(self.priority).read_unaligned();
                priority.encode(buf)?;
                let version = std::ptr::addr_of!(self.version).read_unaligned();
                version.encode(buf)?;
                let data_pre_padding_len =
                    std::ptr::addr_of!(self.data_pre_padding_len).read_unaligned();
                data_pre_padding_len.encode(buf)?;
                let data_off = std::ptr::addr_of!(self.data_off).read_unaligned();
                data_off.encode(buf)?;
                let ack_seq = std::ptr::addr_of!(self.ack_seq).read_unaligned();
                ack_seq.encode(buf)?;
                let flags = std::ptr::addr_of!(self.flags).read_unaligned();
                flags.encode(buf)?;
                let compat_version = std::ptr::addr_of!(self.compat_version).read_unaligned();
                compat_version.encode(buf)?;
                let reserved = std::ptr::addr_of!(self.reserved).read_unaligned();
                reserved.encode(buf)?;
            }
        }

        Ok(())
    }

    fn encoded_size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl denc::zerocopy::Decode for MsgHeader {
    fn decode<B: Buf>(buf: &mut B) -> std::result::Result<Self, denc::zerocopy::DecodeError> {
        let size = std::mem::size_of::<Self>();
        if buf.remaining() < size {
            return Err(denc::zerocopy::DecodeError::UnexpectedEof {
                expected: size,
                available: buf.remaining(),
            });
        }

        // Zero-copy for little-endian systems
        #[cfg(target_endian = "little")]
        {
            unsafe {
                let mut value = std::mem::MaybeUninit::<Self>::uninit();
                let dst_ptr = value.as_mut_ptr() as *mut u8;
                let dst_slice = std::slice::from_raw_parts_mut(dst_ptr, size);
                buf.copy_to_slice(dst_slice);
                Ok(value.assume_init())
            }
        }

        // Field-by-field for big-endian
        #[cfg(not(target_endian = "little"))]
        {
            use denc::zerocopy::Decode;
            Ok(Self {
                seq: u64::decode(buf)?,
                tid: u64::decode(buf)?,
                msg_type: u16::decode(buf)?,
                priority: u16::decode(buf)?,
                version: u16::decode(buf)?,
                data_pre_padding_len: u32::decode(buf)?,
                data_off: u16::decode(buf)?,
                ack_seq: u64::decode(buf)?,
                flags: u8::decode(buf)?,
                compat_version: u16::decode(buf)?,
                reserved: u16::decode(buf)?,
            })
        }
    }
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
