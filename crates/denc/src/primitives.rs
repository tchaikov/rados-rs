use crate::traits::{Denc, FixedSize};
use bytes::{Buf, BufMut};
use std::io;

// Macro to implement Denc for primitive integer types
macro_rules! impl_denc_for_int {
    ($t:ty, $size:expr, $put:ident, $get:ident) => {
        impl Denc for $t {
            fn encode<B: BufMut>(&self, buf: &mut B) -> io::Result<()> {
                buf.$put(*self);
                Ok(())
            }

            fn decode<B: Buf>(buf: &mut B) -> io::Result<Self> {
                if buf.remaining() < Self::SIZE {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "buffer too short: expected {} bytes, got {}",
                            Self::SIZE,
                            buf.remaining()
                        ),
                    ));
                }
                Ok(buf.$get())
            }

            fn encoded_size(&self) -> Option<usize> {
                Some(Self::SIZE)
            }
        }

        impl FixedSize for $t {
            const SIZE: usize = $size;
        }
    };
}

// Implement for all integer types (little-endian as per Ceph)
impl_denc_for_int!(u8, 1, put_u8, get_u8);
impl_denc_for_int!(u16, 2, put_u16_le, get_u16_le);
impl_denc_for_int!(u32, 4, put_u32_le, get_u32_le);
impl_denc_for_int!(u64, 8, put_u64_le, get_u64_le);
impl_denc_for_int!(i8, 1, put_i8, get_i8);
impl_denc_for_int!(i16, 2, put_i16_le, get_i16_le);
impl_denc_for_int!(i32, 4, put_i32_le, get_i32_le);
impl_denc_for_int!(i64, 8, put_i64_le, get_i64_le);

// Implement for bool (encoded as u8)
impl Denc for bool {
    fn encode<B: BufMut>(&self, buf: &mut B) -> io::Result<()> {
        buf.put_u8(if *self { 1 } else { 0 });
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B) -> io::Result<Self> {
        if buf.remaining() < 1 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "buffer too short for bool",
            ));
        }
        Ok(buf.get_u8() != 0)
    }

    fn encoded_size(&self) -> Option<usize> {
        Some(1)
    }
}

impl FixedSize for bool {
    const SIZE: usize = 1;
}

// Implement for f32 (IEEE 754 single precision, little-endian)
impl Denc for f32 {
    fn encode<B: BufMut>(&self, buf: &mut B) -> io::Result<()> {
        buf.put_f32_le(*self);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B) -> io::Result<Self> {
        if buf.remaining() < Self::SIZE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "buffer too short for f32",
            ));
        }
        Ok(buf.get_f32_le())
    }

    fn encoded_size(&self) -> Option<usize> {
        Some(Self::SIZE)
    }
}

impl FixedSize for f32 {
    const SIZE: usize = 4;
}

// Implement for f64 (IEEE 754 double precision, little-endian)
impl Denc for f64 {
    fn encode<B: BufMut>(&self, buf: &mut B) -> io::Result<()> {
        buf.put_f64_le(*self);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B) -> io::Result<Self> {
        if buf.remaining() < Self::SIZE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "buffer too short for f64",
            ));
        }
        Ok(buf.get_f64_le())
    }

    fn encoded_size(&self) -> Option<usize> {
        Some(Self::SIZE)
    }
}

impl FixedSize for f64 {
    const SIZE: usize = 8;
}
