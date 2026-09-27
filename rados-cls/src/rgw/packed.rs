//! Ceph's `encode_packed_val`/`decode_packed_val` (`cls_rgw_types.h`): a
//! value below 0x80 is one byte; otherwise a tag `0x80 | n` and the value
//! in `n` little-endian bytes. The C++ picks two bytes up to and
//! including 0x10000, so that one value is written as zero; this mirrors
//! it for byte identity.

use bytes::{Buf, BufMut};
use rados::RadosError;

pub(crate) fn encode<B: BufMut>(v: u64, buf: &mut B) {
    if v < 0x80 {
        buf.put_u8(v as u8);
    } else if v < 0x100 {
        buf.put_u8(0x81);
        buf.put_u8(v as u8);
    } else if v <= 0x10000 {
        buf.put_u8(0x82);
        buf.put_u16_le(v as u16);
    } else if v <= 0x100_0000 {
        buf.put_u8(0x84);
        buf.put_u32_le(v as u32);
    } else {
        buf.put_u8(0x88);
        buf.put_u64_le(v);
    }
}

pub(crate) fn encoded_size(v: u64) -> usize {
    if v < 0x80 {
        1
    } else if v < 0x100 {
        2
    } else if v <= 0x10000 {
        3
    } else if v <= 0x100_0000 {
        5
    } else {
        9
    }
}

pub(crate) fn decode<B: Buf>(buf: &mut B) -> Result<u64, RadosError> {
    fn need<B: Buf>(buf: &B, n: usize) -> Result<(), RadosError> {
        if buf.remaining() < n {
            return Err(RadosError::InvalidData("packed value truncated".into()));
        }
        Ok(())
    }
    need(buf, 1)?;
    let tag = buf.get_u8();
    if tag < 0x80 {
        return Ok(u64::from(tag));
    }
    match tag & !0x80 {
        1 => {
            need(buf, 1)?;
            Ok(u64::from(buf.get_u8()))
        }
        2 => {
            need(buf, 2)?;
            Ok(u64::from(buf.get_u16_le()))
        }
        4 => {
            need(buf, 4)?;
            Ok(u64::from(buf.get_u32_le()))
        }
        8 => {
            need(buf, 8)?;
            Ok(buf.get_u64_le())
        }
        n => Err(RadosError::InvalidData(format!("packed value tag {n:#x}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_forms_and_the_u16_quirk() {
        let cases: &[(u64, &[u8])] = &[
            (0, &[0x00]),
            (0x7f, &[0x7f]),
            (0x80, &[0x81, 0x80]),
            (0xff, &[0x81, 0xff]),
            (0x100, &[0x82, 0x00, 0x01]),
            (12_322, &[0x82, 0x22, 0x30]),
            (0xffff, &[0x82, 0xff, 0xff]),
            // encode_packed_val's `<= 0x10000` writes the value as a u16: zero.
            (0x10000, &[0x82, 0x00, 0x00]),
            (0x10001, &[0x84, 0x01, 0x00, 0x01, 0x00]),
            (0x1000000, &[0x84, 0x00, 0x00, 0x00, 0x01]),
            (0x1000001, &[0x88, 0x01, 0x00, 0x00, 0x01, 0, 0, 0, 0]),
            (
                u64::MAX,
                &[0x88, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
            ),
        ];
        for (value, wire) in cases {
            let mut buf = Vec::new();
            encode(*value, &mut buf);
            assert_eq!(&buf, wire, "{value:#x}");
            assert_eq!(encoded_size(*value), wire.len());
            let expect = if *value == 0x10000 { 0 } else { *value };
            assert_eq!(
                decode(&mut &wire[..]).expect("decode"),
                expect,
                "{value:#x}"
            );
        }
        assert!(decode(&mut &[0x80u8][..]).is_err());
        assert!(decode(&mut &[0x83u8, 0, 0, 0][..]).is_err());
        // A non-minimal form still decodes.
        assert_eq!(decode(&mut &[0x81u8, 0x05][..]).expect("decode"), 5);
    }
}
