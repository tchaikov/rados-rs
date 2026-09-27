//! SHA-1, HMAC-SHA1 and HOTP for the otp cluster tests: the workspace has
//! no sha1 crate, so this is FIPS 180-4 and RFC 2104/4226 by hand, pinned
//! to their published vectors below.

pub fn sha1(msg: &[u8]) -> [u8; 20] {
    let mut h: [u32; 5] = [
        0x6745_2301,
        0xefcd_ab89,
        0x98ba_dcfe,
        0x1032_5476,
        0xc3d2_e1f0,
    ];
    let mut data = msg.to_vec();
    data.push(0x80);
    while data.len() % 64 != 56 {
        data.push(0);
    }
    data.extend_from_slice(&((msg.len() as u64) * 8).to_be_bytes());
    for block in data.chunks(64) {
        let mut w = [0u32; 80];
        for (i, word) in block.chunks(4).enumerate() {
            w[i] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let [mut a, mut b, mut c, mut d, mut e] = h;
        for (i, wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..20 => ((b & c) | (!b & d), 0x5a82_7999),
                20..40 => (b ^ c ^ d, 0x6ed9_eba1),
                40..60 => ((b & c) | (b & d) | (c & d), 0x8f1b_bcdc),
                _ => (b ^ c ^ d, 0xca62_c1d6),
            };
            let t = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(*wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = t;
        }
        for (s, v) in h.iter_mut().zip([a, b, c, d, e]) {
            *s = s.wrapping_add(v);
        }
    }
    let mut out = [0u8; 20];
    for (chunk, s) in out.chunks_mut(4).zip(h) {
        chunk.copy_from_slice(&s.to_be_bytes());
    }
    out
}

pub fn hmac_sha1(key: &[u8], msg: &[u8]) -> [u8; 20] {
    let mut block = [0u8; 64];
    if key.len() > 64 {
        block[..20].copy_from_slice(&sha1(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let mut inner: Vec<u8> = block.iter().map(|b| b ^ 0x36).collect();
    inner.extend_from_slice(msg);
    let mut outer: Vec<u8> = block.iter().map(|b| b ^ 0x5c).collect();
    outer.extend_from_slice(&sha1(&inner));
    sha1(&outer)
}

/// RFC 4226 HOTP; TOTP is `hotp(key, (unix - time_ofs) / step, digits)`.
pub fn hotp(key: &[u8], counter: u64, digits: u32) -> String {
    let mac = hmac_sha1(key, &counter.to_be_bytes());
    let off = usize::from(mac[19] & 0x0f);
    let bin =
        u32::from_be_bytes([mac[off], mac[off + 1], mac[off + 2], mac[off + 3]]) & 0x7fff_ffff;
    let code = bin % 10u32.pow(digits);
    format!("{code:0width$}", width = digits as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    #[test]
    fn sha1_one_block() {
        assert_eq!(
            hex(&sha1(b"abc")),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
    }

    #[test]
    fn sha1_two_blocks() {
        assert_eq!(
            hex(&sha1(
                b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
            )),
            "84983e441c3bd26ebaae4aa1f95129e5e54670f1"
        );
    }

    const KEY: &[u8] = b"12345678901234567890";

    #[test]
    fn totp_rfc6238_t59() {
        assert_eq!(hotp(KEY, 59 / 30, 8), "94287082");
        assert_eq!(hotp(KEY, 59 / 30, 6), "287082");
    }

    #[test]
    fn totp_rfc6238_zero_pad() {
        assert_eq!(hotp(KEY, 1_111_111_109 / 30, 8), "07081804");
    }
}
