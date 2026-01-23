/// Robert Jenkins' hash implementation for CRUSH
/// This is the OLD Jenkins hash (not the lookup3 version)
/// Reference: ~/dev/ceph/src/crush/hash.c
///
/// IMPORTANT: This is the rjenkins1 hash from Ceph, which uses the
/// old crush_hashmix macro. It's different from Bob Jenkins' later
/// lookup3.c hash function.
/// Hash seed used by Ceph's CRUSH
const CRUSH_HASH_SEED: u32 = 1315423911;

/// Old Jenkins hash mix function
/// This is the crush_hashmix macro from ~/dev/ceph/src/crush/hash.c
#[inline]
fn crush_hashmix(a: &mut u32, b: &mut u32, c: &mut u32) {
    *a = a.wrapping_sub(*b);
    *a = a.wrapping_sub(*c);
    *a ^= *c >> 13;

    *b = b.wrapping_sub(*c);
    *b = b.wrapping_sub(*a);
    *b ^= *a << 8;

    *c = c.wrapping_sub(*a);
    *c = c.wrapping_sub(*b);
    *c ^= *b >> 13;

    *a = a.wrapping_sub(*b);
    *a = a.wrapping_sub(*c);
    *a ^= *c >> 12;

    *b = b.wrapping_sub(*c);
    *b = b.wrapping_sub(*a);
    *b ^= *a << 16;

    *c = c.wrapping_sub(*a);
    *c = c.wrapping_sub(*b);
    *c ^= *b >> 5;

    *a = a.wrapping_sub(*b);
    *a = a.wrapping_sub(*c);
    *a ^= *c >> 3;

    *b = b.wrapping_sub(*c);
    *b = b.wrapping_sub(*a);
    *b ^= *a << 10;

    *c = c.wrapping_sub(*a);
    *c = c.wrapping_sub(*b);
    *c ^= *b >> 15;
}

/// Hash a single 32-bit value using rjenkins1
pub fn crush_hash32(mut a: u32) -> u32 {
    let mut hash = CRUSH_HASH_SEED ^ a;
    let mut b = a;
    let mut x = 231232;
    let mut y = 1232;

    crush_hashmix(&mut b, &mut x, &mut hash);
    crush_hashmix(&mut y, &mut a, &mut hash);

    hash
}

/// Hash two 32-bit values using rjenkins1
pub fn crush_hash32_2(mut a: u32, mut b: u32) -> u32 {
    let mut hash = CRUSH_HASH_SEED ^ a ^ b;
    let mut x = 231232;
    let mut y = 1232;

    crush_hashmix(&mut a, &mut b, &mut hash);
    crush_hashmix(&mut x, &mut a, &mut hash);
    crush_hashmix(&mut b, &mut y, &mut hash);

    hash
}

/// Hash three 32-bit values using rjenkins1
pub fn crush_hash32_3(mut a: u32, mut b: u32, mut c: u32) -> u32 {
    let mut hash = CRUSH_HASH_SEED ^ a ^ b ^ c;
    let mut x = 231232;
    let mut y = 1232;

    crush_hashmix(&mut a, &mut b, &mut hash);
    crush_hashmix(&mut c, &mut x, &mut hash);
    crush_hashmix(&mut y, &mut a, &mut hash);
    crush_hashmix(&mut b, &mut x, &mut hash);
    crush_hashmix(&mut y, &mut c, &mut hash);

    hash
}

/// Hash four 32-bit values using rjenkins1
pub fn crush_hash32_4(mut a: u32, mut b: u32, mut c: u32, mut d: u32) -> u32 {
    let mut hash = CRUSH_HASH_SEED ^ a ^ b ^ c ^ d;
    let mut x = 231232;
    let mut y = 1232;

    crush_hashmix(&mut a, &mut b, &mut hash);
    crush_hashmix(&mut c, &mut d, &mut hash);
    crush_hashmix(&mut a, &mut x, &mut hash);
    crush_hashmix(&mut y, &mut b, &mut hash);
    crush_hashmix(&mut c, &mut x, &mut hash);
    crush_hashmix(&mut y, &mut d, &mut hash);

    hash
}

/// Hash five 32-bit values using rjenkins1
pub fn crush_hash32_5(mut a: u32, mut b: u32, mut c: u32, mut d: u32, mut e: u32) -> u32 {
    let mut hash = CRUSH_HASH_SEED ^ a ^ b ^ c ^ d ^ e;
    let mut x = 231232;
    let mut y = 1232;

    crush_hashmix(&mut a, &mut b, &mut hash);
    crush_hashmix(&mut c, &mut d, &mut hash);
    crush_hashmix(&mut e, &mut x, &mut hash);
    crush_hashmix(&mut y, &mut a, &mut hash);
    crush_hashmix(&mut b, &mut x, &mut hash);
    crush_hashmix(&mut y, &mut c, &mut hash);
    crush_hashmix(&mut d, &mut x, &mut hash);
    crush_hashmix(&mut y, &mut e, &mut hash);

    hash
}

/// Robert Jenkins' hash function for strings
/// This is the rjenkins hash used by Ceph for object name hashing
/// Reference: http://burtleburtle.net/bob/hash/evahash.html
/// C implementation: ~/dev/ceph/src/common/ceph_hash.cc
#[inline]
fn rjenkins_mix(a: &mut u32, b: &mut u32, c: &mut u32) {
    *a = a.wrapping_sub(*b);
    *a = a.wrapping_sub(*c);
    *a ^= *c >> 13;

    *b = b.wrapping_sub(*c);
    *b = b.wrapping_sub(*a);
    *b ^= *a << 8;

    *c = c.wrapping_sub(*a);
    *c = c.wrapping_sub(*b);
    *c ^= *b >> 13;

    *a = a.wrapping_sub(*b);
    *a = a.wrapping_sub(*c);
    *a ^= *c >> 12;

    *b = b.wrapping_sub(*c);
    *b = b.wrapping_sub(*a);
    *b ^= *a << 16;

    *c = c.wrapping_sub(*a);
    *c = c.wrapping_sub(*b);
    *c ^= *b >> 5;

    *a = a.wrapping_sub(*b);
    *a = a.wrapping_sub(*c);
    *a ^= *c >> 3;

    *b = b.wrapping_sub(*c);
    *b = b.wrapping_sub(*a);
    *b ^= *a << 10;

    *c = c.wrapping_sub(*a);
    *c = c.wrapping_sub(*b);
    *c ^= *b >> 15;
}

/// Hash a byte string using rjenkins
pub fn ceph_str_hash_rjenkins(data: &[u8]) -> u32 {
    let mut a: u32 = 0x9e3779b9; // the golden ratio
    let mut b: u32 = a;
    let mut c: u32 = 0;

    let mut i = 0;
    let len = data.len();

    // Handle most of the key (12 bytes at a time)
    while i + 12 <= len {
        a = a.wrapping_add(
            data[i] as u32
                | ((data[i + 1] as u32) << 8)
                | ((data[i + 2] as u32) << 16)
                | ((data[i + 3] as u32) << 24),
        );
        b = b.wrapping_add(
            data[i + 4] as u32
                | ((data[i + 5] as u32) << 8)
                | ((data[i + 6] as u32) << 16)
                | ((data[i + 7] as u32) << 24),
        );
        c = c.wrapping_add(
            data[i + 8] as u32
                | ((data[i + 9] as u32) << 8)
                | ((data[i + 10] as u32) << 16)
                | ((data[i + 11] as u32) << 24),
        );
        rjenkins_mix(&mut a, &mut b, &mut c);
        i += 12;
    }

    // Handle the last 11 bytes
    c = c.wrapping_add(len as u32);

    let remaining = len - i;
    match remaining {
        11 => {
            c = c.wrapping_add((data[i + 10] as u32) << 24);
            c = c.wrapping_add((data[i + 9] as u32) << 16);
            c = c.wrapping_add((data[i + 8] as u32) << 8);
            b = b.wrapping_add((data[i + 7] as u32) << 24);
            b = b.wrapping_add((data[i + 6] as u32) << 16);
            b = b.wrapping_add((data[i + 5] as u32) << 8);
            b = b.wrapping_add(data[i + 4] as u32);
            a = a.wrapping_add((data[i + 3] as u32) << 24);
            a = a.wrapping_add((data[i + 2] as u32) << 16);
            a = a.wrapping_add((data[i + 1] as u32) << 8);
            a = a.wrapping_add(data[i] as u32);
        }
        10 => {
            c = c.wrapping_add((data[i + 9] as u32) << 16);
            c = c.wrapping_add((data[i + 8] as u32) << 8);
            b = b.wrapping_add((data[i + 7] as u32) << 24);
            b = b.wrapping_add((data[i + 6] as u32) << 16);
            b = b.wrapping_add((data[i + 5] as u32) << 8);
            b = b.wrapping_add(data[i + 4] as u32);
            a = a.wrapping_add((data[i + 3] as u32) << 24);
            a = a.wrapping_add((data[i + 2] as u32) << 16);
            a = a.wrapping_add((data[i + 1] as u32) << 8);
            a = a.wrapping_add(data[i] as u32);
        }
        9 => {
            c = c.wrapping_add((data[i + 8] as u32) << 8);
            b = b.wrapping_add((data[i + 7] as u32) << 24);
            b = b.wrapping_add((data[i + 6] as u32) << 16);
            b = b.wrapping_add((data[i + 5] as u32) << 8);
            b = b.wrapping_add(data[i + 4] as u32);
            a = a.wrapping_add((data[i + 3] as u32) << 24);
            a = a.wrapping_add((data[i + 2] as u32) << 16);
            a = a.wrapping_add((data[i + 1] as u32) << 8);
            a = a.wrapping_add(data[i] as u32);
        }
        8 => {
            b = b.wrapping_add((data[i + 7] as u32) << 24);
            b = b.wrapping_add((data[i + 6] as u32) << 16);
            b = b.wrapping_add((data[i + 5] as u32) << 8);
            b = b.wrapping_add(data[i + 4] as u32);
            a = a.wrapping_add((data[i + 3] as u32) << 24);
            a = a.wrapping_add((data[i + 2] as u32) << 16);
            a = a.wrapping_add((data[i + 1] as u32) << 8);
            a = a.wrapping_add(data[i] as u32);
        }
        7 => {
            b = b.wrapping_add((data[i + 6] as u32) << 16);
            b = b.wrapping_add((data[i + 5] as u32) << 8);
            b = b.wrapping_add(data[i + 4] as u32);
            a = a.wrapping_add((data[i + 3] as u32) << 24);
            a = a.wrapping_add((data[i + 2] as u32) << 16);
            a = a.wrapping_add((data[i + 1] as u32) << 8);
            a = a.wrapping_add(data[i] as u32);
        }
        6 => {
            b = b.wrapping_add((data[i + 5] as u32) << 8);
            b = b.wrapping_add(data[i + 4] as u32);
            a = a.wrapping_add((data[i + 3] as u32) << 24);
            a = a.wrapping_add((data[i + 2] as u32) << 16);
            a = a.wrapping_add((data[i + 1] as u32) << 8);
            a = a.wrapping_add(data[i] as u32);
        }
        5 => {
            b = b.wrapping_add(data[i + 4] as u32);
            a = a.wrapping_add((data[i + 3] as u32) << 24);
            a = a.wrapping_add((data[i + 2] as u32) << 16);
            a = a.wrapping_add((data[i + 1] as u32) << 8);
            a = a.wrapping_add(data[i] as u32);
        }
        4 => {
            a = a.wrapping_add((data[i + 3] as u32) << 24);
            a = a.wrapping_add((data[i + 2] as u32) << 16);
            a = a.wrapping_add((data[i + 1] as u32) << 8);
            a = a.wrapping_add(data[i] as u32);
        }
        3 => {
            a = a.wrapping_add((data[i + 2] as u32) << 16);
            a = a.wrapping_add((data[i + 1] as u32) << 8);
            a = a.wrapping_add(data[i] as u32);
        }
        2 => {
            a = a.wrapping_add((data[i + 1] as u32) << 8);
            a = a.wrapping_add(data[i] as u32);
        }
        1 => {
            a = a.wrapping_add(data[i] as u32);
        }
        _ => {}
    }

    rjenkins_mix(&mut a, &mut b, &mut c);

    c
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crush_hash32_2() {
        // Test that matches Ceph's implementation
        // PG 2.a: seed=10, pool=2
        let hash = crush_hash32_2(10, 2);
        assert_eq!(
            hash, 1838530675,
            "Hash should match Ceph's rjenkins1 implementation"
        );
    }

    #[test]
    fn test_ceph_str_hash_rjenkins() {
        // Test with a simple string
        let hash = ceph_str_hash_rjenkins(b"hello");
        // Just verify it produces a reasonable value
        assert_ne!(hash, 0);

        // Same input should give same output
        let hash2 = ceph_str_hash_rjenkins(b"hello");
        assert_eq!(hash, hash2);

        // Different input should give different output (with high probability)
        let hash3 = ceph_str_hash_rjenkins(b"world");
        assert_ne!(hash, hash3);
    }
}
