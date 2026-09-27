/// Robert Jenkins' hash implementation for CRUSH
/// This is the OLD Jenkins hash (not the lookup3 version)
/// Reference: ~/dev/ceph/src/crush/hash.c
///
/// IMPORTANT: This is the rjenkins1 hash from Ceph, which uses the
/// old crush_hashmix macro. It's different from Bob Jenkins' later
/// lookup3.c hash function.
/// Hash seed used by Ceph's CRUSH
const CRUSH_HASH_SEED: u32 = 1315423911;

/// Robert Jenkins' hash mix function
/// Used by both CRUSH hash functions and string hashing.
/// Reference: ~/dev/ceph/src/crush/hash.c (crush_hashmix macro)
/// Reference: ~/dev/ceph/src/common/ceph_hash.cc (rjenkins hash)
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

/// Hash two 32-bit values using rjenkins1
pub fn crush_hash32_2(mut a: u32, mut b: u32) -> u32 {
    let mut hash = CRUSH_HASH_SEED ^ a ^ b;
    let mut x = 231232;
    let mut y = 1232;

    rjenkins_mix(&mut a, &mut b, &mut hash);
    rjenkins_mix(&mut x, &mut a, &mut hash);
    rjenkins_mix(&mut b, &mut y, &mut hash);

    hash
}

/// Hash three 32-bit values using rjenkins1
pub fn crush_hash32_3(mut a: u32, mut b: u32, mut c: u32) -> u32 {
    let mut hash = CRUSH_HASH_SEED ^ a ^ b ^ c;
    let mut x = 231232;
    let mut y = 1232;

    rjenkins_mix(&mut a, &mut b, &mut hash);
    rjenkins_mix(&mut c, &mut x, &mut hash);
    rjenkins_mix(&mut y, &mut a, &mut hash);
    rjenkins_mix(&mut b, &mut x, &mut hash);
    rjenkins_mix(&mut y, &mut c, &mut hash);

    hash
}

/// Hash four 32-bit values using rjenkins1
pub fn crush_hash32_4(mut a: u32, mut b: u32, mut c: u32, mut d: u32) -> u32 {
    let mut hash = CRUSH_HASH_SEED ^ a ^ b ^ c ^ d;
    let mut x = 231232;
    let mut y = 1232;

    rjenkins_mix(&mut a, &mut b, &mut hash);
    rjenkins_mix(&mut c, &mut d, &mut hash);
    rjenkins_mix(&mut a, &mut x, &mut hash);
    rjenkins_mix(&mut y, &mut b, &mut hash);
    rjenkins_mix(&mut c, &mut x, &mut hash);
    rjenkins_mix(&mut y, &mut d, &mut hash);

    hash
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

/// `pg_pool_t::object_hash` value selecting the linux dcache hash
/// (`CEPH_STR_HASH_LINUX`, `src/include/ceph_hash.h`).
pub const CEPH_STR_HASH_LINUX: u8 = 1;

/// `pg_pool_t::object_hash` value selecting the rjenkins hash
/// (`CEPH_STR_HASH_RJENKINS`, `src/include/ceph_hash.h`). A v19 monitor
/// creates every pool with it.
pub const CEPH_STR_HASH_RJENKINS: u8 = 2;

/// Hash a byte string with the linux dcache hash, as Ceph's
/// `ceph_str_hash_linux` (`src/common/ceph_hash.cc`).
pub fn ceph_str_hash_linux(data: &[u8]) -> u32 {
    data.iter().fold(0u32, |hash, &c| {
        let c = u32::from(c);
        hash.wrapping_add(c << 4)
            .wrapping_add(c >> 4)
            .wrapping_mul(11)
    })
}

/// Hash a byte string with the hash a pool's `object_hash` selects, as
/// Ceph's `ceph_str_hash` (`src/common/ceph_hash.cc`).
///
/// Returns `None` for an unknown hash type, where Ceph returns `-1`.
pub fn ceph_str_hash(kind: u8, data: &[u8]) -> Option<u32> {
    match kind {
        CEPH_STR_HASH_LINUX => Some(ceph_str_hash_linux(data)),
        CEPH_STR_HASH_RJENKINS => Some(ceph_str_hash_rjenkins(data)),
        _ => None,
    }
}

/// Hash an object's placement key as Ceph's `pg_pool_t::hash_key`
/// (`src/osd/osd_types.cc`): `key` alone in the default namespace,
/// otherwise the bytes `ns + 0x1f + key`.
pub(crate) fn hash_key(kind: u8, key: &str, ns: &str) -> Option<u32> {
    if ns.is_empty() {
        return ceph_str_hash(kind, key.as_bytes());
    }
    let mut buf = Vec::with_capacity(ns.len() + 1 + key.len());
    buf.extend_from_slice(ns.as_bytes());
    buf.push(0x1f);
    buf.extend_from_slice(key.as_bytes());
    ceph_str_hash(kind, &buf)
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

    // Raw ps values reported by `ceph osd map` on a v19.2.2 cluster.
    #[test]
    fn test_ceph_str_hash_rjenkins() {
        let vectors: [(&[u8], u32); 7] = [
            (b"foo", 0x7fc1f406),
            (b"bar", 0xefe6384b),
            (b"obj", 0xaabc5e21),
            (b"e", 0xef61efce),
            (b"ns1\x1ffoo", 0xf4569544),
            (b"users.uid\x1ftestuser", 0xa13aa4c1),
            (b"gc\x1fgc.0", 0x031bb659),
        ];
        for (data, want) in vectors {
            assert_eq!(ceph_str_hash_rjenkins(data), want, "{data:?}");
        }
    }

    // Transcribed from ceph_hash.cc's ceph_str_hash_linux: a v19 monitor
    // creates no linux-hash pool, so there is no cluster oracle.
    #[test]
    fn test_ceph_str_hash_linux() {
        assert_eq!(ceph_str_hash_linux(b"foo"), 0x0024db2a);
        assert_eq!(ceph_str_hash_linux(b"ns1\x1ffoo"), 0xce6afc4d);
        assert_eq!(ceph_str_hash_linux(b""), 0);
    }

    #[test]
    fn test_ceph_str_hash_dispatch() {
        assert_eq!(ceph_str_hash(1, b"foo"), Some(0x0024db2a));
        assert_eq!(ceph_str_hash(2, b"foo"), Some(0x7fc1f406));
        assert_eq!(ceph_str_hash(0, b"foo"), None);
        assert_eq!(ceph_str_hash(3, b"foo"), None);
    }

    #[test]
    fn test_hash_key() {
        assert_eq!(hash_key(2, "foo", "ns1"), Some(0xf4569544));
        assert_eq!(hash_key(2, "foo", ""), Some(0x7fc1f406));
        assert_eq!(hash_key(0, "foo", "ns1"), None);
    }
}
