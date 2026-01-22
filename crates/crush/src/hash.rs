/// Jenkins hash implementation for CRUSH
/// Based on Bob Jenkins' lookup3.c hash function
/// Reference: ~/dev/ceph/src/crush/hash.c
///
/// Mix three 32-bit values reversibly
#[inline]
fn mix(a: &mut u32, b: &mut u32, c: &mut u32) {
    *a = a.wrapping_sub(*c);
    *a ^= c.rotate_left(4);
    *c = c.wrapping_add(*b);

    *b = b.wrapping_sub(*a);
    *b ^= a.rotate_left(6);
    *a = a.wrapping_add(*c);

    *c = c.wrapping_sub(*b);
    *c ^= b.rotate_left(8);
    *b = b.wrapping_add(*a);

    *a = a.wrapping_sub(*c);
    *a ^= c.rotate_left(16);
    *c = c.wrapping_add(*b);

    *b = b.wrapping_sub(*a);
    *b ^= a.rotate_left(19);
    *a = a.wrapping_add(*c);

    *c = c.wrapping_sub(*b);
    *c ^= b.rotate_left(4);
    *b = b.wrapping_add(*a);
}

/// Final mixing of 3 32-bit values (a,b,c) into c
#[inline]
fn final_mix(a: &mut u32, b: &mut u32, c: &mut u32) {
    *c ^= *b;
    *c = c.wrapping_sub(b.rotate_left(14));

    *a ^= *c;
    *a = a.wrapping_sub(c.rotate_left(11));

    *b ^= *a;
    *b = b.wrapping_sub(a.rotate_left(25));

    *c ^= *b;
    *c = c.wrapping_sub(b.rotate_left(16));

    *a ^= *c;
    *a = a.wrapping_sub(c.rotate_left(4));

    *b ^= *a;
    *b = b.wrapping_sub(a.rotate_left(14));

    *c ^= *b;
    *c = c.wrapping_sub(b.rotate_left(24));
}

/// Hash a single 32-bit value
pub fn crush_hash32(x: u32) -> u32 {
    let mut a = 0xdeadbeef_u32.wrapping_add(4).wrapping_add(13);
    let mut b = a;
    let mut c = a;

    a = a.wrapping_add(x);
    final_mix(&mut a, &mut b, &mut c);

    c
}

/// Hash two 32-bit values
pub fn crush_hash32_2(a: u32, b: u32) -> u32 {
    let mut ha = 0xdeadbeef_u32.wrapping_add(8).wrapping_add(13);
    let mut hb = ha;
    let mut hc = ha;

    hb = hb.wrapping_add(b);
    ha = ha.wrapping_add(a);
    final_mix(&mut ha, &mut hb, &mut hc);

    hc
}

/// Hash three 32-bit values (most commonly used)
pub fn crush_hash32_3(a: u32, b: u32, c: u32) -> u32 {
    let mut ha = 0xdeadbeef_u32.wrapping_add(12).wrapping_add(13);
    let mut hb = ha;
    let mut hc = ha;

    hc = hc.wrapping_add(c);
    hb = hb.wrapping_add(b);
    ha = ha.wrapping_add(a);
    final_mix(&mut ha, &mut hb, &mut hc);

    hc
}

/// Hash four 32-bit values
pub fn crush_hash32_4(a: u32, b: u32, c: u32, d: u32) -> u32 {
    let mut ha = 0xdeadbeef_u32.wrapping_add(16).wrapping_add(13);
    let mut hb = ha;
    let mut hc = ha;

    hc = hc.wrapping_add(d);
    hb = hb.wrapping_add(c);
    ha = ha.wrapping_add(b);
    mix(&mut ha, &mut hb, &mut hc);
    ha = ha.wrapping_add(a);
    final_mix(&mut ha, &mut hb, &mut hc);

    hc
}

/// Hash five 32-bit values
pub fn crush_hash32_5(a: u32, b: u32, c: u32, d: u32, e: u32) -> u32 {
    let mut ha = 0xdeadbeef_u32.wrapping_add(20).wrapping_add(13);
    let mut hb = ha;
    let mut hc = ha;

    hc = hc.wrapping_add(e);
    hb = hb.wrapping_add(d);
    ha = ha.wrapping_add(c);
    mix(&mut ha, &mut hb, &mut hc);
    hb = hb.wrapping_add(b);
    ha = ha.wrapping_add(a);
    final_mix(&mut ha, &mut hb, &mut hc);

    hc
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

/// Ceph's rjenkins string hash function
/// This is used for hashing object names to determine their placement
pub fn ceph_str_hash_rjenkins(data: &[u8]) -> u32 {
    let length = data.len();
    let mut k = data;

    // Set up the internal state
    let mut a = 0x9e3779b9_u32; // the golden ratio; an arbitrary value
    let mut b = a;
    let mut c = 0_u32; // variable initialization of internal state

    // Handle most of the key (12 bytes at a time)
    while k.len() >= 12 {
        a = a.wrapping_add(
            u32::from(k[0])
                | (u32::from(k[1]) << 8)
                | (u32::from(k[2]) << 16)
                | (u32::from(k[3]) << 24),
        );
        b = b.wrapping_add(
            u32::from(k[4])
                | (u32::from(k[5]) << 8)
                | (u32::from(k[6]) << 16)
                | (u32::from(k[7]) << 24),
        );
        c = c.wrapping_add(
            u32::from(k[8])
                | (u32::from(k[9]) << 8)
                | (u32::from(k[10]) << 16)
                | (u32::from(k[11]) << 24),
        );
        rjenkins_mix(&mut a, &mut b, &mut c);
        k = &k[12..];
    }

    // Handle the last 11 bytes
    c = c.wrapping_add(length as u32);

    // All the case statements fall through
    let remaining = k.len();
    if remaining >= 11 {
        c = c.wrapping_add(u32::from(k[10]) << 24);
    }
    if remaining >= 10 {
        c = c.wrapping_add(u32::from(k[9]) << 16);
    }
    if remaining >= 9 {
        c = c.wrapping_add(u32::from(k[8]) << 8);
        // the first byte of c is reserved for the length
    }
    if remaining >= 8 {
        b = b.wrapping_add(u32::from(k[7]) << 24);
    }
    if remaining >= 7 {
        b = b.wrapping_add(u32::from(k[6]) << 16);
    }
    if remaining >= 6 {
        b = b.wrapping_add(u32::from(k[5]) << 8);
    }
    if remaining >= 5 {
        b = b.wrapping_add(u32::from(k[4]));
    }
    if remaining >= 4 {
        a = a.wrapping_add(u32::from(k[3]) << 24);
    }
    if remaining >= 3 {
        a = a.wrapping_add(u32::from(k[2]) << 16);
    }
    if remaining >= 2 {
        a = a.wrapping_add(u32::from(k[1]) << 8);
    }
    if remaining >= 1 {
        a = a.wrapping_add(u32::from(k[0]));
    }
    // case 0: nothing left to add

    rjenkins_mix(&mut a, &mut b, &mut c);

    c
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crush_hash32() {
        // Test basic hash function
        let h1 = crush_hash32(123);
        let h2 = crush_hash32(123);
        assert_eq!(h1, h2, "Hash should be deterministic");

        let h3 = crush_hash32(124);
        assert_ne!(h1, h3, "Different inputs should produce different hashes");
    }

    #[test]
    fn test_crush_hash32_2() {
        let h1 = crush_hash32_2(123, 456);
        let h2 = crush_hash32_2(123, 456);
        assert_eq!(h1, h2, "Hash should be deterministic");

        let h3 = crush_hash32_2(456, 123);
        assert_ne!(h1, h3, "Order should matter");
    }

    #[test]
    fn test_crush_hash32_3() {
        let h1 = crush_hash32_3(123, 456, 789);
        let h2 = crush_hash32_3(123, 456, 789);
        assert_eq!(h1, h2, "Hash should be deterministic");
    }

    #[test]
    fn test_ceph_str_hash_rjenkins() {
        // Test basic hash function
        let h1 = ceph_str_hash_rjenkins(b"hello");
        let h2 = ceph_str_hash_rjenkins(b"hello");
        assert_eq!(h1, h2, "Hash should be deterministic");

        let h3 = ceph_str_hash_rjenkins(b"world");
        assert_ne!(h1, h3, "Different inputs should produce different hashes");

        // Test the specific case from our debugging: "example_object"
        // Expected hash from C implementation: 0x4ec401aa (1321468330 in decimal)
        let h_example = ceph_str_hash_rjenkins(b"example_object");
        assert_eq!(
            h_example, 0x4ec401aa,
            "Hash of 'example_object' should match Ceph's rjenkins output"
        );

        // Test "foo" as well
        let h_foo = ceph_str_hash_rjenkins(b"foo");
        assert_eq!(
            h_foo, 0x7fc1f406,
            "Hash of 'foo' should match Ceph's rjenkins output"
        );
    }
}
