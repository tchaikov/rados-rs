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
}
