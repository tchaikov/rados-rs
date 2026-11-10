// Bucket selection algorithms for CRUSH
// NOTE: Ceph bucket helpers take signed int x/r; hash inputs are u32.
// Casts at those boundaries preserve all 32 bits, including negative IDs.
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c#L54-L390
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/hash.h#L16-L20

use crate::crush::hash::{crush_hash32_3, crush_hash32_4};
use crate::crush::mapper::CRUSH_ITEM_NONE;
use crate::crush::types::{BucketAlgorithm, BucketData, CrushBucket, CrushChooseArg};
use crate::denc::constants::crush::{FIXED_POINT_MASK, LN_LOOKUP_OFFSET};

/// Select an item from a bucket using the appropriate algorithm
pub fn bucket_choose(bucket: &CrushBucket, x: u32, r: u32) -> i32 {
    bucket_choose_with_arg(bucket, x as i32, r as i32, None, 0)
}

/// Select an item, applying the optional Ceph choose argument for STRAW2.
pub(crate) fn bucket_choose_with_arg(
    bucket: &CrushBucket,
    x: i32,
    r: i32,
    arg: Option<&CrushChooseArg>,
    position: usize,
) -> i32 {
    if bucket.size == 0 {
        return CRUSH_ITEM_NONE;
    }
    match bucket.alg {
        BucketAlgorithm::Straw2 => bucket_straw2_choose(bucket, x, r, arg, position),
        BucketAlgorithm::Uniform => bucket_perm_choose(bucket, x, r),
        BucketAlgorithm::List => bucket_list_choose(bucket, x, r),
        BucketAlgorithm::Tree => bucket_tree_choose(bucket, x, r),
        BucketAlgorithm::Straw => bucket_straw_choose(bucket, x, r),
    }
}

/// Compute 2^44*log2(input+1) using lookup tables
/// This is the correct implementation matching Ceph's crush_ln
/// Input is the low 16 bits of the hash, as in Ceph's STRAW2 draw.
/// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c#L229-L274
fn crush_ln(xin: u32) -> u64 {
    use crate::crush::crush_ln_table::{LL_TBL, RH_LH_TBL};

    let mut x = xin;
    x = x.wrapping_add(1);

    let mut iexpon = 15i32;

    if (x & 0x18000) == 0 {
        let bits = (x & 0x1FFFF).leading_zeros() as i32 - 16;
        x <<= bits;
        iexpon = 15 - bits;
    }

    let index1 = ((x >> 8) << 1) as usize;

    // RH ~ 2^56/index1 (from RH_LH_tbl)
    let rh = RH_LH_TBL[index1 - 256] as u64;
    // LH ~ 2^48 * log2(index1/256)
    let lh = RH_LH_TBL[index1 + 1 - 256] as u64;

    // NOTE: C promotes (__s64)x to u64 for multiplication by unsigned RH.
    // https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c#L256-L258
    let mut xl64 = u64::from(x).wrapping_mul(rh);
    xl64 >>= 48;

    let mut result = iexpon as u64;
    result <<= 12 + 32;

    let index2 = (xl64 & 0xff) as usize;
    // LL ~ 2^48*log2(1.0+index2/2^15)
    let ll = LL_TBL[index2] as u64;

    let lh = lh.wrapping_add(ll);
    let lh = lh >> (48 - 12 - 32);
    result = result.wrapping_add(lh);

    result
}

/// Generate exponential distribution for Straw2
/// Uses inversion method: -ln(U) / lambda where U is uniform random
fn generate_exponential_distribution(x: i32, y: i32, z: i32, weight: i32) -> i64 {
    // NOTE: Ceph's signed x/y/z are passed to unsigned hash inputs unchanged in bits.
    // https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/hash.h#L18
    let mut u = crush_hash32_3(x as u32, y as u32, z as u32);
    u &= FIXED_POINT_MASK;

    // NOTE: C subtracts in u64 and assigns to s64; the lookup is at most 2^48,
    // so signed subtraction here preserves the resulting negative value.
    // https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c#L331
    let ln = crush_ln(u) as i64 - LN_LOOKUP_OFFSET;

    // NOTE: Ceph stores weights as u32 but takes an int here; the caller
    // preserves its sign conversion. Unsigned division would change placement.
    // https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c#L315-L353
    // Widening i32 to i64 is lossless. With ln in [-2^48, 0] and zero handled below,
    // neither division by zero nor i64::MIN / -1 is possible.
    if weight == 0 {
        i64::MIN
    } else {
        ln / i64::from(weight)
    }
}

/// Straw2 bucket selection (modern, optimal algorithm)
/// Each item draws a straw based on exponential distribution
/// Item with longest straw (highest draw value) wins
fn bucket_straw2_choose(
    bucket: &CrushBucket,
    x: i32,
    r: i32,
    arg: Option<&CrushChooseArg>,
    position: usize,
) -> i32 {
    let base_weights = match &bucket.data {
        BucketData::Straw2 { item_weights } => item_weights,
        _ => unreachable!("bucket_straw2_choose called on non-Straw2 bucket"),
    };
    let weights = arg
        .and_then(|arg| {
            arg.weight_set
                .get(position.min(arg.weight_set.len().saturating_sub(1)))
        })
        .filter(|weights| weights.len() == bucket.items.len())
        .unwrap_or(base_weights);
    let ids = arg
        .filter(|arg| arg.ids.len() == bucket.items.len())
        .map_or(&bucket.items, |arg| &arg.ids);

    tracing::trace!(
        "bucket_straw2_choose: bucket_id={}, x={}, r={}, size={}",
        bucket.id,
        x,
        r,
        bucket.size
    );

    let mut high = 0usize;
    let mut high_draw = i64::MIN;

    for (i, &weight) in weights.iter().enumerate().take(bucket.size as usize) {
        let draw = if weight > 0 {
            generate_exponential_distribution(x, ids[i], r, weight as i32)
        } else {
            i64::MIN
        };

        tracing::trace!(
            "  item[{}]: id={}, weight=0x{:x}, draw={}{}",
            i,
            bucket.items[i],
            weight,
            draw,
            if i == 0 || draw > high_draw {
                " <- NEW HIGH"
            } else {
                ""
            }
        );

        if i == 0 || draw > high_draw {
            high = i;
            high_draw = draw;
        }
    }

    tracing::trace!(
        "bucket_straw2_choose: SELECTED index={}, item_id={}",
        high,
        bucket.items[high]
    );

    bucket.items[high]
}

/// Ceph's permutation selection for uniform buckets and FIRSTN fallback.
/// Reference: src/crush/mapper.c::bucket_perm_choose, v17.2.7 and v20.2.4.
pub(crate) fn bucket_perm_choose(bucket: &CrushBucket, x: i32, r: i32) -> i32 {
    if bucket.size == 0 {
        return CRUSH_ITEM_NONE;
    }
    // NOTE: C promotes signed r to unsigned because bucket->size is u32.
    // https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c#L58
    let position = r as u32 % bucket.size;
    if position == 0 {
        let index = crush_hash32_3(x as u32, bucket.id as u32, 0) % bucket.size;
        return bucket.items[index as usize];
    }

    // Rebuild in O(bucket size); a per-mapping workspace can cache this if needed.
    let mut permutation: Vec<_> = (0..bucket.size).collect();
    for p in 0..=position {
        if p < bucket.size - 1 {
            let offset = crush_hash32_3(x as u32, bucket.id as u32, p) % (bucket.size - p);
            permutation.swap(p as usize, (p + offset) as usize);
        }
    }
    bucket.items[permutation[position as usize] as usize]
}

/// List bucket selection (legacy)
/// Items in a linked list with arbitrary weights
fn bucket_list_choose(bucket: &CrushBucket, x: i32, r: i32) -> i32 {
    let (item_weights, sum_weights) = match &bucket.data {
        BucketData::List {
            item_weights,
            sum_weights,
        } => (item_weights, sum_weights),
        _ => unreachable!("bucket_list_choose called on non-List bucket"),
    };

    for i in (0..bucket.size as usize).rev() {
        let mut w = u64::from(crush_hash32_4(
            x as u32,
            bucket.items[i] as u32,
            r as u32,
            bucket.id as u32,
        ));
        w &= 0xffff;
        w = w.wrapping_mul(u64::from(sum_weights[i]));
        w >>= 16;

        if w < u64::from(item_weights[i]) {
            return bucket.items[i];
        }
    }

    bucket.items[0]
}

/// Tree bucket selection (legacy, O(log n))
///
/// Mirrors C++ `bucket_tree_choose` in `crush/mapper.c`.
/// The tree uses a 1-indexed implicit binary tree where:
/// - `height(n)` = number of trailing zero bits in `n`
/// - `terminal(n)` = `n` is odd (leaf)
/// - `left(n)` = `n - (1 << (height(n) - 1))`
/// - `right(n)` = `n + (1 << (height(n) - 1))`
fn bucket_tree_choose(bucket: &CrushBucket, x: i32, r: i32) -> i32 {
    let (num_nodes, node_weights) = match &bucket.data {
        BucketData::Tree {
            num_nodes,
            node_weights,
        } => (*num_nodes, node_weights),
        _ => unreachable!("bucket_tree_choose called on non-Tree bucket"),
    };

    if num_nodes < 2 || bucket.items.is_empty() {
        return CRUSH_ITEM_NONE;
    }

    let mut n = (num_nodes >> 1) as i32;

    while n & 1 == 0 && (n as usize) < node_weights.len() {
        let h = n.trailing_zeros();
        if h == 0 {
            break;
        }
        let offset = 1i32 << (h - 1);

        let w = node_weights[n as usize];
        let hash = crush_hash32_4(x as u32, n as u32, r as u32, bucket.id as u32);
        let t = (u64::from(hash) * u64::from(w)) >> 32;

        let l = n - offset;
        if l < 0 || l as usize >= node_weights.len() {
            break;
        }
        if t < u64::from(node_weights[l as usize]) {
            n = l;
        } else {
            n += offset; // right
        }
    }

    let item_idx = (n >> 1) as usize;
    if item_idx < bucket.items.len() {
        bucket.items[item_idx]
    } else {
        CRUSH_ITEM_NONE
    }
}

/// Straw bucket selection (legacy, deprecated)
/// Each item gets a straw with random length
fn bucket_straw_choose(bucket: &CrushBucket, x: i32, r: i32) -> i32 {
    let straws = match &bucket.data {
        BucketData::Straw { straws, .. } => straws,
        _ => unreachable!("bucket_straw_choose called on non-Straw bucket"),
    };

    // Ceph keeps the first item on equal draws; max_by_key keeps the last.
    match (0..bucket.size as usize).rev().max_by_key(|&i| {
        let mut draw = u64::from(crush_hash32_3(x as u32, bucket.items[i] as u32, r as u32));
        draw &= 0xffff;
        draw.wrapping_mul(u64::from(straws[i]))
    }) {
        Some(idx) => bucket.items[idx],
        None => CRUSH_ITEM_NONE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crush::types::{BucketAlgorithm, BucketData, CrushBucket};

    #[test]
    fn test_straw2_choose() {
        let bucket = CrushBucket {
            id: -1,
            bucket_type: 1,
            alg: BucketAlgorithm::Straw2,
            hash: 0,         // CRUSH_HASH_RJENKINS1
            weight: 0x20000, // 2.0 in 16.16 fixed point
            size: 3,
            items: vec![0, 1, 2],
            data: BucketData::Straw2 {
                item_weights: vec![0x10000, 0x10000, 0x10000], // Equal weights
            },
        };

        // Should return a valid item
        let item = bucket_straw2_choose(&bucket, 123, 0, None, 0);
        assert!((0..=2).contains(&item));

        // Same input should give same output (deterministic)
        let item2 = bucket_straw2_choose(&bucket, 123, 0, None, 0);
        assert_eq!(item, item2);

        // Different input should potentially give different output
        let _item3 = bucket_straw2_choose(&bucket, 456, 0, None, 0);
    }

    #[test]
    fn straw2_sign_extends_high_bit_weights() {
        assert_eq!(generate_exponential_distribution(24, -2, 0, 0), i64::MIN);
        // Signed draw values from the pinned Tentacle C implementation.
        for (weight, expected) in [
            (1, -15855628602878),
            (i32::MAX, -7383),
            (i32::MIN, 7383),
            (-1, 15855628602878),
        ] {
            assert_eq!(
                generate_exponential_distribution(i32::MIN, -2, -1, weight),
                expected
            );
        }

        let mut bucket = CrushBucket {
            id: -1,
            bucket_type: 2,
            alg: BucketAlgorithm::Straw2,
            hash: 0,
            weight: 0xe0000000,
            size: 2,
            items: vec![-2, -3],
            data: BucketData::Straw2 {
                item_weights: vec![0xc0000000, 0x20000000],
            },
        };

        // Quincy and Tentacle both select -2 for this review reproducer.
        assert_eq!(bucket_choose(&bucket, 24, 0), -2);

        bucket.data = BucketData::Straw2 {
            item_weights: vec![0, 0x20000000],
        };
        assert_eq!(bucket_choose(&bucket, 24, 0), -3);
        for weight in [0x80000000, 0xc0000000, 0xffffffff] {
            let arg = CrushChooseArg {
                weight_set: vec![vec![weight, 0x20000000]],
                ids: vec![],
            };
            assert_eq!(bucket_choose_with_arg(&bucket, 24, 0, Some(&arg), 0), -2);
        }
    }

    #[test]
    fn bucket_helpers_match_ceph_for_high_bit_inputs() {
        let weights = vec![0, 0x7fffffff, 0x80000000];
        let algorithms = [
            (
                BucketAlgorithm::Uniform,
                BucketData::Uniform {
                    item_weight: u32::MAX,
                },
            ),
            (
                BucketAlgorithm::List,
                BucketData::List {
                    item_weights: weights.clone(),
                    sum_weights: vec![0, 0x7fffffff, u32::MAX],
                },
            ),
            (
                BucketAlgorithm::Tree,
                BucketData::Tree {
                    num_nodes: 8,
                    node_weights: vec![
                        0,
                        0,
                        0x7fffffff,
                        0x7fffffff,
                        u32::MAX,
                        0x80000000,
                        0x80000000,
                        0,
                    ],
                },
            ),
            (
                BucketAlgorithm::Straw,
                BucketData::Straw {
                    item_weights: weights.clone(),
                    straws: weights,
                },
            ),
            (
                BucketAlgorithm::Straw2,
                BucketData::Straw2 {
                    item_weights: vec![0x10000, 0x20000, 0x30000],
                },
            ),
        ];
        // Oracle: direct calls to the five pinned C bucket helpers, in the order above.
        // https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c#L54-L365
        let cases = [
            (0, 0, [-4, -4, -4, -4, -4]),
            (0x7fffffff, 0x7fffffff, [-4, -3, -3, -3, -3]),
            (0x80000000, 0, [-3, -3, -4, -4, -4]),
            (0x80000000, 0x80000000, [-2, -4, -4, -4, -4]),
            (0xffffffff, 1, [-2, -4, -4, -3, -4]),
            (0xffffffff, 0xffffffff, [-4, -4, -3, -4, -4]),
            (0xffffffff, 0xfffffffe, [-3, -3, -4, -4, -4]),
            (1, 0x80000000, [-3, -3, -3, -4, -4]),
        ];
        for (index, (alg, data)) in algorithms.into_iter().enumerate() {
            let bucket = CrushBucket {
                id: -1,
                bucket_type: 2,
                alg,
                hash: 0,
                weight: u32::MAX,
                size: 3,
                items: vec![-2, -3, -4],
                data,
            };
            for (x, r, expected) in cases {
                assert_eq!(
                    bucket_choose(&bucket, x, r),
                    expected[index],
                    "{alg:?}, x={x:#x}, r={r:#x}"
                );
            }
        }
    }

    #[test]
    fn crush_ln_matches_ceph_for_all_hash_inputs() {
        // The same fold over every 16-bit input of the pinned C crush_ln.
        // https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c#L229-L274
        let mut digest = 0xcbf29ce484222325u64;
        for u in 0..=FIXED_POINT_MASK {
            let ln = crush_ln(u);
            assert!(ln <= LN_LOOKUP_OFFSET as u64);
            digest = (digest ^ ln).wrapping_mul(0x100000001b3);
        }
        assert_eq!(digest, 0x655ae82589f114e5);
    }

    #[test]
    fn test_uniform_choose() {
        let bucket = CrushBucket {
            id: -1,
            bucket_type: 1,
            alg: BucketAlgorithm::Uniform,
            hash: 0, // CRUSH_HASH_RJENKINS1
            weight: 0x30000,
            size: 3,
            items: vec![0, 1, 2],
            data: BucketData::Uniform {
                item_weight: 0x10000,
            },
        };

        let item = bucket_choose(&bucket, 123, 0);
        assert!((0..=2).contains(&item));
    }

    #[test]
    fn test_bucket_choose() {
        let bucket = CrushBucket {
            id: -1,
            bucket_type: 1,
            alg: BucketAlgorithm::Straw2,
            hash: 0, // CRUSH_HASH_RJENKINS1
            weight: 0x20000,
            size: 2,
            items: vec![0, 1],
            data: BucketData::Straw2 {
                item_weights: vec![0x10000, 0x10000],
            },
        };

        let result = bucket_choose(&bucket, 123, 0);
        assert!(result == 0 || result == 1);
    }

    #[test]
    fn test_crush_ln() {
        // Test that crush_ln produces reasonable values
        let ln1 = crush_ln(0x8000);
        let ln2 = crush_ln(0xFFFF);

        // ln should be monotonically increasing
        assert!(ln2 > ln1);
    }
}
