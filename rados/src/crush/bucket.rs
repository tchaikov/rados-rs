// Bucket selection algorithms for CRUSH
// Reference: ~/dev/ceph/src/crush/mapper.c

use crate::crush::hash::{crush_hash32_2, crush_hash32_3, crush_hash32_4};
use crate::crush::types::{BucketAlgorithm, BucketData, CrushBucket};
use crate::denc::constants::crush::{FIXED_POINT_MASK, LN_LOOKUP_OFFSET};

/// Select an item from a bucket using the appropriate algorithm
pub fn bucket_choose(bucket: &CrushBucket, x: u32, r: u32) -> i32 {
    match bucket.alg {
        BucketAlgorithm::Straw2 => bucket_straw2_choose(bucket, x, r),
        BucketAlgorithm::Uniform => bucket_uniform_choose(bucket, x, r),
        BucketAlgorithm::List => bucket_list_choose(bucket, x, r),
        BucketAlgorithm::Tree => bucket_tree_choose(bucket, x, r),
        BucketAlgorithm::Straw => bucket_straw_choose(bucket, x, r),
    }
}

/// Compute 2^44*log2(input+1) using lookup tables
/// This is the correct implementation matching Ceph's crush_ln
/// Reference: ~/dev/ceph/src/crush/mapper.c lines 246-288
fn crush_ln(xin: u32) -> u64 {
    use crate::crush::crush_ln_table::{LL_TBL, RH_LH_TBL};

    let mut x = xin;
    x = x.wrapping_add(1);

    // Normalize input
    let mut iexpon = 15i32;

    // Figure out number of bits we need to shift and do it in one step
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

    // RH*x ~ 2^48 * (2^15 + xf), xf<2^8
    let mut xl64 = (x as u64).wrapping_mul(rh);
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
fn generate_exponential_distribution(x: u32, y: i32, z: u32, weight: u32) -> i64 {
    let mut u = crush_hash32_3(x, y as u32, z);
    u &= FIXED_POINT_MASK;

    // Natural log lookup maps [0,0xffff] to [0, 0xffffffffffff]
    // corresponding to real numbers [-11.090355, 0]
    let ln = crush_ln(u) as i64 - LN_LOOKUP_OFFSET;

    // Divide by 16.16 fixed-point weight
    // ln is negative, so larger weight means larger (less negative) draw
    if weight == 0 {
        i64::MIN
    } else {
        ln / weight as i64
    }
}

/// Straw2 bucket selection (modern, optimal algorithm)
/// Each item draws a straw based on exponential distribution
/// Item with longest straw (highest draw value) wins
fn bucket_straw2_choose(bucket: &CrushBucket, x: u32, r: u32) -> i32 {
    let weights = match &bucket.data {
        BucketData::Straw2 { item_weights } => item_weights,
        _ => unreachable!("bucket_straw2_choose called on non-Straw2 bucket"),
    };

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
            generate_exponential_distribution(x, bucket.items[i], r, weight)
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

/// Uniform bucket selection (O(1), simple hash).
///
/// NOTE: This is a simplified implementation that does NOT match the C++
/// `bucket_perm_choose` permutation scheme used for uniform buckets.
/// Uniform buckets are deprecated since Hammer (2015) and no modern Ceph
/// cluster uses them — all clusters use straw2.  Since our minimum Ceph
/// version is Quincy (v17), this code path is effectively dead.  If uniform
/// bucket support is ever needed, this must be replaced with the full
/// stateful Fisher-Yates permutation from `crush/mapper.c`.
fn bucket_uniform_choose(bucket: &CrushBucket, x: u32, r: u32) -> i32 {
    let hash = crush_hash32_2(x, r);
    let index = (hash % bucket.size) as usize;
    bucket.items[index]
}

/// List bucket selection (legacy)
/// Items in a linked list with arbitrary weights
fn bucket_list_choose(bucket: &CrushBucket, x: u32, r: u32) -> i32 {
    let (item_weights, sum_weights) = match &bucket.data {
        BucketData::List {
            item_weights,
            sum_weights,
        } => (item_weights, sum_weights),
        _ => unreachable!("bucket_list_choose called on non-List bucket"),
    };

    // Iterate from end to beginning
    for i in (0..bucket.size as usize).rev() {
        let mut w = crush_hash32_4(x, bucket.items[i] as u32, r, bucket.id as u32) as u64;
        w &= 0xffff;
        w = w.wrapping_mul(sum_weights[i] as u64);
        w >>= 16;

        if w < item_weights[i] as u64 {
            return bucket.items[i];
        }
    }

    // Fallback to first item
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
fn bucket_tree_choose(bucket: &CrushBucket, x: u32, r: u32) -> i32 {
    let (num_nodes, node_weights) = match &bucket.data {
        BucketData::Tree {
            num_nodes,
            node_weights,
        } => (*num_nodes, node_weights),
        _ => unreachable!("bucket_tree_choose called on non-Tree bucket"),
    };

    // Start at root (middle of the 1-indexed array)
    let mut n = (num_nodes >> 1) as i32;

    // Descend until we reach a terminal (leaf) node
    while n & 1 == 0 {
        // height(n) = trailing zeros
        let h = n.trailing_zeros();
        let offset = 1i32 << (h - 1);

        // Pick point in [0, w) using hash scaled by node weight
        let w = node_weights[n as usize] as u64;
        let hash = crush_hash32_4(x, n as u32, r, bucket.id as u32) as u64;
        let t = (hash * w) >> 32;

        // Descend left or right based on weighted comparison
        let l = n - offset;
        if t < node_weights[l as usize] as u64 {
            n = l;
        } else {
            n += offset; // right
        }
    }

    bucket.items[(n >> 1) as usize]
}

/// Straw bucket selection (legacy, deprecated)
/// Each item gets a straw with random length
fn bucket_straw_choose(bucket: &CrushBucket, x: u32, r: u32) -> i32 {
    let straws = match &bucket.data {
        BucketData::Straw { straws, .. } => straws,
        _ => unreachable!("bucket_straw_choose called on non-Straw bucket"),
    };

    let i = (0..bucket.size as usize)
        .max_by_key(|&i| {
            let mut draw = crush_hash32_3(x, bucket.items[i] as u32, r) as u64;
            draw &= 0xffff;
            draw.wrapping_mul(straws[i] as u64)
        })
        .expect("bucket_straw_choose: size > 0 guaranteed by decode validation");
    bucket.items[i]
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
        let item = bucket_straw2_choose(&bucket, 123, 0);
        assert!((0..=2).contains(&item));

        // Same input should give same output (deterministic)
        let item2 = bucket_straw2_choose(&bucket, 123, 0);
        assert_eq!(item, item2);

        // Different input should potentially give different output
        let _item3 = bucket_straw2_choose(&bucket, 456, 0);
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

        let item = bucket_uniform_choose(&bucket, 123, 0);
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
