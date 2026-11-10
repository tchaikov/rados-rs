//! Assertion-bearing ports of Ceph's legacy STRAW and STRAW2 weight tests.

use rados::crush::{
    BucketAlgorithm, BucketData, CrushBucket, CrushMap, CrushRule, CrushRuleStep, RuleOp, RuleType,
    mapper::crush_do_rule,
};

const W: u32 = 0x1_0000;

fn step(op: RuleOp, arg1: i32, arg2: i32) -> CrushRuleStep {
    CrushRuleStep { op, arg1, arg2 }
}

fn root(id: i32, alg: BucketAlgorithm, weights: Vec<u32>) -> CrushBucket {
    let items: Vec<_> = (0..weights.len() as i32).collect();
    let weight = weights.iter().sum();
    let data = match alg {
        BucketAlgorithm::Straw => BucketData::Straw {
            straws: vec![0; weights.len()],
            item_weights: weights,
        },
        BucketAlgorithm::Straw2 => BucketData::Straw2 {
            item_weights: weights,
        },
        _ => unreachable!(),
    };
    CrushBucket {
        id,
        bucket_type: 1,
        alg,
        hash: 0,
        weight,
        size: items.len() as u32,
        items,
        data,
    }
}

fn map(alg: BucketAlgorithm, left: Vec<u32>, right: Vec<u32>) -> CrushMap {
    let mut map = CrushMap::new();
    map.max_devices = left.len() as i32;
    map.max_buckets = 2;
    map.max_rules = 2;
    map.choose_total_tries = 50;
    map.chooseleaf_descend_once = 1;
    map.chooseleaf_vary_r = 1;
    map.chooseleaf_stable = 1;
    map.allowed_bucket_algs = (1 << 1) | (1 << 2) | (1 << 4) | (1 << 5);
    map.buckets = vec![Some(root(-1, alg, left)), Some(root(-2, alg, right))];
    map.rules = [-1, -2]
        .into_iter()
        .enumerate()
        .map(|(rule_id, bucket)| {
            Some(CrushRule {
                rule_id: rule_id as u32,
                rule_type: RuleType::Replicated,
                steps: vec![
                    step(RuleOp::Take, bucket, 0),
                    step(RuleOp::ChooseFirstN, 0, 0),
                    step(RuleOp::Emit, 0, 0),
                ],
            })
        })
        .collect();
    map
}

fn set_straws(map: &mut CrushMap, bucket: usize, straws: &[u32]) {
    let BucketData::Straw { straws: actual, .. } = &mut map.buckets[bucket].as_mut().unwrap().data
    else {
        unreachable!()
    };
    actual.copy_from_slice(straws);
}

fn place(map: &CrushMap, rule: u32, x: u32, weights: &[u32]) -> Vec<i32> {
    let mut out = Vec::new();
    crush_do_rule(map, rule, x, &mut out, 1, weights).unwrap();
    out
}

fn digest(mut hash: u64, out: &[i32]) -> u64 {
    hash ^= out.len() as u64;
    hash = hash.wrapping_mul(1_099_511_628_211);
    for &item in out {
        hash ^= item as u32 as u64;
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    hash
}

fn report_audit(name: &str, detail: impl std::fmt::Display, hash: u64) {
    if std::env::var_os("CRUSH_REFERENCE_AUDIT").is_some() {
        let detail = detail.to_string();
        if detail.is_empty() {
            println!("WEIGHTS {name} digest={hash:016x}");
        } else {
            println!("WEIGHTS {name} {detail} digest={hash:016x}");
        }
    }
}

fn report_straws(name: &str, straws: &[u32]) {
    if std::env::var_os("CRUSH_REFERENCE_AUDIT").is_some() {
        let straws = straws
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(",");
        println!("STRAW_RUST {name} straws={straws}");
    }
}

// Upstream: v17.2.7/src/test/crush/crush.cc::CRUSHTest.straw_zero
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/crush/crush.cc#L268
// Upstream: v20.2.4/src/test/crush/crush.cc::CRUSHTest.straw_zero
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/crush.cc#L709
// Setup and assertions are identical in the two pinned releases.
// The exact STRAW lengths come from unmodified pinned builder.c with
// straw_calc_version=1; reference/verify-reference.py regenerates them.
#[test]
fn straw_zero() {
    let left = vec![4 * W, 3 * W, 2 * W, W, 0];
    let right = left[..4].to_vec();
    let mut map = map(BucketAlgorithm::Straw, left, right);
    for (index, expected) in [
        (0, &[99_499, 89_549, 78_975, 65_536, 0][..]),
        (1, &[99_499, 89_549, 78_975, 65_536][..]),
    ] {
        set_straws(&mut map, index, expected);
        let BucketData::Straw { straws, .. } = &map.buckets[index].as_ref().unwrap().data else {
            unreachable!()
        };
        assert_eq!(straws, expected);
        report_straws(if index == 0 { "zero0" } else { "zero1" }, straws);
    }
    let mut hash = 1_469_598_103_934_665_603;
    for x in 0..10_000 {
        let first = place(&map, 0, x, &[W; 5]);
        let second = place(&map, 1, x, &[W; 5]);
        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert_eq!(first[0], second[0]);
        hash = digest(hash, &first);
        hash = digest(hash, &second);
    }
    report_audit("straw_zero", "", hash);
}

// Upstream: v17.2.7/src/test/crush/crush.cc::CRUSHTest.straw_same
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/crush/crush.cc#L322
// Upstream: v20.2.4/src/test/crush/crush.cc::CRUSHTest.straw_same
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/crush.cc#L763
// Setup and assertions are identical in the two pinned releases.
// Exact lengths are captured from the pinned legacy STRAW builder, not inferred
// from the Rust mapper.
#[test]
fn straw_same() {
    let left: Vec<_> = (0u32..10).map(|i| W * (i.div_ceil(2) + 1)).collect();
    let right: Vec<_> = left
        .iter()
        .enumerate()
        .map(|(i, &weight)| weight + (i % 2) as u32 * 100)
        .collect();
    let mut map = map(BucketAlgorithm::Straw, left, right);
    for (index, expected) in [
        (
            0,
            &[
                65_536, 70_380, 70_380, 73_605, 73_605, 76_241, 76_241, 78_625, 78_625, 80_937,
            ][..],
        ),
        (
            1,
            &[
                65_536, 70_386, 70_380, 73_609, 73_605, 76_244, 76_240, 78_627, 78_624, 80_940,
            ][..],
        ),
    ] {
        set_straws(&mut map, index, expected);
        let BucketData::Straw { straws, .. } = &map.buckets[index].as_ref().unwrap().data else {
            unreachable!()
        };
        assert_eq!(straws, expected);
        report_straws(if index == 0 { "same0" } else { "same1" }, straws);
    }
    let mut hash = 1_469_598_103_934_665_603;
    let mut different = 0;
    for x in 0..100_000 {
        let first = place(&map, 0, x, &[W; 10]);
        let second = place(&map, 1, x, &[W; 10]);
        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        different += (first[0] != second[0]) as usize;
        hash = digest(hash, &first);
        hash = digest(hash, &second);
    }
    assert!((different as f64 / 100_000.0) < 0.001);
    report_audit("straw_same", format_args!("different={different}"), hash);
}

// Upstream: v17.2.7/src/test/crush/crush.cc::CRUSHTest.straw2_reweight
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/crush/crush.cc#L533
// Upstream: v20.2.4/src/test/crush/crush.cc::CRUSHTest.straw2_reweight
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/crush.cc#L974
// Setup and assertions are identical in the two pinned releases.
// Pinned C's unseeded process RNG reports rand()%10 == 7 on the reference
// platform, so the original integer-divided changed weight is 45,871.
#[test]
fn straw2_reweight() {
    let left = vec![
        W,
        W,
        2 * W,
        2 * W,
        3 * W,
        5 * W,
        W / 2,
        2 * W,
        W,
        W,
        2 * W,
        W,
        W,
        2 * W,
        48 * W,
    ];
    let mut right = left.clone();
    right[1] = W / 10 * 7;
    let map = map(BucketAlgorithm::Straw2, left, right);
    let mut hash = 1_469_598_103_934_665_603;
    for x in 0..1_000_000 {
        let first = place(&map, 0, x, &[W; 15]);
        let second = place(&map, 1, x, &[W; 15]);
        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert!(first[0] == 1 || first[0] == second[0]);
        hash = digest(hash, &first);
        hash = digest(hash, &second);
    }
    report_audit(
        "straw2_reweight",
        "rand_mod_10=7 changed_weight=45871",
        hash,
    );
}
