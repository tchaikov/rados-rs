//! Functional ports of Ceph's CRUSH mapper tests.

use std::collections::{HashMap, HashSet};

use rados::crush::{
    BucketAlgorithm, BucketData, CrushBucket, CrushMap, CrushRule, CrushRuleStep, RuleOp, RuleType,
    mapper::crush_do_rule,
};

const NONE: i32 = 0x7fff_ffff;
const WEIGHT: u32 = 0x1_0000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Normal,
    Msr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Family {
    Indep,
    FirstN,
}

fn bucket(id: i32, bucket_type: i32, items: Vec<i32>, item_weight: u32) -> CrushBucket {
    let size = items.len() as u32;
    // build_*_map explicitly creates a STRAW root. insert_item creates the
    // intermediate buckets with the optimal default algorithm, STRAW2.
    let alg = if bucket_type == 5 {
        BucketAlgorithm::Straw
    } else {
        BucketAlgorithm::Straw2
    };
    let item_weights = vec![item_weight; items.len()];
    CrushBucket {
        id,
        bucket_type,
        alg,
        hash: 0,
        weight: item_weight * size,
        size,
        data: if alg == BucketAlgorithm::Straw {
            BucketData::Straw {
                item_weights,
                straws: vec![WEIGHT; items.len()],
            }
        } else {
            BucketData::Straw2 { item_weights }
        },
        items,
    }
}

fn store_bucket(map: &mut CrushMap, bucket: CrushBucket) {
    let index = (-1 - bucket.id) as usize;
    if map.buckets.len() <= index {
        map.buckets.resize(index + 1, None);
    }
    map.buckets[index] = Some(bucket);
}

fn optimal_map() -> CrushMap {
    let mut map = CrushMap::new();
    map.choose_local_tries = 0;
    map.choose_local_fallback_tries = 0;
    map.choose_total_tries = 50;
    map.chooseleaf_descend_once = 1;
    map.chooseleaf_vary_r = 1;
    map.chooseleaf_stable = 1;
    map.allowed_bucket_algs = (1 << 1) | (1 << 2) | (1 << 4) | (1 << 5);
    map
}

fn build_map(mode: Mode, family: Family, racks: usize, hosts: usize, osds: usize) -> CrushMap {
    let mut map = optimal_map();
    map.max_devices = (racks * hosts * osds) as i32;

    let mut next_bucket = -2;
    let mut next_osd = 0;
    let mut rack_ids = Vec::with_capacity(racks);
    for _ in 0..racks {
        let mut host_ids = Vec::with_capacity(hosts);
        let mut rack_id = None;
        for host in 0..hosts {
            let host_id = next_bucket;
            next_bucket -= 1;
            host_ids.push(host_id);
            if host == 0 {
                rack_id = Some(next_bucket);
                next_bucket -= 1;
            }
            let devices: Vec<_> = (next_osd..next_osd + osds as i32).collect();
            next_osd += osds as i32;
            store_bucket(&mut map, bucket(host_id, 1, devices, WEIGHT));
        }
        let rack_id = rack_id.unwrap();
        rack_ids.push(rack_id);
        store_bucket(&mut map, bucket(rack_id, 3, host_ids, WEIGHT * osds as u32));
    }
    store_bucket(
        &mut map,
        bucket(-1, 5, rack_ids, WEIGHT * (hosts * osds) as u32),
    );
    map.max_buckets = map.buckets.len() as i32;

    let (rule_type, steps) = match (mode, family) {
        (Mode::Msr, Family::Indep) => (
            RuleType::MsrIndep,
            vec![
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseMsr, 0, 1),
                step(RuleOp::ChooseMsr, 1, 0),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
        (Mode::Msr, Family::FirstN) => (
            RuleType::MsrFirstN,
            vec![
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseMsr, 0, 1),
                step(RuleOp::ChooseMsr, 1, 0),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
        (Mode::Normal, Family::Indep) => (
            RuleType::Erasure,
            vec![
                step(RuleOp::SetChooseLeafTries, 10, 0),
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseLeafIndep, 0, 1),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
        (Mode::Normal, Family::FirstN) => (
            RuleType::Erasure,
            vec![
                step(RuleOp::SetChooseLeafTries, 0, 0),
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseLeafFirstN, 0, 1),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
    };
    map.rules = vec![Some(CrushRule {
        rule_id: 0,
        rule_type,
        steps,
    })];
    map.max_rules = 1;
    map
}

fn step(op: RuleOp, arg1: i32, arg2: i32) -> CrushRuleStep {
    CrushRuleStep { op, arg1, arg2 }
}

fn place(map: &CrushMap, x: u32, count: usize, weights: &[u32]) -> Vec<i32> {
    let mut out = Vec::new();
    crush_do_rule(map, 0, x, &mut out, count, weights).unwrap();
    out
}

fn assert_no_duplicates(out: &[i32], context: &str) {
    let selected: Vec<_> = out.iter().copied().filter(|&item| item != NONE).collect();
    assert_eq!(
        selected.iter().copied().collect::<HashSet<_>>().len(),
        selected.len(),
        "{context}: {out:?}"
    );
}

fn toosmall(mode: Mode, family: Family) {
    let map = build_map(mode, family, 1, 3, 1);
    let weights = vec![WEIGHT; map.max_devices as usize];
    for x in 0..100 {
        let out = place(&map, x, 5, &weights);
        assert_no_duplicates(&out, &format!("{mode:?} {family:?} x={x}"));
        match family {
            Family::Indep => assert_eq!(out.iter().filter(|&&item| item == NONE).count(), 2),
            Family::FirstN => {
                assert_eq!(out.len(), 3);
                assert!(!out.contains(&NONE));
            }
        }
    }
}

fn basic(mode: Mode, family: Family) {
    let map = build_map(mode, family, 3, 3, 3);
    let weights = vec![WEIGHT; map.max_devices as usize];
    let count = if family == Family::Indep { 5 } else { 3 };
    for x in 0..100 {
        let out = place(&map, x, count, &weights);
        assert_eq!(out.len(), count, "{mode:?} {family:?} x={x}: {out:?}");
        assert!(!out.contains(&NONE));
        assert_no_duplicates(&out, &format!("{mode:?} {family:?} x={x}"));
    }
}

fn single_out_first(mode: Mode, family: Family) {
    let map = build_map(mode, family, 3, 3, 3);
    let count = if family == Family::Indep { 5 } else { 3 };
    for x in 0..1000 {
        let mut weights = vec![WEIGHT; map.max_devices as usize];
        let before = place(&map, x, count, &weights);
        assert_eq!(before.len(), count);
        assert!(!before.contains(&NONE));
        assert_no_duplicates(&before, &format!("before {mode:?} {family:?} x={x}"));
        weights[before[0] as usize] = 0;
        let after = place(&map, x, count, &weights);
        assert_eq!(after.len(), count, "{mode:?} {family:?} x={x}: {after:?}");
        assert_no_duplicates(&after, &format!("after {mode:?} {family:?} x={x}"));
        assert!(!after.contains(&before[0]));
        if family == Family::Indep {
            assert_ne!(after[0], NONE);
            assert_eq!(&after[1..], &before[1..]);
        } else if mode == Mode::Msr {
            assert_eq!(&after[..2], &before[1..]);
        }
    }
}

fn single_out_last(mode: Mode, family: Family) {
    let map = build_map(mode, family, 3, 3, 3);
    let count = if family == Family::Indep { 5 } else { 3 };
    for x in 0..1000 {
        let mut weights = vec![WEIGHT; map.max_devices as usize];
        let before = place(&map, x, count, &weights);
        assert_eq!(before.len(), count);
        assert!(!before.contains(&NONE));
        assert_no_duplicates(&before, &format!("before {mode:?} {family:?} x={x}"));
        let last = count - 1;
        weights[before[last] as usize] = 0;
        let after = place(&map, x, count, &weights);
        assert_eq!(after.len(), count, "{mode:?} {family:?} x={x}: {after:?}");
        assert_no_duplicates(&after, &format!("after {mode:?} {family:?} x={x}"));
        assert!(!after.contains(&before[last]));
        if family == Family::Indep {
            assert_ne!(after[last], NONE);
        }
        assert_eq!(&after[..last], &before[..last]);
    }
}

fn out_alt(mode: Mode, family: Family) {
    let mut map = build_map(mode, family, 3, 3, 3);
    if family == Family::Indep {
        map.choose_total_tries = 100;
    } else if mode == Mode::Normal {
        map.choose_total_tries = 500;
    }
    let mut weights = vec![WEIGHT; map.max_devices as usize];
    for osd in (0..26).step_by(2) {
        weights[osd] = 0;
    }
    for x in 0..100 {
        let out = place(&map, x, 9, &weights);
        assert_eq!(out.len(), 9, "{mode:?} {family:?} x={x}: {out:?}");
        assert!(!out.contains(&NONE));
        assert_no_duplicates(&out, &format!("{mode:?} {family:?} x={x}"));
    }
}

fn out_contig(mode: Mode, family: Family) {
    let mut map = build_map(mode, family, 3, 3, 3);
    if family == Family::Indep {
        map.choose_total_tries = 100;
    } else if mode == Mode::Normal {
        map.choose_total_tries = 500;
    }
    let mut weights = vec![WEIGHT; map.max_devices as usize];
    weights[..9].fill(0);
    for x in 0..100 {
        let out = place(&map, x, 7, &weights);
        assert_no_duplicates(&out, &format!("{mode:?} {family:?} x={x}"));
        match family {
            Family::Indep => {
                assert_eq!(out.len(), 7);
                assert_eq!(out.iter().filter(|&&item| item == NONE).count(), 1);
            }
            Family::FirstN => assert_eq!(out.len(), 6),
        }
    }
}

fn out_progressive(mode: Mode, family: Family) {
    let mut map = build_map(mode, family, 3, 3, 3);
    if family == Family::Indep {
        map.choose_total_tries = 100;
    } else if mode == Mode::Normal {
        map.choose_total_tries = 500;
    }
    for x in 1..5 {
        let mut weights = vec![WEIGHT; map.max_devices as usize];
        let mut previous = Vec::new();
        let mut positions = HashMap::new();
        for osd in 0..weights.len() {
            let out = place(&map, x, 7, &weights);
            assert_no_duplicates(&out, &format!("{mode:?} {family:?} x={x} out={osd}"));
            if osd > 0 {
                let changed = if family == Family::Indep {
                    out.iter()
                        .enumerate()
                        .filter(|&(index, item)| previous.get(index) != Some(item))
                        .count()
                } else {
                    let previous: HashSet<_> = previous.iter().copied().collect();
                    out.iter().filter(|item| !previous.contains(item)).count()
                };
                assert!(changed <= 3, "{mode:?} {family:?} x={x} out={osd}: {out:?}");
                if family == Family::Indep {
                    let moved = out
                        .iter()
                        .enumerate()
                        .filter(|(_, item)| **item != NONE)
                        .filter(|&(index, item)| {
                            positions.get(item).is_some_and(|&old| old != index)
                        })
                        .count();
                    assert!(moved <= 1, "{mode:?} x={x} out={osd}: {out:?}");
                }
            }
            weights[osd] = 0;
            positions = out
                .iter()
                .enumerate()
                .filter(|(_, item)| **item != NONE)
                .map(|(index, &item)| (item, index))
                .collect();
            previous = out;
        }
    }
}

macro_rules! variants {
    ($normal:ident, $msr:ident, $scenario:ident, $family:expr) => {
        #[test]
        fn $normal() {
            $scenario(Mode::Normal, $family);
        }

        #[test]
        fn $msr() {
            $scenario(Mode::Msr, $family);
        }
    };
}

// Upstream: v17.2.7/src/test/crush/crush.cc::CRUSHTest.indep_toosmall (NORMAL)
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/crush/crush.cc#L115
// Mapping-only adaptation: the source's undecodable test type 123 is equivalent
// to Erasure type 3 for every source vector; verified by reference-check.c.
// Upstream: v20.2.4/src/test/crush/crush.cc::IndepTest.toosmall (NORMAL, MSR)
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/crush.cc#L152
variants!(
    indep_toosmall_normal,
    indep_toosmall_msr,
    toosmall,
    Family::Indep
);

// Upstream: v17.2.7/src/test/crush/crush.cc::CRUSHTest.indep_basic (NORMAL)
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/crush/crush.cc#L134
// Mapping-only adaptation: source type 123 maps as Erasure after the C proof above.
// Upstream: v20.2.4/src/test/crush/crush.cc::IndepTest.basic (NORMAL, MSR)
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/crush.cc#L171
variants!(indep_basic_normal, indep_basic_msr, basic, Family::Indep);

// Upstream: v20.2.4/src/test/crush/crush.cc::IndepTest.single_out_first (NORMAL, MSR)
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/crush.cc#L190
variants!(
    indep_single_out_first_normal,
    indep_single_out_first_msr,
    single_out_first,
    Family::Indep
);

// Upstream: v20.2.4/src/test/crush/crush.cc::IndepTest.single_out_last (NORMAL, MSR)
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/crush.cc#L230
variants!(
    indep_single_out_last_normal,
    indep_single_out_last_msr,
    single_out_last,
    Family::Indep
);

// Upstream: v17.2.7/src/test/crush/crush.cc::CRUSHTest.indep_out_alt (NORMAL)
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/crush/crush.cc#L153
// Mapping-only adaptation: source type 123 maps as Erasure after the C proof above.
// Upstream: v20.2.4/src/test/crush/crush.cc::IndepTest.out_alt (NORMAL, MSR)
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/crush.cc#L271
variants!(
    indep_out_alt_normal,
    indep_out_alt_msr,
    out_alt,
    Family::Indep
);

// Upstream: v17.2.7/src/test/crush/crush.cc::CRUSHTest.indep_out_contig (NORMAL)
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/crush/crush.cc#L179
// Mapping-only adaptation: source type 123 maps as Erasure after the C proof above.
// Upstream: v20.2.4/src/test/crush/crush.cc::IndepTest.out_contig (NORMAL, MSR)
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/crush.cc#L297
variants!(
    indep_out_contig_normal,
    indep_out_contig_msr,
    out_contig,
    Family::Indep
);

// Upstream: v17.2.7/src/test/crush/crush.cc::CRUSHTest.indep_out_progressive (NORMAL)
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/crush/crush.cc#L205
// Mapping-only adaptation: source type 123 maps as Erasure after the C proof above.
// Upstream: v20.2.4/src/test/crush/crush.cc::IndepTest.out_progressive (NORMAL, MSR)
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/crush.cc#L322
variants!(
    indep_out_progressive_normal,
    indep_out_progressive_msr,
    out_progressive,
    Family::Indep
);

// Upstream: v20.2.4/src/test/crush/crush.cc::FirstnTest.basic (NORMAL, MSR)
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/crush.cc#L485
variants!(firstn_basic_normal, firstn_basic_msr, basic, Family::FirstN);

// Upstream: v20.2.4/src/test/crush/crush.cc::FirstnTest.toosmall (NORMAL, MSR)
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/crush.cc#L502
variants!(
    firstn_toosmall_normal,
    firstn_toosmall_msr,
    toosmall,
    Family::FirstN
);

// Upstream: v20.2.4/src/test/crush/crush.cc::FirstnTest.single_out_first (NORMAL, MSR)
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/crush.cc#L519
variants!(
    firstn_single_out_first_normal,
    firstn_single_out_first_msr,
    single_out_first,
    Family::FirstN
);

// Upstream: v20.2.4/src/test/crush/crush.cc::FirstnTest.single_out_last (NORMAL, MSR)
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/crush.cc#L560
variants!(
    firstn_single_out_last_normal,
    firstn_single_out_last_msr,
    single_out_last,
    Family::FirstN
);

// Upstream: v20.2.4/src/test/crush/crush.cc::FirstnTest.out_alt (NORMAL, MSR)
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/crush.cc#L598
variants!(
    firstn_out_alt_normal,
    firstn_out_alt_msr,
    out_alt,
    Family::FirstN
);

// Upstream: v20.2.4/src/test/crush/crush.cc::FirstnTest.out_contig (NORMAL, MSR)
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/crush.cc#L621
variants!(
    firstn_out_contig_normal,
    firstn_out_contig_msr,
    out_contig,
    Family::FirstN
);

// Upstream: v20.2.4/src/test/crush/crush.cc::FirstnTest.out_progressive (NORMAL, MSR)
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/crush.cc#L644
variants!(
    firstn_out_progressive_normal,
    firstn_out_progressive_msr,
    out_progressive,
    Family::FirstN
);

// Mirrors create_crush_heirarchy and msr_multi_root's populate_root: roots
// and hosts are STRAW2, IDs follow insertion order, all OSDs weigh 1.0.
fn build_msr_map(
    roots: i32,
    hosts: i32,
    osds: i32,
    choose_hosts: i32,
    choose_osds: i32,
) -> CrushMap {
    let mut map = optimal_map();
    map.max_devices = roots * hosts * osds;
    let mut steps = Vec::new();
    for root in 0..roots {
        let root_id = -1 - map.buckets.len() as i32;
        map.buckets.push(None);
        let mut host_ids = Vec::new();
        for host in 0..hosts {
            let id = -1 - map.buckets.len() as i32;
            let first = (root * hosts + host) * osds;
            store_bucket(
                &mut map,
                bucket(id, 1, (first..first + osds).collect(), WEIGHT),
            );
            host_ids.push(id);
        }
        store_bucket(&mut map, bucket(root_id, 2, host_ids, osds as u32 * WEIGHT));
        steps.extend([
            step(RuleOp::Take, root_id, 0),
            step(RuleOp::ChooseMsr, choose_hosts, 1),
            step(RuleOp::ChooseMsr, choose_osds, 0),
            step(RuleOp::Emit, 0, 0),
        ]);
    }
    map.max_buckets = map.buckets.len() as i32;
    map.max_rules = 1;
    map.rules = vec![Some(CrushRule {
        rule_id: 0,
        rule_type: RuleType::MsrIndep,
        steps,
    })];
    map
}

fn take_host_out(weights: &mut [u32], osd: i32, osds_per_host: usize) {
    let first = osd as usize / osds_per_host * osds_per_host;
    weights[first..first + osds_per_host].fill(0);
}

fn check_msr_host_replacement(
    hosts: i32,
    osds: i32,
    choose_hosts: i32,
    per_host: usize,
    count: usize,
) {
    let map = build_msr_map(1, hosts, osds, choose_hosts, per_host as i32);
    let mut weights = vec![WEIGHT; map.max_devices as usize];
    let before = place(&map, 0, count, &weights);
    assert_eq!(before.len(), count);
    assert!(
        before
            .iter()
            .all(|&osd| (0..map.max_devices).contains(&osd))
    );
    take_host_out(&mut weights, before[0], osds as usize);
    let after = place(&map, 0, count, &weights);
    assert_eq!(after.len(), count);
    for i in 0..per_host {
        assert_ne!(before[i], after[i]);
        assert!((0..map.max_devices).contains(&after[i]));
        assert_ne!(before[i] / osds, after[i] / osds);
    }
    assert_eq!(before[per_host..], after[per_host..]);
}

// Upstream: v20.2.4/src/test/crush/crush.cc::CRUSHTest.msr_4_host_2_choose_rule
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/crush.cc#L1277
#[test]
fn msr_4_host_2_choose_rule() {
    let map = build_msr_map(1, 4, 3, 3, 1);
    let all_in = vec![WEIGHT; 12];
    let before = place(&map, 0, 3, &all_in);
    assert_eq!(before.len(), 3);
    assert!(before.iter().all(|&osd| (0..12).contains(&osd)));
    let mut host_out = all_in.clone();
    take_host_out(&mut host_out, before[0], 3);
    let mut osd_out = all_in;
    osd_out[before[0] as usize] = 0;
    for weights in [host_out, osd_out] {
        let after = place(&map, 0, 3, &weights);
        assert_eq!(after.len(), 3);
        assert_eq!(
            before.iter().filter(|&&osd| osd != NONE).count(),
            after.iter().filter(|&&osd| osd != NONE).count()
        );
    }
}

// Upstream: v20.2.4/src/test/crush/crush.cc::CRUSHTest.msr_2_host_2_osd
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/crush.cc#L1325
#[test]
fn msr_2_host_2_osd() {
    check_msr_host_replacement(3, 2, 2, 2, 3);
}

// Upstream: v20.2.4/src/test/crush/crush.cc::CRUSHTest.msr_5_host_8_6_ec_choose
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/crush.cc#L1371
#[test]
fn msr_5_host_8_6_ec_choose() {
    check_msr_host_replacement(5, 4, 4, 4, 14);
}

fn validate_multi_root(out: &[i32]) {
    assert_eq!(out.len(), 8);
    let mut hosts = HashSet::new();
    for (group, items) in out.chunks_exact(2).enumerate() {
        // Additional coverage: upstream looks up out[start] for every item.
        // Check each item's root and all hosts used by each failure domain.
        let mut group_hosts = HashSet::new();
        for &osd in items {
            assert!((0..24).contains(&osd));
            assert_eq!(osd / 12, (group / 2) as i32);
            group_hosts.insert(osd / 3);
        }
        for host in group_hosts {
            assert!(
                hosts.insert(host),
                "host {host} reused across failure domains: {out:?}"
            );
        }
    }
}

// Upstream: v20.2.4/src/test/crush/crush.cc::CRUSHTest.msr_multi_root
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/crush.cc#L1417
#[test]
fn msr_multi_root() {
    let map = build_msr_map(2, 4, 3, 2, 2);
    let all_in = vec![WEIGHT; 24];
    for x in 0..1000 {
        let before = place(&map, x, 8, &all_in);
        validate_multi_root(&before);

        let mut osd_out = all_in.clone();
        for i in [1, 5] {
            osd_out[before[i] as usize] = 0;
        }
        let after = place(&map, x, 8, &osd_out);
        validate_multi_root(&after);
        for i in 0..8 {
            if [1, 5].contains(&i) {
                assert_ne!(before[i], after[i], "x={x}, position={i}");
            } else {
                assert_eq!(before[i], after[i], "x={x}, position={i}");
            }
        }

        let mut host_out = all_in.clone();
        for i in [2, 6] {
            take_host_out(&mut host_out, before[i], 3);
        }
        let after = place(&map, x, 8, &host_out);
        validate_multi_root(&after);
        for i in 0..8 {
            if host_out[before[i] as usize] == 0 {
                assert_ne!(before[i], after[i], "x={x}, position={i}");
            } else {
                assert_eq!(before[i], after[i], "x={x}, position={i}");
            }
        }
    }
}

fn weight_distribution_deviation(replicas: usize) -> i64 {
    // CrushCompiler::compile starts with legacy tunables when the text
    // map supplies none, as in crush_weights.sh. Preserve that profile.
    let mut map = CrushMap::new();
    map.max_buckets = 1;
    map.max_devices = 5;
    map.max_rules = 1;
    let mut root = bucket(-1, 1, (0..5).collect(), WEIGHT);
    root.weight = 41 * WEIGHT;
    root.data = BucketData::Straw2 {
        item_weights: vec![10 * WEIGHT, 10 * WEIGHT, 10 * WEIGHT, 10 * WEIGHT, WEIGHT],
    };
    map.buckets = vec![Some(root)];
    map.rules = vec![Some(CrushRule {
        rule_id: 0,
        rule_type: RuleType::Replicated,
        steps: vec![
            step(RuleOp::Take, -1, 0),
            step(RuleOp::ChooseFirstN, 0, 0),
            step(RuleOp::Emit, 0, 0),
        ],
    })];
    let mut counts = [0i64; 5];
    let mut out = Vec::new();
    for x in 1..=1_000_000 {
        crush_do_rule(&map, 0, x, &mut out, replicas, &[WEIGHT; 5]).unwrap();
        for &osd in &out {
            counts[osd as usize] += 1;
        }
    }
    // bc scale=5 truncates the positive division before subtracting from 10.
    10 * 100_000 - counts[0] * 100_000 / counts[4]
}

// Upstream (locally assigned block name):
// v17.2.7/src/test/crush/crush_weights.sh::three-replica-distribution
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/crush/crush_weights.sh#L44
// v20.2.4/src/test/crush/crush_weights.sh::three-replica-distribution
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/crush_weights.sh#L44
#[test]
fn three_replica_weight_distribution() {
    assert!(weight_distribution_deviation(3) >= 75_000);
}

// Upstream (locally assigned block name):
// v17.2.7/src/test/crush/crush_weights.sh::one-replica-distribution
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/crush/crush_weights.sh#L53
// v20.2.4/src/test/crush/crush_weights.sh::one-replica-distribution
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/crush_weights.sh#L53
#[test]
fn one_replica_weight_distribution() {
    assert!((-10_000..=10_000).contains(&weight_distribution_deviation(1)));
}

// Upstream (cmd-02/cmd-03 are locally assigned):
// v17.2.7/src/test/cli/crushtool/bad-mappings.t::cmd-02,cmd-03
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/bad-mappings.t#L2
// v20.2.4/src/test/cli/crushtool/bad-mappings.t::cmd-02,cmd-03
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/bad-mappings.t#L2
#[test]
fn bad_mappings() {
    // Exact topology from fixtures/bad-mappings.crushmap.txt. The compiler
    // starts with legacy tunables; equal STRAW weights produce 0x10000 straws.
    let mut map = CrushMap::new();
    map.max_devices = 5;
    map.max_buckets = 1;
    map.max_rules = 2;
    map.buckets = vec![Some(CrushBucket {
        id: -1,
        bucket_type: 1,
        alg: BucketAlgorithm::Straw,
        hash: 0,
        weight: 5 * WEIGHT,
        size: 5,
        items: (0..5).collect(),
        data: BucketData::Straw {
            item_weights: vec![WEIGHT; 5],
            straws: vec![WEIGHT; 5],
        },
    })];
    map.rules = [
        (RuleType::Replicated, RuleOp::ChooseFirstN),
        (RuleType::Erasure, RuleOp::ChooseIndep),
    ]
    .into_iter()
    .enumerate()
    .map(|(id, (rule_type, op))| {
        Some(CrushRule {
            rule_id: id as u32,
            rule_type,
            steps: vec![
                step(RuleOp::Take, -1, 0),
                step(op, 0, 0),
                step(RuleOp::Emit, 0, 0),
            ],
        })
    })
    .collect();
    for (rule_id, expected) in [
        (0, &[4, 0, 2, 3, 1][..]),
        (1, &[4, 0, 2, 1, 3, NONE, NONE, NONE, NONE, NONE][..]),
    ] {
        let mut out = Vec::new();
        crush_do_rule(&map, rule_id, 1, &mut out, 10, &[WEIGHT; 5]).unwrap();
        assert_eq!(out, expected, "rule {rule_id}");
    }
}
