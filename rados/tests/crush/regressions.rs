//! Local differential regressions, not ports of named upstream tests.
//! Expected vectors come from pinned Ceph C code; see reference/README.md.

use rados::crush::{
    BucketAlgorithm, BucketData, CrushBucket, CrushError, CrushMap, CrushRule, CrushRuleStep,
    RuleOp, RuleType, mapper::crush_do_rule,
};

const WEIGHT: u32 = 0x1_0000;

fn step(op: RuleOp, arg1: i32, arg2: i32) -> CrushRuleStep {
    CrushRuleStep { op, arg1, arg2 }
}

fn map_with_rule(rule_type: RuleType, steps: Vec<CrushRuleStep>) -> CrushMap {
    let mut map = CrushMap::new();
    map.max_devices = 4;
    map.max_buckets = 3;
    map.max_rules = 1;
    map.choose_total_tries = 50;
    map.choose_local_tries = 0;
    map.choose_local_fallback_tries = 0;
    map.chooseleaf_descend_once = 1;
    map.chooseleaf_vary_r = 1;
    map.chooseleaf_stable = 1;
    map.msr_descents = 100;
    map.msr_collision_tries = 100;
    map.buckets = [
        (-1, 2, [-2, -3], 2 * WEIGHT),
        (-2, 1, [0, 1], WEIGHT),
        (-3, 1, [2, 3], WEIGHT),
    ]
    .into_iter()
    .map(|(id, bucket_type, items, weight)| {
        Some(CrushBucket {
            id,
            bucket_type,
            alg: BucketAlgorithm::Straw2,
            hash: 0,
            weight: 2 * weight,
            size: 2,
            items: items.to_vec(),
            data: BucketData::Straw2 {
                item_weights: vec![weight; 2],
            },
        })
    })
    .collect();
    map.rules = vec![Some(CrushRule {
        rule_id: 0,
        rule_type,
        steps,
    })];
    map
}

fn check_vectors(scenario: i32, map: &CrushMap, count: usize) {
    let records: Vec<Vec<i32>> = include_str!("reference/mapper-regressions.txt")
        .lines()
        .map(|line| {
            line.split_whitespace()
                .map(|n| n.parse().unwrap())
                .collect()
        })
        .collect();
    assert_eq!(records.len(), 7 * 16 * 100);
    for (index, record) in records.iter().enumerate() {
        assert_eq!(
            &record[..3],
            &[
                (index / 1600) as i32,
                (index / 100 % 16) as i32,
                (index % 100) as i32
            ]
        );
        assert_eq!(record[3] as usize, record.len() - 4);
    }
    let mut actual = vec![99];
    for record in records.iter().filter(|record| record[0] == scenario) {
        let mask = record[1];
        let x = record[2] as u32;
        let weights: Vec<_> = (0..4)
            .map(|i| if mask & (1 << i) == 0 { WEIGHT } else { 0 })
            .collect();
        crush_do_rule(map, 0, x, &mut actual, count, &weights).unwrap();
        assert_eq!(
            actual,
            &record[4..],
            "scenario={scenario} mask={mask} x={x}"
        );
    }
}

fn check_retry_vectors(cases: &[(i32, usize, Vec<CrushRuleStep>)]) {
    let records: Vec<Vec<i32>> = include_str!("reference/mapper-retries.txt")
        .lines()
        .map(|line| {
            line.split_whitespace()
                .map(|number| number.parse().unwrap())
                .collect()
        })
        .collect();
    assert_eq!(records.len(), cases.len() * 100);

    let mut actual = vec![99];
    for (case_index, (scenario, count, steps)) in cases.iter().enumerate() {
        let mut map = map_with_rule(RuleType::Replicated, steps.clone());
        if *scenario == 12 {
            map.choose_total_tries = 0;
        }
        for (seed, record) in records[case_index * 100..(case_index + 1) * 100]
            .iter()
            .enumerate()
        {
            assert_eq!(record[0], *scenario);
            assert_eq!(
                record[1],
                if (13..=16).contains(scenario) || (23..=24).contains(scenario) {
                    1
                } else {
                    0
                }
            );
            assert_eq!(record[2], seed as i32);
            assert_eq!(record[3] as usize, record.len() - 4);
            let weights = (0..4)
                .map(|device| {
                    if record[1] & (1 << device) == 0 {
                        WEIGHT
                    } else {
                        0
                    }
                })
                .collect::<Vec<_>>();
            crush_do_rule(&map, 0, seed as u32, &mut actual, *count, &weights).unwrap();
            assert_eq!(actual, &record[4..], "scenario={scenario} seed={seed}");
        }
    }
}

// Local case 0; v17.2.7/v20.2.4 src/crush/mapper.c::crush_do_rule[_no_retry].
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/crush/mapper.c#L999
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c#L948
#[test]
fn chained_indep_skips_unresolved_domains() {
    let map = map_with_rule(
        RuleType::Erasure,
        vec![
            step(RuleOp::Take, -1, 0),
            step(RuleOp::ChooseIndep, 3, 1),
            step(RuleOp::ChooseIndep, 1, 0),
            step(RuleOp::Emit, 0, 0),
        ],
    );
    check_vectors(0, &map, 3);
}

// Local case 1; v17.2.7/v20.2.4 src/crush/mapper.c::crush_do_rule[_no_retry].
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/crush/mapper.c#L1007
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c#L956
#[test]
fn firstn_honors_explicit_leaf_retries() {
    let mut map = map_with_rule(
        RuleType::Erasure,
        vec![
            step(RuleOp::SetChooseLeafTries, 10, 0),
            step(RuleOp::Take, -1, 0),
            step(RuleOp::ChooseLeafFirstN, 1, 1),
            step(RuleOp::Emit, 0, 0),
        ],
    );
    map.choose_total_tries = 0;
    check_vectors(1, &map, 1);
}

// Local cases 11..24; v17.2.7/v20.2.4 src/crush/mapper.c::crush_do_rule_no_retry.
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/crush/mapper.c#L941
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c#L890
#[test]
fn rule_retry_overrides_match_ceph() {
    check_retry_vectors(&[
        (
            11,
            2,
            vec![
                step(RuleOp::SetChooseTries, 1, 0),
                step(RuleOp::SetChooseTries, 0, 0),
                step(RuleOp::SetChooseTries, -1, 0),
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseLeafFirstN, 2, 1),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
        (
            12,
            2,
            vec![
                step(RuleOp::SetChooseLeafTries, 2, 0),
                step(RuleOp::SetChooseLeafTries, 0, 0),
                step(RuleOp::SetChooseLeafTries, -1, 0),
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseLeafFirstN, 2, 1),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
        (
            13,
            2,
            vec![
                step(RuleOp::SetChooseLocalTries, 1, 0),
                step(RuleOp::SetChooseLocalTries, -1, 0),
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseLeafFirstN, 2, 1),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
        (
            14,
            2,
            vec![
                step(RuleOp::SetChooseLocalTries, 1, 0),
                step(RuleOp::SetChooseLocalTries, 0, 0),
                step(RuleOp::SetChooseLocalTries, -1, 0),
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseLeafFirstN, 2, 1),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
        (
            15,
            2,
            vec![
                step(RuleOp::SetChooseLocalFallbackTries, 1, 0),
                step(RuleOp::SetChooseLocalFallbackTries, -1, 0),
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseLeafFirstN, 2, 1),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
        (
            16,
            2,
            vec![
                step(RuleOp::SetChooseLocalFallbackTries, 1, 0),
                step(RuleOp::SetChooseLocalFallbackTries, 0, 0),
                step(RuleOp::SetChooseLocalFallbackTries, -1, 0),
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseLeafFirstN, 2, 1),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
        (
            17,
            2,
            vec![
                step(RuleOp::SetChooseLeafVaryR, 2, 0),
                step(RuleOp::SetChooseLeafVaryR, -1, 0),
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseLeafFirstN, 2, 1),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
        (
            18,
            2,
            vec![
                step(RuleOp::SetChooseLeafVaryR, 2, 0),
                step(RuleOp::SetChooseLeafVaryR, 0, 0),
                step(RuleOp::SetChooseLeafVaryR, -1, 0),
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseLeafFirstN, 2, 1),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
        (
            19,
            2,
            vec![
                step(RuleOp::SetChooseLeafStable, 1, 0),
                step(RuleOp::SetChooseLeafStable, 0, 0),
                step(RuleOp::SetChooseLeafStable, -1, 0),
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseLeafFirstN, 2, 1),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
        (
            20,
            1,
            vec![
                step(RuleOp::SetChooseTries, 1, 0),
                step(RuleOp::SetChooseTries, 0, 0),
                step(RuleOp::SetChooseTries, -1, 0),
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseFirstN, 1, 0),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
        (
            21,
            3,
            vec![
                step(RuleOp::SetChooseLocalTries, 1, 0),
                step(RuleOp::SetChooseLocalTries, -1, 0),
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseFirstN, 3, 0),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
        (
            22,
            3,
            vec![
                step(RuleOp::SetChooseLocalTries, 1, 0),
                step(RuleOp::SetChooseLocalTries, 0, 0),
                step(RuleOp::SetChooseLocalTries, -1, 0),
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseFirstN, 3, 0),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
        (
            23,
            3,
            vec![
                step(RuleOp::SetChooseLocalFallbackTries, 1, 0),
                step(RuleOp::SetChooseLocalFallbackTries, -1, 0),
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseFirstN, 3, 0),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
        (
            24,
            3,
            vec![
                step(RuleOp::SetChooseLocalFallbackTries, 1, 0),
                step(RuleOp::SetChooseLocalFallbackTries, 0, 0),
                step(RuleOp::SetChooseLocalFallbackTries, -1, 0),
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseFirstN, 3, 0),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
    ]);
}

// Local case 2; v17.2.7/v20.2.4 src/crush/mapper.c::crush_do_rule[_no_retry].
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/crush/mapper.c#L1075
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c#L1024
#[test]
fn conventional_rules_skip_choose_msr() {
    for rule_type in [RuleType::Replicated, RuleType::Erasure] {
        let map = map_with_rule(
            rule_type,
            vec![
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseMsr, 2, 0),
                step(RuleOp::Emit, 0, 0),
            ],
        );
        check_vectors(2, &map, 3);
    }
}

// Local cases 3, 7..10; v20.2.4/src/crush/mapper.c::crush_msr_do_rule.
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c#L1847
#[test]
fn malformed_msr_blocks_discard_output() {
    let invalid_rules = [
        vec![
            step(RuleOp::Take, -1, 0),
            step(RuleOp::ChooseFirstN, 2, 0),
            step(RuleOp::Emit, 0, 0),
        ],
        vec![step(RuleOp::Emit, 0, 0)],
        vec![step(RuleOp::Take, -1, 0), step(RuleOp::ChooseMsr, 2, 0)],
        vec![
            step(RuleOp::Take, -1, 0),
            step(RuleOp::ChooseMsr, 1, 0),
            step(RuleOp::Emit, 0, 0),
            step(RuleOp::Emit, 0, 0),
        ],
        vec![
            step(RuleOp::Take, 0, 0),
            step(RuleOp::ChooseMsr, 1, 0),
            step(RuleOp::Emit, 0, 0),
        ],
    ];
    for rule_type in [RuleType::MsrFirstN, RuleType::MsrIndep] {
        for steps in &invalid_rules {
            let map = map_with_rule(rule_type, steps.clone());
            let mut actual = vec![99];
            crush_do_rule(&map, 0, 0, &mut actual, 3, &[WEIGHT; 4]).unwrap();
            assert!(actual.is_empty(), "{rule_type:?} {steps:?}: {actual:?}");
        }
        check_vectors(3, &map_with_rule(rule_type, invalid_rules[0].clone()), 3);
    }
}

// Local case 4; v17.2.7/v20.2.4 src/crush/mapper.c::crush_do_rule[_no_retry].
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/crush/mapper.c#L1067
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c#L1016
#[test]
fn emit_clears_working_set() {
    let map = map_with_rule(
        RuleType::Erasure,
        vec![
            step(RuleOp::Take, 0, 0),
            step(RuleOp::Emit, 0, 0),
            step(RuleOp::Emit, 0, 0),
        ],
    );
    check_vectors(4, &map, 3);
}

// Local cases 5/6; v20.2.4/src/crush/mapper.c::crush_msr_choose.
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/mapper.c#L1512
#[test]
fn msr_truncated_fanout_matches_ceph_for_every_out_mask() {
    for (scenario, rule_type) in [(5, RuleType::MsrFirstN), (6, RuleType::MsrIndep)] {
        let map = map_with_rule(
            rule_type,
            vec![
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseMsr, 2, 1),
                step(RuleOp::ChooseMsr, 2, 0),
                step(RuleOp::Emit, 0, 0),
            ],
        );
        check_vectors(scenario, &map, 3);
    }
}

// Rust contract; no upstream analogue found. Negative MSR fanout must be
// rejected before an integer cast or allocation, including in nested steps.
#[test]
fn msr_rejects_negative_fanout() {
    for rule_type in [RuleType::MsrFirstN, RuleType::MsrIndep] {
        for count in [-1, i32::MIN] {
            for choose_index in [1, 2] {
                let mut steps = vec![
                    step(RuleOp::Take, -1, 0),
                    step(RuleOp::ChooseMsr, 1, 1),
                    step(RuleOp::ChooseMsr, 1, 0),
                    step(RuleOp::Emit, 0, 0),
                ];
                steps[choose_index].arg1 = count;
                let map = map_with_rule(rule_type, steps);
                let mut actual = vec![99];
                assert!(matches!(
                    crush_do_rule(&map, 0, 0, &mut actual, 2, &[WEIGHT; 4]),
                    Err(CrushError::InvalidMsrFanout(value)) if value == count
                ));
            }
        }
    }
}
