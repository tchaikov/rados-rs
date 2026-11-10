// Upstream: v17.2.7/src/test/crush/CrushWrapper.cc::CrushWrapperTest.choose_args_compat
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/crush/CrushWrapper.cc#L1014
// Tentacle has the same setup and assertions at
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/CrushWrapper.cc#L1011.

use bytes::Bytes;
use rados::crush::mapper::crush_do_rule;
use rados::crush::{
    BucketAlgorithm, BucketData, CrushBucket, CrushChooseArg, CrushMap, CrushRule, CrushRuleStep,
    RuleOp, RuleType,
};
use rados::crush::{PgId, crush_do_rule_with_choose_args, pg_to_osds};

fn decode_fixture(bytes: &'static [u8]) -> CrushMap {
    CrushMap::decode(&mut Bytes::from_static(bytes)).unwrap()
}

fn map_with_default_choose_arg() -> CrushMap {
    let mut map = CrushMap::new();
    map.max_devices = 2;
    map.max_buckets = 1;
    map.buckets = vec![Some(CrushBucket {
        id: -1,
        bucket_type: 1,
        alg: BucketAlgorithm::Straw2,
        hash: 0,
        weight: 2 << 16,
        size: 2,
        items: vec![0, 1],
        data: BucketData::Straw2 {
            item_weights: vec![1 << 16, 1 << 16],
        },
    })];
    map.rules = vec![Some(CrushRule {
        rule_id: 0,
        rule_type: RuleType::Replicated,
        steps: vec![
            CrushRuleStep {
                op: RuleOp::Take,
                arg1: -1,
                arg2: 0,
            },
            CrushRuleStep {
                op: RuleOp::ChooseFirstN,
                arg1: 1,
                arg2: 0,
            },
            CrushRuleStep {
                op: RuleOp::Emit,
                arg1: 0,
                arg2: 0,
            },
        ],
    })];
    map.choose_args.insert(
        -1,
        vec![Some(CrushChooseArg {
            weight_set: vec![vec![1 << 16, 0]],
            ids: vec![],
        })],
    );
    map
}

#[test]
fn default_choose_args_override_straw2_weights() {
    let map = map_with_default_choose_arg();
    for x in 0..100 {
        let mut result = Vec::new();
        crush_do_rule(&map, 0, x, &mut result, 1, &[1 << 16, 1 << 16]).unwrap();
        assert_eq!(result, vec![0], "x={x}");
    }
}

#[test]
fn choose_args_compat_keeps_alternate_weights_and_legacy_folds_them() {
    // Upstream: v17.2.7/src/test/crush/CrushWrapper.cc::CrushWrapperTest.choose_args_compat
    // Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/crush/CrushWrapper.cc#L1014
    for bytes in [
        include_bytes!("reference/choose-args-compat-quincy.crushmap").as_slice(),
        include_bytes!("reference/choose-args-compat-tentacle.crushmap").as_slice(),
    ] {
        let map = decode_fixture(bytes);
        let bucket = map.get_bucket(-1).unwrap();
        assert_eq!(bucket.items, vec![1]);
        assert!(matches!(
            &bucket.data,
            BucketData::Straw2 { item_weights } if item_weights == &[12 << 16]
        ));
        let arg = map.choose_args.get(&-1).unwrap()[0].as_ref().unwrap();
        assert_eq!(arg.weight_set, vec![vec![666 << 16]]);
        assert!(arg.ids.is_empty());
    }
    for bytes in [
        include_bytes!("reference/choose-args-compat-quincy-legacy.crushmap").as_slice(),
        include_bytes!("reference/choose-args-compat-tentacle-legacy.crushmap").as_slice(),
    ] {
        let map = decode_fixture(bytes);
        assert!(map.choose_args.is_empty());
        assert!(matches!(
            &map.get_bucket(-1).unwrap().data,
            BucketData::Straw2 { item_weights } if item_weights == &[666 << 16]
        ));
    }
}

#[test]
fn choose_args_count_is_independent_of_bucket_count() {
    // Upstream: v17.2.7/src/crush/CrushWrapper.cc::encode/decode
    // Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/crush/CrushWrapper.cc#L3052-L3055
    // Upstream: v20.2.4/src/crush/CrushWrapper.cc::encode/decode
    // Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/CrushWrapper.cc#L3210-L3213
    for fixture in [
        include_bytes!("reference/choose-args-compat-quincy.crushmap").as_slice(),
        include_bytes!("reference/choose-args-compat-tentacle.crushmap").as_slice(),
    ] {
        let mut bytes = fixture.to_vec();
        let count_offset = bytes.len() - 36;
        assert_eq!(&bytes[count_offset..count_offset + 4], &1u32.to_le_bytes());
        bytes[count_offset..count_offset + 4].copy_from_slice(&9u32.to_le_bytes());
        for index in 0i64..8 {
            bytes.extend_from_slice(&index.to_le_bytes());
            bytes.extend_from_slice(&0u32.to_le_bytes());
        }

        let map = CrushMap::decode(&mut Bytes::from(bytes)).unwrap();
        assert_eq!(map.buckets.len(), 8);
        assert_eq!(map.choose_args.len(), 9);
    }
}

#[test]
fn choose_args_cli_fixture_keeps_empty_index_and_signed_hash_ids() {
    // Upstream: v17.2.7/src/test/cli/crushtool/choose-args.t::cmd-01
    // Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/choose-args.t#L1
    for bytes in [
        include_bytes!("reference/choose-args-quincy.crushmap").as_slice(),
        include_bytes!("reference/choose-args-tentacle.crushmap").as_slice(),
    ] {
        let map = decode_fixture(bytes);
        assert!(map.choose_args.contains_key(&1));
        assert!(map.choose_args[&1].iter().all(Option::is_none));
        assert!(!map.choose_args.contains_key(&0));
        for (id, bucket_type, weight, items, item_weights) in [
            (-1, 1, 1, vec![0], vec![1 << 16]),
            (-2, 1, 1, vec![1], vec![1 << 16]),
            (-3, 2, 3, vec![-1, -2, -5], vec![1 << 16, 1 << 16, 1 << 16]),
            (-4, 3, 4, vec![-3], vec![4 << 16]),
            (-5, 1, 1, vec![2], vec![1 << 16]),
        ] {
            let bucket = map.get_bucket(id).unwrap();
            assert_eq!(bucket.alg, BucketAlgorithm::Straw2);
            assert_eq!(bucket.hash, 0);
            assert_eq!(bucket.bucket_type, bucket_type);
            assert_eq!(bucket.weight, weight << 16);
            assert_eq!(bucket.size, items.len() as u32);
            assert_eq!(bucket.items, items);
            assert!(
                matches!(&bucket.data, BucketData::Straw2 { item_weights: actual } if actual == &item_weights)
            );
        }

        let arg2 = map.choose_args[&2][2].as_ref().unwrap();
        assert!(arg2.weight_set.is_empty());
        assert_eq!(arg2.ids, vec![-20, 30, -25]);
        let arg = map.choose_args[&3][2].as_ref().unwrap();
        assert_eq!(
            arg.weight_set,
            vec![
                vec![1 << 16, 2 << 16, 5 << 16],
                vec![3 << 16, 2 << 16, 5 << 16]
            ]
        );
        assert_eq!(arg.ids, vec![-20, -30, -25]);
        let arg4 = map.choose_args[&4][1].as_ref().unwrap();
        assert_eq!(arg4.weight_set, vec![vec![1 << 16], vec![3 << 16]]);
        assert!(arg4.ids.is_empty());
        assert_eq!(map.choose_args[&5][0].as_ref().unwrap().ids, vec![-450]);
        assert!(
            map.choose_args[&5][0]
                .as_ref()
                .unwrap()
                .weight_set
                .is_empty()
        );
        let args6 = &map.choose_args[&6];
        assert_eq!(args6[0].as_ref().unwrap().ids, vec![-450]);
        assert_eq!(
            args6[1].as_ref().unwrap().weight_set,
            vec![vec![1 << 16], vec![3 << 16]]
        );
        assert!(args6[1].as_ref().unwrap().ids.is_empty());
        assert_eq!(args6[2].as_ref().unwrap().weight_set, arg.weight_set);
        assert_eq!(args6[2].as_ref().unwrap().ids, arg.ids);
        assert!(args6[3].is_none());
        assert!(args6[4].is_none());
    }
}

#[test]
fn selected_empty_choose_arg_does_not_fall_back_to_default() {
    // Local C-mapper contract: an existing empty index is distinct from an
    // absent index; see CrushWrapper::choose_args_get_with_fallback.
    let mut map = map_with_default_choose_arg();
    map.choose_args.insert(1, vec![None]);
    let mut default = Vec::new();
    let mut empty = Vec::new();
    let mut absent = Vec::new();
    crush_do_rule_with_choose_args(&map, 0, 2, &mut default, 1, &[1 << 16; 2], -1).unwrap();
    crush_do_rule_with_choose_args(&map, 0, 2, &mut empty, 1, &[1 << 16; 2], 1).unwrap();
    crush_do_rule_with_choose_args(&map, 0, 2, &mut absent, 1, &[1 << 16; 2], 99).unwrap();
    assert_eq!(default, vec![0]);
    assert_eq!(empty, vec![1]);
    assert_eq!(absent, default);

    assert_eq!(
        pg_to_osds(&map, PgId::new(1, 1), 64, 0, &[1 << 16; 2], 1, false).unwrap(),
        empty
    );
}

#[test]
fn replacement_ids_change_straw2_hashes_but_not_returned_items() {
    // Local C-mapper contract; replacement IDs are hash inputs, never topology
    // references. The returned value must remain a canonical bucket item.
    let mut map = map_with_default_choose_arg();
    map.choose_args.insert(
        2,
        vec![Some(CrushChooseArg {
            weight_set: vec![vec![1 << 16, 1 << 16]],
            ids: vec![-450, 30],
        })],
    );
    let mut base = Vec::new();
    let mut alternate = Vec::new();
    let mut changed = false;
    for x in 0..100 {
        crush_do_rule_with_choose_args(&map, 0, x, &mut base, 1, &[1 << 16; 2], 1).unwrap();
        crush_do_rule_with_choose_args(&map, 0, x, &mut alternate, 1, &[1 << 16; 2], 2).unwrap();
        assert!(matches!(alternate.as_slice(), [0 | 1]));
        changed |= base != alternate;
    }
    assert!(changed);
}

#[test]
fn decoder_rejects_text_and_truncated_choose_arg_data() {
    // Rust contract; no upstream analogue found. The related upstream CLI case
    // is src/test/cli/crushtool/check-invalid-map.t::cmd-01.
    assert!(
        CrushMap::decode(&mut Bytes::from_static(include_bytes!(
            "reference/choose-args-hosts.txt"
        )))
        .is_err()
    );
    let bytes = include_bytes!("reference/choose-args-quincy.crushmap");
    assert!(CrushMap::decode(&mut Bytes::from_static(&bytes[..bytes.len() - 1])).is_err());

    // These are fully readable producer-byte mutations. They name the exact
    // decoder guard, so a later EOF cannot accidentally make the test pass.
    // The one-entry compat map is smaller and has a stable final choose-arg
    // record: bucket-index follows its index, map-size and record-count.
    let compat = include_bytes!("reference/choose-args-compat-quincy.crushmap");
    let mut invalid_bucket = compat.to_vec();
    let bucket_index = invalid_bucket.len() - 20;
    assert_eq!(
        &invalid_bucket[bucket_index..bucket_index + 4],
        &[0, 0, 0, 0]
    );
    invalid_bucket[bucket_index] = 99;
    let error = CrushMap::decode(&mut Bytes::from(invalid_bucket)).unwrap_err();
    assert!(
        error.to_string().contains("invalid bucket index"),
        "{error}"
    );

    let mut invalid_weight = compat.to_vec();
    let weight_len = invalid_weight.len() - 12;
    assert_eq!(&invalid_weight[weight_len..weight_len + 4], &[1, 0, 0, 0]);
    invalid_weight[weight_len] = 0;
    let error = CrushMap::decode(&mut Bytes::from(invalid_weight)).unwrap_err();
    // The zero-length weight set is valid and normalized. Its old weight
    // remains on the wire and is now read as a malformed ID count.
    assert!(error.to_string().contains("ID length"));

    let mut invalid_ids = compat.to_vec();
    let ids_len = invalid_ids.len() - 4;
    assert_eq!(&invalid_ids[ids_len..ids_len + 4], &[0, 0, 0, 0]);
    invalid_ids[ids_len] = 2;
    invalid_ids.extend_from_slice(&123i32.to_le_bytes());
    invalid_ids.extend_from_slice(&456i32.to_le_bytes());
    let error = CrushMap::decode(&mut Bytes::from(invalid_ids)).unwrap_err();
    assert!(error.to_string().contains("ID length"));
}

#[test]
fn qa_choose_args_transition_maps_decode_original_assertions() {
    // Exact upstream state maps published by crush-choose-args.sh:
    // TEST_choose_args_update and TEST_no_update_weight_set.
    let update = decode_fixture(include_bytes!(
        "reference/qa-update-one-more-quincy.crushmap"
    ));
    let update_args = &update.choose_args[&0];
    assert_eq!(
        update_args[0].as_ref().unwrap().weight_set,
        vec![vec![5 << 16], vec![5 << 16]]
    );
    assert_eq!(update_args[0].as_ref().unwrap().ids, vec![-10]);
    assert_eq!(
        update_args[1].as_ref().unwrap().weight_set,
        vec![vec![2 << 16, 3 << 16], vec![2 << 16, 3 << 16]]
    );
    assert_eq!(update_args[1].as_ref().unwrap().ids, vec![-20, 1]);
    let update_tentacle = decode_fixture(include_bytes!(
        "reference/qa-update-one-more-tentacle.crushmap"
    ));
    assert_eq!(update.choose_args, update_tentacle.choose_args);

    let no_update = decode_fixture(include_bytes!(
        "reference/qa-no-update-one-more-quincy.crushmap"
    ));
    let no_update_args = &no_update.choose_args[&0];
    assert_eq!(
        no_update_args[0].as_ref().unwrap().weight_set,
        vec![vec![2 << 16], vec![1 << 16]]
    );
    assert_eq!(
        no_update_args[1].as_ref().unwrap().weight_set,
        vec![vec![2 << 16, 0], vec![1 << 16, 0]]
    );
    assert_eq!(no_update_args[1].as_ref().unwrap().ids, vec![-20, 1]);
    let no_update_tentacle = decode_fixture(include_bytes!(
        "reference/qa-no-update-one-more-tentacle.crushmap"
    ));
    assert_eq!(no_update.choose_args, no_update_tentacle.choose_args);

    // TEST_reweight and TEST_move_bucket publish tree totals rather than a
    // text map. These local, source-shaped final states retain those exact
    // canonical/compat totals and are decoded from pinned-C compiled bytes.
    let reweight = decode_fixture(include_bytes!(
        "reference/qa-reweight-final-quincy.crushmap"
    ));
    assert_eq!(reweight.get_bucket(-1).unwrap().weight, 10 << 16);
    assert_eq!(
        reweight.choose_args[&-1][0].as_ref().unwrap().weight_set,
        vec![vec![9 << 16]]
    );
    assert_eq!(
        reweight.choose_args[&-1][1].as_ref().unwrap().weight_set,
        vec![vec![2 << 16, 3 << 16, 4 << 16]]
    );
    let reweight_tentacle = decode_fixture(include_bytes!(
        "reference/qa-reweight-final-tentacle.crushmap"
    ));
    assert_eq!(reweight.choose_args, reweight_tentacle.choose_args);

    let moved = decode_fixture(include_bytes!("reference/qa-move-final-quincy.crushmap"));
    assert_eq!(moved.get_bucket(-3).unwrap().weight, 6 << 16);
    assert_eq!(
        moved.choose_args[&-1][0].as_ref().unwrap().weight_set,
        vec![vec![0, 3 << 16]]
    );
    assert_eq!(
        moved.choose_args[&-1][2].as_ref().unwrap().weight_set,
        vec![vec![3 << 16, 0]]
    );
    let moved_tentacle =
        decode_fixture(include_bytes!("reference/qa-move-final-tentacle.crushmap"));
    assert_eq!(moved.choose_args, moved_tentacle.choose_args);
}

#[test]
fn qa_choose_args_states_match_pinned_c_placement_vectors() {
    // `qa-choose-args-vectors.txt` records pinned crushtool vectors for each
    // decoded QA state. See its header for the index-0/default CLI adaptation.
    let maps = [
        (
            "update",
            include_bytes!("reference/qa-update-one-more-quincy.crushmap").as_slice(),
            0,
        ),
        (
            "no-update",
            include_bytes!("reference/qa-no-update-one-more-quincy.crushmap").as_slice(),
            0,
        ),
        (
            "reweight",
            include_bytes!("reference/qa-reweight-final-quincy.crushmap").as_slice(),
            -1,
        ),
        (
            "move",
            include_bytes!("reference/qa-move-final-quincy.crushmap").as_slice(),
            -1,
        ),
    ];
    for (name, bytes, index) in maps {
        let map = decode_fixture(bytes);
        for line in include_str!("reference/qa-choose-args-vectors.txt")
            .lines()
            .filter(|line| !line.starts_with('#'))
            .filter(|line| line.starts_with(name))
        {
            let fields: Vec<i32> = line
                .split_whitespace()
                .skip(1)
                .map(str::parse)
                .collect::<Result<_, _>>()
                .unwrap();
            let mut actual = Vec::new();
            crush_do_rule_with_choose_args(
                &map,
                0,
                fields[0] as u32,
                &mut actual,
                2,
                &[1 << 16; 3],
                index,
            )
            .unwrap();
            assert_eq!(actual, fields[1..], "{name} x={}", fields[0]);
        }
    }
}

#[test]
fn qa_choose_args_published_transition_inventory_is_complete() {
    // The upstream QA shell emits two exact text maps and grep assertions for
    // the remaining transitions. Keep every published intermediate here until
    // corresponding source-shaped compiled maps/vectors are added.
    let states: Vec<_> = include_str!("reference/qa-choose-args-published-states.txt")
        .lines()
        .filter(|line| !line.starts_with('#'))
        .collect();
    assert_eq!(states.len(), 16);
    for required in [
        "update-pre",
        "update-add",
        "update-remove",
        "no-update-pre",
        "no-update-add",
        "no-update-remove",
        "reweight-create",
        "reweight-compat-osd0",
        "reweight-add-osd2",
        "reweight-canonical-osd2",
        "reweight-compat-osd2",
        "move-create",
        "move-rack",
        "move-compat-osd0",
        "move-osd0-to-FOO",
        "move-osd1-to-FOO",
    ] {
        assert!(states.iter().any(|state| state.starts_with(required)));
    }
}

#[test]
fn qa_intermediate_states_match_pinned_c_placement_vectors() {
    let maps = [
        (
            "update-pre",
            include_bytes!("reference/qa-update-pre-quincy.crushmap").as_slice(),
            0,
        ),
        (
            "update-remove",
            include_bytes!("reference/qa-update-remove-quincy.crushmap").as_slice(),
            0,
        ),
        (
            "no-update-pre",
            include_bytes!("reference/qa-no-update-pre-quincy.crushmap").as_slice(),
            0,
        ),
        (
            "no-update-remove",
            include_bytes!("reference/qa-no-update-remove-quincy.crushmap").as_slice(),
            0,
        ),
        (
            "reweight-create",
            include_bytes!("reference/qa-reweight-create-quincy.crushmap").as_slice(),
            -1,
        ),
        (
            "reweight-compat-osd0",
            include_bytes!("reference/qa-reweight-compat-osd0-quincy.crushmap").as_slice(),
            -1,
        ),
        (
            "reweight-add-osd2",
            include_bytes!("reference/qa-reweight-add-osd2-quincy.crushmap").as_slice(),
            -1,
        ),
        (
            "reweight-canonical-osd2",
            include_bytes!("reference/qa-reweight-canonical-osd2-quincy.crushmap").as_slice(),
            -1,
        ),
        (
            "move-create",
            include_bytes!("reference/qa-move-create-quincy.crushmap").as_slice(),
            -1,
        ),
        (
            "move-rack",
            include_bytes!("reference/qa-move-rack-quincy.crushmap").as_slice(),
            -1,
        ),
        (
            "move-compat-osd0",
            include_bytes!("reference/qa-move-compat-osd0-quincy.crushmap").as_slice(),
            -1,
        ),
        (
            "move-osd0-to-foo",
            include_bytes!("reference/qa-move-osd0-to-foo-quincy.crushmap").as_slice(),
            -1,
        ),
    ];
    let mut index_zero_metadata_cases = 0;
    for (name, bytes, index) in maps {
        let map = decode_fixture(bytes);
        match name {
            "update-pre" | "update-remove" => {
                index_zero_metadata_cases += 1;
                assert!(
                    !map.choose_args.contains_key(&-1),
                    "{name} must not fall back"
                );
                let args = map.choose_args.get(&0).unwrap();
                assert_eq!(
                    args[0].as_ref().unwrap().weight_set,
                    vec![vec![2 << 16], vec![2 << 16]]
                );
                assert_eq!(args[0].as_ref().unwrap().ids, vec![-10]);
                assert_eq!(
                    args[1].as_ref().unwrap().weight_set,
                    vec![vec![2 << 16], vec![2 << 16]]
                );
                assert_eq!(args[1].as_ref().unwrap().ids, vec![-20]);
            }
            "no-update-pre" | "no-update-remove" => {
                index_zero_metadata_cases += 1;
                assert!(
                    !map.choose_args.contains_key(&-1),
                    "{name} must not fall back"
                );
                let args = map.choose_args.get(&0).unwrap();
                assert_eq!(
                    args[0].as_ref().unwrap().weight_set,
                    vec![vec![2 << 16], vec![1 << 16]]
                );
                assert_eq!(args[0].as_ref().unwrap().ids, vec![-10]);
                assert_eq!(
                    args[1].as_ref().unwrap().weight_set,
                    vec![vec![2 << 16], vec![1 << 16]]
                );
                assert_eq!(args[1].as_ref().unwrap().ids, vec![-20]);
            }
            _ => {}
        }
        let (bucket_id, canonical, alternate) = match name {
            "update-pre" | "update-remove" | "no-update-pre" | "no-update-remove" => (-2, 3, 2),
            "reweight-create" => (-2, 6, 6),
            "reweight-compat-osd0" => (-2, 6, 5),
            "reweight-add-osd2" => (-2, 9, 5),
            "reweight-canonical-osd2" => (-2, 10, 5),
            "move-create" | "move-rack" => (-4, 6, 4),
            "move-compat-osd0" => (-4, 6, 3),
            "move-osd0-to-foo" => (-4, 3, 2),
            _ => unreachable!(),
        };
        assert_eq!(
            map.get_bucket(bucket_id).unwrap().weight,
            (canonical as u32) << 16,
            "{name}"
        );
        let arg = map.choose_args[&index][(-1 - bucket_id) as usize]
            .as_ref()
            .unwrap();
        assert_eq!(
            arg.weight_set[0].iter().sum::<u32>(),
            (alternate as u32) << 16,
            "{name}"
        );
        for line in include_str!("reference/qa-intermediate-vectors.txt")
            .lines()
            .filter(|line| !line.starts_with('#'))
            .filter(|line| line.starts_with(name))
        {
            let fields: Vec<i32> = line
                .split_whitespace()
                .skip(1)
                .map(str::parse)
                .collect::<Result<_, _>>()
                .unwrap();
            let mut actual = Vec::new();
            crush_do_rule_with_choose_args(
                &map,
                0,
                fields[0] as u32,
                &mut actual,
                2,
                &[1 << 16; 3],
                index,
            )
            .unwrap();
            assert_eq!(actual, fields[1..], "{name} x={}", fields[0]);
        }
    }
    assert_eq!(index_zero_metadata_cases, 4);
}

fn choose_arg_vector_map() -> CrushMap {
    const W: u32 = 1 << 16;
    let bucket = |id: i32, bucket_type: i32, items: Vec<i32>, weights: Vec<u32>| CrushBucket {
        id,
        bucket_type,
        alg: BucketAlgorithm::Straw2,
        hash: 0,
        weight: weights.iter().sum(),
        size: items.len() as u32,
        items,
        data: BucketData::Straw2 {
            item_weights: weights,
        },
    };
    let mut map = CrushMap::new();
    map.max_devices = 4;
    map.max_buckets = 3;
    map.choose_local_tries = 0;
    map.choose_local_fallback_tries = 0;
    map.choose_total_tries = 50;
    map.chooseleaf_descend_once = 1;
    map.chooseleaf_vary_r = 1;
    map.chooseleaf_stable = 1;
    map.buckets = vec![
        Some(bucket(-1, 2, vec![-2, -3], vec![2 * W, 2 * W])),
        Some(bucket(-2, 1, vec![0, 1], vec![W, W])),
        Some(bucket(-3, 1, vec![2, 3], vec![W, W])),
    ];
    let step = |op, arg1, arg2| CrushRuleStep { op, arg1, arg2 };
    let rule = |rule_type, steps| {
        Some(CrushRule {
            rule_id: 0,
            rule_type,
            steps,
        })
    };
    map.rules = vec![rule(
        RuleType::Erasure,
        vec![
            step(RuleOp::Take, -1, 0),
            step(RuleOp::ChooseFirstN, 3, 0),
            step(RuleOp::Emit, 0, 0),
        ],
    )];
    let choose_arg = |weight_set, ids| CrushChooseArg { weight_set, ids };
    map.choose_args.insert(
        -1,
        vec![
            Some(choose_arg(
                vec![vec![2 * W, W], vec![W, 2 * W]],
                vec![-20, 30],
            )),
            Some(choose_arg(vec![vec![W, 0], vec![0, W]], vec![-450, 30])),
            Some(choose_arg(vec![vec![W, 0], vec![0, W]], vec![-20, -25])),
        ],
    );
    map
}

fn choose_arg_rule(map: &mut CrushMap, scenario: i32) {
    let step = |op, arg1, arg2| CrushRuleStep { op, arg1, arg2 };
    let (rule_type, steps) = match scenario {
        0 => (
            RuleType::Erasure,
            vec![
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseFirstN, 3, 0),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
        1 => (
            RuleType::Erasure,
            vec![
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseIndep, 3, 0),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
        2 => (
            RuleType::Erasure,
            vec![
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseLeafFirstN, 3, 1),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
        3 => (
            RuleType::Erasure,
            vec![
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseLeafIndep, 3, 1),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
        4 => (
            RuleType::Erasure,
            vec![
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseIndep, 2, 1),
                step(RuleOp::ChooseIndep, 1, 0),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
        5 => (
            RuleType::MsrIndep,
            vec![
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseMsr, 2, 1),
                step(RuleOp::ChooseMsr, 2, 0),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
        6 => (
            RuleType::MsrFirstN,
            vec![
                step(RuleOp::Take, -1, 0),
                step(RuleOp::ChooseMsr, 2, 1),
                step(RuleOp::ChooseMsr, 2, 0),
                step(RuleOp::Emit, 0, 0),
            ],
        ),
        _ => unreachable!(),
    };
    map.rules[0] = Some(CrushRule {
        rule_id: 0,
        rule_type,
        steps,
    });
}

#[test]
fn pinned_c_choose_argument_vectors_cover_all_rule_paths() {
    // Local generator linked against unmodified Quincy/Tentacle mapper.c/hash.c:
    // `python3 rados/tests/crush/reference/generate-choose-args-reference.py ../ceph --check`.
    // Scenarios are FIRSTN, INDEP, recursive CHOOSELEAF variants, chained
    // choices, and Tentacle's MSR path. Each uses position-dependent weights
    // and signed replacement IDs for every bucket.
    for line in include_str!("reference/choose-args-vectors.txt")
        .lines()
        .filter(|line| !line.starts_with('#'))
    {
        let fields: Vec<_> = line
            .split_whitespace()
            .map(str::parse::<i32>)
            .collect::<Result<_, _>>()
            .unwrap();
        let (scenario, mode, x, n) = (fields[0], fields[1], fields[2], fields[3] as usize);
        let expected = &fields[4..];
        assert_eq!(expected.len(), n);
        let mut map = choose_arg_vector_map();
        choose_arg_rule(&mut map, scenario);
        if mode == 0 {
            map.choose_args.clear();
        } else if mode == 1 {
            map.choose_args.insert(7, vec![None; 3]);
        } else {
            let selected = map.choose_args.remove(&-1).unwrap();
            map.choose_args.insert(7, selected);
        }
        let mut actual = Vec::new();
        let weights = [
            1 << 16,
            if scenario >= 5 { 0 } else { 1 << 16 },
            1 << 16,
            1 << 16,
        ];
        crush_do_rule_with_choose_args(&map, 0, x as u32, &mut actual, 3, &weights, 7).unwrap();
        assert_eq!(actual, expected, "scenario={scenario} mode={mode} x={x}");
    }
}

#[test]
fn pg_pool_index_uses_selected_default_absent_and_empty_choose_sets() {
    // Pinned C FIRSTN vectors prove the placement-facing caller's pool-index
    // selection after legacy `raw_pg_to_pps` adds the pool id to seed zero.
    let vectors: Vec<Vec<i32>> = include_str!("reference/choose-args-vectors.txt")
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| {
            let fields: Vec<i32> = line
                .split_whitespace()
                .map(str::parse)
                .collect::<Result<_, _>>()
                .ok()?;
            (fields[0] == 0).then_some(fields)
        })
        .collect();
    let expected_for = |mode, x| {
        vectors
            .iter()
            .find(|candidate| candidate[1] == mode && candidate[2] == x)
            .unwrap()[4..]
            .to_vec()
    };
    for (pool, mode, x) in [(7, 2, 7), (99, 2, 99), (1, 1, 1)] {
        let expected = expected_for(mode, x);
        let mut map = choose_arg_vector_map();
        let selected = map.choose_args.remove(&-1).unwrap();
        map.choose_args.insert(7, selected.clone());
        map.choose_args.insert(-1, selected);
        map.choose_args.insert(1, vec![None; 3]);
        assert_eq!(
            pg_to_osds(&map, PgId::new(pool, 0), 128, 0, &[1 << 16; 4], 3, false).unwrap(),
            expected,
            "pool={pool}"
        );
        map.choose_args.remove(&-1);
        if pool == 99 {
            assert_eq!(
                pg_to_osds(&map, PgId::new(pool, 0), 128, 0, &[1 << 16; 4], 3, false).unwrap(),
                expected_for(0, x),
                "absent x={x}"
            );
        }
    }
}
