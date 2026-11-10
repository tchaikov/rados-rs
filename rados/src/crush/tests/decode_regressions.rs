use super::*;

fn encode_map(buckets: &[Option<(BucketAlgorithm, &[i32])>]) -> Vec<u8> {
    let mut bytes = Vec::new();
    let max_devices = buckets
        .iter()
        .flatten()
        .flat_map(|(_, items)| items.iter().copied())
        .filter(|&item| item >= 0)
        .max()
        .map_or(0, |item| item + 1);
    CRUSH_MAGIC.encode(&mut bytes, 0).unwrap();
    (buckets.len() as i32).encode(&mut bytes, 0).unwrap();
    0u32.encode(&mut bytes, 0).unwrap(); // no rules
    max_devices.encode(&mut bytes, 0).unwrap();
    for (index, bucket) in buckets.iter().enumerate() {
        let Some((alg, items)) = bucket else {
            0u32.encode(&mut bytes, 0).unwrap();
            continue;
        };
        (*alg as u32).encode(&mut bytes, 0).unwrap();
        (-1 - index as i32).encode(&mut bytes, 0).unwrap();
        1u16.encode(&mut bytes, 0).unwrap();
        (*alg as u8).encode(&mut bytes, 0).unwrap();
        0u8.encode(&mut bytes, 0).unwrap(); // RJenkins1
        0u32.encode(&mut bytes, 0).unwrap(); // total weight
        items.to_vec().encode(&mut bytes, 0).unwrap();
        match alg {
            BucketAlgorithm::Uniform => 1u32.encode(&mut bytes, 0).unwrap(),
            BucketAlgorithm::Straw2 => {
                for _ in *items {
                    1u32.encode(&mut bytes, 0).unwrap();
                }
            }
            _ => unreachable!(),
        }
    }
    bytes.extend_from_slice(&[0; 12]); // name maps
    bytes.extend_from_slice(&[0; 23]); // tunables through chooseleaf_stable
    bytes.extend_from_slice(&[0; 12]); // class maps
    bytes
}

fn append_choose_args(bytes: &mut Vec<u8>, args: &[(u32, CrushChooseArg)]) {
    1u32.encode(bytes, 0).unwrap();
    (-1i64).encode(bytes, 0).unwrap();
    (args.len() as u32).encode(bytes, 0).unwrap();
    for (index, arg) in args {
        index.encode(bytes, 0).unwrap();
        arg.weight_set.encode(bytes, 0).unwrap();
        arg.ids.encode(bytes, 0).unwrap();
    }
}

#[test]
fn optional_sections_reject_every_partial_field_and_preserve_legacy_endings() {
    let mut bytes = encode_map(&[]);
    bytes.extend_from_slice(&[0; 12]); // empty choose args, then both MSR tunables
    // Header/name maps, retry trio, descend_once, vary_r, straw_calc_version,
    // allowed algorithms, stable, class maps, choose args, MSR tunables.
    let complete_sections = [28, 40, 44, 45, 46, 50, 51, 63, 67, 75];
    for len in 0..=bytes.len() {
        let result = CrushMap::decode(&mut Bytes::copy_from_slice(&bytes[..len]));
        assert_eq!(
            result.is_ok(),
            complete_sections.contains(&len),
            "len={len}"
        );
    }
}

#[test]
fn rule_mask_id_must_match_its_full_width_slot_index() {
    for (slot, ruleset_id) in [(0u32, 0u8), (1, 1), (1, 0), (256, 0)] {
        let mut bytes = encode_map(&[]);
        bytes[8..12].copy_from_slice(&(slot + 1).to_le_bytes());
        let mut rules = vec![0; slot as usize * 4]; // preceding empty slots
        1u32.encode(&mut rules, 0).unwrap(); // occupied slot
        0u32.encode(&mut rules, 0).unwrap(); // no steps
        rules.extend_from_slice(&[ruleset_id, RuleType::Replicated as u8, 1, 3]);
        bytes.splice(16..16, rules);
        let result = CrushMap::decode(&mut Bytes::from(bytes));
        assert_eq!(result.is_ok(), u32::from(ruleset_id) == slot, "slot={slot}");
    }
}

#[test]
fn choose_args_resize_weights_with_prefix_and_zero_tail() {
    let mut bytes = encode_map(&[Some((BucketAlgorithm::Straw2, &[0, 1, 2]))]);
    append_choose_args(
        &mut bytes,
        &[(
            0,
            CrushChooseArg {
                weight_set: vec![vec![65536, 65536], vec![1, 2, 3, 4], vec![]],
                ids: vec![-20, 30, -40],
            },
        )],
    );
    let map = CrushMap::decode(&mut Bytes::from(bytes)).unwrap();
    let arg = map.choose_args[&-1][0].as_ref().unwrap();
    assert_eq!(
        arg.weight_set,
        vec![vec![65536, 65536, 0], vec![1, 2, 3], vec![0, 0, 0]]
    );
    assert_eq!(arg.ids, vec![-20, 30, -40]);
}

#[test]
fn choose_args_remove_missing_and_non_straw2_buckets() {
    let mut bytes = encode_map(&[
        None,
        Some((BucketAlgorithm::Uniform, &[0])),
        Some((BucketAlgorithm::Straw2, &[1, 2])),
    ]);
    append_choose_args(
        &mut bytes,
        &[
            (
                0,
                CrushChooseArg {
                    weight_set: vec![vec![7]],
                    ids: vec![],
                },
            ),
            (
                1,
                CrushChooseArg {
                    weight_set: vec![vec![8, 9]],
                    ids: vec![100],
                },
            ),
            (
                2,
                CrushChooseArg {
                    weight_set: vec![vec![10]],
                    ids: vec![],
                },
            ),
        ],
    );
    let map = CrushMap::decode(&mut Bytes::from(bytes)).unwrap();
    assert!(map.choose_args[&-1][0].is_none());
    assert!(map.choose_args[&-1][1].is_none());
    assert_eq!(
        map.choose_args[&-1][2].as_ref().unwrap().weight_set,
        vec![vec![10, 0]]
    );
}

#[test]
fn choose_args_reject_invalid_indices_and_id_lengths() {
    for (bucket, index, ids) in [
        (None, 0, vec![0]),
        (Some((BucketAlgorithm::Straw2, &[0, 1][..])), 1, vec![]),
        (Some((BucketAlgorithm::Straw2, &[0, 1][..])), 0, vec![0]),
        (
            Some((BucketAlgorithm::Straw2, &[0, 1][..])),
            0,
            vec![0, 1, 2],
        ),
    ] {
        let mut bytes = encode_map(&[bucket]);
        append_choose_args(
            &mut bytes,
            &[(
                index,
                CrushChooseArg {
                    weight_set: vec![],
                    ids,
                },
            )],
        );
        assert!(CrushMap::decode(&mut Bytes::from(bytes)).is_err());
    }
}

#[test]
fn inconsistent_positions_do_not_accept_weights_ceph_leaves_unnormalized() {
    // Ceph infers positions before clearing arguments for deleted buckets.
    for weights in [vec![7], vec![7, 8]] {
        let mut bytes = encode_map(&[None, Some((BucketAlgorithm::Straw2, &[0, 1]))]);
        append_choose_args(
            &mut bytes,
            &[
                (
                    0,
                    CrushChooseArg {
                        weight_set: vec![vec![], vec![]],
                        ids: vec![],
                    },
                ),
                (
                    1,
                    CrushChooseArg {
                        weight_set: vec![weights.clone()],
                        ids: vec![],
                    },
                ),
            ],
        );
        let result = CrushMap::decode(&mut Bytes::from(bytes));
        assert_eq!(result.is_ok(), weights.len() == 2);
    }
}

#[test]
fn unsupported_bucket_hash_is_rejected() {
    let mut bytes = encode_map(&[Some((BucketAlgorithm::Straw2, &[0]))]);
    bytes[27] = 1;
    let error = CrushMap::decode(&mut Bytes::from(bytes)).unwrap_err();
    assert!(error.to_string().contains("hash type"), "{error}");
}

#[test]
fn malformed_counts_are_rejected_before_allocation() {
    for (offset, count) in [(4, -1i32), (4, i32::MAX), (8, -1), (12, -1)] {
        let mut bytes = encode_map(&[]);
        bytes[offset..offset + 4].copy_from_slice(&count.to_le_bytes());
        assert!(CrushMap::decode(&mut Bytes::from(bytes)).is_err());
    }
    for item_size in [4, 8, 12] {
        assert!(decode_n::<u32>(&mut Bytes::new(), u32::MAX as usize, item_size).is_err());
    }
    let mut bytes = encode_map(&[Some((BucketAlgorithm::Straw2, &[0]))]);
    bytes[32..36].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(CrushMap::decode(&mut Bytes::from(bytes)).is_err());

    let mut rule = Vec::new();
    u32::MAX.encode(&mut rule, 0).unwrap();
    rule.extend_from_slice(&[0, RuleType::Replicated as u8, 1, 3]);
    assert!(decode_rule(&mut Bytes::from(rule)).is_err());

    let mut bytes = encode_map(&[Some((BucketAlgorithm::Straw2, &[0]))]);
    append_choose_args(&mut bytes, &[(0, CrushChooseArg::default())]);
    let positions = bytes.len() - 8;
    bytes[positions..positions + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(CrushMap::decode(&mut Bytes::from(bytes)).is_err());
}

#[test]
fn data_backed_bucket_and_position_counts_can_exceed_ten_thousand() {
    let items: Vec<i32> = (0..10_001).collect();
    let bytes = encode_map(&[Some((BucketAlgorithm::Straw2, &items))]);
    let map = CrushMap::decode(&mut Bytes::from(bytes)).unwrap();
    assert_eq!(map.buckets[0].as_ref().unwrap().size, 10_001);

    let mut bytes = encode_map(&[Some((BucketAlgorithm::Straw2, &[]))]);
    append_choose_args(
        &mut bytes,
        &[(
            0,
            CrushChooseArg {
                weight_set: vec![vec![]; 10_001],
                ids: vec![],
            },
        )],
    );
    let map = CrushMap::decode(&mut Bytes::from(bytes)).unwrap();
    assert_eq!(
        map.choose_args[&-1][0].as_ref().unwrap().weight_set.len(),
        10_001
    );
}

#[test]
fn cycles_are_rejected_but_shared_children_and_deep_hierarchies_are_valid() {
    for buckets in [
        vec![Some((BucketAlgorithm::Uniform, &[-1][..]))],
        vec![
            Some((BucketAlgorithm::Uniform, &[-2][..])),
            Some((BucketAlgorithm::Uniform, &[-1][..])),
        ],
    ] {
        let error = CrushMap::decode(&mut Bytes::from(encode_map(&buckets))).unwrap_err();
        assert!(error.to_string().contains("cycle"), "{error}");
    }
    let dag = [
        Some((BucketAlgorithm::Uniform, &[-2, -3][..])),
        Some((BucketAlgorithm::Uniform, &[-3][..])),
        Some((BucketAlgorithm::Uniform, &[0][..])),
    ];
    CrushMap::decode(&mut Bytes::from(encode_map(&dag))).unwrap();

    let mut items: Vec<[i32; 1]> = (0..20_000).map(|i| [-2 - i]).collect();
    *items.last_mut().unwrap() = [0];
    let buckets: Vec<_> = items
        .iter()
        .map(|items| Some((BucketAlgorithm::Uniform, &items[..])))
        .collect();
    CrushMap::decode(&mut Bytes::from(encode_map(&buckets))).unwrap();
}
