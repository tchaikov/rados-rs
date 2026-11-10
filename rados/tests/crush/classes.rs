use bytes::Bytes;
use rados::crush::{
    BucketData, CrushError, CrushMap, RuleOp, RuleType, crush_do_rule_with_choose_args,
};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

const QUINCY: &[u8] = include_bytes!("reference/device-class-quincy.crushmap");
const TENTACLE: &[u8] = include_bytes!("reference/device-class-tentacle.crushmap");
const VECTORS: &str = include_str!("reference/device-class-vectors.txt");

fn decode(bytes: &'static [u8]) -> CrushMap {
    CrushMap::decode(&mut Bytes::from_static(bytes)).unwrap()
}

fn assert_straw_bucket(
    map: &CrushMap,
    id: i32,
    bucket_type: i32,
    weight: u32,
    items: &[i32],
    weights: &[u32],
) {
    let bucket = map.get_bucket(id).unwrap();
    assert_eq!(
        (bucket.id, bucket.bucket_type, bucket.weight, bucket.size),
        (id, bucket_type, weight, items.len() as u32)
    );
    assert_eq!(bucket.items, items);
    assert!(
        matches!(&bucket.data, BucketData::Straw { item_weights, .. } | BucketData::Straw2 { item_weights } if item_weights == weights)
    );
}

fn c_vectors() -> impl Iterator<Item = (u32, u32, Vec<i32>)> {
    VECTORS.lines().map(|line| {
        let fields: Vec<_> = line.split_whitespace().collect();
        let rule = fields[2].parse().unwrap();
        let x = fields[4].parse().unwrap();
        let items = line
            .split_once('[')
            .unwrap()
            .1
            .trim_end_matches(']')
            .split(',')
            .filter(|item| !item.is_empty())
            .map(|item| item.trim().parse().unwrap())
            .collect();
        (rule, x, items)
    })
}

#[test]
fn device_class_fixture_keeps_all_shadows_and_class_rules() {
    // Upstream: v17.2.7/src/test/cli/crushtool/device-class.t::locally assigned compiled-map consumer state (cmd-02)
    // Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/device-class.t#L2
    // Upstream: v20.2.4/src/test/cli/crushtool/device-class.t::locally assigned compiled-map consumer state (cmd-02)
    // Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/device-class.t#L2
    for bytes in [QUINCY, TENTACLE] {
        let map = decode(bytes);
        assert_eq!(map.class_name.get(&0).map(String::as_str), Some("ssd"));
        assert_eq!(map.class_name.get(&1).map(String::as_str), Some("hdd"));
        assert_eq!(map.get_class_id("ssd"), Some(0));
        assert_eq!(map.get_class_id("hdd"), Some(1));
        assert_eq!(map.class_map.len(), 13);
        for (id, class) in [
            (0, "ssd"),
            (1, "ssd"),
            (2, "hdd"),
            (-6, "ssd"),
            (-7, "ssd"),
            (-8, "ssd"),
            (-9, "ssd"),
            (-10, "ssd"),
            (-11, "hdd"),
            (-12, "hdd"),
            (-13, "hdd"),
            (-14, "hdd"),
            (-15, "hdd"),
        ] {
            assert_eq!(map.get_device_class(id), Some(class));
        }
        assert_eq!(
            map.class_bucket,
            [
                (-1, [(0, -6), (1, -11)]),
                (-2, [(0, -7), (1, -12)]),
                (-3, [(0, -9), (1, -14)]),
                (-4, [(0, -10), (1, -15)]),
                (-5, [(0, -8), (1, -13)]),
            ]
            .into_iter()
            .map(|(bucket, classes)| (bucket, classes.into_iter().collect()))
            .collect()
        );
        for (id, name) in [
            (-6, "host0~ssd"),
            (-7, "host1~ssd"),
            (-8, "host2~ssd"),
            (-9, "rack0~ssd"),
            (-10, "root~ssd"),
            (-11, "host0~hdd"),
            (-12, "host1~hdd"),
            (-13, "host2~hdd"),
            (-14, "rack0~hdd"),
            (-15, "root~hdd"),
        ] {
            assert_eq!(map.names.get(&id).map(String::as_str), Some(name));
        }
        for (id, bucket_type, weight, items, weights) in [
            (-6, 1, 65536, &[0][..], &[65536][..]),
            (-7, 1, 65536, &[1][..], &[65536][..]),
            (-8, 1, 0, &[][..], &[][..]),
            (-9, 2, 131072, &[-6, -7, -8][..], &[65536, 65536, 0][..]),
            (-10, 3, 131072, &[-9][..], &[131072][..]),
            (-11, 1, 0, &[][..], &[][..]),
            (-12, 1, 0, &[][..], &[][..]),
            (-13, 1, 65536, &[2][..], &[65536][..]),
            (-14, 2, 65536, &[-11, -12, -13][..], &[0, 0, 65536][..]),
            (-15, 3, 65536, &[-14][..], &[65536][..]),
            (-4, 3, 262144, &[-3][..], &[262144][..]),
        ] {
            assert_straw_bucket(&map, id, bucket_type, weight, items, weights);
        }
        for (rule_id, take) in [(1, -10), (2, -15), (3, -4)] {
            let rule = map.get_rule(rule_id).unwrap();
            assert_eq!(rule.rule_type, RuleType::Replicated);
            assert_eq!(rule.steps.len(), 3);
            assert_eq!((rule.steps[0].op, rule.steps[0].arg1), (RuleOp::Take, take));
            assert_eq!(
                (rule.steps[1].op, rule.steps[1].arg1, rule.steps[1].arg2),
                (RuleOp::ChooseLeafFirstN, 0, 2)
            );
            assert_eq!(rule.steps[2].op, RuleOp::Emit);
        }
    }
}

#[test]
fn device_class_shadow_lookup_matches_split_id_class_consumer_behavior() {
    // Upstream: v17.2.7/src/test/crush/CrushWrapper.cc::CrushWrapperTest.split_id_class
    // Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/crush/CrushWrapper.cc#L1197
    // Upstream: v20.2.4/src/test/crush/CrushWrapper.cc::CrushWrapperTest.split_id_class
    // Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/CrushWrapper.cc#L1194
    for bytes in [QUINCY, TENTACLE] {
        let map = decode(bytes);
        assert_eq!(map.split_id_class(-10).unwrap(), (-4, Some(0)));
        assert_eq!(map.split_id_class(-15).unwrap(), (-4, Some(1)));
        assert_eq!(map.split_id_class(-4).unwrap(), (-4, None));
        assert!(matches!(
            map.split_id_class(-16),
            Err(CrushError::ItemNotFound(-16))
        ));
    }
}

#[test]
fn device_class_rules_match_pinned_c_vectors_and_membership() {
    // Rust contract; local C differential against original device-class.crush,
    // rules 1/2, x=0..19 and num-rep=3. Vectors come from both pinned releases.
    // Sources: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/device-class.crush#L1
    // and https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/device-class.crush#L1
    for bytes in [QUINCY, TENTACLE] {
        let map = decode(bytes);
        let mut rows = 0;
        for (rule, x, expected) in c_vectors() {
            let mut actual = Vec::new();
            crush_do_rule_with_choose_args(&map, rule, x, &mut actual, 3, &[65536; 3], -1).unwrap();
            assert_eq!(actual, expected, "rule={rule} x={x}");
            let class = if rule == 1 { "ssd" } else { "hdd" };
            assert!(actual.iter().all(|&id| map.device_has_class(id, class)));
            rows += 1;
        }
        assert_eq!(rows, 40);
    }
}

fn reclassify_map(case: &str, state: &str, release: &str) -> &'static [u8] {
    macro_rules! maps {
        ($case:literal) => {
            match (state, release) {
                ("before", "quincy") => include_bytes!(concat!(
                    "reference/reclassify-",
                    $case,
                    "-before-quincy.crushmap"
                )),
                ("after", "quincy") => include_bytes!(concat!(
                    "reference/reclassify-",
                    $case,
                    "-after-quincy.crushmap"
                )),
                ("before", "tentacle") => include_bytes!(concat!(
                    "reference/reclassify-",
                    $case,
                    "-before-tentacle.crushmap"
                )),
                ("after", "tentacle") => include_bytes!(concat!(
                    "reference/reclassify-",
                    $case,
                    "-after-tentacle.crushmap"
                )),
                _ => unreachable!(),
            }
        };
    }
    match case {
        "a" => maps!("a"),
        "d" => maps!("d"),
        "e" => maps!("e"),
        "c" => maps!("c"),
        "beesly" => maps!("beesly"),
        "flax" => maps!("flax"),
        "gabe2" => maps!("gabe2"),
        "b" => maps!("b"),
        "f" => maps!("f"),
        "g" => maps!("g"),
        _ => unreachable!(),
    }
}

fn mapping_digest(map: &CrushMap, rule: u32, replicas: u32) -> (String, Vec<Vec<i32>>) {
    let mut digest = Sha256::new();
    let mut rows = Vec::new();
    let weights = vec![65536; map.max_devices as usize];
    for x in 0..1024 {
        let mut actual = Vec::new();
        crush_do_rule_with_choose_args(map, rule, x, &mut actual, replicas as usize, &weights, -1)
            .unwrap();
        digest.update((actual.len() as u32).to_be_bytes());
        for item in &actual {
            digest.update(item.to_be_bytes());
        }
        rows.push(actual);
    }
    (format!("{:x}", digest.finalize()), rows)
}

#[test]
fn reclassify_before_after_maps_match_pinned_c_complete_workload() {
    // Upstream: v17.2.7/src/test/cli/crushtool/reclassify.t::locally assigned successful pairs cmd-01/02,03/04,05/06,07/08,09/10,11/12,14/15,16/17,18/19,20/21
    // Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/reclassify.t#L1
    // Upstream: v20.2.4/src/test/cli/crushtool/reclassify.t::locally assigned successful pairs cmd-01/02,03/04,05/06,07/08,09/10,11/12,14/15,16/17,18/19,20/21
    // Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/reclassify.t#L1
    let cases: &[(&str, &[u32], &[usize])] = &[
        ("a", &[0, 1], &[0, 0]),
        ("d", &[0, 1], &[0, 0]),
        ("e", &[0, 1], &[6540, 8417]),
        ("c", &[0, 1, 2], &[158, 138, 0]),
        ("beesly", &[0, 1, 2, 4], &[0, 0, 0, 0]),
        ("flax", &[0], &[0]),
        ("gabe2", &[0, 1], &[627, 652]),
        ("b", &[0, 1], &[0, 0]),
        ("f", &[0, 1], &[627, 652]),
        ("g", &[0, 1], &[0, 0]),
    ];
    let vectors: HashMap<_, _> = include_str!("reference/reclassify-vectors.txt")
        .lines()
        .map(|line| {
            let fields: Vec<_> = line.split_whitespace().collect();
            assert_eq!(fields.len(), 7, "{line}");
            (
                (
                    fields[0],
                    fields[1],
                    fields[2],
                    fields[3].parse::<u32>().unwrap(),
                    fields[4].parse::<u32>().unwrap(),
                ),
                (fields[5].parse::<usize>().unwrap(), fields[6]),
            )
        })
        .collect();
    assert_eq!(vectors.len(), 880);
    for release in ["quincy", "tentacle"] {
        for &(case, rules, mismatches) in cases {
            for (rule, expected_mismatches) in rules.iter().zip(mismatches) {
                let mut states = HashMap::new();
                for state in ["before", "after"] {
                    let map = decode(reclassify_map(case, state, release));
                    let mut complete = Vec::new();
                    for replicas in 1..=10 {
                        let (actual, rows) = mapping_digest(&map, *rule, replicas);
                        let &(count, expected) = vectors
                            .get(&(release, case, state, *rule, replicas))
                            .unwrap_or_else(|| {
                                panic!("missing {release} {case} {state} {rule} {replicas}")
                            });
                        assert_eq!(count, rows.len());
                        assert_eq!(
                            actual, expected,
                            "{release} {case} {state} rule={rule} replicas={replicas}"
                        );
                        complete.extend(rows);
                    }
                    assert_eq!(complete.len(), 10240);
                    states.insert(state, complete);
                }
                assert_eq!(
                    states["before"]
                        .iter()
                        .zip(&states["after"])
                        .filter(|(before, after)| before != after)
                        .count(),
                    *expected_mismatches,
                    "{release} {case} rule={rule}"
                );
            }
        }
    }
}

fn mon_classes_map(state: &str, release: &str) -> &'static [u8] {
    match (state, release) {
        ("removed", "quincy") => include_bytes!("reference/mon-classes-removed-quincy.crushmap"),
        ("asdf", "quincy") => include_bytes!("reference/mon-classes-asdf-quincy.crushmap"),
        ("abc", "quincy") => include_bytes!("reference/mon-classes-abc-quincy.crushmap"),
        ("class2", "quincy") => include_bytes!("reference/mon-classes-class2-quincy.crushmap"),
        ("removed", "tentacle") => {
            include_bytes!("reference/mon-classes-removed-tentacle.crushmap")
        }
        ("asdf", "tentacle") => include_bytes!("reference/mon-classes-asdf-tentacle.crushmap"),
        ("abc", "tentacle") => include_bytes!("reference/mon-classes-abc-tentacle.crushmap"),
        ("class2", "tentacle") => include_bytes!("reference/mon-classes-class2-tentacle.crushmap"),
        _ => unreachable!(),
    }
}

fn assert_class_rule(map: &CrushMap, rule: u32, name: &str, take: i32) {
    let rule = map.get_rule(rule).unwrap();
    assert_eq!(
        map.rule_names.get(&rule.rule_id).map(String::as_str),
        Some(name)
    );
    assert_eq!(rule.rule_type, RuleType::Replicated);
    assert_eq!(rule.steps.len(), 3);
    assert_eq!((rule.steps[0].op, rule.steps[0].arg1), (RuleOp::Take, take));
    assert_eq!(
        (rule.steps[1].op, rule.steps[1].arg1, rule.steps[1].arg2),
        (RuleOp::ChooseLeafFirstN, 0, 1)
    );
    assert_eq!(rule.steps[2].op, RuleOp::Emit);
}

fn assert_default_rule(map: &CrushMap) {
    let rule = map.get_rule(0).unwrap();
    assert_eq!(
        map.rule_names.get(&0).map(String::as_str),
        Some("replicated_rule")
    );
    assert_eq!(rule.rule_type, RuleType::Replicated);
    assert_eq!(rule.steps.len(), 3);
    assert_eq!((rule.steps[0].op, rule.steps[0].arg1), (RuleOp::Take, -1));
    assert_eq!(
        (rule.steps[1].op, rule.steps[1].arg1, rule.steps[1].arg2),
        (RuleOp::ChooseFirstN, 0, 0)
    );
    assert_eq!(rule.steps[2].op, RuleOp::Emit);
}

fn assert_complete_class_map(map: &CrushMap, devices: &[(i32, i32)]) {
    let expected = map
        .class_bucket
        .values()
        .flat_map(|shadows| shadows.iter().map(|(&class, &shadow)| (shadow, class)))
        .chain(devices.iter().copied())
        .collect();
    assert_eq!(map.class_map, expected);
    for (&base, shadows) in &map.class_bucket {
        for (&class, &shadow) in shadows {
            assert_eq!(map.split_id_class(shadow).unwrap(), (base, Some(class)));
            assert_eq!(
                map.names.get(&shadow),
                Some(&format!("{}~{}", map.names[&base], map.class_name[&class]))
            );
        }
    }
}

#[test]
fn mon_classes_retained_maps_keep_lifecycle_metadata_and_filtered_placements() {
    // Upstream: v17.2.7/qa/standalone/crush/crush-classes.sh::TEST_mon_classes
    // Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/qa/standalone/crush/crush-classes.sh#L166
    // Upstream: v20.2.4/qa/standalone/crush/crush-classes.sh::TEST_mon_classes
    // Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/qa/standalone/crush/crush-classes.sh#L166
    for release in ["quincy", "tentacle"] {
        let removed = decode(mon_classes_map("removed", release));
        assert!(
            removed.class_name.is_empty()
                && removed.class_map.is_empty()
                && removed.class_bucket.is_empty()
        );
        assert!(removed.choose_args.is_empty());
        assert_default_rule(&removed);

        let asdf = decode(mon_classes_map("asdf", release));
        assert_eq!(
            asdf.class_name,
            [(0, "asdf".to_string())].into_iter().collect()
        );
        assert_eq!(asdf.class_map, [(-3, 0), (-4, 0)].into_iter().collect());
        assert_eq!(
            asdf.class_bucket,
            [
                (-2, [(0, -3)].into_iter().collect()),
                (-1, [(0, -4)].into_iter().collect())
            ]
            .into_iter()
            .collect()
        );
        assert_eq!(asdf.names.get(&-3).map(String::as_str), Some("colima~asdf"));
        assert_eq!(
            asdf.names.get(&-4).map(String::as_str),
            Some("default~asdf")
        );
        assert_straw_bucket(&asdf, -3, 1, 0, &[], &[]);
        assert_straw_bucket(&asdf, -4, 11, 0, &[-3], &[0]);
        assert_complete_class_map(&asdf, &[]);
        assert!(asdf.choose_args.is_empty());
        assert_class_rule(&asdf, 1, "asdf-rule", -4);

        let abc = decode(mon_classes_map("abc", release));
        assert_eq!(
            abc.class_name,
            [
                (0, "asdf".to_string()),
                (1, "abc".to_string()),
                (2, "hdd".to_string())
            ]
            .into_iter()
            .collect()
        );
        assert_eq!(abc.get_device_class(0), Some("hdd"));
        assert_eq!(abc.get_device_class(1), None);
        assert_eq!(abc.get_device_class(2), Some("abc"));
        assert_eq!(abc.class_bucket.len(), 5);
        for (base, shadows) in [
            (-1, [(0, -4), (1, -15), (2, -20)]),
            (-2, [(0, -3), (1, -14), (2, -19)]),
            (-7, [(0, -10), (1, -5), (2, -16)]),
            (-8, [(0, -11), (1, -6), (2, -17)]),
            (-9, [(0, -12), (1, -13), (2, -18)]),
        ] {
            assert_eq!(
                abc.class_bucket.get(&base),
                Some(&shadows.into_iter().collect())
            );
        }
        for (id, name) in [
            (-5, "foo-host~abc"),
            (-6, "foo-rack~abc"),
            (-13, "foo~abc"),
            (-15, "default~abc"),
            (-20, "default~hdd"),
        ] {
            assert_eq!(abc.names.get(&id).map(String::as_str), Some(name));
        }
        assert_straw_bucket(&abc, -5, 1, 65, &[2], &[65]);
        assert_straw_bucket(&abc, -6, 3, 65, &[-5], &[65]);
        assert_straw_bucket(&abc, -13, 11, 65, &[-6], &[65]);
        for (id, weight, items, weights) in [
            (-3, 0, &[][..], &[][..]),
            (-4, 0, &[-3][..], &[0][..]),
            (-5, 65, &[2][..], &[65][..]),
            (-6, 65, &[-5][..], &[65][..]),
            (-10, 0, &[][..], &[][..]),
            (-11, 0, &[-10][..], &[0][..]),
            (-12, 0, &[-11][..], &[0][..]),
            (-13, 65, &[-6][..], &[65][..]),
            (-14, 0, &[][..], &[][..]),
            (-15, 0, &[-14][..], &[0][..]),
            (-16, 0, &[][..], &[][..]),
            (-17, 0, &[-16][..], &[0][..]),
            (-18, 0, &[-17][..], &[0][..]),
            (-19, 65, &[0][..], &[65][..]),
            (-20, 65, &[-19][..], &[65][..]),
        ] {
            let bucket = abc.get_bucket(id).unwrap();
            assert_eq!(bucket.weight, weight);
            assert_eq!(bucket.items, items);
            assert!(
                matches!(&bucket.data, BucketData::Straw2 { item_weights } if item_weights == weights)
            );
        }
        assert_complete_class_map(&abc, &[(0, 2), (2, 1)]);
        assert!(abc.choose_args.is_empty());
        assert_class_rule(&abc, 1, "asdf-rule", -4);
        assert_class_rule(&abc, 2, "foo-rule", -13);

        let class2 = decode(mon_classes_map("class2", release));
        assert_eq!(
            class2.class_name,
            [
                (0, "asdf".to_string()),
                (1, "abc".to_string()),
                (2, "class_2".to_string())
            ]
            .into_iter()
            .collect()
        );
        assert_eq!(
            (
                class2.get_device_class(0),
                class2.get_device_class(1),
                class2.get_device_class(2)
            ),
            (Some("class_2"), Some("class_2"), Some("class_2"))
        );
        assert_eq!(class2.class_bucket, abc.class_bucket);
        for (id, name) in [
            (-16, "foo-host~class_2"),
            (-17, "foo-rack~class_2"),
            (-18, "foo~class_2"),
            (-20, "default~class_2"),
        ] {
            assert_eq!(class2.names.get(&id).map(String::as_str), Some(name));
        }
        assert_straw_bucket(&class2, -16, 1, 65, &[2], &[65]);
        assert_straw_bucket(&class2, -17, 3, 65, &[-16], &[65]);
        assert_straw_bucket(&class2, -18, 11, 65, &[-17], &[65]);
        assert_straw_bucket(&class2, -19, 1, 130, &[0, 1], &[65, 65]);
        assert_straw_bucket(&class2, -20, 11, 130, &[-19], &[130]);
        for (id, kind, weight, items, weights) in [
            (-3, 1, 0, &[][..], &[][..]),
            (-4, 11, 0, &[-3][..], &[0][..]),
            (-5, 1, 0, &[][..], &[][..]),
            (-6, 3, 0, &[-5][..], &[0][..]),
            (-10, 1, 0, &[][..], &[][..]),
            (-11, 3, 0, &[-10][..], &[0][..]),
            (-12, 11, 0, &[-11][..], &[0][..]),
            (-13, 11, 0, &[-6][..], &[0][..]),
            (-14, 1, 0, &[][..], &[][..]),
            (-15, 11, 0, &[-14][..], &[0][..]),
            (-16, 1, 65, &[2][..], &[65][..]),
            (-17, 3, 65, &[-16][..], &[65][..]),
            (-18, 11, 65, &[-17][..], &[65][..]),
            (-19, 1, 130, &[0, 1][..], &[65, 65][..]),
            (-20, 11, 130, &[-19][..], &[130][..]),
        ] {
            assert_straw_bucket(&class2, id, kind, weight, items, weights);
        }
        assert_complete_class_map(&class2, &[(0, 2), (1, 2), (2, 2)]);
        assert!(class2.choose_args.is_empty());
        assert_eq!(class2.rules.len(), 4);
        assert_eq!(
            class2
                .rules
                .iter()
                .flatten()
                .map(|rule| rule.rule_id)
                .collect::<Vec<_>>(),
            [0, 1, 2, 3]
        );
        assert_eq!(class2.rule_names.len(), 4);
        assert_default_rule(&class2);
        assert_class_rule(&class2, 1, "asdf-rule", -4);
        assert_class_rule(&class2, 2, "foo-rule", -13);
        assert_class_rule(&class2, 3, "class_1_rule", -20);
    }

    let mut rows = 0;
    for line in include_str!("reference/mon-classes-vectors.txt").lines() {
        let (prefix, values) = line.split_once(" [").unwrap();
        let fields: Vec<_> = prefix.split_whitespace().collect();
        let release = fields[0];
        let state = fields[1];
        let rule = fields[2].parse().unwrap();
        let x = fields[3].parse().unwrap();
        let expected = values
            .trim_end_matches(']')
            .split_whitespace()
            .map(str::parse)
            .collect::<Result<Vec<i32>, _>>()
            .unwrap();
        let map = decode(mon_classes_map(state, release));
        let mut actual = Vec::new();
        crush_do_rule_with_choose_args(
            &map,
            rule,
            x,
            &mut actual,
            3,
            &vec![65536; map.max_devices as usize],
            -1,
        )
        .unwrap();
        assert_eq!(actual, expected, "{release} {state} rule={rule} x={x}");
        let (_, Some(class)) = map
            .split_id_class(map.get_rule(rule).unwrap().steps[0].arg1)
            .unwrap()
        else {
            panic!("class rule {rule}")
        };
        let class = map.class_name.get(&class).unwrap();
        assert!(actual.iter().all(|&id| map.device_has_class(id, class)));
        rows += 1;
    }
    assert_eq!(rows, 240);
}
