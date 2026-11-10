use bytes::Bytes;
use rados::crush::{BucketAlgorithm, BucketData, CrushMap, mapper::crush_do_rule};
use std::collections::HashMap;

const W: u32 = 65536;
const VECTORS: &str = include_str!("reference/weight-topology-vectors.txt");

fn fixture(release: &str, case: &str) -> &'static [u8] {
    macro_rules! maps {
        ($case:literal) => {
            match release {
                "quincy" => include_bytes!(concat!(
                    "reference/weight-topology-",
                    $case,
                    "-quincy.crushmap"
                )),
                "tentacle" => include_bytes!(concat!(
                    "reference/weight-topology-",
                    $case,
                    "-tentacle.crushmap"
                )),
                _ => unreachable!(),
            }
        };
    }
    match case {
        "adjust-before" => maps!("adjust-before"),
        "adjust-item-after" => maps!("adjust-item-after"),
        "adjust-subtree-after" => maps!("adjust-subtree-after"),
        "multitype-after" => maps!("multitype-after"),
        "multitree-after" => maps!("multitree-after"),
        "crushdiff-before" => maps!("crushdiff-before"),
        "crushdiff-after" => maps!("crushdiff-after"),
        "added-straw" => maps!("added-straw"),
        _ => unreachable!(),
    }
}

fn decode(release: &str, case: &str) -> CrushMap {
    CrushMap::decode(&mut Bytes::from_static(fixture(release, case))).unwrap()
}

fn weights(map: &CrushMap, id: i32) -> &[u32] {
    match &map.get_bucket(id).unwrap().data {
        BucketData::Straw { item_weights, .. }
        | BucketData::Straw2 { item_weights }
        | BucketData::List { item_weights, .. } => item_weights,
        BucketData::Uniform { item_weight } => std::slice::from_ref(item_weight),
        BucketData::Tree { .. } => panic!("tree weights are not item weights"),
    }
}

fn mappings(map: &CrushMap, replicas: usize, rows: usize) -> Vec<Vec<i32>> {
    let device_weights = vec![W; map.max_devices as usize];
    (0..rows as u32)
        .map(|x| {
            let mut actual = Vec::new();
            crush_do_rule(map, 0, x, &mut actual, replicas, &device_weights).unwrap();
            actual
        })
        .collect()
}

fn expected_rows(case: &str) -> usize {
    match case {
        "crushdiff-before" | "crushdiff-after" => 1000,
        "adjust-before"
        | "adjust-item-after"
        | "adjust-subtree-after"
        | "multitype-after"
        | "multitree-after"
        | "added-straw" => 128,
        _ => panic!("unknown weight/topology case {case}"),
    }
}

fn parse_manifest(input: &str) -> HashMap<(&str, &str), Vec<Vec<i32>>> {
    let mut mappings = HashMap::new();
    let mut movements = HashMap::new();
    for line in input
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
    {
        let fields: Vec<_> = line.split('|').collect();
        if fields.first() == Some(&"movement") {
            assert_eq!(fields.len(), 4, "malformed movement: {line}");
            assert!(
                matches!(fields[1], "quincy" | "tentacle"),
                "unknown movement: {line}"
            );
            assert_eq!(fields[2], "crushdiff", "unknown movement: {line}");
            let moved = fields[3].parse().expect("invalid movement count");
            assert_eq!(
                movements.insert(fields[1], moved),
                None,
                "duplicate movement: {line}"
            );
            continue;
        }
        assert_eq!(fields.len(), 4, "malformed mapping: {line}");
        assert!(
            matches!(fields[0], "quincy" | "tentacle"),
            "unknown mapping: {line}"
        );
        let expected = expected_rows(fields[1]);
        let x = fields[2].parse::<usize>().expect("invalid mapping x");
        let rows = mappings
            .entry((fields[0], fields[1]))
            .or_insert_with(Vec::new);
        assert_eq!(x, rows.len(), "non-sequential mapping x: {line}");
        assert!(rows.len() < expected, "too many mapping rows: {line}");
        rows.push(
            fields[3]
                .split(',')
                .filter(|item| !item.is_empty())
                .map(|item| item.parse().expect("invalid mapping item"))
                .collect(),
        );
    }
    for release in ["quincy", "tentacle"] {
        for case in [
            "adjust-before",
            "adjust-item-after",
            "adjust-subtree-after",
            "multitype-after",
            "multitree-after",
            "crushdiff-before",
            "crushdiff-after",
            "added-straw",
        ] {
            assert_eq!(
                mappings.get(&(release, case)).map(Vec::len),
                Some(expected_rows(case)),
                "missing mapping rows: {release} {case}"
            );
        }
        assert_eq!(
            movements.get(release),
            Some(&384),
            "missing movement: {release}"
        );
    }
    assert_eq!(movements.len(), 2, "unexpected movement records");
    mappings
}

fn expected_mappings(case: &str, release: &str) -> Vec<Vec<i32>> {
    parse_manifest(VECTORS)
        .remove(&(release, case))
        .unwrap_or_else(|| panic!("missing mapping rows: {release} {case}"))
}

fn assert_map_vectors(case: &str, replicas: usize) {
    for release in ["quincy", "tentacle"] {
        let expected = expected_mappings(case, release);
        let map = decode(release, case);
        assert_eq!(
            mappings(&map, replicas, expected.len()),
            expected,
            "{release} {case}"
        );
    }
}

#[test]
fn weight_topology_manifest_is_complete_and_ordered() {
    parse_manifest(VECTORS);
}

#[test]
fn weight_topology_manifest_rejects_incomplete_or_invalid_rows() {
    let missing = VECTORS.replace("tentacle|crushdiff-after|999|3,4,1\n", "");
    let missing_case = VECTORS
        .lines()
        .filter(|line| !line.starts_with("quincy|added-straw|"))
        .collect::<Vec<_>>()
        .join("\n");
    let malformed = format!("{VECTORS}quincy|adjust-before|0\n");
    let unknown = format!("{VECTORS}quincy|unknown|0|0\n");
    let duplicate = format!("{VECTORS}quincy|adjust-before|0|0\n");
    let unordered = VECTORS.replacen("quincy|adjust-before|1|0", "quincy|adjust-before|8|0", 1);
    for manifest in [
        &missing,
        &missing_case,
        &malformed,
        &unknown,
        &duplicate,
        &unordered,
    ] {
        assert!(std::panic::catch_unwind(|| parse_manifest(manifest)).is_err());
    }
}

// Upstream: v17.2.7/src/test/crush/CrushWrapper.cc::CrushWrapperTest.adjust_item_weight
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/crush/CrushWrapper.cc#L439
// Upstream: v20.2.4/src/test/crush/CrushWrapper.cc::CrushWrapperTest.adjust_item_weight
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/CrushWrapper.cc#L439
#[test]
fn adjust_item_weight_retains_all_and_single_shared_leaf_locations() {
    for release in ["quincy", "tentacle"] {
        let before = decode(release, "adjust-before");
        assert_eq!(weights(&before, -2), &[W, W]);
        assert_eq!(weights(&before, -3), &[W, W]);
        assert_eq!(weights(&before, -1), &[2 * W, 2 * W]);
        assert_eq!(before.get_bucket(-2).unwrap().weight, 2 * W);
        assert_eq!(before.get_bucket(-3).unwrap().weight, 2 * W);
        assert_eq!(before.get_bucket(-1).unwrap().weight, 4 * W);

        let after = decode(release, "adjust-item-after");
        assert_eq!(weights(&after, -2), &[2 * W, W]);
        assert_eq!(weights(&after, -3), &[2 * W, 2 * W]);
        assert_eq!(weights(&after, -1), &[3 * W, 4 * W]);
        assert_eq!(after.get_bucket(-2).unwrap().weight, 3 * W);
        assert_eq!(after.get_bucket(-3).unwrap().weight, 4 * W);
        assert_eq!(after.get_bucket(-1).unwrap().weight, 7 * W);
    }
    assert_map_vectors("adjust-before", 1);
    assert_map_vectors("adjust-item-after", 1);
}

// Upstream: v17.2.7/src/test/crush/CrushWrapper.cc::CrushWrapperTest.adjust_subtree_weight
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/crush/CrushWrapper.cc#L558
// Upstream: v20.2.4/src/test/crush/CrushWrapper.cc::CrushWrapperTest.adjust_subtree_weight
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/CrushWrapper.cc#L553
#[test]
fn adjust_subtree_weight_retains_descendant_host_and_root_totals() {
    for release in ["quincy", "tentacle"] {
        let after = decode(release, "adjust-subtree-after");
        assert_eq!(weights(&after, -2), &[2 * W, 2 * W]);
        assert_eq!(weights(&after, -3), &[W, W]);
        assert_eq!(weights(&after, -1), &[4 * W, 2 * W]);
        assert_eq!(after.get_bucket(-2).unwrap().weight, 4 * W);
        assert_eq!(after.get_bucket(-1).unwrap().weight, 6 * W);
    }
    assert_map_vectors("adjust-subtree-after", 1);
}

// Upstream: v17.2.7/src/test/cli/crushtool/reweight.t::locally assigned cmd-01..cmd-08
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/reweight.t#L1
// Upstream: v20.2.4/src/test/cli/crushtool/reweight.t::locally assigned cmd-01..cmd-08
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/reweight.t#L1
#[test]
fn reweight_keeps_multitype_bucket_weights_and_c_mapping() {
    for release in ["quincy", "tentacle"] {
        let map = decode(release, "multitype-after");
        assert_eq!(weights(&map, -2), &[2 * W, W]);
        assert_eq!(weights(&map, -3), &[2 * W, W]);
        let BucketData::Tree {
            num_nodes,
            node_weights,
        } = &map.get_bucket(-4).unwrap().data
        else {
            panic!("host2 must remain TREE")
        };
        assert_eq!(*num_nodes, 16);
        assert_eq!(
            node_weights,
            &[
                0,
                W,
                3 * W,
                2 * W,
                W * 9 / 2,
                W / 2,
                W * 3 / 2,
                W,
                W * 11 / 2,
                W,
                W,
                0,
                W,
                0,
                0,
                0,
            ]
        );
        assert_eq!(map.get_bucket(-4).unwrap().weight, 5 * W + W / 2);
        assert_eq!(weights(&map, -1), &[3 * W, 3 * W, 5 * W + W / 2]);
    }
    assert_map_vectors("multitype-after", 1);
}

// Upstream: v17.2.7/src/test/cli/crushtool/reweight_multiple.t::locally assigned cmd-01..cmd-05
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/reweight_multiple.t#L1
// Upstream: v20.2.4/src/test/cli/crushtool/reweight_multiple.t::locally assigned cmd-01..cmd-05
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/reweight_multiple.t#L1
#[test]
fn reweight_multiple_keeps_shared_multitree_weights_and_c_mapping() {
    for release in ["quincy", "tentacle"] {
        let map = decode(release, "multitree-after");
        assert_eq!(weights(&map, -1), &[W]);
        assert_eq!(weights(&map, -2), &[W * 5 / 2]);
        assert_eq!(weights(&map, -3), &[W, W * 5 / 2]);
        assert_eq!(weights(&map, -4), &[W, W * 5 / 2]);
    }
    assert_map_vectors("multitree-after", 1);
}

// Upstream: v17.2.7/qa/workunits/rados/test_crushdiff.sh::unchanged and changed-map checks
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/qa/workunits/rados/test_crushdiff.sh#L48
// Upstream: v20.2.4/qa/workunits/rados/test_crushdiff.sh::unchanged and changed-map checks
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/qa/workunits/rados/test_crushdiff.sh#L48
#[test]
fn crushdiff_source_shaped_maps_keep_zero_and_nonzero_movement() {
    for release in ["quincy", "tentacle"] {
        let before = decode(release, "crushdiff-before");
        let after = decode(release, "crushdiff-after");
        let expected_before = expected_mappings("crushdiff-before", release);
        let expected_after = expected_mappings("crushdiff-after", release);
        let before_rows = mappings(&before, 3, expected_before.len());
        let after_rows = mappings(&after, 3, expected_after.len());
        assert_eq!(before_rows, expected_before);
        assert_eq!(after_rows, expected_after);
        let moved = before_rows
            .iter()
            .zip(&after_rows)
            .filter(|(before, after)| before != after)
            .count();
        assert_eq!(moved, 384);
        assert!(moved > 0);
    }
}

// Upstream: v17.2.7/src/test/test_crush_bucket.sh::TEST_crush_bucket
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/test_crush_bucket.sh#L25
// Upstream: v20.2.4/src/test/test_crush_bucket.sh::TEST_crush_bucket
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/test_crush_bucket.sh#L25
#[test]
fn crush_bucket_added_straw_map_decodes_exact_shape_and_c_mapping() {
    for release in ["quincy", "tentacle"] {
        let map = decode(release, "added-straw");
        assert_eq!(
            map.names
                .iter()
                .find_map(|(&id, name)| (name == "test").then_some(id)),
            Some(-3)
        );
        let bucket = map.get_bucket(-3).unwrap();
        assert_eq!(
            (bucket.id, bucket.bucket_type, bucket.alg, bucket.weight),
            (-3, 1, BucketAlgorithm::Straw, 195)
        );
        assert_eq!(bucket.items, [-2]);
        assert!(matches!(&bucket.data,
            BucketData::Straw { item_weights, straws }
                if item_weights == &[195] && straws == &[W]));
        assert_eq!(map.names.get(&-3).map(String::as_str), Some("test"));
    }
    assert_map_vectors("added-straw", 1);
}
