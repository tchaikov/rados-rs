use bytes::Bytes;
use rados::crush::{
    BucketAlgorithm, BucketData, CrushMap, RuleOp, RuleType, mapper::crush_do_rule,
};

const NONE: i32 = 0x7fff_ffff;
const VECTORS: &str = include_str!("reference/legacy-bucket-vectors.txt");

fn decode(bytes: &'static [u8]) -> CrushMap {
    CrushMap::decode(&mut Bytes::from_static(bytes)).unwrap()
}

fn fixture(release: &str, algorithm: &str) -> &'static [u8] {
    match (release, algorithm) {
        ("quincy", "uniform") => include_bytes!("reference/uniform-local-quincy.crushmap"),
        ("tentacle", "uniform") => include_bytes!("reference/uniform-local-tentacle.crushmap"),
        ("quincy", "list") => include_bytes!("reference/list-local-quincy.crushmap"),
        ("tentacle", "list") => include_bytes!("reference/list-local-tentacle.crushmap"),
        ("quincy", "tree") => include_bytes!("reference/tree-add-item-quincy.crushmap"),
        ("tentacle", "tree") => include_bytes!("reference/tree-add-item-tentacle.crushmap"),
        _ => unreachable!(),
    }
}

fn expected(items: &str) -> Vec<i32> {
    items
        .split(',')
        .filter(|item| !item.is_empty())
        .map(|item| {
            if item == "NONE" {
                NONE
            } else {
                item.parse().unwrap()
            }
        })
        .collect()
}

fn weights(case: &str, devices: usize) -> Vec<u32> {
    let mut weights = vec![65536; devices];
    match case {
        "complete" | "collision" => {}
        "retry" => weights[1] = 0,
        "hole" => {
            weights[1] = 0;
            weights[2] = 0;
        }
        _ => unreachable!(),
    }
    weights
}

// Upstream: v17.2.7/src/test/cli/crushtool/add-item-in-tree.t::locally assigned cmd-01..cmd-10
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/add-item-in-tree.t#L1
// Upstream: v20.2.4/src/test/cli/crushtool/add-item-in-tree.t::locally assigned cmd-01..cmd-10
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/add-item-in-tree.t#L1
#[test]
fn legacy_bucket_fixtures_decode_the_pinned_c_shapes() {
    for release in ["quincy", "tentacle"] {
        let uniform = decode(fixture(release, "uniform"));
        assert!(matches!(
            uniform.get_bucket(-1).unwrap().data,
            BucketData::Uniform { item_weight: 65536 }
        ));

        let list = decode(fixture(release, "list"));
        assert!(matches!(
            &list.get_bucket(-1).unwrap().data,
            BucketData::List { item_weights, .. } if item_weights == &[65536, 131072, 196608]
        ));

        let tree = decode(fixture(release, "tree"));
        for (id, num_nodes, node_weights) in [
            (-1, 2, &[0, 8 << 16][..]),
            (
                -2,
                16,
                &[
                    0,
                    1 << 16,
                    2 << 16,
                    1 << 16,
                    4 << 16,
                    1 << 16,
                    2 << 16,
                    1 << 16,
                    8 << 16,
                    1 << 16,
                    2 << 16,
                    1 << 16,
                    4 << 16,
                    1 << 16,
                    2 << 16,
                    1 << 16,
                ][..],
            ),
        ] {
            let bucket = tree.get_bucket(id).unwrap();
            assert_eq!(bucket.alg, BucketAlgorithm::Tree);
            assert!(matches!(
                &bucket.data,
                BucketData::Tree { num_nodes: actual_nodes, node_weights: actual_weights }
                    if *actual_nodes == num_nodes && actual_weights == node_weights
            ));
        }
        for rule in 0..3 {
            let rule = tree.get_rule(rule).unwrap();
            assert_eq!(rule.rule_type, RuleType::Replicated);
            assert_eq!(
                rule.steps
                    .iter()
                    .map(|step| (step.op, step.arg1, step.arg2))
                    .collect::<Vec<_>>(),
                vec![
                    (RuleOp::Take, -1, 0),
                    (RuleOp::ChooseLeafFirstN, 0, 1),
                    (RuleOp::Emit, 0, 0),
                ]
            );
        }
    }
}

// Rust contract; no upstream ordered Uniform/List mapping oracle exists.
// These local maps are compiled and mapped by both pinned crushtool releases.
// TREE retains all rules from add-item-in-tree.t; see fixture provenance.
#[test]
fn legacy_buckets_match_pinned_c_ordered_vectors() {
    let mut cases = 0;
    for release in ["quincy", "tentacle"] {
        for algorithm in ["uniform", "list", "tree"] {
            let map = decode(fixture(release, algorithm));
            for line in VECTORS.lines().filter(|line| line.starts_with(release)) {
                let fields: Vec<_> = line.split('|').collect();
                assert_eq!(fields.len(), 8, "{line}");
                if fields[1] != algorithm {
                    continue;
                }
                let rule = fields[3].parse().unwrap();
                let x = fields[4].parse().unwrap();
                let replicas = fields[5].parse().unwrap();
                let length = fields[6].parse::<usize>().unwrap();
                let expected = expected(fields[7]);
                assert_eq!(expected.len(), length, "{line}");
                let mut actual = Vec::new();
                crush_do_rule(
                    &map,
                    rule,
                    x,
                    &mut actual,
                    replicas,
                    &weights(fields[2], map.max_devices as usize),
                )
                .unwrap();
                assert_eq!(actual, expected, "{line}");
                cases += 1;
            }
        }
    }
    assert_eq!(cases, 352);
}
