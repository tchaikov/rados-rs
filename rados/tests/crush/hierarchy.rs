//! Hierarchy and location-query ports from Ceph's CRUSH wrapper tests.

use bytes::Bytes;
use rados::crush::{BucketAlgorithm, BucketData, CrushBucket, CrushError, CrushMap};

const WEIGHT: u32 = 0x1_0000;

fn store_bucket(map: &mut CrushMap, bucket: CrushBucket) {
    let index = (-1 - bucket.id) as usize;
    if map.buckets.len() <= index {
        map.buckets.resize(index + 1, None);
    }
    map.buckets[index] = Some(bucket);
}

fn bucket(id: i32, bucket_type: i32, items: &[i32], item_weights: &[u32]) -> CrushBucket {
    CrushBucket {
        id,
        bucket_type,
        alg: BucketAlgorithm::Straw,
        hash: 0,
        weight: item_weights.iter().sum(),
        size: items.len() as u32,
        items: items.to_vec(),
        data: BucketData::Straw {
            item_weights: item_weights.to_vec(),
            straws: vec![WEIGHT; items.len()],
        },
    }
}

fn empty_root_map(root_type: i32, include_host_type: bool) -> CrushMap {
    let mut map = CrushMap::new();
    map.type_names.insert(0, "osd".into());
    if include_host_type {
        map.type_names.insert(1, "host".into());
    }
    map.type_names.insert(root_type, "root".into());
    map.names.insert(-1, "default".into());
    store_bucket(&mut map, bucket(-1, root_type, &[], &[]));
    map.max_buckets = 1;
    map
}

fn distance_map() -> CrushMap {
    let mut map = CrushMap::new();
    map.type_names = [(1, "host"), (2, "rack"), (3, "root")]
        .into_iter()
        .map(|(id, name)| (id, name.to_owned()))
        .collect();
    map.names = [
        (0, "osd.0"),
        (1, "osd.1"),
        (2, "osd.2"),
        (3, "osd.3"),
        (-1, "default"),
        (-2, "a1"),
        (-3, "a"),
        (-4, "a2"),
        (-5, "b1"),
        (-6, "b"),
        (-7, "b2"),
    ]
    .into_iter()
    .map(|(id, name)| (id, name.to_owned()))
    .collect();
    store_bucket(
        &mut map,
        bucket(-1, 3, &[-3, -6], &[2 * WEIGHT, 2 * WEIGHT]),
    );
    store_bucket(&mut map, bucket(-2, 1, &[0], &[WEIGHT]));
    store_bucket(&mut map, bucket(-3, 2, &[-2, -4], &[WEIGHT, WEIGHT]));
    store_bucket(&mut map, bucket(-4, 1, &[1], &[WEIGHT]));
    store_bucket(&mut map, bucket(-5, 1, &[2], &[WEIGHT]));
    store_bucket(&mut map, bucket(-6, 2, &[-5, -7], &[WEIGHT, WEIGHT]));
    store_bucket(&mut map, bucket(-7, 1, &[3], &[WEIGHT]));
    map.max_buckets = 7;
    map.max_devices = 10;
    map
}

// Upstream: v17.2.7/src/test/crush/CrushWrapper.cc::CrushWrapperTest.get_immediate_parent
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/crush/CrushWrapper.cc#L55
// Upstream: v20.2.4/src/test/crush/CrushWrapper.cc::CrushWrapperTest.get_immediate_parent
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/CrushWrapper.cc#L55
#[test]
fn immediate_parent_reports_missing_then_parent() {
    let mut map = empty_root_map(1, false);
    assert!(matches!(
        map.immediate_parent(0),
        Err(CrushError::ItemNotFound(0))
    ));
    map.names.insert(0, "osd.0".into());
    store_bucket(&mut map, bucket(-1, 1, &[0], &[WEIGHT]));
    assert_eq!(
        map.immediate_parent(0).unwrap(),
        ("root".into(), "default".into())
    );
}

// Upstream: v17.2.7/src/crush/CrushWrapper.cc::CrushWrapper::get_immediate_parent
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/crush/CrushWrapper.cc#L1619
// v20.2.4 has the identical implementation at line 1660.
#[test]
fn immediate_parent_scans_edges_without_a_child_name() {
    let mut map = empty_root_map(1, false);
    store_bucket(&mut map, bucket(-1, 1, &[0], &[WEIGHT]));
    assert_eq!(
        map.immediate_parent(0).unwrap(),
        ("root".into(), "default".into())
    );
}

// Upstream: v17.2.7/src/test/crush/CrushWrapper.cc::CrushWrapperTest.check_item_loc
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/crush/CrushWrapper.cc#L280
// Upstream: v20.2.4/src/test/crush/CrushWrapper.cc::CrushWrapperTest.check_item_loc
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/CrushWrapper.cc#L280
#[test]
fn item_location_requires_existing_bucket_and_returns_weight() {
    let empty = CrushMap::new();
    assert_eq!(empty.item_at_location_weight(0, &[]), None);
    let mut map = empty_root_map(2, true);
    assert_eq!(
        map.item_at_location_weight(0, &[("root".into(), "default".into())]),
        None
    );
    assert_eq!(
        map.item_at_location_weight(
            0,
            &[
                ("root".into(), "default".into()),
                ("host".into(), "host0".into())
            ]
        ),
        None
    );
    map.names.insert(0, "osd.0".into());
    store_bucket(&mut map, bucket(-1, 2, &[0], &[WEIGHT]));
    assert_eq!(
        map.item_at_location_weight(0, &[("root".into(), "osd.0".into())]),
        None
    );
    assert_eq!(
        map.item_at_location_weight(0, &[("root".into(), "default".into())]),
        Some(WEIGHT)
    );
}

// Upstream: v17.2.7/src/test/crush/CrushWrapper.cc::CrushWrapperTest.bucket_types
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/crush/CrushWrapper.cc#L839
// Upstream: v20.2.4/src/test/crush/CrushWrapper.cc::CrushWrapperTest.bucket_types
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/CrushWrapper.cc#L839
#[test]
fn bucket_type_queries_preserve_id_name_pairs() {
    let mut map = CrushMap::new();
    map.type_names.insert(123, "NAME".into());
    assert_eq!(map.type_count(), 1);
    assert_eq!(map.type_id("NAME"), Some(123));
    assert_eq!(map.type_name(123), Some("NAME"));
}

// Upstream: v17.2.7/src/test/crush/CrushWrapper.cc::CrushWrapperTest.distance
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/crush/CrushWrapper.cc#L945
// Upstream: v20.2.4/src/test/crush/CrushWrapper.cc::CrushWrapperTest.distance
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/crush/CrushWrapper.cc#L942
#[test]
fn location_and_distance_preserve_order_and_multipath_minimum() {
    let map = distance_map();
    assert_eq!(
        map.location_ordered(3).unwrap(),
        [
            ("host".into(), "b2".into()),
            ("rack".into(), "b".into()),
            ("root".into(), "default".into())
        ]
    );
    let locations = [
        ("host".into(), "b2".into()),
        ("rack".into(), "b".into()),
        ("root".into(), "default".into()),
    ];
    assert_eq!(map.common_ancestor_distance(0, &locations).unwrap(), 3);
    assert_eq!(map.common_ancestor_distance(1, &locations).unwrap(), 3);
    assert_eq!(map.common_ancestor_distance(2, &locations).unwrap(), 2);
    assert_eq!(map.common_ancestor_distance(3, &locations).unwrap(), 1);
    assert!(matches!(
        map.common_ancestor_distance(123, &locations),
        Err(CrushError::ItemNotFound(123))
    ));
    assert!(matches!(
        map.common_ancestor_distance(0, &[]),
        Err(CrushError::NoCommonAncestor)
    ));
    let multipath = [
        ("host".into(), "b2".into()),
        ("rack".into(), "b".into()),
        ("root".into(), "default".into()),
        ("host".into(), "b1".into()),
    ];
    assert_eq!(map.common_ancestor_distance(2, &multipath).unwrap(), 1);
    assert_eq!(map.common_ancestor_distance(3, &multipath).unwrap(), 1);
}

// Upstream: v17.2.7/src/test/cli/crushtool/location.t::cmd-01/cmd-02
// Source cmd-01: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/location.t#L1
// Source cmd-02: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/location.t#L2
// Upstream: v20.2.4/src/test/cli/crushtool/location.t::cmd-01/cmd-02
// Source cmd-01: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/location.t#L1
// Source cmd-02: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/location.t#L2
// Upstream: v17.2.7/src/test/cli/crushtool/location.t::cmd-03/cmd-04/cmd-05
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/cli/crushtool/location.t#L3
// Upstream: v20.2.4/src/test/cli/crushtool/location.t::cmd-03/cmd-04/cmd-05
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/cli/crushtool/location.t#L3
#[test]
fn large_location_fixture_preserves_ordered_paths() {
    let mut bytes = Bytes::from_static(include_bytes!("fixtures/test-map-big-1.crushmap"));
    let map = CrushMap::decode(&mut bytes).unwrap();
    assert!(bytes.is_empty());
    assert_eq!(map.location_ordered(44).unwrap(), []);
    assert!(matches!(
        map.location_ordered(16),
        Err(CrushError::ItemNotFound(16))
    ));
    assert_eq!(
        map.location_ordered(167).unwrap(),
        [
            ("host".into(), "p05151113587529".into()),
            ("rack".into(), "RJ45".into()),
            ("room".into(), "0513-R-0050".into()),
            ("root".into(), "default".into()),
        ]
    );
    assert_eq!(
        map.location_ordered(258).unwrap(),
        [
            ("host".into(), "lxfssi44a06".into()),
            ("rack".into(), "SI44".into()),
            ("root".into(), "castor".into())
        ]
    );
    assert_eq!(
        map.location_ordered(87).unwrap(),
        [
            ("host".into(), "p05151113576052".into()),
            ("rack".into(), "RJ43".into()),
            ("room".into(), "0513-R-0050".into()),
            ("root".into(), "default".into()),
        ]
    );
}

// Rust contract; no upstream analogue found. Public construction and malformed
// decoded data can create a parent cycle.
#[test]
fn location_rejects_parent_cycles() {
    let mut map = CrushMap::new();
    map.type_names.insert(1, "host".into());
    map.names = [(-1, "one"), (-2, "two")]
        .into_iter()
        .map(|(id, name)| (id, name.to_owned()))
        .collect();
    store_bucket(&mut map, bucket(-1, 1, &[-2], &[WEIGHT]));
    store_bucket(&mut map, bucket(-2, 1, &[-1], &[WEIGHT]));
    assert!(matches!(
        map.location_ordered(-1),
        Err(CrushError::InvalidHierarchy("cycle"))
    ));
}
