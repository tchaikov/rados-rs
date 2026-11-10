//! Original live CRUSH class and pool-rule integration gates.
//!
//! Run through `rados/tests/crush/reference/live-rust-crush-gates.sh`; it
//! starts an isolated pinned Ceph cluster and supplies `CEPH_CONF` plus
//! `CEPH_LIVE_CONTAINER` for the selected ignored test.

use std::process::{Command, Output};
use std::time::Duration;

use bytes::Bytes;
use rados::crush::{CrushMap, RuleOp, RuleType, crush_do_rule_with_choose_args};

mod common;
use common::build_test_client;

const STRICT_BASH: [&str; 5] = ["bash", "-e", "-o", "pipefail", "-lc"];

fn admin_output(script: &str) -> Output {
    let container = std::env::var("CEPH_LIVE_CONTAINER")
        .expect("CEPH_LIVE_CONTAINER is required; use live-rust-crush-gates.sh");
    let config = std::env::var("CEPH_LIVE_CONFIG")
        .expect("CEPH_LIVE_CONFIG is required; use live-rust-crush-gates.sh");
    let script = format!("export CEPH_CONF={config}; {script}");
    Command::new("docker")
        .args(["exec", &container])
        .args(STRICT_BASH)
        .arg(&script)
        .output()
        .expect("run pinned Ceph admin command")
}

#[test]
fn admin_shell_propagates_intermediate_failures() {
    for script in ["false\ntrue", "false | true\ntrue"] {
        let status = Command::new(STRICT_BASH[0])
            .args(&STRICT_BASH[1..])
            .arg(script)
            .status()
            .expect("run strict shell check");
        assert!(!status.success(), "script unexpectedly succeeded: {script}");
    }
}

fn admin(script: &str) {
    let output = admin_output(script);
    assert!(
        output.status.success(),
        "admin command failed:\n{script}\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn live_map(path: &str) -> CrushMap {
    admin(&format!("ceph osd getcrushmap -o {path}"));
    let container = std::env::var("CEPH_LIVE_CONTAINER").expect("CEPH_LIVE_CONTAINER");
    let directory = tempfile::tempdir().expect("temporary map directory");
    let local = directory.path().join("map.crushmap");
    let output = Command::new("docker")
        .args([
            "cp",
            &format!("{container}:{path}"),
            local.to_str().unwrap(),
        ])
        .output()
        .expect("copy live CRUSH map");
    assert!(
        output.status.success(),
        "copy live CRUSH map failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    CrushMap::decode(&mut Bytes::from(
        std::fs::read(local).expect("read live CRUSH map"),
    ))
    .expect("decode live CRUSH map")
}

fn c_mappings(path: &str, rule: u32) -> Vec<Vec<i32>> {
    let output = admin_output(&format!(
        "crushtool -i {path} --test --rule {rule} --num-rep 3 --min-x 0 --max-x 15 --show-mappings"
    ));
    assert!(
        output.status.success(),
        "pinned C placement failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
        .stdout
        .split(|&byte| byte == b'\n')
        .filter_map(|line| {
            std::str::from_utf8(line)
                .ok()?
                .split_once('[')
                .map(|(_, items)| items)
        })
        .map(|items| {
            items
                .trim_end_matches(']')
                .split(',')
                .filter(|item| !item.is_empty())
                .map(|item| item.trim().parse().expect("C mapping item"))
                .collect()
        })
        .collect()
}

fn assert_c_rust_mappings(path: &str, map: &CrushMap, rule: u32) {
    let c = c_mappings(path, rule);
    assert_eq!(c.len(), 16, "C mapping row count");
    let weights = vec![65536; map.max_devices as usize];
    for (x, expected) in c.into_iter().enumerate() {
        let mut actual = Vec::new();
        crush_do_rule_with_choose_args(map, rule, x as u32, &mut actual, 3, &weights, -1)
            .expect("Rust placement");
        assert_eq!(actual, expected, "rule={rule} x={x}");
    }
}

fn item_weight(map: &CrushMap, bucket: &str, item: i32) -> u32 {
    let id = map
        .names
        .iter()
        .find_map(|(&id, name)| (name == bucket).then_some(id))
        .expect("named bucket");
    let bucket = map.get_bucket(id).expect("bucket");
    let index = bucket
        .items
        .iter()
        .position(|&candidate| candidate == item)
        .expect("bucket item");
    match &bucket.data {
        rados::crush::BucketData::Straw { item_weights, .. }
        | rados::crush::BucketData::Straw2 { item_weights }
        | rados::crush::BucketData::List { item_weights, .. } => item_weights[index],
        rados::crush::BucketData::Uniform { item_weight } => *item_weight,
        rados::crush::BucketData::Tree { .. } => panic!("host is not a TREE bucket"),
    }
}

async fn write(client: &rados::Client, object: &str) {
    let ioctx = client.open_pool("rbd").await.expect("open rbd");
    tokio::time::timeout(
        Duration::from_secs(45),
        ioctx.write_full(object, Bytes::from_static(b"ABCDEF\n")),
    )
    .await
    .expect("write_full deadline")
    .expect("write_full");
}

// Upstream: v17.2.7/qa/standalone/crush/crush-classes.sh::TEST_classes
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/qa/standalone/crush/crush-classes.sh#L88-L139
// Upstream: v20.2.4/qa/standalone/crush/crush-classes.sh::TEST_classes
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/qa/standalone/crush/crush-classes.sh#L88-L139
// Upstream: v17.2.7/qa/standalone/crush/crush-classes.sh::TEST_set_device_class
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/qa/standalone/crush/crush-classes.sh#L141-L164
// Upstream: v20.2.4/qa/standalone/crush/crush-classes.sh::TEST_set_device_class
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/qa/standalone/crush/crush-classes.sh#L141-L164
// Upstream: v17.2.7/src/test/librados/pool.cc::LibRadosPools.PoolCreateWithCrushRule
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/librados/pool.cc#L124-L135
// Helper: v17.2.7/src/test/librados/test.cc::create_one_pool
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/librados/test.cc#L21-L48
// Upstream: v20.2.4/src/test/librados/pool.cc::LibRadosPools.PoolCreateWithCrushRule
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/librados/pool.cc#L125-L136
// Helper: v20.2.4/src/test/librados/test.cc::create_one_pool
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/librados/test.cc#L21-L48
#[tokio::test]
#[ignore]
async fn test_original_crush_class_and_pool_rule_gates() {
    common::init_tracing();
    let client = build_test_client().await.expect("build_test_client");

    admin(
        r#"
        up() {
          actual=$(ceph osd map rbd "$1" -f json | python3 -c 'import json,sys; print(" ".join(map(str, json.load(sys.stdin)["up"])))')
          test "$actual" = "$2"
        }
        up SOMETHING '1 2 0'
        "#,
    );
    write(&client, "SOMETHING").await;

    admin(
        r#"
        ceph osd getcrushmap > /tmp/task9a-map
        crushtool -d /tmp/task9a-map -o /tmp/task9a-map.txt
        sed -i -e '/device 0 osd.0/s/$/ class ssd/' -e '/step take default/s/$/ class ssd/' /tmp/task9a-map.txt
        crushtool -c /tmp/task9a-map.txt -o /tmp/task9a-map-new
        ceph osd setcrushmap -i /tmp/task9a-map-new
        "#,
    );
    admin(
        r#"
        up() {
          actual=$(ceph osd map rbd SOMETHING_ELSE -f json | python3 -c 'import json,sys; print(" ".join(map(str, json.load(sys.stdin)["up"])))')
          test "$actual" = '0'
        }
        for delay in 2 4 8 16 32 64 128 256; do
          up && exit 0
          sleep "$delay"
          ceph osd dump
          ceph pg dump
        done
        exit 1
        "#,
    );
    write(&client, "SOMETHING_ELSE").await;

    admin(
        r#"
        ceph osd crush dump | grep -q '~ssd'
        ceph osd crush set-device-class ssd osd.0
        ceph osd crush class ls-osd ssd | grep -qx '0'
        ceph osd crush set-device-class ssd osd.1
        ceph osd crush class ls-osd ssd | grep -qx '1'
        ceph osd crush set-device-class ssd 0 1
        "#,
    );
    admin(
        r#"
        up() {
          actual=$(ceph osd map rbd SOMETHING_ELSE -f json | python3 -c 'import json,sys; print(" ".join(map(str, json.load(sys.stdin)["up"])))')
          test "$actual" = '0 1'
        }
        for delay in 2 4 8 16 32 64 128 256; do
          up && exit 0
          sleep "$delay"
          ceph osd crush dump
          ceph osd dump
          ceph pg dump
        done
        exit 1
        "#,
    );

    let osd = client.osd_client();
    let guard = format!("task9a-guard-{}", rand::random::<u32>());
    let explicit = format!("task9a-rule-{}", rand::random::<u32>());
    osd.create_pool(&guard, None)
        .await
        .expect("create default-rule guard pool");
    let _ = client.open_pool(&guard).await.expect("open guard pool");
    let application = client
        .mon_client()
        .invoke(
            vec![format!(
                r#"{{"prefix":"osd pool application enable","pool":"{guard}","app":"rados"}}"#
            )],
            Bytes::new(),
        )
        .await
        .expect("enable rados application");
    assert_eq!(application.retval, 0, "enable rados application");
    osd.create_pool(&explicit, Some(0))
        .await
        .expect("create explicit-rule pool");
    osd.delete_pool(&explicit, true)
        .await
        .expect("delete explicit-rule pool");
    osd.delete_pool(&guard, true)
        .await
        .expect("delete guard pool");
}

// Upstream: v17.2.7/qa/standalone/crush/crush-classes.sh::TEST_reweight_vs_classes
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/qa/standalone/crush/crush-classes.sh#L60-L86
// Upstream: v20.2.4/qa/standalone/crush/crush-classes.sh::TEST_reweight_vs_classes
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/qa/standalone/crush/crush-classes.sh#L60-L86
#[test]
#[ignore]
fn test_reweight_vs_classes_consumes_live_pre_and_post_maps() {
    let host = String::from_utf8(admin_output("hostname -s").stdout)
        .expect("hostname")
        .trim()
        .to_owned();
    admin(
        r#"
        ceph osd crush set-device-class ssd osd.0
        ceph osd crush class ls-osd ssd | grep -qx 0
        ceph osd crush set-device-class ssd osd.1
        ceph osd crush class ls-osd ssd | grep -qx 1
        ceph osd crush reweight osd.0 1
        "#,
    );
    let before = live_map("/tmp/task9b-reweight-before.crushmap");
    assert_eq!(item_weight(&before, &host, 0), 65536);
    assert_eq!(item_weight(&before, &format!("{host}~ssd"), 0), 65536);
    assert_c_rust_mappings("/tmp/task9b-reweight-before.crushmap", &before, 0);

    admin(&format!("ceph osd crush set 0 2 host={host}"));
    let after = live_map("/tmp/task9b-reweight-after.crushmap");
    assert_eq!(item_weight(&after, &host, 0), 131072);
    assert_eq!(item_weight(&after, &format!("{host}~ssd"), 0), 131072);
    assert_c_rust_mappings("/tmp/task9b-reweight-after.crushmap", &after, 0);
}

// Upstream: v17.2.7/qa/standalone/crush/crush-classes.sh::TEST_mon_classes
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/qa/standalone/crush/crush-classes.sh#L166-L265
// Upstream: v20.2.4/qa/standalone/crush/crush-classes.sh::TEST_mon_classes
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/qa/standalone/crush/crush-classes.sh#L166-L265
#[tokio::test]
#[ignore]
async fn test_mon_classes_writes_before_lifecycle_mutations() {
    common::init_tracing();
    let client = build_test_client().await.expect("build_test_client");
    admin(
        r#"
        actual=$(ceph osd map rbd SOMETHING -f json | python3 -c 'import json,sys; print(" ".join(map(str, json.load(sys.stdin)["up"])))')
        test "$actual" = '1 2 0'
        "#,
    );
    write(&client, "SOMETHING").await;

    admin(
        r#"
        ceph osd crush class create CLASS
        ceph osd crush class create CLASS
        ceph osd crush class rename CLASS TEMP
        ceph osd crush class rename TEMP CLASS
        ceph osd erasure-code-profile set myprofile plugin=jerasure technique=reed_sol_van k=2 m=1 crush-failure-domain=osd crush-device-class=CLASS
        ceph osd erasure-code-profile rm myprofile
        ceph osd crush class rm CLASS
        ceph osd crush class rm CLASS
        ceph osd crush set-device-class aaa osd.0
        ceph osd crush set-device-class bbb osd.1
        ceph osd crush set-device-class ccc osd.2
        ceph osd crush rm-device-class 0
        ceph osd crush rm-device-class 1
        ceph osd crush rm-device-class 2
        ceph osd crush set-device-class asdf all
        ceph osd crush rule create-replicated asdf-rule default host asdf
        ceph osd crush rm-device-class all
        ceph osd crush set-device-class abc osd.2
        ceph osd crush move osd.2 root=foo rack=foo-rack host=foo-host
        ceph osd crush rm-device-class osd.2
        ceph osd crush set-device-class abc osd.2
        ceph osd crush rule create-replicated foo-rule foo host abc
        ceph osd crush set-device-class hdd osd.0
        ceph osd crush rm-device-class all
        ceph osd crush set-device-class class_1 all
        ceph osd crush rule create-replicated class_1_rule default host class_1
        ceph osd crush class rename class_1 class_2
        ceph osd crush class rename class_1 class_2
        "#,
    );
}

fn assert_erasure_rule(map: &CrushMap) {
    let rule = map.get_rule(1).expect("generated erasure rule");
    assert_eq!(map.rule_names.get(&1).map(String::as_str), Some("rule3"));
    assert_eq!(rule.rule_type, RuleType::Erasure);
    assert_eq!(
        rule.steps
            .iter()
            .map(|step| (step.op, step.arg1, step.arg2))
            .collect::<Vec<_>>(),
        [
            (RuleOp::SetChooseLeafTries, 5, 0),
            (RuleOp::SetChooseTries, 100, 0),
            (RuleOp::Take, -1, 0),
            (RuleOp::ChooseLeafIndep, 0, 1),
            (RuleOp::Emit, 0, 0),
        ]
    );
    assert_eq!(map.names.get(&-1).map(String::as_str), Some("default"));
}

// Upstream: v17.2.7/qa/standalone/mon/osd-crush.sh::TEST_crush_rule_create_erasure
// Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/qa/standalone/mon/osd-crush.sh#L84-L121
// Upstream: v20.2.4/qa/standalone/mon/osd-crush.sh::TEST_crush_rule_create_erasure
// Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/qa/standalone/mon/osd-crush.sh#L84-L121
#[test]
#[ignore]
fn test_crush_rule_create_erasure_consumes_default_and_explicit_rules() {
    admin("ceph osd crush rule create-erasure rule3");
    let implicit = live_map("/tmp/task9b-erasure-implicit.crushmap");
    assert_erasure_rule(&implicit);
    assert_c_rust_mappings("/tmp/task9b-erasure-implicit.crushmap", &implicit, 1);
    admin("ceph osd crush rule rm rule3");

    admin("ceph osd crush rule create-erasure rule3 default");
    let explicit = live_map("/tmp/task9b-erasure-explicit.crushmap");
    assert_erasure_rule(&explicit);
    assert_c_rust_mappings("/tmp/task9b-erasure-explicit.crushmap", &explicit, 1);
    admin("ceph osd crush rule rm rule3");
}
