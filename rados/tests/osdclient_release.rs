//! `require_osd_release` against a live cluster of any release: the
//! expected value is the cluster's own, which `osd dump` names. The flip
//! to a later release cannot be exercised here: `ceph osd
//! require-osd-release` never goes down and cannot pass the daemons' own
//! release, so these are no-clobber checks. The map's value survives the
//! incrementals a pool create and delete produce, and a client that
//! assumes the release before the map's reports its own while the map
//! keeps the cluster's. The unit tests on `apply_to` pin the rule itself.
//! Run with:
//!   CEPH_CONF=... cargo test -p rados --test osdclient_release -- --ignored --nocapture

mod common;

use std::time::Duration;

use bytes::Bytes;
use common::{build_test_client, test_client_builder, test_pool_name};
use rados::{CephRelease, Client};

const MAP_WAIT: Duration = Duration::from_secs(10);

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("{prefix}-{}-{nanos}", std::process::id())
}

/// The cluster's `require_osd_release`, from `osd dump` through `admin`.
/// The dump spells it with `ceph_release_name`, which
/// [`CephRelease::name`] mirrors.
async fn cluster_release(admin: &Client) -> CephRelease {
    let cmd = serde_json::json!({"prefix": "osd dump", "format": "json"});
    let result = admin
        .mon_client()
        .invoke(vec![cmd.to_string()], Bytes::new())
        .await
        .expect("osd dump");
    assert_eq!(result.retval, 0, "osd dump: {}", result.outs);
    let dump: serde_json::Value = serde_json::from_slice(&result.outbl).expect("osd dump JSON");
    let name = dump["require_osd_release"]
        .as_str()
        .expect("require_osd_release in osd dump");
    (1..=u8::MAX)
        .map(CephRelease)
        .find(|release| name != "unknown" && release.name() == name)
        .unwrap_or_else(|| panic!("osd dump's require_osd_release {name:?} is no known release"))
}

#[tokio::test]
#[ignore]
async fn require_osd_release_is_the_clusters_and_survives_incrementals() {
    let client = build_test_client().await.expect("client");
    let release = cluster_release(&client).await;
    println!("cluster require_osd_release: {release}");
    let ioctx = client.open_pool(&test_pool_name()).await.expect("pool");
    let osd = client.osd_client();
    assert_eq!(client.require_osd_release(), Some(release));
    assert_eq!(osd.require_osd_release(), Some(release));
    assert_eq!(ioctx.require_osd_release(), Some(release));

    let e0 = osd.get_osdmap().await.expect("map").epoch.as_u32();
    let pool = unique("release");
    osd.create_pool(&pool, None).await.expect("create_pool");
    let created = osd.wait_for_latest_osdmap(MAP_WAIT).await.expect("map");
    assert!(
        created.epoch.as_u32() > e0,
        "creating a pool moves the epoch"
    );
    // Those incrementals carried 0xff, "no change".
    assert_eq!(osd.require_osd_release(), Some(release));

    osd.delete_pool(&pool, true).await.expect("delete_pool");
    let deleted = osd.wait_for_latest_osdmap(MAP_WAIT).await.expect("map");
    assert!(deleted.epoch.as_u32() > created.epoch.as_u32());
    assert_eq!(osd.require_osd_release(), Some(release));
}

#[tokio::test]
#[ignore]
async fn assume_osd_release_takes_precedence() {
    let admin = build_test_client().await.expect("admin client");
    let release = cluster_release(&admin).await;
    // An assumed release equal to the map's would prove nothing.
    let assumed = CephRelease(release.0 - 1);
    let client = test_client_builder()
        .expect("builder")
        .assume_osd_release(assumed)
        .build()
        .await
        .expect("client");
    let ioctx = client.open_pool(&test_pool_name()).await.expect("pool");
    assert_eq!(client.require_osd_release(), Some(assumed));
    assert_eq!(ioctx.require_osd_release(), Some(assumed));
    let map = client.osd_client().get_osdmap().await.expect("map");
    assert_eq!(map.require_osd_release, release.0);
}
