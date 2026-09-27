//! `require_osd_release` against the Ceph v19.2.2 test cluster, whose map
//! says `squid`. The flip to a later release cannot be exercised here:
//! `ceph osd require-osd-release` never goes down and cannot pass the
//! daemons' own release, so these are no-clobber checks: the map's value
//! survives the incrementals a pool create and delete produce. The unit
//! tests on `apply_to` pin the rule itself. Run with:
//!   CEPH_CONF=... cargo test -p rados --test osdclient_release -- --ignored --nocapture

mod common;

use std::time::Duration;

use common::{build_test_client, test_client_builder, test_pool_name};
use rados::CephRelease;

const MAP_WAIT: Duration = Duration::from_secs(10);

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("{prefix}-{}-{nanos}", std::process::id())
}

#[tokio::test]
#[ignore]
async fn require_osd_release_is_squid_and_survives_incrementals() {
    let client = build_test_client().await.expect("client");
    let ioctx = client.open_pool(&test_pool_name()).await.expect("pool");
    let osd = client.osd_client();
    assert_eq!(client.require_osd_release(), Some(CephRelease::SQUID));
    assert_eq!(osd.require_osd_release(), Some(CephRelease::SQUID));
    assert_eq!(ioctx.require_osd_release(), Some(CephRelease::SQUID));

    let e0 = osd.get_osdmap().await.expect("map").epoch.as_u32();
    let pool = unique("release");
    osd.create_pool(&pool, None).await.expect("create_pool");
    let created = osd.wait_for_latest_osdmap(MAP_WAIT).await.expect("map");
    assert!(
        created.epoch.as_u32() > e0,
        "creating a pool moves the epoch"
    );
    // Those incrementals carried 0xff, "no change".
    assert_eq!(osd.require_osd_release(), Some(CephRelease::SQUID));

    osd.delete_pool(&pool, true).await.expect("delete_pool");
    let deleted = osd.wait_for_latest_osdmap(MAP_WAIT).await.expect("map");
    assert!(deleted.epoch.as_u32() > created.epoch.as_u32());
    assert_eq!(osd.require_osd_release(), Some(CephRelease::SQUID));
}

#[tokio::test]
#[ignore]
async fn assume_osd_release_takes_precedence() {
    let client = test_client_builder()
        .expect("builder")
        .assume_osd_release(CephRelease::TENTACLE)
        .build()
        .await
        .expect("client");
    let ioctx = client.open_pool(&test_pool_name()).await.expect("pool");
    assert_eq!(client.require_osd_release(), Some(CephRelease::TENTACLE));
    assert_eq!(ioctx.require_osd_release(), Some(CephRelease::TENTACLE));
    let map = client.osd_client().get_osdmap().await.expect("map");
    assert_eq!(map.require_osd_release, 19);
}
