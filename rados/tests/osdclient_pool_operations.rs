//! Integration tests for pool management operations (create/list/delete).
//!
//! **Must be run with `--test-threads=1`** to avoid races when multiple tests
//! create/delete pools simultaneously.
//!
//! Environment:
//! - `CEPH_CONF` (required) — path to `ceph.conf`
//! - `CEPH_KEYRING` (optional) — overrides the keyring path from `ceph.conf`
//!
//! Run with:
//! ```bash
//! CEPH_CONF=/path/to/ceph.conf \
//!   cargo test --package rados --test osdclient_pool_operations \
//!     -- --ignored --test-threads=1 --nocapture
//! ```

use std::time::Duration;
use tracing::info;

mod common;
use common::build_test_client;

/// Poll [`OSDClient::list_pools`] until `predicate` is satisfied or the budget
/// is exhausted. Centralises the "wait for OSDMap to catch up" loop that every
/// pool test needs after create/delete.
async fn poll_until<F>(
    client: &std::sync::Arc<rados::osdclient::OSDClient>,
    label: &str,
    mut predicate: F,
) -> bool
where
    F: FnMut(&[rados::osdclient::PoolInfo]) -> bool,
{
    // 100 attempts * 50ms = 5s budget. Same budget as the old hand-rolled loops.
    for i in 0..100 {
        let pool_infos = client.list_pools().await.expect("list_pools");
        if predicate(&pool_infos) {
            info!("{label} satisfied after {}ms", i * 50);
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

#[tokio::test]
#[ignore]
async fn test_create_pool() {
    common::init_tracing();
    info!("testing pool creation");

    let client = build_test_client().await.expect("build_test_client");
    let osd = client.osd_client();

    let pool_name = format!("test-create-{}", rand::random::<u32>());
    info!("creating pool {pool_name}");
    osd.create_pool(&pool_name, None)
        .await
        .expect("create_pool");

    let found = poll_until(osd, "pool visible in OSDMap", |pools| {
        pools.iter().any(|p| p.pool_name == pool_name)
    })
    .await;
    assert!(found, "pool {pool_name} not found in pool list");

    osd.delete_pool(&pool_name, true)
        .await
        .expect("cleanup delete_pool");
}

#[tokio::test]
#[ignore]
async fn test_delete_pool() {
    common::init_tracing();
    info!("testing pool deletion");

    let client = build_test_client().await.expect("build_test_client");
    let osd = client.osd_client();

    let pool_name = format!("test-delete-{}", rand::random::<u32>());
    info!("creating pool {pool_name}");
    osd.create_pool(&pool_name, None)
        .await
        .expect("create_pool");

    // Wait for the pool to appear in a received incremental OSDMap before
    // deleting, so the create/delete don't get batched into the same epoch.
    let found = poll_until(osd, "pool visible after create", |pools| {
        pools.iter().any(|p| p.pool_name == pool_name)
    })
    .await;
    assert!(found, "pool {pool_name} not found in OSDMap after create");

    // Small extra wait so the create has fully committed before we delete.
    tokio::time::sleep(Duration::from_millis(100)).await;

    info!("deleting pool {pool_name}");
    osd.delete_pool(&pool_name, true)
        .await
        .expect("delete_pool");

    let removed = poll_until(osd, "pool removed from OSDMap", |pools| {
        !pools.iter().any(|p| p.pool_name == pool_name)
    })
    .await;
    assert!(
        removed,
        "pool {pool_name} still exists after deletion + 5s wait"
    );
}

#[tokio::test]
#[ignore]
async fn test_delete_pool_requires_confirmation() {
    common::init_tracing();
    info!("testing pool deletion confirmation requirement");

    let client = build_test_client().await.expect("build_test_client");
    let osd = client.osd_client();
    let mon = client.mon_client();

    let pool_name = format!("test-confirm-{}", rand::random::<u32>());
    osd.create_pool(&pool_name, None)
        .await
        .expect("create_pool");

    mon.get_version(rados::monclient::MonService::OsdMap)
        .await
        .expect("get osdmap version");

    // Delete without confirmation must fail.
    let result = osd.delete_pool(&pool_name, false).await;
    assert!(result.is_err(), "delete should fail without confirmation");

    // Pool must still exist.
    let pool_infos = osd.list_pools().await.expect("list_pools");
    assert!(
        pool_infos.iter().any(|p| p.pool_name == pool_name),
        "pool should still exist after failed delete"
    );

    osd.delete_pool(&pool_name, true)
        .await
        .expect("cleanup delete_pool");
}

#[tokio::test]
#[ignore]
async fn test_list_pools() {
    common::init_tracing();
    info!("testing pool listing");

    let client = build_test_client().await.expect("build_test_client");
    let osd = client.osd_client();
    let mon = client.mon_client();

    let pool_infos = osd.list_pools().await.expect("list_pools");
    info!("found {} pools initially", pool_infos.len());

    let pool_name = format!("test-list-{}", rand::random::<u32>());
    osd.create_pool(&pool_name, None)
        .await
        .expect("create_pool");

    mon.get_version(rados::monclient::MonService::OsdMap)
        .await
        .expect("get osdmap version");

    let pools_after = osd.list_pools().await.expect("list_pools");
    assert!(
        pools_after.iter().any(|p| p.pool_name == pool_name),
        "new pool should be in list"
    );

    osd.delete_pool(&pool_name, true)
        .await
        .expect("cleanup delete_pool");
}

#[tokio::test]
#[ignore]
async fn test_pool_workflow() {
    common::init_tracing();
    info!("testing complete pool workflow");

    let client = build_test_client().await.expect("build_test_client");
    let osd = client.osd_client();
    let mon = client.mon_client();

    let pool_name = format!("test-workflow-{}", rand::random::<u32>());
    info!("creating pool {pool_name}");
    osd.create_pool(&pool_name, None)
        .await
        .expect("create_pool");

    mon.get_version(rados::monclient::MonService::OsdMap)
        .await
        .expect("get osdmap version");

    let pools_after = osd.list_pools().await.expect("list_pools");
    assert!(pools_after.iter().any(|p| p.pool_name == pool_name));

    info!("deleting pool {pool_name}");
    osd.delete_pool(&pool_name, true)
        .await
        .expect("delete_pool");

    let deleted = poll_until(osd, "pool removed in workflow", |pools| {
        !pools.iter().any(|p| p.pool_name == pool_name)
    })
    .await;
    assert!(
        deleted,
        "pool {pool_name} still exists after deletion + 5s wait"
    );
}
