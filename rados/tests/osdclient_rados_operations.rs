//! Integration tests for basic RADOS operations (write, read, stat, remove).
//!
//! Requires a running Ceph cluster. Configuration is read from `ceph.conf`
//! via [`rados::Client::builder`]; see the shared `common` module for details.
//!
//! Environment:
//! - `CEPH_CONF` (required) — path to `ceph.conf`
//! - `CEPH_KEYRING` (optional) — overrides the keyring path from `ceph.conf`
//! - `CEPH_TEST_POOL` (optional, default: `test-pool`) — pool to operate on
//!
//! Run with:
//! ```bash
//! CEPH_CONF=/path/to/ceph.conf \
//!   cargo test --package rados --test osdclient_rados_operations -- --ignored --nocapture
//! ```

use bytes::Bytes;
use tracing::info;

mod common;
use common::create_ioctx;

#[tokio::test]
#[ignore]
async fn test_write_operation() {
    common::init_tracing();
    info!("testing RADOS WRITE operation");

    let ioctx = create_ioctx().await.expect("create_ioctx");

    let object_name = format!("test-write-{}", rand::random::<u32>());
    let test_data = Bytes::from("Hello from RADOS write test!");

    info!("writing object: {object_name}");
    let result = ioctx
        .write_full(&object_name, test_data.clone())
        .await
        .expect("write_full");
    info!("write ok, version={}", result.version);

    ioctx.remove(&object_name).await.expect("remove");
    info!("cleanup ok");
}

#[tokio::test]
#[ignore]
async fn test_read_operation() {
    common::init_tracing();
    info!("testing RADOS READ operation");

    let ioctx = create_ioctx().await.expect("create_ioctx");

    let object_name = format!("test-read-{}", rand::random::<u32>());
    let test_data = Bytes::from("Hello from RADOS read test!");

    ioctx
        .write_full(&object_name, test_data.clone())
        .await
        .expect("write_full");

    let result = ioctx
        .read(&object_name, 0, test_data.len() as u64)
        .await
        .expect("read");
    info!("read ok, version={}", result.version);

    assert_eq!(result.data, test_data, "read data mismatch");

    ioctx.remove(&object_name).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn test_stat_operation() {
    common::init_tracing();
    info!("testing RADOS STAT operation");

    let ioctx = create_ioctx().await.expect("create_ioctx");

    let object_name = format!("test-stat-{}", rand::random::<u32>());
    let test_data = Bytes::from("Hello from RADOS stat test!");

    ioctx
        .write_full(&object_name, test_data.clone())
        .await
        .expect("write_full");

    let stat = ioctx.stat(&object_name).await.expect("stat");
    info!("stat ok, size={} mtime={:?}", stat.size, stat.mtime);

    assert_eq!(
        stat.size as usize,
        test_data.len(),
        "stat size mismatch vs written data"
    );

    ioctx.remove(&object_name).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn test_remove_operation() {
    common::init_tracing();
    info!("testing RADOS REMOVE operation");

    let ioctx = create_ioctx().await.expect("create_ioctx");

    let object_name = format!("test-remove-{}", rand::random::<u32>());
    let test_data = Bytes::from("Hello from RADOS remove test!");

    ioctx
        .write_full(&object_name, test_data.clone())
        .await
        .expect("write_full");

    // stat pre-remove: must succeed
    ioctx
        .stat(&object_name)
        .await
        .expect("object should exist before removal");

    ioctx.remove(&object_name).await.expect("remove");

    // stat post-remove: must fail
    let stat_result = ioctx.stat(&object_name).await;
    assert!(
        stat_result.is_err(),
        "object should not exist after removal"
    );
}

#[tokio::test]
#[ignore]
async fn test_write_read_stat_remove_workflow() {
    common::init_tracing();
    info!("testing complete RADOS workflow");

    let ioctx = create_ioctx().await.expect("create_ioctx");

    let object_name = format!("test-workflow-{}", rand::random::<u32>());
    let test_data = Bytes::from("Hello from RADOS workflow test!");

    let write_result = ioctx
        .write_full(&object_name, test_data.clone())
        .await
        .expect("write_full");
    info!("write ok, version={}", write_result.version);

    let read_result = ioctx
        .read(&object_name, 0, test_data.len() as u64)
        .await
        .expect("read");
    assert_eq!(read_result.data, test_data, "data mismatch");

    let stat_result = ioctx.stat(&object_name).await.expect("stat");
    assert_eq!(stat_result.size as usize, test_data.len(), "size mismatch");

    ioctx.remove(&object_name).await.expect("remove");

    let stat_after_remove = ioctx.stat(&object_name).await;
    assert!(
        stat_after_remove.is_err(),
        "object should not exist after removal"
    );
}

#[tokio::test]
#[ignore]
async fn test_client_shutdown_is_clean_and_idempotent() {
    common::init_tracing();
    info!("testing Client::shutdown end-to-end");

    // Build a client, issue a successful op to confirm everything is wired
    // up, then call shutdown() and assert a follow-up op fails cleanly.
    let client = common::build_test_client()
        .await
        .expect("build_test_client");
    let pool = common::test_pool_name();
    let ioctx = client.open_pool(&pool).await.expect("open_pool");

    let object_name = format!("test-shutdown-{}", rand::random::<u32>());
    let test_data = Bytes::from("hello from shutdown test");
    ioctx
        .write_full(&object_name, test_data.clone())
        .await
        .expect("pre-shutdown write_full");
    ioctx
        .remove(&object_name)
        .await
        .expect("pre-shutdown remove");

    // First shutdown: must succeed and terminate background tasks.
    client.shutdown().await.expect("first shutdown");

    // Second shutdown: must be idempotent — both MonClient and OSDClient
    // short-circuit when already cancelled, so this should still be Ok.
    client.shutdown().await.expect("second shutdown idempotent");

    // Post-shutdown op must fail rather than hang forever. We assert on
    // "failed" rather than a specific kind because OSDClient returns one of
    // several errors depending on which background task noticed the
    // cancellation first.
    let post_shutdown = ioctx.stat(&object_name).await;
    assert!(
        post_shutdown.is_err(),
        "stat after shutdown should fail, got {:?}",
        post_shutdown
    );
}
