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
