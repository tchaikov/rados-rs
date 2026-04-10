//! Integration tests for MonClient::invoke() — the low-level mon command path.
//!
//! Environment:
//! - `CEPH_CONF` (required) — path to `ceph.conf`
//! - `CEPH_KEYRING` (optional) — overrides the keyring path from `ceph.conf`
//!
//! Run with:
//! ```bash
//! CEPH_CONF=/path/to/ceph.conf \
//!   cargo test --package rados --test monclient_command_operations -- --ignored --nocapture
//! ```

use bytes::Bytes;
use tracing::info;

mod common;
use common::build_test_client;

#[tokio::test]
#[ignore]
async fn test_invoke_status_command() {
    common::init_tracing();
    info!("testing invoke() with status command");

    let client = build_test_client().await.expect("build_test_client");
    let mon_client = client.mon_client();

    let cmd = vec![r#"{"prefix": "status"}"#.to_string()];
    let result = mon_client
        .invoke(cmd, Bytes::new())
        .await
        .expect("invoke status");
    info!(
        "status ok, retval={} outs={} outbl_len={}",
        result.retval,
        result.outs,
        result.outbl.len()
    );
    assert_eq!(result.retval, 0, "status command should succeed");
}

#[tokio::test]
#[ignore]
async fn test_invoke_list_pools() {
    common::init_tracing();
    info!("testing invoke() with pool ls command");

    let client = build_test_client().await.expect("build_test_client");
    let mon_client = client.mon_client();

    let cmd = vec![r#"{"prefix": "osd pool ls"}"#.to_string()];
    let result = mon_client
        .invoke(cmd, Bytes::new())
        .await
        .expect("invoke osd pool ls");
    assert_eq!(result.retval, 0, "pool ls should succeed");
    if !result.outbl.is_empty()
        && let Ok(pool_list) = String::from_utf8(result.outbl.to_vec())
    {
        info!("pools: {pool_list}");
        assert!(!pool_list.is_empty(), "should have at least one pool");
    }
}

#[tokio::test]
#[ignore]
async fn test_invoke_create_and_delete_pool() {
    common::init_tracing();
    info!("testing invoke() create + delete pool");

    let client = build_test_client().await.expect("build_test_client");
    let mon_client = client.mon_client();

    let pool_name = format!("test-invoke-{}", rand::random::<u32>());

    let cmd = vec![format!(
        r#"{{"prefix": "osd pool create", "pool": "{pool_name}"}}"#
    )];
    let result = mon_client
        .invoke(cmd, Bytes::new())
        .await
        .expect("invoke create pool");
    assert_eq!(result.retval, 0, "pool creation should succeed");

    // Give the monitor a moment to commit.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let cmd = vec![r#"{"prefix": "osd pool ls"}"#.to_string()];
    let result = mon_client
        .invoke(cmd, Bytes::new())
        .await
        .expect("invoke osd pool ls");
    let pool_list = String::from_utf8(result.outbl.to_vec()).expect("outbl is valid utf8");
    assert!(pool_list.contains(&pool_name), "pool should appear in list");

    let cmd = vec![format!(
        r#"{{"prefix": "osd pool rm", "pool": "{pool_name}", "pool2": "{pool_name}", "yes_i_really_really_mean_it": true}}"#
    )];
    let result = mon_client
        .invoke(cmd, Bytes::new())
        .await
        .expect("invoke delete pool");
    assert_eq!(result.retval, 0, "pool deletion should succeed");

    let cmd = vec![r#"{"prefix": "osd pool ls"}"#.to_string()];
    let result = mon_client
        .invoke(cmd, Bytes::new())
        .await
        .expect("invoke osd pool ls after delete");
    let pool_list = String::from_utf8(result.outbl.to_vec()).expect("outbl is valid utf8");
    assert!(
        !pool_list.contains(&pool_name),
        "pool should not exist after deletion"
    );
}

#[tokio::test]
#[ignore]
async fn test_invoke_with_input_buffer() {
    common::init_tracing();
    info!("testing invoke() with input buffer (config-key put/get/del)");

    let client = build_test_client().await.expect("build_test_client");
    let mon_client = client.mon_client();

    let cmd = vec![r#"{"prefix": "config-key put", "key": "test-invoke-key"}"#.to_string()];
    let input_data = Bytes::from("test-value-from-invoke");
    let result = mon_client
        .invoke(cmd, input_data)
        .await
        .expect("invoke config-key put");
    assert_eq!(result.retval, 0, "config-key put should succeed");

    let cmd = vec![r#"{"prefix": "config-key get", "key": "test-invoke-key"}"#.to_string()];
    let result = mon_client
        .invoke(cmd, Bytes::new())
        .await
        .expect("invoke config-key get");
    let value = String::from_utf8(result.outbl.to_vec()).expect("outbl is valid utf8");
    assert_eq!(value, "test-value-from-invoke", "value should match");

    // Best-effort cleanup; ignore errors so a failed del doesn't mask the real assertion.
    let cmd = vec![r#"{"prefix": "config-key del", "key": "test-invoke-key"}"#.to_string()];
    let _ = mon_client.invoke(cmd, Bytes::new()).await;
}
