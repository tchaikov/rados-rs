//! Integration tests for mon command operations using the invoke() API
//!
//! These tests demonstrate using the low-level invoke() method to send
//! arbitrary monitor commands.
//!
//! ## Running the tests
//!
//! ```bash
//! export CEPH_CONF=/path/to/ceph.conf
//! cargo test --package monclient --test command_operations -- --ignored --nocapture
//! ```

use bytes::Bytes;
use std::path::Path;
use std::sync::Arc;
use tracing::info;

/// Helper to get test configuration from environment and ceph.conf
struct TestConfig {
    mon_addrs: Vec<String>,
    keyring_path: String,
    entity_name: String,
}

impl TestConfig {
    fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        // Get ceph.conf path
        let ceph_conf_path =
            std::env::var("CEPH_CONF").unwrap_or_else(|_| "/etc/ceph/ceph.conf".to_string());

        if !Path::new(&ceph_conf_path).exists() {
            return Err(format!("ceph.conf not found at: {}", ceph_conf_path).into());
        }

        // Parse ceph.conf
        let ceph_config = cephconfig::CephConfig::from_file(&ceph_conf_path)?;

        // Get monitor addresses
        let mon_addrs = ceph_config.mon_addrs()?;

        // Get keyring path
        let keyring_path = std::env::var("CEPH_KEYRING")
            .or_else(|_| ceph_config.keyring())
            .unwrap_or_else(|_| "/etc/ceph/ceph.client.admin.keyring".to_string());

        // Get entity name
        let entity_name = ceph_config.entity_name();

        Ok(Self {
            mon_addrs,
            keyring_path,
            entity_name,
        })
    }
}

/// Helper to create and initialize a MonClient
async fn create_mon_client(
    config: &TestConfig,
) -> Result<Arc<monclient::MonClient>, Box<dyn std::error::Error>> {
    // Create MonClient
    let mon_config = monclient::MonClientConfig {
        entity_name: config.entity_name.clone(),
        mon_addrs: config.mon_addrs.clone(),
        keyring_path: config.keyring_path.clone(),
        ..Default::default()
    };

    // Create shared MessageBus
    let message_bus = Arc::new(msgr2::MessageBus::new());

    let mon_client = Arc::new(monclient::MonClient::new(mon_config, message_bus).await?);

    // Initialize connection
    mon_client.init().await?;

    // Register MonClient handlers on MessageBus
    mon_client.clone().register_handlers().await?;

    info!("✓ Connected to monitor");

    // Wait for authentication to complete - use event-driven wait
    mon_client
        .wait_for_auth(std::time::Duration::from_secs(5))
        .await?;

    // Wait for MonMap to arrive - use event-driven wait
    mon_client
        .wait_for_monmap(std::time::Duration::from_secs(2))
        .await?;

    Ok(mon_client)
}

#[tokio::test]
#[ignore] // Requires a running Ceph cluster
async fn test_invoke_status_command() {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    info!("Testing invoke() with status command");
    info!("====================================");

    let config = TestConfig::from_env().expect("Failed to load test configuration");
    let mon_client = create_mon_client(&config)
        .await
        .expect("Failed to create MonClient");

    // Use invoke() to get cluster status
    info!("Getting cluster status");
    let cmd = vec![r#"{"prefix": "status"}"#.to_string()];
    let result = mon_client
        .invoke(cmd, Bytes::new())
        .await
        .expect("Failed to get status");

    info!("✓ Status command succeeded");
    info!("Return value: {}", result.retval);
    info!("Output (outs): {}", result.outs);

    // Output buffer (outbl) may contain JSON data
    if !result.outbl.is_empty() {
        info!("Output buffer size: {} bytes", result.outbl.len());
        if let Ok(json_str) = String::from_utf8(result.outbl.to_vec()) {
            info!("Output buffer (JSON): {}", json_str);
        }
    }

    assert_eq!(result.retval, 0, "Status command should succeed");
    info!("✓ Test completed successfully");
}

#[tokio::test]
#[ignore] // Requires a running Ceph cluster
async fn test_invoke_list_pools() {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    info!("Testing invoke() with pool ls command");
    info!("=====================================");

    let config = TestConfig::from_env().expect("Failed to load test configuration");
    let mon_client = create_mon_client(&config)
        .await
        .expect("Failed to create MonClient");

    // Use invoke() to list pools
    info!("Listing pools");
    let cmd = vec![r#"{"prefix": "osd pool ls"}"#.to_string()];
    let result = mon_client
        .invoke(cmd, Bytes::new())
        .await
        .expect("Failed to list pools");

    info!("✓ Pool list command succeeded");
    info!("Return value: {}", result.retval);

    // The pool list is typically in the output buffer
    if !result.outbl.is_empty() {
        if let Ok(pool_list) = String::from_utf8(result.outbl.to_vec()) {
            info!("Pools: {}", pool_list);
            assert!(!pool_list.is_empty(), "Should have at least some pools");
        }
    }

    assert_eq!(result.retval, 0, "Pool list command should succeed");
    info!("✓ Test completed successfully");
}

#[tokio::test]
#[ignore] // Requires a running Ceph cluster
async fn test_invoke_create_and_delete_pool() {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    info!("Testing invoke() for pool creation and deletion");
    info!("==============================================");

    let config = TestConfig::from_env().expect("Failed to load test configuration");
    let mon_client = create_mon_client(&config)
        .await
        .expect("Failed to create MonClient");

    let pool_name = format!("test-invoke-{}", rand::random::<u32>());

    // Create pool using invoke()
    info!("Creating pool: {}", pool_name);
    let cmd = vec![format!(
        r#"{{"prefix": "osd pool create", "pool": "{}"}}"#,
        pool_name
    )];
    let result = mon_client
        .invoke(cmd, Bytes::new())
        .await
        .expect("Failed to create pool");

    info!("✓ Pool created");
    info!("Return value: {}", result.retval);
    assert_eq!(result.retval, 0, "Pool creation should succeed");

    // Wait for pool to be fully created
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Verify pool exists
    let cmd = vec![r#"{"prefix": "osd pool ls"}"#.to_string()];
    let result = mon_client
        .invoke(cmd, Bytes::new())
        .await
        .expect("Failed to list pools");

    let pool_list = String::from_utf8(result.outbl.to_vec()).unwrap();
    assert!(pool_list.contains(&pool_name), "Pool should exist in list");
    info!("✓ Pool verified");

    // Delete pool using invoke()
    info!("Deleting pool: {}", pool_name);
    let cmd = vec![format!(
        r#"{{"prefix": "osd pool rm", "pool": "{}", "pool2": "{}", "yes_i_really_really_mean_it": true}}"#,
        pool_name, pool_name
    )];
    let result = mon_client
        .invoke(cmd, Bytes::new())
        .await
        .expect("Failed to delete pool");

    info!("✓ Pool deleted");
    info!("Return value: {}", result.retval);
    assert_eq!(result.retval, 0, "Pool deletion should succeed");

    // Verify pool is gone
    let cmd = vec![r#"{"prefix": "osd pool ls"}"#.to_string()];
    let result = mon_client
        .invoke(cmd, Bytes::new())
        .await
        .expect("Failed to list pools");

    let pool_list = String::from_utf8(result.outbl.to_vec()).unwrap();
    assert!(
        !pool_list.contains(&pool_name),
        "Pool should not exist after deletion"
    );
    info!("✓ Pool deletion verified");

    info!("✓ Test completed successfully");
}

#[tokio::test]
#[ignore] // Requires a running Ceph cluster
async fn test_invoke_with_input_buffer() {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    info!("Testing invoke() with input buffer");
    info!("=================================");

    let config = TestConfig::from_env().expect("Failed to load test configuration");
    let mon_client = create_mon_client(&config)
        .await
        .expect("Failed to create MonClient");

    // Example: Send a command with an input buffer
    // This demonstrates the full invoke() API capability
    info!("Sending command with input buffer");

    let cmd = vec![r#"{"prefix": "config-key put", "key": "test-invoke-key"}"#.to_string()];
    let input_data = Bytes::from("test-value-from-invoke");

    let result = mon_client
        .invoke(cmd, input_data)
        .await
        .expect("Failed to put config key");

    info!("✓ Command with input buffer succeeded");
    info!("Return value: {}", result.retval);
    assert_eq!(result.retval, 0, "Config key put should succeed");

    // Verify by reading the key back
    let cmd = vec![r#"{"prefix": "config-key get", "key": "test-invoke-key"}"#.to_string()];
    let result = mon_client
        .invoke(cmd, Bytes::new())
        .await
        .expect("Failed to get config key");

    let value = String::from_utf8(result.outbl.to_vec()).unwrap();
    info!("✓ Retrieved value: {}", value);
    assert_eq!(value, "test-value-from-invoke", "Value should match");

    // Cleanup
    let cmd = vec![r#"{"prefix": "config-key del", "key": "test-invoke-key"}"#.to_string()];
    let _ = mon_client.invoke(cmd, Bytes::new()).await;

    info!("✓ Test completed successfully");
}
