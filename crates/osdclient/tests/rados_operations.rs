//! Integration tests for RADOS operations
//!
//! These tests require a running Ceph cluster and proper configuration.
//! Tests are marked with `#[ignore]` to skip them during regular `cargo test` runs.
//!
//! To run these tests:
//! ```bash
//! export CEPH_CONF=/path/to/ceph.conf
//! export CEPH_TEST_POOL=test-pool
//! cargo test --package osdclient --test rados_operations -- --ignored --nocapture
//! ```
//!
//! ## Configuration
//!
//! The tests use the cephconfig crate to parse ceph.conf and automatically
//! discover monitor addresses and keyring paths.
//!
//! Environment variables:
//! - `CEPH_CONF`: Path to ceph.conf file (default: /etc/ceph/ceph.conf)
//! - `CEPH_TEST_POOL`: Pool name or ID to use for testing (default: test-pool)
//!
//! Note: The keyring path is automatically discovered from ceph.conf.
//! Do not set environment variables other than CEPH_CONF.
//!

use bytes::Bytes;
use std::path::Path;
use std::sync::Arc;
use tracing::info;

/// Helper to get test configuration from environment and ceph.conf
struct TestConfig {
    mon_addrs: Vec<String>,
    keyring_path: String,
    entity_name: String,
    pool: String,
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

        // Get keyring path from ceph.conf
        let keyring_path = ceph_config
            .keyring()
            .unwrap_or_else(|_| "/etc/ceph/ceph.client.admin.keyring".to_string());

        // Get entity name
        let entity_name = ceph_config.entity_name();

        // Get test pool
        let pool = std::env::var("CEPH_TEST_POOL").unwrap_or_else(|_| "test-pool".to_string());

        Ok(Self {
            mon_addrs,
            keyring_path,
            entity_name,
            pool,
        })
    }
}

/// Helper to create and initialize an OSD client
async fn create_osd_client(
    config: &TestConfig,
) -> Result<(Arc<osdclient::OSDClient>, Arc<monclient::MonClient>), Box<dyn std::error::Error>> {
    // Create shared MessageBus FIRST - both MonClient and OSDClient must use the same bus
    let message_bus = Arc::new(msgr2::MessageBus::new());

    // Create MonClient with shared MessageBus
    let mon_config = monclient::MonClientConfig {
        entity_name: config.entity_name.clone(),
        mon_addrs: config.mon_addrs.clone(),
        keyring_path: config.keyring_path.clone(),
        ..Default::default()
    };

    let mon_client =
        Arc::new(monclient::MonClient::new(mon_config, Arc::clone(&message_bus)).await?);

    // Initialize connection
    mon_client.init().await?;

    // Register MonClient handlers on MessageBus
    mon_client.clone().register_handlers().await?;

    info!("✓ Connected to monitor");

    // Wait for authentication to fully complete with all service tickets
    // This ensures OSD service tickets are available before creating OSDClient
    mon_client
        .wait_for_auth(std::time::Duration::from_secs(5))
        .await?;

    // Wait for MonMap to arrive (contains FSID) - use event-driven wait
    mon_client
        .wait_for_monmap(std::time::Duration::from_secs(2))
        .await?;

    // Create OSD client with unique client_inc
    let client_inc = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32;

    let osd_config = osdclient::OSDClientConfig {
        entity_name: config.entity_name.clone(),
        keyring_path: Some(config.keyring_path.clone()),
        client_inc,
        ..Default::default()
    };

    // Get FSID from MonClient
    let fsid = mon_client.get_fsid().await;

    // Create OSDClient with the SAME MessageBus that MonClient is using
    let osd_client = osdclient::OSDClient::new(
        osd_config,
        fsid,
        Arc::clone(&mon_client),
        Arc::clone(&message_bus),
    )
    .await?;
    info!("✓ OSD client created");

    // Register OSDClient on MessageBus to receive OSDMap messages
    osd_client.clone().register_handlers().await?;
    info!("✓ OSDClient registered on MessageBus");

    // NOW subscribe to OSDMap - OSDClient is ready to receive
    mon_client.subscribe("osdmap", 0, 0).await?;

    // Wait for OSDMap to arrive - use event-driven wait
    osd_client
        .wait_for_osdmap(std::time::Duration::from_secs(2))
        .await?;
    info!("✓ OSDMap received");

    Ok((osd_client, mon_client))
}

/// Helper to parse pool name or ID
async fn parse_pool(
    pool: &str,
    osd_client: &Arc<osdclient::OSDClient>,
) -> Result<u64, Box<dyn std::error::Error>> {
    // Try parsing as integer first
    if let Ok(id) = pool.parse::<u64>() {
        return Ok(id);
    }

    // Otherwise, look up pool by name in OSDMap
    let osdmap = match osd_client.get_osdmap().await {
        Ok(map) => map,
        Err(_) => return Err("OSDMap not available".into()),
    };

    // Search for pool by name
    for (pool_id, pool_name) in &osdmap.pool_name {
        if pool_name == pool {
            return Ok(*pool_id);
        }
    }

    Err(format!("Pool '{}' not found", pool).into())
}

#[tokio::test]
#[ignore] // Requires a running Ceph cluster
async fn test_write_operation() {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    info!("Testing RADOS WRITE operation");
    info!("==============================");

    let config = TestConfig::from_env().expect("Failed to load test configuration");
    let (osd_client, _mon_client) = create_osd_client(&config)
        .await
        .expect("Failed to create OSD client");

    let pool_id = parse_pool(&config.pool, &osd_client)
        .await
        .expect("Failed to parse pool");

    let object_name = format!("test-write-{}", rand::random::<u32>());
    let test_data = Bytes::from("Hello from RADOS write test!");

    info!("Writing object: {}", object_name);
    info!("Data: {:?}", String::from_utf8_lossy(&test_data));

    let result = osd_client
        .write_full(pool_id, &object_name, test_data.clone())
        .await
        .expect("Write operation failed");

    info!("✓ Write successful, version: {}", result.version);

    // Cleanup
    osd_client
        .delete(pool_id, &object_name)
        .await
        .expect("Cleanup failed");

    info!("✓ Test completed successfully");
}

#[tokio::test]
#[ignore] // Requires a running Ceph cluster
async fn test_read_operation() {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    info!("Testing RADOS READ operation");
    info!("=============================");

    let config = TestConfig::from_env().expect("Failed to load test configuration");
    let (osd_client, _mon_client) = create_osd_client(&config)
        .await
        .expect("Failed to create OSD client");

    let pool_id = parse_pool(&config.pool, &osd_client)
        .await
        .expect("Failed to parse pool");

    let object_name = format!("test-read-{}", rand::random::<u32>());
    let test_data = Bytes::from("Hello from RADOS read test!");

    // First write the object
    info!("Writing test object: {}", object_name);
    osd_client
        .write_full(pool_id, &object_name, test_data.clone())
        .await
        .expect("Write operation failed");

    // Now read it back
    info!("Reading object: {}", object_name);
    let result = osd_client
        .read(pool_id, &object_name, 0, test_data.len() as u64)
        .await
        .expect("Read operation failed");

    info!("✓ Read successful, version: {}", result.version);
    info!("Data: {:?}", String::from_utf8_lossy(&result.data));

    // Verify data matches
    assert_eq!(
        result.data, test_data,
        "Read data does not match written data"
    );
    info!("✓ Data verification successful");

    // Cleanup
    osd_client
        .delete(pool_id, &object_name)
        .await
        .expect("Cleanup failed");

    info!("✓ Test completed successfully");
}

#[tokio::test]
#[ignore] // Requires a running Ceph cluster
async fn test_stat_operation() {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    info!("Testing RADOS STAT operation");
    info!("=============================");

    let config = TestConfig::from_env().expect("Failed to load test configuration");
    let (osd_client, _mon_client) = create_osd_client(&config)
        .await
        .expect("Failed to create OSD client");

    let pool_id = parse_pool(&config.pool, &osd_client)
        .await
        .expect("Failed to parse pool");

    let object_name = format!("test-stat-{}", rand::random::<u32>());
    let test_data = Bytes::from("Hello from RADOS stat test!");

    // First write the object
    info!("Writing test object: {}", object_name);
    osd_client
        .write_full(pool_id, &object_name, test_data.clone())
        .await
        .expect("Write operation failed");

    // Now stat it
    info!("Getting object stats: {}", object_name);
    let stat = osd_client
        .stat(pool_id, &object_name)
        .await
        .expect("Stat operation failed");

    info!("✓ Stat successful");
    info!("  Size: {} bytes", stat.size);
    info!("  Mtime: {:?}", stat.mtime);

    // Verify size matches
    assert_eq!(
        stat.size as usize,
        test_data.len(),
        "Object size does not match written data size"
    );
    info!("✓ Size verification successful");

    // Cleanup
    osd_client
        .delete(pool_id, &object_name)
        .await
        .expect("Cleanup failed");

    info!("✓ Test completed successfully");
}

#[tokio::test]
#[ignore] // Requires a running Ceph cluster
async fn test_remove_operation() {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    info!("Testing RADOS REMOVE operation");
    info!("===============================");

    let config = TestConfig::from_env().expect("Failed to load test configuration");
    let (osd_client, _mon_client) = create_osd_client(&config)
        .await
        .expect("Failed to create OSD client");

    let pool_id = parse_pool(&config.pool, &osd_client)
        .await
        .expect("Failed to parse pool");

    let object_name = format!("test-remove-{}", rand::random::<u32>());
    let test_data = Bytes::from("Hello from RADOS remove test!");

    // First write the object
    info!("Writing test object: {}", object_name);
    osd_client
        .write_full(pool_id, &object_name, test_data.clone())
        .await
        .expect("Write operation failed");

    // Verify it exists
    info!("Verifying object exists: {}", object_name);
    osd_client
        .stat(pool_id, &object_name)
        .await
        .expect("Object should exist before removal");
    info!("✓ Object exists");

    // Now remove it
    info!("Removing object: {}", object_name);
    osd_client
        .delete(pool_id, &object_name)
        .await
        .expect("Remove operation failed");
    info!("✓ Remove successful");

    // Verify it's gone (stat should fail)
    info!("Verifying object is removed: {}", object_name);
    let stat_result = osd_client.stat(pool_id, &object_name).await;
    assert!(
        stat_result.is_err(),
        "Object should not exist after removal"
    );
    info!("✓ Object successfully removed");

    info!("✓ Test completed successfully");
}

#[tokio::test]
#[ignore] // Requires a running Ceph cluster
async fn test_write_read_stat_remove_workflow() {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    info!("Testing complete RADOS workflow");
    info!("================================");

    let config = TestConfig::from_env().expect("Failed to load test configuration");
    let (osd_client, _mon_client) = create_osd_client(&config)
        .await
        .expect("Failed to create OSD client");

    let pool_id = parse_pool(&config.pool, &osd_client)
        .await
        .expect("Failed to parse pool");

    let object_name = format!("test-workflow-{}", rand::random::<u32>());
    let test_data = Bytes::from("Hello from RADOS workflow test!");

    // 1. Write
    info!("1. Writing object: {}", object_name);
    let write_result = osd_client
        .write_full(pool_id, &object_name, test_data.clone())
        .await
        .expect("Write operation failed");
    info!("   ✓ Write successful, version: {}", write_result.version);

    // 2. Read
    info!("2. Reading object: {}", object_name);
    let read_result = osd_client
        .read(pool_id, &object_name, 0, test_data.len() as u64)
        .await
        .expect("Read operation failed");
    assert_eq!(read_result.data, test_data, "Data mismatch");
    info!("   ✓ Read successful, data matches");

    // 3. Stat
    info!("3. Getting object stats: {}", object_name);
    let stat_result = osd_client
        .stat(pool_id, &object_name)
        .await
        .expect("Stat operation failed");
    assert_eq!(stat_result.size as usize, test_data.len(), "Size mismatch");
    info!("   ✓ Stat successful, size: {} bytes", stat_result.size);

    // 4. Remove
    info!("4. Removing object: {}", object_name);
    osd_client
        .delete(pool_id, &object_name)
        .await
        .expect("Remove operation failed");
    info!("   ✓ Remove successful");

    // 5. Verify removal
    info!("5. Verifying removal: {}", object_name);
    let stat_after_remove = osd_client.stat(pool_id, &object_name).await;
    assert!(
        stat_after_remove.is_err(),
        "Object should not exist after removal"
    );
    info!("   ✓ Object successfully removed");

    info!("✓ Complete workflow test passed!");
}
