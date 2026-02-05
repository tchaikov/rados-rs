//! Integration tests for pool management operations
//!
//! These tests require a running Ceph cluster and proper configuration.
//! Tests are marked with `#[ignore]` to skip them during regular `cargo test` runs.
//!
//! ## Running the tests
//!
//! **IMPORTANT**: These tests must be run sequentially with `--test-threads=1` to avoid
//! race conditions when multiple tests create/delete pools simultaneously.
//!
//! ```bash
//! export CEPH_CONF=/path/to/ceph.conf
//! cargo test --package monclient --test pool_operations -- --ignored --test-threads=1 --nocapture
//! ```
//!
//! ## Configuration
//!
//! The tests use the cephconfig crate to parse ceph.conf and automatically
//! discover monitor addresses and keyring paths.
//!
//! Required environment variables:
//! - `CEPH_CONF`: Path to ceph.conf file (default: /etc/ceph/ceph.conf)
//!

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

    let mon_client = Arc::new(
        monclient::MonClient::new(mon_config, message_bus)
            .await?,
    );

    // Initialize connection
    mon_client.init().await?;

    // Register MonClient handlers on MessageBus
    mon_client.clone().register_handlers().await?;

    info!("✓ Connected to monitor");

    // Wait a bit for monmap to be received
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    Ok(mon_client)
}

#[tokio::test]
#[ignore] // Requires a running Ceph cluster
async fn test_create_pool() {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    info!("Testing pool creation");
    info!("====================");

    let config = TestConfig::from_env().expect("Failed to load test configuration");
    let mon_client = create_mon_client(&config)
        .await
        .expect("Failed to create MonClient");

    let pool_name = format!("test-create-{}", rand::random::<u32>());

    info!("Creating pool: {}", pool_name);
    mon_client
        .create_pool(&pool_name, None)
        .await
        .expect("Failed to create pool");

    info!("✓ Pool created successfully");

    // Wait for osdmap to be updated with the new pool
    info!("Waiting for osdmap update...");
    mon_client
        .get_version("osdmap")
        .await
        .expect("Failed to get osdmap version");

    // Verify pool exists by listing pools
    let pools = mon_client.list_pools().await.expect("Failed to list pools");

    assert!(
        pools.contains(&pool_name),
        "Pool {} not found in pool list",
        pool_name
    );
    info!("✓ Pool verified in pool list");

    // Cleanup
    info!("Cleaning up pool: {}", pool_name);
    mon_client
        .delete_pool(&pool_name, true)
        .await
        .expect("Failed to delete pool");

    info!("✓ Test completed successfully");
}

#[tokio::test]
#[ignore] // Requires a running Ceph cluster
async fn test_delete_pool() {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    info!("Testing pool deletion");
    info!("====================");

    let config = TestConfig::from_env().expect("Failed to load test configuration");
    let mon_client = create_mon_client(&config)
        .await
        .expect("Failed to create MonClient");

    let pool_name = format!("test-delete-{}", rand::random::<u32>());

    // First create a pool
    info!("Creating pool: {}", pool_name);
    mon_client
        .create_pool(&pool_name, None)
        .await
        .expect("Failed to create pool");

    info!("✓ Pool created");

    // IMPORTANT: Wait for the pool to actually appear in a received incremental OSDMap
    // This prevents the create and delete from being batched into the same epoch by the monitor
    info!("Waiting for pool to appear in OSDMap...");
    for attempt in 0..10 {
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
        let pools = mon_client.list_pools().await.expect("Failed to list pools");
        if pools.contains(&pool_name) {
            info!("✓ Pool found in OSDMap after {} attempts", attempt + 1);
            break;
        }
        if attempt == 9 {
            panic!("Pool {} not found in OSDMap after creation", pool_name);
        }
    }

    // Wait an additional epoch to ensure the pool creation is fully committed
    tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;

    // Verify it exists
    let pools = mon_client.list_pools().await.expect("Failed to list pools");
    assert!(pools.contains(&pool_name), "Pool should exist");
    info!("✓ Pool verified");

    // Now delete it
    info!("Deleting pool: {}", pool_name);
    mon_client
        .delete_pool(&pool_name, true)
        .await
        .expect("Failed to delete pool");

    info!("✓ Pool deleted successfully");

    // Wait for pool to be removed (monitor needs time to update)
    // In low-activity test clusters, new epochs may only be created every 30-60 seconds
    info!("Waiting for pool to be removed...");
    for i in 0..60 {
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
        let pools = mon_client.list_pools().await.expect("Failed to list pools");
        if !pools.contains(&pool_name) {
            info!("✓ Pool removed after {} seconds", i + 1);
            break;
        }
        if i == 59 {
            // Try one more time with an explicit osdmap request
            info!("Pool still exists, requesting latest osdmap...");
            mon_client.get_version("osdmap").await.ok();
            tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

            let pools_final = mon_client.list_pools().await.expect("Failed to list pools");
            if pools_final.contains(&pool_name) {
                panic!(
                    "Pool {} still exists after deletion and 60+ seconds of waiting",
                    pool_name
                );
            }
        }
    }

    // Verify it's gone
    let pools = mon_client.list_pools().await.expect("Failed to list pools");
    assert!(
        !pools.contains(&pool_name),
        "Pool should not exist after deletion"
    );
    info!("✓ Pool deletion verified");

    info!("✓ Test completed successfully");
}

#[tokio::test]
#[ignore] // Requires a running Ceph cluster
async fn test_delete_pool_requires_confirmation() {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    info!("Testing pool deletion confirmation requirement");
    info!("==============================================");

    let config = TestConfig::from_env().expect("Failed to load test configuration");
    let mon_client = create_mon_client(&config)
        .await
        .expect("Failed to create MonClient");

    let pool_name = format!("test-confirm-{}", rand::random::<u32>());

    // First create a pool
    info!("Creating pool: {}", pool_name);
    mon_client
        .create_pool(&pool_name, None)
        .await
        .expect("Failed to create pool");

    info!("✓ Pool created");

    // Wait for osdmap to be updated with the new pool
    info!("Waiting for osdmap update...");
    mon_client
        .get_version("osdmap")
        .await
        .expect("Failed to get osdmap version");

    // Try to delete without confirmation (should fail)
    info!("Attempting to delete without confirmation");
    let result = mon_client.delete_pool(&pool_name, false).await;
    assert!(result.is_err(), "Delete should fail without confirmation");
    info!("✓ Delete correctly rejected without confirmation");

    // Verify pool still exists
    let pools = mon_client.list_pools().await.expect("Failed to list pools");
    assert!(
        pools.contains(&pool_name),
        "Pool should still exist after failed delete"
    );
    info!("✓ Pool still exists");

    // Cleanup with proper confirmation
    info!("Cleaning up pool: {}", pool_name);
    mon_client
        .delete_pool(&pool_name, true)
        .await
        .expect("Failed to delete pool");

    info!("✓ Test completed successfully");
}

#[tokio::test]
#[ignore] // Requires a running Ceph cluster
async fn test_list_pools() {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    info!("Testing pool listing");
    info!("===================");

    let config = TestConfig::from_env().expect("Failed to load test configuration");
    let mon_client = create_mon_client(&config)
        .await
        .expect("Failed to create MonClient");

    // List pools
    info!("Listing pools");
    let pools = mon_client.list_pools().await.expect("Failed to list pools");

    info!("✓ Found {} pools", pools.len());
    for pool in &pools {
        info!("  - {}", pool);
    }

    // Create a test pool
    let pool_name = format!("test-list-{}", rand::random::<u32>());
    info!("Creating test pool: {}", pool_name);
    mon_client
        .create_pool(&pool_name, None)
        .await
        .expect("Failed to create pool");

    // Wait for osdmap to be updated with the new pool
    info!("Waiting for osdmap update...");
    mon_client
        .get_version("osdmap")
        .await
        .expect("Failed to get osdmap version");

    // List again and verify
    let pools_after = mon_client.list_pools().await.expect("Failed to list pools");

    assert_eq!(
        pools_after.len(),
        pools.len() + 1,
        "Pool count should increase by 1"
    );
    assert!(
        pools_after.contains(&pool_name),
        "New pool should be in list"
    );
    info!("✓ Pool list updated correctly");

    // Cleanup
    info!("Cleaning up pool: {}", pool_name);
    mon_client
        .delete_pool(&pool_name, true)
        .await
        .expect("Failed to delete pool");

    info!("✓ Test completed successfully");
}

#[tokio::test]
#[ignore] // Requires a running Ceph cluster
async fn test_get_pool_stats() {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    info!("Testing pool statistics");
    info!("=======================");

    let config = TestConfig::from_env().expect("Failed to load test configuration");
    let mon_client = create_mon_client(&config)
        .await
        .expect("Failed to create MonClient");

    let pool_name = format!("test-stats-{}", rand::random::<u32>());

    // Create a pool
    info!("Creating pool: {}", pool_name);
    mon_client
        .create_pool(&pool_name, None)
        .await
        .expect("Failed to create pool");

    info!("✓ Pool created");

    // Wait for osdmap to be updated with the new pool
    info!("Waiting for osdmap update...");
    mon_client
        .get_version("osdmap")
        .await
        .expect("Failed to get osdmap version");

    // Get pool stats
    info!("Getting pool stats for: {}", pool_name);
    let stats = mon_client
        .get_pool_stats(&pool_name)
        .await
        .expect("Failed to get pool stats");

    info!("✓ Pool stats retrieved");
    info!("Stats: {}", stats);

    // Verify stats contain the pool name
    assert!(stats.contains(&pool_name), "Stats should contain pool name");
    info!("✓ Stats verified");

    // Cleanup
    info!("Cleaning up pool: {}", pool_name);
    mon_client
        .delete_pool(&pool_name, true)
        .await
        .expect("Failed to delete pool");

    info!("✓ Test completed successfully");
}

#[tokio::test]
#[ignore] // Requires a running Ceph cluster
async fn test_pool_workflow() {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    info!("Testing complete pool workflow");
    info!("==============================");

    let config = TestConfig::from_env().expect("Failed to load test configuration");
    let mon_client = create_mon_client(&config)
        .await
        .expect("Failed to create MonClient");

    let pool_name = format!("test-workflow-{}", rand::random::<u32>());

    // 1. List pools before
    info!("1. Listing pools before creation");
    let pools_before = mon_client.list_pools().await.expect("Failed to list pools");
    info!("   ✓ Found {} pools", pools_before.len());

    // 2. Create pool
    info!("2. Creating pool: {}", pool_name);
    mon_client
        .create_pool(&pool_name, None)
        .await
        .expect("Failed to create pool");
    info!("   ✓ Pool created");

    // Wait for osdmap to be updated with the new pool
    info!("   Waiting for osdmap update...");
    mon_client
        .get_version("osdmap")
        .await
        .expect("Failed to get osdmap version");

    // 3. List pools after creation
    info!("3. Listing pools after creation");
    let pools_after = mon_client.list_pools().await.expect("Failed to list pools");
    assert_eq!(pools_after.len(), pools_before.len() + 1);
    assert!(pools_after.contains(&pool_name));
    info!("   ✓ Pool verified in list");

    // 4. Get pool stats
    info!("4. Getting pool stats");
    let stats = mon_client
        .get_pool_stats(&pool_name)
        .await
        .expect("Failed to get pool stats");
    assert!(stats.contains(&pool_name));
    info!("   ✓ Pool stats retrieved");

    // 5. Delete pool
    info!("5. Deleting pool: {}", pool_name);
    mon_client
        .delete_pool(&pool_name, true)
        .await
        .expect("Failed to delete pool");
    info!("   ✓ Pool deleted");

    // 6. Verify deletion (with retry for eventual consistency)
    info!("6. Verifying deletion");
    let mut deletion_verified = false;
    for i in 0..20 {
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
        let pools_final = mon_client.list_pools().await.expect("Failed to list pools");
        if !pools_final.contains(&pool_name) {
            info!("   ✓ Pool removed after {} seconds", i + 1);
            assert_eq!(pools_final.len(), pools_before.len());
            deletion_verified = true;
            break;
        }
    }

    if !deletion_verified {
        // Try one more time with an explicit osdmap request
        info!("   Pool still exists, requesting latest osdmap...");
        mon_client.get_version("osdmap").await.ok();
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        let pools_final = mon_client.list_pools().await.expect("Failed to list pools");
        if !pools_final.contains(&pool_name) {
            assert_eq!(pools_final.len(), pools_before.len());
            deletion_verified = true;
        }
    }

    if !deletion_verified {
        panic!(
            "Pool {} still exists after deletion and 20+ seconds of waiting",
            pool_name
        );
    }

    info!("   ✓ Pool deletion verified");

    info!("✓ Complete workflow test passed!");
}
