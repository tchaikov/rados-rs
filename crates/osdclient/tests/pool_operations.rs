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
//! cargo test --package osdclient --test pool_operations -- --ignored --test-threads=1 --nocapture
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

/// Helper to create and initialize OSD client
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
    mon_client
        .wait_for_auth(std::time::Duration::from_secs(5))
        .await?;

    // Wait for MonMap to arrive - use event-driven wait
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

#[tokio::test]
#[ignore] // Requires a running Ceph cluster
async fn test_create_pool() {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    info!("Testing pool creation");
    info!("====================");

    let config = TestConfig::from_env().expect("Failed to load test configuration");
    let (osd_client, _mon_client) = create_osd_client(&config)
        .await
        .expect("Failed to create OSD client");

    let pool_name = format!("test-create-{}", rand::random::<u32>());

    info!("Creating pool: {}", pool_name);
    osd_client
        .create_pool(&pool_name, None)
        .await
        .expect("Failed to create pool");

    info!("✓ Pool created successfully");

    // Wait for pool to appear in OSDMap (poll efficiently)
    info!("Waiting for pool to appear in OSDMap...");
    let mut found = false;
    for i in 0..20 {
        // 20 * 50ms = 1s max
        let pool_infos = osd_client.list_pools().await.expect("Failed to list pools");
        let pool_names: Vec<String> = pool_infos.iter().map(|p| p.pool_name.clone()).collect();
        if pool_names.contains(&pool_name) {
            info!("✓ Pool found in OSDMap after {}ms", i * 50);
            found = true;
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    assert!(found, "Pool {} not found in pool list", pool_name);
    info!("✓ Pool verified in pool list");

    // Cleanup
    info!("Cleaning up pool: {}", pool_name);
    osd_client
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
    let (osd_client, _mon_client) = create_osd_client(&config)
        .await
        .expect("Failed to create OSD client");

    let pool_name = format!("test-delete-{}", rand::random::<u32>());

    // First create a pool
    info!("Creating pool: {}", pool_name);
    osd_client
        .create_pool(&pool_name, None)
        .await
        .expect("Failed to create pool");

    info!("✓ Pool created");

    // IMPORTANT: Wait for the pool to actually appear in a received incremental OSDMap
    // This prevents the create and delete from being batched into the same epoch by the monitor
    info!("Waiting for pool to appear in OSDMap...");
    let mut found = false;
    for i in 0..100 {
        // 100 * 50ms = 5s max
        let pool_infos = osd_client.list_pools().await.expect("Failed to list pools");
        let pool_names: Vec<String> = pool_infos.iter().map(|p| p.pool_name.clone()).collect();
        if pool_names.contains(&pool_name) {
            info!("✓ Pool found in OSDMap after {}ms", i * 50);
            found = true;
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    assert!(
        found,
        "Pool {} not found in OSDMap after creation",
        pool_name
    );

    // Wait an additional short period to ensure the pool creation is fully committed
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Verify it exists
    let pool_infos = osd_client.list_pools().await.expect("Failed to list pools");
    let pool_names: Vec<String> = pool_infos.iter().map(|p| p.pool_name.clone()).collect();
    assert!(pool_names.contains(&pool_name), "Pool should exist");
    info!("✓ Pool verified");

    // Now delete it
    info!("Deleting pool: {}", pool_name);
    osd_client
        .delete_pool(&pool_name, true)
        .await
        .expect("Failed to delete pool");

    info!("✓ Pool deleted successfully");

    // Wait for pool to be removed from OSDMap (poll efficiently)
    info!("Waiting for pool to be removed from OSDMap...");
    let mut removed = false;
    for i in 0..100 {
        // 100 * 50ms = 5s max
        let pool_infos = osd_client.list_pools().await.expect("Failed to list pools");
        let pool_names: Vec<String> = pool_infos.iter().map(|p| p.pool_name.clone()).collect();
        if !pool_names.contains(&pool_name) {
            info!("✓ Pool removed after {}ms", i * 50);
            removed = true;
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    assert!(
        removed,
        "Pool {} still exists after deletion and 5s of waiting",
        pool_name
    );

    // Verify it's gone
    let pool_infos = osd_client.list_pools().await.expect("Failed to list pools");
    assert!(
        !pool_infos.iter().any(|p| p.pool_name == pool_name),
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
    let (osd_client, _mon_client) = create_osd_client(&config)
        .await
        .expect("Failed to create OSD client");

    let pool_name = format!("test-confirm-{}", rand::random::<u32>());

    // First create a pool
    info!("Creating pool: {}", pool_name);
    osd_client
        .create_pool(&pool_name, None)
        .await
        .expect("Failed to create pool");

    info!("✓ Pool created");

    // Wait for osdmap to be updated with the new pool
    info!("Waiting for osdmap update...");
    _mon_client
        .get_version("osdmap")
        .await
        .expect("Failed to get osdmap version");

    // Try to delete without confirmation (should fail)
    info!("Attempting to delete without confirmation");
    let result = osd_client.delete_pool(&pool_name, false).await;
    assert!(result.is_err(), "Delete should fail without confirmation");
    info!("✓ Delete correctly rejected without confirmation");

    // Verify pool still exists
    let pool_infos = osd_client.list_pools().await.expect("Failed to list pools");
    assert!(
        pool_infos.iter().any(|p| p.pool_name == pool_name),
        "Pool should still exist after failed delete"
    );
    info!("✓ Pool still exists");

    // Cleanup with proper confirmation
    info!("Cleaning up pool: {}", pool_name);
    osd_client
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
    let (osd_client, _mon_client) = create_osd_client(&config)
        .await
        .expect("Failed to create OSD client");

    // List pools
    info!("Listing pools");
    let pool_infos = osd_client.list_pools().await.expect("Failed to list pools");

    info!("✓ Found {} pools", pool_infos.len());
    for pool_info in &pool_infos {
        info!("  - {} (id: {})", pool_info.pool_name, pool_info.pool_id);
    }

    // Create a test pool
    let pool_name = format!("test-list-{}", rand::random::<u32>());
    info!("Creating test pool: {}", pool_name);
    osd_client
        .create_pool(&pool_name, None)
        .await
        .expect("Failed to create pool");

    // Wait for osdmap to be updated with the new pool
    info!("Waiting for osdmap update...");
    _mon_client
        .get_version("osdmap")
        .await
        .expect("Failed to get osdmap version");

    // List again and verify
    let pools_after = osd_client.list_pools().await.expect("Failed to list pools");

    // Don't check exact count (other tests may be creating pools in parallel)
    // Just verify our specific pool exists
    assert!(
        pools_after.iter().any(|p| p.pool_name == pool_name),
        "New pool should be in list"
    );
    info!("✓ Pool list updated correctly (our pool found)");

    // Cleanup
    info!("Cleaning up pool: {}", pool_name);
    osd_client
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
    let (osd_client, _mon_client) = create_osd_client(&config)
        .await
        .expect("Failed to create OSD client");

    let pool_name = format!("test-workflow-{}", rand::random::<u32>());

    // 1. List pools before
    info!("1. Listing pools before creation");
    let pools_before = osd_client.list_pools().await.expect("Failed to list pools");
    info!("   ✓ Found {} pools", pools_before.len());

    // 2. Create pool
    info!("2. Creating pool: {}", pool_name);
    osd_client
        .create_pool(&pool_name, None)
        .await
        .expect("Failed to create pool");
    info!("   ✓ Pool created");

    // Wait for osdmap to be updated with the new pool
    info!("   Waiting for osdmap update...");
    _mon_client
        .get_version("osdmap")
        .await
        .expect("Failed to get osdmap version");

    // 3. List pools after creation
    info!("3. Listing pools after creation");
    let pools_after = osd_client.list_pools().await.expect("Failed to list pools");
    // Don't check exact count (other tests may be running in parallel)
    // Just verify our specific pool exists
    assert!(pools_after.iter().any(|p| p.pool_name == pool_name));
    info!("   ✓ Pool verified in list");

    // 4. Delete pool
    info!("4. Deleting pool: {}", pool_name);
    osd_client
        .delete_pool(&pool_name, true)
        .await
        .expect("Failed to delete pool");
    info!("   ✓ Pool deleted");

    // 5. Verify deletion (with retry for eventual consistency)
    info!("5. Verifying deletion");
    let mut deletion_verified = false;
    for i in 0..100 {
        // 100 * 50ms = 5s max
        let pools_final = osd_client.list_pools().await.expect("Failed to list pools");
        if !pools_final.iter().any(|p| p.pool_name == pool_name) {
            info!("   ✓ Pool removed after {}ms", i * 50);
            deletion_verified = true;
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    if !deletion_verified {
        panic!(
            "Pool {} still exists after deletion and 5s of waiting",
            pool_name
        );
    }

    info!("   ✓ Pool deletion verified");

    info!("✓ Complete workflow test passed!");
}
