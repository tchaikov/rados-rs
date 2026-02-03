//! Integration tests for OSDClient
//!
//! These tests require a running Ceph cluster.
//!
//! Configuration can be provided in two ways:
//! 1. Via environment variables (legacy):
//!    CEPH_MON_ADDR=v2:127.0.0.1:3300 CEPH_KEYRING=/path/to/keyring cargo test
//! 2. Via ceph.conf file (recommended):
//!    CEPH_CONF=/path/to/ceph.conf cargo test
//!
//! Example:
//!   CEPH_CONF=/home/kefu/dev/ceph/build/ceph.conf cargo test --package osdclient --test integration_test

use bytes::Bytes;
use std::env;
use std::sync::Arc;
use uuid::Uuid;

/// Test configuration
struct TestConfig {
    mon_addrs: Vec<String>,
    keyring_path: String,
    entity_name: String,
    pool_id: i64,
}

impl TestConfig {
    fn from_env() -> Self {
        // Try to load from ceph.conf first
        if let Ok(conf_path) = env::var("CEPH_CONF") {
            if let Ok(config) = Self::from_ceph_conf(&conf_path) {
                return config;
            }
            eprintln!("Warning: Failed to parse CEPH_CONF, falling back to environment variables");
        }

        // Fall back to environment variables
        let mon_addr =
            env::var("CEPH_MON_ADDR").expect("CEPH_MON_ADDR must be set for integration tests");

        let keyring_path = env::var("CEPH_KEYRING")
            .unwrap_or_else(|_| "/home/kefu/dev/ceph/build/keyring".to_string());

        let entity_name =
            env::var("CEPH_ENTITY_NAME").unwrap_or_else(|_| "client.admin".to_string());

        let pool_id = env::var("CEPH_POOL_ID")
            .unwrap_or_else(|_| "1".to_string())
            .parse()
            .expect("CEPH_POOL_ID must be a valid integer");

        Self {
            mon_addrs: vec![mon_addr],
            keyring_path,
            entity_name,
            pool_id,
        }
    }

    fn from_ceph_conf(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let config = cephconfig::CephConfig::from_file(path)?;

        // Get monitor addresses (prefer v2)
        let mon_addrs = config.mon_addrs()?;

        // Get keyring path
        let keyring_path = config.keyring()?;

        // Get entity name (defaults to client.admin)
        let entity_name = config.entity_name();

        // Pool ID still comes from environment variable
        let pool_id = env::var("CEPH_POOL_ID")
            .unwrap_or_else(|_| "2".to_string()) // Default to pool 2 (test pool)
            .parse()
            .unwrap_or(2);

        Ok(Self {
            mon_addrs,
            keyring_path,
            entity_name,
            pool_id,
        })
    }
}

/// Setup test environment
async fn setup() -> (Arc<monclient::MonClient>, osdclient::OSDClient, i64) {
    // Initialize tracing (ignore error if already initialized)
    let _ = tracing_subscriber::fmt().try_init();

    let config = TestConfig::from_env();

    // Create MonClient
    let mon_config = monclient::MonClientConfig {
        entity_name: config.entity_name.clone(),
        mon_addrs: config.mon_addrs.clone(),
        keyring_path: config.keyring_path.clone(),
        ..Default::default()
    };

    let mon_client = Arc::new(
        monclient::MonClient::new(mon_config)
            .await
            .expect("Failed to create MonClient"),
    );

    // Initialize connection
    mon_client
        .init()
        .await
        .expect("Failed to initialize MonClient");

    // Wait for authentication to fully complete with all service tickets
    // This ensures OSD service tickets are available before creating OSDClient
    mon_client
        .wait_for_auth(std::time::Duration::from_secs(5))
        .await
        .expect("Failed to complete authentication");

    // Subscribe to OSDMap
    mon_client
        .subscribe("osdmap", 0, 0)
        .await
        .expect("Failed to subscribe to OSDMap");

    // Wait for OSDMap to arrive (increased timeout for slower systems)
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Create OSD client
    let osd_config = osdclient::OSDClientConfig {
        entity_name: config.entity_name.clone(),
        ..Default::default()
    };

    let osd_client = osdclient::OSDClient::new(osd_config, Arc::clone(&mon_client))
        .await
        .expect("Failed to create OSDClient");

    (mon_client, osd_client, config.pool_id)
}

/// Generate unique object name for test
fn test_object_name(prefix: &str) -> String {
    format!("test-{}-{}", prefix, Uuid::new_v4())
}

#[tokio::test]
async fn test_write_read_roundtrip() {
    let (_mon, osd, pool) = setup().await;

    let object = test_object_name("roundtrip");
    let test_data = Bytes::from("Hello, RADOS! This is a test.");

    // Write object
    let write_result = osd
        .write_full(pool, &object, test_data.clone())
        .await
        .expect("write_full failed");

    assert!(write_result.version > 0, "Version should be positive");

    // Read back
    let read_result = osd
        .read(pool, &object, 0, test_data.len() as u64)
        .await
        .expect("read failed");

    // Verify data matches
    assert_eq!(
        read_result.data, test_data,
        "Read data should match written data"
    );
    assert_eq!(
        read_result.version, write_result.version,
        "Version should match"
    );

    // Cleanup
    osd.delete(pool, &object).await.ok();
}

#[tokio::test]
async fn test_stat_operation() {
    let (_mon, osd, pool) = setup().await;

    let object = test_object_name("stat");
    let test_data = Bytes::from("Test data for stat operation");
    let data_len = test_data.len();

    // Write object
    osd.write_full(pool, &object, test_data)
        .await
        .expect("write_full failed");

    // Give the cluster a moment to persist the write
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Stat object
    let stat_result = osd.stat(pool, &object).await.expect("stat failed");

    // Verify size
    assert_eq!(
        stat_result.size, data_len as u64,
        "Stat size should match written data size"
    );

    // Verify mtime exists (SystemTime is always set)
    // Just check that we can access it without panicking
    let _ = stat_result.mtime;

    // Cleanup
    osd.delete(pool, &object).await.ok();
}

#[tokio::test]
async fn test_remove_operation() {
    let (_mon, osd, pool) = setup().await;

    let object = test_object_name("remove");
    let test_data = Bytes::from("Data to be removed");

    // Write object
    osd.write_full(pool, &object, test_data)
        .await
        .expect("write_full failed");

    // Verify exists with stat
    osd.stat(pool, &object)
        .await
        .expect("stat should succeed before delete");

    // Remove object
    osd.delete(pool, &object).await.expect("delete failed");

    // Verify stat fails after delete
    let stat_result = osd.stat(pool, &object).await;
    assert!(stat_result.is_err(), "stat should fail after delete");
}

#[tokio::test]
async fn test_write_full_vs_partial() {
    let (_mon, osd, pool) = setup().await;

    let object = test_object_name("partial");

    // Write full object
    let initial_data = Bytes::from("AAAAAAAAAA");
    osd.write_full(pool, &object, initial_data.clone())
        .await
        .expect("write_full failed");

    // Partial write at offset
    let partial_data = Bytes::from("BBB");
    osd.write(pool, &object, 3, partial_data.clone())
        .await
        .expect("write failed");

    // Read back entire object
    let read_result = osd.read(pool, &object, 0, 100).await.expect("read failed");

    // Verify: "AAABBBAAAA"
    let expected = Bytes::from("AAABBBAAAA");
    assert_eq!(
        read_result.data, expected,
        "Partial write should modify only specified range"
    );

    // Cleanup
    osd.delete(pool, &object).await.ok();
}

#[tokio::test]
async fn test_nonexistent_object() {
    let (_mon, osd, pool) = setup().await;

    let object = test_object_name("nonexistent");

    // Try to read non-existent object
    let read_result = osd.read(pool, &object, 0, 100).await;
    assert!(
        read_result.is_err(),
        "Reading non-existent object should fail"
    );

    // Try to stat non-existent object
    let stat_result = osd.stat(pool, &object).await;
    assert!(
        stat_result.is_err(),
        "Statting non-existent object should fail"
    );
}

#[tokio::test]
async fn test_large_object() {
    let (_mon, osd, pool) = setup().await;

    let object = test_object_name("large");

    // Create 1MB of data
    let data_size = 1024 * 1024;
    let test_data: Vec<u8> = (0..data_size).map(|i| (i % 256) as u8).collect();
    let test_data = Bytes::from(test_data);

    // Write large object
    osd.write_full(pool, &object, test_data.clone())
        .await
        .expect("write_full failed for large object");

    // Read back
    let read_result = osd
        .read(pool, &object, 0, data_size as u64)
        .await
        .expect("read failed for large object");

    // Verify
    assert_eq!(
        read_result.data.len(),
        data_size,
        "Read size should match written size"
    );
    assert_eq!(
        read_result.data, test_data,
        "Large object data should match"
    );

    // Stat
    let stat_result = osd
        .stat(pool, &object)
        .await
        .expect("stat failed for large object");
    assert_eq!(
        stat_result.size, data_size as u64,
        "Stat size should match for large object"
    );

    // Cleanup
    osd.delete(pool, &object).await.ok();
}

#[tokio::test]
async fn test_empty_object() {
    let (_mon, osd, pool) = setup().await;

    let object = test_object_name("empty");
    let test_data = Bytes::new();

    // Write empty object
    osd.write_full(pool, &object, test_data.clone())
        .await
        .expect("write_full failed for empty object");

    // Read back
    let read_result = osd
        .read(pool, &object, 0, 100)
        .await
        .expect("read failed for empty object");

    // Verify
    assert_eq!(
        read_result.data.len(),
        0,
        "Empty object should have zero length"
    );

    // Stat
    let stat_result = osd
        .stat(pool, &object)
        .await
        .expect("stat failed for empty object");
    assert_eq!(stat_result.size, 0, "Empty object size should be 0");

    // Cleanup
    osd.delete(pool, &object).await.ok();
}

#[tokio::test]
async fn test_overwrite_object() {
    let (_mon, osd, pool) = setup().await;

    let object = test_object_name("overwrite");

    // Write initial data
    let data1 = Bytes::from("First version");
    let result1 = osd
        .write_full(pool, &object, data1.clone())
        .await
        .expect("first write_full failed");

    // Overwrite with new data
    let data2 = Bytes::from("Second version - longer");
    let result2 = osd
        .write_full(pool, &object, data2.clone())
        .await
        .expect("second write_full failed");

    // Version should increase
    eprintln!(
        "DEBUG: result1.version={}, result2.version={}",
        result1.version, result2.version
    );
    assert!(
        result2.version > result1.version,
        "Version should increase after overwrite: result1.version={}, result2.version={}",
        result1.version,
        result2.version
    );

    // Read back
    let read_result = osd.read(pool, &object, 0, 100).await.expect("read failed");

    // Should get second version
    assert_eq!(
        read_result.data, data2,
        "Should read second version after overwrite"
    );

    // Cleanup
    osd.delete(pool, &object).await.ok();
}
