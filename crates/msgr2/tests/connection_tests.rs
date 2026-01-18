//! Integration tests for msgr2 connection functionality
//!
//! These tests require a running Ceph cluster and the CEPH_MON_ADDR environment variable to be set.
//! Tests will fail if CEPH_MON_ADDR is not set or the cluster is not accessible.
//!
//! To run these tests:
//! ```bash
//! export CEPH_MON_ADDR=<your_monitor_address:port>
//! cargo test --test connection_tests -- --nocapture
//! ```
//!
//! ## Authentication Configuration
//!
//! The tests support both CephX authentication and no-auth clusters:
//!
//! ### For clusters with authentication enabled (CephX):
//! ```bash
//! # Option 1: Point to your keyring file (recommended)
//! export CEPH_KEYRING=/path/to/ceph/build/keyring
//! 
//! # Option 2: Explicitly set auth method
//! export CEPH_AUTH_METHOD=cephx
//! export CEPH_KEYRING=/path/to/ceph/build/keyring
//! ```
//!
//! ### For clusters with authentication disabled:
//! ```bash
//! # Option 1: Explicit (recommended for CI)
//! export CEPH_AUTH_METHOD=none
//!
//! # Option 2: Auto-detection (will use no-auth if keyring file doesn't exist)
//! # No additional configuration needed
//! ```
//!
//! ### Auto-detection behavior:
//! - If CEPH_AUTH_METHOD is not set, the client will auto-detect:
//!   - If CEPH_KEYRING is set and the file exists → use CephX authentication
//!   - If /etc/ceph/ceph.client.admin.keyring exists → use CephX authentication
//!   - Otherwise → use no authentication
//!

use msgr2::protocol::Connection;
use msgr2::ConnectionConfig;
use std::net::SocketAddr;

/// Helper function to get the Ceph monitor address from environment variable
/// Panics if CEPH_MON_ADDR is not set or invalid
fn get_ceph_mon_addr() -> SocketAddr {
    std::env::var("CEPH_MON_ADDR")
        .expect("CEPH_MON_ADDR environment variable not set. Set it with: export CEPH_MON_ADDR=<monitor_address:port>")
        .parse()
        .expect("Failed to parse CEPH_MON_ADDR as a valid socket address (format: IP:PORT)")
}

#[tokio::test]
async fn test_compression_disabled() {
    let addr = get_ceph_mon_addr();

    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    tracing::info!("Testing msgr2 connection WITHOUT compression feature");
    tracing::info!("===================================================");

    // Explicitly disable compression
    let config = ConnectionConfig::without_compression();
    tracing::info!(
        "Config: supported_features={:#x}",
        config.supported_features
    );

    let mut conn = Connection::connect(addr, config)
        .await
        .expect("Failed to establish connection with compression disabled");
    tracing::info!("✓ Connection established with compression disabled");

    conn.establish_session()
        .await
        .expect("Failed to establish session");
    tracing::info!("✓ Session established successfully");

    tracing::info!("🎉 Compression-disabled test completed!");
}

#[tokio::test]
async fn test_compression_enabled() {
    let addr = get_ceph_mon_addr();

    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    tracing::info!("Testing msgr2 connection WITH compression feature");
    tracing::info!("================================================");

    // Explicitly enable compression
    let config = ConnectionConfig::with_compression();
    tracing::info!(
        "Config: supported_features={:#x}",
        config.supported_features
    );

    let mut conn = Connection::connect(addr, config)
        .await
        .expect("Failed to establish connection with compression enabled");
    tracing::info!("✓ Connection established with compression enabled");

    conn.establish_session()
        .await
        .expect("Failed to establish session");
    tracing::info!("✓ Session established successfully");

    tracing::info!("🎉 Compression-enabled test completed!");
}

#[tokio::test]
async fn test_crc_mode() {
    let addr = get_ceph_mon_addr();

    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    tracing::info!("Testing msgr2 connection with CRC mode (no encryption)");
    tracing::info!("=======================================================");

    // Request CRC mode (no encryption)
    let config = ConnectionConfig::prefer_crc_mode();
    tracing::info!("Config: preferred_modes={:?}", config.preferred_modes);

    let mut conn = Connection::connect(addr, config)
        .await
        .expect("Failed to establish connection with CRC mode");
    tracing::info!("✓ Connection established with CRC mode requested");

    conn.establish_session()
        .await
        .expect("Failed to establish session");
    tracing::info!("✓ Session established successfully");

    tracing::info!("🎉 CRC mode test completed!");
}

#[tokio::test]
async fn test_secure_mode() {
    let addr = get_ceph_mon_addr();

    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    tracing::info!("Testing msgr2 connection with SECURE mode (encryption)");
    tracing::info!("========================================================");

    // Request SECURE mode (encryption)
    let config = ConnectionConfig::prefer_secure_mode();
    tracing::info!("Config: preferred_modes={:?}", config.preferred_modes);

    let mut conn = Connection::connect(addr, config)
        .await
        .expect("Failed to establish connection with SECURE mode");
    tracing::info!("✓ Connection established with SECURE mode requested");

    conn.establish_session()
        .await
        .expect("Failed to establish session");
    tracing::info!("✓ Session established successfully");

    tracing::info!("🎉 SECURE mode test completed!");
}

#[tokio::test]
async fn test_session_connecting() {
    let addr = get_ceph_mon_addr();

    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    tracing::info!("Testing SessionConnecting with real Ceph cluster");
    tracing::info!("==============================================");

    // Connect to Ceph cluster using the high-level API with default config
    let mut conn = Connection::connect(addr, ConnectionConfig::default())
        .await
        .expect("Failed to establish connection");
    tracing::info!("✓ Connection established and banner exchanged");

    // Establish session (HELLO, AUTH, SESSION_CONNECTING)
    conn.establish_session()
        .await
        .expect("Failed to establish session");
    tracing::info!("✓ Session established successfully");

    tracing::info!("🎉 SessionConnecting SERVER_IDENT test completed successfully!");
}
