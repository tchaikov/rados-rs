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
//! The tests support both CephX authentication and no-auth clusters.
//! Authentication method can be controlled via the `CEPH_AUTH_METHOD` environment variable:
//!
//! ### For clusters with authentication disabled (no-auth):
//! ```bash
//! export CEPH_AUTH_METHOD=none
//! ```
//!
//! ### For clusters with authentication enabled (CephX):
//! ```bash
//! export CEPH_AUTH_METHOD=cephx
//! export CEPH_KEYRING=/path/to/ceph/build/keyring
//! ```
//!
//! ### Auto-detection behavior:
//! If `CEPH_AUTH_METHOD` is not set, the protocol layer will auto-detect:
//! - If `CEPH_KEYRING` is set and the file exists → use CephX authentication
//! - If `/etc/ceph/ceph.client.admin.keyring` exists → use CephX authentication
//! - Otherwise → use no authentication
//!

use msgr2::protocol::Connection;
use msgr2::{AuthMethod, ConnectionConfig};
use std::net::SocketAddr;

/// Helper function to get the Ceph monitor address from environment variable
/// Panics if CEPH_MON_ADDR is not set or invalid
fn get_ceph_mon_addr() -> SocketAddr {
    std::env::var("CEPH_MON_ADDR")
        .expect("CEPH_MON_ADDR environment variable not set. Set it with: export CEPH_MON_ADDR=<monitor_address:port>")
        .parse()
        .expect("Failed to parse CEPH_MON_ADDR as a valid socket address (format: IP:PORT)")
}

/// Helper function to configure authentication method based on CEPH_AUTH_METHOD environment variable
/// This allows tests to explicitly control authentication via environment variable
fn configure_auth_method(mut config: ConnectionConfig) -> ConnectionConfig {
    // Debug: Print all environment variables related to CEPH
    tracing::info!("=== Environment Variable Debug ===");
    for (key, value) in std::env::vars() {
        if key.starts_with("CEPH") || key.starts_with("RUST") {
            tracing::info!("  {}={}", key, value);
        }
    }
    tracing::info!("=== End Environment Variables ===");
    
    match std::env::var("CEPH_AUTH_METHOD") {
        Ok(auth_env) => {
            tracing::info!("✓ CEPH_AUTH_METHOD environment variable found: '{}'", auth_env);
            tracing::info!("  Before: auth_method = {:?}", config.auth_method);
            match auth_env.to_lowercase().as_str() {
                "none" => {
                    tracing::info!("  Setting auth_method to None (no authentication)");
                    config.auth_method = Some(AuthMethod::None);
                }
                "cephx" => {
                    tracing::info!("  Setting auth_method to Cephx authentication");
                    config.auth_method = Some(AuthMethod::Cephx);
                }
                _ => {
                    tracing::warn!(
                        "  Unknown CEPH_AUTH_METHOD value: '{}', using auto-detection",
                        auth_env
                    );
                }
            }
            tracing::info!("  After: auth_method = {:?}", config.auth_method);
        }
        Err(e) => {
            tracing::warn!("✗ CEPH_AUTH_METHOD environment variable not found: {:?}", e);
            tracing::info!("  Using auto-detection for auth_method");
        }
    }
    config
}

#[tokio::test]
async fn test_compression_disabled() {
    let addr = get_ceph_mon_addr();

    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    tracing::info!("Testing msgr2 connection WITHOUT compression feature");
    tracing::info!("===================================================");

    // Explicitly disable compression
    let config = configure_auth_method(ConnectionConfig::without_compression());
    tracing::info!(
        "Config: supported_features={:#x}, auth_method={:?}",
        config.supported_features,
        config.auth_method
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
    let config = configure_auth_method(ConnectionConfig::with_compression());
    tracing::info!(
        "Config: supported_features={:#x}, auth_method={:?}",
        config.supported_features,
        config.auth_method
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
    let config = configure_auth_method(ConnectionConfig::prefer_crc_mode());
    tracing::info!("Config: preferred_modes={:?}, auth_method={:?}", config.preferred_modes, config.auth_method);

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
    let config = configure_auth_method(ConnectionConfig::prefer_secure_mode());
    tracing::info!("Config: preferred_modes={:?}, auth_method={:?}", config.preferred_modes, config.auth_method);

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
    let config = configure_auth_method(ConnectionConfig::default());
    let mut conn = Connection::connect(addr, config)
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
