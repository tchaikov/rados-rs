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
//! - If ceph.conf is found → parse `auth client required` setting
//! - Filter out cephx if keyring is not available or not readable
//! - Use the available authentication methods from ceph.conf
//!

mod ceph_config;

use msgr2::protocol::Connection;
use msgr2::{AuthMethod, ConnectionConfig};
use std::net::SocketAddr;
use std::path::Path;

/// Helper function to get the Ceph monitor address from environment variable
/// Panics if CEPH_MON_ADDR is not set or invalid
fn get_ceph_mon_addr() -> SocketAddr {
    std::env::var("CEPH_MON_ADDR")
        .expect("CEPH_MON_ADDR environment variable not set. Set it with: export CEPH_MON_ADDR=<monitor_address:port>")
        .parse()
        .expect("Failed to parse CEPH_MON_ADDR as a valid socket address (format: IP:PORT)")
}

/// Helper function to configure authentication method based on ceph.conf
/// This reads the ceph.conf file, parses the 'auth client required' setting,
/// and configures the authentication methods accordingly.
fn configure_auth_method(mut config: ConnectionConfig) -> ConnectionConfig {
    // Debug: Print all environment variables related to CEPH
    tracing::info!("=== Environment Variable Debug ===");
    for (key, value) in std::env::vars() {
        if key.starts_with("CEPH") || key.starts_with("RUST") {
            tracing::info!("  {}={}", key, value);
        }
    }
    tracing::info!("=== End Environment Variables ===");
    
    // Try to find ceph.conf
    let ceph_conf_path = std::env::var("CEPH_CONF")
        .unwrap_or_else(|_| "/etc/ceph/ceph.conf".to_string());
    
    tracing::info!("Looking for ceph.conf at: {}", ceph_conf_path);
    
    if !Path::new(&ceph_conf_path).exists() {
        tracing::warn!("ceph.conf not found at {}, using default authentication", ceph_conf_path);
        return config;
    }
    
    // Parse ceph.conf
    match ceph_config::parse_ceph_conf(Path::new(&ceph_conf_path)) {
        Ok(ceph_config) => {
            tracing::info!("✓ Successfully parsed ceph.conf");
            
            // Get auth_client_required setting
            if let Some(auth_methods_str) = ceph_config::get_auth_client_required(&ceph_config) {
                tracing::info!("  auth_client_required = {:?}", auth_methods_str);
                
                // Convert string auth methods to AuthMethod enum
                let mut supported_methods = Vec::new();
                
                for method in auth_methods_str {
                    match method.as_str() {
                        "cephx" => {
                            // Check if keyring is available
                            let keyring_path = std::env::var("CEPH_KEYRING")
                                .or_else(|_| std::env::var("CEPH_CLIENT_KEYRING"))
                                .unwrap_or_else(|_| "/etc/ceph/ceph.client.admin.keyring".to_string());
                            
                            tracing::info!("  Checking for keyring at: {}", keyring_path);
                            
                            if Path::new(&keyring_path).exists() {
                                // Check if readable by trying to read it
                                match std::fs::read(&keyring_path) {
                                    Ok(_) => {
                                        tracing::info!("  ✓ Keyring is available and readable");
                                        supported_methods.push(AuthMethod::Cephx);
                                        config.keyring_path = Some(keyring_path);
                                    }
                                    Err(e) => {
                                        tracing::warn!("  Keyring exists but is not readable: {}, skipping cephx", e);
                                    }
                                }
                            } else {
                                tracing::warn!("  Keyring not found at {}, skipping cephx", keyring_path);
                            }
                        }
                        "none" => {
                            tracing::info!("  ✓ Adding AuthMethod::None");
                            supported_methods.push(AuthMethod::None);
                        }
                        _ => {
                            tracing::warn!("  Unknown auth method: {}", method);
                        }
                    }
                }
                
                if !supported_methods.is_empty() {
                    tracing::info!("  Final supported_auth_methods = {:?}", supported_methods);
                    config.supported_auth_methods = supported_methods;
                } else {
                    tracing::warn!("  No supported auth methods found, using default");
                }
            } else {
                tracing::info!("  auth_client_required not found in ceph.conf, using default");
            }
        }
        Err(e) => {
            tracing::error!("Failed to parse ceph.conf: {}", e);
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
        "Config: supported_features={:#x}, supported_auth_methods={:?}",
        config.supported_features,
        config.supported_auth_methods
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
        "Config: supported_features={:#x}, supported_auth_methods={:?}",
        config.supported_features,
        config.supported_auth_methods
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
    tracing::info!("Config: preferred_modes={:?}, supported_auth_methods={:?}", config.preferred_modes, config.supported_auth_methods);

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
    tracing::info!("Config: preferred_modes={:?}, supported_auth_methods={:?}", config.preferred_modes, config.supported_auth_methods);

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
