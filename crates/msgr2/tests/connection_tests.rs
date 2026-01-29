//! Integration tests for msgr2 connection functionality
//!
//! These tests require a running Ceph cluster and CEPH_CONF environment variable.
//!
//! Configuration:
//!    CEPH_CONF=/path/to/ceph.conf cargo test --test connection_tests -- --ignored
//!
//! Example:
//!   CEPH_CONF=/home/kefu/dev/ceph/build/ceph.conf cargo test --package msgr2 --test connection_tests -- --ignored
//!
//! To run all tests including these integration tests locally:
//! ```bash
//! cargo test --workspace --all-targets -- --include-ignored
//! ```
//!
//! ## Authentication Configuration
//!
//! Authentication settings are read from ceph.conf automatically.
//!

use msgr2::protocol::Connection;
use msgr2::{AuthMethod, ConnectionConfig};
use std::env;
use std::net::SocketAddr;
use std::path::Path;

/// Helper function to get the Ceph monitor address from ceph.conf
fn get_ceph_mon_addr() -> SocketAddr {
    let conf_path =
        env::var("CEPH_CONF").unwrap_or_else(|_| "/home/kefu/dev/ceph/build/ceph.conf".to_string());

    let config = cephconfig::CephConfig::from_file(&conf_path)
        .unwrap_or_else(|_| panic!("Failed to read ceph.conf at {}", conf_path));

    let addr = config
        .first_v2_mon_addr()
        .expect("Failed to get v2 monitor address from ceph.conf");

    // Parse v2:IP:PORT format
    let stripped = addr
        .strip_prefix("v2:")
        .unwrap_or_else(|| panic!("Monitor address does not have v2: prefix: {}", addr));

    stripped
        .parse::<SocketAddr>()
        .unwrap_or_else(|_| panic!("Failed to parse monitor address: {}", stripped))
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
    let ceph_conf_path =
        std::env::var("CEPH_CONF").unwrap_or_else(|_| "/etc/ceph/ceph.conf".to_string());

    tracing::info!("Looking for ceph.conf at: {}", ceph_conf_path);

    if !Path::new(&ceph_conf_path).exists() {
        tracing::warn!(
            "ceph.conf not found at {}, using default authentication",
            ceph_conf_path
        );
        return config;
    }

    // Parse ceph.conf using cephconfig crate
    match cephconfig::CephConfig::from_file(&ceph_conf_path) {
        Ok(ceph_config) => {
            tracing::info!("✓ Successfully parsed ceph.conf");

            // Get auth_client_required setting from global section
            let auth_methods_str = ceph_config
                .get_with_fallback(&["global", "client"], "auth client required")
                .or_else(|| {
                    ceph_config.get_with_fallback(&["global", "client"], "auth_client_required")
                });

            if let Some(auth_str) = auth_methods_str {
                // Parse auth methods (comma, semicolon, space separated)
                let auth_methods: Vec<String> = auth_str
                    .split(&[',', ';', ' ', '\t'][..])
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_lowercase())
                    .collect();

                tracing::info!("  auth_client_required = {:?}", auth_methods);

                // Convert string auth methods to AuthMethod enum
                let mut supported_methods = Vec::new();

                for method in auth_methods {
                    match method.as_str() {
                        "cephx" => {
                            // Try to get keyring path from config or environment
                            let keyring_path = std::env::var("CEPH_KEYRING")
                                .or_else(|_| std::env::var("CEPH_CLIENT_KEYRING"))
                                .or_else(|_| ceph_config.keyring())
                                .unwrap_or_else(|_| {
                                    "/etc/ceph/ceph.client.admin.keyring".to_string()
                                });

                            tracing::info!("  Checking for keyring at: {}", keyring_path);

                            if Path::new(&keyring_path).exists() {
                                // Check if readable by trying to load auth provider
                                match auth::MonitorAuthProvider::new("client.admin".to_string()) {
                                    Ok(mut mon_auth) => {
                                        match mon_auth.set_secret_key_from_keyring(&keyring_path) {
                                            Ok(_) => {
                                                tracing::info!(
                                                    "  ✓ Keyring is available and readable"
                                                );
                                                supported_methods.push(AuthMethod::Cephx);
                                                config.auth_provider = Some(Box::new(mon_auth));
                                            }
                                            Err(e) => {
                                                tracing::warn!("  Keyring exists but cannot load key: {}, skipping cephx", e);
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            "  Failed to create auth provider: {}, skipping cephx",
                                            e
                                        );
                                    }
                                }
                            } else {
                                tracing::warn!(
                                    "  Keyring not found at {}, skipping cephx",
                                    keyring_path
                                );
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
#[ignore] // Requires a running Ceph cluster
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
#[ignore] // Requires a running Ceph cluster
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
#[ignore] // Requires a running Ceph cluster
async fn test_crc_mode() {
    let addr = get_ceph_mon_addr();

    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    tracing::info!("Testing msgr2 connection with CRC mode (no encryption)");
    tracing::info!("=======================================================");

    // Request CRC mode (no encryption)
    let config = configure_auth_method(ConnectionConfig::prefer_crc_mode());
    tracing::info!(
        "Config: preferred_modes={:?}, supported_auth_methods={:?}",
        config.preferred_modes,
        config.supported_auth_methods
    );

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
#[ignore] // Requires a running Ceph cluster
async fn test_secure_mode() {
    let addr = get_ceph_mon_addr();

    tracing_subscriber::fmt().with_test_writer().try_init().ok();

    tracing::info!("Testing msgr2 connection with SECURE mode (encryption)");
    tracing::info!("========================================================");

    // Request SECURE mode (encryption)
    let config = configure_auth_method(ConnectionConfig::prefer_secure_mode());
    tracing::info!(
        "Config: preferred_modes={:?}, supported_auth_methods={:?}",
        config.preferred_modes,
        config.supported_auth_methods
    );

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
#[ignore] // Requires a running Ceph cluster
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
