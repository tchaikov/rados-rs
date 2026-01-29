//! Simple test to verify message reception
//!
//! Usage:
//!   CEPH_MON_ADDR=192.168.1.37:40390 cargo run --example simple_test

use monclient::{MonClient, MonClientConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(false)
        .init();

    tracing::info!("=== Simple MonClient Test ===");

    // Create config
    let config = MonClientConfig {
        entity_name: "client.admin".to_string(),
        mon_addrs: vec![],
        keyring_path: "/etc/ceph/ceph.client.admin.keyring".to_string(),
        connect_timeout: Duration::from_secs(30),
        command_timeout: Duration::from_secs(60),
        hunt_interval: Duration::from_secs(3),
        hunt_parallel: 3,
        ..Default::default()
    };

    // Create client
    let client = MonClient::new(config).await?;

    // Initialize
    tracing::info!("Calling init()...");
    client.init().await?;
    tracing::info!("✓ init() completed");

    // Check monmap epoch
    let epoch = client.get_monmap_epoch().await;
    tracing::info!("Current monmap epoch: {}", epoch);

    // Wait a bit for messages
    tracing::info!("Waiting 5 seconds for messages...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Check monmap epoch again
    let epoch = client.get_monmap_epoch().await;
    tracing::info!("Monmap epoch after wait: {}", epoch);

    // Shutdown
    client.shutdown().await?;
    tracing::info!("=== Test Complete ===");

    Ok(())
}
