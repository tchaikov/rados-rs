//! Example: Async wait for monmap
//!
//! Demonstrates async wait_for_map and event notifications
//!
//! Usage:
//!   CEPH_MON_ADDR=192.168.1.37:40390 cargo run --example async_wait

use monclient::{MonClient, MonClientConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .init();

    tracing::info!("=== Async MonClient Test ===");

    // Create config
    let config = MonClientConfig {
        entity_name: "client.admin".to_string(),
        mon_addrs: vec![],
        keyring_path: "/etc/ceph/ceph.client.admin.keyring".to_string(),
        connect_timeout: Duration::from_secs(30),
        command_timeout: Duration::from_secs(60),
        hunt_interval: Duration::from_secs(3),
    };

    // Create client
    let client = MonClient::new(config).await?;

    // Subscribe to map events
    let mut events = client.subscribe_events();

    // Spawn task to listen for events
    tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            tracing::info!("📢 Event received: {:?}", event);
        }
    });

    // Initialize
    client.init().await?;

    tracing::info!("✓ Connected and authenticated");

    // Wait for monmap asynchronously
    tracing::info!("Waiting for monmap epoch >= 1...");
    client.wait_for_map("monmap", 1).await?;
    tracing::info!("✓ Got monmap!");

    // Check FSID
    let fsid = client.get_fsid().await;
    tracing::info!("Cluster FSID: {}", fsid);

    // Keep alive
    tracing::info!("Keeping connection alive for 10 seconds...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Shutdown
    client.shutdown().await?;
    tracing::info!("=== Test Complete ===");

    Ok(())
}
