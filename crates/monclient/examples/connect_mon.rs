//! Example: Connect to a Ceph monitor
//!
//! Usage:
//!   CEPH_MON_ADDR=192.168.1.37:40390 cargo run --example connect_mon

use monclient::{MonClient, MonClientConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .init();

    tracing::info!("=== MonClient Connection Test ===");

    // Create config (will use CEPH_MON_ADDR env var)
    let config = MonClientConfig {
        entity_name: "client.admin".to_string(),
        mon_addrs: vec![], // Will use CEPH_MON_ADDR
        keyring_path: "/etc/ceph/ceph.client.admin.keyring".to_string(),
        connect_timeout: Duration::from_secs(30),
        command_timeout: Duration::from_secs(60),
        hunt_interval: Duration::from_secs(3),
    };

    // Create client
    tracing::info!("Creating MonClient...");
    let client = MonClient::new(config).await?;

    // Initialize and connect
    tracing::info!("Initializing and connecting to monitor...");
    client.init().await?;

    // Check connection status
    if client.is_connected().await {
        tracing::info!("✓ Successfully connected to monitor!");
        tracing::info!("  Authenticated: {}", client.is_authenticated().await);
        tracing::info!("  Monitor count: {}", client.get_mon_count().await);
        tracing::info!("  FSID: {}", client.get_fsid().await);
    } else {
        tracing::error!("✗ Failed to connect to monitor");
    }

    // Try to subscribe to monmap
    tracing::info!("Subscribing to monmap...");
    match client.subscribe("monmap", 0, 0).await {
        Ok(_) => tracing::info!("✓ Subscribed to monmap"),
        Err(e) => tracing::warn!("Failed to subscribe: {}", e),
    }

    // Keep connection alive for a bit to receive messages
    tracing::info!("Keeping connection alive for 30 seconds to receive messages...");
    tokio::time::sleep(Duration::from_secs(30)).await;

    // Shutdown
    tracing::info!("Shutting down...");
    client.shutdown().await?;

    tracing::info!("=== Test Complete ===");
    Ok(())
}
