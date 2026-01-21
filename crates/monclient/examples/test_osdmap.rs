//! Test osdmap subscription and decoding
//!
//! Usage:
//!   CEPH_MON_ADDR=192.168.1.37:40353 cargo run --example test_osdmap

use monclient::{MonClient, MonClientConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .init();

    tracing::info!("=== OSDMap Subscription Test ===");

    // Create config
    let config = MonClientConfig {
        entity_name: "client.admin".to_string(),
        mon_addrs: vec![],
        keyring_path: "/home/kefu/dev/ceph/build/keyring".to_string(),
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

    if client.is_connected().await {
        tracing::info!("✓ Successfully connected to monitor!");
        tracing::info!("  Authenticated: {}", client.is_authenticated().await);
    } else {
        tracing::error!("✗ Failed to connect to monitor");
        return Ok(());
    }

    // Subscribe to osdmap
    tracing::info!("Subscribing to osdmap...");
    match client.subscribe("osdmap", 0, 0).await {
        Ok(_) => tracing::info!("✓ Subscribed to osdmap"),
        Err(e) => {
            tracing::error!("Failed to subscribe to osdmap: {}", e);
            return Err(e.into());
        }
    }

    // Wait for osdmap
    tracing::info!("Waiting for osdmap (30 seconds)...");
    tokio::time::sleep(Duration::from_secs(30)).await;

    // Shutdown
    tracing::info!("Shutting down...");
    client.shutdown().await?;

    tracing::info!("=== Test Complete ===");
    Ok(())
}
