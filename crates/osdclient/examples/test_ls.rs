//! Test the ls operation which lists all objects in a pool
//!
//! This example demonstrates the simple ls() method that automatically
//! handles pagination and returns all objects in the pool.
//!
//! Usage:
//!   cargo run --package osdclient --example test_ls

use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("🚀 RADOS ls Operation Test");
    println!("===========================\n");

    // 1. Create MonClient and connect to monitors
    println!("1️⃣  Connecting to monitor...");

    // Create shared MessageBus
    let message_bus = Arc::new(msgr2::MessageBus::new());

    let mon_config = monclient::MonClientConfig {
        entity_name: "client.admin".to_string(),
        mon_addrs: vec!["v2:192.168.1.43:40490".to_string()],
        keyring_path: "/home/kefu/dev/ceph/build/keyring".to_string(),
        ..Default::default()
    };

    let mon_client =
        Arc::new(monclient::MonClient::new(mon_config, Arc::clone(&message_bus)).await?);

    // Initialize connection
    mon_client.init().await?;
    println!("   ✓ Connected to monitor\n");

    // 2. Subscribe to OSDMap
    println!("2️⃣  Subscribing to OSDMap...");
    mon_client.subscribe("osdmap", 0, 0).await?;

    // Wait a moment for OSDMap to arrive
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    println!("   ✓ OSDMap received\n");

    // 3. Create OSD client
    println!("3️⃣  Creating OSD client...");
    let osd_config = osdclient::OSDClientConfig {
        entity_name: "client.admin".to_string(),
        keyring_path: Some("/home/kefu/dev/ceph/build/keyring".to_string()),
        ..Default::default()
    };

    // Get FSID
    let fsid = mon_client.get_fsid().await;

    let osd_client = osdclient::OSDClient::new(
        osd_config,
        fsid,
        Arc::clone(&mon_client),
        Arc::clone(&message_bus),
    )
    .await?;
    println!("   ✓ OSD client created\n");

    // 4. Create IoCtx for test pool (pool 2)
    println!("4️⃣  Creating IoCtx for pool 2...");
    let ioctx = osdclient::IoCtx::new(osd_client.clone(), 2).await?;
    println!("   ✓ IoCtx created for pool 2\n");

    // 5. List all objects using the simple ls() method
    println!("5️⃣  Listing all objects using ls()...");
    let objects = ioctx.ls().await?;

    println!("   ✓ Found {} objects:", objects.len());
    for obj in &objects {
        println!("     - {}", obj);
    }
    println!();

    println!("✅ ls operation completed successfully!");

    Ok(())
}
