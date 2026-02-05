//! Simple test for listing objects in a pool
//!
//! This example:
//! 1. Connects to monitor
//! 2. Lists objects in a pool
//!
//! Usage:
//!   cargo run --package osdclient --example test_list_simple

use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("🚀 RADOS List Objects Test");
    println!("==========================\n");

    // 1. Create MonClient and connect to monitors
    println!("1️⃣  Connecting to monitor...");

    // Create shared MessageBus
    let message_bus = Arc::new(msgr2::MessageBus::new());

    let mon_config = monclient::MonClientConfig {
        entity_name: "client.admin".to_string(),
        mon_addrs: vec!["v2:192.168.1.43:40239".to_string()],
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

    // 5. List objects with pagination
    println!("5️⃣  Listing objects (max 10 per page)...");
    let mut cursor = None;
    let mut page = 1;
    let mut total_objects = 0;

    loop {
        println!("   Fetching page {}...", page);
        let (objects, next_cursor) = ioctx.list_objects(cursor, 10).await?;

        println!("   Page {}: Found {} objects", page, objects.len());
        for obj in &objects {
            println!("     - {}", obj);
            total_objects += 1;
        }

        cursor = next_cursor.clone();
        println!("   Next cursor: {:?}", next_cursor);

        if cursor.is_none() {
            break;
        }
        page += 1;

        // Safety limit
        if page > 100 {
            println!("   ⚠️  Stopping after 100 pages");
            break;
        }
    }
    println!("   ✓ Total objects found: {}\n", total_objects);

    println!("✅ List test completed successfully!");

    Ok(())
}
