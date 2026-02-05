//! Simple example demonstrating writing an object to a Ceph cluster
//!
//! This example:
//! 1. Connects to monitor at localhost:3300
//! 2. Subscribes to OSDMap to get cluster topology
//! 3. Creates an OSD client
//! 4. Writes "Hello RADOS!" to an object
//! 5. Reads the data back
//! 6. Stats the object
//! 7. Deletes the object
//!
//! Usage:
//!   cargo run --package osdclient --example simple_write

use bytes::Bytes;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("🚀 RADOS OSD Client Example");
    println!("==========================\n");

    // 1. Create shared MessageBus and MonClient
    println!("1️⃣  Creating MessageBus and connecting to monitor...");

    // Create shared MessageBus FIRST - both MonClient and OSDClient must use the same bus
    let message_bus = Arc::new(msgr2::MessageBus::new());

    let mon_config = monclient::MonClientConfig {
        entity_name: "client.admin".to_string(),
        mon_addrs: vec!["v2:192.168.1.37:40830".to_string()],
        keyring_path: "/home/kefu/dev/ceph/build/keyring".to_string(),
        ..Default::default()
    };

    let mon_client =
        Arc::new(monclient::MonClient::new(mon_config, Arc::clone(&message_bus)).await?);

    // Initialize connection
    mon_client.init().await?;

    // Register MonClient handlers on MessageBus
    mon_client.clone().register_handlers().await?;

    println!("   ✓ Connected to monitor\n");

    // 2. Wait for MonMap (contains FSID)
    println!("2️⃣  Waiting for MonMap...");
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Get FSID
    let fsid = mon_client.get_fsid().await;
    println!("   ✓ MonMap received\n");

    // 3. Create OSD client
    println!("3️⃣  Creating OSD client...");
    let osd_config = osdclient::OSDClientConfig {
        entity_name: "client.admin".to_string(),
        keyring_path: Some("/home/kefu/dev/ceph/build/keyring".to_string()),
        ..Default::default()
    };

    let osd_client = osdclient::OSDClient::new(
        osd_config,
        fsid,
        Arc::clone(&mon_client),
        Arc::clone(&message_bus),
    )
    .await?;

    // Register OSDClient handlers on MessageBus
    osd_client.clone().register_handlers().await?;

    println!("   ✓ OSD client created\n");

    // 4. Subscribe to OSDMap - both MonClient and OSDClient are ready
    println!("4️⃣  Subscribing to OSDMap...");
    mon_client.subscribe("osdmap", 0, 0).await?;

    // Wait a moment for OSDMap to arrive
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    println!("   ✓ OSDMap received\n");

    // 5. Write data
    println!("5️⃣  Writing object...");
    let pool = 2; // Pool 2 is the 'test' pool in local cluster
    let oid = "example_object";
    let data = Bytes::from("Hello RADOS!");

    println!("   Pool: {}", pool);
    println!("   Object: {}", oid);
    println!("   Data: \"{}\"", String::from_utf8_lossy(&data));

    let write_result = osd_client.write_full(pool, oid, data.clone()).await?;
    println!("   ✓ Write complete, version: {}\n", write_result.version);

    // 6. Read data back
    println!("6️⃣  Reading object...");
    let read_result = osd_client.read(pool, oid, 0, 100).await?;
    println!(
        "   Data: \"{}\"",
        String::from_utf8_lossy(&read_result.data)
    );
    println!("   Version: {}", read_result.version);
    println!("   ✓ Read complete\n");

    // 7. Stat object
    println!("7️⃣  Getting object stats...");
    let stat_result = osd_client.stat(pool, oid).await?;
    println!("   Size: {} bytes", stat_result.size);
    println!("   Mtime: {:?}", stat_result.mtime);
    println!("   ✓ Stat complete\n");

    // 8. Delete object
    println!("8️⃣  Deleting object...");
    osd_client.delete(pool, oid).await?;
    println!("   ✓ Delete complete\n");

    println!("✅ Example completed successfully!");

    Ok(())
}
