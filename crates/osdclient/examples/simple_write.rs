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

    // 1. Create MonClient and connect to monitors
    println!("1️⃣  Connecting to monitor...");
    let mon_config = monclient::MonClientConfig {
        entity_name: "client.admin".to_string(),
        mon_addrs: vec!["v2:192.168.1.37:40830".to_string()],
        keyring_path: "/home/kefu/dev/ceph/build/keyring".to_string(),
        ..Default::default()
    };

    let mon_client = Arc::new(monclient::MonClient::new(mon_config).await?);

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
    let osd_client = osdclient::OSDClient::new(osd_config, Arc::clone(&mon_client)).await?;
    println!("   ✓ OSD client created\n");

    // 4. Write data
    println!("4️⃣  Writing object...");
    let pool = 2; // Pool 2 is the 'test' pool in local cluster
    let oid = "example_object";
    let data = Bytes::from("Hello RADOS!");

    println!("   Pool: {}", pool);
    println!("   Object: {}", oid);
    println!("   Data: \"{}\"", String::from_utf8_lossy(&data));

    let write_result = osd_client.write_full(pool, oid, data.clone()).await?;
    println!("   ✓ Write complete, version: {}\n", write_result.version);

    // 5. Read data back
    println!("5️⃣  Reading object...");
    let read_result = osd_client.read(pool, oid, 0, 100).await?;
    println!(
        "   Data: \"{}\"",
        String::from_utf8_lossy(&read_result.data)
    );
    println!("   Version: {}", read_result.version);
    println!("   ✓ Read complete\n");

    // 6. Stat object
    println!("6️⃣  Getting object stats...");
    let stat_result = osd_client.stat(pool, oid).await?;
    println!("   Size: {} bytes", stat_result.size);
    println!("   Mtime: {:?}", stat_result.mtime);
    println!("   ✓ Stat complete\n");

    // 7. Delete object
    println!("7️⃣  Deleting object...");
    osd_client.delete(pool, oid).await?;
    println!("   ✓ Delete complete\n");

    println!("✅ Example completed successfully!");

    Ok(())
}
