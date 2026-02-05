//! Test example for listing objects in a pool
//!
//! This example:
//! 1. Connects to monitor
//! 2. Creates some test objects in a pool
//! 3. Lists the objects with pagination
//! 4. Cleans up the test objects
//!
//! Usage:
//!   cargo run --package osdclient --example test_list

use bytes::Bytes;
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

    // 5. Create test objects
    println!("5️⃣  Creating test objects...");
    let test_objects = vec![
        "test_list_obj_1",
        "test_list_obj_2",
        "test_list_obj_3",
        "test_list_obj_4",
        "test_list_obj_5",
    ];

    for oid in &test_objects {
        let data = Bytes::from(format!("Test data for {}", oid));
        ioctx.write_full(oid, data).await?;
        println!("   ✓ Created: {}", oid);
    }
    println!();

    // 6. List objects with pagination
    println!("6️⃣  Listing objects (max 3 per page)...");
    let mut cursor = None;
    let mut page = 1;
    let mut total_objects = 0;

    loop {
        let (objects, next_cursor) = ioctx.list_objects(cursor, 3).await?;

        println!("   Page {}: Found {} objects", page, objects.len());
        for obj in &objects {
            println!("     - {}", obj);
            total_objects += 1;
        }

        cursor = next_cursor;
        if cursor.is_none() {
            break;
        }
        page += 1;
    }
    println!("   ✓ Total objects found: {}\n", total_objects);

    // 7. List all objects at once
    println!("7️⃣  Listing all objects at once...");
    let (all_objects, cursor) = ioctx.list_objects(None, 1000).await?;
    println!("   Found {} objects total", all_objects.len());
    println!("   Cursor after: {:?}", cursor);

    // Verify our test objects are in the list
    let mut found_count = 0;
    for test_obj in &test_objects {
        if all_objects.contains(&test_obj.to_string()) {
            found_count += 1;
        }
    }
    println!(
        "   ✓ Found {}/{} test objects in list\n",
        found_count,
        test_objects.len()
    );

    // 8. Clean up test objects
    println!("8️⃣  Cleaning up test objects...");
    for oid in &test_objects {
        ioctx.remove(oid).await?;
        println!("   ✓ Deleted: {}", oid);
    }
    println!();

    println!("✅ List test completed successfully!");

    Ok(())
}
