use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    println!("=== Testing RADOS Operations ===\n");

    // Connect to cluster
    let mon_addrs = vec!["v2:127.0.0.1:3300".to_string()];
    let mon_config = monclient::MonClientConfig {
        entity_name: "client.admin".to_string(),
        mon_addrs,
        keyring_path: "/home/kefu/dev/ceph/build/keyring".to_string(),
        ..Default::default()
    };

    let mon_client = Arc::new(monclient::MonClient::new(mon_config).await?);
    mon_client.init().await?;
    mon_client.subscribe("osdmap", 0, 0).await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let osd_config = osdclient::OSDClientConfig {
        entity_name: "client.admin".to_string(),
        keyring_path: Some("/home/kefu/dev/ceph/build/keyring".to_string()),
        ..Default::default()
    };

    let osd_client = osdclient::OSDClient::new(osd_config, Arc::clone(&mon_client)).await?;

    let pool = 2; // testpool
    let test_objects = vec![
        ("test_cli_obj", "Short name"),
        ("test_cli_object", "Long name with 15 chars"),
        ("test_really_long_object_name", "Very long name"),
    ];

    for (obj_name, data) in &test_objects {
        println!("\n--- Testing: {} ---", obj_name);
        println!("Object name length: {} bytes", obj_name.len());

        // Write
        print!("Writing... ");
        let write_result = osd_client
            .write_full(pool, obj_name, bytes::Bytes::from(data.as_bytes()))
            .await?;
        println!("✓ (version: {})", write_result.version);

        // Stat
        print!("Stat... ");
        let stat_result = osd_client.stat(pool, obj_name).await?;
        println!("✓ (size: {} bytes)", stat_result.size);

        if stat_result.size != data.len() as u64 {
            println!("⚠️  Size mismatch! Expected {} got {}", data.len(), stat_result.size);
        }

        // Read
        print!("Reading... ");
        let read_result = osd_client.read(pool, obj_name, 0, data.len() as u64).await?;
        println!("✓ (read {} bytes)", read_result.data.len());

        let read_str = String::from_utf8_lossy(&read_result.data);
        if read_str != *data {
            println!("⚠️  Data mismatch! Expected '{}' got '{}'", data, read_str);
        }

        // List objects using official ceph client to verify
        println!("Verifying with official client...");
        let output = std::process::Command::new("/home/kefu/dev/ceph/build/bin/rados")
            .args(&["-p", "testpool", "ls"])
            .env("CEPH_CONF", "/home/kefu/dev/ceph/build/ceph.conf")
            .output()?;

        let ls_output = String::from_utf8_lossy(&output.stdout);
        if ls_output.contains(obj_name) {
            println!("✓ Object found in cluster as: {}", obj_name);
        } else {
            println!("⚠️  Object NOT found with exact name!");
            println!("   Objects in pool matching pattern:");
            for line in ls_output.lines() {
                if line.starts_with("test_") {
                    println!("   - {}", line);
                }
            }
        }

        // Delete
        print!("Deleting... ");
        osd_client.delete(pool, obj_name).await?;
        println!("✓");

        // Verify deletion
        print!("Verifying deletion... ");
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        match osd_client.stat(pool, obj_name).await {
            Ok(_) => println!("⚠️  Object still exists after delete!"),
            Err(_) => println!("✓ Object successfully deleted"),
        }
    }

    Ok(())
}
