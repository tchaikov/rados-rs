//! Minimal test for listing objects
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    println!("\n🧪 Minimal List Test\n");

    // Create map channel for routing MOSDMap messages
    let (osdmap_tx, osdmap_rx) = msgr2::map_channel::<monclient::MOSDMap>(64);

    let mon_config = monclient::MonClientConfig {
        entity_name: "client.admin".to_string(),
        mon_addrs: vec!["v2:192.168.1.43:40490".to_string()],
        keyring_path: "/home/kefu/dev/ceph/build/keyring".to_string(),
        ..Default::default()
    };

    let mon_client = monclient::MonClient::new(mon_config, Some(osdmap_tx.clone())).await?;
    mon_client.init().await?;
    println!("✓ Mon connected");

    mon_client.subscribe("osdmap", 0, 0).await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    println!("✓ OSDMap received");

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
        osdmap_tx,
        osdmap_rx,
    )
    .await?;
    println!("✓ OSD client created");

    println!("\n📋 Calling list() on pool 2...");
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        osd_client.list(2, None, 10),
    )
    .await;

    match result {
        Ok(Ok(list_result)) => {
            println!("✓ List succeeded!");
            println!("  Found {} objects", list_result.entries.len());
            for entry in &list_result.entries {
                println!("    - {}", entry.oid);
            }
            println!("  Next cursor: {:?}", list_result.cursor);
        }
        Ok(Err(e)) => {
            println!("✗ List failed: {}", e);
            return Err(e.into());
        }
        Err(_) => {
            println!("✗ List timed out after 10s");
            return Err("Timeout".into());
        }
    }

    Ok(())
}
