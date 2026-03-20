//! Test listing objects after creating some
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("\n🧪 List Test with Objects\n");

    let (osdmap_tx, osdmap_rx) = rados::msgr2::map_channel::<rados::monclient::MOSDMap>(64);

    let auth = rados::monclient::AuthConfig::from_keyring(
        "client.admin".to_string(),
        "/home/kefu/dev/ceph/build/keyring",
    )?;

    let mon_config = rados::monclient::MonClientConfig {
        mon_addrs: vec!["v2:192.168.1.43:40490".to_string()],
        auth: Some(auth),
        ..Default::default()
    };

    let mon_client = rados::monclient::MonClient::new(mon_config, Some(osdmap_tx.clone())).await?;
    mon_client.init().await?;
    println!("✓ Mon connected");

    mon_client
        .subscribe(rados::monclient::MonService::OsdMap, 0, 0)
        .await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    println!("✓ OSDMap received");

    let osd_config = rados::osdclient::OSDClientConfig::default();

    // Get FSID
    let fsid = mon_client.get_fsid().await;

    let osd_client = rados::osdclient::OSDClient::new(
        osd_config,
        fsid,
        Arc::clone(&mon_client),
        osdmap_tx,
        osdmap_rx,
    )
    .await?;
    println!("✓ OSD client created");

    // Create test objects
    println!("\n📝 Creating test objects...");
    for i in 0..5 {
        let oid = format!("test_object_{}", i);
        let data = format!("Test data {}", i);
        osd_client
            .write_full(2, &oid, bytes::Bytes::from(data))
            .await?;
        println!("  ✓ Created {}", oid);
    }

    // List objects
    println!("\n📋 Listing objects in pool 2...");
    let result = osd_client.list(2, None, 100).await?;
    println!("✓ List succeeded!");
    println!("  Found {} objects", result.entries.len());
    for entry in &result.entries {
        println!("    - {}", entry.oid);
    }

    Ok(())
}
