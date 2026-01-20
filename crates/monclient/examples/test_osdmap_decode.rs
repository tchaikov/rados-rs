use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let mon_config = monclient::MonClientConfig {
        entity_name: "client.admin".to_string(),
        mon_addrs: vec!["v2:127.0.0.1:6789".to_string()],
        keyring_path: "/tmp/rados-rs/ceph.client.admin.keyring".to_string(),
        ..Default::default()
    };

    println!("Connecting to Ceph monitor...");
    let mon_client = Arc::new(monclient::MonClient::new(mon_config).await?);
    mon_client.init().await?;
    
    println!("Subscribing to osdmap...");
    mon_client.subscribe("osdmap", 0, 0).await?;

    // Wait for OSDMap to arrive
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    println!("Getting OSDMap...");
    match mon_client.get_osdmap().await {
        Ok(osdmap) => {
            println!("✓ SUCCESS! OSDMap decoded successfully");
            println!("  Epoch: {}", osdmap.epoch);
            println!("  Max OSD: {}", osdmap.max_osd);
            println!("  Number of pools: {}", osdmap.pools.len());
            println!("  OSD state entries: {}", osdmap.osd_state.len());
            println!("  OSD weight entries: {}", osdmap.osd_weight.len());
            println!("  OSD addrs entries: {}", osdmap.osd_addrs_client.len());
            
            for (pool_id, pool_name) in &osdmap.pool_name {
                println!("  Pool {}: {}", pool_id, pool_name);
            }
            
            Ok(())
        }
        Err(e) => {
            println!("✗ FAILED: {}", e);
            Err(e.into())
        }
    }
}
