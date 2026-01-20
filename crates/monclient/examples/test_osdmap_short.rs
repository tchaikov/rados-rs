use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN) // Less verbose
        .init();

    let mon_config = monclient::MonClientConfig {
        entity_name: "client.admin".to_string(),
        mon_addrs: vec!["v2:127.0.0.1:6789".to_string()],
        keyring_path: "/tmp/rados-rs/ceph.client.admin.keyring".to_string(),
        ..Default::default()
    };

    let mon_client = Arc::new(monclient::MonClient::new(mon_config).await?);
    mon_client.init().await?;
    mon_client.subscribe("osdmap", 0, 0).await?;

    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    match mon_client.get_osdmap().await {
        Ok(osdmap) => {
            println!("✓ SUCCESS!");
            println!("  Epoch: {}", osdmap.epoch);
            println!("  Pools: {}", osdmap.pools.len());
        }
        Err(e) => {
            println!("✗ FAILED: {}", e);
            return Err(e.into());
        }
    }

    Ok(())
}
