//! Minimal list test: open a pool and print the first 10 object names.
//!
//! Environment:
//! - `CEPH_CONF` - path to `ceph.conf` (default: `/etc/ceph/ceph.conf`)
//! - `RADOS_POOL` - pool name to open (default: `test`)

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    println!("\nMinimal List Test\n");

    let ceph_conf = std::env::var("CEPH_CONF").unwrap_or_else(|_| "/etc/ceph/ceph.conf".into());
    let pool_name = std::env::var("RADOS_POOL").unwrap_or_else(|_| "test".into());

    let client = rados::Client::builder()
        .config_file(&ceph_conf)
        .build()
        .await?;
    println!("  client ready");

    let ioctx = client.open_pool(&pool_name).await?;
    println!("  pool '{pool_name}' opened");

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        ioctx.list_objects(None, 10),
    )
    .await;

    match result {
        Ok(Ok((objects, next_cursor))) => {
            println!("  list succeeded, {} objects", objects.len());
            for oid in &objects {
                println!("    - {oid}");
            }
            println!("  next cursor: {next_cursor:?}");
        }
        Ok(Err(e)) => {
            println!("  list failed: {e}");
            return Err(e.into());
        }
        Err(_) => {
            println!("  list timed out after 10s");
            return Err("Timeout".into());
        }
    }

    Ok(())
}
