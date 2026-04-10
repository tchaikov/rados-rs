//! Create a handful of objects, then list them.
//!
//! Environment:
//! - `CEPH_CONF` - path to `ceph.conf` (default: `/etc/ceph/ceph.conf`)
//! - `RADOS_POOL` - pool name to open (default: `test`)

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("\nList Test with Objects\n");

    let ceph_conf = std::env::var("CEPH_CONF").unwrap_or_else(|_| "/etc/ceph/ceph.conf".into());
    let pool_name = std::env::var("RADOS_POOL").unwrap_or_else(|_| "test".into());

    let client = rados::Client::builder()
        .config_file(&ceph_conf)
        .build()
        .await?;
    let ioctx = client.open_pool(&pool_name).await?;
    println!("Opened pool '{pool_name}'");

    println!("\nCreating test objects...");
    for i in 0..5 {
        let oid = format!("test_object_{i}");
        let data = format!("Test data {i}");
        ioctx.write_full(&oid, bytes::Bytes::from(data)).await?;
        println!("  created {oid}");
    }

    println!("\nListing objects in pool '{pool_name}'...");
    let (objects, cursor) = ioctx.list_objects(None, 100).await?;
    println!("  found {} objects", objects.len());
    for oid in &objects {
        println!("    - {oid}");
    }
    println!("  next cursor: {cursor:?}");

    Ok(())
}
