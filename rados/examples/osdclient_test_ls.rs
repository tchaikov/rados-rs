//! Test the ls() operation which lists all objects in a pool.
//!
//! The ls() helper handles pagination internally and returns every object
//! name in the pool as a single Vec<String>.
//!
//! Environment:
//! - `CEPH_CONF` - path to `ceph.conf` (default: `/etc/ceph/ceph.conf`)
//! - `RADOS_POOL` - pool name to open (default: `test`)

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("RADOS ls Operation Test");
    println!("=======================\n");

    let ceph_conf = std::env::var("CEPH_CONF").unwrap_or_else(|_| "/etc/ceph/ceph.conf".into());
    let pool_name = std::env::var("RADOS_POOL").unwrap_or_else(|_| "test".into());

    let client = rados::Client::builder()
        .config_file(&ceph_conf)
        .build()
        .await?;
    let ioctx = client.open_pool(&pool_name).await?;
    println!("Opened pool '{pool_name}'\n");

    println!("Listing all objects using ls()...");
    let objects = ioctx.ls().await?;
    println!("  found {} objects:", objects.len());
    for obj in &objects {
        println!("    - {obj}");
    }
    println!();

    println!("ls operation completed successfully.");
    Ok(())
}
