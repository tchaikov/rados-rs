//! Simple end-to-end example: connect, write, read, stat, remove.
//!
//! Uses the high-level [`rados::Client`] builder to avoid the 30-line
//! MonClient + OSDClient + channel boilerplate.
//!
//! Environment:
//! - `CEPH_CONF` - path to `ceph.conf` (default: `/etc/ceph/ceph.conf`)
//! - `RADOS_POOL` - pool name to open (default: `test`)
//! - `RADOS_OID`  - object name (default: `example_object`)
//!
//! Usage:
//!   CEPH_CONF=/home/kefu/dev/ceph/build/ceph.conf \
//!     cargo run --package rados --example osdclient_simple_write

use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("RADOS OSD Client Example");
    println!("========================\n");

    let ceph_conf = std::env::var("CEPH_CONF").unwrap_or_else(|_| "/etc/ceph/ceph.conf".into());
    let pool_name = std::env::var("RADOS_POOL").unwrap_or_else(|_| "test".into());
    let oid = std::env::var("RADOS_OID").unwrap_or_else(|_| "example_object".into());

    // Build the client: parses ceph.conf, brings MonClient + OSDClient up to a
    // ready state, and waits deterministically for the first MonMap and OSDMap.
    // One-liner connect: equivalent to
    // Client::builder().config_file(&ceph_conf).build().await
    println!("Connecting using rados::connect({ceph_conf})...");
    let client = rados::connect(&ceph_conf).await?;
    println!("  client ready\n");

    // Open the pool by name - no manual OSDMap walking.
    println!("Opening pool '{pool_name}'...");
    let ioctx = client.open_pool(&pool_name).await?;
    println!("  ioctx opened\n");

    let data = Bytes::from("Hello RADOS!");
    println!("Writing '{oid}' = \"{}\"", String::from_utf8_lossy(&data));
    let write_result = ioctx.write_full(&oid, data.clone()).await?;
    println!("  version={}\n", write_result.version);

    println!("Reading '{oid}'...");
    let read_result = ioctx.read(&oid, 0, 100).await?;
    println!(
        "  data=\"{}\" version={}\n",
        String::from_utf8_lossy(&read_result.data),
        read_result.version
    );

    println!("Stat '{oid}'...");
    let stat_result = ioctx.stat(&oid).await?;
    println!(
        "  size={} mtime={:?}\n",
        stat_result.size, stat_result.mtime
    );

    println!("Removing '{oid}'...");
    ioctx.remove(&oid).await?;
    println!("  removed\n");

    println!("Example completed successfully.");
    Ok(())
}
