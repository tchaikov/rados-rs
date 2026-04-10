//! Write several objects, list them with pagination, then clean up.
//!
//! Uses [`rados::Client`] for cluster setup and [`rados::IoCtx`] for
//! per-pool operations.
//!
//! Environment:
//! - `CEPH_CONF` - path to `ceph.conf` (default: `/etc/ceph/ceph.conf`)
//! - `RADOS_POOL` - pool name to open (default: `test`)
//!
//! Usage:
//!   CEPH_CONF=/home/kefu/dev/ceph/build/ceph.conf \
//!     cargo run --package rados --example osdclient_test_list

use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("RADOS List Objects Test");
    println!("=======================\n");

    let ceph_conf = std::env::var("CEPH_CONF").unwrap_or_else(|_| "/etc/ceph/ceph.conf".into());
    let pool_name = std::env::var("RADOS_POOL").unwrap_or_else(|_| "test".into());

    println!("Building client from {ceph_conf}...");
    let client = rados::Client::builder()
        .config_file(&ceph_conf)
        .build()
        .await?;
    println!("  client ready\n");

    println!("Opening pool '{pool_name}'...");
    let ioctx = client.open_pool(&pool_name).await?;
    println!("  ioctx opened\n");

    let test_objects = [
        "test_list_obj_1",
        "test_list_obj_2",
        "test_list_obj_3",
        "test_list_obj_4",
        "test_list_obj_5",
    ];

    println!("Creating test objects...");
    for oid in &test_objects {
        let data = Bytes::from(format!("Test data for {oid}"));
        ioctx.write_full(oid, data).await?;
        println!("  created: {oid}");
    }
    println!();

    println!("Listing objects (page size 3)...");
    let mut cursor = None;
    let mut page = 1;
    let mut total_objects = 0;
    loop {
        let (objects, next_cursor) = ioctx.list_objects(cursor, 3).await?;
        println!("  page {page}: {} objects", objects.len());
        for obj in &objects {
            println!("    - {obj}");
            total_objects += 1;
        }
        cursor = next_cursor;
        if cursor.is_none() {
            break;
        }
        page += 1;
    }
    println!("  total listed: {total_objects}\n");

    println!("Listing all objects at once (page size 1000)...");
    let (all_objects, cursor) = ioctx.list_objects(None, 1000).await?;
    println!("  found {} objects", all_objects.len());
    println!("  cursor after: {cursor:?}");

    let found_count = test_objects
        .iter()
        .filter(|oid| all_objects.iter().any(|obj| obj == *oid))
        .count();
    println!(
        "  found {found_count}/{} test objects in list\n",
        test_objects.len()
    );

    println!("Cleaning up test objects...");
    for oid in &test_objects {
        ioctx.remove(oid).await?;
        println!("  removed: {oid}");
    }
    println!();

    println!("List test completed successfully.");
    Ok(())
}
