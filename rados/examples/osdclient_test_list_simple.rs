//! List objects in a pool with pagination.
//!
//! Environment:
//! - `CEPH_CONF` - path to `ceph.conf` (default: `/etc/ceph/ceph.conf`)
//! - `RADOS_POOL` - pool name to open (default: `test`)

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("RADOS List Objects Test");
    println!("=======================\n");

    let ceph_conf = std::env::var("CEPH_CONF").unwrap_or_else(|_| "/etc/ceph/ceph.conf".into());
    let pool_name = std::env::var("RADOS_POOL").unwrap_or_else(|_| "test".into());

    let client = rados::Client::builder()
        .config_file(&ceph_conf)
        .build()
        .await?;
    let ioctx = client.open_pool(&pool_name).await?;
    println!("Opened pool '{pool_name}'\n");

    println!("Listing objects (page size 10)...");
    let mut cursor = None;
    let mut page = 1;
    let mut total_objects = 0;
    loop {
        println!("  fetching page {page}...");
        let (objects, next_cursor) = ioctx.list_objects(cursor, 10).await?;
        println!("  page {page}: {} objects", objects.len());
        for obj in &objects {
            println!("    - {obj}");
            total_objects += 1;
        }
        println!("  next cursor: {next_cursor:?}");

        cursor = next_cursor;
        if cursor.is_none() {
            break;
        }
        page += 1;

        if page > 100 {
            println!("  safety limit: stopping after 100 pages");
            break;
        }
    }
    println!("  total: {total_objects}\n");

    println!("List test completed successfully.");
    Ok(())
}
