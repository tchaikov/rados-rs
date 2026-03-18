//! Integration tests for object I/O: RadosObject (AsyncRead/AsyncWrite/AsyncSeek)
//! and list_objects_stream (paginated object listing).
//!
//! Requires a running Ceph cluster. Run with:
//! ```bash
//! CEPH_LIB=/path/to/ceph/build/lib \
//! ASAN_OPTIONS=detect_odr_violation=0,detect_leaks=0 \
//! CEPH_CONF=/path/to/ceph.conf \
//! cargo test -p osdclient --test object_io_test -- --ignored --nocapture
//! ```

use bytes::Bytes;
use futures::StreamExt;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::info;

/// Helper to get test configuration from environment and ceph.conf
struct TestConfig {
    mon_addrs: Vec<String>,
    keyring_path: Option<String>,
    entity_name: String,
    pool: String,
}

impl TestConfig {
    fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let ceph_conf_path =
            std::env::var("CEPH_CONF").unwrap_or_else(|_| "/etc/ceph/ceph.conf".to_string());
        if !Path::new(&ceph_conf_path).exists() {
            return Err(format!("ceph.conf not found at: {ceph_conf_path}").into());
        }
        let ceph_config = cephconfig::CephConfig::from_file(&ceph_conf_path)?;
        let mon_addrs = ceph_config.mon_addrs()?;
        let auth_methods = ceph_config.get_auth_client_required();
        let keyring_path = if auth_methods.contains(&auth::protocol::CEPH_AUTH_CEPHX) {
            Some(
                std::env::var("CEPH_KEYRING")
                    .or_else(|_| ceph_config.keyring())
                    .unwrap_or_else(|_| "/etc/ceph/ceph.client.admin.keyring".to_string()),
            )
        } else {
            None
        };
        let entity_name = ceph_config.entity_name();
        let pool = std::env::var("CEPH_TEST_POOL").unwrap_or_else(|_| "test-pool".to_string());
        Ok(Self {
            mon_addrs,
            keyring_path,
            entity_name,
            pool,
        })
    }
}

async fn create_ioctx(config: &TestConfig) -> Result<osdclient::IoCtx, Box<dyn std::error::Error>> {
    let (osdmap_tx, osdmap_rx) = msgr2::map_channel::<monclient::MOSDMap>(64);
    let auth = if let Some(keyring_path) = &config.keyring_path {
        monclient::AuthConfig::from_keyring(config.entity_name.clone(), keyring_path)?
    } else {
        monclient::AuthConfig::no_auth(config.entity_name.clone())
    };
    let mon_config = monclient::MonClientConfig {
        mon_addrs: config.mon_addrs.clone(),
        auth: Some(auth),
        ..Default::default()
    };
    let mon_client = monclient::MonClient::new(mon_config, Some(osdmap_tx.clone())).await?;
    mon_client.init().await?;
    if config.keyring_path.is_some() {
        mon_client
            .wait_for_auth(std::time::Duration::from_secs(5))
            .await?;
    }
    mon_client
        .wait_for_monmap(std::time::Duration::from_secs(2))
        .await?;

    let client_inc = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32;
    let osd_config = osdclient::OSDClientConfig {
        client_inc,
        ..Default::default()
    };
    let fsid = mon_client.get_fsid().await;
    let osd_client = osdclient::OSDClient::new(
        osd_config,
        fsid,
        Arc::clone(&mon_client),
        osdmap_tx,
        osdmap_rx,
    )
    .await?;
    osd_client
        .wait_for_latest_osdmap(std::time::Duration::from_secs(2))
        .await?;

    // Resolve pool name → id
    let osdmap = osd_client.get_osdmap().await?;
    let pool_id = osdmap
        .pool_name
        .iter()
        .find(|(_, name)| name.as_str() == config.pool)
        .map(|(id, _)| *id)
        .ok_or_else(|| format!("Pool '{}' not found", config.pool))?;

    let ioctx = osdclient::IoCtx::new(osd_client, pool_id).await?;
    Ok(ioctx)
}

// ─── RadosObject tests ────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_rados_object_write_and_read_back() {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();
    info!("test_rados_object_write_and_read_back");

    let config = TestConfig::from_env().expect("TestConfig");
    let ioctx = create_ioctx(&config).await.expect("create_ioctx");

    let name = format!("tokio-rw-{}", rand::random::<u32>());
    let mut obj = osdclient::RadosObject::new(ioctx.clone(), name.clone());

    // Write data starting at offset 0
    obj.write_all(b"hello world").await.expect("write_all");
    info!("wrote 'hello world'");

    // Seek back to start and read
    let pos = obj.seek(SeekFrom::Start(0)).await.expect("seek start");
    assert_eq!(pos, 0, "seek should return new offset");

    let mut buf = Vec::new();
    obj.read_to_end(&mut buf).await.expect("read_to_end");
    assert_eq!(buf, b"hello world");
    info!("read back: {:?}", String::from_utf8_lossy(&buf));

    // Clean up
    ioctx.remove(&name).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn test_rados_object_partial_write_and_seek() {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();
    info!("test_rados_object_partial_write_and_seek");

    let config = TestConfig::from_env().expect("TestConfig");
    let ioctx = create_ioctx(&config).await.expect("create_ioctx");

    let name = format!("tokio-seek-{}", rand::random::<u32>());
    let mut obj = osdclient::RadosObject::new(ioctx.clone(), name.clone());

    // Write initial content
    obj.write_all(b"AAAAAAAAAA").await.expect("write 10 A's");

    // Seek to offset 5 and overwrite
    obj.seek(SeekFrom::Start(5)).await.expect("seek to 5");
    obj.write_all(b"BBBBB").await.expect("write 5 B's");

    // Seek to beginning and read full object
    obj.seek(SeekFrom::Start(0)).await.expect("seek to 0");
    let mut buf = [0u8; 10];
    obj.read_exact(&mut buf).await.expect("read_exact 10");
    assert_eq!(&buf, b"AAAAABBBBB");

    // SeekFrom::Current
    obj.seek(SeekFrom::Current(-5))
        .await
        .expect("seek current -5");
    assert_eq!(obj.offset(), 5);

    // Clean up
    ioctx.remove(&name).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn test_rados_object_seek_end() {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();
    info!("test_rados_object_seek_end");

    let config = TestConfig::from_env().expect("TestConfig");
    let ioctx = create_ioctx(&config).await.expect("create_ioctx");

    let name = format!("tokio-seekend-{}", rand::random::<u32>());
    let mut obj = osdclient::RadosObject::new(ioctx.clone(), name.clone());

    // Write known content
    obj.write_all(b"0123456789").await.expect("write");

    // SeekFrom::End(0) should put us at size == 10
    let pos = obj.seek(SeekFrom::End(0)).await.expect("seek end");
    assert_eq!(pos, 10, "seek-end-0 should land at object size");

    // SeekFrom::End(-3) should land at 7
    let pos = obj.seek(SeekFrom::End(-3)).await.expect("seek end -3");
    assert_eq!(pos, 7);

    // Clean up
    ioctx.remove(&name).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn test_rados_object_nonexistent_reads_as_empty() {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();
    info!("test_rados_object_nonexistent_reads_as_empty");

    let config = TestConfig::from_env().expect("TestConfig");
    let ioctx = create_ioctx(&config).await.expect("create_ioctx");

    // Use a name that very likely doesn't exist
    let name = format!("tokio-noexist-{}", rand::random::<u64>());
    let mut obj = osdclient::RadosObject::new(ioctx.clone(), name.clone());

    let mut buf = Vec::new();
    obj.read_to_end(&mut buf)
        .await
        .expect("read_to_end nonexistent");
    assert!(buf.is_empty(), "non-existent object should read as empty");
}

// ─── list_objects_stream tests ────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_list_objects_stream_basic() {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();
    info!("test_list_objects_stream_basic");

    let config = TestConfig::from_env().expect("TestConfig");
    let ioctx = create_ioctx(&config).await.expect("create_ioctx");

    // Create a set of objects with a unique prefix
    let prefix = format!("list-stream-{}", rand::random::<u32>());
    let count = 7usize;
    for i in 0..count {
        ioctx
            .write_full(&format!("{prefix}-{i}"), Bytes::from(vec![i as u8]))
            .await
            .expect("write");
    }
    info!("Created {count} objects with prefix '{prefix}'");

    // Collect through the stream using a small page size to exercise pagination.
    // Propagate errors (don't silently drop them) so failures are visible.
    let stream = osdclient::list_objects_stream(ioctx.clone(), 3);
    let all_results: Vec<Result<String, osdclient::OSDClientError>> = stream.collect().await;

    // Check for errors
    for r in &all_results {
        if let Err(e) = r {
            panic!("list_objects_stream error: {e}");
        }
    }

    let mut names: Vec<String> = all_results
        .into_iter()
        .filter_map(|r| r.ok())
        .filter(|n| n.starts_with(&prefix))
        .collect();

    names.sort();
    info!("Found {} matching objects", names.len());
    assert_eq!(names.len(), count, "should find all {count} objects");

    // Verify names are correct
    for i in 0..count {
        assert!(names.contains(&format!("{prefix}-{i}")));
    }

    // Clean up
    for i in 0..count {
        ioctx
            .remove(&format!("{prefix}-{i}"))
            .await
            .expect("remove");
    }
}
