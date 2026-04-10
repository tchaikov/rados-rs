//! Integration tests for object I/O: RadosObject (AsyncRead/AsyncWrite/AsyncSeek)
//! and list_objects_stream (paginated object listing).
//!
//! Requires a running Ceph cluster. Run with:
//! ```bash
//! CEPH_LIB=/path/to/ceph/build/lib \
//! ASAN_OPTIONS=detect_odr_violation=0,detect_leaks=0 \
//! CEPH_CONF=/path/to/ceph.conf \
//! cargo test -p rados --test osdclient_object_io_test -- --ignored --nocapture
//! ```

use bytes::Bytes;
use futures::StreamExt;
use std::io::SeekFrom;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::info;

mod common;
use common::create_ioctx;

// ─── RadosObject tests ────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_rados_object_write_and_read_back() {
    common::init_tracing();
    info!("test_rados_object_write_and_read_back");

    let ioctx = create_ioctx().await.expect("create_ioctx");

    let name = format!("tokio-rw-{}", rand::random::<u32>());
    let mut obj = rados::osdclient::RadosObject::new(ioctx.clone(), name.clone());

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
    common::init_tracing();
    info!("test_rados_object_partial_write_and_seek");

    let ioctx = create_ioctx().await.expect("create_ioctx");

    let name = format!("tokio-seek-{}", rand::random::<u32>());
    let mut obj = rados::osdclient::RadosObject::new(ioctx.clone(), name.clone());

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
    common::init_tracing();
    info!("test_rados_object_seek_end");

    let ioctx = create_ioctx().await.expect("create_ioctx");

    let name = format!("tokio-seekend-{}", rand::random::<u32>());
    let mut obj = rados::osdclient::RadosObject::new(ioctx.clone(), name.clone());

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
    common::init_tracing();
    info!("test_rados_object_nonexistent_reads_as_empty");

    let ioctx = create_ioctx().await.expect("create_ioctx");

    // Use a name that very likely doesn't exist
    let name = format!("tokio-noexist-{}", rand::random::<u64>());
    let mut obj = rados::osdclient::RadosObject::new(ioctx.clone(), name.clone());

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
    common::init_tracing();
    info!("test_list_objects_stream_basic");

    let ioctx = create_ioctx().await.expect("create_ioctx");

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
    let stream = rados::osdclient::list_objects_stream(ioctx.clone(), 3);
    let all_results: Vec<Result<String, rados::osdclient::OSDClientError>> = stream.collect().await;

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
