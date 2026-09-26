//! Cluster tests for the queue and rgw_gc classes. Run with:
//!   CEPH_CONF=... cargo test -p rados-cls --test cls_queue_gc -- --ignored --nocapture

#[path = "../../rados/tests/common/mod.rs"]
mod common;

use bytes::Bytes;
use common::create_ioctx;
use rados::OSDClientError;
use rados_cls::rgw::types::{GcObjInfo, Obj, ObjChain, ObjKey};
use rados_cls::{queue, rgw_gc};

const EEXIST: i32 = 17;
const EINVAL: i32 = 22;
const ENOSPC: i32 = 28;

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("{prefix}-{}-{nanos}", std::process::id())
}

fn is_osd_error(err: &OSDClientError, errno: i32) -> bool {
    matches!(err, OSDClientError::OSDError { code, .. } if *code == -errno)
}

fn payloads(ret: &queue::ListRet) -> Vec<&[u8]> {
    ret.entries.iter().map(|e| e.data.as_ref()).collect()
}

fn markers(ret: &queue::ListRet) -> Vec<&str> {
    ret.entries.iter().map(|e| e.marker.as_str()).collect()
}

fn tags(ret: &rgw_gc::ListRet) -> Vec<&str> {
    ret.entries.iter().map(|e| e.tag.as_str()).collect()
}

/// test_cls_rgw_gc.cc's create_obj: two objects per chain.
fn chain(i: u32) -> GcObjInfo {
    let obj = |j: u32| Obj {
        pool: format!("pool-{i}.{j}"),
        key: ObjKey {
            name: format!("oid-{i}.{j}"),
            instance: String::new(),
        },
        loc: format!("loc-{i}.{j}"),
    };
    GcObjInfo {
        tag: format!("chain-{i}"),
        chain: ObjChain {
            objs: vec![obj(1), obj(2)],
        },
        ..GcObjInfo::default()
    }
}

#[tokio::test]
#[ignore]
async fn queue_ring_init_enqueue_list_remove() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-queue-ring");
    ioctx.create(&oid, true).await.expect("create");

    queue::init(&ioctx, &oid, 1024).await.expect("init");
    assert_eq!(
        queue::get_capacity(&ioctx, &oid).await.expect("capacity"),
        1024
    );
    let err = queue::init(&ioctx, &oid, 1024)
        .await
        .expect_err("second init");
    assert!(is_osd_error(&err, EEXIST), "{err:?}");

    // Empty: nothing, not truncated, the next marker is the front.
    let ret = queue::list_entries(&ioctx, &oid, "", 10, "")
        .await
        .expect("list");
    assert!(ret.entries.is_empty() && !ret.is_truncated);
    assert_eq!(ret.next_marker, "0/1024");

    // Each entry costs ten bytes of magic and length plus its payload.
    queue::enqueue(
        &ioctx,
        &oid,
        vec![
            Bytes::from_static(b"one"),
            Bytes::from_static(b"two"),
            Bytes::from_static(b"three"),
        ],
    )
    .await
    .expect("enqueue");
    let page = queue::list_entries(&ioctx, &oid, "", 2, "")
        .await
        .expect("list");
    assert_eq!(payloads(&page), [b"one".as_ref(), b"two"]);
    assert_eq!(markers(&page), ["0/1024", "0/1037"]);
    assert!(page.is_truncated);
    assert_eq!(page.next_marker, "0/1050");
    let rest = queue::list_entries(&ioctx, &oid, &page.next_marker, 10, "")
        .await
        .expect("list");
    assert_eq!(payloads(&rest), [b"three".as_ref()]);
    assert_eq!(markers(&rest), ["0/1050"]);
    assert!(!rest.is_truncated);
    assert_eq!(rest.next_marker, "0/1065", "the tail");

    // end_marker is exclusive.
    let until = queue::list_entries(&ioctx, &oid, "", u64::MAX, "0/1050")
        .await
        .expect("list");
    assert_eq!(payloads(&until), [b"one".as_ref(), b"two"]);

    // Remove up to (not including) the third entry, then everything.
    queue::remove_entries(&ioctx, &oid, "0/1050")
        .await
        .expect("remove");
    let ret = queue::list_entries(&ioctx, &oid, "", 10, "")
        .await
        .expect("list");
    assert_eq!(payloads(&ret), [b"three".as_ref()]);
    let err = queue::remove_entries(&ioctx, &oid, "0/1")
        .await
        .expect_err("behind the front");
    assert!(is_osd_error(&err, EINVAL), "{err:?}");
    queue::remove_entries(&ioctx, &oid, "0/1065")
        .await
        .expect("remove to the tail");
    let ret = queue::list_entries(&ioctx, &oid, "", 10, "")
        .await
        .expect("list");
    assert!(ret.entries.is_empty() && !ret.is_truncated);

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn queue_enqueue_past_capacity_is_enospc() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-queue-full");
    ioctx.create(&oid, true).await.expect("create");

    queue::init(&ioctx, &oid, 32).await.expect("init");
    let err = queue::enqueue(&ioctx, &oid, vec![Bytes::from(vec![0u8; 40])])
        .await
        .expect_err("does not fit");
    assert!(is_osd_error(&err, ENOSPC), "{err:?}");
    queue::enqueue(&ioctx, &oid, vec![Bytes::from(vec![0u8; 22])])
        .await
        .expect("exactly fills the ring");
    let err = queue::enqueue(&ioctx, &oid, vec![Bytes::new()])
        .await
        .expect_err("a full ring");
    assert!(is_osd_error(&err, ENOSPC), "{err:?}");

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn rgw_gc_enqueue_list_remove() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-rgw-gc");
    ioctx.create(&oid, true).await.expect("create");

    rgw_gc::init(&ioctx, &oid, 4096, 10).await.expect("init");
    assert_eq!(
        rgw_gc::get_capacity(&ioctx, &oid).await.expect("capacity"),
        4096
    );
    let ret = rgw_gc::list_entries(&ioctx, &oid, "", 10, false)
        .await
        .expect("list");
    assert!(ret.entries.is_empty() && !ret.truncated);

    // Two due now, one due in five minutes.
    for i in 0..3 {
        let secs = if i == 2 { 300 } else { 0 };
        rgw_gc::enqueue(&ioctx, &oid, secs, &chain(i))
            .await
            .expect("enqueue");
    }
    let due = rgw_gc::list_entries(&ioctx, &oid, "", 10, true)
        .await
        .expect("list expired");
    assert_eq!(tags(&due), ["chain-0", "chain-1"]);
    assert!(!due.truncated);
    assert_eq!(due.entries[0].chain.objs[1].key.name, "oid-0.2");
    assert!(
        due.entries[0].time.sec > 1_700_000_000,
        "the class sets the entry's time"
    );

    // Paging: next_marker is set only when truncated.
    let first = rgw_gc::list_entries(&ioctx, &oid, "", 1, false)
        .await
        .expect("list");
    assert_eq!(tags(&first), ["chain-0"]);
    assert!(first.truncated && !first.next_marker.is_empty());
    let second = rgw_gc::list_entries(&ioctx, &oid, &first.next_marker, 1, false)
        .await
        .expect("list");
    assert_eq!(tags(&second), ["chain-1"]);
    assert!(second.truncated);
    let third = rgw_gc::list_entries(&ioctx, &oid, &second.next_marker, 1, false)
        .await
        .expect("list");
    assert_eq!(tags(&third), ["chain-2"]);
    assert!(!third.truncated && third.next_marker.is_empty());

    rgw_gc::remove_entries(&ioctx, &oid, 2)
        .await
        .expect("remove");
    let left = rgw_gc::list_entries(&ioctx, &oid, "", 10, false)
        .await
        .expect("list");
    assert_eq!(tags(&left), ["chain-2"]);

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn rgw_gc_defer_entry() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-rgw-gc-defer");
    ioctx.create(&oid, true).await.expect("create");

    rgw_gc::init(&ioctx, &oid, 4096, 1).await.expect("init");
    rgw_gc::enqueue(&ioctx, &oid, 0, &chain(0))
        .await
        .expect("enqueue");
    rgw_gc::enqueue(&ioctx, &oid, 0, &chain(1))
        .await
        .expect("enqueue");

    // A deferred tag disappears from listings until its new time.
    rgw_gc::defer_entry(&ioctx, &oid, 600, &chain(1))
        .await
        .expect("defer");
    let ret = rgw_gc::list_entries(&ioctx, &oid, "", 10, false)
        .await
        .expect("list");
    assert_eq!(tags(&ret), ["chain-0"]);

    // Deferring it again only moves its time; a second tag is one too many.
    rgw_gc::defer_entry(&ioctx, &oid, 900, &chain(1))
        .await
        .expect("defer again");
    let err = rgw_gc::defer_entry(&ioctx, &oid, 600, &chain(0))
        .await
        .expect_err("past the allowance");
    assert!(is_osd_error(&err, ENOSPC), "{err:?}");

    ioctx.remove(&oid).await.expect("remove");
}
