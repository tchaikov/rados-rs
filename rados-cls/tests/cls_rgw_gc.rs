//! Cluster tests for the rgw class's omap-era GC methods. Run with:
//!   CEPH_CONF=... cargo test -p rados-cls --test cls_rgw_gc -- --ignored --nocapture

#[path = "../../rados/tests/common/mod.rs"]
mod common;

use std::time::Duration;

use common::create_ioctx;
use rados::OSDClientError;
use rados_cls::rgw::gc::{self, ListRet};
use rados_cls::rgw::types::{GcObjInfo, Obj, ObjChain, ObjKey};

const ENOENT: i32 = 2;

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

fn tags(ret: &ListRet) -> Vec<&str> {
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
async fn rgw_gc_set_lists_in_expiry_order() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-rgw-omap-gc-order");
    ioctx.create(&oid, true).await.expect("create");

    for i in 0..10 {
        gc::set_entry(&ioctx, &oid, 0, &chain(i))
            .await
            .expect("set_entry");
    }
    let all: Vec<String> = (0..10).map(|i| format!("chain-{i}")).collect();

    // The marker is the last time key returned, set only when truncated.
    let first = gc::list(&ioctx, &oid, "", 8, true).await.expect("list");
    assert_eq!(tags(&first), all[..8]);
    assert!(first.truncated);
    assert!(
        first.next_marker.starts_with("1_"),
        "{:?}",
        first.next_marker
    );
    assert!(
        first.entries[0].time.sec > 1_700_000_000,
        "the class sets the entry's time"
    );
    let rest = gc::list(&ioctx, &oid, &first.next_marker, 8, true)
        .await
        .expect("list");
    assert_eq!(tags(&rest), all[8..]);
    assert!(!rest.truncated);
    assert!(rest.next_marker.is_empty(), "{:?}", rest.next_marker);

    let whole = gc::list(&ioctx, &oid, "", 10, true).await.expect("list");
    assert_eq!(tags(&whole), all);
    assert!(!whole.truncated);
    assert_eq!(whole.entries[3].chain, chain(3).chain);

    // Zero asks for the class's default of 128.
    let default = gc::list(&ioctx, &oid, "", 0, true).await.expect("list");
    assert_eq!(tags(&default), all);
    assert!(!default.truncated);

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn rgw_gc_set_upserts() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-rgw-omap-gc-upsert");
    ioctx.create(&oid, true).await.expect("create");

    gc::set_entry(&ioctx, &oid, 0, &chain(0))
        .await
        .expect("set_entry");
    let second = GcObjInfo {
        tag: "chain-0".to_owned(),
        ..chain(1)
    };
    gc::set_entry(&ioctx, &oid, 300, &second)
        .await
        .expect("set_entry again");

    // The second set moved the tag's one time key five minutes out.
    let due = gc::list(&ioctx, &oid, "", 10, true).await.expect("list");
    assert!(due.entries.is_empty() && !due.truncated, "{due:?}");
    let all = gc::list(&ioctx, &oid, "", 10, false).await.expect("list");
    assert_eq!(tags(&all), ["chain-0"]);
    assert_eq!(all.entries[0].chain, second.chain);

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn rgw_gc_defer_and_remove() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-rgw-omap-gc-defer");
    ioctx.create(&oid, true).await.expect("create");

    gc::set_entry(&ioctx, &oid, 0, &chain(0))
        .await
        .expect("set_entry");
    let due = gc::list(&ioctx, &oid, "", 10, true).await.expect("list");
    assert_eq!(tags(&due), ["chain-0"]);

    gc::defer_entry(&ioctx, &oid, 5, "chain-0")
        .await
        .expect("defer");
    let due = gc::list(&ioctx, &oid, "", 10, true).await.expect("list");
    assert!(due.entries.is_empty() && !due.truncated, "{due:?}");
    let all = gc::list(&ioctx, &oid, "", 10, false).await.expect("list");
    assert_eq!(tags(&all), ["chain-0"]);

    // The OSD's clock decides when the deferred entry falls due again.
    tokio::time::sleep(Duration::from_secs(6)).await;
    let due = gc::list(&ioctx, &oid, "", 10, true).await.expect("list");
    assert_eq!(tags(&due), ["chain-0"]);

    let err = gc::defer_entry(&ioctx, &oid, 5, "missing")
        .await
        .expect_err("an unknown tag");
    assert!(is_osd_error(&err, ENOENT), "{err:?}");

    let tag = ["chain-0".to_owned()];
    gc::remove(&ioctx, &oid, &tag).await.expect("remove");
    for expired_only in [true, false] {
        let ret = gc::list(&ioctx, &oid, "", 10, expired_only)
            .await
            .expect("list");
        assert!(ret.entries.is_empty() && !ret.truncated, "{ret:?}");
    }
    gc::remove(&ioctx, &oid, &tag)
        .await
        .expect("remove an absent tag");

    ioctx.remove(&oid).await.expect("remove");
}
