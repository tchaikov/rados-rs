//! Cluster tests for the rgw class's lifecycle methods. Run with:
//!   CEPH_CONF=... cargo test -p rados-cls --test cls_rgw_lc -- --ignored --nocapture

#[path = "../../rados/tests/common/mod.rs"]
mod common;

use std::time::UNIX_EPOCH;

use common::create_ioctx;
use rados::OSDClientError;
use rados_cls::rgw::lc::{
    self, LcEntry, LcObjHead, STATUS_COMPLETE, STATUS_PROCESSING, STATUS_UNINITIAL,
};

const ENOENT: i32 = 2;

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("{prefix}-{}-{nanos}", std::process::id())
}

fn is_osd_error(err: &OSDClientError, errno: i32) -> bool {
    matches!(err, OSDClientError::OSDError { code, .. } if *code == -errno)
}

/// An entry keyed as RGW's `get_bucket_lc_key` keys one of an empty
/// tenant: `":name:marker"`.
fn entry(bucket: &str, status: u32) -> LcEntry {
    LcEntry {
        bucket: bucket.to_owned(),
        start_time: 0,
        status,
    }
}

#[tokio::test]
#[ignore]
async fn lc_head_lives_in_the_omap_header() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-rgw-lc-head");
    ioctx.create(&oid, true).await.expect("create");

    let head = lc::get_head(&ioctx, &oid).await.expect("get empty head");
    assert_eq!(head, LcObjHead::default());

    // The rollover date, which the dump leaves out, survives the header.
    let want = LcObjHead {
        start_date: 10,
        marker: "m".to_owned(),
        shard_rollover_date: 20,
    };
    lc::put_head(&ioctx, &oid, &want).await.expect("put_head");
    assert_eq!(lc::get_head(&ioctx, &oid).await.expect("get_head"), want);
    ioctx.remove(&oid).await.expect("remove");

    let err = lc::get_head(&ioctx, &unique("cls-rgw-lc-absent"))
        .await
        .expect_err("get_head on a missing object");
    assert!(is_osd_error(&err, ENOENT), "{err:?}");
}

#[tokio::test]
#[ignore]
async fn lc_entries_set_get_next_list_rm() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-rgw-lc-entries");
    let b1 = entry(":b1:m1", STATUS_UNINITIAL);
    let b2 = entry(":b2:m2", STATUS_PROCESSING);
    let b3 = entry(":b3:m3", STATUS_COMPLETE);
    for e in [&b1, &b2, &b3] {
        lc::set_entry(&ioctx, &oid, e).await.expect("set_entry");
    }

    let got = lc::get_entry(&ioctx, &oid, ":b2:m2")
        .await
        .expect("get_entry");
    assert_eq!(got, b2);
    let err = lc::get_entry(&ioctx, &oid, "nope")
        .await
        .expect_err("get_entry of an absent key");
    assert!(is_osd_error(&err, ENOENT), "{err:?}");

    let next = |marker: &'static str| lc::get_next_entry(&ioctx, &oid, marker);
    assert_eq!(next("").await.expect("next from start"), b1);
    assert_eq!(next(":b1:m1").await.expect("next after b1"), b2);
    // Past the last key the class answers an empty entry, not ENOENT.
    assert_eq!(
        next(":b3:m3").await.expect("next after b3"),
        LcEntry::default()
    );

    let page = lc::list(&ioctx, &oid, "", 2)
        .await
        .expect("list first page");
    assert_eq!(page.entries, [b1.clone(), b2.clone()]);
    assert!(page.is_truncated);
    let page = lc::list(&ioctx, &oid, ":b2:m2", 10)
        .await
        .expect("list after b2");
    assert_eq!(page.entries, std::slice::from_ref(&b3));
    assert!(!page.is_truncated);

    let b1_done = LcEntry {
        start_time: 5,
        ..entry(":b1:m1", STATUS_COMPLETE)
    };
    lc::set_entry(&ioctx, &oid, &b1_done)
        .await
        .expect("overwrite b1");
    assert_eq!(
        lc::get_entry(&ioctx, &oid, ":b1:m1").await.expect("get b1"),
        b1_done
    );

    lc::rm_entry(&ioctx, &oid, &b2).await.expect("rm b2");
    let all = lc::list(&ioctx, &oid, "", 10).await.expect("list all");
    assert_eq!(all.entries, [b1_done, b3]);
    assert!(!all.is_truncated);
    lc::rm_entry(&ioctx, &oid, &b2).await.expect("rm b2 again");

    ioctx.remove(&oid).await.expect("remove");
}
