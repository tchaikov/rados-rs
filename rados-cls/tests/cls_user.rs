//! Cluster tests for the user class. Run with:
//!   CEPH_CONF=... cargo test -p rados-cls --test cls_user -- --ignored --nocapture

#[path = "../../rados/tests/common/mod.rs"]
mod common;

use bytes::Bytes;
use common::create_ioctx;
use rados::{OSDClientError, UTime};
use rados_cls::user::{self, AccountResource, Bucket, BucketEntry, ResetStats2Op, Stats};

const ENOENT: i32 = 2;
const EEXIST: i32 = 17;
const EUSERS: i32 = 87;

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

fn at(sec: u32) -> UTime {
    UTime { sec, nsec: 0 }
}

fn bucket(name: &str) -> Bucket {
    Bucket {
        name: name.to_owned(),
        marker: format!("{name}-marker"),
        bucket_id: format!("{name}-id"),
        ..Bucket::default()
    }
}

fn entry(name: &str, size: u64, count: u64) -> BucketEntry {
    BucketEntry {
        bucket: bucket(name),
        size,
        size_rounded: size + 2,
        creation_time: at(1_700_000_000),
        count,
        user_stats_sync: false,
    }
}

fn names(ret: &user::ListBucketsRet) -> Vec<String> {
    ret.entries.iter().map(|e| e.bucket.name.clone()).collect()
}

fn resource(name: &str, path: &str) -> AccountResource {
    AccountResource {
        name: name.to_owned(),
        path: path.to_owned(),
        metadata: Bytes::from_static(b"opaque"),
    }
}

#[tokio::test]
#[ignore]
async fn user_set_list_remove_buckets() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-user-buckets");
    ioctx.create(&oid, true).await.expect("create");

    let entries = [entry("b1", 10, 1), entry("b2", 20, 2)];
    user::set_buckets(&ioctx, &oid, &entries, true, at(400_000_000))
        .await
        .expect("set_buckets add");
    let listed = user::list_buckets(&ioctx, &oid, "", "", 1000)
        .await
        .expect("list");
    assert_eq!(names(&listed), vec!["b1".to_owned(), "b2".to_owned()]);
    assert!(!listed.truncated);
    assert!(listed.entries.iter().all(|e| e.user_stats_sync));
    assert_eq!(listed.entries[0].size, 10);
    assert_eq!(listed.entries[1].bucket.bucket_id, "b2-id");

    let header = user::get_header(&ioctx, &oid).await.expect("header");
    assert_eq!(
        header.stats,
        Stats {
            total_entries: 3,
            total_bytes: 30,
            total_bytes_rounded: 34,
        }
    );
    assert_eq!(header.last_stats_update, at(400_000_000));

    // Without `add`, an absent bucket is skipped and stats are overwritten.
    user::set_buckets(
        &ioctx,
        &oid,
        &[entry("b1", 100, 5), entry("b9", 1, 1)],
        false,
        at(400_000_001),
    )
    .await
    .expect("set_buckets overwrite");
    let listed = user::list_buckets(&ioctx, &oid, "", "", 1000)
        .await
        .expect("list");
    assert_eq!(names(&listed), vec!["b1".to_owned(), "b2".to_owned()]);
    assert_eq!(listed.entries[0].size, 100);
    let header = user::get_header(&ioctx, &oid).await.expect("header");
    assert_eq!(header.stats.total_entries, 7);
    assert_eq!(header.stats.total_bytes, 120);

    user::remove_bucket(&ioctx, &oid, &bucket("b1"))
        .await
        .expect("remove b1");
    user::remove_bucket(&ioctx, &oid, &bucket("b1"))
        .await
        .expect("removing an absent bucket succeeds");
    let listed = user::list_buckets(&ioctx, &oid, "", "", 1000)
        .await
        .expect("list");
    assert_eq!(names(&listed), vec!["b2".to_owned()]);
    let header = user::get_header(&ioctx, &oid).await.expect("header");
    assert_eq!(
        header.stats,
        Stats {
            total_entries: 2,
            total_bytes: 20,
            total_bytes_rounded: 22,
        }
    );

    user::complete_stats_sync(&ioctx, &oid, at(400_000_050))
        .await
        .expect("complete_stats_sync");
    let header = user::get_header(&ioctx, &oid).await.expect("header");
    assert_eq!(header.last_stats_sync, at(400_000_050));

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn user_list_buckets_pages() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-user-pages");
    ioctx.create(&oid, true).await.expect("create");
    let entries = [entry("a", 1, 1), entry("b", 1, 1), entry("c", 1, 1)];
    user::set_buckets(&ioctx, &oid, &entries, true, at(400_000_000))
        .await
        .expect("set_buckets");

    let first = user::list_buckets(&ioctx, &oid, "", "", 2)
        .await
        .expect("page 1");
    assert_eq!(names(&first), vec!["a".to_owned(), "b".to_owned()]);
    assert!(first.truncated);
    assert_eq!(first.marker, "b");
    let second = user::list_buckets(&ioctx, &oid, &first.marker, "", 2)
        .await
        .expect("page 2");
    assert_eq!(names(&second), vec!["c".to_owned()]);
    assert!(!second.truncated);
    assert_eq!(second.marker, "", "marker is set only when truncated");

    let bounded = user::list_buckets(&ioctx, &oid, "", "b", 1000)
        .await
        .expect("end_marker");
    assert_eq!(
        names(&bounded),
        vec!["a".to_owned()],
        "stops before end_marker"
    );
    assert!(!bounded.truncated);

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn user_reset_stats() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-user-reset");
    ioctx.create(&oid, true).await.expect("create");
    let entries = [entry("x", 5, 1), entry("y", 7, 3)];
    user::set_buckets(&ioctx, &oid, &entries, true, at(400_000_000))
        .await
        .expect("set_buckets");
    let want = Stats {
        total_entries: 4,
        total_bytes: 12,
        total_bytes_rounded: 16,
    };

    user::reset_stats(&ioctx, &oid, at(400_000_100))
        .await
        .expect("reset_stats");
    let header = user::get_header(&ioctx, &oid).await.expect("header");
    assert_eq!(header.stats, want);
    assert_eq!(header.last_stats_update, at(400_000_100));

    let mut op = ResetStats2Op {
        time: at(400_000_200),
        ..ResetStats2Op::default()
    };
    let ret = loop {
        let ret = user::reset_stats2(&ioctx, &oid, &op)
            .await
            .expect("reset_stats2");
        if !ret.truncated {
            break ret;
        }
        op.marker = ret.marker;
        op.acc_stats = ret.acc_stats;
    };
    assert_eq!(ret.acc_stats, want);
    let header = user::get_header(&ioctx, &oid).await.expect("header");
    assert_eq!(header.stats, want);
    assert_eq!(header.last_stats_update, at(400_000_200));

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn user_account_resources() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-user-account");
    ioctx.create(&oid, true).await.expect("create");

    user::account_resource_add(&ioctx, &oid, &resource("Alpha", "/a/x"), false, 2)
        .await
        .expect("add Alpha");
    user::account_resource_add(&ioctx, &oid, &resource("beta", "/b/y"), false, 2)
        .await
        .expect("add beta");
    let err = user::account_resource_add(&ioctx, &oid, &resource("gamma", "/a/z"), false, 2)
        .await
        .expect_err("past the limit");
    assert!(is_osd_error(&err, EUSERS), "{err:?}");
    let err = user::account_resource_add(&ioctx, &oid, &resource("Alpha", "/a/x"), true, 2)
        .await
        .expect_err("exclusive add of an existing resource");
    assert!(is_osd_error(&err, EEXIST), "{err:?}");
    user::account_resource_add(&ioctx, &oid, &resource("Alpha", "/a/x2"), false, 2)
        .await
        .expect("a non-exclusive add overwrites");

    let got = user::account_resource_get(&ioctx, &oid, "alpha")
        .await
        .expect("get by lower-cased name");
    assert_eq!(got.name, "Alpha");
    assert_eq!(got.path, "/a/x2");
    assert_eq!(got.metadata, Bytes::from_static(b"opaque"));
    let err = user::account_resource_get(&ioctx, &oid, "nope")
        .await
        .expect_err("missing");
    assert!(is_osd_error(&err, ENOENT), "{err:?}");

    let listed = user::account_resource_list(&ioctx, &oid, "", "/a/", 100)
        .await
        .expect("list");
    assert_eq!(listed.entries.len(), 1);
    assert_eq!(listed.entries[0].name, "Alpha");
    assert!(!listed.truncated);
    let all = user::account_resource_list(&ioctx, &oid, "", "", 100)
        .await
        .expect("list all");
    assert_eq!(all.entries.len(), 2);
    assert_eq!(
        all.marker, "beta",
        "the last omap key of the page, lower-cased"
    );

    user::account_resource_rm(&ioctx, &oid, "ALPHA")
        .await
        .expect("rm by any case");
    let err = user::account_resource_rm(&ioctx, &oid, "alpha")
        .await
        .expect_err("already removed");
    assert!(is_osd_error(&err, ENOENT), "{err:?}");
    user::account_resource_add(&ioctx, &oid, &resource("gamma", "/a/z"), false, 2)
        .await
        .expect("the count went down, so gamma fits");

    ioctx.remove(&oid).await.expect("remove");
}
