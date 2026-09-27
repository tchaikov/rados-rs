//! Cluster tests for the rgw class's usage-log methods. Run with:
//!   CEPH_CONF=... cargo test -p rados-cls --test cls_rgw_usage -- --ignored --nocapture

#[path = "../../rados/tests/common/mod.rs"]
mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::time::UNIX_EPOCH;

use common::create_ioctx;
use rados::{IoCtx, OSDClientError};
use rados_cls::rgw::usage::{self, ReadRet, UsageData, UsageLogEntry, UsageLogInfo, UserBucket};

const ENOENT: i32 = 2;

/// An hour boundary, as RGW rounds an entry's epoch.
const E: u64 = 1_755_892_800;

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

fn entry(owner: &str, bucket: &str, epoch: u64, bytes_sent: u64, ops: u64) -> UsageLogEntry {
    let data = UsageData {
        bytes_sent,
        bytes_received: bytes_sent * 2,
        ops,
        successful_ops: ops,
    };
    UsageLogEntry {
        owner: owner.to_owned(),
        bucket: bucket.to_owned(),
        epoch,
        total_usage: data,
        usage_map: BTreeMap::from([("get_obj".to_owned(), data)]),
        ..UsageLogEntry::default()
    }
}

fn info(entries: Vec<UsageLogEntry>) -> UsageLogInfo {
    UsageLogInfo { entries }
}

fn key(user: &str, bucket: &str) -> UserBucket {
    UserBucket {
        user: user.to_owned(),
        bucket: bucket.to_owned(),
    }
}

fn keys(ret: &ReadRet) -> Vec<UserBucket> {
    ret.usage.keys().cloned().collect()
}

/// The first test's log: two adds into hour `E` and one into the next.
async fn add_two_hours(ioctx: &IoCtx, oid: &str) {
    for e in [
        entry("u1", "b1", E, 100, 1),
        entry("u1", "b1", E, 100, 1),
        entry("u1", "b1", E + 3600, 5, 1),
    ] {
        usage::add(ioctx, oid, &info(vec![e])).await.expect("add");
    }
}

#[tokio::test]
#[ignore]
async fn usage_add_aggregates_and_read_merges() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-rgw-usage-merge");
    add_two_hours(&ioctx, &oid).await;

    // By user: both hours merge into one value per (user, bucket).
    let ret = usage::read(&ioctx, &oid, "u1", "", 0, E + 7200, 100, "")
        .await
        .expect("read");
    assert_eq!(keys(&ret), [key("u1", "b1")]);
    let merged = &ret.usage[&key("u1", "b1")];
    assert_eq!(merged.total_usage.bytes_sent, 205);
    assert_eq!(merged.total_usage.bytes_received, 410);
    assert_eq!(merged.total_usage.ops, 3);
    assert_eq!(merged.usage_map["get_obj"], merged.total_usage);
    assert!(!ret.truncated);
    assert_eq!(ret.next_iter, "");

    // By time: the end epoch is exclusive, so the second hour is out; the
    // first hour's two adds were aggregated into one stored entry.
    let ret = usage::read(&ioctx, &oid, "", "", 0, E + 60, 100, "")
        .await
        .expect("read by time");
    assert_eq!(keys(&ret), [key("u1", "b1")]);
    assert_eq!(ret.usage[&key("u1", "b1")].total_usage.bytes_sent, 200);
    assert_eq!(ret.usage[&key("u1", "b1")].total_usage.ops, 2);

    let ret = usage::read(&ioctx, &oid, "u1", "b2", 0, E + 7200, 100, "")
        .await
        .expect("read other bucket");
    assert!(ret.usage.is_empty(), "{ret:?}");

    let ret = usage::read(&ioctx, &oid, "u1", "", E + 3600, E + 7200, 100, "")
        .await
        .expect("read second hour");
    assert_eq!(keys(&ret), [key("u1", "b1")]);
    assert_eq!(ret.usage[&key("u1", "b1")].total_usage.bytes_sent, 5);
}

#[tokio::test]
#[ignore]
async fn usage_read_pages_by_iter() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-rgw-usage-page");
    let buckets: Vec<String> = (0..5).map(|i| format!("b{i}")).collect();
    let entries = buckets.iter().map(|b| entry("u2", b, E, 1, 1)).collect();
    usage::add(&ioctx, &oid, &info(entries)).await.expect("add");

    let first = usage::read(&ioctx, &oid, "u2", "", 0, E + 60, 2, "")
        .await
        .expect("read");
    assert!(first.truncated);
    assert!(!first.next_iter.is_empty());
    assert_eq!(first.usage.len(), 2);

    let mut seen: BTreeSet<UserBucket> = first.usage.keys().cloned().collect();
    let mut iter = first.next_iter;
    let mut pages = 1;
    loop {
        let page = usage::read(&ioctx, &oid, "u2", "", 0, E + 60, 2, &iter)
            .await
            .expect("read page");
        pages += 1;
        for k in page.usage.keys() {
            assert!(seen.insert(k.clone()), "page {pages} repeats {k:?}");
        }
        if !page.truncated {
            assert_eq!(page.next_iter, "");
            break;
        }
        assert!(pages < 10, "paging did not end");
        iter = page.next_iter;
    }
    let want: BTreeSet<UserBucket> = buckets.iter().map(|b| key("u2", b)).collect();
    assert_eq!(seen, want);
}

#[tokio::test]
#[ignore]
async fn usage_payer_keys_the_entry() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-rgw-usage-payer");
    let mut e = entry("u3", "b1", E, 7, 1);
    e.payer = "p3".to_owned();
    usage::add(&ioctx, &oid, &info(vec![e])).await.expect("add");

    let ret = usage::read(&ioctx, &oid, "p3", "", 0, E + 60, 100, "")
        .await
        .expect("read payer");
    assert_eq!(keys(&ret), [key("p3", "b1")]);
    let stored = &ret.usage[&key("p3", "b1")];
    assert_eq!((stored.owner.as_str(), stored.payer.as_str()), ("u3", "p3"));
    assert_eq!(stored.total_usage.bytes_sent, 7);

    let ret = usage::read(&ioctx, &oid, "u3", "", 0, E + 60, 100, "")
        .await
        .expect("read owner");
    assert!(ret.usage.is_empty(), "{ret:?}");
}

#[tokio::test]
#[ignore]
async fn usage_trim_gives_up_on_a_payer_keyed_entry() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-rgw-usage-stuck-trim");
    let mut e = entry("u4", "b1", E, 7, 1);
    e.payer = "p4".to_owned();
    usage::add(&ioctx, &oid, &info(vec![e])).await.expect("add");

    // v19 derives the keys to remove from the owner, so a trim by time
    // finds the payer-keyed entry every round and removes nothing; the
    // class never answers ENODATA and the client gives up. Ceph commit
    // 674d42d9023 (main, 2025-08-25) derives the keys from the payer, so
    // this pin inverts once the CI image carries it.
    let err = usage::trim(&ioctx, &oid, "", "", 0, u64::MAX)
        .await
        .expect_err("trim by time over a payer-keyed entry");
    assert!(
        matches!(&err, OSDClientError::Other(msg) if msg.contains("rounds without ENODATA")),
        "{err:?}"
    );
    // By owner there is nothing under u4's keys, so the class answers
    // ENODATA at once, and the entry is still there.
    usage::trim(&ioctx, &oid, "u4", "", 0, u64::MAX)
        .await
        .expect("trim by owner");
    let ret = usage::read(&ioctx, &oid, "p4", "", 0, E + 60, 100, "")
        .await
        .expect("read payer");
    assert_eq!(keys(&ret), [key("p4", "b1")]);
}

#[tokio::test]
#[ignore]
async fn usage_trim_and_clear() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-rgw-usage-trim");

    // The class stats the shard before trimming; clear takes a missing
    // shard as already clear.
    let err = usage::trim(&ioctx, &oid, "u1", "", 0, u64::MAX)
        .await
        .expect_err("trim on a missing shard");
    assert!(is_osd_error(&err, ENOENT), "{err:?}");
    usage::clear(&ioctx, &oid)
        .await
        .expect("clear on a missing shard");

    add_two_hours(&ioctx, &oid).await;
    usage::trim(&ioctx, &oid, "u1", "", 0, E + 60)
        .await
        .expect("trim first hour by user");
    let ret = usage::read(&ioctx, &oid, "u1", "", 0, E + 7200, 100, "")
        .await
        .expect("read");
    assert_eq!(keys(&ret), [key("u1", "b1")]);
    assert_eq!(ret.usage[&key("u1", "b1")].total_usage.bytes_sent, 5);
    assert_eq!(ret.usage[&key("u1", "b1")].epoch, E + 3600);

    usage::trim(&ioctx, &oid, "", "", 0, u64::MAX)
        .await
        .expect("trim everything by time");
    for user in ["u1", ""] {
        let ret = usage::read(&ioctx, &oid, user, "", 0, u64::MAX, 100, "")
            .await
            .expect("read after trim");
        assert!(ret.usage.is_empty(), "{user:?}: {ret:?}");
    }

    add_two_hours(&ioctx, &oid).await;
    usage::clear(&ioctx, &oid).await.expect("clear");
    for user in ["u1", ""] {
        let ret = usage::read(&ioctx, &oid, user, "", 0, u64::MAX, 100, "")
            .await
            .expect("read after clear");
        assert!(ret.usage.is_empty(), "{user:?}: {ret:?}");
    }
}
