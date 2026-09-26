//! Cluster tests for the rgw class's OLH (object versioning) methods. Run
//! with:
//!   CEPH_CONF=... cargo test -p rados-cls --test cls_rgw_olh -- --ignored --nocapture

#[path = "../../rados/tests/common/mod.rs"]
mod common;

use std::time::UNIX_EPOCH;

use common::create_ioctx;
use rados::{IoCtx, OSDClientError};
use rados_cls::rgw::index::{self, CompleteOp, DirEntry, DirEntryMeta, ListOp, PrepareOp};
use rados_cls::rgw::olh::{self, LinkOlhOp, OlhLogOp, ReadOlhLogRet, UnlinkInstanceOp};
use rados_cls::rgw::types::{EntryVer, FLAG_VER, ModifyOp, ObjCategory, ObjKey};

const ENOENT: i32 = 2;
const ECANCELED: i32 = 125;

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

fn key(name: &str) -> ObjKey {
    ObjKey {
        name: name.to_owned(),
        instance: String::new(),
    }
}

fn instance(name: &str, instance: &str) -> ObjKey {
    ObjKey {
        name: name.to_owned(),
        instance: instance.to_owned(),
    }
}

fn meta(size: u64) -> DirEntryMeta {
    DirEntryMeta {
        category: ObjCategory::MAIN,
        size,
        accounted_size: size,
        etag: "e".to_owned(),
        ..DirEntryMeta::default()
    }
}

fn olh_tag(name: &str) -> String {
    format!("olh-{name}")
}

/// A fresh, initialised index shard.
async fn shard(prefix: &str) -> (IoCtx, String) {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique(prefix);
    ioctx.create(&oid, true).await.expect("create");
    index::init_index(&ioctx, &oid).await.expect("init_index");
    (ioctx, oid)
}

/// Prepare and complete an `ADD` of the instance `name`/`inst` at
/// `epoch`, leaving its instance entry for a link to find.
async fn add_instance(
    ioctx: &IoCtx,
    oid: &str,
    name: &str,
    inst: &str,
    tag: &str,
    epoch: u64,
    size: u64,
) {
    let prepare = PrepareOp {
        op: ModifyOp::ADD,
        key: instance(name, inst),
        tag: tag.to_owned(),
        ..PrepareOp::default()
    };
    index::prepare(ioctx, oid, &prepare).await.expect("prepare");
    let complete = CompleteOp {
        op: ModifyOp::ADD,
        key: instance(name, inst),
        ver: EntryVer { pool: 1, epoch },
        meta: meta(size),
        tag: tag.to_owned(),
        ..CompleteOp::default()
    };
    index::complete(ioctx, oid, &complete)
        .await
        .expect("complete");
}

fn link_req(name: &str, inst: &str, tag: &str, epoch: u64, size: u64) -> LinkOlhOp {
    LinkOlhOp {
        key: instance(name, inst),
        olh_tag: olh_tag(name),
        op_tag: tag.to_owned(),
        meta: meta(size),
        olh_epoch: epoch,
        ..LinkOlhOp::default()
    }
}

/// Write the instance `name`/`inst` and link it under OLH epoch `epoch`
/// (0 for the class to pick) with the tag `olh-<name>`.
async fn put_version(
    ioctx: &IoCtx,
    oid: &str,
    name: &str,
    inst: &str,
    tag: &str,
    epoch: u64,
    size: u64,
) {
    add_instance(ioctx, oid, name, inst, tag, epoch, size).await;
    olh::link_olh(ioctx, oid, &link_req(name, inst, tag, epoch, size))
        .await
        .expect("link_olh");
}

/// Every listed version of `name`, newest first.
async fn versions(ioctx: &IoCtx, oid: &str, name: &str) -> Vec<DirEntry> {
    let op = ListOp {
        num_entries: 100,
        list_versions: true,
        ..ListOp::default()
    };
    let ret = index::list(ioctx, oid, &op).await.expect("list versions");
    ret.dir
        .entries
        .into_values()
        .filter(|e| e.key.name == name)
        .collect()
}

/// The entries a plain (non-versioned) listing returns for `name`.
async fn plain(ioctx: &IoCtx, oid: &str, name: &str) -> Vec<DirEntry> {
    let op = ListOp {
        num_entries: 100,
        ..ListOp::default()
    };
    let ret = index::list(ioctx, oid, &op).await.expect("list");
    ret.dir
        .entries
        .into_values()
        .filter(|e| e.key.name == name)
        .collect()
}

fn current(entries: &[DirEntry]) -> Option<&str> {
    entries
        .iter()
        .find(|e| e.is_current())
        .map(|e| e.key.instance.as_str())
}

fn instances(entries: &[DirEntry]) -> Vec<&str> {
    entries.iter().map(|e| e.key.instance.as_str()).collect()
}

fn epochs(log: &ReadOlhLogRet) -> Vec<u64> {
    log.log.keys().copied().collect()
}

/// The op and instance of every entry logged under `epoch`.
fn logged(log: &ReadOlhLogRet, epoch: u64) -> Vec<(OlhLogOp, &str)> {
    log.log[&epoch]
        .iter()
        .map(|e| (e.op, e.key.instance.as_str()))
        .collect()
}

async fn read_log(ioctx: &IoCtx, oid: &str, name: &str, marker: u64) -> ReadOlhLogRet {
    olh::read_olh_log(ioctx, oid, &key(name), marker, &olh_tag(name))
        .await
        .expect("read_olh_log")
}

#[tokio::test]
#[ignore]
async fn olh_link_promotes_and_logs() {
    let (ioctx, oid) = shard("cls-rgw-olh-link").await;

    put_version(&ioctx, &oid, "o", "v1", "t1", 10, 100).await;
    let v = versions(&ioctx, &oid, "o").await;
    assert_eq!(instances(&v), ["v1"]);
    assert!(v[0].is_current());
    assert_ne!(v[0].flags & FLAG_VER, 0);
    assert_eq!(instances(&plain(&ioctx, &oid, "o").await), ["v1"]);

    put_version(&ioctx, &oid, "o", "v2", "t2", 20, 200).await;
    let v = versions(&ioctx, &oid, "o").await;
    assert_eq!(instances(&v), ["v2", "v1"]);
    assert_eq!(current(&v), Some("v2"));
    assert!(!v[1].is_current());

    let log = read_log(&ioctx, &oid, "o", 0).await;
    assert_eq!(epochs(&log), [10, 20]);
    assert_eq!(logged(&log, 10), [(OlhLogOp::LINK_OLH, "v1")]);
    assert_eq!(logged(&log, 20), [(OlhLogOp::LINK_OLH, "v2")]);
    assert!(!log.is_truncated);

    let log = read_log(&ioctx, &oid, "o", 10).await;
    assert_eq!(epochs(&log), [20]);

    let err = olh::read_olh_log(&ioctx, &oid, &key("o"), 0, "wrong")
        .await
        .expect_err("read with the wrong tag");
    assert!(is_osd_error(&err, ECANCELED), "{err:?}");

    add_instance(&ioctx, &oid, "o", "v3", "t3", 30, 300).await;
    let wrong = LinkOlhOp {
        olh_tag: "wrong".to_owned(),
        ..link_req("o", "v3", "t3", 30, 300)
    };
    let err = olh::link_olh(&ioctx, &oid, &wrong)
        .await
        .expect_err("link with the wrong tag");
    assert!(is_osd_error(&err, ECANCELED), "{err:?}");
    assert_eq!(current(&versions(&ioctx, &oid, "o").await), Some("v2"));

    olh::trim_olh_log(&ioctx, &oid, &key("o"), 10, &olh_tag("o"))
        .await
        .expect("trim_olh_log");
    let log = read_log(&ioctx, &oid, "o", 0).await;
    assert_eq!(epochs(&log), [20]);

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn olh_stale_epoch_leaves_the_head() {
    let (ioctx, oid) = shard("cls-rgw-olh-stale").await;

    put_version(&ioctx, &oid, "o", "v1", "t1", 10, 100).await;
    put_version(&ioctx, &oid, "o", "v2", "t2", 20, 200).await;
    put_version(&ioctx, &oid, "o", "v0", "t0", 15, 50).await;

    let v = versions(&ioctx, &oid, "o").await;
    assert_eq!(current(&v), Some("v2"));
    let v0 = v
        .iter()
        .find(|e| e.key.instance == "v0")
        .expect("v0 listed");
    assert!(!v0.is_current());
    assert_eq!(epochs(&read_log(&ioctx, &oid, "o", 0).await), [10, 20]);

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn olh_unlink_promotes_the_previous_version() {
    let (ioctx, oid) = shard("cls-rgw-olh-unlink").await;

    put_version(&ioctx, &oid, "o", "v1", "t1", 10, 100).await;
    put_version(&ioctx, &oid, "o", "v2", "t2", 20, 200).await;

    let unlink = |inst: &str, op_tag: &str, epoch: u64| UnlinkInstanceOp {
        key: instance("o", inst),
        op_tag: op_tag.to_owned(),
        olh_epoch: epoch,
        olh_tag: olh_tag("o"),
        ..UnlinkInstanceOp::default()
    };
    olh::unlink_instance(&ioctx, &oid, &unlink("v2", "u1", 30))
        .await
        .expect("unlink v2");
    let v = versions(&ioctx, &oid, "o").await;
    assert_eq!(instances(&v), ["v1"]);
    assert_eq!(current(&v), Some("v1"));

    let err = olh::unlink_instance(&ioctx, &oid, &unlink("missing", "u2", 35))
        .await
        .expect_err("unlink of a missing instance");
    assert!(is_osd_error(&err, ENOENT), "{err:?}");

    olh::unlink_instance(&ioctx, &oid, &unlink("v1", "u3", 40))
        .await
        .expect("unlink v1");
    assert!(plain(&ioctx, &oid, "o").await.is_empty());
    let log = read_log(&ioctx, &oid, "o", 0).await;
    assert_eq!(epochs(&log).last(), Some(&40));
    assert!(
        logged(&log, 40)
            .iter()
            .any(|(op, _)| *op == OlhLogOp::UNLINK_OLH),
        "{log:?}"
    );

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn olh_delete_marker_and_clear() {
    let (ioctx, oid) = shard("cls-rgw-olh-dm").await;

    put_version(&ioctx, &oid, "o", "v1", "t1", 10, 100).await;
    let marker = LinkOlhOp {
        key: instance("o", "dm1"),
        olh_tag: olh_tag("o"),
        delete_marker: true,
        op_tag: "d1".to_owned(),
        olh_epoch: 20,
        ..LinkOlhOp::default()
    };
    olh::link_olh(&ioctx, &oid, &marker)
        .await
        .expect("link delete marker");

    assert!(plain(&ioctx, &oid, "o").await.is_empty());
    let v = versions(&ioctx, &oid, "o").await;
    assert_eq!(instances(&v), ["dm1", "v1"]);
    assert_eq!(current(&v), Some("dm1"));
    assert!(v[0].is_delete_marker());
    assert!(!v[1].is_current());

    let err = olh::clear_olh(&ioctx, &oid, &key("o"), "wrong")
        .await
        .expect_err("clear with the wrong tag");
    assert!(is_osd_error(&err, ECANCELED), "{err:?}");
    olh::clear_olh(&ioctx, &oid, &key("o"), &olh_tag("o"))
        .await
        .expect("clear_olh");
    olh::clear_olh(&ioctx, &oid, &key("o"), "")
        .await
        .expect("clear_olh of a missing OLH");

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn olh_epoch_zero_is_assigned_by_the_class() {
    let (ioctx, oid) = shard("cls-rgw-olh-epoch0").await;

    put_version(&ioctx, &oid, "p", "a", "ta", 0, 1).await;
    put_version(&ioctx, &oid, "p", "b", "tb", 0, 1).await;

    let log = read_log(&ioctx, &oid, "p", 0).await;
    assert_eq!(epochs(&log), [2, 3]);
    assert_eq!(current(&versions(&ioctx, &oid, "p").await), Some("b"));

    ioctx.remove(&oid).await.expect("remove");
}
