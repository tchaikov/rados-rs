//! Cluster tests for the rgw class's bucket-index, resharding and
//! head-object methods. Run with:
//!   CEPH_CONF=... cargo test -p rados-cls --test cls_rgw_index -- --ignored --nocapture

#[path = "../../rados/tests/common/mod.rs"]
mod common;

use std::collections::BTreeMap;
use std::time::{Duration, UNIX_EPOCH};

use bytes::Bytes;
use common::create_ioctx;
use rados::{IoCtx, OSDClientError, OmapMap, OpBuilder, UTime};
use rados_cls::rgw::index::{
    self, CompleteOp, DirEntry, DirEntryMeta, ERR_BUSY_RESHARDING, ListOp, ListRet, PrepareOp,
    SuggestOp, Suggestion,
};
use rados_cls::rgw::types::{
    CategoryStats, CheckMtimeType, EntryVer, FLAG_VER_MARKER, ModifyOp, ObjCategory, ObjKey,
    ReshardStatus,
};

const ENOENT: i32 = 2;
const EINVAL: i32 = 22;
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

fn meta(size: u64) -> DirEntryMeta {
    DirEntryMeta {
        category: ObjCategory::MAIN,
        size,
        accounted_size: size,
        etag: "e".to_owned(),
        ..DirEntryMeta::default()
    }
}

async fn prepare(
    ioctx: &IoCtx,
    oid: &str,
    op: ModifyOp,
    tag: &str,
    name: &str,
) -> Result<(), OSDClientError> {
    let req = PrepareOp {
        op,
        key: key(name),
        tag: tag.to_owned(),
        ..PrepareOp::default()
    };
    index::prepare(ioctx, oid, &req).await
}

fn complete_req(op: ModifyOp, tag: &str, name: &str, epoch: u64, size: u64) -> CompleteOp {
    CompleteOp {
        op,
        key: key(name),
        ver: EntryVer { pool: 1, epoch },
        meta: meta(size),
        tag: tag.to_owned(),
        ..CompleteOp::default()
    }
}

async fn complete(
    ioctx: &IoCtx,
    oid: &str,
    op: ModifyOp,
    tag: &str,
    name: &str,
    epoch: u64,
    size: u64,
) -> Result<(), OSDClientError> {
    index::complete(ioctx, oid, &complete_req(op, tag, name, epoch, size)).await
}

/// Prepare and complete an `ADD` of `name`.
async fn add(ioctx: &IoCtx, oid: &str, name: &str, epoch: u64, size: u64) {
    let tag = format!("add-{name}-{epoch}");
    prepare(ioctx, oid, ModifyOp::ADD, &tag, name)
        .await
        .expect("prepare");
    complete(ioctx, oid, ModifyOp::ADD, &tag, name, epoch, size)
        .await
        .expect("complete");
}

async fn stats(ioctx: &IoCtx, oid: &str) -> CategoryStats {
    index::dir_header(ioctx, oid)
        .await
        .expect("dir_header")
        .stats
        .get(&ObjCategory::MAIN)
        .copied()
        .unwrap_or_default()
}

fn names(ret: &ListRet) -> Vec<String> {
    ret.dir.entries.keys().cloned().collect()
}

async fn list(ioctx: &IoCtx, oid: &str, op: ListOp) -> ListRet {
    index::list(ioctx, oid, &op).await.expect("list")
}

fn page(num_entries: u32) -> ListOp {
    ListOp {
        num_entries,
        ..ListOp::default()
    }
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

#[tokio::test]
#[ignore]
async fn index_init_and_header() {
    let (ioctx, oid) = shard("cls-rgw-init").await;

    let err = index::init_index(&ioctx, &oid)
        .await
        .expect_err("second init");
    assert!(is_osd_error(&err, EINVAL), "{err:?}");

    let header = index::dir_header(&ioctx, &oid).await.expect("header");
    assert_eq!(header.ver, 1);
    assert!(header.stats.is_empty());
    assert_eq!(header.tag_timeout, 0);

    index::set_tag_timeout(&ioctx, &oid, 120)
        .await
        .expect("set_tag_timeout");
    let header = index::dir_header(&ioctx, &oid).await.expect("header");
    assert_eq!(header.tag_timeout, 120);
    assert_eq!(header.ver, 2);

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn index_prepare_then_complete_accounts() {
    let (ioctx, oid) = shard("cls-rgw-accounts").await;

    let all: Vec<String> = (0..10).map(|i| format!("obj-{i}")).collect();
    for (i, name) in (0u64..).zip(&all) {
        let tag = format!("t{i}");
        prepare(&ioctx, &oid, ModifyOp::ADD, &tag, name)
            .await
            .expect("prepare");
        assert_eq!(stats(&ioctx, &oid).await.num_entries, i);

        complete(&ioctx, &oid, ModifyOp::ADD, &tag, name, i + 1, 1024)
            .await
            .expect("complete");
        let s = stats(&ioctx, &oid).await;
        assert_eq!(s.num_entries, i + 1);
        assert_eq!(s.total_size, 1024 * (i + 1));
        assert_eq!(s.total_size_rounded, 4096 * (i + 1));
    }

    let ret = list(&ioctx, &oid, page(100)).await;
    assert_eq!(names(&ret), all);
    assert!(!ret.is_truncated);

    let err = complete(&ioctx, &oid, ModifyOp::ADD, "never", "obj-0", 20, 1)
        .await
        .expect_err("complete of an unprepared tag");
    assert!(is_osd_error(&err, EINVAL), "{err:?}");

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn index_stale_epoch_is_a_cancel() {
    let (ioctx, oid) = shard("cls-rgw-stale").await;

    for i in 0..10 {
        prepare(&ioctx, &oid, ModifyOp::ADD, &format!("t{i}"), "x")
            .await
            .expect("prepare");
    }
    for epoch in (1..=10u64).rev() {
        let tag = format!("t{}", epoch - 1);
        complete(&ioctx, &oid, ModifyOp::ADD, &tag, "x", epoch, 1024 * epoch)
            .await
            .expect("complete");
    }

    let s = stats(&ioctx, &oid).await;
    assert_eq!(s.num_entries, 1);
    assert_eq!(s.total_size, 10240);
    let ret = list(&ioctx, &oid, page(100)).await;
    assert_eq!(names(&ret), vec!["x".to_owned()]);
    // Each cancelled complete still stores its ver on the entry.
    assert_eq!(ret.dir.entries["x"].ver.epoch, 1);

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn index_delete_with_a_pending_tag_keeps_the_key() {
    let (ioctx, oid) = shard("cls-rgw-del-pending").await;

    add(&ioctx, &oid, "x", 1, 1024).await;
    prepare(&ioctx, &oid, ModifyOp::DEL, "d", "x")
        .await
        .expect("prepare del");
    prepare(&ioctx, &oid, ModifyOp::ADD, "a", "x")
        .await
        .expect("prepare add");

    complete(&ioctx, &oid, ModifyOp::DEL, "d", "x", 2, 0)
        .await
        .expect("complete del");
    assert_eq!(stats(&ioctx, &oid).await.num_entries, 0);
    let versions = ListOp {
        list_versions: true,
        ..page(100)
    };
    let ret = list(&ioctx, &oid, versions).await;
    assert_eq!(names(&ret), vec!["x".to_owned()]);
    assert!(!ret.dir.entries["x"].exists);
    // bucket_list does not look at exists; RGW filters such entries.
    let ret = list(&ioctx, &oid, page(100)).await;
    assert_eq!(names(&ret), vec!["x".to_owned()]);
    assert!(!ret.dir.entries["x"].exists);

    complete(&ioctx, &oid, ModifyOp::ADD, "a", "x", 3, 2048)
        .await
        .expect("complete add");
    let s = stats(&ioctx, &oid).await;
    assert_eq!(s.num_entries, 1);
    assert_eq!(s.total_size, 2048);
    let ret = list(&ioctx, &oid, page(100)).await;
    assert_eq!(names(&ret), vec!["x".to_owned()]);
    assert!(ret.dir.entries["x"].exists);

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn index_list_pages_and_delimits() {
    let (ioctx, oid) = shard("cls-rgw-pages").await;

    for (epoch, name) in (1u64..).zip(["a/1", "a/2", "b", "c/1", "d"]) {
        add(&ioctx, &oid, name, epoch, 10).await;
    }
    let strings = |v: &[&str]| v.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();

    let first = list(&ioctx, &oid, page(2)).await;
    assert_eq!(names(&first), strings(&["a/1", "a/2"]));
    assert!(first.is_truncated);
    assert!(!first.marker.name.is_empty());

    let rest = ListOp {
        start_obj: first.marker.clone(),
        ..page(100)
    };
    let rest = list(&ioctx, &oid, rest).await;
    assert_eq!(names(&rest), strings(&["b", "c/1", "d"]));
    assert!(!rest.is_truncated);

    let delimited = ListOp {
        delimiter: "/".to_owned(),
        ..page(100)
    };
    let delimited = list(&ioctx, &oid, delimited).await;
    assert_eq!(names(&delimited), strings(&["a/", "b", "c/", "d"]));
    for (name, entry) in &delimited.dir.entries {
        assert_eq!(entry.is_common_prefix(), name.ends_with('/'), "{name}");
    }

    let prefixed = ListOp {
        filter_prefix: "a/".to_owned(),
        ..page(100)
    };
    let prefixed = list(&ioctx, &oid, prefixed).await;
    assert_eq!(names(&prefixed), strings(&["a/1", "a/2"]));

    let after_b = ListOp {
        start_obj: key("b"),
        ..page(100)
    };
    let after_b = list(&ioctx, &oid, after_b).await;
    assert_eq!(names(&after_b), strings(&["c/1", "d"]));

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn index_list_skips_the_special_namespace() {
    let (ioctx, oid) = shard("cls-rgw-namespace").await;

    // "é" starts with 0xc3, so one real entry sorts after the namespace.
    add(&ioctx, &oid, "a", 1, 10).await;
    add(&ioctx, &oid, "é", 2, 10).await;
    let mut raw = OmapMap::new();
    raw.insert(
        Bytes::from_static(b"\x801000_junk"),
        Bytes::from_static(b"x"),
    );
    raw.insert(Bytes::from_static(b"\x800_junk"), Bytes::from_static(b"x"));
    ioctx.omap_set(&oid, &raw).await.expect("omap_set");

    let ret = list(&ioctx, &oid, page(100)).await;
    assert_eq!(names(&ret), vec!["a".to_owned(), "é".to_owned()]);
    assert_eq!(
        index::dir_header(&ioctx, &oid).await.expect("header").stats[&ObjCategory::MAIN]
            .num_entries,
        2
    );

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn index_list_advances_past_invalid_entries() {
    let (ioctx, oid) = shard("cls-rgw-advance").await;

    for chunk in 0..9 {
        let mut vals = OmapMap::new();
        for i in chunk * 1000..(chunk + 1) * 1000 {
            let name = format!("inv-{i:05}");
            let entry = DirEntry {
                key: key(&name),
                exists: true,
                meta: meta(1),
                flags: FLAG_VER_MARKER,
                ..DirEntry::default()
            };
            vals.insert(
                Bytes::from(name),
                rados::encode_with_capacity(&entry, 0).expect("encode"),
            );
        }
        ioctx.omap_set(&oid, &vals).await.expect("omap_set");
    }
    add(&ioctx, &oid, "z", 1, 10).await;

    // One call scans at most eight times num_entries keys (cls_rgw.cc's
    // max_attempts) before answering EFBIG with a marker, so 9000
    // invalid entries make the client's loop send a second call.
    let ret = list(&ioctx, &oid, page(1000)).await;
    assert_eq!(names(&ret), vec!["z".to_owned()]);

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn index_suggest_after_the_tag_expires() {
    let (ioctx, oid) = shard("cls-rgw-suggest").await;

    index::set_tag_timeout(&ioctx, &oid, 1)
        .await
        .expect("set_tag_timeout");
    prepare(&ioctx, &oid, ModifyOp::ADD, "p", "s")
        .await
        .expect("prepare");
    let update = Suggestion {
        op: SuggestOp::Update,
        log: false,
        entry: DirEntry {
            key: key("s"),
            exists: true,
            meta: meta(512),
            ..DirEntry::default()
        },
    };

    index::suggest_changes(&ioctx, &oid, std::slice::from_ref(&update))
        .await
        .expect("suggest while pending");
    assert_eq!(stats(&ioctx, &oid).await.num_entries, 0);

    tokio::time::sleep(Duration::from_secs(2)).await;
    index::suggest_changes(&ioctx, &oid, std::slice::from_ref(&update))
        .await
        .expect("suggest after expiry");
    let s = stats(&ioctx, &oid).await;
    assert_eq!(s.num_entries, 1);
    assert_eq!(s.total_size, 512);
    let ret = list(&ioctx, &oid, page(100)).await;
    assert_eq!(names(&ret), vec!["s".to_owned()]);

    // The applied update stamped the entry with the header's ver; a
    // suggestion with an older index_ver is ignored.
    let remove = Suggestion {
        op: SuggestOp::Remove,
        log: false,
        entry: ret.dir.entries["s"].clone(),
    };
    index::suggest_changes(&ioctx, &oid, std::slice::from_ref(&remove))
        .await
        .expect("suggest remove");
    assert_eq!(stats(&ioctx, &oid).await.num_entries, 0);
    index::suggest_changes(&ioctx, &oid, std::slice::from_ref(&remove))
        .await
        .expect("repeated remove");
    assert_eq!(stats(&ioctx, &oid).await.num_entries, 0);

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn index_check_and_rebuild() {
    let (ioctx, oid) = shard("cls-rgw-rebuild").await;

    add(&ioctx, &oid, "one", 1, 100).await;
    add(&ioctx, &oid, "two", 2, 5000).await;
    let real = CategoryStats {
        total_size: 5100,
        total_size_rounded: 4096 + 8192,
        num_entries: 2,
        actual_size: 5100,
    };
    assert_eq!(stats(&ioctx, &oid).await, real);

    let corrupt = CategoryStats {
        total_size: 1,
        total_size_rounded: 4096,
        num_entries: 1,
        actual_size: 1,
    };
    let stats_map = BTreeMap::from([(ObjCategory::MAIN, corrupt)]);
    index::update_stats(&ioctx, &oid, true, &stats_map)
        .await
        .expect("update_stats");

    let check = index::check_index(&ioctx, &oid).await.expect("check_index");
    assert_eq!(check.existing_header.stats[&ObjCategory::MAIN], corrupt);
    assert_eq!(check.calculated_header.stats[&ObjCategory::MAIN], real);

    let before = index::dir_header(&ioctx, &oid).await.expect("header").ver;
    index::rebuild_index(&ioctx, &oid)
        .await
        .expect("rebuild_index");
    let header = index::dir_header(&ioctx, &oid).await.expect("header");
    assert_eq!(header.stats, check.calculated_header.stats);
    assert!(header.ver > before, "{} > {before}", header.ver);

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn index_resharding_guard() {
    let (ioctx, oid) = shard("cls-rgw-guard").await;

    let guarded_prepare = |tag: &str, name: &str| {
        let req = PrepareOp {
            op: ModifyOp::ADD,
            key: key(name),
            tag: tag.to_owned(),
            ..PrepareOp::default()
        };
        OpBuilder::new()
            .op(index::guard_op().expect("guard_op"))
            .op(index::prepare_op(&req).expect("prepare_op"))
            .build()
    };
    let versions = ListOp {
        list_versions: true,
        ..page(100)
    };

    assert_eq!(
        index::get_bucket_resharding(&ioctx, &oid)
            .await
            .expect("get")
            .reshard_status,
        ReshardStatus::NOT_RESHARDING
    );
    ioctx
        .execute_op(&oid, guarded_prepare("g1", "g"))
        .await
        .expect("guarded prepare while not resharding");

    index::set_bucket_resharding(&ioctx, &oid, ReshardStatus::IN_PROGRESS)
        .await
        .expect("set_bucket_resharding");
    assert_eq!(
        index::get_bucket_resharding(&ioctx, &oid)
            .await
            .expect("get")
            .reshard_status,
        ReshardStatus::IN_PROGRESS
    );

    let before = stats(&ioctx, &oid).await;
    let err = ioctx
        .execute_op(&oid, guarded_prepare("h1", "h"))
        .await
        .expect_err("guarded prepare while resharding");
    assert!(is_osd_error(&err, ERR_BUSY_RESHARDING), "{err:?}");
    assert_eq!(stats(&ioctx, &oid).await, before);
    let ret = list(&ioctx, &oid, versions.clone()).await;
    assert_eq!(names(&ret), vec!["g".to_owned()]);

    let err = index::guard_bucket_resharding(&ioctx, &oid, -ERR_BUSY_RESHARDING)
        .await
        .expect_err("guard alone");
    assert!(is_osd_error(&err, ERR_BUSY_RESHARDING), "{err:?}");

    index::clear_bucket_resharding(&ioctx, &oid)
        .await
        .expect("clear_bucket_resharding");
    ioctx
        .execute_op(&oid, guarded_prepare("h2", "h"))
        .await
        .expect("guarded prepare after the reshard");
    let ret = list(&ioctx, &oid, versions).await;
    assert_eq!(names(&ret), vec!["g".to_owned(), "h".to_owned()]);

    ioctx.remove(&oid).await.expect("remove");
}

fn utime(t: std::time::SystemTime) -> UTime {
    let d = t.duration_since(UNIX_EPOCH).expect("after the epoch");
    UTime {
        sec: u32::try_from(d.as_secs()).expect("fits"),
        nsec: d.subsec_nanos(),
    }
}

#[tokio::test]
#[ignore]
async fn head_object_helpers() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-rgw-head");
    ioctx
        .write_full(&oid, Bytes::from_static(b"head"))
        .await
        .expect("write_full");
    ioctx
        .set_xattr(&oid, "user.rgw.a", Bytes::from_static(b"a"))
        .await
        .expect("set_xattr");
    ioctx
        .set_xattr(&oid, "user.other", Bytes::from_static(b"o"))
        .await
        .expect("set_xattr");

    for (prefix, fail_if_exist, errno) in [
        ("user.rgw.", true, Some(ECANCELED)),
        ("user.rgw.", false, None),
        ("user.none.", true, None),
        ("user.none.", false, Some(ECANCELED)),
        ("", true, Some(EINVAL)),
        ("", false, Some(EINVAL)),
    ] {
        let got = index::check_attrs_prefix(&ioctx, &oid, prefix, fail_if_exist).await;
        match errno {
            None => got.expect("check_attrs_prefix"),
            Some(errno) => {
                let err = got.expect_err("check_attrs_prefix");
                assert!(
                    is_osd_error(&err, errno),
                    "{prefix:?} {fail_if_exist}: {err:?}"
                );
            }
        }
    }

    let m = utime(ioctx.stat(&oid).await.expect("stat").mtime);
    let shift = |secs: i64| UTime {
        sec: u32::try_from(i64::from(m.sec) + secs).expect("fits"),
        nsec: m.nsec,
    };
    for (mtime, kind, errno) in [
        (m, CheckMtimeType::EQ, None),
        (m, CheckMtimeType::GT, Some(ECANCELED)),
        (shift(-1), CheckMtimeType::GT, None),
        (shift(1), CheckMtimeType::LT, None),
    ] {
        let got = index::check_mtime(&ioctx, &oid, mtime, kind, false).await;
        match errno {
            None => got.expect("check_mtime"),
            Some(errno) => {
                let err = got.expect_err("check_mtime");
                assert!(is_osd_error(&err, errno), "{kind:?}: {err:?}");
            }
        }
    }

    index::store_pg_ver(&ioctx, &oid, "user.pgver")
        .await
        .expect("store_pg_ver");
    let ver = ioctx
        .get_xattr(&oid, "user.pgver")
        .await
        .expect("get_xattr");
    let ver = u64::from_le_bytes(ver.as_ref().try_into().expect("a u64"));
    assert!(ver > 0);

    index::remove_obj(&ioctx, &oid, &["user.rgw.".to_owned()])
        .await
        .expect("remove_obj keeping user.rgw.");
    ioctx.stat(&oid).await.expect("recreated");
    let xattrs = ioctx.get_xattrs(&oid).await.expect("get_xattrs");
    assert_eq!(
        xattrs.keys().collect::<Vec<_>>(),
        vec!["user.rgw.a"],
        "{xattrs:?}"
    );

    index::remove_obj(&ioctx, &oid, &[])
        .await
        .expect("remove_obj");
    let err = ioctx.stat(&oid).await.expect_err("gone");
    assert!(is_osd_error(&err, ENOENT), "{err:?}");
    let err = index::remove_obj(&ioctx, &oid, &[])
        .await
        .expect_err("remove_obj of a missing object");
    assert!(is_osd_error(&err, ENOENT), "{err:?}");
}
