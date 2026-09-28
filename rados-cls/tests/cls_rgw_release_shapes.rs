//! Cluster tests for what a Squid rgw class does with the request shapes
//! of later releases. They pin Squid only: on a cluster whose
//! `require_osd_release` is any other release the same requests mean
//! something else, so there every test prints a `skipped` line and
//! passes. `CEPH_TEST_EXPECT_OSD_RELEASE` names the release the cluster
//! must be, and any other fails instead; CI sets it to squid, so a newer
//! compose image cannot turn these checks into skips. What Tentacle and
//! Umbrella themselves do with these shapes cannot be exercised here; the
//! byte pins in `rgw::index` and `rgw::olh`, taken from their
//! `ceph-dencoder`, are the evidence for those shapes. Run with:
//!   CEPH_CONF=... cargo test -p rados-cls --test cls_rgw_release_shapes -- --ignored --nocapture

#[path = "../../rados/tests/common/mod.rs"]
mod common;

use std::collections::BTreeMap;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use common::create_ioctx;
use rados::{CephRelease, IoCtx, OSDClientError, UTime};
use rados_cls::rgw::index::{
    self, CompleteOp, DirEntry, DirEntryMeta, ListOp, PrepareOp, RestoreInfo, UpdateStatsOp,
};
use rados_cls::rgw::olh::{self, LinkOlhOp};
use rados_cls::rgw::types::{CategoryStats, EntryVer, ModifyOp, ObjCategory, ObjKey};

const EOPNOTSUPP: i32 = 95;

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

/// Whether the cluster is Squid, so `test` runs; if not, prints why it is
/// skipped. A cluster whose release is not the one
/// `CEPH_TEST_EXPECT_OSD_RELEASE` names fails instead.
fn on_squid(ioctx: &IoCtx, test: &str) -> bool {
    let release = ioctx.require_osd_release();
    let name = release.map_or("unset", CephRelease::name);
    if let Ok(expected) = std::env::var("CEPH_TEST_EXPECT_OSD_RELEASE") {
        assert_eq!(
            name, expected,
            "the cluster's require_osd_release is not CEPH_TEST_EXPECT_OSD_RELEASE"
        );
    }
    if release == Some(CephRelease::SQUID) {
        return true;
    }
    println!("skipped {test}: the cluster's require_osd_release is {name}, not squid");
    false
}

/// A fresh, initialised index shard on a Squid cluster, or `None` when
/// [`on_squid`] skips the test.
async fn shard(prefix: &str) -> Option<(IoCtx, String)> {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    if !on_squid(&ioctx, prefix) {
        return None;
    }
    let oid = unique(prefix);
    ioctx.create(&oid, true).await.expect("create");
    index::init_index(&ioctx, &oid).await.expect("init_index");
    Some((ioctx, oid))
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

fn restored() -> RestoreInfo {
    RestoreInfo {
        status: 2,
        expiry_date: UTime {
            sec: 1_234_567_890,
            nsec: 0,
        },
    }
}

async fn main_stats(ioctx: &IoCtx, oid: &str) -> CategoryStats {
    index::dir_header(ioctx, oid)
        .await
        .expect("dir_header")
        .stats[&ObjCategory::MAIN]
}

#[tokio::test]
#[ignore]
async fn osd_release_is_squid() {
    let ioctx = create_ioctx().await.expect("create_ioctx");
    on_squid(&ioctx, "osd_release_is_squid");
}

#[tokio::test]
#[ignore]
async fn update_stats_tentacle_shape_is_accepted_as_v1() {
    let Some((ioctx, a)) = shard("cls-rgw-shapes-stats-a").await else {
        return;
    };
    let b = unique("cls-rgw-shapes-stats-b");
    ioctx.create(&b, true).await.expect("create");
    index::init_index(&ioctx, &b).await.expect("init_index");

    let s = CategoryStats {
        total_size: 1024,
        total_size_rounded: 4096,
        num_entries: 1,
        actual_size: 1024,
    };
    let stats = BTreeMap::from([(ObjCategory::MAIN, s)]);
    index::update_stats(&ioctx, &a, CephRelease::SQUID, false, &stats)
        .await
        .expect("squid update_stats");
    index::update_stats(&ioctx, &b, CephRelease::TENTACLE, false, &stats)
        .await
        .expect("tentacle update_stats");
    let header_a = index::dir_header(&ioctx, &a).await.expect("header a");
    let header_b = index::dir_header(&ioctx, &b).await.expect("header b");
    assert_eq!(header_a.stats, header_b.stats);
    assert_eq!(header_b.stats, stats);

    // A Tentacle class would subtract dec_stats and leave S.
    let raw = |absolute| {
        rados::encode_with_capacity(
            &UpdateStatsOp {
                absolute,
                stats: stats.clone(),
                dec_stats: Some(stats.clone()),
            },
            0,
        )
        .expect("encode")
    };
    ioctx
        .exec(&b, "rgw", "bucket_update_stats", raw(false))
        .await
        .expect("v2 with dec_stats");
    let doubled = CategoryStats {
        total_size: 2 * s.total_size,
        total_size_rounded: 2 * s.total_size_rounded,
        num_entries: 2 * s.num_entries,
        actual_size: 2 * s.actual_size,
    };
    assert_eq!(main_stats(&ioctx, &b).await, doubled);

    // A Tentacle class would answer EINVAL.
    ioctx
        .exec(&b, "rgw", "bucket_update_stats", raw(true))
        .await
        .expect("absolute v2 with dec_stats");
    assert_eq!(main_stats(&ioctx, &b).await, s);
}

#[tokio::test]
#[ignore]
async fn read_olh_log_umbrella_shape_is_accepted() {
    let Some((ioctx, oid)) = shard("cls-rgw-shapes-olh-log").await else {
        return;
    };
    // The class reads past the end of an empty log, so give it an entry.
    add_instance(&ioctx, &oid, "o", "v1", "t1", 0, 100).await;
    olh::link_olh(&ioctx, &oid, &link_req("o", "v1", "t1", 0, 100))
        .await
        .expect("link_olh");

    let squid = olh::read_olh_log(
        &ioctx,
        &oid,
        CephRelease::SQUID,
        &key("o"),
        0,
        &olh_tag("o"),
    )
    .await
    .expect("squid read_olh_log");
    let umbrella = olh::read_olh_log(
        &ioctx,
        &oid,
        CephRelease::UMBRELLA,
        &key("o"),
        0,
        &olh_tag("o"),
    )
    .await
    .expect("umbrella read_olh_log");
    assert!(!squid.log.is_empty());
    assert_eq!(squid, umbrella);
}

#[tokio::test]
#[ignore]
async fn meta_v8_in_complete_is_stored_as_v7() {
    let Some((ioctx, oid)) = shard("cls-rgw-shapes-complete").await else {
        return;
    };
    let prepare = PrepareOp {
        op: ModifyOp::ADD,
        key: key("x"),
        tag: "t".to_owned(),
        ..PrepareOp::default()
    };
    index::prepare(&ioctx, &oid, &prepare)
        .await
        .expect("prepare");
    let sent = DirEntryMeta {
        restore: Some(restored()),
        ..meta(1024)
    };
    let complete = CompleteOp {
        op: ModifyOp::ADD,
        key: key("x"),
        ver: EntryVer { pool: 1, epoch: 1 },
        meta: sent.clone(),
        tag: "t".to_owned(),
        ..CompleteOp::default()
    };
    index::complete(&ioctx, &oid, &complete)
        .await
        .expect("complete with meta v8");

    let stats = main_stats(&ioctx, &oid).await;
    assert_eq!((stats.num_entries, stats.total_size), (1, 1024));

    let op = ListOp {
        num_entries: 100,
        ..ListOp::default()
    };
    let listed = index::list(&ioctx, &oid, &op).await.expect("list");
    let entry = listed.dir.entries.values().find(|e| e.key.name == "x");
    let entry = entry.expect("x is listed");
    assert_eq!(entry.meta.restore, None);
    assert_eq!(
        entry.meta,
        DirEntryMeta {
            restore: None,
            ..sent
        }
    );
}

#[tokio::test]
#[ignore]
async fn meta_v8_in_link_olh_is_stored_as_v7() {
    let Some((ioctx, oid)) = shard("cls-rgw-shapes-link").await else {
        return;
    };
    add_instance(&ioctx, &oid, "o", "v1", "t1", 0, 100).await;
    olh::link_olh(&ioctx, &oid, &link_req("o", "v1", "t1", 0, 100))
        .await
        .expect("link_olh v1");

    // On v19 the class writes the delete marker's instance entry from
    // op.meta (init_as_delete_marker, cls_rgw.cc:1333-1341,1743@v19.2.2).
    let marker = LinkOlhOp {
        delete_marker: true,
        meta: DirEntryMeta {
            etag: "dm".to_owned(),
            restore: Some(restored()),
            ..meta(0)
        },
        ..link_req("o", "dm", "t2", 0, 0)
    };
    olh::link_olh(&ioctx, &oid, &marker)
        .await
        .expect("link_olh delete marker with meta v8");

    let v = versions(&ioctx, &oid, "o").await;
    let dm = v
        .iter()
        .find(|e| e.key.instance == "dm")
        .expect("dm is listed");
    assert!(dm.is_current());
    assert!(dm.is_delete_marker());
    assert_eq!(dm.meta.etag, "dm");
    assert_eq!(dm.meta.restore, None);
}

#[tokio::test]
#[ignore]
async fn later_methods_are_eopnotsupp() {
    let Some((ioctx, oid)) = shard("cls-rgw-shapes-methods").await else {
        return;
    };
    // The lookup fails before any decode: OpInfo::set_from_op maps
    // get_method_flags' ENOENT to EOPNOTSUPP
    // (src/osd/osd_op_util.cc:189-196@v19.2.2).
    for method in [
        "bucket_init_index2",
        "bi_put_entries",
        "reshard_log_trim",
        "bucket_refresh_instance",
    ] {
        let err = ioctx
            .exec(&oid, "rgw", method, Bytes::new())
            .await
            .expect_err(method);
        assert!(is_osd_error(&err, EOPNOTSUPP), "{method}: {err:?}");
    }
}
