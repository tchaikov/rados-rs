//! Cluster tests for the version and refcount classes. Run with:
//!   CEPH_CONF=... cargo test -p rados-cls --test cls_version_refcount -- --ignored --nocapture

// The rados crate's cluster helpers; one copy, shared across the workspace.
#[path = "../../rados/tests/common/mod.rs"]
mod common;

use bytes::Bytes;
use common::create_ioctx;
use rados::{OSDClientError, OpBuilder};
use rados_cls::refcount;
use rados_cls::version::{self, ObjVersion, VersionCond};

const ENOENT: i32 = 2;
const EINVAL: i32 = 22;
const ECANCELED: i32 = 125;

fn val(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

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

fn objv(ver: u64, tag: &str) -> ObjVersion {
    ObjVersion {
        ver,
        tag: tag.to_owned(),
    }
}

fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| (*s).to_owned()).collect()
}

#[tokio::test]
#[ignore]
async fn version_set_then_read() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-version-set");
    ioctx.write_full(&oid, val("x")).await.expect("write_full");

    version::set(&ioctx, &oid, &objv(5, "tagA"))
        .await
        .expect("set");
    assert_eq!(
        version::read(&ioctx, &oid).await.expect("read"),
        objv(5, "tagA")
    );

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn version_inc_creates_then_bumps() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-version-inc");
    ioctx.write_full(&oid, val("x")).await.expect("write_full");

    let none = version::read(&ioctx, &oid).await.expect("read");
    assert_eq!(
        none,
        ObjVersion::default(),
        "never versioned reads as {{0, \"\"}}"
    );
    assert!(none.is_empty());

    version::inc(&ioctx, &oid).await.expect("first inc");
    let first = version::read(&ioctx, &oid).await.expect("read");
    assert_eq!(first.ver, 2, "init_version stores 1 and inc bumps it");
    assert_eq!(
        first.tag.len(),
        24,
        "init_version makes a 24-char tag: {first:?}"
    );

    version::inc(&ioctx, &oid).await.expect("second inc");
    let second = version::read(&ioctx, &oid).await.expect("read");
    assert_eq!(second, objv(3, &first.tag), "inc keeps the tag");

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn version_inc_conds_and_check() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-version-conds");
    ioctx.write_full(&oid, val("x")).await.expect("write_full");
    version::set(&ioctx, &oid, &objv(5, "t"))
        .await
        .expect("set");

    version::inc_conds(&ioctx, &oid, &objv(5, "t"), VersionCond::Eq)
        .await
        .expect("matching version increments");
    assert_eq!(
        version::read(&ioctx, &oid).await.expect("read"),
        objv(6, "t")
    );

    let err = version::inc_conds(&ioctx, &oid, &objv(5, "t"), VersionCond::Eq)
        .await
        .expect_err("stale version");
    assert!(is_osd_error(&err, ECANCELED), "{err:?}");
    assert_eq!(
        version::read(&ioctx, &oid).await.expect("read"),
        objv(6, "t")
    );

    version::check(&ioctx, &oid, &objv(6, "t"), VersionCond::Eq)
        .await
        .expect("check eq");
    version::check(&ioctx, &oid, &objv(4, ""), VersionCond::Gt)
        .await
        .expect("6 > 4, tag ignored");
    let err = version::check(&ioctx, &oid, &objv(6, "other"), VersionCond::TagEq)
        .await
        .expect_err("tag differs");
    assert!(is_osd_error(&err, ECANCELED), "{err:?}");

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn version_check_guards_a_write() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-version-guard");
    ioctx.write_full(&oid, val("v0")).await.expect("write_full");
    version::set(&ioctx, &oid, &objv(1, "t"))
        .await
        .expect("set");

    // RGW's shape: check the version, then write, in one request.
    let guarded = OpBuilder::new()
        .op(version::check_op(&objv(1, "t"), VersionCond::Eq).expect("op"))
        .write_full(val("v1"))
        .build();
    ioctx.execute_op(&oid, guarded).await.expect("guard holds");
    assert_eq!(ioctx.read(&oid, 0, 16).await.expect("read").data, val("v1"));

    let stale = OpBuilder::new()
        .op(version::check_op(&objv(2, "t"), VersionCond::Eq).expect("op"))
        .write_full(val("v2"))
        .build();
    let err = ioctx.execute_op(&oid, stale).await.expect_err("stale");
    assert!(is_osd_error(&err, ECANCELED), "{err:?}");
    assert_eq!(
        ioctx.read(&oid, 0, 16).await.expect("read").data,
        val("v1"),
        "a failed check must abort the whole request"
    );

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn version_read_op_in_a_compound() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-version-compound-read");
    ioctx
        .write_full(&oid, val("data"))
        .await
        .expect("write_full");
    version::set(&ioctx, &oid, &objv(4, "t"))
        .await
        .expect("set");

    // RGW's shape: the class read inside a compound read, decoded from its
    // own slot of the reply.
    let compound = OpBuilder::new()
        .op(version::read_op().expect("op"))
        .stat()
        .build();
    let result = ioctx.execute_op(&oid, compound).await.expect("execute_op");
    assert_eq!(result.ops.len(), 2);
    assert_eq!(
        version::decode_read(&result.ops[0]).expect("decode"),
        objv(4, "t")
    );

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn refcount_get_put_removes_at_zero() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-refcount-getput");
    ioctx.write_full(&oid, val("x")).await.expect("write_full");

    refcount::get(&ioctx, &oid, "a", false)
        .await
        .expect("get a");
    refcount::get(&ioctx, &oid, "b", false)
        .await
        .expect("get b");
    refcount::get(&ioctx, &oid, "a", false)
        .await
        .expect("get a again is a no-op");
    assert_eq!(
        refcount::read(&ioctx, &oid, false).await.expect("read"),
        strings(&["a", "b"])
    );

    refcount::put(&ioctx, &oid, "a", false)
        .await
        .expect("put a");
    assert_eq!(
        refcount::read(&ioctx, &oid, false).await.expect("read"),
        strings(&["b"])
    );
    refcount::put(&ioctx, &oid, "a", false)
        .await
        .expect("a retired tag is a silent success");
    refcount::put(&ioctx, &oid, "nope", false)
        .await
        .expect("an unknown tag is a silent success");
    assert_eq!(
        refcount::read(&ioctx, &oid, false).await.expect("read"),
        strings(&["b"])
    );

    refcount::put(&ioctx, &oid, "b", false)
        .await
        .expect("put b");
    let err = ioctx
        .stat(&oid)
        .await
        .expect_err("the last put removes the object");
    assert!(is_osd_error(&err, ENOENT), "{err:?}");
}

#[tokio::test]
#[ignore]
async fn refcount_set_overwrites_and_empty_removes() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-refcount-set");
    ioctx.write_full(&oid, val("x")).await.expect("write_full");

    refcount::get(&ioctx, &oid, "old", false)
        .await
        .expect("get");
    refcount::set(&ioctx, &oid, &strings(&["x", "y"]))
        .await
        .expect("set");
    assert_eq!(
        refcount::read(&ioctx, &oid, false).await.expect("read"),
        strings(&["x", "y"])
    );

    refcount::set(&ioctx, &oid, &[]).await.expect("set none");
    let err = ioctx
        .stat(&oid)
        .await
        .expect_err("set none removes the object");
    assert!(is_osd_error(&err, ENOENT), "{err:?}");
}

#[tokio::test]
#[ignore]
async fn refcount_implicit_ref() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-refcount-implicit");
    ioctx.write_full(&oid, val("x")).await.expect("write_full");

    assert!(
        refcount::read(&ioctx, &oid, false)
            .await
            .expect("read")
            .is_empty()
    );
    assert_eq!(
        refcount::read(&ioctx, &oid, true)
            .await
            .expect("read implicit"),
        vec![refcount::WILDCARD_TAG.to_owned()]
    );

    refcount::put(&ioctx, &oid, "z", true)
        .await
        .expect("put with implicit_ref retires the wildcard");
    let err = ioctx.stat(&oid).await.expect_err("and removes the object");
    assert!(is_osd_error(&err, ENOENT), "{err:?}");
}

#[tokio::test]
#[ignore]
async fn refcount_put_without_refs_is_einval() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-refcount-einval");
    ioctx.write_full(&oid, val("x")).await.expect("write_full");

    let err = refcount::put(&ioctx, &oid, "a", false)
        .await
        .expect_err("no references at all");
    assert!(is_osd_error(&err, EINVAL), "{err:?}");

    ioctx.remove(&oid).await.expect("remove");
}
