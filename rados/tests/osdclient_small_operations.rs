//! Cluster tests for cmpxattr, assert_exists, zero, set_alloc_hint and
//! list_watchers. Run with:
//!   CEPH_CONF=... cargo test -p rados --test osdclient_small_operations -- --ignored --nocapture

mod common;

use bytes::Bytes;
use common::create_ioctx;
use rados::osdclient::types::OSDOp;
use rados::{AllocHintFlags, CmpOp, OSDClientError, OpBuilder};

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

#[tokio::test]
#[ignore]
async fn cmpxattr_guard_protects_a_write() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("small-cmpxattr-guard");
    ioctx
        .set_xattr(&oid, "user.tag", val("t1"))
        .await
        .expect("set tag");

    // RGW's shape: assert the id tag, then write under it.
    let guarded = OpBuilder::new()
        .cmpxattr("user.tag", CmpOp::Eq, val("t1"))
        .expect("cmp")
        .op(OSDOp::set_xattr("user.data", val("v1")).expect("set"))
        .build();
    ioctx
        .execute_op(&oid, guarded)
        .await
        .expect("the tag matches, the write applies");
    assert_eq!(
        ioctx.get_xattr(&oid, "user.data").await.expect("get"),
        val("v1")
    );

    let stale = OpBuilder::new()
        .cmpxattr("user.tag", CmpOp::Eq, val("t0"))
        .expect("cmp")
        .op(OSDOp::set_xattr("user.data", val("v2")).expect("set"))
        .build();
    let err = ioctx
        .execute_op(&oid, stale)
        .await
        .expect_err("a stale tag must fail");
    assert!(is_osd_error(&err, ECANCELED), "{err:?}");
    assert_eq!(
        ioctx.get_xattr(&oid, "user.data").await.expect("get"),
        val("v1"),
        "a failed guard must abort the whole request"
    );

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn cmpxattr_match_returns_one() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("small-cmpxattr-one");
    ioctx
        .set_xattr(&oid, "user.tag", val("t1"))
        .await
        .expect("set tag");

    let op = OpBuilder::new()
        .cmpxattr("user.tag", CmpOp::Eq, val("t1"))
        .expect("cmp")
        .build();
    let result = ioctx
        .execute_op(&oid, op)
        .await
        .expect("a holding comparison is a success, not an error");
    // PrimaryLogPG returns the comparison's truth value as the op's result.
    assert_eq!(result.ops[0].return_code, 1);
    assert_eq!(
        result.result, 1,
        "a read-only request reports the comparison's truth value"
    );

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn cmpxattr_missing_xattr_compares_as_empty() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("small-cmpxattr-missing");
    ioctx
        .write_full(&oid, val("data"))
        .await
        .expect("write_full");

    let absent = OpBuilder::new()
        .cmpxattr("user.none", CmpOp::Eq, Bytes::new())
        .expect("cmp")
        .build();
    ioctx
        .execute_op(&oid, absent)
        .await
        .expect("a missing attribute equals the empty string");

    let present = OpBuilder::new()
        .cmpxattr("user.none", CmpOp::Eq, val("x"))
        .expect("cmp")
        .build();
    let err = ioctx
        .execute_op(&oid, present)
        .await
        .expect_err("a missing attribute is not \"x\"");
    assert!(
        is_osd_error(&err, ECANCELED),
        "ECANCELED, not ENODATA: {err:?}"
    );

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn cmpxattr_u64_compares_decimal_text() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("small-cmpxattr-u64");
    ioctx
        .set_xattr(&oid, "user.ver", val("5"))
        .await
        .expect("set ver");

    // The OSD evaluates `value <op> stored`: the supplied operand is the
    // left-hand side (do_cmp_xattr in PrimaryLogPG.cc).
    for (op, value) in [
        (CmpOp::Eq, 5),
        (CmpOp::Gte, 5),
        (CmpOp::Gte, 6),
        (CmpOp::Lt, 4),
    ] {
        let built = OpBuilder::new()
            .cmpxattr_u64("user.ver", op, value)
            .expect("cmp")
            .build();
        ioctx
            .execute_op(&oid, built)
            .await
            .unwrap_or_else(|e| panic!("{value} {op:?} 5 must hold: {e:?}"));
    }

    let built = OpBuilder::new()
        .cmpxattr_u64("user.ver", CmpOp::Gte, 4)
        .expect("cmp")
        .build();
    let err = ioctx
        .execute_op(&oid, built)
        .await
        .expect_err("4 >= 5 must fail");
    assert!(is_osd_error(&err, ECANCELED), "{err:?}");

    // The OSD parses the stored value as decimal text; anything else is EINVAL.
    ioctx
        .set_xattr(&oid, "user.ver", val("five"))
        .await
        .expect("set text");
    let built = OpBuilder::new()
        .cmpxattr_u64("user.ver", CmpOp::Eq, 5)
        .expect("cmp")
        .build();
    let err = ioctx
        .execute_op(&oid, built)
        .await
        .expect_err("non-numeric text");
    assert!(is_osd_error(&err, EINVAL), "{err:?}");

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn assert_exists_guards_a_write() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("small-assert-exists");
    let write = || {
        OpBuilder::new()
            .assert_exists()
            .write_full(val("x"))
            .build()
    };

    let err = ioctx
        .execute_op(&oid, write())
        .await
        .expect_err("missing object");
    assert!(is_osd_error(&err, ENOENT), "{err:?}");
    let err = ioctx
        .stat(&oid)
        .await
        .expect_err("the failed guard must not create the object");
    assert!(is_osd_error(&err, ENOENT), "{err:?}");

    ioctx.write_full(&oid, val("seed")).await.expect("create");
    ioctx
        .execute_op(&oid, write())
        .await
        .expect("existing object");
    let read = ioctx.read(&oid, 0, 16).await.expect("read");
    assert_eq!(read.data, val("x"));

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn zero_clears_a_range_and_ignores_missing_objects() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("small-zero");
    ioctx
        .write_full(&oid, val("abcdefgh"))
        .await
        .expect("write_full");

    ioctx.zero(&oid, 2, 3).await.expect("zero");
    let read = ioctx.read(&oid, 0, 8).await.expect("read");
    assert_eq!(&read.data[..], b"ab\0\0\0fgh");

    let missing = unique("small-zero-missing");
    ioctx
        .zero(&missing, 0, 4)
        .await
        .expect("zeroing a missing object is a no-op");
    let err = ioctx
        .stat(&missing)
        .await
        .expect_err("and does not create it");
    assert!(is_osd_error(&err, ENOENT), "{err:?}");

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn set_alloc_hint_creates_the_object() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("small-alloc-hint");

    ioctx
        .set_alloc_hint(&oid, 4 << 20, 4 << 20, AllocHintFlags::INCOMPRESSIBLE)
        .await
        .expect("hint");
    let st = ioctx.stat(&oid).await.expect("the hint created the object");
    assert_eq!(st.size, 0);

    // RGW's RadosWriter sends the hint and the data in one request.
    let oid2 = unique("small-alloc-hint-write");
    let op = OpBuilder::new()
        .set_alloc_hint(0, 0, AllocHintFlags::INCOMPRESSIBLE)
        .write_full(val("body"))
        .build();
    ioctx.execute_op(&oid2, op).await.expect("hint then write");
    let read = ioctx.read(&oid2, 0, 16).await.expect("read");
    assert_eq!(read.data, val("body"));

    ioctx.remove(&oid).await.expect("remove");
    ioctx.remove(&oid2).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn list_watchers_is_empty_without_watchers() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("small-watchers");
    ioctx
        .write_full(&oid, val("data"))
        .await
        .expect("write_full");

    let watchers = ioctx.list_watchers(&oid).await.expect("list_watchers");
    assert!(watchers.is_empty(), "{watchers:?}");

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn list_watchers_on_missing_object_is_enoent() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("small-watchers-missing");

    let err = ioctx
        .list_watchers(&oid)
        .await
        .expect_err("missing object must fail");
    assert!(is_osd_error(&err, ENOENT), "{err:?}");
}
