//! xattr cluster tests. Run with:
//!   CEPH_CONF=... cargo test -p rados --test osdclient_xattr_operations -- --ignored --nocapture

mod common;

use std::collections::BTreeMap;

use bytes::Bytes;
use common::create_ioctx;
use rados::osdclient::OSDClientError;

const ENOENT: i32 = 2;
const ENODATA: i32 = 61;

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

#[tokio::test]
#[ignore]
async fn set_get_remove_roundtrip() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("xattr-roundtrip");

    ioctx
        .set_xattr(&oid, "user.a", val("1"))
        .await
        .expect("set user.a");
    ioctx
        .set_xattr(&oid, "user.b", val("22"))
        .await
        .expect("set user.b");

    let a = ioctx.get_xattr(&oid, "user.a").await.expect("get user.a");
    assert_eq!(a, val("1"));
    let b = ioctx.get_xattr(&oid, "user.b").await.expect("get user.b");
    assert_eq!(b, val("22"));

    let all = ioctx.get_xattrs(&oid).await.expect("get_xattrs");
    let want = BTreeMap::from([
        ("user.a".to_owned(), val("1")),
        ("user.b".to_owned(), val("22")),
    ]);
    assert_eq!(all, want);

    let names = ioctx.list_xattrs(&oid).await.expect("list_xattrs");
    assert_eq!(names, vec!["user.a".to_owned(), "user.b".to_owned()]);

    ioctx
        .remove_xattr(&oid, "user.a")
        .await
        .expect("remove user.a");
    let all = ioctx.get_xattrs(&oid).await.expect("get_xattrs");
    assert_eq!(all, BTreeMap::from([("user.b".to_owned(), val("22"))]));

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn xattr_name_survives_on_the_osd() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("xattr-name");

    ioctx
        .set_xattr(&oid, "user.probe", val("v"))
        .await
        .expect("set user.probe");
    let all = ioctx.get_xattrs(&oid).await.expect("get_xattrs");
    assert_eq!(all, BTreeMap::from([("user.probe".to_owned(), val("v"))]));

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn get_xattr_missing_is_enodata() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("xattr-missing");

    ioctx
        .write_full(&oid, val("data"))
        .await
        .expect("write_full");
    let err = ioctx
        .get_xattr(&oid, "user.none")
        .await
        .expect_err("missing xattr must fail");
    assert!(
        matches!(err, OSDClientError::OSDError { code, .. } if code == -ENODATA),
        "{err:?}"
    );

    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn get_xattrs_on_missing_object_is_enoent() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("xattr-no-object");

    let err = ioctx
        .get_xattrs(&oid)
        .await
        .expect_err("missing object must fail");
    assert!(
        matches!(err, OSDClientError::OSDError { code, .. } if code == -ENOENT),
        "{err:?}"
    );
}
