//! omap cluster tests. Run with:
//!   CEPH_CONF=... cargo test -p rados --test osdclient_omap_operations -- --ignored --nocapture

mod common;

use std::collections::BTreeMap;

use bytes::Bytes;
use common::create_ioctx;
use rados::osdclient::types::OSDOp;
use rados::{CmpOp, OSDClientError, OmapAssertion, OmapKeySet, OmapMap, OpBuilder};

const ECANCELED: i32 = 125;
const ENOENT: i32 = 2;

fn key(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("{prefix}-{}-{nanos}", std::process::id())
}

fn sample() -> OmapMap {
    let mut m = OmapMap::new();
    for (k, v) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")] {
        m.insert(key(k), key(v));
    }
    m
}

#[tokio::test]
#[ignore]
async fn set_get_rm_roundtrip() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("omap-roundtrip");

    ioctx.omap_set(&oid, &sample()).await.expect("omap_set");
    let vals = ioctx
        .omap_get_vals(&oid, b"", 100, b"")
        .await
        .expect("get_vals");
    assert_eq!(vals.vals, sample());
    assert!(!vals.more);

    let keys = ioctx.omap_get_keys(&oid, b"", 100).await.expect("get_keys");
    assert_eq!(keys.keys, sample().into_keys().collect::<OmapKeySet>());
    assert!(!keys.more);

    let mut want = OmapKeySet::new();
    want.insert(key("b"));
    want.insert(key("zzz"));
    let by = ioctx
        .omap_get_vals_by_keys(&oid, &want)
        .await
        .expect("by_keys");
    assert_eq!(by.len(), 1);
    assert_eq!(by.get(&key("b")), Some(&key("2")));

    let mut rm = OmapKeySet::new();
    rm.insert(key("a"));
    rm.insert(key("missing"));
    ioctx
        .omap_rm_keys(&oid, &rm)
        .await
        .expect("rm_keys tolerates missing keys");
    let vals = ioctx
        .omap_get_vals(&oid, b"", 100, b"")
        .await
        .expect("get_vals");
    assert!(!vals.vals.contains_key(&key("a")));
    assert_eq!(vals.vals.len(), 3);

    ioctx.omap_clear(&oid).await.expect("clear");
    let vals = ioctx
        .omap_get_vals(&oid, b"", 100, b"")
        .await
        .expect("get_vals");
    assert!(vals.vals.is_empty());
    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn get_vals_pages_with_more() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("omap-paging");
    ioctx.omap_set(&oid, &sample()).await.expect("omap_set");

    let page = ioctx
        .omap_get_vals(&oid, b"", 2, b"")
        .await
        .expect("page 1");
    assert_eq!(page.vals.len(), 2);
    assert!(page.more);
    let last = page.vals.keys().next_back().expect("last key").clone();

    let rest = ioctx
        .omap_get_vals(&oid, &last, 100, b"")
        .await
        .expect("page 2");
    assert_eq!(rest.vals.len(), 2);
    assert!(!rest.more);
    assert!(rest.vals.keys().all(|k| k > &last));

    let filtered = ioctx
        .omap_get_vals(&oid, b"", 100, b"c")
        .await
        .expect("prefix");
    assert_eq!(
        filtered.vals.keys().cloned().collect::<Vec<_>>(),
        vec![key("c")]
    );
    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn keys_are_bytes_not_strings() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("omap-bytes");
    let raw = Bytes::from_static(&[0x80, b'0', b'_', b'v']);
    let mut m = OmapMap::new();
    m.insert(raw.clone(), key("versioned"));
    ioctx.omap_set(&oid, &m).await.expect("omap_set");
    let vals = ioctx
        .omap_get_vals(&oid, b"", 100, b"")
        .await
        .expect("get_vals");
    assert_eq!(vals.vals.get(&raw), Some(&key("versioned")));
    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn header_roundtrip() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("omap-header");
    ioctx
        .omap_set_header(&oid, Bytes::from_static(b"hdr"))
        .await
        .expect("set_header");
    assert_eq!(
        ioctx.omap_get_header(&oid).await.expect("get_header"),
        Bytes::from_static(b"hdr")
    );
    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn rm_range_end_is_exclusive() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("omap-range");
    ioctx.omap_set(&oid, &sample()).await.expect("omap_set");
    ioctx
        .omap_rm_range(&oid, b"b", b"d")
        .await
        .expect("rm_range");
    let vals = ioctx
        .omap_get_vals(&oid, b"", 100, b"")
        .await
        .expect("get_vals");
    assert_eq!(
        vals.vals.keys().cloned().collect::<Vec<_>>(),
        vec![key("a"), key("d")]
    );
    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn cmp_failure_aborts_transaction() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("omap-cmp");
    ioctx.omap_set(&oid, &sample()).await.expect("omap_set");

    let mut assertions = BTreeMap::new();
    assertions.insert(
        key("a"),
        OmapAssertion {
            value: key("1"),
            op: CmpOp::Eq,
        },
    );
    let mut update = OmapMap::new();
    update.insert(key("e"), key("5"));
    let ok = OpBuilder::new()
        .omap_cmp(&assertions)
        .expect("cmp")
        .omap_set(&update)
        .expect("set")
        .build();
    ioctx
        .execute_op(&oid, ok)
        .await
        .expect("assertion holds, set applied");

    let mut bad = BTreeMap::new();
    bad.insert(
        key("a"),
        OmapAssertion {
            value: key("wrong"),
            op: CmpOp::Eq,
        },
    );
    let mut update = OmapMap::new();
    update.insert(key("f"), key("6"));
    let failing = OpBuilder::new()
        .omap_cmp(&bad)
        .expect("cmp")
        .omap_set(&update)
        .expect("set")
        .build();
    let err = ioctx
        .execute_op(&oid, failing)
        .await
        .expect_err("assertion fails");
    assert!(
        matches!(err, OSDClientError::OSDError { code, .. } if code == -ECANCELED),
        "{err:?}"
    );

    let vals = ioctx
        .omap_get_vals(&oid, b"", 100, b"")
        .await
        .expect("get_vals");
    assert!(vals.vals.contains_key(&key("e")));
    assert!(
        !vals.vals.contains_key(&key("f")),
        "a failed assertion must abort the whole transaction"
    );
    ioctx.remove(&oid).await.expect("remove");
}

#[tokio::test]
#[ignore]
async fn read_on_missing_object_is_enoent() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("omap-missing");
    let err = ioctx
        .omap_get_vals(&oid, b"", 100, b"")
        .await
        .expect_err("missing object");
    assert!(
        matches!(err, OSDClientError::OSDError { code, .. } if code == -ENOENT),
        "{err:?}"
    );
}

#[tokio::test]
#[ignore]
async fn compound_with_data_and_xattr() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("omap-compound");
    let mut m = OmapMap::new();
    m.insert(key("k"), key("v"));
    let xattr = OSDOp::set_xattr("user.x", Bytes::from_static(b"1")).expect("xattr");
    let op = OpBuilder::new()
        .write_full(Bytes::from_static(b"payload"))
        .op(xattr)
        .omap_set(&m)
        .expect("set")
        .build();
    ioctx.execute_op(&oid, op).await.expect("compound write");
    assert_eq!(
        ioctx.get_xattr(&oid, "user.x").await.expect("xattr"),
        Bytes::from_static(b"1")
    );
    assert_eq!(
        ioctx
            .omap_get_vals(&oid, b"", 100, b"")
            .await
            .expect("vals")
            .vals,
        m
    );
    ioctx.remove(&oid).await.expect("remove");
}
