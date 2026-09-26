//! Integration tests for `IoCtx::exec` (CLS object class method invocation)
//! and the lock helpers built on top of it.
//!
//! Requires a running Ceph cluster. Configuration is read from `ceph.conf`
//! via [`rados::Client::builder`]; see the shared `common` module for details.
//!
//! Environment:
//! - `CEPH_CONF` (required) — path to `ceph.conf`
//! - `CEPH_KEYRING` (optional) — overrides the keyring path from `ceph.conf`
//! - `CEPH_TEST_POOL` (optional, default: `test-pool`) — pool to operate on
//!
//! Run with:
//! ```bash
//! CEPH_CONF=/path/to/ceph.conf \
//!   cargo test --package rados --test osdclient_exec_operations -- --ignored --nocapture
//! ```

use bytes::Bytes;
use rados::OSDClientError;
use tracing::info;

mod common;
use common::create_ioctx;

#[tokio::test]
#[ignore]
async fn exec_hello_say_hello() {
    common::init_tracing();
    info!("testing exec() against the built-in `hello` object class");

    let ioctx = create_ioctx().await.expect("create_ioctx");

    let object_name = format!("test-exec-hello-{}", rand::random::<u32>());
    let test_data = Bytes::from("exec test placeholder object");

    ioctx
        .write_full(&object_name, test_data.clone())
        .await
        .expect("write_full");

    let outdata = ioctx
        .exec(&object_name, "hello", "say_hello", Bytes::new())
        .await
        .expect("exec hello::say_hello");
    info!("exec ok, outdata={outdata:?}");

    // cls_hello's say_hello() replies "Hello, " + (input or "world") + "!".
    assert_eq!(outdata, Bytes::from_static(b"Hello, world!"));

    ioctx.remove(&object_name).await.expect("remove");
    info!("cleanup ok");
}

#[tokio::test]
#[ignore]
async fn exec_unknown_class_is_eperm() {
    common::init_tracing();
    info!("testing exec() against a class that does not exist");

    let ioctx = create_ioctx().await.expect("create_ioctx");

    let object_name = format!("test-exec-unknown-class-{}", rand::random::<u32>());
    let test_data = Bytes::from("exec test placeholder object");

    ioctx
        .write_full(&object_name, test_data.clone())
        .await
        .expect("write_full");

    let result = ioctx
        .exec(&object_name, "nosuchclass", "m", Bytes::new())
        .await;

    // ClassHandler::open_class() (ceph/src/osd/ClassHandler.cc) returns
    // -EPERM, not -EOPNOTSUPP, for a class name that isn't on
    // `osd_class_load_list` — confirmed against this cluster's default
    // config, which doesn't list "nosuchclass".
    match result {
        Err(OSDClientError::OSDError { code, .. }) => {
            assert_eq!(code, -1, "expected EPERM (-1), got {code}");
        }
        other => panic!("expected OSDError {{ code: -1, .. }}, got {other:?}"),
    }

    ioctx.remove(&object_name).await.expect("remove");
    info!("cleanup ok");
}

#[tokio::test]
#[ignore]
async fn lock_exclusive_roundtrip() {
    common::init_tracing();
    info!("testing lock_exclusive / unlock roundtrip");

    let ioctx = create_ioctx().await.expect("create_ioctx");

    let object_name = format!("test-exec-lock-{}", rand::random::<u32>());
    let test_data = Bytes::from("exec test placeholder object");

    ioctx
        .write_full(&object_name, test_data.clone())
        .await
        .expect("write_full");

    let lock_name = "test-lock";
    let first_cookie = format!("cookie-{}", rand::random::<u32>());
    let second_cookie = format!("cookie-{}", rand::random::<u32>());

    ioctx
        .lock_exclusive(&object_name, lock_name, &first_cookie, "first holder", None)
        .await
        .expect("first lock_exclusive");
    info!("first lock_exclusive ok, cookie={first_cookie}");

    let second_lock = ioctx
        .lock_exclusive(
            &object_name,
            lock_name,
            &second_cookie,
            "second holder",
            None,
        )
        .await;

    match second_lock {
        Err(OSDClientError::OSDError { code, .. }) => {
            assert_eq!(code, -16, "expected EBUSY (-16), got {code}");
        }
        other => panic!("expected OSDError {{ code: -16, .. }}, got {other:?}"),
    }
    info!("second lock_exclusive correctly rejected with EBUSY");

    ioctx
        .unlock(&object_name, lock_name, &first_cookie)
        .await
        .expect("unlock with first cookie");
    info!("unlock ok");

    ioctx.remove(&object_name).await.expect("remove");
    info!("cleanup ok");
}
