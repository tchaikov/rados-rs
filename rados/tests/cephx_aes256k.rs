//! Cephx key-type cluster test: I/O with the keyring's key, then the ticket
//! types the monitor issued. Run with:
//!   CEPH_CONF=... cargo test -p rados --test cephx_aes256k -- --ignored --nocapture
//!
//! `CEPH_TEST_EXPECT_KEY_TYPE` pins the keyring key's type (1 = aes,
//! 2 = aes256k), and `CEPH_TEST_EXPECT_OSD_TICKET_TYPE` the OSD ticket's; the
//! OSD ticket defaults to the key's type when only the first is set.

mod common;

use std::time::Duration;

use bytes::Bytes;
use common::{build_test_client, test_pool_name};
use rados::{CephConfig, EntityType, Keyring, WatchEvent};

const ENTITY: &str = "client.admin";

fn expected_type(var: &str) -> Option<u16> {
    let value = std::env::var(var).ok()?;
    Some(
        value
            .parse()
            .unwrap_or_else(|_| panic!("{var}={value:?} is not a key type number")),
    )
}

fn keyring_path() -> String {
    if let Ok(path) = std::env::var("CEPH_KEYRING") {
        return path;
    }
    let conf = std::env::var("CEPH_CONF").unwrap_or_else(|_| "/etc/ceph/ceph.conf".to_owned());
    CephConfig::from_file(&conf)
        .expect("ceph.conf")
        .keyring()
        .expect("keyring in ceph.conf")
}

#[tokio::test]
#[ignore]
async fn keyring_key_type_end_to_end() {
    common::init_tracing();

    let keyring = Keyring::from_file(keyring_path()).expect("keyring");
    let key = keyring.get_key(ENTITY).expect("keyring key");
    let key_type = key.key_type().expect("known key type");
    println!("{ENTITY} key type {key_type:?} ({})", key.crypto_type);
    if let Some(expected) = expected_type("CEPH_TEST_EXPECT_KEY_TYPE") {
        assert_eq!(key.crypto_type, expected, "keyring key type");
    }

    let client = build_test_client().await.expect("client");
    let ioctx = client.open_pool(&test_pool_name()).await.expect("pool");
    let oid = format!("cephx-key-type-{}", rand::random::<u32>());

    ioctx
        .write_full(&oid, Bytes::from_static(b"cephx"))
        .await
        .expect("write");
    let read = ioctx.read(&oid, 0, 16).await.expect("read");
    assert_eq!(read.data, Bytes::from_static(b"cephx"));

    let hello = ioctx
        .exec(&oid, "hello", "say_hello", Bytes::new())
        .await
        .expect("exec hello.say_hello");
    assert_eq!(hello, Bytes::from_static(b"Hello, world!"));

    let notifier = common::create_ioctx().await.expect("second client");
    let mut watcher = ioctx.watch(oid.as_str()).await.expect("watch");
    let ack = async {
        match tokio::time::timeout(Duration::from_secs(15), watcher.recv())
            .await
            .expect("a notify in time")
            .expect("watch channel open")
        {
            WatchEvent::Notify { notify_id, .. } => watcher
                .notify_ack(notify_id, Bytes::from_static(b"ack"))
                .await
                .expect("notify_ack"),
            other => panic!("expected a notify, got {other:?}"),
        }
    };
    let (result, ()) = tokio::join!(
        notifier.notify(oid.as_str(), Bytes::from_static(b"ping"), 0),
        ack
    );
    let result = result.expect("notify");
    assert_eq!(result.acks.len(), 1, "{result:?}");
    assert_eq!(result.acks[0].reply, Bytes::from_static(b"ack"));
    watcher.unwatch().await.expect("unwatch");

    let provider = client
        .mon_client()
        .get_service_auth_provider()
        .await
        .expect("service auth provider");
    let (auth_type, osd_type) = {
        let handler = provider.handler().lock().expect("handler lock");
        let tickets = &handler.get_session().expect("session").ticket_handlers;
        (
            tickets[&EntityType::AUTH].session_key.crypto_type,
            tickets[&EntityType::OSD].session_key.crypto_type,
        )
    };
    println!("AUTH ticket type {auth_type}, OSD ticket type {osd_type}");
    assert_eq!(auth_type, key.crypto_type, "AUTH ticket has the key's type");
    match expected_type("CEPH_TEST_EXPECT_OSD_TICKET_TYPE")
        .or_else(|| expected_type("CEPH_TEST_EXPECT_KEY_TYPE"))
    {
        Some(expected) => assert_eq!(osd_type, expected, "OSD ticket type"),
        None => assert!(
            osd_type <= key.crypto_type,
            "OSD ticket type {osd_type} above the key's {}",
            key.crypto_type
        ),
    }

    ioctx.remove(&oid).await.expect("remove");
}
