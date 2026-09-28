//! Cephx key-type cluster tests: I/O with the keyring's key, then the ticket
//! types the monitor issued; and an aes and an aes256k client side by side.
//! Run with:
//!   CEPH_CONF=... cargo test -p rados --test cephx_aes256k -- --ignored --nocapture
//!
//! `CEPH_TEST_EXPECT_KEY_TYPE` pins the keyring key's type (1 = aes,
//! 2 = aes256k), and `CEPH_TEST_EXPECT_OSD_TICKET_TYPE` the OSD ticket's; the
//! OSD ticket defaults to the key's type when only the first is set.
//!
//! These tests change no cluster policy, so they may run alongside other
//! suites. See `common` for running them against a rooket cluster.

mod cephx;
mod common;

use std::panic::AssertUnwindSafe;
use std::time::Duration;

use bytes::Bytes;
use common::{build_test_client, test_pool_name};
use futures::FutureExt;
use rados::{CephConfig, Client, EntityType, IoCtx, Keyring, WatchEvent};

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
        .keyring_for(ENTITY)
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

/// `watcher_io` watches `oid`, `notifier_io` notifies it, and the notify
/// gets the watcher's one ack.
async fn watch_and_notify(watcher_io: &IoCtx, notifier_io: &IoCtx, oid: &str) {
    let mut watcher = watcher_io.watch(oid).await.expect("watch");
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
    let (result, ()) = tokio::join!(notifier_io.notify(oid, Bytes::from_static(b"ping"), 0), ack);
    let result = result.expect("notify");
    assert_eq!(result.acks.len(), 1, "{result:?}");
    assert_eq!(result.acks[0].reply, Bytes::from_static(b"ack"));
    watcher.unwatch().await.expect("unwatch");
}

async fn side_by_side(
    admin: &Client,
    service_cipher: u16,
    entities: &mut Vec<cephx::Entity>,
    objects: &[String; 2],
) {
    entities.push(cephx::create_entity(admin, "aes", "aes").await);
    entities.push(cephx::create_entity(admin, "aes256k", "aes256k").await);
    let aes = cephx::client_for(&entities[0]).await.expect("aes client");
    let aes256k = cephx::client_for(&entities[1])
        .await
        .expect("aes256k client");

    assert_eq!(
        cephx::ticket_types(&aes).await,
        (1, 1.min(service_cipher)),
        "the aes client's AUTH and OSD ticket types"
    );
    assert_eq!(
        cephx::ticket_types(&aes256k).await,
        (2, 2.min(service_cipher)),
        "the aes256k client's AUTH and OSD ticket types"
    );

    let pool = test_pool_name();
    let aes_io = aes.open_pool(&pool).await.expect("aes pool");
    let aes256k_io = aes256k.open_pool(&pool).await.expect("aes256k pool");
    let [aes_obj, aes256k_obj] = objects;
    aes_io
        .write_full(aes_obj, Bytes::from_static(b"aes"))
        .await
        .expect("aes write");
    aes256k_io
        .write_full(aes256k_obj, Bytes::from_static(b"aes256k"))
        .await
        .expect("aes256k write");
    let read = aes_io.read(aes256k_obj, 0, 64).await.expect("aes read");
    assert_eq!(read.data, Bytes::from_static(b"aes256k"));
    let read = aes256k_io.read(aes_obj, 0, 64).await.expect("aes256k read");
    assert_eq!(read.data, Bytes::from_static(b"aes"));

    watch_and_notify(&aes256k_io, &aes_io, aes256k_obj).await;
    watch_and_notify(&aes_io, &aes256k_io, aes_obj).await;
}

/// Rook v1.20.7 allows both key types, keeps CSI's key AES and makes new
/// keys aes256k: one entity of each type, both clients up at once.
#[tokio::test]
#[ignore]
async fn two_key_types_side_by_side() {
    common::init_tracing();

    let admin = build_test_client().await.expect("admin client");
    let Some(policy) = cephx::mon_auth(&admin).await else {
        println!("skipped: the monmap has no cephx policy (Ceph before v19.2.6)");
        return;
    };
    if !(policy.allowed.contains(&1) && policy.allowed.contains(&2)) {
        println!(
            "skipped: auth_allowed_ciphers {:?} lacks aes or aes256k",
            policy.allowed
        );
        return;
    }

    let objects = [
        cephx::unique("side-by-side-aes"),
        cephx::unique("side-by-side-aes256k"),
    ];
    let mut entities = Vec::new();
    let result = AssertUnwindSafe(side_by_side(
        &admin,
        policy.service_cipher,
        &mut entities,
        &objects,
    ))
    .catch_unwind()
    .await;

    match admin.open_pool(&test_pool_name()).await {
        Ok(ioctx) => {
            for oid in &objects {
                if let Err(e) = cephx::remove_object(&ioctx, oid).await {
                    eprintln!("cleanup: {e}");
                }
            }
        }
        Err(e) => eprintln!("cleanup: opening the pool: {e}"),
    }
    for entity in &entities {
        if let Err(e) = cephx::remove_entity(&admin, &entity.name).await {
            eprintln!("cleanup: {e}");
        }
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
