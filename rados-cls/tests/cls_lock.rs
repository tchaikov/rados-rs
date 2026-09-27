//! Cluster tests for the lock class. Run with:
//!   CEPH_CONF=... cargo test -p rados-cls --test cls_lock -- --ignored --nocapture

#[path = "../../rados/tests/common/mod.rs"]
mod common;

use std::time::Duration;

use rados::osdclient::error::Result;
use rados::{Denc, EntityAddrType, IoCtx, OSDClientError, OpBuilder, UTime};
use rados_cls::lock::{
    self, GetInfoReply, LockFlags, LockInfo, LockOp, LockType, PackedEntityName,
};

const ENOENT: i32 = 2;
const EIO: i32 = 5;
const EBUSY: i32 = 16;
const EEXIST: i32 = 17;
const EINVAL: i32 = 22;

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

#[track_caller]
fn assert_errno<T: std::fmt::Debug>(r: Result<T>, errno: i32, what: &str) {
    match r {
        Err(e) if is_osd_error(&e, errno) => {}
        other => panic!("{what}: expected errno {errno}, got {other:?}"),
    }
}

fn secs(n: u32) -> UTime {
    UTime::new(n, 0)
}

fn lock_req(
    name: &str,
    lock_type: LockType,
    cookie: &str,
    tag: &str,
    duration: UTime,
    flags: LockFlags,
) -> LockOp {
    LockOp {
        name: name.to_owned(),
        lock_type,
        cookie: cookie.to_owned(),
        tag: tag.to_owned(),
        description: "test".to_owned(),
        duration,
        flags,
    }
}

fn exclusive(name: &str, cookie: &str) -> LockOp {
    lock_req(
        name,
        LockType::Exclusive,
        cookie,
        "",
        secs(30),
        LockFlags::empty(),
    )
}

/// A new client, so a new global id, with the test pool open and the
/// entity the lock class records for it.
async fn client() -> (rados::Client, IoCtx, PackedEntityName) {
    let client = common::build_test_client().await.expect("client");
    let ioctx = client
        .open_pool(&common::test_pool_name())
        .await
        .expect("open_pool");
    let gid = client.mon_client().get_global_id().await;
    (client, ioctx, PackedEntityName::new(0x08, gid))
}

fn holders(info: &GetInfoReply) -> Vec<(String, String)> {
    info.lockers
        .keys()
        .map(|id| (id.locker.to_string(), id.cookie.clone()))
        .collect()
}

async fn cleanup(ioctx: &IoCtx, oid: &str) {
    let _ = ioctx.remove(oid).await;
}

#[tokio::test]
#[ignore]
async fn lock_exclusive_get_info_list_locks() {
    common::init_tracing();
    let (_c, ioctx, me) = client().await;
    let oid = unique("cls-lock-basic");
    let name = "basic";
    ioctx.create(&oid, true).await.expect("create");

    lock::lock(&ioctx, &oid, &exclusive(name, "c1"))
        .await
        .expect("lock");
    let info = lock::get_info(&ioctx, &oid, name).await.expect("get_info");
    assert_eq!(info.lock_type, LockType::Exclusive);
    assert_eq!(info.tag, "");
    assert_eq!(holders(&info), [(me.to_string(), "c1".to_owned())]);
    let held = info.lockers.values().next().expect("one holder");
    assert_eq!(held.description, "test");
    assert_eq!(held.addr.addr_type, EntityAddrType::Legacy);
    assert!(held.expiration.sec > 315_360_000, "{:?}", held.expiration);
    assert_eq!(
        lock::list_locks(&ioctx, &oid).await.expect("list_locks"),
        [name]
    );

    let created = unique("cls-lock-created");
    lock::lock(&ioctx, &created, &exclusive(name, "c1"))
        .await
        .expect("lock on a missing object");
    assert_eq!(ioctx.stat(&created).await.expect("stat").size, 0);

    let guarded = unique("cls-lock-guarded");
    let op = OpBuilder::new()
        .assert_exists()
        .op(lock::lock_op(&exclusive(name, "c1")).expect("op"))
        .build();
    assert_errno(
        ioctx.execute_op(&guarded, op).await,
        ENOENT,
        "assert_exists + lock",
    );
    assert_errno(ioctx.stat(&guarded).await, ENOENT, "stat after guard");

    cleanup(&ioctx, &oid).await;
    cleanup(&ioctx, &created).await;
}

#[tokio::test]
#[ignore]
async fn second_cookie_is_busy() {
    common::init_tracing();
    let (_c, ioctx, _) = client().await;
    let oid = unique("cls-lock-busy");
    let name = "busy";
    ioctx.create(&oid, true).await.expect("create");

    lock::lock(&ioctx, &oid, &exclusive(name, "c1"))
        .await
        .expect("lock");
    assert_errno(
        lock::lock(&ioctx, &oid, &exclusive(name, "c2")).await,
        EBUSY,
        "exclusive c2",
    );
    let shared = lock_req(
        name,
        LockType::Shared,
        "c2",
        "",
        secs(30),
        LockFlags::empty(),
    );
    assert_errno(lock::lock(&ioctx, &oid, &shared).await, EBUSY, "shared c2");
    assert_errno(
        lock::lock(&ioctx, &oid, &exclusive(name, "c1")).await,
        EEXIST,
        "exclusive c1 again",
    );

    cleanup(&ioctx, &oid).await;
}

#[tokio::test]
#[ignore]
async fn renew_may_and_must() {
    common::init_tracing();
    let (_c, ioctx, _) = client().await;
    let oid = unique("cls-lock-renew");
    let name = "renew";
    ioctx.create(&oid, true).await.expect("create");

    let expiration = |info: &GetInfoReply| {
        let held = info.lockers.values().next().expect("one holder");
        (held.expiration.sec, held.expiration.nsec)
    };
    let with_flags = |cookie: &str, flags: LockFlags| {
        lock_req(name, LockType::Exclusive, cookie, "", secs(30), flags)
    };

    lock::lock(&ioctx, &oid, &exclusive(name, "c1"))
        .await
        .expect("lock");
    let e1 = expiration(&lock::get_info(&ioctx, &oid, name).await.expect("info"));

    lock::lock(&ioctx, &oid, &with_flags("c1", LockFlags::MAY_RENEW))
        .await
        .expect("may renew");
    let e2 = expiration(&lock::get_info(&ioctx, &oid, name).await.expect("info"));
    assert!(e2 > e1, "{e2:?} > {e1:?}");

    lock::lock(&ioctx, &oid, &with_flags("c1", LockFlags::MUST_RENEW))
        .await
        .expect("must renew");
    let e3 = expiration(&lock::get_info(&ioctx, &oid, name).await.expect("info"));
    assert!(e3 > e2, "{e3:?} > {e2:?}");

    assert_errno(
        lock::lock(&ioctx, &oid, &with_flags("c2", LockFlags::MUST_RENEW)).await,
        ENOENT,
        "must renew c2",
    );
    assert_errno(
        lock::lock(
            &ioctx,
            &oid,
            &with_flags("c1", LockFlags::MAY_RENEW | LockFlags::MUST_RENEW),
        )
        .await,
        EINVAL,
        "both renew flags",
    );
    let info = lock::get_info(&ioctx, &oid, name).await.expect("info");
    assert_eq!(
        info.lockers
            .keys()
            .map(|id| id.cookie.as_str())
            .collect::<Vec<_>>(),
        ["c1"]
    );

    cleanup(&ioctx, &oid).await;
}

#[tokio::test]
#[ignore]
async fn lock_expires() {
    common::init_tracing();
    let (_c, ioctx, _) = client().await;
    let oid = unique("cls-lock-expire");
    let name = "expire";
    ioctx.create(&oid, true).await.expect("create");

    let short = lock_req(
        name,
        LockType::Exclusive,
        "c1",
        "",
        secs(1),
        LockFlags::empty(),
    );
    lock::lock(&ioctx, &oid, &short).await.expect("lock");
    tokio::time::sleep(Duration::from_secs(2)).await;
    lock::lock(&ioctx, &oid, &exclusive(name, "c2"))
        .await
        .expect("lock after expiry");
    assert_errno(
        lock::unlock(&ioctx, &oid, name, "c1").await,
        ENOENT,
        "unlock expired",
    );
    let info = lock::get_info(&ioctx, &oid, name).await.expect("info");
    assert_eq!(
        holders(&info)
            .into_iter()
            .map(|(_, c)| c)
            .collect::<Vec<_>>(),
        ["c2"]
    );

    cleanup(&ioctx, &oid).await;
}

#[tokio::test]
#[ignore]
async fn shared_locks_and_the_tag_rule() {
    common::init_tracing();
    let (_c, ioctx, me) = client().await;
    let oid = unique("cls-lock-shared");
    let name = "shared";
    ioctx.create(&oid, true).await.expect("create");

    let req = |t: LockType, cookie: &str, tag: &str, flags: LockFlags| {
        lock_req(name, t, cookie, tag, secs(30), flags)
    };
    let none = LockFlags::empty();

    for cookie in ["c2", "c1"] {
        lock::lock(&ioctx, &oid, &req(LockType::Shared, cookie, "t", none))
            .await
            .expect("shared");
    }
    let info = lock::get_info(&ioctx, &oid, name).await.expect("info");
    assert_eq!(info.lock_type, LockType::Shared);
    assert_eq!(info.tag, "t");
    assert_eq!(
        holders(&info),
        [
            (me.to_string(), "c1".to_owned()),
            (me.to_string(), "c2".to_owned())
        ]
    );

    assert_errno(
        lock::lock(&ioctx, &oid, &req(LockType::Shared, "c3", "u", none)).await,
        EBUSY,
        "shared under another tag",
    );
    assert_errno(
        lock::lock(
            &ioctx,
            &oid,
            &req(LockType::Shared, "c1", "u", LockFlags::MAY_RENEW),
        )
        .await,
        EBUSY,
        "renew under another tag",
    );
    assert_errno(
        lock::lock(&ioctx, &oid, &req(LockType::Exclusive, "c3", "t", none)).await,
        EBUSY,
        "exclusive over shared",
    );

    cleanup(&ioctx, &oid).await;
}

#[tokio::test]
#[ignore]
async fn unlock_keeps_the_xattr() {
    common::init_tracing();
    let (_c, ioctx, _) = client().await;
    let oid = unique("cls-lock-xattr");
    let name = "xattr";
    ioctx.create(&oid, true).await.expect("create");

    let req = lock_req(
        name,
        LockType::Exclusive,
        "c1",
        "t",
        secs(30),
        LockFlags::empty(),
    );
    lock::lock(&ioctx, &oid, &req).await.expect("lock");
    lock::unlock(&ioctx, &oid, name, "c1")
        .await
        .expect("unlock");

    let info = lock::get_info(&ioctx, &oid, name).await.expect("info");
    assert_eq!(info.lock_type, LockType::Exclusive);
    assert_eq!(info.tag, "t");
    assert!(info.lockers.is_empty());
    assert_eq!(
        lock::list_locks(&ioctx, &oid).await.expect("list_locks"),
        [name]
    );

    let mut raw = ioctx
        .get_xattr(&oid, lock::xattr_name(name))
        .await
        .expect("get_xattr");
    let stored = LockInfo::decode(&mut raw, 0).expect("decode xattr");
    assert_eq!(stored, LockInfo::from(info));

    assert_errno(
        lock::unlock(&ioctx, &oid, name, "c1").await,
        ENOENT,
        "unlock again",
    );

    cleanup(&ioctx, &oid).await;
}

#[tokio::test]
#[ignore]
async fn break_lock_by_locker() {
    common::init_tracing();
    let (_a, a, a_name) = client().await;
    let (_b, b, b_name) = client().await;
    let oid = unique("cls-lock-break");
    let name = "break";
    a.create(&oid, true).await.expect("create");

    lock::lock(&a, &oid, &exclusive(name, "c1"))
        .await
        .expect("A locks");
    assert_errno(
        lock::break_lock(&b, &oid, name, "c2", &a_name).await,
        ENOENT,
        "break with the wrong cookie",
    );
    assert_errno(
        lock::break_lock(&b, &oid, name, "c1", &b_name).await,
        ENOENT,
        "break naming B",
    );
    lock::break_lock(&b, &oid, name, "c1", &a_name)
        .await
        .expect("B breaks A's lock");
    lock::lock(&b, &oid, &exclusive(name, "c1"))
        .await
        .expect("B locks");
    let info = lock::get_info(&b, &oid, name).await.expect("info");
    assert_eq!(holders(&info), [(b_name.to_string(), "c1".to_owned())]);

    cleanup(&a, &oid).await;
}

#[tokio::test]
#[ignore]
async fn set_cookie_moves_the_holder() {
    common::init_tracing();
    let (_c, ioctx, me) = client().await;
    let oid = unique("cls-lock-cookie");
    let name = "cookie";
    ioctx.create(&oid, true).await.expect("create");

    lock::lock(&ioctx, &oid, &exclusive(name, "c1"))
        .await
        .expect("lock");
    let info = lock::get_info(&ioctx, &oid, name).await.expect("info");
    let e1 = info.lockers.values().next().expect("holder").expiration;

    lock::set_cookie(&ioctx, &oid, name, LockType::Exclusive, "c1", "", "c2")
        .await
        .expect("set_cookie");
    let info = lock::get_info(&ioctx, &oid, name).await.expect("info");
    assert_eq!(holders(&info), [(me.to_string(), "c2".to_owned())]);
    assert_eq!(info.lockers.values().next().expect("holder").expiration, e1);
    assert_errno(
        lock::unlock(&ioctx, &oid, name, "c1").await,
        ENOENT,
        "unlock the old cookie",
    );
    lock::unlock(&ioctx, &oid, name, "c2")
        .await
        .expect("unlock the new cookie");

    let shared_name = "cookie-shared";
    for cookie in ["c1", "c2"] {
        let req = lock_req(
            shared_name,
            LockType::Shared,
            cookie,
            "t",
            secs(30),
            LockFlags::empty(),
        );
        lock::lock(&ioctx, &oid, &req).await.expect("shared");
    }
    assert_errno(
        lock::set_cookie(&ioctx, &oid, shared_name, LockType::Shared, "c1", "t", "c2").await,
        EBUSY,
        "new cookie already held",
    );
    assert_errno(
        lock::set_cookie(
            &ioctx,
            &oid,
            shared_name,
            LockType::Exclusive,
            "c1",
            "t",
            "c3",
        )
        .await,
        EBUSY,
        "type mismatch",
    );

    cleanup(&ioctx, &oid).await;
}

#[tokio::test]
#[ignore]
async fn assert_locked_standalone_and_compound() {
    common::init_tracing();
    let (_c, ioctx, _) = client().await;
    let oid = unique("cls-lock-assert");
    let name = "assert";
    ioctx.create(&oid, true).await.expect("create");

    lock::lock(&ioctx, &oid, &exclusive(name, "c1"))
        .await
        .expect("lock");
    lock::assert_locked(&ioctx, &oid, name, LockType::Exclusive, "c1", "")
        .await
        .expect("assert_locked");
    for (t, cookie, tag) in [
        (LockType::Exclusive, "c2", ""),
        (LockType::Exclusive, "c1", "x"),
        (LockType::Shared, "c1", ""),
    ] {
        assert_errno(
            lock::assert_locked(&ioctx, &oid, name, t, cookie, tag).await,
            EBUSY,
            &format!("assert {t:?} {cookie} {tag}"),
        );
    }

    let guarded_write = |cookie: &str, data: &'static [u8]| {
        OpBuilder::new()
            .op(lock::assert_locked_op(name, LockType::Exclusive, cookie, "").expect("op"))
            .write_full(data)
            .build()
    };
    ioctx
        .execute_op(&oid, guarded_write("c1", b"ok"))
        .await
        .expect("guarded write");
    assert_eq!(
        &ioctx.read(&oid, 0, 16).await.expect("read").data[..],
        b"ok"
    );
    assert_errno(
        ioctx.execute_op(&oid, guarded_write("c2", b"no")).await,
        EBUSY,
        "write guarded by another cookie",
    );
    assert_eq!(
        &ioctx.read(&oid, 0, 16).await.expect("read").data[..],
        b"ok"
    );

    let guarded_read = OpBuilder::new()
        .op(lock::assert_locked_op(name, LockType::Exclusive, "c1", "").expect("op"))
        .stat()
        .build();
    ioctx
        .execute_op(&oid, guarded_read)
        .await
        .expect("guarded read");

    lock::unlock(&ioctx, &oid, name, "c1")
        .await
        .expect("unlock");
    assert_errno(
        lock::assert_locked(&ioctx, &oid, name, LockType::Exclusive, "c1", "").await,
        EBUSY,
        "assert after unlock",
    );

    cleanup(&ioctx, &oid).await;
}

#[tokio::test]
#[ignore]
async fn ephemeral_unlock_and_break_delete_the_object() {
    common::init_tracing();
    let (_a, a, a_name) = client().await;
    let name = "ephemeral";
    let ephemeral = |cookie: &str| {
        lock_req(
            name,
            LockType::ExclusiveEphemeral,
            cookie,
            "",
            secs(30),
            LockFlags::empty(),
        )
    };

    let oid = unique("cls-lock-ephemeral-unlock");
    a.write_full(&oid, &b"data"[..]).await.expect("write");
    lock::lock(&a, &oid, &ephemeral("c1")).await.expect("lock");
    assert_errno(
        lock::assert_locked(&a, &oid, name, LockType::Exclusive, "c1", "").await,
        EBUSY,
        "exclusive does not match exclusive-ephemeral",
    );
    lock::unlock(&a, &oid, name, "c1").await.expect("unlock");
    assert_errno(a.stat(&oid).await, ENOENT, "stat after unlock");

    let (_b, b, _) = client().await;
    let oid = unique("cls-lock-ephemeral-break");
    a.write_full(&oid, &b"data"[..]).await.expect("write");
    lock::lock(&a, &oid, &ephemeral("c1")).await.expect("lock");
    lock::break_lock(&b, &oid, name, "c1", &a_name)
        .await
        .expect("break");
    assert_errno(a.stat(&oid).await, ENOENT, "stat after break");
}

#[tokio::test]
#[ignore]
async fn expired_ephemeral_read_is_eio() {
    common::init_tracing();
    let (_c, ioctx, _) = client().await;
    let oid = unique("cls-lock-ephemeral-expired");
    let name = "ephemeral";
    ioctx.write_full(&oid, &b"data"[..]).await.expect("write");

    let short = lock_req(
        name,
        LockType::ExclusiveEphemeral,
        "c1",
        "",
        secs(1),
        LockFlags::empty(),
    );
    lock::lock(&ioctx, &oid, &short).await.expect("lock");
    tokio::time::sleep(Duration::from_secs(2)).await;

    assert_errno(
        lock::get_info(&ioctx, &oid, name).await,
        EIO,
        "get_info of an expired ephemeral lock",
    );
    assert_eq!(ioctx.stat(&oid).await.expect("stat").size, 4);
    assert_errno(
        lock::unlock(&ioctx, &oid, name, "c1").await,
        ENOENT,
        "unlock of an expired ephemeral lock",
    );
    assert_eq!(ioctx.stat(&oid).await.expect("stat").size, 4);

    lock::lock(&ioctx, &oid, &exclusive(name, "c2"))
        .await
        .expect("relock");
    assert_eq!(ioctx.stat(&oid).await.expect("stat").size, 0);

    cleanup(&ioctx, &oid).await;
}

#[tokio::test]
#[ignore]
async fn two_clients_are_two_lockers() {
    common::init_tracing();
    let (_a, a, a_name) = client().await;
    let (_b, b, b_name) = client().await;
    assert_ne!(a_name, b_name);
    let oid = unique("cls-lock-two-clients");
    let name = "two";
    a.create(&oid, true).await.expect("create");

    lock::lock(&a, &oid, &exclusive(name, "c1"))
        .await
        .expect("A locks");
    assert_errno(
        lock::unlock(&b, &oid, name, "c1").await,
        ENOENT,
        "B unlocks A's lock",
    );
    assert_errno(
        lock::lock(&b, &oid, &exclusive(name, "c1")).await,
        EBUSY,
        "B locks with A's cookie",
    );
    let renew = |flags| lock_req(name, LockType::Exclusive, "c1", "", secs(30), flags);
    assert_errno(
        lock::lock(&b, &oid, &renew(LockFlags::MAY_RENEW)).await,
        EBUSY,
        "B renews A's lock with MAY_RENEW",
    );
    assert_errno(
        lock::lock(&b, &oid, &renew(LockFlags::MUST_RENEW)).await,
        ENOENT,
        "B renews A's lock with MUST_RENEW",
    );
    assert_errno(
        lock::assert_locked(&b, &oid, name, LockType::Exclusive, "c1", "").await,
        EBUSY,
        "B asserts A's lock",
    );
    let info = lock::get_info(&a, &oid, name).await.expect("info");
    assert_eq!(holders(&info), [(a_name.to_string(), "c1".to_owned())]);
    lock::unlock(&a, &oid, name, "c1").await.expect("A unlocks");

    cleanup(&a, &oid).await;
}
