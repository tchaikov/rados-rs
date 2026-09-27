//! Watch/notify cluster tests against Ceph v19.2.2, after
//! `src/test/librados/watch_notify.cc`. Run with:
//!   CEPH_CONF=... cargo test -p rados --test watch_notify -- --ignored --nocapture

mod common;

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::task::Poll;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::{build_test_client, create_ioctx, test_pool_name};
use rados::{IoCtx, NotifyAck, NotifyTimeout, OSDClientError, WatchEvent, Watcher};

const ENOENT: i32 = 2;
const ENOTCONN: i32 = 107;

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

/// Run `body`, then remove `oids` whether it passed or panicked, so a
/// failing test leaves nothing in the pool.
async fn guarded(ioctx: &IoCtx, oids: &[&str], body: impl Future<Output = ()>) {
    let mut body = std::pin::pin!(body);
    let result = std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(AssertUnwindSafe(|| body.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(())) => Poll::Ready(Ok(())),
            Err(panic) => Poll::Ready(Err(panic)),
        }
    })
    .await;
    for oid in oids {
        if let Err(err) = ioctx.remove(oid).await
            && !is_osd_error(&err, ENOENT)
        {
            eprintln!("cleanup: removing {oid}: {err:?}");
        }
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

async fn next_event(watcher: &mut Watcher, within: Duration) -> WatchEvent {
    tokio::time::timeout(within, watcher.recv())
        .await
        .expect("a watch event in time")
        .expect("the watch channel is open")
}

/// Receive one notify and ack it with `reply`; returns the notify's
/// notifier gid and payload.
async fn ack_next(watcher: &mut Watcher, reply: &'static [u8]) -> (u64, Bytes) {
    match next_event(watcher, Duration::from_secs(15)).await {
        WatchEvent::Notify {
            notify_id,
            notifier_gid,
            payload,
        } => {
            watcher
                .notify_ack(notify_id, Bytes::from_static(reply))
                .await
                .expect("notify_ack");
            (notifier_gid, payload)
        }
        other => panic!("expected a notify, got {other:?}"),
    }
}

async fn two_clients() -> (IoCtx, IoCtx) {
    let a = create_ioctx().await.expect("client A");
    let b = create_ioctx().await.expect("client B");
    assert_ne!(a.instance_id(), b.instance_id(), "two clients, two gids");
    (a, b)
}

#[tokio::test]
#[ignore]
async fn watch_on_a_missing_object_is_enoent() {
    let a = create_ioctx().await.expect("ioctx");
    let err = a
        .watch(unique("wn-missing"))
        .await
        .expect_err("watch of a missing object");
    assert!(is_osd_error(&err, ENOENT), "{err:?}");
}

#[tokio::test]
#[ignore]
async fn notify_reaches_the_watcher_and_returns_its_reply() {
    let (a, b) = two_clients().await;
    let oid = unique("wn-notify");
    a.write_full(&oid, Bytes::from_static(b"x"))
        .await
        .expect("write");
    guarded(&a, &[&oid], async {
        let mut watcher = a.watch(oid.as_str()).await.expect("watch");
        assert!(watcher.cookie() > 1000, "cookie {}", watcher.cookie());

        let (result, (gid, payload)) = tokio::join!(
            b.notify(oid.as_str(), Bytes::from_static(b"hello"), 0),
            ack_next(&mut watcher, b"reply"),
        );
        assert_eq!(gid, b.instance_id());
        assert_eq!(payload, Bytes::from_static(b"hello"));
        let result = result.expect("notify");
        assert_eq!(
            result.acks,
            vec![NotifyAck {
                gid: a.instance_id(),
                cookie: watcher.cookie(),
                reply: Bytes::from_static(b"reply"),
            }]
        );
        assert!(result.missed.is_empty());
        assert!(!result.timed_out);
        watcher.check().expect("watch healthy");

        let err = b
            .notify(unique("wn-missing"), Bytes::new(), 0)
            .await
            .expect_err("notify of a missing object");
        assert!(is_osd_error(&err, ENOENT), "{err:?}");

        let watchers = a.list_watchers(oid.as_str()).await.expect("list_watchers");
        assert!(
            watchers
                .iter()
                .any(|w| w.cookie == watcher.cookie() && w.name.num.get() == a.instance_id()),
            "{watchers:?}"
        );
        watcher.unwatch().await.expect("unwatch");
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn two_watches_from_one_client() {
    let (a, b) = two_clients().await;
    let oid = unique("wn-multi");
    a.write_full(&oid, Bytes::from_static(b"x"))
        .await
        .expect("write");
    guarded(&a, &[&oid], async {
        let mut w1 = a.watch(oid.as_str()).await.expect("watch 1");
        let mut w2 = a.watch(oid.as_str()).await.expect("watch 2");
        assert_ne!(w1.cookie(), w2.cookie());

        let (result, _, _) = tokio::join!(
            b.notify(oid.as_str(), Bytes::from_static(b"both"), 0),
            ack_next(&mut w1, b"one"),
            ack_next(&mut w2, b"two"),
        );
        let result = result.expect("notify");
        assert!(!result.timed_out);
        assert!(result.missed.is_empty());
        let mut acks: Vec<(u64, u64, Bytes)> = result
            .acks
            .into_iter()
            .map(|a| (a.gid, a.cookie, a.reply))
            .collect();
        acks.sort();
        let mut want = vec![
            (a.instance_id(), w1.cookie(), Bytes::from_static(b"one")),
            (a.instance_id(), w2.cookie(), Bytes::from_static(b"two")),
        ];
        want.sort();
        assert_eq!(acks, want);

        w1.unwatch().await.expect("unwatch 1");
        w2.unwatch().await.expect("unwatch 2");
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn notify_times_out_when_a_watcher_does_not_ack() {
    let (a, b) = two_clients().await;
    let oid = unique("wn-timeout");
    a.write_full(&oid, Bytes::from_static(b"x"))
        .await
        .expect("write");
    guarded(&a, &[&oid], async {
        let mut watcher = a.watch(oid.as_str()).await.expect("watch");

        let started = Instant::now();
        let result = b
            .notify(oid.as_str(), Bytes::from_static(b"ignored"), 1000)
            .await
            .expect("a timed-out notify is still Ok");
        eprintln!("1 s notify completed after {:?}", started.elapsed());
        assert!(result.timed_out);
        assert!(result.acks.is_empty());
        assert_eq!(
            result.missed,
            vec![NotifyTimeout {
                gid: a.instance_id(),
                cookie: watcher.cookie(),
            }]
        );
        // The unacked notify still reached the watcher.
        assert!(matches!(
            next_event(&mut watcher, Duration::from_secs(5)).await,
            WatchEvent::Notify { .. }
        ));

        let (result, _) = tokio::join!(
            b.notify(oid.as_str(), Bytes::from_static(b"acked"), 300_000),
            ack_next(&mut watcher, b"ok"),
        );
        let result = result.expect("notify");
        assert!(!result.timed_out);
        assert_eq!(result.acks.len(), 1);
        assert!(result.missed.is_empty());
        watcher.check().expect("watch healthy");
        watcher.unwatch().await.expect("unwatch");
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn deleting_the_object_disconnects_the_watch() {
    let (a, b) = two_clients().await;
    let oid = unique("wn-delete");
    a.write_full(&oid, Bytes::from_static(b"x"))
        .await
        .expect("write");
    guarded(&a, &[&oid], async {
        let mut watcher = a.watch(oid.as_str()).await.expect("watch");
        b.remove(&oid).await.expect("remove");

        let started = Instant::now();
        assert_eq!(
            next_event(&mut watcher, Duration::from_secs(30)).await,
            WatchEvent::Disconnect { code: -ENOTCONN }
        );
        eprintln!("disconnect after {:?}", started.elapsed());
        let err = watcher.check().expect_err("watch lost");
        assert!(is_osd_error(&err, ENOTCONN), "{err:?}");
        let err = watcher
            .unwatch()
            .await
            .expect_err("unwatch of a deleted object");
        assert!(is_osd_error(&err, ENOENT), "{err:?}");
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn watch_times_out_without_pings() {
    let client_a = build_test_client().await.expect("client A");
    let a = client_a.open_pool(&test_pool_name()).await.expect("pool");
    let b = create_ioctx().await.expect("client B");
    let oid = unique("wn-ping");
    a.write_full(&oid, Bytes::from_static(b"x"))
        .await
        .expect("write");
    guarded(&a, &[&oid], async {
        let mut watcher = a.watch_with_timeout(oid.as_str(), 4).await.expect("watch");
        let age = watcher.check().expect("fresh watch");
        assert!(age < Duration::from_secs(1), "age {age:?}");

        client_a.osd_client().set_watch_pings_enabled(false);
        let started = Instant::now();
        let event = next_event(&mut watcher, Duration::from_secs(10)).await;
        eprintln!("unpinged watch lost after {:?}", started.elapsed());
        assert_eq!(event, WatchEvent::Disconnect { code: -ENOTCONN });

        let result = b
            .notify(oid.as_str(), Bytes::from_static(b"nobody"), 0)
            .await
            .expect("notify");
        assert!(result.acks.is_empty(), "{result:?}");
        assert!(result.missed.is_empty(), "{result:?}");
        drop(watcher);

        client_a.osd_client().set_watch_pings_enabled(true);
        let mut watcher = a.watch(oid.as_str()).await.expect("re-watch");
        let (result, _) = tokio::join!(
            b.notify(oid.as_str(), Bytes::from_static(b"again"), 0),
            ack_next(&mut watcher, b"back"),
        );
        let result = result.expect("notify");
        assert_eq!(result.acks.len(), 1, "{result:?}");
        assert_eq!(result.acks[0].cookie, watcher.cookie());
        watcher.unwatch().await.expect("unwatch");
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn watch_survives_a_session_reset() {
    common::init_tracing();
    let (a, b) = two_clients().await;
    let oid = unique("wn-reset");
    a.write_full(&oid, Bytes::from_static(b"x"))
        .await
        .expect("write");
    guarded(&a, &[&oid], async {
        let mut watcher = a.watch(oid.as_str()).await.expect("watch");
        a.close_primary_session_for_test(oid.as_str())
            .await
            .expect("close the primary's session");

        let started = Instant::now();
        let (result, (gid, _)) = tokio::join!(
            b.notify(oid.as_str(), Bytes::from_static(b"after reset"), 10_000),
            ack_next(&mut watcher, b"still here"),
        );
        eprintln!("notify after reset acked in {:?}", started.elapsed());
        assert!(started.elapsed() < Duration::from_secs(10));
        assert_eq!(gid, b.instance_id());
        let result = result.expect("notify");
        assert!(!result.timed_out, "{result:?}");
        assert_eq!(result.acks.len(), 1, "{result:?}");
        assert_eq!(result.acks[0].gid, a.instance_id());
        assert_eq!(result.acks[0].cookie, watcher.cookie());
        watcher.check().expect("watch healthy after the reset");
        watcher.unwatch().await.expect("unwatch");
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn unwatch_then_notify_finds_no_watcher() {
    let (a, b) = two_clients().await;
    let oid = unique("wn-unwatch");
    a.write_full(&oid, Bytes::from_static(b"x"))
        .await
        .expect("write");
    guarded(&a, &[&oid], async {
        let watcher = a.watch(oid.as_str()).await.expect("watch");
        watcher.unwatch().await.expect("unwatch");

        let started = Instant::now();
        let result = b
            .notify(oid.as_str(), Bytes::from_static(b"anyone"), 0)
            .await
            .expect("notify");
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(result.acks.is_empty());
        assert!(result.missed.is_empty());
        assert!(!result.timed_out);
    })
    .await;
}
