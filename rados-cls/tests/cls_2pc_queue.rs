//! Cluster tests for the 2pc_queue class, against Ceph v19.2.2. Run with:
//!   CEPH_CONF=... cargo test -p rados-cls --test cls_2pc_queue -- --ignored --nocapture

#[path = "../../rados/tests/common/mod.rs"]
mod common;

use std::collections::BTreeMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use common::create_ioctx;
use rados::{Denc, IoCtx, OSDClientError, OpBuilder, UTime};
use rados_cls::queue;
use rados_cls::two_pc_queue::{self as q, Reservation, UrgentData};

const ENOENT: i32 = 2;
const EEXIST: i32 = 17;
const EINVAL: i32 = 22;
const ENOSPC: i32 = 28;
const ENODATA: i32 = 61;

/// 256 KiB: upstream ReserveError's queue.
const CAPACITY: u64 = 256 * 1024;

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

fn payloads(ret: &queue::ListRet) -> Vec<&[u8]> {
    ret.entries.iter().map(|e| e.data.as_ref()).collect()
}

fn markers(ret: &queue::ListRet) -> Vec<&str> {
    ret.entries.iter().map(|e| e.marker.as_str()).collect()
}

/// Create and init in one op, as RGW does.
async fn new_queue(ioctx: &IoCtx, oid: &str, size: u64) {
    let op = OpBuilder::new()
        .create(true)
        .op(q::init_op(size).expect("op"))
        .build();
    ioctx.execute_op(oid, op).await.expect("create and init");
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

/// The next instant after `t`, for a strict `<` cut just past it.
fn after(t: UTime) -> UTime {
    if t.nsec == 999_999_999 {
        UTime {
            sec: t.sec + 1,
            nsec: 0,
        }
    } else {
        UTime {
            nsec: t.nsec + 1,
            ..t
        }
    }
}

fn ids(r: &BTreeMap<u32, Reservation>) -> Vec<u32> {
    r.keys().copied().collect()
}

fn one(b: &'static [u8]) -> Vec<Bytes> {
    vec![Bytes::from_static(b)]
}

#[tokio::test]
#[ignore]
async fn init_and_capacity() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-2pc-init");
    let missing = unique("cls-2pc-missing");
    guarded(&ioctx, &[&oid, &missing], async {
        new_queue(&ioctx, &oid, CAPACITY).await;

        assert_eq!(
            q::get_capacity(&ioctx, &oid).await.expect("capacity"),
            CAPACITY
        );
        assert_eq!(
            queue::get_capacity(&ioctx, &oid)
                .await
                .expect("queue capacity"),
            CAPACITY,
            "the plain queue class reads the same ring"
        );

        let err = q::init(&ioctx, &oid, CAPACITY)
            .await
            .expect_err("second init");
        assert!(is_osd_error(&err, EEXIST), "{err:?}");

        let state = q::read_head(&ioctx, &oid).await.expect("head");
        let start = queue::Marker {
            offset: 24_576,
            generation: 0,
        };
        assert_eq!(state.head.max_head_size, 24_576);
        assert_eq!(state.head.front, start);
        assert_eq!(state.head.tail, start);
        assert_eq!(state.head.queue_size, CAPACITY + 24_576);
        assert_eq!(state.head.max_urgent_data_size, 23_552);
        assert_eq!(state.urgent_data, UrgentData::default());
        println!("urgent data version: {}", state.urgent_data_version);
        assert!(
            matches!(state.urgent_data_version, 2 | 3),
            "{}",
            state.urgent_data_version
        );

        // A writing call on a missing object reads zero bytes, which the class
        // takes as uninitialised: init creates the object with the same head.
        q::init(&ioctx, &missing, CAPACITY)
            .await
            .expect("init of a missing object");
        let created = q::read_head(&ioctx, &missing).await.expect("head");
        println!("created head version: {}", created.urgent_data_version);
        assert_eq!(created.head.queue_size, CAPACITY + 24_576);
        assert_eq!(created, state);
        ioctx.remove(&missing).await.expect("remove");

        let stats = q::get_topic_stats(&ioctx, &oid).await.expect("stats");
        assert_eq!((stats.queue_size, stats.queue_entries), (0, 0));
        assert!(
            q::list_reservations(&ioctx, &oid)
                .await
                .expect("reservations")
                .is_empty()
        );
        let list = q::list_entries(&ioctx, &oid, "", 10).await.expect("list");
        assert!(list.entries.is_empty() && !list.is_truncated);
        assert_eq!(list.next_marker, "0/24576");
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn reserve_ids_start_at_one() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-2pc-ids");
    guarded(&ioctx, &[&oid], async {
        new_queue(&ioctx, &oid, CAPACITY).await;

        for i in 1..=10 {
            assert_eq!(q::reserve(&ioctx, &oid, 100, i).await.expect("reserve"), i);
        }
        let reservations = q::list_reservations(&ioctx, &oid)
            .await
            .expect("reservations");
        assert_eq!(ids(&reservations), (1..=10).collect::<Vec<_>>());
        for (&id, r) in &reservations {
            assert_eq!((r.size, r.entries), (100, id), "{id}");
            assert_ne!(r.timestamp.sec, 0, "{id}");
        }

        let state = q::read_head(&ioctx, &oid).await.expect("head");
        assert_eq!(state.urgent_data.last_id, 10);
        assert_eq!(
            state.urgent_data.reserved_size, 1550,
            "10 x 100 plus 10 x (1 + ... + 10)"
        );
        assert_eq!(state.urgent_data.reservations, reservations);
        assert!(!state.urgent_data.has_xattrs);
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn reserve_errors() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-2pc-errors");
    let plain = unique("cls-2pc-plain");
    let empty = unique("cls-2pc-empty");
    let missing = unique("cls-2pc-missing");
    guarded(&ioctx, &[&oid, &plain, &empty, &missing], async {
        new_queue(&ioctx, &oid, CAPACITY).await;

        for (size, entries) in [(0, 1), (1, 0)] {
            let err = q::reserve(&ioctx, &oid, size, entries)
                .await
                .expect_err("zero size or count");
            assert!(is_osd_error(&err, EINVAL), "{size} {entries}: {err:?}");
        }

        // A plain ring's urgent data does not decode as the 2pc bookkeeping.
        let op = OpBuilder::new()
            .create(true)
            .op(queue::init_op(CAPACITY).expect("op"))
            .build();
        ioctx
            .execute_op(&plain, op)
            .await
            .expect("create and init a plain ring");
        let err = q::reserve(&ioctx, &plain, 1, 1)
            .await
            .expect_err("plain ring");
        assert!(is_osd_error(&err, EINVAL), "{err:?}");

        ioctx.create(&empty, true).await.expect("create");
        let err = q::reserve(&ioctx, &empty, 1, 1)
            .await
            .expect_err("empty object");
        assert!(is_osd_error(&err, EINVAL), "{err:?}");

        // A writing call on a missing object reads zero bytes, as for an
        // empty one, and creates nothing.
        let err = q::reserve(&ioctx, &missing, 1, 1)
            .await
            .expect_err("missing object");
        assert!(is_osd_error(&err, EINVAL), "{err:?}");
        let err = ioctx.stat(&missing).await.expect_err("nothing created");
        assert!(is_osd_error(&err, ENOENT), "{err:?}");

        assert_eq!(
            q::reserve(&ioctx, &oid, 1, 1).await.expect("reserve"),
            1,
            "the failures persisted nothing"
        );
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn reserve_enospc_at_the_exact_boundary() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-2pc-enospc");
    guarded(&ioctx, &[&oid], async {
        new_queue(&ioctx, &oid, CAPACITY).await;

        // 253 x (1024 + 10) = 261,602 fits in 262,144; a 254th is 262,636.
        for _ in 0..253 {
            q::reserve(&ioctx, &oid, 1024, 1).await.expect("reserve");
        }
        let err = q::reserve(&ioctx, &oid, 1024, 1).await.expect_err("254th");
        assert!(is_osd_error(&err, ENOSPC), "{err:?}");

        assert_eq!(
            q::list_reservations(&ioctx, &oid)
                .await
                .expect("reservations")
                .len(),
            253
        );
        let state = q::read_head(&ioctx, &oid).await.expect("head");
        assert_eq!(state.urgent_data.reserved_size, 261_602);
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn reserve_commit_list() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-2pc-commit");
    guarded(&ioctx, &[&oid], async {
        new_queue(&ioctx, &oid, CAPACITY).await;

        let id = q::reserve(&ioctx, &oid, 1000, 3).await.expect("reserve");
        q::commit(
            &ioctx,
            &oid,
            id,
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"bb")],
        )
        .await
        .expect("commit");

        let list = q::list_entries(&ioctx, &oid, "", 10).await.expect("list");
        assert_eq!(payloads(&list), [b"a".as_ref(), b"bb"]);
        assert_eq!(markers(&list), ["0/24576", "0/24587"]);
        assert_eq!(list.next_marker, "0/24599");
        assert!(!list.is_truncated);
        assert!(
            q::list_reservations(&ioctx, &oid)
                .await
                .expect("reservations")
                .is_empty()
        );

        // 1 + 10 plus 2 + 10 bytes, and the reserved entries, not the payloads.
        let stats = q::get_topic_stats(&ioctx, &oid).await.expect("stats");
        assert_eq!((stats.queue_size, stats.queue_entries), (23, 3));

        let err = q::commit(&ioctx, &oid, id, one(b"x"))
            .await
            .expect_err("commit of a closed id");
        assert!(is_osd_error(&err, ENOENT), "{err:?}");

        let id2 = q::reserve(&ioctx, &oid, 4, 1).await.expect("reserve");
        let err = q::commit(&ioctx, &oid, id2, one(b"12345"))
            .await
            .expect_err("payload past the reservation");
        assert!(is_osd_error(&err, EINVAL), "{err:?}");
        assert_eq!(
            ids(&q::list_reservations(&ioctx, &oid)
                .await
                .expect("reservations")),
            [id2],
            "a refused commit leaves the reservation open"
        );
        q::commit(&ioctx, &oid, id2, one(b"1234"))
            .await
            .expect("commit");
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn abort_of_an_unknown_id_succeeds() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-2pc-abort");
    guarded(&ioctx, &[&oid], async {
        new_queue(&ioctx, &oid, CAPACITY).await;

        let id = q::reserve(&ioctx, &oid, 100, 1).await.expect("reserve");
        q::abort(&ioctx, &oid, id).await.expect("abort");
        assert!(
            q::list_reservations(&ioctx, &oid)
                .await
                .expect("reservations")
                .is_empty()
        );
        q::abort(&ioctx, &oid, id).await.expect("abort again");
        q::abort(&ioctx, &oid, 9999).await.expect("abort unknown");
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn expire_is_strictly_before_the_stale_time() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-2pc-expire");
    guarded(&ioctx, &[&oid], async {
        new_queue(&ioctx, &oid, CAPACITY).await;

        let a = q::reserve(&ioctx, &oid, 10, 1).await.expect("reserve");
        // The OSD stamps with a coarse clock; the pause keeps the two apart.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let b = q::reserve(&ioctx, &oid, 10, 1).await.expect("reserve");

        // Thresholds come from the OSD's stamps, never the client clock.
        let r = q::list_reservations(&ioctx, &oid)
            .await
            .expect("reservations");
        let (ta, tb) = (r[&a].timestamp, r[&b].timestamp);
        assert!(ta < tb, "{ta:?} {tb:?}");

        q::expire_reservations(&ioctx, &oid, UTime::default())
            .await
            .expect("expire at the epoch");
        let r = q::list_reservations(&ioctx, &oid)
            .await
            .expect("reservations");
        assert_eq!(ids(&r), [a, b]);

        q::expire_reservations(&ioctx, &oid, tb)
            .await
            .expect("expire at tb");
        let r = q::list_reservations(&ioctx, &oid)
            .await
            .expect("reservations");
        assert_eq!(ids(&r), [b], "tb is not strictly before itself");

        q::expire_reservations(&ioctx, &oid, after(tb))
            .await
            .expect("expire past tb");
        assert!(
            q::list_reservations(&ioctx, &oid)
                .await
                .expect("reservations")
                .is_empty()
        );
    })
    .await;
}

/// One single-entry reservation committed with one payload.
async fn commit_one(ioctx: &IoCtx, oid: &str) {
    let id = q::reserve(ioctx, oid, 1, 1).await.expect("reserve");
    q::commit(ioctx, oid, id, one(b"x")).await.expect("commit");
}

async fn entries(ioctx: &IoCtx, oid: &str) -> u32 {
    q::get_topic_stats(ioctx, oid)
        .await
        .expect("stats")
        .queue_entries
}

async fn list(ioctx: &IoCtx, oid: &str) -> queue::ListRet {
    q::list_entries(ioctx, oid, "", 10).await.expect("list")
}

#[tokio::test]
#[ignore]
async fn remove_entries_and_committed_entries() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-2pc-remove");
    guarded(&ioctx, &[&oid], async {
        new_queue(&ioctx, &oid, CAPACITY).await;

        for _ in 0..3 {
            commit_one(&ioctx, &oid).await;
        }
        assert_eq!(
            markers(&list(&ioctx, &oid).await),
            ["0/24576", "0/24587", "0/24598"]
        );

        // An explicit count.
        q::remove_entries(&ioctx, &oid, "0/24598", 2)
            .await
            .expect("remove two");
        assert_eq!(list(&ioctx, &oid).await.entries.len(), 1);
        assert_eq!(entries(&ioctx, &oid).await, 1);

        // No count: the class counts the entries before the marker.
        let next = list(&ioctx, &oid).await.next_marker;
        q::remove_entries(&ioctx, &oid, &next, 0)
            .await
            .expect("remove, counted");
        assert!(list(&ioctx, &oid).await.entries.is_empty());
        assert_eq!(entries(&ioctx, &oid).await, 0);

        // A Reef client's cls_queue_remove_op: counted the same way.
        commit_one(&ioctx, &oid).await;
        commit_one(&ioctx, &oid).await;
        let op = queue::RemoveOp {
            end_marker: list(&ioctx, &oid).await.next_marker,
        };
        ioctx
            .exec(
                &oid,
                q::CLASS,
                "2pc_queue_remove_entries",
                rados::encode_with_capacity(&op, 0).expect("encode"),
            )
            .await
            .expect("v1 remove");
        assert!(list(&ioctx, &oid).await.entries.is_empty());
        assert_eq!(entries(&ioctx, &oid).await, 0);

        // Commit counts the reserved entries, removal counts payloads.
        let id = q::reserve(&ioctx, &oid, 100, 3).await.expect("reserve");
        q::commit(
            &ioctx,
            &oid,
            id,
            vec![Bytes::from_static(b"x"), Bytes::from_static(b"y")],
        )
        .await
        .expect("commit");
        assert_eq!(entries(&ioctx, &oid).await, 3);
        let next = list(&ioctx, &oid).await.next_marker;
        q::remove_entries(&ioctx, &oid, &next, 0)
            .await
            .expect("remove, counted");
        assert_eq!(entries(&ioctx, &oid).await, 1);
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn reserve_without_returnvec_loses_the_id() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-2pc-returnvec");
    guarded(&ioctx, &[&oid], async {
        new_queue(&ioctx, &oid, CAPACITY).await;

        let result = ioctx
            .execute_op(
                &oid,
                OpBuilder::new()
                    .op(q::reserve_op(100, 1).expect("op"))
                    .build(),
            )
            .await
            .expect("reserve without RETURNVEC");
        assert!(result.first_outdata().expect("outdata").is_empty());
        assert!(q::decode_reserve(result.first_reply().expect("reply")).is_err());

        assert_eq!(
            ids(&q::list_reservations(&ioctx, &oid)
                .await
                .expect("reservations")),
            [1],
            "the reservation was made"
        );
        assert_eq!(q::reserve(&ioctx, &oid, 100, 1).await.expect("reserve"), 2);
    })
    .await;
}

/// v19.2.2 is expected to leak ten bytes per reserved entry and v19.2.4+
/// and main not; no request carries `reserved_size`, so a client cannot
/// repair it.
#[tokio::test]
#[ignore]
async fn squid_leaks_the_entry_overhead() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-2pc-leak");
    guarded(&ioctx, &[&oid], async {
        new_queue(&ioctx, &oid, CAPACITY).await;
        let reserved = || async {
            q::read_head(&ioctx, &oid)
                .await
                .expect("head")
                .urgent_data
                .reserved_size
        };

        let v = q::read_head(&ioctx, &oid)
            .await
            .expect("head")
            .urgent_data_version;
        let leaks = v < 3;
        println!("urgent data version: {v}");

        let id = q::reserve(&ioctx, &oid, 100, 3).await.expect("reserve");
        assert_eq!(reserved().await, 130);
        q::commit(&ioctx, &oid, id, vec![Bytes::from_static(&[7; 30]); 3])
            .await
            .expect("commit");
        assert_eq!(reserved().await, if leaks { 30 } else { 0 });

        let id = q::reserve(&ioctx, &oid, 50, 2).await.expect("reserve");
        q::abort(&ioctx, &oid, id).await.expect("abort");
        assert_eq!(reserved().await, if leaks { 50 } else { 0 });

        let id = q::reserve(&ioctx, &oid, 40, 1).await.expect("reserve");
        let t = q::list_reservations(&ioctx, &oid)
            .await
            .expect("reservations")[&id]
            .timestamp;
        q::expire_reservations(&ioctx, &oid, after(t))
            .await
            .expect("expire");
        assert_eq!(reserved().await, if leaks { 60 } else { 0 });
        assert!(
            q::list_reservations(&ioctx, &oid)
                .await
                .expect("reservations")
                .is_empty()
        );

        let next = list(&ioctx, &oid).await.next_marker;
        q::remove_entries(&ioctx, &oid, &next, 0)
            .await
            .expect("remove");
        let stats = q::get_topic_stats(&ioctx, &oid).await.expect("stats");
        assert_eq!(
            (stats.queue_size, stats.queue_entries),
            (0, 0),
            "every byte of the ring is free again"
        );

        if leaks {
            let err = q::reserve(&ioctx, &oid, CAPACITY - 60 - 10 + 1, 1)
                .await
                .expect_err("the leaked sixty bytes");
            assert!(is_osd_error(&err, ENOSPC), "{err:?}");
            q::reserve(&ioctx, &oid, CAPACITY - 60 - 10, 1)
                .await
                .expect("all but the leaked bytes");
        } else {
            q::reserve(&ioctx, &oid, CAPACITY - 10, 1)
                .await
                .expect("the whole ring");
        }
    })
    .await;
}

#[tokio::test]
#[ignore]
async fn reservations_spill_into_the_xattr_at_785() {
    common::init_tracing();
    let ioctx = create_ioctx().await.expect("create_ioctx");
    let oid = unique("cls-2pc-spill");
    guarded(&ioctx, &[&oid], async {
        new_queue(&ioctx, &oid, CAPACITY).await;
        let xattr_ids = || async {
            let mut xattr = ioctx
                .get_xattr(&oid, q::URGENT_DATA_XATTR)
                .await
                .expect("xattr");
            ids(&BTreeMap::<u32, Reservation>::decode(&mut xattr, 0).expect("decode"))
        };

        for _ in 0..784 {
            q::reserve(&ioctx, &oid, 1, 1).await.expect("reserve");
        }
        let state = q::read_head(&ioctx, &oid).await.expect("head");
        assert_eq!(state.urgent_data.reservations.len(), 784);
        assert!(!state.urgent_data.has_xattrs);
        let err = ioctx
            .get_xattr(&oid, q::URGENT_DATA_XATTR)
            .await
            .expect_err("no xattr yet");
        assert!(is_osd_error(&err, ENODATA), "{err:?}");

        // 27 + 30 x 785 bytes would pass the head's 23,552.
        assert_eq!(q::reserve(&ioctx, &oid, 1, 1).await.expect("reserve"), 785);
        let state = q::read_head(&ioctx, &oid).await.expect("head");
        assert_eq!(state.urgent_data.reservations.len(), 784);
        assert!(state.urgent_data.has_xattrs);
        assert_eq!(xattr_ids().await, [785]);
        assert_eq!(
            ids(&q::list_reservations(&ioctx, &oid)
                .await
                .expect("reservations")),
            (1..=785).collect::<Vec<_>>()
        );

        q::commit(&ioctx, &oid, 785, one(b"x"))
            .await
            .expect("commit from the xattr");
        assert_eq!(
            q::list_reservations(&ioctx, &oid)
                .await
                .expect("reservations")
                .len(),
            784
        );
        assert!(xattr_ids().await.is_empty());
        let state = q::read_head(&ioctx, &oid).await.expect("head");
        assert!(state.urgent_data.has_xattrs, "never cleared");
        let err = q::commit(&ioctx, &oid, 9999, one(b"x"))
            .await
            .expect_err("commit of an unknown id after the spill");
        assert!(is_osd_error(&err, ENOENT), "{err:?}");

        assert_eq!(q::reserve(&ioctx, &oid, 1, 1).await.expect("reserve"), 786);
        assert_eq!(xattr_ids().await, [786]);
        q::abort(&ioctx, &oid, 786)
            .await
            .expect("abort from the xattr");
        assert_eq!(
            q::list_reservations(&ioctx, &oid)
                .await
                .expect("reservations")
                .len(),
            784
        );
        q::abort(&ioctx, &oid, 9999).await.expect("abort unknown");
    })
    .await;
}
