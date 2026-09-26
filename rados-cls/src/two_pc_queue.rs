//! The `2pc_queue` object class (`cls_2pc_queue_client.h`): a `queue`
//! ring written in two phases. A writer reserves payload bytes and an
//! entry count, gets an id back, and later commits payloads into that
//! reservation or aborts it; stale reservations are expired. The head,
//! the entry format, init, capacity and listing are the `queue` class's
//! (its types and decoders are reused); the class keeps [`UrgentData`]
//! in the head's urgent data, spilling reservations into the
//! [`URGENT_DATA_XATTR`] xattr past [`MAX_URGENT_DATA_SIZE`]. The
//! writing methods are init, reserve, commit, abort, remove entries and
//! expire reservations; reserve is the only one that replies, so it
//! needs `RETURNVEC`. Everything else is RD.
//!
//! # The capacity leak before v19.2.4 and v20.2.3
//!
//! From the class's first commit (4fba777a1d1) through every release
//! before v19.2.4 and v20.2.3 (Pacific, Quincy, Reef, Squid through
//! v19.2.3, Tentacle through v20.2.2), an OSD adds `size + 10·entries`
//! to `reserved_size` per reservation but takes back only `size` on
//! commit, abort or expire, so every reserved entry permanently costs
//! ten bytes of capacity until `2pc_queue_reserve` answers `ENOSPC`
//! whatever the ring holds (about 12.8 million single-entry
//! reservations at RGW's queue size). v19.2.4, v20.2.3 and main
//! subtract the overhead and write [`UrgentData`] as version 3, but
//! only reserve recomputes `reserved_size`, on a head it decodes at
//! version 2 or older. Commit, abort, expire and remove entries write a
//! version-2 head back as version 3 without recomputing, so a head one
//! of them rewrites first keeps its drift: version 3 does not mean
//! `reserved_size` is exact. A client cannot repair the count on a
//! leaking OSD: no request carries it. [`read_head`] shows it.
//!
//! # How RGW uses the class
//!
//! Facts about `rgw_notify.cc` a Rust driver mirrors; none of it is in
//! this crate. A persistent topic's queue is the object
//! `<account or tenant>:<topic>` in the zone's notification pool
//! (`<zone>.rgw.log`, namespace `notif`), created with `create(true)`
//! and init at 128,000,000 bytes in one op (`EEXIST` is success), then
//! registered as one empty-valued omap key on the object
//! `queues_list_object` in the same pool, a name RGW refuses for a
//! queue. RGW takes no lock on the registry; it pages it 1024 keys at a
//! time. Each daemon, every 30 s plus jitter, asserts the queue exists
//! and takes `cls_lock` `<queue>_lock` on the queue object: exclusive,
//! a 16-character random cookie per daemon, `LOCK_FLAG_MAY_RENEW`, for
//! the failover time (90 s on Squid; three times
//! `rgw_topic_ownership_update_period`, 30 s by default, on main);
//! `EBUSY` means another daemon owns the queue. Before an object write
//! RGW reserves 4096 bytes and one entry per persistent topic (with
//! `RETURNVEC`; `ENOSPC` becomes `SlowDown`); after the write it
//! commits the encoded event, first aborting and re-reserving at the
//! event's size when it exceeds 4096, and a failed write aborts. The
//! owner's deliverer runs `assert_exists`, `assert_locked` and a list
//! of 1024 entries in one read op, then `assert_exists`,
//! `assert_locked` and a remove in one write op; the owner alone, every
//! 30 s, runs `assert_exists`, `assert_locked` and an expire at now
//! minus 120 s. The expiry needs no lock in the class, but a gateway
//! that takes `<queue>_lock` to expire becomes the owner, and radosgw's
//! deliverer then skips that queue.
//!
//! Two driver-level facts a Rust gateway cannot reproduce: main creates
//! `rgw_bucket_persistent_notif_num_shards` (11) queues per topic, `<q>`
//! and `<q>.1` to `<q>.10`, each registered, and picks one per event
//! with libstdc++'s `std::hash<std::string>` of `<bucket>:<key>` modulo
//! the count, which is not portable (nothing checks it: the deliverer
//! drains every shard); and Squid's registry listing never advances
//! past its first 1024 keys, so a registry that main's shards push past
//! 1024 (about 94 topics) makes a Squid radosgw sharing it loop on the
//! first page.

use std::collections::BTreeMap;

use bytes::{Buf, BufMut, Bytes};
use rados::osdclient::error::{OSDClientError, Result};
use rados::osdclient::{IoCtx, OSDOp, OpReply};
use rados::{Denc, RadosError, UTime, VersionedDenc, VersionedEncode};
use serde::Serialize;

use crate::call;
use crate::queue::{GetCapacityRet, GetStatsRet, Head, InitOp, ListOp, ListRet};

pub use crate::queue::{decode_get_capacity, decode_list};

pub const CLASS: &str = "2pc_queue";

/// `cls_2pc_reservation::NO_ID`: RGW's no-reservation sentinel; the
/// class itself hands out 0 once `last_id` wraps, and RGW then skips
/// committing it.
pub const NO_ID: u32 = 0;

/// The urgent-data allowance `2pc_queue_init` gives the head.
pub const MAX_URGENT_DATA_SIZE: u64 = 23_552;

/// The head's size (`queue::HEAD_SIZE_1K` plus [`MAX_URGENT_DATA_SIZE`])
/// and so the offset of the first entry: markers start at `0/24576`.
pub const HEAD_SIZE: u64 = crate::queue::HEAD_SIZE_1K + MAX_URGENT_DATA_SIZE;

/// The xattr a reservation spills into once the head's urgent data would
/// exceed [`MAX_URGENT_DATA_SIZE`]: an encoded `BTreeMap<u32,
/// Reservation>` with no struct header. The `rgw_gc` class uses the same
/// name for its own overflow.
pub const URGENT_DATA_XATTR: &str = "cls_queue_urgent_data";

/// `cls_2pc_reservation`: `size` payload bytes and `entries` entries set
/// aside by `2pc_queue_reserve`, stamped with the OSD's clock (not the
/// client's). The dump leaves `entries` out. Version 1 (Pacific to Reef)
/// had no `entries`; it is decoded as 0 because the 18.2.0 corpus
/// archive holds such samples.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Reservation {
    pub size: u64,
    #[serde(serialize_with = "crate::dump::real_time")]
    pub timestamp: UTime,
    #[serde(skip)]
    pub entries: u32,
}

impl VersionedEncode for Reservation {
    const MAX_DECODE_VERSION: u8 = 2;

    fn encoding_version(&self, _features: u64) -> u8 {
        2
    }

    fn compat_version(&self, _features: u64) -> u8 {
        1
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> std::result::Result<(), RadosError> {
        self.size.encode(buf, features)?;
        self.timestamp.encode(buf, features)?;
        self.entries.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 1, "Reservation", "Pacific v16+");

        let size = u64::decode(buf, features)?;
        let timestamp = UTime::decode(buf, features)?;
        let entries = if struct_v >= 2 {
            u32::decode(buf, features)?
        } else {
            0
        };
        Ok(Self {
            size,
            timestamp,
            entries,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        Some(20)
    }
}

rados::impl_denc_for_versioned!(Reservation);

/// `cls_2pc_urgent_data`: the class's bookkeeping, kept in the queue
/// head's urgent data. `reserved_size` is the bytes promised to open
/// reservations, entry overheads included; on an OSD before v19.2.4 or
/// v20.2.3 each committed, aborted or expired entry leaves ten bytes
/// behind (see the module doc), and a client cannot repair it.
/// `last_id` is the last id handed out. `has_xattrs` says reservations
/// have spilled into [`URGENT_DATA_XATTR`]; it is never cleared.
/// `committed_entries` counts reserved entries committed less entries
/// removed; the dump leaves it out.
///
/// Ceph keeps `reservations` in an `unordered_map` and writes and dumps
/// it in libstdc++ hash order; this map re-encodes and dumps in
/// ascending id order, so two or more reservations differ from Ceph's
/// bytes and dump in order only. Encodes version 2, what a leaking OSD
/// writes; decodes version 3 (v19.2.4, v20.2.3 and main, same layout,
/// written by an OSD without the leak, though a v3 head need not have
/// an exact `reserved_size`: only reserve recomputes it on a v2 head,
/// and commit, abort, expire and remove entries rewrite a v2 head as v3
/// with its drift intact) and version 1 (no `committed_entries`), the
/// latter because the 18.2.0 corpus archive holds such samples.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct UrgentData {
    pub reserved_size: u64,
    pub last_id: u32,
    #[serde(serialize_with = "reservations_dump")]
    pub reservations: BTreeMap<u32, Reservation>,
    pub has_xattrs: bool,
    #[serde(skip)]
    pub committed_entries: u32,
}

impl VersionedEncode for UrgentData {
    const MAX_DECODE_VERSION: u8 = 3;

    fn encoding_version(&self, _features: u64) -> u8 {
        2
    }

    fn compat_version(&self, _features: u64) -> u8 {
        1
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> std::result::Result<(), RadosError> {
        self.reserved_size.encode(buf, features)?;
        self.last_id.encode(buf, features)?;
        self.reservations.encode(buf, features)?;
        self.has_xattrs.encode(buf, features)?;
        self.committed_entries.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 1, "UrgentData", "Pacific v16+");

        let reserved_size = u64::decode(buf, features)?;
        let last_id = u32::decode(buf, features)?;
        let reservations = BTreeMap::<u32, Reservation>::decode(buf, features)?;
        let has_xattrs = bool::decode(buf, features)?;
        let committed_entries = if struct_v >= 2 {
            u32::decode(buf, features)?
        } else {
            0
        };
        Ok(Self {
            reserved_size,
            last_id,
            reservations,
            has_xattrs,
            committed_entries,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

rados::impl_denc_for_versioned!(UrgentData);

/// `encode_json` of a `cls_2pc_reservations`: an array of `{"id", "size",
/// "timestamp"}`, here in ascending id order.
fn reservations_dump<S: serde::Serializer>(
    m: &BTreeMap<u32, Reservation>,
    s: S,
) -> std::result::Result<S::Ok, S::Error> {
    #[derive(Serialize)]
    struct Item {
        id: u32,
        size: u64,
        #[serde(serialize_with = "crate::dump::real_time")]
        timestamp: UTime,
    }
    s.collect_seq(m.iter().map(|(&id, r)| Item {
        id,
        size: r.size,
        timestamp: r.timestamp,
    }))
}

/// `cls_2pc_queue_reserve_op`: set aside `size` payload bytes for
/// `entries` entries.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct ReserveOp {
    pub size: u64,
    pub entries: u32,
}

/// `cls_2pc_queue_reserve_ret`: the new reservation's id. The reply of a
/// write, so it arrives only with `RETURNVEC`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct ReserveRet {
    pub id: u32,
}

/// `cls_2pc_queue_commit_op`: the payloads to enqueue into reservation
/// `id`, one entry each. The dump prints them as base64 `bl_data_vec`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct CommitOp {
    pub id: u32,
    #[serde(rename = "bl_data_vec", serialize_with = "crate::dump::base64_vec")]
    pub data: Vec<Bytes>,
}

/// `cls_2pc_queue_abort_op`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct AbortOp {
    pub id: u32,
}

/// `cls_2pc_queue_expire_op`: reservations stamped strictly before
/// `stale_time` (OSD clock) go.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct ExpireOp {
    #[serde(serialize_with = "crate::dump::real_time")]
    pub stale_time: UTime,
}

/// `cls_2pc_queue_reservations_ret`: the open reservations, head and
/// xattr merged. Ordered by id; see [`UrgentData`] on Ceph's order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct ReservationsRet {
    #[serde(serialize_with = "reservations_dump")]
    pub reservations: BTreeMap<u32, Reservation>,
}

/// `cls_2pc_queue_remove_op`: drop the entries before `end_marker` and
/// take `entries_to_remove` off `committed_entries`; 0 makes the class
/// count them itself. Version 2 (Squid) added the count; what Reef sent
/// is `cls_queue_remove_op`, `queue::RemoveOp`, which the class still
/// accepts. Ceph gives this type no dump and no `ceph-dencoder`
/// registration.
#[derive(Debug, Clone, Default, PartialEq, Eq, VersionedDenc)]
#[denc(
    crate = "rados",
    version = 2,
    compat = 1,
    min_version = 2,
    ceph_release = "Squid v19+"
)]
pub struct RemoveOp {
    pub end_marker: String,
    pub entries_to_remove: u32,
}

/// `QUEUE_HEAD_START`: the magic ahead of the encoded head at offset zero.
const QUEUE_HEAD_START: u16 = 0xDEAD;

/// `cls_2pc_queue_init`: lay out a ring of `size` usable bytes behind a
/// [`HEAD_SIZE`] head holding an empty [`UrgentData`]. `EEXIST` if the
/// object holds a head. On a missing object it succeeds and creates the
/// object with a fresh head: a writing class call on a missing object
/// reads zero bytes, which `queue_read_head` answers with `EINVAL` (the
/// `ret == 0` branch, `cls_queue_src.cc:57-60` at v19.2.2) and
/// `queue_init` takes as uninitialised. `queue_init` already answers
/// `EEXIST` for an object holding a queue head; what RGW's `create(true)`
/// ahead of this op in the same compound adds is `EEXIST` for an existing
/// object that is not a queue, which init would otherwise overwrite (its
/// head read answers `EINVAL` on bad magic or a failed decode, and init
/// treats that as uninitialised). radosgw takes `EEXIST` as success, so a
/// non-queue object under a queue's name leaves that topic's reserves
/// failing with `EINVAL`.
pub fn init_op(size: u64) -> Result<OSDOp> {
    call::op(
        CLASS,
        "2pc_queue_init",
        &InitOp {
            queue_size: size,
            ..InitOp::default()
        },
    )
}

/// `cls_2pc_queue_get_capacity`: the usable bytes; decode with
/// [`decode_get_capacity`].
pub fn get_capacity_op() -> Result<OSDOp> {
    call::raw_op(CLASS, "2pc_queue_get_capacity", Bytes::new())
}

/// `cls_2pc_queue_get_topic_stats`: the ring's used bytes (entry
/// overheads included) and `committed_entries`; decode with
/// [`decode_get_topic_stats`].
pub fn get_topic_stats_op() -> Result<OSDOp> {
    call::raw_op(CLASS, "2pc_queue_get_topic_stats", Bytes::new())
}

/// Decode the reply to [`get_topic_stats_op`].
pub fn decode_get_topic_stats(reply: &OpReply) -> Result<GetStatsRet> {
    call::decode(reply)
}

/// `cls_2pc_queue_reserve`: set aside `size` payload bytes for `entries`
/// entries. `EINVAL` if either is 0 or the object is not a 2pc queue
/// (a plain `queue` ring, an empty object) or is missing, since a
/// writing class call on a missing object reads zero bytes (see
/// [`init_op`]); nothing is created then, and only the read methods
/// answer `ENOENT` for a missing object. `ENOSPC` iff
/// `size + reserved_size + 10·entries` exceeds the ring's free bytes.
/// Ids start at 1 and count up as `u32`, wrapping to [`NO_ID`]; a
/// collision after a wrap is `EAGAIN`. The reservation is stamped with
/// the OSD's clock. On an OSD before v19.2.4 or v20.2.3 the `10·entries`
/// overhead is never given back (see the module doc).
///
/// The id comes back in the out-data of a write, which the OSD keeps
/// only when the operation carries `OpBuilder::returnvec()`; without it
/// the reservation is made and its id lost. The C++ client warns this
/// op cannot be batched with other read/write ops: keep it alone in its
/// compound. Decode the reply with [`decode_reserve`].
pub fn reserve_op(size: u64, entries: u32) -> Result<OSDOp> {
    call::op(CLASS, "2pc_queue_reserve", &ReserveOp { size, entries })
}

/// Decode the reply to [`reserve_op`]: the id.
pub fn decode_reserve(reply: &OpReply) -> Result<u32> {
    Ok(call::decode::<ReserveRet>(reply)?.id)
}

/// `cls_2pc_queue_commit`: enqueue `data`, one entry per payload, into
/// reservation `id` and close it. `EINVAL` if the payloads' total length
/// exceeds the reservation's `size` (neither their count nor the entry
/// overhead is checked, and the reservation stays open); `ENOENT` if no
/// such reservation is open. Once reservations have spilled into the
/// xattr, every release compares the xattr lookup against the head map's
/// `end()` (fixed on main): formally undefined behaviour, but with
/// libstdc++ it still answers `ENOENT`, since every `_Hashtable::end()`
/// is a null iterator. A missing object is `EINVAL`, as for [`reserve_op`],
/// and nothing is created. `committed_entries` grows by the reservation's
/// `entries`, not by `data.len()`; RGW reserves one entry and commits one
/// payload.
pub fn commit_op(id: u32, data: Vec<Bytes>) -> Result<OSDOp> {
    call::op(CLASS, "2pc_queue_commit", &CommitOp { id, data })
}

/// `cls_2pc_queue_abort`: close reservation `id` without enqueueing. An
/// id that is not open is success, a no-op; a missing object is
/// `EINVAL`, as for [`reserve_op`], and nothing is created.
pub fn abort_op(id: u32) -> Result<OSDOp> {
    call::op(CLASS, "2pc_queue_abort", &AbortOp { id })
}

/// `cls_2pc_queue_list_reservations`: every open reservation, head and
/// xattr merged; decode with [`decode_list_reservations`].
pub fn list_reservations_op() -> Result<OSDOp> {
    call::raw_op(CLASS, "2pc_queue_list_reservations", Bytes::new())
}

/// Decode the reply to [`list_reservations_op`].
pub fn decode_list_reservations(reply: &OpReply) -> Result<BTreeMap<u32, Reservation>> {
    Ok(call::decode::<ReservationsRet>(reply)?.reservations)
}

/// `cls_2pc_queue_list_entries`: up to `max` committed entries from
/// `marker` (empty for the front; the first entry is `0/24576`), as
/// `queue_list_entries` answers; decode with [`decode_list`]. Like the
/// C++ client, no end marker.
pub fn list_entries_op(marker: &str, max: u32) -> Result<OSDOp> {
    call::op(CLASS, "2pc_queue_list_entries", &list_request(marker, max))
}

fn list_request(marker: &str, max: u32) -> ListOp {
    ListOp {
        max: u64::from(max),
        start_marker: marker.to_owned(),
        end_marker: String::new(),
    }
}

/// `cls_2pc_queue_remove_entries`: move the front to `end_marker` (as
/// `queue_remove_entries`: a no-op on an empty ring, `EINVAL` behind the
/// front or more than a wrap ahead), then take `entries_to_remove` off
/// `committed_entries`, a `u32` that wraps if overstated. With 0 the
/// class counts the entries before `end_marker` itself.
pub fn remove_entries_op(end_marker: &str, entries_to_remove: u32) -> Result<OSDOp> {
    call::op(
        CLASS,
        "2pc_queue_remove_entries",
        &RemoveOp {
            end_marker: end_marker.to_owned(),
            entries_to_remove,
        },
    )
}

/// `cls_2pc_queue_expire_reservations`: close every reservation stamped
/// strictly before `stale_time`, head and xattr alike. The stamps are
/// the OSD's clock, so client clock skew moves the cut. Writes nothing
/// when nothing is stale. RGW's owner passes now minus 120 s.
pub fn expire_reservations_op(stale_time: UTime) -> Result<OSDOp> {
    call::op(
        CLASS,
        "2pc_queue_expire_reservations",
        &ExpireOp { stale_time },
    )
}

/// Init the ring on an existing `oid`; see [`init_op`].
pub async fn init(ioctx: &IoCtx, oid: &str, size: u64) -> Result<()> {
    call::exec(
        ioctx,
        oid,
        CLASS,
        "2pc_queue_init",
        &InitOp {
            queue_size: size,
            ..InitOp::default()
        },
    )
    .await?;
    Ok(())
}

/// See [`get_capacity_op`].
pub async fn get_capacity(ioctx: &IoCtx, oid: &str) -> Result<u64> {
    let out = call::exec_raw(ioctx, oid, CLASS, "2pc_queue_get_capacity", Bytes::new()).await?;
    Ok(call::decode_bytes::<GetCapacityRet>(out)?.queue_capacity)
}

/// See [`get_topic_stats_op`].
pub async fn get_topic_stats(ioctx: &IoCtx, oid: &str) -> Result<GetStatsRet> {
    let out = call::exec_raw(ioctx, oid, CLASS, "2pc_queue_get_topic_stats", Bytes::new()).await?;
    call::decode_bytes(out)
}

/// See [`reserve_op`]; runs it with `RETURNVEC`.
pub async fn reserve(ioctx: &IoCtx, oid: &str, size: u64, entries: u32) -> Result<u32> {
    let out = call::exec_returnvec(
        ioctx,
        oid,
        CLASS,
        "2pc_queue_reserve",
        &ReserveOp { size, entries },
    )
    .await?;
    Ok(call::decode_bytes::<ReserveRet>(out)?.id)
}

/// See [`commit_op`].
pub async fn commit(ioctx: &IoCtx, oid: &str, id: u32, data: Vec<Bytes>) -> Result<()> {
    call::exec(
        ioctx,
        oid,
        CLASS,
        "2pc_queue_commit",
        &CommitOp { id, data },
    )
    .await?;
    Ok(())
}

/// See [`abort_op`].
pub async fn abort(ioctx: &IoCtx, oid: &str, id: u32) -> Result<()> {
    call::exec(ioctx, oid, CLASS, "2pc_queue_abort", &AbortOp { id }).await?;
    Ok(())
}

/// See [`list_reservations_op`].
pub async fn list_reservations(ioctx: &IoCtx, oid: &str) -> Result<BTreeMap<u32, Reservation>> {
    let out = call::exec_raw(
        ioctx,
        oid,
        CLASS,
        "2pc_queue_list_reservations",
        Bytes::new(),
    )
    .await?;
    Ok(call::decode_bytes::<ReservationsRet>(out)?.reservations)
}

/// See [`list_entries_op`].
pub async fn list_entries(ioctx: &IoCtx, oid: &str, marker: &str, max: u32) -> Result<ListRet> {
    let out = call::exec(
        ioctx,
        oid,
        CLASS,
        "2pc_queue_list_entries",
        &list_request(marker, max),
    )
    .await?;
    call::decode_bytes(out)
}

/// See [`remove_entries_op`].
pub async fn remove_entries(
    ioctx: &IoCtx,
    oid: &str,
    end_marker: &str,
    entries_to_remove: u32,
) -> Result<()> {
    call::exec(
        ioctx,
        oid,
        CLASS,
        "2pc_queue_remove_entries",
        &RemoveOp {
            end_marker: end_marker.to_owned(),
            entries_to_remove,
        },
    )
    .await?;
    Ok(())
}

/// See [`expire_reservations_op`].
pub async fn expire_reservations(ioctx: &IoCtx, oid: &str, stale_time: UTime) -> Result<()> {
    call::exec(
        ioctx,
        oid,
        CLASS,
        "2pc_queue_expire_reservations",
        &ExpireOp { stale_time },
    )
    .await?;
    Ok(())
}

/// A 2pc queue's head as the class stores it at offset zero: the
/// `queue` head, the [`UrgentData`] in it, and that data's encoding
/// version (2 from an OSD before v19.2.4 or v20.2.3, whose
/// `reserved_size` leaks; 3 from a fixed one, not necessarily exact: see
/// the module doc). The only view of `reserved_size`, `last_id`,
/// `has_xattrs` and `committed_entries`, for tests and operator tools.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadState {
    pub head: Head,
    pub urgent_data: UrgentData,
    pub urgent_data_version: u8,
}

/// Parse the first [`HEAD_SIZE`] bytes of a 2pc queue object: the
/// `u16` magic `0xDEAD`, the head's `u64` length, the head, and the
/// urgent data inside it. A plain `queue` ring's head has no urgent
/// data and is refused; an `rgw_gc` queue head also carries urgent data
/// and would be misread, so only 2pc heads are valid input.
pub fn parse_head(data: &[u8]) -> Result<HeadState> {
    let bad = |what: &str| OSDClientError::Other(format!("2pc_queue head: {what}"));
    let mut buf = data;
    if buf.len() < 10 {
        return Err(bad("shorter than its framing"));
    }
    if buf.get_u16_le() != QUEUE_HEAD_START {
        return Err(bad("bad magic"));
    }
    let len = usize::try_from(buf.get_u64_le()).map_err(|_| bad("length"))?;
    let mut body = buf.get(..len).ok_or_else(|| bad("length past the data"))?;
    let head = Head::decode(&mut body, 0)?;
    let urgent_data_version = *head
        .urgent_data
        .first()
        .ok_or_else(|| bad("no urgent data, not a 2pc_queue"))?;
    let urgent_data = UrgentData::decode(&mut head.urgent_data.clone(), 0)?;
    Ok(HeadState {
        head,
        urgent_data,
        urgent_data_version,
    })
}

/// Read and parse the head of the 2pc queue on `oid`; see [`parse_head`].
/// Reservations spilled into [`URGENT_DATA_XATTR`] are not in it.
pub async fn read_head(ioctx: &IoCtx, oid: &str) -> Result<HeadState> {
    parse_head(&ioctx.read(oid, 0, HEAD_SIZE).await?.data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rados::encode_with_capacity;
    use rados::osdclient::types::OpData;
    use std::fmt::Debug;

    fn unhex(s: &str) -> Vec<u8> {
        s.as_bytes()
            .chunks(2)
            .map(|pair| {
                u8::from_str_radix(std::str::from_utf8(pair).expect("ascii"), 16).expect("hex")
            })
            .collect()
    }

    fn bytes<T: Denc>(v: &T) -> Vec<u8> {
        encode_with_capacity(v, 0).expect("encode").to_vec()
    }

    fn json<T: Serialize>(v: &T) -> String {
        serde_json::to_string(v).expect("json")
    }

    fn decode<T: Denc>(hex: &str) -> T {
        T::decode(&mut &unhex(hex)[..], 0).expect("decode")
    }

    /// Encode, decode and dump all match one instance.
    fn pin<T: Denc + Serialize + PartialEq + Debug>(v: &T, hex: &str, dump: &str) {
        assert_eq!(bytes(v), unhex(hex));
        assert_eq!(&decode::<T>(hex), v);
        assert_eq!(json(v), dump);
    }

    #[test]
    fn ops_match_the_oracle_instances() {
        // ceph-dencoder v19.2.2 `select_test N encode export` / dump_json.
        pin(&AbortOp { id: 1 }, "01010400000001000000", r#"{"id":1}"#);
        pin(
            &CommitOp {
                id: 123,
                data: vec![Bytes::from_static(b"foo"), Bytes::from_static(b"bar")],
            },
            "0101160000007b0000000200000003000000666f6f03000000626172",
            r#"{"id":123,"bl_data_vec":["Zm9v","YmFy"]}"#,
        );
        // Instances 1 and 2 are identical: coarse_real_time::min() is the epoch.
        pin(
            &ExpireOp::default(),
            "0101080000000000000000000000",
            r#"{"stale_time":"1970-01-01T00:00:00.000000+0000"}"#,
        );
        pin(
            &ReserveOp::default(),
            "01010c000000000000000000000000000000",
            r#"{"size":0,"entries":0}"#,
        );
        pin(
            &ReserveOp {
                size: 123,
                entries: 456,
            },
            "01010c0000007b00000000000000c8010000",
            r#"{"size":123,"entries":456}"#,
        );
        pin(
            &ReserveRet { id: 123 },
            "0101040000007b000000",
            r#"{"id":123}"#,
        );
        // Corpus 18.2.0 cls_2pc_queue_expire_op/7ace2f35....
        let e: ExpireOp = decode("010108000000077bcf64889b822b");
        assert_eq!(
            e.stale_time,
            UTime {
                sec: 1_691_319_047,
                nsec: 729_979_784
            }
        );
        assert_eq!(
            json(&e),
            r#"{"stale_time":"2023-08-06T10:50:47.729979+0000"}"#
        );
    }

    #[test]
    fn reservation_is_v2_dumps_without_entries_and_reads_reef_v1() {
        pin(
            &Reservation::default(),
            "0201140000000000000000000000000000000000000000000000",
            r#"{"size":0,"timestamp":"1970-01-01T00:00:00.000000+0000"}"#,
        );
        pin(
            &Reservation {
                size: 123,
                ..Reservation::default()
            },
            "0201140000007b00000000000000000000000000000000000000",
            r#"{"size":123,"timestamp":"1970-01-01T00:00:00.000000+0000"}"#,
        );
        // Corpus 19.2.0 cls_2pc_reservation/ff1b6965...: entries 23, not dumped.
        pin(
            &Reservation {
                size: 358,
                timestamp: UTime {
                    sec: 1_727_592_400,
                    nsec: 841_226_706,
                },
                entries: 23,
            },
            "0201140000006601000000000000d0f7f866d219243217000000",
            r#"{"size":358,"timestamp":"2024-09-29T06:46:40.841226+0000"}"#,
        );
        // Corpus 18.2.0 cls_2pc_reservation/fff84520...: version 1, no entries.
        let v1: Reservation = decode("010110000000fa00000000000000e97acf643817981b");
        assert_eq!(
            v1,
            Reservation {
                size: 250,
                timestamp: UTime {
                    sec: 1_691_319_017,
                    nsec: 462_952_248
                },
                entries: 0,
            }
        );
        assert_eq!(
            bytes(&v1),
            unhex("020114000000fa00000000000000e97acf643817981b00000000")
        );
        assert_eq!(
            json(&v1),
            r#"{"size":250,"timestamp":"2023-08-06T10:50:17.462952+0000"}"#
        );
    }

    #[test]
    fn urgent_data_matches_the_oracle_and_the_leak_witness() {
        pin(
            &UrgentData::default(),
            "020115000000000000000000000000000000000000000000000000",
            r#"{"reserved_size":0,"last_id":0,"reservations":[],"has_xattrs":false}"#,
        );
        let mut u = UrgentData {
            reserved_size: 123,
            last_id: 456,
            has_xattrs: true,
            ..UrgentData::default()
        };
        u.reservations.insert(
            789,
            Reservation {
                size: 1,
                timestamp: UTime::default(),
                entries: 2,
            },
        );
        pin(
            &u,
            "0201330000007b00000000000000c8010000010000001503000002011400000001000000000000000000000000000000020000000100000000",
            r#"{"reserved_size":123,"last_id":456,"reservations":[{"id":789,"size":1,"timestamp":"1970-01-01T00:00:00.000000+0000"}],"has_xattrs":true}"#,
        );
        // Corpus 19.2.0 cls_2pc_urgent_data/fc55a9f4...: no reservation open,
        // committed_entries 253, yet reserved_size 2530 = 253 x 10: the leak.
        pin(
            &UrgentData {
                reserved_size: 2530,
                last_id: 11,
                committed_entries: 253,
                ..UrgentData::default()
            },
            "020115000000e2090000000000000b0000000000000000fd000000",
            r#"{"reserved_size":2530,"last_id":11,"reservations":[],"has_xattrs":false}"#,
        );
    }

    #[test]
    fn urgent_data_reads_reef_v1_and_main_v3_and_writes_v2() {
        // Corpus 18.2.0 cls_2pc_urgent_data/ff4ac206...: no committed_entries.
        let v1: UrgentData = decode("0101110000009015000000000000180000000000000000");
        assert_eq!(
            v1,
            UrgentData {
                reserved_size: 5520,
                last_id: 24,
                ..UrgentData::default()
            }
        );
        assert_eq!(
            bytes(&v1),
            unhex("020115000000901500000000000018000000000000000000000000")
        );
        // main and v19.2.4+ write version 3 with the same layout.
        let v3: UrgentData = decode("030115000000e2090000000000000b0000000000000000fd000000");
        assert_eq!((v3.reserved_size, v3.committed_entries), (2530, 253));
        assert_eq!(bytes(&v3)[0], 2);
    }

    #[test]
    fn reservations_ret_dumps_and_encodes_in_ascending_id_order() {
        pin(
            &ReservationsRet::default(),
            "01010400000000000000",
            r#"{"reservations":[]}"#,
        );
        // Oracle instance 2: ids 2 then 1 on the wire (libstdc++ order).
        let r: ReservationsRet = decode(
            "01014000000002000000020000000201140000000000000000000000000000000000000000000000010000000201140000000000000000000000000000000000000000000000",
        );
        assert_eq!(r.reservations.keys().copied().collect::<Vec<_>>(), [1, 2]);
        assert_eq!(
            json(&r),
            r#"{"reservations":[{"id":1,"size":0,"timestamp":"1970-01-01T00:00:00.000000+0000"},{"id":2,"size":0,"timestamp":"1970-01-01T00:00:00.000000+0000"}]}"#
        );
        // Derived, not an oracle: the BTreeMap re-encodes ascending.
        assert_eq!(
            bytes(&r),
            unhex(
                "01014000000002000000010000000201140000000000000000000000000000000000000000000000020000000201140000000000000000000000000000000000000000000000"
            )
        );
    }

    #[test]
    fn remove_op_matches_the_corpus_and_refuses_version_1() {
        // Derived: v19.2.2's ceph-dencoder does not register this type.
        // Corpus 19.2.0 cls_2pc_queue_remove_op/e1487ca5....
        let r = RemoveOp {
            end_marker: "0/31796".to_owned(),
            entries_to_remove: 42,
        };
        assert_eq!(
            bytes(&r),
            unhex("02010f00000007000000302f33313739362a000000")
        );
        assert_eq!(
            decode::<RemoveOp>("02010f00000007000000302f33313739362a000000"),
            r
        );
        // Version 1 on the wire is cls_queue_remove_op, queue::RemoveOp.
        assert!(
            RemoveOp::decode(&mut &unhex("01010b00000007000000302f3331373936")[..], 0).is_err()
        );
    }
    #[test]
    fn ops_name_the_class_and_method() {
        for (op, method) in [
            (init_op(1000), "2pc_queue_init"),
            (get_capacity_op(), "2pc_queue_get_capacity"),
            (get_topic_stats_op(), "2pc_queue_get_topic_stats"),
            (reserve_op(4096, 1), "2pc_queue_reserve"),
            (commit_op(1, vec![]), "2pc_queue_commit"),
            (abort_op(1), "2pc_queue_abort"),
            (list_reservations_op(), "2pc_queue_list_reservations"),
            (list_entries_op("", 1024), "2pc_queue_list_entries"),
            (remove_entries_op("0/24576", 0), "2pc_queue_remove_entries"),
            (
                expire_reservations_op(UTime::default()),
                "2pc_queue_expire_reservations",
            ),
        ] {
            let op = op.expect("op");
            assert!(
                op.indata.starts_with(format!("{CLASS}{method}").as_bytes()),
                "{method}"
            );
            assert!(
                matches!(op.op_data, OpData::Call { class_len: 9, method_len, .. } if method_len as usize == method.len()),
                "{method}"
            );
        }
        // init sends cls_queue_init_op with only queue_size set.
        assert!(init_op(1000).expect("op").indata.ends_with(&unhex(
            "010114000000e803000000000000000000000000000000000000"
        )));
        assert!(
            reserve_op(4096, 1)
                .expect("op")
                .indata
                .ends_with(&unhex("01010c000000001000000000000001000000"))
        );
        for op in [
            get_capacity_op(),
            get_topic_stats_op(),
            list_reservations_op(),
        ] {
            assert!(matches!(
                op.expect("op").op_data,
                OpData::Call { indata_len: 0, .. }
            ));
        }
    }

    #[test]
    fn decoders_unwrap_the_replies() {
        let reply = |hex: &str| rados::OpReply {
            return_code: 0,
            outdata: Bytes::from(unhex(hex)),
        };
        // The 10-byte RETURNVEC reply; without the flag it is empty.
        assert_eq!(
            decode_reserve(&reply("0101040000007b000000")).expect("decode"),
            123
        );
        assert!(decode_reserve(&reply("")).is_err());
        assert!(
            decode_list_reservations(&reply("01010400000000000000"))
                .expect("decode")
                .is_empty()
        );
        let s =
            decode_get_topic_stats(&reply("01010c000000324d0000000000000e030000")).expect("decode");
        assert_eq!((s.queue_size, s.queue_entries), (19_762, 782));
    }

    /// What a `HEAD_SIZE` read of offset zero returns: the `0xDEAD` magic,
    /// the head's length, the head, then zeros.
    fn framed(head: &crate::queue::Head) -> Vec<u8> {
        let body = bytes(head);
        let mut out = 0xDEADu16.to_le_bytes().to_vec();
        out.extend_from_slice(&(body.len() as u64).to_le_bytes());
        out.extend_from_slice(&body);
        out.resize(HEAD_SIZE as usize, 0);
        out
    }

    #[test]
    fn parse_head_reads_what_2pc_init_writes() {
        let start = crate::queue::Marker {
            offset: HEAD_SIZE,
            generation: 0,
        };
        let head = crate::queue::Head {
            max_head_size: HEAD_SIZE,
            front: start,
            tail: start,
            queue_size: 1000 + HEAD_SIZE,
            max_urgent_data_size: MAX_URGENT_DATA_SIZE,
            urgent_data: encode_with_capacity(&UrgentData::default(), 0).expect("encode"),
        };
        assert_eq!(
            bytes(&head).len(),
            105,
            "78 bytes of head, 27 of urgent data"
        );
        let state = parse_head(&framed(&head)).expect("parse");
        assert_eq!(state.head, head);
        assert_eq!(state.urgent_data, UrgentData::default());
        assert_eq!(state.urgent_data_version, 2);
    }

    #[test]
    fn parse_head_rejects_what_is_not_a_2pc_head() {
        assert!(parse_head(&[]).is_err(), "no framing");
        assert!(
            parse_head(&[0xad, 0xde, 0xff, 0, 0, 0, 0, 0, 0, 0]).is_err(),
            "length past the data"
        );
        let mut wire = framed(&crate::queue::Head::default());
        assert!(
            parse_head(&wire).is_err(),
            "a plain queue head has no urgent data"
        );
        wire[0] = 0;
        assert!(parse_head(&wire).is_err(), "bad magic");
    }
}
