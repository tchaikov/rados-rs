//! The `lock` class: advisory locks on an object. Mirrors
//! `cls_lock_client.h`.
//!
//! Server facts the API rests on (`src/cls/lock/cls_lock.cc`, Ceph v19):
//!
//! - State is one xattr per lock, `lock.<name>`, holding [`LockInfo`]; no
//!   omap, no data. `lock` on a missing object creates it (RGW prepends
//!   `assert_exists` where that matters); every other method on a missing
//!   object is `ENOENT`.
//! - A holder is `(client.<global_id>, cookie)`: the entity of the RADOS
//!   client session, shared by every `IoCtx` of one client. The address is
//!   recorded (retyped `LEGACY`) and reported, never compared. So a new
//!   client instance (restart, reconnect under a new global id) cannot
//!   renew, unlock or assert its predecessor's lock (renew → `EBUSY` with
//!   `MAY_RENEW` (a second entry for a shared lock), `ENOENT` with
//!   `MUST_RENEW`; unlock → `ENOENT`) and must wait for
//!   expiry or `break_lock`; and two workers in one client with one cookie
//!   are one holder: flags 0 → `EEXIST`, `MAY_RENEW` → a silent takeover.
//!   RGW's lifecycle code therefore treats `EBUSY || EEXIST` as busy (GC
//!   checks only `EBUSY`). Cookies need only be unique within one client;
//!   RGW's random ones are 15 characters (`COOKIE_LEN 16` less the NUL) or,
//!   for the notification manager, 16.
//! - `lock`: `EINVAL` for type `NONE` or unknown, an empty name, or both
//!   renew flags; then `EBUSY` if any unexpired holder exists under a
//!   different `tag` (checked before renewal, so even the holder cannot
//!   renew under another tag); the same holder again: flags 0 → `EEXIST`,
//!   either renew flag → renewed (expiration, address and description
//!   refreshed); `MUST_RENEW` without that holder → `ENOENT`; other holders
//!   remain: an exclusive request → `EBUSY`, a shared request over a
//!   different stored type → `EBUSY`; shared locks under one tag coexist.
//!   Expiration is the OSD's clock plus `duration`; a zero duration never
//!   expires. Expired holders are dropped on every read of the state.
//! - `unlock` (the caller's entity) and `break_lock` (the given `locker`;
//!   anyone with write access who knows name, entity and cookie; no
//!   PROMOTE flag) are `ENOENT` for an absent or expired holder; after the
//!   last holder leaves, the xattr stays with its type and tag, and
//!   `get_info` reports them with no holders. Unlocking or breaking an
//!   **ephemeral** lock deletes the object.
//! - `get_info` on an existing object without the lock succeeds with type
//!   `none`. `list_locks` lists every `lock.`-prefixed xattr and never drops
//!   expired or fully unlocked locks.
//! - `assert_locked` is `EBUSY` when there is no holder, the stored type is
//!   not exactly the requested one (`EXCLUSIVE` does not match
//!   `EXCLUSIVE_EPHEMERAL`), the tag differs, or the caller's `(entity,
//!   cookie)` is not a holder; it does not renew and composes into read and
//!   write ops (a failing assert fails the whole op). `set_cookie` makes the
//!   same checks, is `EBUSY` if `new_cookie` is already a holder of the
//!   caller, and moves the entry keeping expiration, address and
//!   description.
//! - An ephemeral lock whose last holder has expired is deleted with its
//!   object only by a `lock` that succeeds, which recreates the object
//!   empty in the same op. Every other method's delete is discarded with
//!   the op (a class error drops the whole transaction): `unlock` and
//!   `break_lock` answer `ENOENT`, `set_cookie` `EBUSY`, `lock` with
//!   `MUST_RENEW` `ENOENT`, and `get_info` and `assert_locked` (RD only)
//!   `EIO`; all of them leave the object in place.
//!
//! No method writes a reply, and the two that reply are read-only, so none
//! needs `OpBuilder::returnvec`.
//!
//! # radosgw's locks
//!
//! As Squid v19.2.2 takes them (`main` differences noted). A Rust RGW that
//! shares a cluster with radosgw takes the same locks by the same rules.
//!
//! | Worker | Lock name | Object (pool) | Cookie | Duration (option, default) | Type, flags | Renewal | On `EBUSY` |
//! |---|---|---|---|---|---|---|---|
//! | GC (`RGWGC::process`) | `gc_process` | `gc.<i>`, `i < rgw_gc_max_objs` (32) (GC pool) | empty | `rgw_gc_processor_max_time`, 1 h (≤ 0: `EAGAIN`, no lock) | exclusive, 0, empty tag | none; unlock at the end | skip the shard (only `EBUSY` is checked) |
//! | LC shard walk (`RGWLC::process`) | `lc_process` | `lc.<i>`, `i < rgw_lc_max_objs` (32) (LC pool) | `lc_thrd: <ix>` | `rgw_lc_lock_max_time`, 90 s | exclusive, 0 | none; dropped before `bucket_lc_process` | `EBUSY`/`EEXIST`: retry, backoff 5 × 50 ms |
//! | LC single bucket (`process_bucket`), `bucket_lc_post`, `guard_lc_modify` (S3 Put/Delete lifecycle) | `lc_process` | `lc.<i>` | `lc_thrd: <ix>`; `RGWLC::cookie` (15 random, per process); that or a fresh 15-char cookie | 90 s | exclusive, 0 | none | `EBUSY`/`EEXIST`: return `EBUSY`; sleep 5 s and retry forever; retry every 100 ms, 500 times |
//! | Reshard logshard (`RGWBucketReshardLock`) | `reshard_process` | `reshard.%010u` (reshard pool) | 15 random | `rgw_reshard_bucket_lock_duration`, 360 s (min 30) | exclusive, 0 | `MUST_RENEW` after `duration / 2`, per entry; `ENOENT` = expired | logged, returned |
//! | Reshard per bucket (same class) | `reshard_process` | `[tenant:]name[:bucket_id]` (reshard pool) | 15 random | 360 s | **exclusive-ephemeral**, 0 | as above, renewing the logshard lock first | logged, returned |
//! | Multipart completion (`RGWCompleteMultipart`, S3 path) | `RGWCompleteMultipart` | the upload's meta object (the placement's data-extra pool, `*.rgw.buckets.non-ec`; the data pool if none is set) | empty | `rgw_mp_lock_max_time`, 10 min | `assert_exists` + exclusive, 0 | none on Squid; `main` renews every `duration / 2` with `MUST_RENEW` | `ENOENT` with a completed upload = success; else "already in progress" |
//! | Notification queue (`rgw_notify.cc`) | `<queue>_lock` | the queue object `<queue>` (notification pool) | 16 random, per manager | 90 s (Squid; `main`: 3 × `rgw_topic_ownership_update_period`, 30 s) | `assert_exists` + exclusive, `MAY_RENEW` | re-lock every 30 s + 100-500 ms jitter | owned elsewhere, skip; `ENOENT`: queue deleted |
//!
//! Queue ownership is checked with `assert_exists` +
//! `assert_locked(EXCLUSIVE, cookie, "")` batched with each 2pc-queue op
//! (`EBUSY`: ownership moved, stop); release is `assert_exists` + `unlock`,
//! with `ENOENT` and `EBUSY` treated as done. `bucket_instance_lock` is
//! constructed and never taken; the notification registry object
//! (`queues_list_object`) has no lock. (The Swift object expirer, outside
//! a Rust RGW's scope, also locks `gc_process`, on
//! `obj_delete_at_hint.%010u`.)

use std::collections::BTreeMap;

use bytes::Bytes;
use rados::osdclient::error::Result;
use rados::osdclient::{IoCtx, OSDOp, OpReply};
use rados::{UTime, VersionedDenc};
use serde::Serialize;
use serde::ser::{SerializeSeq, SerializeStruct};

use crate::call;
use crate::dump;

pub use rados::osdclient::types::PackedEntityName;
pub use rados::{EntityAddr, LockFlags, LockType};

/// The class name.
pub const CLASS: &str = "lock";

/// `cls_lock_type_str`: `none`, `exclusive`, `shared`,
/// `exclusive-ephemeral`.
pub fn lock_type_str(t: LockType) -> &'static str {
    match t {
        LockType::None => "none",
        LockType::Exclusive => "exclusive",
        LockType::Shared => "shared",
        LockType::ExclusiveEphemeral => "exclusive-ephemeral",
    }
}

fn type_as_str<S: serde::Serializer>(t: &LockType, s: S) -> std::result::Result<S::Ok, S::Error> {
    s.serialize_str(lock_type_str(*t))
}

fn type_as_int<S: serde::Serializer>(t: &LockType, s: S) -> std::result::Result<S::Ok, S::Error> {
    s.serialize_u8(*t as u8)
}

fn flags_as_int<S: serde::Serializer>(f: &LockFlags, s: S) -> std::result::Result<S::Ok, S::Error> {
    s.serialize_u8(f.bits())
}

fn entity_name<S: serde::Serializer>(
    n: &PackedEntityName,
    s: S,
) -> std::result::Result<S::Ok, S::Error> {
    s.collect_str(n)
}

fn legacy_addr<S: serde::Serializer>(a: &EntityAddr, s: S) -> std::result::Result<S::Ok, S::Error> {
    s.serialize_str(&a.legacy_str())
}

/// `locker_id_t`: a holder, the client's entity plus the cookie it
/// locked with. Ordered as the C++ key is, entity first.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct LockerId {
    #[serde(serialize_with = "entity_name")]
    pub locker: PackedEntityName,
    pub cookie: String,
}

/// `locker_info_t`: what the class records about a holder. `expiration`
/// is on the OSD's clock, zero for a lock that never expires.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct LockerInfo {
    #[serde(serialize_with = "dump::utime_localtime")]
    pub expiration: UTime,
    #[serde(serialize_with = "legacy_addr")]
    pub addr: EntityAddr,
    pub description: String,
}

/// `lock_info_t`: the value of the object's `lock.<name>` xattr.
#[derive(Debug, Clone, Default, PartialEq, Eq, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct LockInfo {
    pub lockers: BTreeMap<LockerId, LockerInfo>,
    pub lock_type: LockType,
    pub tag: String,
}

/// The dump prints the type as an int and nests each holder as
/// `{"id", "info"}`.
impl Serialize for LockInfo {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Locker<'a> {
            id: &'a LockerId,
            info: &'a LockerInfo,
        }

        struct Lockers<'a>(&'a BTreeMap<LockerId, LockerInfo>);

        impl Serialize for Lockers<'_> {
            fn serialize<S: serde::Serializer>(
                &self,
                s: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                let mut seq = s.serialize_seq(Some(self.0.len()))?;
                for (id, info) in self.0 {
                    seq.serialize_element(&Locker { id, info })?;
                }
                seq.end()
            }
        }

        #[derive(Serialize)]
        struct Dump<'a> {
            #[serde(serialize_with = "type_as_int")]
            lock_type: &'a LockType,
            tag: &'a str,
            lockers: Lockers<'a>,
        }

        Dump {
            lock_type: &self.lock_type,
            tag: &self.tag,
            lockers: Lockers(&self.lockers),
        }
        .serialize(serializer)
    }
}

/// `cls_lock_lock_op`. `duration` is sent as given (RGW's multisite path
/// puts milliseconds in `nsec`); zero never expires.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct LockOp {
    pub name: String,
    #[serde(rename = "type", serialize_with = "type_as_str")]
    pub lock_type: LockType,
    pub cookie: String,
    pub tag: String,
    pub description: String,
    #[serde(serialize_with = "dump::utime_localtime")]
    pub duration: UTime,
    #[serde(serialize_with = "flags_as_int")]
    pub flags: LockFlags,
}

/// `cls_lock_unlock_op`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct UnlockOp {
    pub name: String,
    pub cookie: String,
}

/// `cls_lock_break_op`; the dump prints `cookie` before `locker`.
#[derive(Debug, Clone, Default, PartialEq, Eq, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct BreakOp {
    pub name: String,
    pub locker: PackedEntityName,
    pub cookie: String,
}

impl Serialize for BreakOp {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("BreakOp", 3)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("cookie", &self.cookie)?;
        state.serialize_field("locker", &self.locker.to_string())?;
        state.end()
    }
}

/// `cls_lock_get_info_op`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct GetInfoOp {
    pub name: String,
}

/// `cls_lock_get_info_reply`: [`LockInfo`]'s wire, with expired holders
/// already dropped. The dump prints the type as a string and flattens
/// each holder.
#[derive(Debug, Clone, Default, PartialEq, Eq, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct GetInfoReply {
    pub lockers: BTreeMap<LockerId, LockerInfo>,
    pub lock_type: LockType,
    pub tag: String,
}

impl Serialize for GetInfoReply {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Locker<'a> {
            locker: String,
            description: &'a str,
            cookie: &'a str,
            expiration: String,
            addr: String,
        }

        let lockers: Vec<Locker<'_>> = self
            .lockers
            .iter()
            .map(|(id, info)| Locker {
                locker: id.locker.to_string(),
                description: &info.description,
                cookie: &id.cookie,
                expiration: dump::localtime(&info.expiration),
                addr: info.addr.legacy_str(),
            })
            .collect();
        let mut state = serializer.serialize_struct("GetInfoReply", 3)?;
        state.serialize_field("lock_type", lock_type_str(self.lock_type))?;
        state.serialize_field("tag", &self.tag)?;
        state.serialize_field("lockers", &lockers)?;
        state.end()
    }
}

impl From<GetInfoReply> for LockInfo {
    fn from(r: GetInfoReply) -> Self {
        Self {
            lockers: r.lockers,
            lock_type: r.lock_type,
            tag: r.tag,
        }
    }
}

impl From<LockInfo> for GetInfoReply {
    fn from(i: LockInfo) -> Self {
        Self {
            lockers: i.lockers,
            lock_type: i.lock_type,
            tag: i.tag,
        }
    }
}

/// `cls_lock_list_locks_reply`; the dump wraps each name in its own
/// array.
#[derive(Debug, Clone, Default, PartialEq, Eq, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct ListLocksReply {
    pub locks: Vec<String>,
}

impl Serialize for ListLocksReply {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let wrapped: Vec<[&str; 1]> = self.locks.iter().map(|l| [l.as_str()]).collect();
        let mut state = serializer.serialize_struct("ListLocksReply", 1)?;
        state.serialize_field("locks", &wrapped)?;
        state.end()
    }
}

/// `cls_lock_assert_op`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct AssertOp {
    pub name: String,
    #[serde(rename = "type", serialize_with = "type_as_str")]
    pub lock_type: LockType,
    pub cookie: String,
    pub tag: String,
}

/// `cls_lock_set_cookie_op`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct SetCookieOp {
    pub name: String,
    #[serde(rename = "type", serialize_with = "type_as_str")]
    pub lock_type: LockType,
    pub cookie: String,
    pub tag: String,
    pub new_cookie: String,
}

/// The xattr that holds the lock's state, for a reader that fetches it
/// among the object's other xattrs; decode it with `LockInfo::decode`
/// ([`rados::Denc`]). Unlike [`get_info`], the raw value is not trimmed of
/// expired holders, whose `expiration` is on the OSD's clock.
pub fn xattr_name(lock_name: &str) -> String {
    format!("lock.{lock_name}")
}

/// `rados::cls::lock::lock`: take or renew `op.name` as `(this client,
/// op.cookie)`. Sent as given, both renew flags included (the class
/// answers `EINVAL`). `EINVAL` for type `None`, an empty name or both
/// renew flags; `EBUSY` for a live holder under another tag or a
/// conflicting holder; `EEXIST` for the same holder without a renew flag;
/// `ENOENT` for `MUST_RENEW` without that holder.
pub fn lock_op(op: &LockOp) -> Result<OSDOp> {
    call::op(CLASS, "lock", op)
}

/// `rados::cls::lock::unlock`: drop `(this client, cookie)`; `ENOENT` when
/// it is not a live holder or the object is missing. Unlocking an
/// ephemeral lock deletes the object.
pub fn unlock_op(name: &str, cookie: &str) -> Result<OSDOp> {
    call::op(CLASS, "unlock", &unlock_req(name, cookie))
}

/// `rados::cls::lock::break_lock`: drop `(locker, cookie)`, whoever holds
/// it; `ENOENT` when it is not a live holder or the object is missing.
/// Breaking an ephemeral lock deletes the object.
pub fn break_lock_op(name: &str, cookie: &str, locker: &PackedEntityName) -> Result<OSDOp> {
    call::op(CLASS, "break_lock", &break_req(name, cookie, locker))
}

/// `rados::cls::lock::get_info`: the live holders, type and tag; type
/// `None` when the object has no such lock, `ENOENT` when the object is
/// missing, `EIO` when the lock is ephemeral and its last holder expired.
/// Decode with [`decode_get_info`].
pub fn get_info_op(name: &str) -> Result<OSDOp> {
    call::op(CLASS, "get_info", &get_info_req(name))
}

/// `rados::cls::lock::list_locks`: every lock name on the object,
/// unlocked and expired ones included; `ENOENT` when the object is
/// missing. The request carries no bytes. Decode with
/// [`decode_list_locks`].
pub fn list_locks_op() -> Result<OSDOp> {
    call::raw_op(CLASS, "list_locks", Bytes::new())
}

/// `rados::cls::lock::assert_locked`: `EBUSY` unless `(this client,
/// cookie)` holds `name` with exactly `lock_type` and `tag`; `EINVAL` for
/// type `None` or an empty name; `ENOENT` when the object is missing;
/// `EIO` when the lock is ephemeral and its last holder expired.
/// Inside a compound op a failure fails the whole op.
pub fn assert_locked_op(name: &str, lock_type: LockType, cookie: &str, tag: &str) -> Result<OSDOp> {
    call::op(
        CLASS,
        "assert_locked",
        &assert_req(name, lock_type, cookie, tag),
    )
}

/// `rados::cls::lock::set_cookie`: move `(this client, cookie)` to
/// `new_cookie`, keeping its expiration, address and description. The
/// checks and errors of [`assert_locked_op`], plus `EBUSY` when
/// `new_cookie` is already one of this client's holders.
pub fn set_cookie_op(
    name: &str,
    lock_type: LockType,
    cookie: &str,
    tag: &str,
    new_cookie: &str,
) -> Result<OSDOp> {
    call::op(
        CLASS,
        "set_cookie",
        &set_cookie_req(name, lock_type, cookie, tag, new_cookie),
    )
}

/// Decode the reply to [`get_info_op`].
pub fn decode_get_info(reply: &OpReply) -> Result<GetInfoReply> {
    call::decode(reply)
}

/// Decode the reply to [`list_locks_op`].
pub fn decode_list_locks(reply: &OpReply) -> Result<Vec<String>> {
    Ok(call::decode::<ListLocksReply>(reply)?.locks)
}

fn unlock_req(name: &str, cookie: &str) -> UnlockOp {
    UnlockOp {
        name: name.to_owned(),
        cookie: cookie.to_owned(),
    }
}

fn break_req(name: &str, cookie: &str, locker: &PackedEntityName) -> BreakOp {
    BreakOp {
        name: name.to_owned(),
        locker: locker.clone(),
        cookie: cookie.to_owned(),
    }
}

fn get_info_req(name: &str) -> GetInfoOp {
    GetInfoOp {
        name: name.to_owned(),
    }
}

fn assert_req(name: &str, lock_type: LockType, cookie: &str, tag: &str) -> AssertOp {
    AssertOp {
        name: name.to_owned(),
        lock_type,
        cookie: cookie.to_owned(),
        tag: tag.to_owned(),
    }
}

fn set_cookie_req(
    name: &str,
    lock_type: LockType,
    cookie: &str,
    tag: &str,
    new_cookie: &str,
) -> SetCookieOp {
    SetCookieOp {
        name: name.to_owned(),
        lock_type,
        cookie: cookie.to_owned(),
        tag: tag.to_owned(),
        new_cookie: new_cookie.to_owned(),
    }
}

/// See [`lock_op`].
pub async fn lock(ioctx: &IoCtx, oid: &str, op: &LockOp) -> Result<()> {
    call::exec(ioctx, oid, CLASS, "lock", op).await.map(drop)
}

/// See [`unlock_op`].
pub async fn unlock(ioctx: &IoCtx, oid: &str, name: &str, cookie: &str) -> Result<()> {
    call::exec(ioctx, oid, CLASS, "unlock", &unlock_req(name, cookie))
        .await
        .map(drop)
}

/// See [`break_lock_op`].
pub async fn break_lock(
    ioctx: &IoCtx,
    oid: &str,
    name: &str,
    cookie: &str,
    locker: &PackedEntityName,
) -> Result<()> {
    let req = break_req(name, cookie, locker);
    call::exec(ioctx, oid, CLASS, "break_lock", &req)
        .await
        .map(drop)
}

/// See [`get_info_op`].
pub async fn get_info(ioctx: &IoCtx, oid: &str, name: &str) -> Result<GetInfoReply> {
    let out = call::exec(ioctx, oid, CLASS, "get_info", &get_info_req(name)).await?;
    call::decode_bytes(out)
}

/// See [`list_locks_op`].
pub async fn list_locks(ioctx: &IoCtx, oid: &str) -> Result<Vec<String>> {
    let out = call::exec_raw(ioctx, oid, CLASS, "list_locks", Bytes::new()).await?;
    Ok(call::decode_bytes::<ListLocksReply>(out)?.locks)
}

/// See [`assert_locked_op`].
pub async fn assert_locked(
    ioctx: &IoCtx,
    oid: &str,
    name: &str,
    lock_type: LockType,
    cookie: &str,
    tag: &str,
) -> Result<()> {
    let req = assert_req(name, lock_type, cookie, tag);
    call::exec(ioctx, oid, CLASS, "assert_locked", &req)
        .await
        .map(drop)
}

/// See [`set_cookie_op`].
pub async fn set_cookie(
    ioctx: &IoCtx,
    oid: &str,
    name: &str,
    lock_type: LockType,
    cookie: &str,
    tag: &str,
    new_cookie: &str,
) -> Result<()> {
    let req = set_cookie_req(name, lock_type, cookie, tag, new_cookie);
    call::exec(ioctx, oid, CLASS, "set_cookie", &req)
        .await
        .map(drop)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rados::osdclient::types::OpData;
    use rados::{Denc, EntityAddrType, encode_with_capacity};

    fn bytes<T: Denc>(v: &T) -> Vec<u8> {
        encode_with_capacity(v, 0).expect("encode").to_vec()
    }

    fn json<T: Serialize>(v: &T) -> String {
        serde_json::to_string(v).expect("json")
    }

    /// Bytes from a hex string, as `ceph-dencoder ... encode export` wrote them.
    fn unhex(s: &str) -> Vec<u8> {
        s.as_bytes()
            .chunks(2)
            .map(|pair| {
                u8::from_str_radix(std::str::from_utf8(pair).expect("ascii"), 16).expect("hex")
            })
            .collect()
    }

    /// Encodes to `hex`, decodes back to `v`, and dumps as `expected`.
    fn pin<T: Denc + Serialize + PartialEq + std::fmt::Debug>(v: &T, hex: &str, expected: &str) {
        assert_eq!(bytes(v), unhex(hex));
        assert_eq!(&T::decode(&mut &unhex(hex)[..], 0).expect("decode"), v);
        assert_eq!(json(v), expected);
    }

    fn s(v: &str) -> String {
        v.to_owned()
    }

    fn client(n: u64) -> PackedEntityName {
        PackedEntityName::new(0x08, n)
    }

    fn legacy(addr: &str, nonce: u32) -> EntityAddr {
        EntityAddr {
            nonce,
            ..EntityAddr::from_socket_addr(EntityAddrType::Legacy, addr.parse().expect("addr"))
        }
    }

    fn id(n: u64, cookie: &str) -> LockerId {
        LockerId {
            locker: client(n),
            cookie: s(cookie),
        }
    }

    fn info(sec: u32, addr: EntityAddr, description: &str) -> LockerInfo {
        LockerInfo {
            expiration: UTime::new(sec, 0),
            addr,
            description: s(description),
        }
    }

    const LOCKER_INFO_1: &str = "01013a00000005000000000000000101011c000000010000000100000010000000020000027f00010200000000000000000b0000006465736372697074696f6e";
    const LOCK_OP_1: &str = "010132000000040000006e616d650206000000636f6f6b6965030000007461670b0000006465736372697074696f6e050000000000000001";

    #[test]
    fn locker_id_matches_the_oracle() {
        pin(
            &id(1, "cookie"),
            "01011300000008010000000000000006000000636f6f6b6965",
            r#"{"locker":"client.1","cookie":"cookie"}"#,
        );
        pin(
            &LockerId::default(),
            "01010d00000000000000000000000000000000",
            r#"{"locker":"unknown.0","cookie":""}"#,
        );
    }

    #[test]
    fn locker_info_matches_the_oracle() {
        pin(
            &info(5, legacy("127.0.1.2:2", 1), "description"),
            LOCKER_INFO_1,
            r#"{"expiration":"5.000000","addr":"127.0.1.2:2/1","description":"description"}"#,
        );
        pin(
            &LockerInfo::default(),
            "01013b00000000000000000000000101012800000000000000000000001c0000000000000000000000000000000000000000000000000000000000000000000000",
            r#"{"expiration":"0.000000","addr":"(unrecognized address family 0)/0","description":""}"#,
        );
    }

    #[test]
    fn lock_info_matches_the_oracle() {
        let i = LockInfo {
            lockers: BTreeMap::from([(
                id(1, "cookie"),
                info(5, legacy("127.0.1.2:2", 1), "description"),
            )]),
            lock_type: LockType::Exclusive,
            tag: s("tag"),
        };
        pin(
            &i,
            "0101650000000100000001011300000008010000000000000006000000636f6f6b696501013a00000005000000000000000101011c000000010000000100000010000000020000027f00010200000000000000000b0000006465736372697074696f6e0103000000746167",
            r#"{"lock_type":1,"tag":"tag","lockers":[{"id":{"locker":"client.1","cookie":"cookie"},"info":{"expiration":"5.000000","addr":"127.0.1.2:2/1","description":"description"}}]}"#,
        );
        pin(
            &LockInfo::default(),
            "010109000000000000000000000000",
            r#"{"lock_type":0,"tag":"","lockers":[]}"#,
        );
    }

    #[test]
    fn lock_info_matches_a_corpus_object() {
        // ceph-object-corpus 19.2.0-404-g78ddc7f9027 lock_info_t
        // f4cf81c9b553c2fb2c66e15ebd54a829.
        let i = LockInfo {
            lockers: BTreeMap::from([(
                LockerId {
                    locker: client(4533),
                    cookie: s("j1_EFM8DDt8SI1R"),
                },
                LockerInfo {
                    expiration: UTime::new(1_727_604_086, 460_555_954),
                    addr: legacy("172.21.5.153:0", 1_725_310_796),
                    description: String::new(),
                },
            )]),
            lock_type: LockType::Exclusive,
            tag: String::new(),
        };
        pin(
            &i,
            "0101600000000100000001011c00000008b5110000000000000f0000006a315f45464d38444474385349315201012f0000007625f966b286731b0101011c000000010000004c27d6661000000002000000ac1505990000000000000000000000000100000000",
            r#"{"lock_type":1,"tag":"","lockers":[{"id":{"locker":"client.4533","cookie":"j1_EFM8DDt8SI1R"},"info":{"expiration":"2024-09-29T10:01:26.460555+0000","addr":"172.21.5.153:0/1725310796","description":""}}]}"#,
        );
    }

    #[test]
    fn lock_op_matches_the_oracle() {
        pin(
            &LockOp {
                name: s("name"),
                lock_type: LockType::Shared,
                cookie: s("cookie"),
                tag: s("tag"),
                description: s("description"),
                duration: UTime::new(5, 0),
                flags: LockFlags::MAY_RENEW,
            },
            LOCK_OP_1,
            r#"{"name":"name","type":"shared","cookie":"cookie","tag":"tag","description":"description","duration":"5.000000","flags":1}"#,
        );
        pin(
            &LockOp::default(),
            "01011a0000000000000000000000000000000000000000000000000000000000",
            r#"{"name":"","type":"none","cookie":"","tag":"","description":"","duration":"0.000000","flags":0}"#,
        );
    }

    #[test]
    fn lock_op_with_an_unknown_type_is_refused() {
        let mut wire = unhex(LOCK_OP_1);
        assert_eq!(wire[14], 2);
        wire[14] = 4;
        assert!(LockOp::decode(&mut &wire[..], 0).is_err());
    }

    #[test]
    fn unlock_op_matches_the_oracle() {
        pin(
            &UnlockOp {
                name: s("name"),
                cookie: s("cookie"),
            },
            "010112000000040000006e616d6506000000636f6f6b6965",
            r#"{"name":"name","cookie":"cookie"}"#,
        );
        pin(
            &UnlockOp::default(),
            "0101080000000000000000000000",
            r#"{"name":"","cookie":""}"#,
        );
    }

    #[test]
    fn break_op_matches_the_oracle() {
        pin(
            &BreakOp {
                name: s("name"),
                locker: client(1),
                cookie: s("cookie"),
            },
            "01011b000000040000006e616d6508010000000000000006000000636f6f6b6965",
            r#"{"name":"name","cookie":"cookie","locker":"client.1"}"#,
        );
        pin(
            &BreakOp::default(),
            "0101110000000000000000000000000000000000000000",
            r#"{"name":"","cookie":"","locker":"unknown.0"}"#,
        );
    }

    #[test]
    fn get_info_op_matches_the_oracle() {
        pin(
            &GetInfoOp { name: s("name") },
            "010108000000040000006e616d65",
            r#"{"name":"name"}"#,
        );
        pin(
            &GetInfoOp::default(),
            "01010400000000000000",
            r#"{"name":""}"#,
        );
    }

    const GET_INFO_REPLY_1: &str = "0101c20000000200000001011400000008010000000000000007000000636f6f6b69653101013b0000000a000000000000000101011c000000010000000a00000010000000020000147f00010200000000000000000c0000006465736372697074696f6e3101011400000008020000000000000007000000636f6f6b69653201013b00000014000000000000000101011c000000010000001e00000010000000020000287f00010200000000000000000c0000006465736372697074696f6e320203000000746167";

    fn get_info_reply_1(reverse: bool) -> GetInfoReply {
        let mut entries = vec![
            (
                id(1, "cookie1"),
                info(10, legacy("127.0.1.2:20", 10), "description1"),
            ),
            (
                id(2, "cookie2"),
                info(20, legacy("127.0.1.2:40", 30), "description2"),
            ),
        ];
        if reverse {
            entries.reverse();
        }
        let mut lockers = BTreeMap::new();
        for (k, v) in entries {
            lockers.insert(k, v);
        }
        GetInfoReply {
            lockers,
            lock_type: LockType::Shared,
            tag: s("tag"),
        }
    }

    #[test]
    fn get_info_reply_matches_the_oracle() {
        pin(
            &get_info_reply_1(false),
            GET_INFO_REPLY_1,
            r#"{"lock_type":"shared","tag":"tag","lockers":[{"locker":"client.1","description":"description1","cookie":"cookie1","expiration":"10.000000","addr":"127.0.1.2:20/10"},{"locker":"client.2","description":"description2","cookie":"cookie2","expiration":"20.000000","addr":"127.0.1.2:40/30"}]}"#,
        );
        pin(
            &GetInfoReply::default(),
            "010109000000000000000000000000",
            r#"{"lock_type":"none","tag":"","lockers":[]}"#,
        );
    }

    #[test]
    fn lockers_encode_in_the_osds_order_whatever_the_insertion_order() {
        assert_eq!(bytes(&get_info_reply_1(true)), unhex(GET_INFO_REPLY_1));

        let mut lockers = BTreeMap::new();
        for key in [id(1, "b"), id(1, "a"), id(u64::MAX, "z")] {
            lockers.insert(key, LockerInfo::default());
        }
        let order: Vec<(String, &str)> = lockers
            .keys()
            .map(|k| (k.locker.to_string(), k.cookie.as_str()))
            .collect();
        assert_eq!(
            order,
            [
                (s("client.?"), "z"),
                (s("client.1"), "a"),
                (s("client.1"), "b")
            ]
        );
    }

    #[test]
    fn lock_info_and_get_info_reply_share_the_wire() {
        let reply = get_info_reply_1(false);
        let info = LockInfo::from(reply.clone());
        assert_eq!(bytes(&info), bytes(&reply));
        assert_eq!(GetInfoReply::from(info), reply);
    }

    #[test]
    fn list_locks_reply_matches_the_oracle() {
        pin(
            &ListLocksReply {
                locks: vec![s("lock1"), s("lock2"), s("lock3")],
            },
            "01011f00000003000000050000006c6f636b31050000006c6f636b32050000006c6f636b33",
            r#"{"locks":[["lock1"],["lock2"],["lock3"]]}"#,
        );
        pin(
            &ListLocksReply::default(),
            "01010400000000000000",
            r#"{"locks":[]}"#,
        );
    }

    #[test]
    fn assert_op_matches_the_oracle() {
        pin(
            &AssertOp {
                name: s("name"),
                lock_type: LockType::Shared,
                cookie: s("cookie"),
                tag: s("tag"),
            },
            "01011a000000040000006e616d650206000000636f6f6b696503000000746167",
            r#"{"name":"name","type":"shared","cookie":"cookie","tag":"tag"}"#,
        );
        pin(
            &AssertOp::default(),
            "01010d00000000000000000000000000000000",
            r#"{"name":"","type":"none","cookie":"","tag":""}"#,
        );
    }

    #[test]
    fn set_cookie_op_matches_the_oracle() {
        pin(
            &SetCookieOp {
                name: s("name"),
                lock_type: LockType::Shared,
                cookie: s("cookie"),
                tag: s("tag"),
                new_cookie: s("new cookie"),
            },
            "010128000000040000006e616d650206000000636f6f6b6965030000007461670a0000006e657720636f6f6b6965",
            r#"{"name":"name","type":"shared","cookie":"cookie","tag":"tag","new_cookie":"new cookie"}"#,
        );
        pin(
            &SetCookieOp::default(),
            "0101110000000000000000000000000000000000000000",
            r#"{"name":"","type":"none","cookie":"","tag":"","new_cookie":""}"#,
        );
    }

    fn assert_call(op: &OSDOp, method: &str) {
        let prefix = format!("lock{method}");
        assert!(op.indata.starts_with(prefix.as_bytes()), "{method}");
        match op.op_data {
            OpData::Call {
                class_len,
                method_len,
                ..
            } => {
                assert_eq!(class_len, 4);
                assert_eq!(usize::from(method_len), method.len());
            }
            _ => panic!("{method}: not a CALL"),
        }
    }

    #[test]
    fn ops_name_the_class_and_method() {
        assert_call(&lock_op(&LockOp::default()).expect("op"), "lock");
        assert_call(&unlock_op("n", "c").expect("op"), "unlock");
        assert_call(
            &break_lock_op("n", "c", &client(1)).expect("op"),
            "break_lock",
        );
        assert_call(&get_info_op("n").expect("op"), "get_info");
        assert_call(
            &assert_locked_op("n", LockType::Exclusive, "c", "").expect("op"),
            "assert_locked",
        );
        assert_call(
            &set_cookie_op("n", LockType::Exclusive, "c", "", "d").expect("op"),
            "set_cookie",
        );
        let op = list_locks_op().expect("op");
        assert_call(&op, "list_locks");
        assert_eq!(&op.indata[..], b"locklist_locks");
        assert!(matches!(op.op_data, OpData::Call { indata_len: 0, .. }));
    }

    #[test]
    fn ops_carry_the_request_after_the_names() {
        let op = unlock_op("name", "cookie").expect("op");
        assert_eq!(
            &op.indata[b"lockunlock".len()..],
            &unhex("010112000000040000006e616d6506000000636f6f6b6965")[..]
        );
    }

    #[test]
    fn decoders_unwrap_the_replies() {
        let reply = OpReply {
            return_code: 0,
            outdata: Bytes::from(unhex(GET_INFO_REPLY_1)),
        };
        assert_eq!(
            decode_get_info(&reply).expect("decode"),
            get_info_reply_1(false)
        );
        let reply = OpReply {
            return_code: 0,
            outdata: Bytes::from(unhex(
                "01011f00000003000000050000006c6f636b31050000006c6f636b32050000006c6f636b33",
            )),
        };
        assert_eq!(
            decode_list_locks(&reply).expect("decode"),
            [s("lock1"), s("lock2"), s("lock3")]
        );
    }

    #[test]
    fn xattr_name_prefixes_lock() {
        assert_eq!(xattr_name("gc_process"), "lock.gc_process");
    }

    #[test]
    fn lock_type_strings() {
        assert_eq!(lock_type_str(LockType::None), "none");
        assert_eq!(lock_type_str(LockType::Exclusive), "exclusive");
        assert_eq!(lock_type_str(LockType::Shared), "shared");
        assert_eq!(
            lock_type_str(LockType::ExclusiveEphemeral),
            "exclusive-ephemeral"
        );
    }
}
