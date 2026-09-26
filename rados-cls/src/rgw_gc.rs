//! The `rgw_gc` object class (`cls_rgw_gc_client.h`): RGW's garbage
//! collection queue. Entries are [`GcObjInfo`]s in a `queue` ring whose
//! head carries [`UrgentData`], the deferrals. `rgw_gc_queue_init`,
//! `rgw_gc_queue_enqueue`, `rgw_gc_queue_remove_entries` and
//! `rgw_gc_queue_update_entry` (the defer) are RD|WR methods;
//! `rgw_gc_queue_list_entries` is RD. Capacity is read with the `queue`
//! class's own method.
//!
//! What `cls_rgw_gc.cc` does with them: an entry falls due at enqueue
//! time plus `expiration_secs`; a listing skips any entry whose tag is
//! deferred to a later time and, with `expired_only`, any entry not yet
//! due; a removal walks the first `num_entries` entries the class does
//! not treat as deferred and moves the front past them, dropping a
//! deferral whose time equals its entry's; a defer records the tag's
//! new due time without touching the ring and is `ENOSPC` once more
//! tags are deferred than the init allowed.

use std::collections::BTreeMap;

use rados::osdclient::error::Result;
use rados::osdclient::{IoCtx, OSDOp, OpReply};
use rados::{UTime, VersionedDenc};
use serde::Serialize;
use serde::ser::{SerializeMap, SerializeStruct};

use crate::call;
use crate::rgw::gc::SetEntryOp;
use crate::rgw::types::GcObjInfo;

pub use crate::queue::{decode_get_capacity, get_capacity, get_capacity_op};
// Callers name a GC listing's op and reply as `rgw_gc::ListOp`/`ListRet`,
// the same way they name every other rgw_gc type; the queue class's own
// ListOp/ListRet stay at their own path since callers there reach them
// through `queue::`.
pub use crate::rgw::gc::{ListOp, ListRet};

pub const CLASS: &str = "rgw_gc";

/// `GC_LIST_DEFAULT_MAX`: what the class uses when a list or remove asks
/// for zero.
pub const LIST_DEFAULT_MAX: u32 = 128;

/// `cls_rgw_gc_urgent_data`: deferred tags and their new due times, plus
/// the allowance from the init and how many live in the head and in the
/// `cls_queue_urgent_data` xattr. The class inits the head with no room
/// for them, so every deferral lands in the xattr. Ceph keeps the map
/// unordered, so a re-encode of two or more entries may order them
/// differently from the bytes Ceph wrote. The dump prints each tag as its
/// own value.
#[derive(Debug, Clone, Default, PartialEq, Eq, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct UrgentData {
    pub urgent_data_map: BTreeMap<String, UTime>,
    pub num_urgent_data_entries: u32,
    pub num_head_urgent_entries: u32,
    pub num_xattr_urgent_entries: u32,
}

struct TagsAsValues<'a>(&'a BTreeMap<String, UTime>);

impl Serialize for TagsAsValues<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for tag in self.0.keys() {
            map.serialize_entry(tag, tag)?;
        }
        map.end()
    }
}

impl Serialize for UrgentData {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("UrgentData", 4)?;
        state.serialize_field("urgent_data_map", &TagsAsValues(&self.urgent_data_map))?;
        state.serialize_field("num_urgent_data_entries", &self.num_urgent_data_entries)?;
        state.serialize_field("num_head_urgent_entries", &self.num_head_urgent_entries)?;
        state.serialize_field("num_xattr_urgent_entries", &self.num_xattr_urgent_entries)?;
        state.end()
    }
}

/// `cls_rgw_gc_queue_init_op`: `size` usable bytes and how many tags may
/// be deferred at once.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct InitOp {
    pub size: u64,
    pub num_deferred_entries: u64,
}

/// `cls_rgw_gc_queue_remove_entries_op`. Not registered with
/// `ceph-dencoder`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct RemoveEntriesOp {
    pub num_entries: u64,
}

/// `cls_rgw_gc_queue_defer_entry_op`: the entry to defer, whole, and its
/// new delay. Not registered with `ceph-dencoder`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct DeferEntryOp {
    pub expiration_secs: u32,
    pub info: GcObjInfo,
}

/// `cls_rgw_gc_queue_init`: a ring of `size` usable bytes that may hold
/// `num_deferred_entries` deferrals. `EEXIST` if the object already
/// holds a ring.
///
/// A missing object is created: a writing class call on a missing object
/// reads zero bytes, which `queue_read_head` answers with `EINVAL` (the
/// `ret == 0` branch, `cls_queue_src.cc:57-60` at v19.2.2) and
/// `queue_init` takes as uninitialised. `queue_init` already answers
/// `EEXIST` for an object holding a queue head; what `create(true)` in
/// front of the init in one compound op adds is `EEXIST` for an existing
/// object that is not a queue, which init would otherwise overwrite (its
/// head read answers `EINVAL` on bad magic or a failed decode, and init
/// treats that as uninitialised). The upstream tests use
/// `op.create(true)`, here `OpBuilder::new().create(true).op(init_op(..)?)`;
/// `RGWGC::initialize` puts `op.create(false)` before `gc_log_init2`,
/// which adds neither.
pub fn init_op(size: u64, num_deferred_entries: u64) -> Result<OSDOp> {
    call::op(
        CLASS,
        "rgw_gc_queue_init",
        &InitOp {
            size,
            num_deferred_entries,
        },
    )
}

/// `cls_rgw_gc_queue_enqueue`: append `info`, due `expiration_secs` from
/// now (the class sets `info.time` itself). `ENOSPC` when it does not fit.
pub fn enqueue_op(expiration_secs: u32, info: &GcObjInfo) -> Result<OSDOp> {
    call::op(
        CLASS,
        "rgw_gc_queue_enqueue",
        &SetEntryOp {
            expiration_secs,
            info: info.clone(),
        },
    )
}

/// `cls_rgw_gc_queue_list_entries`: up to `max` entries (zero means
/// [`LIST_DEFAULT_MAX`]) from `marker`, only the due ones when
/// `expired_only`, never one deferred to a later time. Decode the reply
/// with [`decode_list_entries`]; its `next_marker` is set only when
/// `truncated`.
pub fn list_entries_op(marker: &str, max: u32, expired_only: bool) -> Result<OSDOp> {
    call::op(
        CLASS,
        "rgw_gc_queue_list_entries",
        &ListOp {
            marker: marker.to_owned(),
            max,
            expired_only,
        },
    )
}

/// Decode the reply to [`list_entries_op`].
pub fn decode_list_entries(reply: &OpReply) -> Result<ListRet> {
    call::decode(reply)
}

/// `cls_rgw_gc_queue_remove_entries`: drop the first `num_entries`
/// entries (zero means [`LIST_DEFAULT_MAX`]) that are not deferred past
/// their time, and whatever deferred entries lie among them.
pub fn remove_entries_op(num_entries: u64) -> Result<OSDOp> {
    call::op(
        CLASS,
        "rgw_gc_queue_remove_entries",
        &RemoveEntriesOp { num_entries },
    )
}

/// `cls_rgw_gc_queue_defer_entry`: make `info`'s tag due `expiration_secs`
/// from now without re-queueing it. `ENOSPC` once more tags are deferred
/// than the init allowed; deferring a tag again only moves its time.
pub fn defer_entry_op(expiration_secs: u32, info: &GcObjInfo) -> Result<OSDOp> {
    call::op(
        CLASS,
        "rgw_gc_queue_update_entry",
        &DeferEntryOp {
            expiration_secs,
            info: info.clone(),
        },
    )
}

/// Lay out the GC ring on `oid`; see [`init_op`].
///
/// A missing object is created: a writing class call on a missing object
/// reads zero bytes, which `queue_read_head` answers with `EINVAL` (the
/// `ret == 0` branch, `cls_queue_src.cc:57-60` at v19.2.2) and
/// `queue_init` takes as uninitialised. `queue_init` already answers
/// `EEXIST` for an object holding a queue head; what `create(true)` in
/// front of the init in one compound op adds is `EEXIST` for an existing
/// object that is not a queue, which init would otherwise overwrite (its
/// head read answers `EINVAL` on bad magic or a failed decode, and init
/// treats that as uninitialised). The upstream tests use
/// `op.create(true)`, here `OpBuilder::new().create(true).op(init_op(..)?)`;
/// `RGWGC::initialize` puts `op.create(false)` before `gc_log_init2`,
/// which adds neither.
pub async fn init(ioctx: &IoCtx, oid: &str, size: u64, num_deferred_entries: u64) -> Result<()> {
    call::exec(
        ioctx,
        oid,
        CLASS,
        "rgw_gc_queue_init",
        &InitOp {
            size,
            num_deferred_entries,
        },
    )
    .await?;
    Ok(())
}

/// Append `info` to the GC ring on `oid`; see [`enqueue_op`].
pub async fn enqueue(
    ioctx: &IoCtx,
    oid: &str,
    expiration_secs: u32,
    info: &GcObjInfo,
) -> Result<()> {
    call::exec(
        ioctx,
        oid,
        CLASS,
        "rgw_gc_queue_enqueue",
        &SetEntryOp {
            expiration_secs,
            info: info.clone(),
        },
    )
    .await?;
    Ok(())
}

/// List GC entries on `oid`; see [`list_entries_op`].
pub async fn list_entries(
    ioctx: &IoCtx,
    oid: &str,
    marker: &str,
    max: u32,
    expired_only: bool,
) -> Result<ListRet> {
    let out = call::exec(
        ioctx,
        oid,
        CLASS,
        "rgw_gc_queue_list_entries",
        &ListOp {
            marker: marker.to_owned(),
            max,
            expired_only,
        },
    )
    .await?;
    call::decode_bytes(out)
}

/// Drop the first `num_entries` GC entries on `oid`; see
/// [`remove_entries_op`].
pub async fn remove_entries(ioctx: &IoCtx, oid: &str, num_entries: u64) -> Result<()> {
    call::exec(
        ioctx,
        oid,
        CLASS,
        "rgw_gc_queue_remove_entries",
        &RemoveEntriesOp { num_entries },
    )
    .await?;
    Ok(())
}

/// Defer `info`'s tag on `oid`; see [`defer_entry_op`].
pub async fn defer_entry(
    ioctx: &IoCtx,
    oid: &str,
    expiration_secs: u32,
    info: &GcObjInfo,
) -> Result<()> {
    call::exec(
        ioctx,
        oid,
        CLASS,
        "rgw_gc_queue_update_entry",
        &DeferEntryOp {
            expiration_secs,
            info: info.clone(),
        },
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rados::Denc;
    use rados::encode_with_capacity;
    use rados::osdclient::types::OpData;

    fn bytes<T: Denc>(v: &T) -> Vec<u8> {
        encode_with_capacity(v, 0).expect("encode").to_vec()
    }

    fn json<T: Serialize>(v: &T) -> String {
        serde_json::to_string(v).expect("json")
    }

    #[test]
    fn urgent_data_dumps_each_tag_as_its_own_value() {
        // Corpus cls_rgw_gc_urgent_data/75025f3e...: empty map, 10, 0, 0.
        let u = UrgentData {
            num_urgent_data_entries: 10,
            ..UrgentData::default()
        };
        assert_eq!(
            bytes(&u),
            b"\x01\x01\x10\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        );
        assert_eq!(
            json(&u),
            r#"{"urgent_data_map":{},"num_urgent_data_entries":10,"num_head_urgent_entries":0,"num_xattr_urgent_entries":0}"#
        );
        let mut u = UrgentData::default();
        u.urgent_data_map
            .insert("chain-1".to_owned(), UTime { sec: 7, nsec: 0 });
        assert_eq!(
            bytes(&u),
            b"\x01\x01\x23\x00\x00\x00\x01\x00\x00\x00\x07\x00\x00\x00chain-1\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        );
        assert!(json(&u).starts_with(r#"{"urgent_data_map":{"chain-1":"chain-1"}"#));
        assert_eq!(
            UrgentData::decode(&mut &bytes(&u)[..], 0).expect("decode"),
            u
        );
    }

    #[test]
    fn init_remove_and_defer_are_plain_structs() {
        // Corpus cls_rgw_gc_queue_init_op/400582ba...: 344, 10.
        let i = InitOp {
            size: 344,
            num_deferred_entries: 10,
        };
        assert_eq!(
            bytes(&i),
            b"\x01\x01\x10\x00\x00\x00\x58\x01\x00\x00\x00\x00\x00\x00\x0a\x00\x00\x00\x00\x00\x00\x00"
        );
        assert_eq!(json(&i), r#"{"size":344,"num_deferred_entries":10}"#);
        let r = RemoveEntriesOp { num_entries: 2 };
        assert_eq!(
            bytes(&r),
            b"\x01\x01\x08\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00"
        );
        let d = DeferEntryOp {
            expiration_secs: 10,
            info: GcObjInfo::default(),
        };
        assert_eq!(
            bytes(&d),
            b"\x01\x01\x20\x00\x00\x00\x0a\x00\x00\x00\x01\x01\x16\x00\x00\x00\x00\x00\x00\x00\x01\x01\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        );
        assert_eq!(
            DeferEntryOp::decode(&mut &bytes(&d)[..], 0).expect("decode"),
            d
        );
    }

    #[test]
    fn ops_name_the_class_and_method() {
        let op = init_op(4096, 10).expect("op");
        assert!(op.indata.starts_with(b"rgw_gcrgw_gc_queue_init"));
        assert!(matches!(
            op.op_data,
            OpData::Call {
                class_len: 6,
                method_len: 17,
                ..
            }
        ));
        let op = enqueue_op(0, &GcObjInfo::default()).expect("op");
        assert!(op.indata.starts_with(b"rgw_gcrgw_gc_queue_enqueue"));
        let op = list_entries_op("", 0, true).expect("op");
        assert!(op.indata.starts_with(b"rgw_gcrgw_gc_queue_list_entries"));
        let op = remove_entries_op(1).expect("op");
        assert!(op.indata.starts_with(b"rgw_gcrgw_gc_queue_remove_entries"));
        let op = defer_entry_op(10, &GcObjInfo::default()).expect("op");
        assert!(op.indata.starts_with(b"rgw_gcrgw_gc_queue_update_entry"));
        // get_capacity is the queue class's method on the same object.
        let op = get_capacity_op().expect("op");
        assert!(op.indata.starts_with(b"queuequeue_get_capacity"));
    }

    #[test]
    fn decoder_unwraps_the_list_reply() {
        let ret = ListRet {
            entries: vec![GcObjInfo::default()],
            next_marker: "0/1037".to_owned(),
            truncated: true,
        };
        let reply = rados::OpReply {
            return_code: 0,
            outdata: encode_with_capacity(&ret, 0).expect("encode"),
        };
        assert_eq!(decode_list_entries(&reply).expect("decode"), ret);
    }
}
