//! The `queue` object class (`cls_queue_client.h`): a ring of opaque
//! entries inside one object. The head (1 KiB plus the urgent-data
//! allowance) sits at offset zero; entries follow, each ten bytes of
//! magic and length ahead of its payload; `front` and `tail` markers
//! delimit the live region and `gen` counts wraps. `queue_init`,
//! `queue_enqueue` and `queue_remove_entries` are RD|WR methods;
//! `queue_get_capacity` and `queue_list_entries` are RD.

use std::fmt;

use bytes::{Buf, BufMut, Bytes};
use rados::osdclient::error::Result;
use rados::osdclient::{IoCtx, OSDOp, OpReply};
use rados::{Denc, RadosError, VersionedDenc, VersionedEncode};
use serde::Serialize;
use serde::ser::SerializeStruct;

use crate::call;

pub const CLASS: &str = "queue";

/// `QUEUE_HEAD_SIZE_1K`: the head's size before any urgent-data allowance,
/// and so the first entry's offset when `max_urgent_data_size` is zero
/// (the class adds `max_urgent_data_size` on top).
pub const HEAD_SIZE_1K: u64 = 1024;

/// `QUEUE_ENTRY_OVERHEAD`: the `u16` magic and `u64` length ahead of
/// every payload in the ring.
pub const ENTRY_OVERHEAD: u64 = 10;

/// `cls_queue_entry`: one payload and the marker naming its slot. The dump
/// carries the marker and the payload's length as `data_len`.
#[derive(Debug, Clone, Default, PartialEq, Eq, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct Entry {
    pub data: Bytes,
    pub marker: String,
}

impl Serialize for Entry {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("Entry", 2)?;
        state.serialize_field("marker", &self.marker)?;
        state.serialize_field("data_len", &(self.data.len() as u64))?;
        state.end()
    }
}

/// `cls_queue_marker`: a position in the ring, printed as `gen/offset`.
/// The wire order is `gen` then `offset`, the reverse of the declaration
/// and of the dump. `gen` on the wire and in the dump; `generation` here
/// because `gen` is a keyword.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Marker {
    pub offset: u64,
    #[serde(rename = "gen")]
    pub generation: u64,
}

impl VersionedEncode for Marker {
    const MAX_DECODE_VERSION: u8 = 1;

    fn encoding_version(&self, _features: u64) -> u8 {
        1
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
        self.generation.encode(buf, features)?;
        self.offset.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 1, "Marker", "Squid v19+");

        let generation = u64::decode(buf, features)?;
        let offset = u64::decode(buf, features)?;
        Ok(Self { offset, generation })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        Some(16)
    }
}

rados::impl_denc_for_versioned!(Marker);

impl fmt::Display for Marker {
    /// `cls_queue_marker::to_str`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.generation, self.offset)
    }
}

/// `cls_queue_head`: the ring's bookkeeping, stored at offset zero of the
/// object. The dump flattens the two markers and leaves the urgent data
/// out.
#[derive(Debug, Clone, PartialEq, Eq, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct Head {
    pub max_head_size: u64,
    pub front: Marker,
    pub tail: Marker,
    pub queue_size: u64,
    pub max_urgent_data_size: u64,
    pub urgent_data: Bytes,
}

impl Default for Head {
    fn default() -> Self {
        let start = Marker {
            offset: HEAD_SIZE_1K,
            generation: 0,
        };
        Self {
            max_head_size: HEAD_SIZE_1K,
            front: start,
            tail: start,
            queue_size: 0,
            max_urgent_data_size: 0,
            urgent_data: Bytes::new(),
        }
    }
}

impl Serialize for Head {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("Head", 7)?;
        state.serialize_field("max_head_size", &self.max_head_size)?;
        state.serialize_field("queue_size", &self.queue_size)?;
        state.serialize_field("max_urgent_data_size", &self.max_urgent_data_size)?;
        state.serialize_field("front_offset", &self.front.offset)?;
        state.serialize_field("front_gen", &self.front.generation)?;
        state.serialize_field("tail_offset", &self.tail.offset)?;
        state.serialize_field("tail_gen", &self.tail.generation)?;
        state.end()
    }
}

/// `cls_queue_init_op`: `queue_size` usable bytes (the class adds the head
/// on top), an urgent-data allowance and its initial contents. The dump
/// prints the contents' length as `urgent_data_len`.
#[derive(Debug, Clone, Default, PartialEq, Eq, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct InitOp {
    pub queue_size: u64,
    pub max_urgent_data_size: u64,
    pub urgent_data: Bytes,
}

impl Serialize for InitOp {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("InitOp", 3)?;
        state.serialize_field("queue_size", &self.queue_size)?;
        state.serialize_field("max_urgent_data_size", &self.max_urgent_data_size)?;
        state.serialize_field("urgent_data_len", &(self.urgent_data.len() as u64))?;
        state.end()
    }
}

/// `cls_queue_enqueue_op`: the payloads to append, one entry each. The
/// dump prints only their count as `data_vec_len`.
#[derive(Debug, Clone, Default, PartialEq, Eq, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct EnqueueOp {
    pub data: Vec<Bytes>,
}

impl Serialize for EnqueueOp {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("EnqueueOp", 1)?;
        state.serialize_field("data_vec_len", &(self.data.len() as u64))?;
        state.end()
    }
}

/// `cls_queue_list_op`: up to `max` entries from `start_marker` (empty
/// for the front), stopping before `end_marker` when it is set. Version 2
/// added `end_marker`; the version-1 branch stays because the 18.2.0
/// corpus carries version-1 samples without it, and the dump leaves it
/// out.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ListOp {
    pub max: u64,
    pub start_marker: String,
    #[serde(skip)]
    pub end_marker: String,
}

impl VersionedEncode for ListOp {
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
        self.max.encode(buf, features)?;
        self.start_marker.encode(buf, features)?;
        self.end_marker.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 1, "ListOp", "Reef v18+");

        let max = u64::decode(buf, features)?;
        let start_marker = String::decode(buf, features)?;
        let end_marker = if struct_v > 1 {
            String::decode(buf, features)?
        } else {
            String::new()
        };
        Ok(Self {
            max,
            start_marker,
            end_marker,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

rados::impl_denc_for_versioned!(ListOp);

/// `cls_queue_list_ret`. `next_marker` is where the next page starts, or
/// the tail when `is_truncated` is false.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct ListRet {
    pub is_truncated: bool,
    pub next_marker: String,
    pub entries: Vec<Entry>,
}

/// `cls_queue_remove_op`: everything before `end_marker` goes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct RemoveOp {
    pub end_marker: String,
}

/// `cls_queue_get_capacity_ret`: the usable bytes, `queue_size` less the head.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct GetCapacityRet {
    pub queue_capacity: u64,
}

/// `cls_queue_get_stats_ret`, the `2pc_queue` class's topic statistics:
/// the ring's used bytes (entry overheads included) and its committed
/// entry count. Ceph gives it no dump and does not register it with
/// `ceph-dencoder`.
#[derive(Debug, Clone, Default, PartialEq, Eq, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct GetStatsRet {
    pub queue_size: u64,
    pub queue_entries: u32,
}

/// `cls_queue_init`: lay out a ring of `size` usable bytes with no urgent
/// data. `EEXIST` if the object already holds a head.
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
pub fn init_op(size: u64) -> Result<OSDOp> {
    call::op(
        CLASS,
        "queue_init",
        &InitOp {
            queue_size: size,
            ..InitOp::default()
        },
    )
}

/// `cls_queue_get_capacity`: the op; decode its reply with
/// [`decode_get_capacity`].
pub fn get_capacity_op() -> Result<OSDOp> {
    call::raw_op(CLASS, "queue_get_capacity", Bytes::new())
}

/// Decode the reply to [`get_capacity_op`].
pub fn decode_get_capacity(reply: &OpReply) -> Result<u64> {
    Ok(call::decode::<GetCapacityRet>(reply)?.queue_capacity)
}

/// `cls_queue_enqueue`: append each payload as one entry, wrapping past
/// the end of the ring. `ENOSPC` when they do not all fit.
pub fn enqueue_op(data: Vec<Bytes>) -> Result<OSDOp> {
    call::op(CLASS, "queue_enqueue", &EnqueueOp { data })
}

/// `cls_queue_list_entries`: up to `max` entries from `start_marker`
/// (empty for the front), stopping before `end_marker` when it is not
/// empty. The C++ end-marker overload passes `u64::MAX` for `max`. Decode
/// the reply with [`decode_list`].
pub fn list_op(start_marker: &str, max: u64, end_marker: &str) -> Result<OSDOp> {
    call::op(
        CLASS,
        "queue_list_entries",
        &ListOp {
            max,
            start_marker: start_marker.to_owned(),
            end_marker: end_marker.to_owned(),
        },
    )
}

/// Decode the reply to [`list_op`].
pub fn decode_list(reply: &OpReply) -> Result<ListRet> {
    call::decode(reply)
}

/// `cls_queue_remove_entries`: move the front to `end_marker`, which must
/// be the front itself or ahead of it within one wrap; anything else is
/// `EINVAL`. A no-op on an empty ring.
pub fn remove_entries_op(end_marker: &str) -> Result<OSDOp> {
    call::op(
        CLASS,
        "queue_remove_entries",
        &RemoveOp {
            end_marker: end_marker.to_owned(),
        },
    )
}

/// Lay out a ring of `size` usable bytes on `oid`; see [`init_op`].
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
pub async fn init(ioctx: &IoCtx, oid: &str, size: u64) -> Result<()> {
    call::exec(
        ioctx,
        oid,
        CLASS,
        "queue_init",
        &InitOp {
            queue_size: size,
            ..InitOp::default()
        },
    )
    .await?;
    Ok(())
}

/// The usable bytes of the ring on `oid`.
pub async fn get_capacity(ioctx: &IoCtx, oid: &str) -> Result<u64> {
    let out = call::exec_raw(ioctx, oid, CLASS, "queue_get_capacity", Bytes::new()).await?;
    Ok(call::decode_bytes::<GetCapacityRet>(out)?.queue_capacity)
}

/// Append `data` to the ring on `oid`; see [`enqueue_op`].
pub async fn enqueue(ioctx: &IoCtx, oid: &str, data: Vec<Bytes>) -> Result<()> {
    call::exec(ioctx, oid, CLASS, "queue_enqueue", &EnqueueOp { data }).await?;
    Ok(())
}

/// List entries of the ring on `oid`; see [`list_op`].
pub async fn list_entries(
    ioctx: &IoCtx,
    oid: &str,
    start_marker: &str,
    max: u64,
    end_marker: &str,
) -> Result<ListRet> {
    let out = call::exec(
        ioctx,
        oid,
        CLASS,
        "queue_list_entries",
        &ListOp {
            max,
            start_marker: start_marker.to_owned(),
            end_marker: end_marker.to_owned(),
        },
    )
    .await?;
    call::decode_bytes(out)
}

/// Drop everything before `end_marker` from the ring on `oid`; see
/// [`remove_entries_op`].
pub async fn remove_entries(ioctx: &IoCtx, oid: &str, end_marker: &str) -> Result<()> {
    call::exec(
        ioctx,
        oid,
        CLASS,
        "queue_remove_entries",
        &RemoveOp {
            end_marker: end_marker.to_owned(),
        },
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rados::encode_with_capacity;
    use rados::osdclient::types::OpData;

    fn bytes<T: Denc>(v: &T) -> Vec<u8> {
        encode_with_capacity(v, 0).expect("encode").to_vec()
    }

    fn json<T: Serialize>(v: &T) -> String {
        serde_json::to_string(v).expect("json")
    }

    #[test]
    fn marker_encodes_gen_before_offset() {
        // Corpus cls_queue_marker/fe7a542e...: gen 0, offset 745307.
        let m = Marker {
            offset: 745_307,
            generation: 0,
        };
        assert_eq!(
            bytes(&m),
            [
                1u8, 1, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x5b, 0x5f, 0x0b, 0, 0, 0, 0, 0
            ]
        );
        assert_eq!(Marker::decode(&mut &bytes(&m)[..], 0).expect("decode"), m);
        assert_eq!(m.to_string(), "0/745307");
        assert_eq!(json(&m), r#"{"offset":745307,"gen":0}"#);
    }

    #[test]
    fn head_default_matches_the_corpus_instance() {
        // Corpus cls_queue_head/18dc7514...: a 1 KiB head, both markers at
        // its end, nothing else set; 78 bytes.
        let wire: Vec<u8> = [
            &[1u8, 1, 72, 0, 0, 0][..],
            &1024u64.to_le_bytes(),
            &[
                1, 1, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0,
            ],
            &[
                1, 1, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0,
            ],
            &0u64.to_le_bytes(),
            &0u64.to_le_bytes(),
            &[0, 0, 0, 0],
        ]
        .concat();
        assert_eq!(bytes(&Head::default()), wire);
        assert_eq!(
            Head::decode(&mut &wire[..], 0).expect("decode"),
            Head::default()
        );
        assert_eq!(
            json(&Head::default()),
            r#"{"max_head_size":1024,"queue_size":0,"max_urgent_data_size":0,"front_offset":1024,"front_gen":0,"tail_offset":1024,"tail_gen":0}"#
        );
    }

    #[test]
    fn entry_and_init_and_enqueue_dump_lengths_not_payloads() {
        // Corpus cls_queue_entry/9f633d96...: data "data", marker "marker".
        let e = Entry {
            data: Bytes::from_static(b"data"),
            marker: "marker".to_owned(),
        };
        assert_eq!(
            bytes(&e),
            b"\x01\x01\x12\x00\x00\x00\x04\x00\x00\x00data\x06\x00\x00\x00marker"
        );
        assert_eq!(json(&e), r#"{"marker":"marker","data_len":4}"#);

        // Corpus cls_queue_init_op/6202a90f...: 1024, 1024, "data".
        let i = InitOp {
            queue_size: 1024,
            max_urgent_data_size: 1024,
            urgent_data: Bytes::from_static(b"data"),
        };
        assert_eq!(
            bytes(&i),
            b"\x01\x01\x18\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00data"
        );
        assert_eq!(
            json(&i),
            r#"{"queue_size":1024,"max_urgent_data_size":1024,"urgent_data_len":4}"#
        );

        // Corpus cls_queue_enqueue_op/c1504f3f...: one payload "data".
        let q = EnqueueOp {
            data: vec![Bytes::from_static(b"data")],
        };
        assert_eq!(
            bytes(&q),
            b"\x01\x01\x0c\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00data"
        );
        assert_eq!(json(&q), r#"{"data_vec_len":1}"#);
    }

    #[test]
    fn list_op_reads_reef_v1_and_writes_v2() {
        // Corpus 18.2.0 cls_queue_list_op: version 1, max 128, no end_marker.
        let v1 = b"\x01\x01\x0c\x00\x00\x00\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";
        let op = ListOp::decode(&mut &v1[..], 0).expect("decode");
        assert_eq!(
            op,
            ListOp {
                max: 128,
                start_marker: String::new(),
                end_marker: String::new(),
            }
        );
        assert_eq!(
            bytes(&op),
            b"\x02\x01\x10\x00\x00\x00\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        );
        assert_eq!(json(&op), r#"{"max":128,"start_marker":""}"#);
    }

    #[test]
    fn list_ret_dumps_a_real_bool_and_the_entries() {
        let ret = ListRet {
            is_truncated: true,
            next_marker: "foo".to_owned(),
            entries: vec![
                Entry::default(),
                Entry {
                    data: Bytes::from_static(b"data"),
                    marker: "id".to_owned(),
                },
            ],
        };
        assert_eq!(
            json(&ret),
            r#"{"is_truncated":true,"next_marker":"foo","entries":[{"marker":"","data_len":0},{"marker":"id","data_len":4}]}"#
        );
        assert_eq!(
            ListRet::decode(&mut &bytes(&ret)[..], 0).expect("decode"),
            ret
        );
    }

    #[test]
    fn remove_and_capacity_are_one_field_each() {
        // Corpus cls_queue_remove_op/988d6dc7... and cls_queue_get_capacity_ret/01d854f6....
        let r = RemoveOp {
            end_marker: "0/145919".to_owned(),
        };
        assert_eq!(
            bytes(&r),
            b"\x01\x01\x0c\x00\x00\x00\x08\x00\x00\x000/145919"
        );
        assert_eq!(json(&r), r#"{"end_marker":"0/145919"}"#);
        let c = GetCapacityRet {
            queue_capacity: 342,
        };
        assert_eq!(
            bytes(&c),
            b"\x01\x01\x08\x00\x00\x00\x56\x01\x00\x00\x00\x00\x00\x00"
        );
        assert_eq!(json(&c), r#"{"queue_capacity":342}"#);
    }

    #[test]
    fn ops_name_the_class_and_method() {
        let op = init_op(4096).expect("op");
        assert!(op.indata.starts_with(b"queuequeue_init"));
        assert!(matches!(
            op.op_data,
            OpData::Call {
                class_len: 5,
                method_len: 10,
                ..
            }
        ));
        let op = get_capacity_op().expect("op");
        assert!(op.indata.starts_with(b"queuequeue_get_capacity"));
        assert!(matches!(op.op_data, OpData::Call { indata_len: 0, .. }));
        let op = list_op("0/1024", 7, "").expect("op");
        assert!(op.indata.starts_with(b"queuequeue_list_entries"));
        let op = remove_entries_op("0/1037").expect("op");
        assert!(op.indata.starts_with(b"queuequeue_remove_entries"));
    }

    #[test]
    fn decoders_unwrap_the_replies() {
        let reply = rados::OpReply {
            return_code: 0,
            outdata: encode_with_capacity(&GetCapacityRet { queue_capacity: 9 }, 0)
                .expect("encode"),
        };
        assert_eq!(decode_get_capacity(&reply).expect("decode"), 9);
        let ret = ListRet {
            is_truncated: false,
            next_marker: "0/1065".to_owned(),
            entries: vec![],
        };
        let reply = rados::OpReply {
            return_code: 0,
            outdata: encode_with_capacity(&ret, 0).expect("encode"),
        };
        assert_eq!(decode_list(&reply).expect("decode"), ret);
    }

    #[test]
    fn get_stats_ret_matches_the_corpus() {
        // Derived: v19.2.2's ceph-dencoder does not register this type.
        // Corpus 19.2.0 cls_queue_get_stats_ret/bf93a2b1....
        let s = GetStatsRet {
            queue_size: 19_762,
            queue_entries: 782,
        };
        let wire = b"\x01\x01\x0c\x00\x00\x00\x32\x4d\x00\x00\x00\x00\x00\x00\x0e\x03\x00\x00";
        assert_eq!(bytes(&s), wire);
        assert_eq!(GetStatsRet::decode(&mut &wire[..], 0).expect("decode"), s);
    }
}
