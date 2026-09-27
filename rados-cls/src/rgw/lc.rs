//! Lifecycle: from `cls_rgw_types.h`, the entry and the per-shard head;
//! from `cls_rgw_ops.h`, the request and reply structs of the `rgw`
//! class's `lc_*` methods; and, as `cls_rgw_client.h` has them, those
//! methods' op constructors and async calls.
//!
//! Server facts (Ceph v19 `cls_rgw.cc`): a lifecycle shard is an omap
//! whose header is the encoded [`LcObjHead`] and whose keys are bucket
//! keys (`tenant:name:marker`) with [`LcEntry`] values. [`get_head`]
//! answers a default head while the header is empty and `EINVAL` when it
//! does not decode. [`set_entry`] writes the entry under its bucket,
//! creating the shard if needed; [`rm_entry`] removes that key and does
//! not mind an absent one. [`get_entry`] is `ENOENT` for an absent key;
//! [`get_next_entry`] answers the first entry after the marker, or a
//! default entry (empty `bucket`) when none follows. [`list`] reads up to
//! `max_entries` keys after the marker, taking a legacy
//! `pair<string, int>` value as an entry with `start_time` 0. A stored
//! entry that does not decode is `EIO`; any call on a missing shard
//! other than [`set_entry`] and [`put_head`] is `ENOENT`.
//!
//! The entry requests and replies are version 2 and the list reply is
//! version 3 (Octopus v15); their `pair<string, int>` and
//! `map<string, int>` forms are below the Squid floor and rejected. The
//! list request is always sent as version 3, so the reply comes back as
//! version 3.

use bytes::Bytes;
use rados::VersionedDenc;
use rados::osdclient::error::Result;
use rados::osdclient::{IoCtx, OSDOp, OpReply};
use serde::Serialize;
use serde::ser::SerializeStruct;

use super::CLASS;
use crate::call;

/// `lc_uninitial` (`rgw_lc.h`'s `LC_BUCKET_STATUS`): not yet processed.
pub const STATUS_UNINITIAL: u32 = 0;
/// `lc_processing`: a worker holds the bucket.
pub const STATUS_PROCESSING: u32 = 1;
/// `lc_failed`.
pub const STATUS_FAILED: u32 = 2;
/// `lc_complete`.
pub const STATUS_COMPLETE: u32 = 3;

/// `cls_rgw_lc_entry`: one bucket's lifecycle-processing state.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct LcEntry {
    pub bucket: String,
    pub start_time: u64,
    pub status: u32,
}

/// `cls_rgw_lc_obj_head`: the per-shard lifecycle marker. Both `time_t`
/// fields are written as eight-byte integers (`start_date` unsigned,
/// `shard_rollover_date` signed); the dump drops `shard_rollover_date`.
/// Version 2 added `shard_rollover_date`; the decoder floors at version 2.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(
    crate = "rados",
    version = 2,
    compat = 2,
    min_version = 2,
    ceph_release = "Reef v18+"
)]
pub struct LcObjHead {
    pub start_date: i64,
    pub marker: String,
    #[serde(skip)]
    pub shard_rollover_date: i64,
}

/// `cls_rgw_lc_get_entry_ret`. Version 1 carried a `pair<string, int>`
/// of bucket and status, or the entry itself; the decoder floors at
/// version 2.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(
    crate = "rados",
    version = 2,
    compat = 2,
    min_version = 2,
    ceph_release = "Octopus v15+"
)]
pub struct GetEntryRet {
    pub entry: LcEntry,
}

/// `cls_rgw_lc_set_entry_op`. The dump flattens the entry's fields.
/// Version 1 carried a `pair<string, int>` of bucket and status; the
/// decoder floors at version 2.
#[derive(Debug, Clone, Default, PartialEq, Eq, VersionedDenc)]
#[denc(
    crate = "rados",
    version = 2,
    compat = 2,
    min_version = 2,
    ceph_release = "Octopus v15+"
)]
pub struct SetEntryOp {
    pub entry: LcEntry,
}

impl Serialize for SetEntryOp {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("SetEntryOp", 3)?;
        state.serialize_field("bucket", &self.entry.bucket)?;
        state.serialize_field("start_time", &self.entry.start_time)?;
        state.serialize_field("status", &self.entry.status)?;
        state.end()
    }
}

/// `cls_rgw_lc_rm_entry_op`: only the entry's `bucket` matters to the
/// class. Version 1 carried a `pair<string, int>` of bucket and status;
/// the decoder floors at version 2.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(
    crate = "rados",
    version = 2,
    compat = 2,
    min_version = 2,
    ceph_release = "Octopus v15+"
)]
pub struct RmEntryOp {
    pub entry: LcEntry,
}

/// `cls_rgw_lc_get_next_entry_ret`. Version 1 carried a
/// `pair<string, int>` of bucket and status; the decoder floors at
/// version 2.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(
    crate = "rados",
    version = 2,
    compat = 2,
    min_version = 2,
    ceph_release = "Octopus v15+"
)]
pub struct GetNextEntryRet {
    pub entry: LcEntry,
}

/// `cls_rgw_lc_get_entry_op`: the bucket key to read.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct GetEntryOp {
    pub marker: String,
}

/// `cls_rgw_lc_get_next_entry_op`: the bucket key to start after; empty
/// for the first.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct GetNextEntryOp {
    pub marker: String,
}

/// `cls_rgw_lc_put_head_op`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct PutHeadOp {
    pub head: LcObjHead,
}

/// `cls_rgw_lc_get_head_ret`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct GetHeadRet {
    pub head: LcObjHead,
}

/// `cls_rgw_lc_list_entries_op`. The class answers in the version the
/// request was sent with; this one is always sent as version 3. The
/// decoder floors at version 3.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(
    crate = "rados",
    version = 3,
    compat = 1,
    min_version = 3,
    ceph_release = "Octopus v15+"
)]
pub struct ListEntriesOp {
    pub marker: String,
    pub max_entries: u32,
}

/// `cls_rgw_lc_list_entries_ret`, version 3. Versions 1 and 2 carried a
/// `map<string, int>` of bucket and status; the decoder floors at
/// version 3.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(
    crate = "rados",
    version = 3,
    compat = 1,
    min_version = 3,
    ceph_release = "Octopus v15+"
)]
pub struct ListRet {
    pub entries: Vec<LcEntry>,
    pub is_truncated: bool,
}

/// `cls_rgw_lc_get_head`: the shard's head, a default one while none has
/// been put. Decode the reply with [`decode_get_head`].
pub fn get_head_op() -> Result<OSDOp> {
    call::raw_op(CLASS, "lc_get_head", Bytes::new())
}

/// `cls_rgw_lc_put_head`: replace the shard's head.
pub fn put_head_op(head: &LcObjHead) -> Result<OSDOp> {
    call::op(CLASS, "lc_put_head", &PutHeadOp { head: head.clone() })
}

/// `cls_rgw_lc_get_entry`: the entry under the bucket key `marker`;
/// `ENOENT` when there is none. Decode the reply with
/// [`decode_get_entry`].
pub fn get_entry_op(marker: &str) -> Result<OSDOp> {
    call::op(
        CLASS,
        "lc_get_entry",
        &GetEntryOp {
            marker: marker.to_owned(),
        },
    )
}

/// `cls_rgw_lc_set_entry`: store `entry` under its bucket key, replacing
/// any entry there.
pub fn set_entry_op(entry: &LcEntry) -> Result<OSDOp> {
    call::op(
        CLASS,
        "lc_set_entry",
        &SetEntryOp {
            entry: entry.clone(),
        },
    )
}

/// `cls_rgw_lc_rm_entry`: remove the key `entry.bucket`; an absent key is
/// not an error.
pub fn rm_entry_op(entry: &LcEntry) -> Result<OSDOp> {
    call::op(
        CLASS,
        "lc_rm_entry",
        &RmEntryOp {
            entry: entry.clone(),
        },
    )
}

/// `cls_rgw_lc_get_next_entry`: the first entry after the bucket key
/// `marker` (empty for the first of the shard), or a default entry with
/// an empty `bucket` when none follows. Decode the reply with
/// [`decode_get_next_entry`].
pub fn get_next_entry_op(marker: &str) -> Result<OSDOp> {
    call::op(
        CLASS,
        "lc_get_next_entry",
        &GetNextEntryOp {
            marker: marker.to_owned(),
        },
    )
}

/// `cls_rgw_lc_list_entries`: up to `max_entries` entries after the
/// bucket key `marker` (empty for the start). Decode the reply with
/// [`decode_list`].
pub fn list_op(marker: &str, max_entries: u32) -> Result<OSDOp> {
    call::op(CLASS, "lc_list_entries", &list_req(marker, max_entries))
}

fn list_req(marker: &str, max_entries: u32) -> ListEntriesOp {
    ListEntriesOp {
        marker: marker.to_owned(),
        max_entries,
    }
}

/// Decode the reply to [`get_head_op`].
pub fn decode_get_head(reply: &OpReply) -> Result<LcObjHead> {
    call::decode::<GetHeadRet>(reply).map(|ret| ret.head)
}

/// Decode the reply to [`get_entry_op`].
pub fn decode_get_entry(reply: &OpReply) -> Result<LcEntry> {
    call::decode::<GetEntryRet>(reply).map(|ret| ret.entry)
}

/// Decode the reply to [`get_next_entry_op`].
pub fn decode_get_next_entry(reply: &OpReply) -> Result<LcEntry> {
    call::decode::<GetNextEntryRet>(reply).map(|ret| ret.entry)
}

/// Decode the reply to [`list_op`], with `entries` sorted by bucket as
/// `cls_rgw_lc_list` sorts them (the class returns them in key order,
/// which is the same order).
pub fn decode_list(reply: &OpReply) -> Result<ListRet> {
    call::decode(reply).map(sorted)
}

fn sorted(mut ret: ListRet) -> ListRet {
    ret.entries.sort_by(|a, b| a.bucket.cmp(&b.bucket));
    ret
}

/// See [`get_head_op`].
pub async fn get_head(ioctx: &IoCtx, oid: &str) -> Result<LcObjHead> {
    let out = call::exec_raw(ioctx, oid, CLASS, "lc_get_head", Bytes::new()).await?;
    call::decode_bytes::<GetHeadRet>(out).map(|ret| ret.head)
}

/// See [`put_head_op`].
pub async fn put_head(ioctx: &IoCtx, oid: &str, head: &LcObjHead) -> Result<()> {
    let op = PutHeadOp { head: head.clone() };
    call::exec(ioctx, oid, CLASS, "lc_put_head", &op)
        .await
        .map(drop)
}

/// See [`get_entry_op`].
pub async fn get_entry(ioctx: &IoCtx, oid: &str, marker: &str) -> Result<LcEntry> {
    let op = GetEntryOp {
        marker: marker.to_owned(),
    };
    let out = call::exec(ioctx, oid, CLASS, "lc_get_entry", &op).await?;
    call::decode_bytes::<GetEntryRet>(out).map(|ret| ret.entry)
}

/// See [`set_entry_op`].
pub async fn set_entry(ioctx: &IoCtx, oid: &str, entry: &LcEntry) -> Result<()> {
    let op = SetEntryOp {
        entry: entry.clone(),
    };
    call::exec(ioctx, oid, CLASS, "lc_set_entry", &op)
        .await
        .map(drop)
}

/// See [`rm_entry_op`].
pub async fn rm_entry(ioctx: &IoCtx, oid: &str, entry: &LcEntry) -> Result<()> {
    let op = RmEntryOp {
        entry: entry.clone(),
    };
    call::exec(ioctx, oid, CLASS, "lc_rm_entry", &op)
        .await
        .map(drop)
}

/// See [`get_next_entry_op`].
pub async fn get_next_entry(ioctx: &IoCtx, oid: &str, marker: &str) -> Result<LcEntry> {
    let op = GetNextEntryOp {
        marker: marker.to_owned(),
    };
    let out = call::exec(ioctx, oid, CLASS, "lc_get_next_entry", &op).await?;
    call::decode_bytes::<GetNextEntryRet>(out).map(|ret| ret.entry)
}

/// See [`list_op`].
pub async fn list(ioctx: &IoCtx, oid: &str, marker: &str, max_entries: u32) -> Result<ListRet> {
    let op = list_req(marker, max_entries);
    let out = call::exec(ioctx, oid, CLASS, "lc_list_entries", &op).await?;
    call::decode_bytes(out).map(sorted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rados::{Denc, RadosError, encode_with_capacity};

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

    #[test]
    fn lc_entry_dumps_its_fields_plainly() {
        // Oracle cls_rgw_lc_entry instance 1: "bucket", 10, 1.
        let e = LcEntry {
            bucket: "bucket".to_owned(),
            start_time: 10,
            status: 1,
        };
        assert_eq!(
            bytes(&e),
            unhex("010116000000060000006275636b65740a0000000000000001000000")
        );
        assert_eq!(
            json(&e),
            r#"{"bucket":"bucket","start_time":10,"status":1}"#
        );
        assert_eq!(LcEntry::decode(&mut &bytes(&e)[..], 0).expect("decode"), e);
    }

    #[test]
    fn lc_obj_head_dumps_start_date_and_marker_only() {
        let h = LcObjHead {
            start_date: 10,
            marker: "m".to_owned(),
            shard_rollover_date: 20,
        };
        assert_eq!(
            bytes(&h),
            unhex("0202150000000a00000000000000010000006d1400000000000000")
        );
        assert_eq!(json(&h), r#"{"start_date":10,"marker":"m"}"#);
        assert_eq!(
            LcObjHead::decode(&mut &bytes(&h)[..], 0).expect("decode"),
            h
        );
    }

    #[test]
    fn lc_obj_head_floors_at_version_2() {
        let mut wire = bytes(&LcObjHead::default());
        wire[..2].copy_from_slice(&[1, 1]);
        let err = LcObjHead::decode(&mut &wire[..], 0).expect_err("version 1");
        assert!(
            matches!(
                err,
                RadosError::Codec(rados::CodecError::VersionTooOld { got: 1, min: 2, .. })
            ),
            "{err:?}"
        );
    }

    #[test]
    fn list_entries_op_floors_at_version_3() {
        let mut wire = bytes(&ListEntriesOp::default());
        wire[..2].copy_from_slice(&[1, 1]);
        let err = ListEntriesOp::decode(&mut &wire[..], 0).expect_err("version 1");
        assert!(
            matches!(
                err,
                RadosError::Codec(rados::CodecError::VersionTooOld { got: 1, min: 3, .. })
            ),
            "{err:?}"
        );
    }

    #[test]
    fn get_entry_ret_matches_the_oracle() {
        // ceph-dencoder v19.2.2 cls_rgw_lc_get_entry_ret instance 1.
        let ret = GetEntryRet {
            entry: LcEntry {
                bucket: "bucket1".to_owned(),
                start_time: 6000,
                status: 0,
            },
        };
        let wire = unhex("02021d000000010117000000070000006275636b657431701700000000000000000000");
        assert_eq!(bytes(&ret), wire);
        assert_eq!(
            json(&ret),
            r#"{"entry":{"bucket":"bucket1","start_time":6000,"status":0}}"#
        );
        assert_eq!(GetEntryRet::decode(&mut &wire[..], 0).expect("decode"), ret);
    }

    #[test]
    fn set_entry_op_matches_the_oracle_and_dumps_flat() {
        // ceph-dencoder v19.2.2 cls_rgw_lc_set_entry_op instance 2.
        let op = SetEntryOp {
            entry: LcEntry {
                bucket: "foo".to_owned(),
                start_time: 123,
                status: 456,
            },
        };
        let wire = unhex("02021900000001011300000003000000666f6f7b00000000000000c8010000");
        assert_eq!(bytes(&op), wire);
        assert_eq!(
            json(&op),
            r#"{"bucket":"foo","start_time":123,"status":456}"#
        );
        assert_eq!(SetEntryOp::decode(&mut &wire[..], 0).expect("decode"), op);
    }

    fn below_floor(err: RadosError) -> bool {
        matches!(
            err,
            RadosError::Codec(rados::CodecError::VersionTooOld { .. })
        )
    }

    #[test]
    fn entry_messages_reject_the_version_1_pair() {
        // Version 1, compat 1: pair<string, int> ("b", 3).
        let pair = unhex("010109000000010000006203000000");
        assert!(below_floor(
            SetEntryOp::decode(&mut &pair[..], 0).expect_err("set")
        ));
        assert!(below_floor(
            RmEntryOp::decode(&mut &pair[..], 0).expect_err("rm")
        ));
        assert!(below_floor(
            GetNextEntryRet::decode(&mut &pair[..], 0).expect_err("next")
        ));
        assert!(below_floor(
            GetEntryRet::decode(&mut &pair[..], 0).expect_err("get")
        ));
        // A version-1 frame around a whole entry, the other form v1
        // get_entry_ret took: still below the floor.
        let v1_entry =
            unhex("01011d000000010117000000070000006275636b657431701700000000000000000000");
        assert!(below_floor(
            GetEntryRet::decode(&mut &v1_entry[..], 0).expect_err("get v1 entry")
        ));
    }

    #[test]
    fn list_ret_is_version_3_and_rejects_the_map_forms() {
        let ret = ListRet {
            entries: vec![
                LcEntry {
                    bucket: "a".to_owned(),
                    start_time: 1,
                    status: STATUS_FAILED,
                },
                LcEntry {
                    bucket: "b".to_owned(),
                    start_time: 3,
                    status: STATUS_UNINITIAL,
                },
            ],
            is_truncated: true,
        };
        let wire = unhex(concat!(
            "030133000000",
            "02000000",
            "0101110000000100000061010000000000000002000000",
            "0101110000000100000062030000000000000000000000",
            "01",
        ));
        assert_eq!(bytes(&ret), wire);
        assert_eq!(ListRet::decode(&mut &wire[..], 0).expect("decode"), ret);
        // Version 2: map<string, int> {"a": 1} then is_truncated.
        let v2 = unhex("02010e0000000100000001000000610100000000");
        assert!(below_floor(
            ListRet::decode(&mut &v2[..], 0).expect_err("v2")
        ));
    }

    #[test]
    fn decode_list_sorts_by_bucket() {
        let entry = |bucket: &str| LcEntry {
            bucket: bucket.to_owned(),
            ..LcEntry::default()
        };
        let ret = ListRet {
            entries: vec![entry(":b2:m2"), entry(":b1:m1")],
            is_truncated: false,
        };
        let reply = rados::OpReply {
            return_code: 0,
            outdata: encode_with_capacity(&ret, 0).expect("encode"),
        };
        let got = decode_list(&reply).expect("decode");
        assert_eq!(got.entries, vec![entry(":b1:m1"), entry(":b2:m2")]);
    }

    #[test]
    fn decoders_unwrap_the_replies() {
        let head = LcObjHead {
            start_date: 10,
            marker: "m".to_owned(),
            shard_rollover_date: 20,
        };
        let entry = LcEntry {
            bucket: ":b1:m1".to_owned(),
            start_time: 5,
            status: STATUS_COMPLETE,
        };
        let reply = |outdata| rados::OpReply {
            return_code: 0,
            outdata,
        };
        let out = encode_with_capacity(&GetHeadRet { head: head.clone() }, 0).expect("encode");
        assert_eq!(decode_get_head(&reply(out)).expect("head"), head);
        let out = encode_with_capacity(
            &GetEntryRet {
                entry: entry.clone(),
            },
            0,
        )
        .expect("encode");
        assert_eq!(decode_get_entry(&reply(out)).expect("entry"), entry);
        let out = encode_with_capacity(
            &GetNextEntryRet {
                entry: entry.clone(),
            },
            0,
        )
        .expect("encode");
        assert_eq!(decode_get_next_entry(&reply(out)).expect("next"), entry);
    }

    fn call_indata(method: &str, req: &[u8]) -> Vec<u8> {
        let mut want = format!("rgw{method}").into_bytes();
        want.extend_from_slice(req);
        want
    }

    #[test]
    fn ops_name_the_rgw_class_and_their_method() {
        use rados::osdclient::types::OpData;

        let head = LcObjHead {
            start_date: 10,
            marker: "m".to_owned(),
            shard_rollover_date: 20,
        };
        let entry = LcEntry {
            bucket: "foo".to_owned(),
            start_time: 123,
            status: 456,
        };
        let entry_wire = "02021900000001011300000003000000666f6f7b00000000000000c8010000";
        let cases = [
            (get_head_op(), "lc_get_head", Vec::new()),
            (
                put_head_op(&head),
                "lc_put_head",
                unhex("01011b0000000202150000000a00000000000000010000006d1400000000000000"),
            ),
            (
                get_entry_op(":b2:m2"),
                "lc_get_entry",
                unhex("01010a000000060000003a62323a6d32"),
            ),
            (set_entry_op(&entry), "lc_set_entry", unhex(entry_wire)),
            (rm_entry_op(&entry), "lc_rm_entry", unhex(entry_wire)),
            (
                get_next_entry_op(""),
                "lc_get_next_entry",
                unhex("01010400000000000000"),
            ),
            (
                list_op("", 2),
                "lc_list_entries",
                unhex("0301080000000000000002000000"),
            ),
        ];
        for (op, method, req) in cases {
            let op = op.expect("op");
            assert_eq!(
                op.indata.as_ref(),
                &call_indata(method, &req)[..],
                "{method}"
            );
            let OpData::Call {
                class_len,
                method_len,
                ..
            } = op.op_data
            else {
                panic!("{method}: not a call");
            };
            assert_eq!((class_len, usize::from(method_len)), (3, method.len()));
        }
    }
}
