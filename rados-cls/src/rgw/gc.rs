//! The `rgw` class's omap-era GC methods (`cls_rgw_client.h`'s
//! `cls_rgw_gc_*`) and their request and reply structs
//! (`cls_rgw_ops.h`). The queue-based GC is the `rgw_gc` class, in
//! `crate::rgw_gc` (feature `rgw_gc`), which reuses [`SetEntryOp`],
//! [`ListOp`] and [`ListRet`]; its `defer_entry_op` takes the whole
//! entry where this module's takes a tag.
//!
//! Server facts (Ceph v19 `cls_rgw.cc`): a GC shard keeps each chain
//! twice in its omap, under the name key `"0_" + tag` and the time key
//! `"1_"` + `"%011llu.%09u"` of its expiry in seconds and nanoseconds.
//! The class sets an entry's `time` to the OSD's clock plus
//! `expiration_secs` and ignores the client's. [`set_entry`] upserts: an
//! existing tag's time key moves. [`defer_entry`] re-keys an existing
//! tag the same way and is `ENOENT` for an absent one. [`list`] takes
//! the marker verbatim as an omap key (empty means the start of the
//! time index), returns up to `max` entries (zero means 128) in expiry
//! order, stops at the first entry not yet due when `expired_only`, and
//! sets `next_marker` (the last key returned) only when `truncated`.
//! [`remove`] deletes both keys of each tag and skips an absent tag. A
//! stored entry that does not decode is `EIO`.
//!
//! How RGW composes the two paths: `RGWGC::send_chain` enqueues on the
//! `rgw_gc` queue behind `version::check_op(objv 1, VersionCond::Eq)`
//! and, when that fails with `ECANCELED` (the shard is still at version
//! 0) or `EPERM` (the OSD will not load `rgw_gc`, the mixed-version
//! upgrade case), sends [`set_entry_op`] with no version check.
//! `RGWGC::list` reads a shard's omap entries until the shard proves
//! transitioned, and [`remove_op`] retires processed tags on shards that
//! have not. [`defer_entry_op`] is reached only from
//! `RGWRados::defer_gc`, which nothing in v19 calls.

use bytes::{Buf, BufMut};
use rados::osdclient::error::Result;
use rados::osdclient::{IoCtx, OSDOp, OpReply};
use rados::{Denc, RadosError, VersionedDenc, VersionedEncode};
use serde::Serialize;

use super::CLASS;
use crate::call;
use crate::rgw::types::GcObjInfo;

/// `cls_rgw_gc_set_entry_op`: an entry due `expiration_secs` from now.
/// The dump nests the entry as `obj_info`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct SetEntryOp {
    pub expiration_secs: u32,
    #[serde(rename = "obj_info")]
    pub info: GcObjInfo,
}

/// `cls_rgw_gc_defer_entry_op`: the omap-era defer, by tag. The `rgw_gc`
/// class defers with its own request carrying the whole entry.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct DeferEntryOp {
    pub expiration_secs: u32,
    pub tag: String,
}

/// `cls_rgw_gc_list_op`: up to `max` entries after `marker`. Version 2
/// added `expired_only`, which the C++ constructor defaults to true; the
/// decoder floors at version 2.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ListOp {
    pub marker: String,
    pub max: u32,
    pub expired_only: bool,
}

impl Default for ListOp {
    fn default() -> Self {
        Self {
            marker: String::new(),
            max: 0,
            expired_only: true,
        }
    }
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
        self.marker.encode(buf, features)?;
        self.max.encode(buf, features)?;
        self.expired_only.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 2, "ListOp", "Firefly v0.80+");

        let marker = String::decode(buf, features)?;
        let max = u32::decode(buf, features)?;
        let expired_only = bool::decode(buf, features)?;
        Ok(Self {
            marker,
            max,
            expired_only,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

rados::impl_denc_for_versioned!(ListOp);

/// `cls_rgw_gc_list_ret`. Version 2 put `next_marker` between the entries
/// and `truncated`; the decoder floors at version 2. The dump prints
/// `truncated` as an int.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ListRet {
    pub entries: Vec<GcObjInfo>,
    pub next_marker: String,
    #[serde(serialize_with = "crate::dump::bool_as_int")]
    pub truncated: bool,
}

impl VersionedEncode for ListRet {
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
        self.entries.encode(buf, features)?;
        self.next_marker.encode(buf, features)?;
        self.truncated.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 2, "ListRet", "Luminous v12+");

        let entries = Vec::<GcObjInfo>::decode(buf, features)?;
        let next_marker = String::decode(buf, features)?;
        let truncated = bool::decode(buf, features)?;
        Ok(Self {
            entries,
            next_marker,
            truncated,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

rados::impl_denc_for_versioned!(ListRet);

/// `cls_rgw_gc_remove_op`: the omap-era remove, by tags.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct RemoveOp {
    pub tags: Vec<String>,
}

/// `cls_rgw_gc_set_entry`: store `info` under its tag, due
/// `expiration_secs` from the OSD's now, replacing any entry with the
/// same tag.
pub fn set_entry_op(expiration_secs: u32, info: &GcObjInfo) -> Result<OSDOp> {
    call::op(
        CLASS,
        "gc_set_entry",
        &SetEntryOp {
            expiration_secs,
            info: info.clone(),
        },
    )
}

/// `cls_rgw_gc_defer_entry`: make `tag` due `expiration_secs` from the
/// OSD's now. `ENOENT` when the shard has no entry for `tag`.
pub fn defer_entry_op(expiration_secs: u32, tag: &str) -> Result<OSDOp> {
    call::op(
        CLASS,
        "gc_defer_entry",
        &DeferEntryOp {
            expiration_secs,
            tag: tag.to_owned(),
        },
    )
}

/// `cls_rgw_gc_list`: up to `max` entries (zero means 128) in expiry
/// order after `marker`, an omap key such as a previous reply's
/// `next_marker`; only the due ones when `expired_only`. Decode the
/// reply with [`decode_list`].
pub fn list_op(marker: &str, max: u32, expired_only: bool) -> Result<OSDOp> {
    call::op(
        CLASS,
        "gc_list",
        &ListOp {
            marker: marker.to_owned(),
            max,
            expired_only,
        },
    )
}

/// `cls_rgw_gc_remove`: drop the entries for `tags`; an absent tag is
/// not an error.
pub fn remove_op(tags: &[String]) -> Result<OSDOp> {
    call::op(
        CLASS,
        "gc_remove",
        &RemoveOp {
            tags: tags.to_vec(),
        },
    )
}

/// Decode the reply to [`list_op`].
pub fn decode_list(reply: &OpReply) -> Result<ListRet> {
    call::decode(reply)
}

/// See [`set_entry_op`].
pub async fn set_entry(
    ioctx: &IoCtx,
    oid: &str,
    expiration_secs: u32,
    info: &GcObjInfo,
) -> Result<()> {
    let op = SetEntryOp {
        expiration_secs,
        info: info.clone(),
    };
    call::exec(ioctx, oid, CLASS, "gc_set_entry", &op)
        .await
        .map(drop)
}

/// See [`defer_entry_op`].
pub async fn defer_entry(ioctx: &IoCtx, oid: &str, expiration_secs: u32, tag: &str) -> Result<()> {
    let op = DeferEntryOp {
        expiration_secs,
        tag: tag.to_owned(),
    };
    call::exec(ioctx, oid, CLASS, "gc_defer_entry", &op)
        .await
        .map(drop)
}

/// See [`list_op`].
pub async fn list(
    ioctx: &IoCtx,
    oid: &str,
    marker: &str,
    max: u32,
    expired_only: bool,
) -> Result<ListRet> {
    let op = ListOp {
        marker: marker.to_owned(),
        max,
        expired_only,
    };
    let out = call::exec(ioctx, oid, CLASS, "gc_list", &op).await?;
    call::decode_bytes(out)
}

/// See [`remove_op`].
pub async fn remove(ioctx: &IoCtx, oid: &str, tags: &[String]) -> Result<()> {
    let op = RemoveOp {
        tags: tags.to_vec(),
    };
    call::exec(ioctx, oid, CLASS, "gc_remove", &op)
        .await
        .map(drop)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rgw::types::GcObjInfo;
    use rados::encode_with_capacity;

    fn bytes<T: Denc>(v: &T) -> Vec<u8> {
        encode_with_capacity(v, 0).expect("encode").to_vec()
    }

    fn json<T: Serialize>(v: &T) -> String {
        serde_json::to_string(v).expect("json")
    }

    #[test]
    fn set_entry_op_nests_the_info_as_obj_info() {
        // Corpus cls_rgw_gc_set_entry_op/68510e49...: 123 seconds, default info.
        let op = SetEntryOp {
            expiration_secs: 123,
            info: GcObjInfo::default(),
        };
        assert_eq!(
            bytes(&op),
            b"\x01\x01\x20\x00\x00\x00\x7b\x00\x00\x00\x01\x01\x16\x00\x00\x00\x00\x00\x00\x00\x01\x01\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        );
        assert_eq!(
            json(&op),
            r#"{"expiration_secs":123,"obj_info":{"tag":"","chain":{"objs":[]},"time":"1970-01-01T00:00:00.000000+0000"}}"#
        );
    }

    #[test]
    fn defer_and_remove_are_the_omap_era_shapes() {
        // Corpus cls_rgw_gc_defer_entry_op/1191bd44... and cls_rgw_gc_remove_op/39615566....
        let d = DeferEntryOp {
            expiration_secs: 5,
            tag: "mychain".to_owned(),
        };
        assert_eq!(
            bytes(&d),
            b"\x01\x01\x0f\x00\x00\x00\x05\x00\x00\x00\x07\x00\x00\x00mychain"
        );
        assert_eq!(json(&d), r#"{"expiration_secs":5,"tag":"mychain"}"#);
        let r = RemoveOp {
            tags: vec!["tag1".to_owned(), "tag2".to_owned()],
        };
        assert_eq!(
            bytes(&r),
            b"\x01\x01\x14\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00tag1\x04\x00\x00\x00tag2"
        );
        assert_eq!(json(&r), r#"{"tags":["tag1","tag2"]}"#);
    }

    #[test]
    fn list_op_defaults_expired_only_and_rejects_v1() {
        // Corpus cls_rgw_gc_list_op/500d868f...: "mymarker", 2312, true.
        let op = ListOp {
            marker: "mymarker".to_owned(),
            max: 2312,
            ..ListOp::default()
        };
        assert!(op.expired_only);
        assert_eq!(
            bytes(&op),
            b"\x02\x01\x11\x00\x00\x00\x08\x00\x00\x00mymarker\x08\x09\x00\x00\x01"
        );
        assert_eq!(
            json(&op),
            r#"{"marker":"mymarker","max":2312,"expired_only":true}"#
        );
        // Corpus cls_rgw_gc_list_op/ac427b0d...: "", 10, false.
        let wire = b"\x02\x01\x09\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x00\x00\x00";
        let op = ListOp::decode(&mut &wire[..], 0).expect("decode");
        assert_eq!((op.max, op.expired_only), (10, false));
        // Version 1 had no flag; the decoder floors at version 2 and
        // rejects it.
        let v1 = b"\x01\x01\x08\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x00\x00";
        assert!(ListOp::decode(&mut &v1[..], 0).is_err());
    }

    #[test]
    fn list_ret_puts_next_marker_before_truncated_and_dumps_it_as_int() {
        // Corpus cls_rgw_gc_list_ret/07ac230e...: one default entry, "", truncated.
        let ret = ListRet {
            entries: vec![GcObjInfo::default()],
            next_marker: String::new(),
            truncated: true,
        };
        let wire = b"\x02\x01\x25\x00\x00\x00\x01\x00\x00\x00\x01\x01\x16\x00\x00\x00\x00\x00\x00\x00\x01\x01\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01";
        assert_eq!(bytes(&ret), wire);
        assert_eq!(ListRet::decode(&mut &wire[..], 0).expect("decode"), ret);
        assert!(json(&ret).ends_with(r#""next_marker":"","truncated":1}"#));
        // Version 1: entries then truncated, no marker; the decoder floors
        // at version 2 and rejects it.
        let v1 = b"\x01\x01\x05\x00\x00\x00\x00\x00\x00\x00\x01";
        assert!(ListRet::decode(&mut &v1[..], 0).is_err());
    }

    fn unhex(s: &str) -> Vec<u8> {
        s.as_bytes()
            .chunks(2)
            .map(|pair| {
                u8::from_str_radix(std::str::from_utf8(pair).expect("ascii"), 16).expect("hex")
            })
            .collect()
    }

    fn call_indata(method: &str, req: &str) -> Vec<u8> {
        let mut want = format!("rgw{method}").into_bytes();
        want.extend(unhex(req));
        want
    }

    #[test]
    fn ops_name_the_rgw_class_and_their_method() {
        use rados::osdclient::types::OpData;

        let tags = ["tag1".to_owned(), "tag2".to_owned()];
        let cases = [
            (
                set_entry_op(123, &GcObjInfo::default()),
                "gc_set_entry",
                "0101200000007b00000001011600000000000000010104000000000000000000000000000000",
            ),
            (
                defer_entry_op(5, "mychain"),
                "gc_defer_entry",
                "01010f00000005000000070000006d79636861696e",
            ),
            // Also what ceph-dencoder encodes for a default cls_rgw_gc_list_op.
            (
                list_op("", 0, true),
                "gc_list",
                "020109000000000000000000000001",
            ),
            (
                remove_op(&tags),
                "gc_remove",
                "0101140000000200000004000000746167310400000074616732",
            ),
        ];
        for (op, method, req) in cases {
            let op = op.expect("op");
            assert_eq!(
                op.indata.as_ref(),
                &call_indata(method, req)[..],
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

    #[test]
    fn decode_list_unwraps_the_reply() {
        let ret = ListRet {
            entries: vec![GcObjInfo {
                tag: "chain-0".to_owned(),
                ..GcObjInfo::default()
            }],
            next_marker: "1_01700000000.000000001".to_owned(),
            truncated: true,
        };
        let reply = rados::OpReply {
            return_code: 0,
            outdata: encode_with_capacity(&ret, 0).expect("encode"),
        };
        assert_eq!(decode_list(&reply).expect("decode"), ret);
    }
}
