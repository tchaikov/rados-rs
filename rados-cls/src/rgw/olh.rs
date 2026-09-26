//! Object versioning: from `cls_rgw_types.h`, the OLH (object logical
//! head) entry and its log entry; from `cls_rgw_ops.h`, the request and
//! reply structs of the `rgw` class's OLH methods; and, as
//! `cls_rgw_client.h` has them, those methods' op constructors and async
//! calls.
//!
//! The OLH is the bucket-index entry that names a versioned object's
//! current instance. Linking and unlinking an instance rewrite it and
//! append to its pending log, keyed by the OLH's epoch, which RGW replays
//! against the head object and then trims.
//!
//! Server facts (Ceph v19 `cls_rgw.cc`): an `olh_epoch` of 0 in
//! [`link_olh`] or [`unlink_instance`] asks the class for the next epoch
//! (2 for a new OLH, as 1 is reserved for plain entries converted to
//! versioned, then one more each time); an explicit epoch below the OLH's is
//! stale: [`link_olh`] writes the instance as not current and leaves the OLH
//! alone; [`unlink_instance`] drops the instance's listing entry and, unless it
//! is a delete marker, logs `REMOVE_INSTANCE` under the OLH's current epoch.
//! Neither method replies. The writes ([`link_olh`], [`unlink_instance`],
//! [`trim_olh_log`], [`clear_olh`]) take no resharding guard of their
//! own: as for the index writes, RGW puts
//! [`super::index::guard_op`] in front of them in one compound
//! operation. [`read_olh_log`], [`trim_olh_log`] and [`clear_olh`]
//! compare `olh_tag` with the OLH's, and a missing OLH's tag is empty.

use std::collections::BTreeMap;

use bytes::{Buf, BufMut};
use rados::osdclient::error::Result;
use rados::osdclient::{IoCtx, OSDOp, OpReply};
use rados::{Denc, RadosError, UTime, VersionedDenc, VersionedEncode};
use serde::Serialize;
use serde::ser::SerializeStruct;

use super::CLASS;
use super::index::{DirEntryMeta, DumpUtime, ZonesTraceBare};
use super::types::{ObjKey, ZoneSet, byte_enum};
use crate::call;

byte_enum! {
    /// `OLHLogOp`: what one OLH log entry records. Ceph decodes any byte and
    /// `CLS_RGW_OLH_OP_STALE` (4) is not modelled; the newtype keeps the byte.
    OlhLogOp { UNKNOWN = 0, LINK_OLH = 1, UNLINK_OLH = 2, REMOVE_INSTANCE = 3 }
}

impl OlhLogOp {
    /// `to_string(OLHLogOp)`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LINK_OLH => "link_olh",
            Self::UNLINK_OLH => "unlink_olh",
            Self::REMOVE_INSTANCE => "remove_instance",
            _ => "unknown",
        }
    }
}

fn olh_log_op_name<S: serde::Serializer>(
    op: &OlhLogOp,
    s: S,
) -> std::result::Result<S::Ok, S::Error> {
    s.serialize_str(op.as_str())
}

/// `rgw_bucket_olh_log_entry`: one pending change to an OLH object,
/// keyed by epoch in [`OlhEntry::pending_log`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct OlhLogEntry {
    pub epoch: u64,
    #[serde(serialize_with = "olh_log_op_name")]
    pub op: OlhLogOp,
    pub op_tag: String,
    pub key: ObjKey,
    pub delete_marker: bool,
}

fn pending_log<S: serde::Serializer>(
    m: &BTreeMap<u64, Vec<OlhLogEntry>>,
    s: S,
) -> std::result::Result<S::Ok, S::Error> {
    crate::dump::map_entries(m.iter(), s)
}

/// `rgw_bucket_olh_entry`: the current version of a versioned S3 object,
/// with the log of modifications not yet applied.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct OlhEntry {
    pub key: ObjKey,
    pub delete_marker: bool,
    pub epoch: u64,
    #[serde(serialize_with = "pending_log")]
    pub pending_log: BTreeMap<u64, Vec<OlhLogEntry>>,
    pub tag: String,
    pub exists: bool,
    pub pending_removal: bool,
}

/// `rgw_cls_link_olh_op`: make `key` an instance of its object and, when
/// its epoch is the newest, the current one. Version 5 (compat 1); the
/// wire carries `unmod_since`'s whole seconds as a `u64` ahead of the
/// `real_time` that superseded them, which decoding drops.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LinkOlhOp {
    pub key: ObjKey,
    pub olh_tag: String,
    pub delete_marker: bool,
    pub op_tag: String,
    pub meta: DirEntryMeta,
    pub olh_epoch: u64,
    pub log_op: bool,
    pub bilog_flags: u16,
    /// Link only when the existing instance's mtime is older; zero
    /// disables the check.
    pub unmod_since: UTime,
    pub high_precision_time: bool,
    pub zones_trace: ZoneSet,
}

impl VersionedEncode for LinkOlhOp {
    const MAX_DECODE_VERSION: u8 = 5;

    fn encoding_version(&self, _features: u64) -> u8 {
        5
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
        self.key.encode(buf, features)?;
        self.olh_tag.encode(buf, features)?;
        self.delete_marker.encode(buf, features)?;
        self.op_tag.encode(buf, features)?;
        self.meta.encode(buf, features)?;
        self.olh_epoch.encode(buf, features)?;
        self.log_op.encode(buf, features)?;
        self.bilog_flags.encode(buf, features)?;
        u64::from(self.unmod_since.sec).encode(buf, features)?;
        self.unmod_since.encode(buf, features)?;
        self.high_precision_time.encode(buf, features)?;
        self.zones_trace.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 5, "LinkOlhOp", "Luminous v12+");

        let key = ObjKey::decode(buf, features)?;
        let olh_tag = String::decode(buf, features)?;
        let delete_marker = bool::decode(buf, features)?;
        let op_tag = String::decode(buf, features)?;
        let meta = DirEntryMeta::decode(buf, features)?;
        let olh_epoch = u64::decode(buf, features)?;
        let log_op = bool::decode(buf, features)?;
        let bilog_flags = u16::decode(buf, features)?;
        let _unmod_since_sec = u64::decode(buf, features)?;
        let unmod_since = UTime::decode(buf, features)?;
        let high_precision_time = bool::decode(buf, features)?;
        let zones_trace = ZoneSet::decode(buf, features)?;
        Ok(Self {
            key,
            olh_tag,
            delete_marker,
            op_tag,
            meta,
            olh_epoch,
            log_op,
            bilog_flags,
            unmod_since,
            high_precision_time,
            zones_trace,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

rados::impl_denc_for_versioned!(LinkOlhOp);

impl Serialize for LinkOlhOp {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("LinkOlhOp", 11)?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("olh_tag", &self.olh_tag)?;
        state.serialize_field("delete_marker", &self.delete_marker)?;
        state.serialize_field("op_tag", &self.op_tag)?;
        state.serialize_field("meta", &self.meta)?;
        state.serialize_field("olh_epoch", &self.olh_epoch)?;
        state.serialize_field("log_op", &self.log_op)?;
        state.serialize_field("bilog_flags", &self.bilog_flags)?;
        state.serialize_field("unmod_since", &DumpUtime(&self.unmod_since))?;
        state.serialize_field("high_precision_time", &self.high_precision_time)?;
        state.serialize_field("zones_trace", &ZonesTraceBare(&self.zones_trace))?;
        state.end()
    }
}

/// `rgw_cls_unlink_instance_op`: remove `key` from its object's
/// versions. Version 3 (compat 1); the dump leaves out `olh_tag`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UnlinkInstanceOp {
    pub key: ObjKey,
    pub op_tag: String,
    pub olh_epoch: u64,
    pub log_op: bool,
    pub bilog_flags: u16,
    /// Only used when there is no OLH yet, as the tag of the one the
    /// class creates.
    pub olh_tag: String,
    pub zones_trace: ZoneSet,
}

impl VersionedEncode for UnlinkInstanceOp {
    const MAX_DECODE_VERSION: u8 = 3;

    fn encoding_version(&self, _features: u64) -> u8 {
        3
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
        self.key.encode(buf, features)?;
        self.op_tag.encode(buf, features)?;
        self.olh_epoch.encode(buf, features)?;
        self.log_op.encode(buf, features)?;
        self.bilog_flags.encode(buf, features)?;
        self.olh_tag.encode(buf, features)?;
        self.zones_trace.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 3, "UnlinkInstanceOp", "Luminous v12+");

        let key = ObjKey::decode(buf, features)?;
        let op_tag = String::decode(buf, features)?;
        let olh_epoch = u64::decode(buf, features)?;
        let log_op = bool::decode(buf, features)?;
        let bilog_flags = u16::decode(buf, features)?;
        let olh_tag = String::decode(buf, features)?;
        let zones_trace = ZoneSet::decode(buf, features)?;
        Ok(Self {
            key,
            op_tag,
            olh_epoch,
            log_op,
            bilog_flags,
            olh_tag,
            zones_trace,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

rados::impl_denc_for_versioned!(UnlinkInstanceOp);

impl Serialize for UnlinkInstanceOp {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("UnlinkInstanceOp", 6)?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("op_tag", &self.op_tag)?;
        state.serialize_field("olh_epoch", &self.olh_epoch)?;
        state.serialize_field("log_op", &self.log_op)?;
        state.serialize_field("bilog_flags", &self.bilog_flags)?;
        state.serialize_field("zones_trace", &ZonesTraceBare(&self.zones_trace))?;
        state.end()
    }
}

/// `rgw_cls_read_olh_log_op`: the log entries above `ver_marker`. Sent
/// as version 1, all v19 knows; `main`'s version 2 appends
/// `get_stales`, which decoding skips.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ReadOlhLogOp {
    pub olh: ObjKey,
    pub ver_marker: u64,
    pub olh_tag: String,
}

impl VersionedEncode for ReadOlhLogOp {
    const MAX_DECODE_VERSION: u8 = 2;

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
        self.olh.encode(buf, features)?;
        self.ver_marker.encode(buf, features)?;
        self.olh_tag.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 1, "ReadOlhLogOp", "Hammer v0.94+");

        let olh = ObjKey::decode(buf, features)?;
        let ver_marker = u64::decode(buf, features)?;
        let olh_tag = String::decode(buf, features)?;
        Ok(Self {
            olh,
            ver_marker,
            olh_tag,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

rados::impl_denc_for_versioned!(ReadOlhLogOp);

/// `rgw_cls_read_olh_log_ret`: the OLH's pending log from the marker on,
/// keyed by OLH epoch.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct ReadOlhLogRet {
    #[serde(serialize_with = "pending_log")]
    pub log: BTreeMap<u64, Vec<OlhLogEntry>>,
    pub is_truncated: bool,
}

/// `rgw_cls_trim_olh_log_op`: drop the log entries at or below `ver`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct TrimOlhLogOp {
    pub olh: ObjKey,
    pub ver: u64,
    pub olh_tag: String,
}

/// `rgw_cls_bucket_clear_olh_op`: remove the OLH of `key`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct ClearOlhOp {
    pub key: ObjKey,
    pub olh_tag: String,
}

/// `cls_rgw_bucket_link_olh`: link `op.key`, whose instance entry must
/// exist unless `delete_marker` (which creates one). `ECANCELED` when
/// the OLH exists with another tag and is not pending removal; `ENOENT`
/// for a new delete marker on an object whose current version already
/// is one. With `unmod_since` set and an existing instance whose mtime is
/// not older (whole seconds unless `high_precision_time`), nothing is
/// linked and the call succeeds. A newer epoch, or an equal one on an instance
/// that does not sort after the current one, makes the instance current and
/// demotes the previous one; every non-stale link logs `LINK_OLH` under the
/// OLH's epoch.
pub fn link_olh_op(op: &LinkOlhOp) -> Result<OSDOp> {
    call::op(CLASS, "bucket_link_olh", op)
}

/// `cls_rgw_bucket_unlink_instance`: unlink `op.key` (`ENOENT` when it
/// has no instance entry). When it was current, the next older version
/// becomes current; when it was the last, the OLH logs
/// `UNLINK_OLH` and is marked pending removal. A plain entry with no OLH
/// is first converted to a versioned one.
pub fn unlink_instance_op(op: &UnlinkInstanceOp) -> Result<OSDOp> {
    call::op(CLASS, "bucket_unlink_instance", op)
}

/// `cls_rgw_get_olh_log`: the OLH's log after `ver_marker`, up to 1000
/// epochs, with `is_truncated` when more follow. `EINVAL` when `olh` has
/// an instance, `ECANCELED` when `olh_tag` is not the OLH's. The class
/// reads past the end of an empty log, so read only a log known to hold
/// entries. Decode the reply with [`decode_read_olh_log`].
pub fn read_olh_log_op(olh: &ObjKey, ver_marker: u64, olh_tag: &str) -> Result<OSDOp> {
    call::op(
        CLASS,
        "bucket_read_olh_log",
        &read_req(olh, ver_marker, olh_tag),
    )
}

fn read_req(olh: &ObjKey, ver_marker: u64, olh_tag: &str) -> ReadOlhLogOp {
    ReadOlhLogOp {
        olh: olh.clone(),
        ver_marker,
        olh_tag: olh_tag.to_owned(),
    }
}

/// Decode the reply to [`read_olh_log_op`].
pub fn decode_read_olh_log(reply: &OpReply) -> Result<ReadOlhLogRet> {
    call::decode(reply)
}

/// `cls_rgw_trim_olh_log`: erase every log epoch at or below `ver`.
/// `EINVAL` and `ECANCELED` as [`read_olh_log_op`].
pub fn trim_olh_log_op(olh: &ObjKey, ver: u64, olh_tag: &str) -> Result<OSDOp> {
    call::op(CLASS, "bucket_trim_olh_log", &trim_req(olh, ver, olh_tag))
}

fn trim_req(olh: &ObjKey, ver: u64, olh_tag: &str) -> TrimOlhLogOp {
    TrimOlhLogOp {
        olh: olh.clone(),
        ver,
        olh_tag: olh_tag.to_owned(),
    }
}

/// `cls_rgw_clear_olh`: remove the OLH of `key` and, when it is only a
/// version marker, the plain entry of the same name. `EINVAL` when `key`
/// has an instance, `ECANCELED` when `olh_tag` is not the OLH's.
pub fn clear_olh_op(key: &ObjKey, olh_tag: &str) -> Result<OSDOp> {
    call::op(CLASS, "bucket_clear_olh", &clear_req(key, olh_tag))
}

fn clear_req(key: &ObjKey, olh_tag: &str) -> ClearOlhOp {
    ClearOlhOp {
        key: key.clone(),
        olh_tag: olh_tag.to_owned(),
    }
}

/// See [`link_olh_op`].
pub async fn link_olh(ioctx: &IoCtx, oid: &str, op: &LinkOlhOp) -> Result<()> {
    call::exec(ioctx, oid, CLASS, "bucket_link_olh", op)
        .await
        .map(drop)
}

/// See [`unlink_instance_op`].
pub async fn unlink_instance(ioctx: &IoCtx, oid: &str, op: &UnlinkInstanceOp) -> Result<()> {
    call::exec(ioctx, oid, CLASS, "bucket_unlink_instance", op)
        .await
        .map(drop)
}

/// See [`read_olh_log_op`].
pub async fn read_olh_log(
    ioctx: &IoCtx,
    oid: &str,
    olh: &ObjKey,
    ver_marker: u64,
    olh_tag: &str,
) -> Result<ReadOlhLogRet> {
    let req = read_req(olh, ver_marker, olh_tag);
    let out = call::exec(ioctx, oid, CLASS, "bucket_read_olh_log", &req).await?;
    call::decode_bytes(out)
}

/// See [`trim_olh_log_op`].
pub async fn trim_olh_log(
    ioctx: &IoCtx,
    oid: &str,
    olh: &ObjKey,
    ver: u64,
    olh_tag: &str,
) -> Result<()> {
    let req = trim_req(olh, ver, olh_tag);
    call::exec(ioctx, oid, CLASS, "bucket_trim_olh_log", &req)
        .await
        .map(drop)
}

/// See [`clear_olh_op`].
pub async fn clear_olh(ioctx: &IoCtx, oid: &str, key: &ObjKey, olh_tag: &str) -> Result<()> {
    let req = clear_req(key, olh_tag);
    call::exec(ioctx, oid, CLASS, "bucket_clear_olh", &req)
        .await
        .map(drop)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rgw::types::ObjCategory;
    use rados::encode_with_capacity;

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

    fn olh_log_entry_instance() -> OlhLogEntry {
        OlhLogEntry {
            epoch: 1234,
            op: OlhLogOp::LINK_OLH,
            op_tag: "op_tag".to_owned(),
            key: ObjKey {
                name: "key.name".to_owned(),
                instance: "key.instance".to_owned(),
            },
            delete_marker: true,
        }
    }

    #[test]
    fn olh_log_entry_prints_op_by_name() {
        let e = olh_log_entry_instance();
        assert_eq!(
            bytes(&e),
            unhex(
                "010136000000d20400000000000001060000006f705f74616701011c000000080000006b65792e6e616d650c0000006b65792e696e7374616e636501"
            )
        );
        assert_eq!(
            json(&e),
            r#"{"epoch":1234,"op":"link_olh","op_tag":"op_tag","key":{"name":"key.name","instance":"key.instance"},"delete_marker":true}"#
        );
        assert_eq!(
            OlhLogEntry::decode(&mut &bytes(&e)[..], 0).expect("decode"),
            e
        );
    }

    fn olh_entry_instance() -> OlhEntry {
        OlhEntry {
            key: ObjKey {
                name: "key.name".to_owned(),
                instance: "key.instance".to_owned(),
            },
            delete_marker: true,
            epoch: 1234,
            pending_log: BTreeMap::new(),
            tag: "tag".to_owned(),
            exists: true,
            pending_removal: true,
        }
    }

    #[test]
    fn olh_entry_dumps_pending_log_as_key_val_pairs() {
        let e = olh_entry_instance();
        assert_eq!(
            bytes(&e),
            unhex(
                "01013800000001011c000000080000006b65792e6e616d650c0000006b65792e696e7374616e636501d20400000000000000000000030000007461670101"
            )
        );
        assert_eq!(
            json(&e),
            r#"{"key":{"name":"key.name","instance":"key.instance"},"delete_marker":true,"epoch":1234,"pending_log":[],"tag":"tag","exists":true,"pending_removal":true}"#
        );
        assert_eq!(OlhEntry::decode(&mut &bytes(&e)[..], 0).expect("decode"), e);

        let mut with_log = olh_entry_instance();
        with_log
            .pending_log
            .insert(5, vec![olh_log_entry_instance()]);
        assert_eq!(
            OlhEntry::decode(&mut &bytes(&with_log)[..], 0).expect("decode"),
            with_log
        );
        assert!(json(&with_log).contains(r#""pending_log":[{"key":5,"val":[{"epoch":1234,"op":"link_olh","op_tag":"op_tag","key":{"name":"key.name","instance":"key.instance"},"delete_marker":true}]}]"#));
    }

    /// `encoded` re-versioned to `version`, its content cut to `keep`
    /// bytes and `tail` appended, with the length rewritten.
    fn reframed(encoded: &[u8], version: u8, keep: usize, tail: &[u8]) -> Vec<u8> {
        let mut out = encoded[..6 + keep].to_vec();
        out[0] = version;
        out.extend_from_slice(tail);
        let len = u32::try_from(out.len() - 6).expect("fits");
        out[2..6].copy_from_slice(&len.to_le_bytes());
        out
    }

    fn key(name: &str) -> ObjKey {
        ObjKey {
            name: name.to_owned(),
            instance: String::new(),
        }
    }

    const LINK_OLH_WIRE: &str = "0501a100000001010c000000040000006e616d6500000000070000006f6c685f74616701060000006f705f74616707035300000001640000000000000000000000000000000400000065746167050000006f776e65720c000000646973706c6179206e616d650c000000636f6e74656e742f7479706500000000000000000000000000000000007b00000000000000010000000000000000000000000000000000000000000000";

    fn link_olh_instance() -> LinkOlhOp {
        LinkOlhOp {
            key: key("name"),
            olh_tag: "olh_tag".to_owned(),
            delete_marker: true,
            op_tag: "op_tag".to_owned(),
            meta: DirEntryMeta {
                category: ObjCategory::MAIN,
                size: 100,
                etag: "etag".to_owned(),
                owner: "owner".to_owned(),
                owner_display_name: "display name".to_owned(),
                content_type: "content/type".to_owned(),
                ..DirEntryMeta::default()
            },
            olh_epoch: 123,
            log_op: true,
            ..LinkOlhOp::default()
        }
    }

    #[test]
    fn link_olh_op_writes_unmod_since_seconds_first() {
        // Oracle rgw_cls_link_olh_op instance 1.
        let op = link_olh_instance();
        assert_eq!(bytes(&op), unhex(LINK_OLH_WIRE));
        assert_eq!(
            json(&op),
            r#"{"key":{"name":"name","instance":""},"olh_tag":"olh_tag","delete_marker":true,"op_tag":"op_tag","meta":{"category":1,"size":100,"mtime":"0.000000","etag":"etag","storage_class":"","owner":"owner","owner_display_name":"display name","content_type":"content/type","accounted_size":0,"user_data":"","appendable":false},"olh_epoch":123,"log_op":true,"bilog_flags":0,"unmod_since":"0.000000","high_precision_time":false,"zones_trace":[]}"#
        );
        assert_eq!(
            LinkOlhOp::decode(&mut &bytes(&op)[..], 0).expect("decode"),
            op
        );

        let timed = LinkOlhOp {
            unmod_since: UTime {
                sec: 0x0102_0304,
                nsec: 5,
            },
            ..link_olh_instance()
        };
        let wire = bytes(&timed);
        // olh_epoch, log_op and bilog_flags, then the seconds as a u64 and
        // the real_time as seconds and nanoseconds.
        let tail = unhex("7b0000000000000001000004030201000000000403020105000000");
        let at = wire
            .windows(tail.len())
            .position(|w| w == tail)
            .expect("seconds precede the real_time");
        assert_eq!(wire.len() - at - tail.len(), 5);
        assert_eq!(LinkOlhOp::decode(&mut &wire[..], 0).expect("decode"), timed);
    }

    #[test]
    fn link_olh_op_rejects_versions_below_squid() {
        // Version 2 ended with the seconds of unmod_since, 13 bytes short
        // of version 5's real_time, high_precision_time and zones_trace.
        let v5 = unhex(LINK_OLH_WIRE);
        let v2 = reframed(&v5, 2, v5.len() - 6 - 13, &[]);
        assert!(LinkOlhOp::decode(&mut &v2[..], 0).is_err());
    }

    #[test]
    fn unlink_instance_op_dumps_no_olh_tag() {
        // Oracle rgw_cls_unlink_instance_op instance 1.
        let op = UnlinkInstanceOp {
            key: key("name"),
            op_tag: "op_tag".to_owned(),
            olh_epoch: 124,
            log_op: true,
            ..UnlinkInstanceOp::default()
        };
        let wire = unhex(
            "03012f00000001010c000000040000006e616d6500000000060000006f705f7461677c000000000000000100000000000000000000",
        );
        assert_eq!(bytes(&op), wire);
        let want = r#"{"key":{"name":"name","instance":""},"op_tag":"op_tag","olh_epoch":124,"log_op":true,"bilog_flags":0,"zones_trace":[]}"#;
        assert_eq!(json(&op), want);
        let tagged = UnlinkInstanceOp {
            olh_tag: "olh_tag".to_owned(),
            ..op.clone()
        };
        assert_eq!(json(&tagged), want);
        assert_eq!(
            UnlinkInstanceOp::decode(&mut &bytes(&tagged)[..], 0).expect("decode"),
            tagged
        );
        // Version 2 lacked zones_trace (the trailing four bytes).
        let v2 = reframed(&wire, 2, wire.len() - 6 - 4, &[]);
        assert!(UnlinkInstanceOp::decode(&mut &v2[..], 0).is_err());
    }

    #[test]
    fn read_olh_log_op_is_sent_as_version_one() {
        // Oracle rgw_cls_read_olh_log_op instance 1.
        let op = ReadOlhLogOp {
            olh: key("name"),
            ver_marker: 123,
            olh_tag: "olh_tag".to_owned(),
        };
        let wire = unhex(
            "01012500000001010c000000040000006e616d65000000007b00000000000000070000006f6c685f746167",
        );
        assert_eq!(bytes(&op), wire);
        assert_eq!(
            json(&op),
            r#"{"olh":{"name":"name","instance":""},"ver_marker":123,"olh_tag":"olh_tag"}"#
        );
        assert_eq!(ReadOlhLogOp::decode(&mut &wire[..], 0).expect("decode"), op);
        // main's version 2 appends get_stales.
        let v2 = reframed(&wire, 2, wire.len() - 6, &[1]);
        assert_eq!(ReadOlhLogOp::decode(&mut &v2[..], 0).expect("v2"), op);
    }

    #[test]
    fn read_olh_log_ret_dumps_the_log_as_key_val_pairs() {
        // Oracle rgw_cls_read_olh_log_ret instance 1.
        let ret = ReadOlhLogRet {
            log: BTreeMap::from([(1, vec![olh_log_entry_instance()])]),
            is_truncated: true,
        };
        assert_eq!(
            bytes(&ret),
            unhex(
                "01014d00000001000000010000000000000001000000010136000000d20400000000000001060000006f705f74616701011c000000080000006b65792e6e616d650c0000006b65792e696e7374616e63650101"
            )
        );
        assert_eq!(
            json(&ret),
            r#"{"log":[{"key":1,"val":[{"epoch":1234,"op":"link_olh","op_tag":"op_tag","key":{"name":"key.name","instance":"key.instance"},"delete_marker":true}]}],"is_truncated":true}"#
        );
        assert_eq!(
            ReadOlhLogRet::decode(&mut &bytes(&ret)[..], 0).expect("decode"),
            ret
        );
    }

    #[test]
    fn trim_olh_log_op_and_clear_olh_op_dump_plainly() {
        // Oracle rgw_cls_trim_olh_log_op and rgw_cls_bucket_clear_olh_op
        // instance 1.
        let trim = TrimOlhLogOp {
            olh: key("olh.name"),
            ver: 100,
            olh_tag: "olh_tag".to_owned(),
        };
        assert_eq!(
            bytes(&trim),
            unhex(
                "010129000000010110000000080000006f6c682e6e616d65000000006400000000000000070000006f6c685f746167"
            )
        );
        assert_eq!(
            json(&trim),
            r#"{"olh":{"name":"olh.name","instance":""},"ver":100,"olh_tag":"olh_tag"}"#
        );
        assert_eq!(
            TrimOlhLogOp::decode(&mut &bytes(&trim)[..], 0).expect("decode"),
            trim
        );

        let clear = ClearOlhOp {
            key: key("key.name"),
            olh_tag: "olh_tag".to_owned(),
        };
        assert_eq!(
            bytes(&clear),
            unhex("010121000000010110000000080000006b65792e6e616d6500000000070000006f6c685f746167")
        );
        assert_eq!(
            json(&clear),
            r#"{"key":{"name":"key.name","instance":""},"olh_tag":"olh_tag"}"#
        );
        assert_eq!(
            ClearOlhOp::decode(&mut &bytes(&clear)[..], 0).expect("decode"),
            clear
        );
    }

    #[test]
    fn decode_read_olh_log_unwraps_the_reply() {
        let ret = ReadOlhLogRet {
            log: BTreeMap::from([(7, vec![olh_log_entry_instance()])]),
            is_truncated: false,
        };
        let reply = rados::OpReply {
            return_code: 0,
            outdata: encode_with_capacity(&ret, 0).expect("encode"),
        };
        assert_eq!(decode_read_olh_log(&reply).expect("decode"), ret);
    }

    fn call_indata(method: &str, req: &[u8]) -> Vec<u8> {
        let mut want = format!("rgw{method}").into_bytes();
        want.extend_from_slice(req);
        want
    }

    #[test]
    fn ops_name_the_rgw_class_and_their_method() {
        use rados::osdclient::types::OpData;

        let unlink = UnlinkInstanceOp {
            key: key("name"),
            op_tag: "op_tag".to_owned(),
            olh_epoch: 124,
            log_op: true,
            ..UnlinkInstanceOp::default()
        };
        let cases = [
            (
                link_olh_op(&link_olh_instance()),
                "bucket_link_olh",
                unhex(LINK_OLH_WIRE),
            ),
            (
                unlink_instance_op(&unlink),
                "bucket_unlink_instance",
                unhex(
                    "03012f00000001010c000000040000006e616d6500000000060000006f705f7461677c000000000000000100000000000000000000",
                ),
            ),
            (
                read_olh_log_op(&key("name"), 123, "olh_tag"),
                "bucket_read_olh_log",
                unhex(
                    "01012500000001010c000000040000006e616d65000000007b00000000000000070000006f6c685f746167",
                ),
            ),
            (
                trim_olh_log_op(&key("olh.name"), 100, "olh_tag"),
                "bucket_trim_olh_log",
                unhex(
                    "010129000000010110000000080000006f6c682e6e616d65000000006400000000000000070000006f6c685f746167",
                ),
            ),
            (
                clear_olh_op(&key("key.name"), "olh_tag"),
                "bucket_clear_olh",
                unhex(
                    "010121000000010110000000080000006b65792e6e616d6500000000070000006f6c685f746167",
                ),
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
