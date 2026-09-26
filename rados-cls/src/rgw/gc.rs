//! The `rgw` class's GC request and reply structs (`cls_rgw_ops.h`).
//! The omap-era GC methods send them; the `rgw_gc` class reuses
//! [`SetEntryOp`], [`ListOp`] and [`ListRet`] for its queue.

use bytes::{Buf, BufMut};
use rados::{Denc, RadosError, VersionedDenc, VersionedEncode};
use serde::Serialize;

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
}
