//! `cls_rgw_types.h`: the OLH (object logical head) log entry and entry
//! that back RGW's object versioning. The `rgw` class methods that read
//! and write them (`bucket_link_olh`, `bucket_unlink_instance`,
//! `bucket_read_olh_log`, `bucket_trim_olh_log`, `bucket_clear_olh`)
//! come with a later plan.

use std::collections::BTreeMap;

use bytes::{Buf, BufMut};
use rados::{Denc, RadosError, VersionedDenc};
use serde::Serialize;

use super::types::{ObjKey, byte_enum};

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

#[cfg(test)]
mod tests {
    use super::*;
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
}
