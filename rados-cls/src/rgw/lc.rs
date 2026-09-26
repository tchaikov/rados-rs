//! `cls_rgw_types.h`: the lifecycle entry and the per-shard head the
//! `rgw` class's `lc_*` methods write. Those methods come with a later
//! plan.

use rados::VersionedDenc;
use serde::Serialize;

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
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 2, compat = 2)]
pub struct LcObjHead {
    pub start_date: i64,
    pub marker: String,
    #[serde(skip)]
    pub shard_rollover_date: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rados::{Denc, encode_with_capacity};

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
}
