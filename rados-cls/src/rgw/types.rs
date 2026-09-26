//! `cls_rgw_types.h`: the pieces every rgw-side class shares. This file
//! holds what the GC classes need; the bucket index, usage, lifecycle
//! and OLH types join it with their classes.

use std::collections::BTreeSet;

use bytes::{Buf, BufMut};
use rados::{Denc, RadosError, UTime, VersionedDenc, VersionedEncode};
use serde::Serialize;
use serde::ser::SerializeStruct;

use super::packed;

/// `cls_rgw_obj_key` (`rgw_obj_index_key`): an object's name and, when it
/// is a version, its instance.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct ObjKey {
    pub name: String,
    pub instance: String,
}

/// `cls_rgw_obj`: one object of a GC chain. Version 2 appended the full
/// key after the three strings version 1 wrote, so `key.name` is on the
/// wire twice; the decoder floors at version 2. The dump calls `key.name`
/// "oid" and `loc` "key".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Obj {
    pub pool: String,
    pub key: ObjKey,
    pub loc: String,
}

impl VersionedEncode for Obj {
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
        self.pool.encode(buf, features)?;
        self.key.name.encode(buf, features)?;
        self.loc.encode(buf, features)?;
        self.key.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 2, "Obj", "Hammer v0.94+");

        let pool = String::decode(buf, features)?;
        let _name = String::decode(buf, features)?;
        let loc = String::decode(buf, features)?;
        let key = ObjKey::decode(buf, features)?;
        Ok(Self { pool, key, loc })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

rados::impl_denc_for_versioned!(Obj);

impl Serialize for Obj {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("Obj", 4)?;
        state.serialize_field("pool", &self.pool)?;
        state.serialize_field("oid", &self.key.name)?;
        state.serialize_field("key", &self.loc)?;
        state.serialize_field("instance", &self.key.instance)?;
        state.end()
    }
}

/// `cls_rgw_obj_chain`: the objects one GC entry deletes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct ObjChain {
    pub objs: Vec<Obj>,
}

/// `cls_rgw_gc_obj_info`: a GC entry: the tag naming it, the chain to
/// delete, and when it falls due. This is the payload of every `rgw_gc`
/// queue entry.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct GcObjInfo {
    pub tag: String,
    pub chain: ObjChain,
    #[serde(serialize_with = "crate::dump::real_time")]
    pub time: UTime,
}

/// A one-byte Ceph enum. Ceph decodes any byte and later releases add
/// values, so the newtype keeps the byte; the constants name the values
/// v19 knows.
macro_rules! byte_enum {
    ($(#[$doc:meta])* $name:ident { $($k:ident = $v:expr),* $(,)? }) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(pub u8);

        impl $name {
            $(pub const $k: Self = Self($v);)*
        }

        impl Denc for $name {
            fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> std::result::Result<(), RadosError> {
                self.0.encode(buf, features)
            }

            fn decode<B: Buf>(buf: &mut B, features: u64) -> std::result::Result<Self, RadosError> {
                Ok(Self(u8::decode(buf, features)?))
            }

            fn encoded_size(&self, _features: u64) -> Option<usize> {
                Some(1)
            }
        }
    };
}
pub(crate) use byte_enum;

byte_enum! {
    /// `RGWObjCategory`: what a bucket-index entry accounts for. Dumps as a number.
    ObjCategory { NONE = 0, MAIN = 1, SHADOW = 2, MULTI_META = 3, CLOUD_TIERED = 4 }
}

byte_enum! {
    /// `RGWModifyOp`: the operation a bilog entry or pending change records.
    ModifyOp {
        ADD = 0, DEL = 1, CANCEL = 2, UNKNOWN = 3, LINK_OLH = 4, LINK_OLH_DM = 5,
        UNLINK_INSTANCE = 6, SYNCSTOP = 7, RESYNC = 8,
    }
}

impl ModifyOp {
    /// `to_string(RGWModifyOp)`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ADD => "write",
            Self::DEL => "del",
            Self::CANCEL => "cancel",
            Self::LINK_OLH => "link_olh",
            Self::LINK_OLH_DM => "link_olh_del",
            Self::UNLINK_INSTANCE => "unlink_instance",
            Self::SYNCSTOP => "syncstop",
            Self::RESYNC => "resync",
            _ => "unknown",
        }
    }
}

byte_enum! {
    /// `RGWPendingState`.
    PendingState { PENDING_MODIFY = 0, COMPLETE = 1, UNKNOWN = 2 }
}

byte_enum! {
    /// `BIIndexType`: which struct a bucket-index entry's payload holds.
    BiIndexType { INVALID = 0, PLAIN = 1, INSTANCE = 2, OLH = 3 }
}

impl BiIndexType {
    /// `to_string(BIIndexType)`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PLAIN => "plain",
            Self::INSTANCE => "instance",
            Self::OLH => "olh",
            _ => "invalid",
        }
    }
}

byte_enum! {
    /// `cls_rgw_reshard_status`.
    ReshardStatus { NOT_RESHARDING = 0, IN_PROGRESS = 1, DONE = 2 }
}

impl ReshardStatus {
    /// `to_string(cls_rgw_reshard_status)`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NOT_RESHARDING => "not-resharding",
            Self::IN_PROGRESS => "in-progress",
            Self::DONE => "done",
            _ => "Unknown reshard status",
        }
    }
}

byte_enum! {
    /// `RGWCheckMTimeType`: how `obj_check_mtime` compares the object's
    /// mtime (left) with the request's (right).
    CheckMtimeType { EQ = 0, LT = 1, LE = 2, GT = 3, GE = 4 }
}

/// `rgw_bucket_dir_entry` flag bits.
pub const FLAG_VER: u16 = 0x1;
pub const FLAG_CURRENT: u16 = 0x2;
pub const FLAG_DELETE_MARKER: u16 = 0x4;
pub const FLAG_VER_MARKER: u16 = 0x8;
pub const FLAG_COMMON_PREFIX: u16 = 0x8000;

/// `cls_rgw_get_rounded_size`: up to the next 4 KiB block, wrapping as
/// the C++ does (a decoded `accounted_size` is untrusted).
pub fn rounded_size(size: u64) -> u64 {
    size.wrapping_add(4095) & !4095
}

/// `rgw_bucket_entry_ver`: both fields packed; `pool` is `-1` when unset
/// and, cast to unsigned, takes the eight-byte form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct EntryVer {
    pub pool: i64,
    pub epoch: u64,
}

impl Default for EntryVer {
    fn default() -> Self {
        Self { pool: -1, epoch: 0 }
    }
}

impl VersionedEncode for EntryVer {
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
        _features: u64,
        _version: u8,
    ) -> std::result::Result<(), RadosError> {
        packed::encode(self.pool as u64, buf);
        packed::encode(self.epoch, buf);
        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        _features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 1, "EntryVer", "Dumpling v0.67+");
        let pool = packed::decode(buf)? as i64;
        let epoch = packed::decode(buf)?;
        Ok(Self { pool, epoch })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        Some(packed::encoded_size(self.pool as u64) + packed::encoded_size(self.epoch))
    }
}

rados::impl_denc_for_versioned!(EntryVer);

/// `rgw_bucket_pending_info`: a change the class has prepared but not
/// completed. Version 2 (compat 2); `op` holds an `RGWModifyOp` value.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 2, compat = 2)]
pub struct PendingInfo {
    pub state: PendingState,
    #[serde(serialize_with = "crate::dump::utime")]
    pub timestamp: UTime,
    pub op: u8,
}

/// `rgw_bucket_category_stats`. Version 3 (compat 2) added `actual_size`,
/// read only when `struct_v >= 3`; the decoder floors at version 3 and
/// does not read the version-2 form.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct CategoryStats {
    pub total_size: u64,
    pub total_size_rounded: u64,
    pub num_entries: u64,
    pub actual_size: u64,
}

impl VersionedEncode for CategoryStats {
    const MAX_DECODE_VERSION: u8 = 3;

    fn encoding_version(&self, _features: u64) -> u8 {
        3
    }

    fn compat_version(&self, _features: u64) -> u8 {
        2
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> std::result::Result<(), RadosError> {
        self.total_size.encode(buf, features)?;
        self.total_size_rounded.encode(buf, features)?;
        self.num_entries.encode(buf, features)?;
        self.actual_size.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 3, "CategoryStats", "Kraken v11+");

        let total_size = u64::decode(buf, features)?;
        let total_size_rounded = u64::decode(buf, features)?;
        let num_entries = u64::decode(buf, features)?;
        let actual_size = u64::decode(buf, features)?;
        Ok(Self {
            total_size,
            total_size_rounded,
            num_entries,
            actual_size,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        Some(32)
    }
}

rados::impl_denc_for_versioned!(CategoryStats);

/// `rgw_zone_set_entry`: `zone[:location_key]` on the wire as one string,
/// split at the first colon; ordered by zone then key with no key first
/// (`Option<String>`'s `Ord` puts `None` before `Some`, as `std::optional`
/// does).
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct ZoneSetEntry {
    pub zone: String,
    pub location_key: Option<String>,
}

/// `rgw_zone_set_entry::from_str`: split at the first colon into a zone and
/// an optional location key. Every string parses.
impl std::str::FromStr for ZoneSetEntry {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(match s.split_once(':') {
            Some((zone, key)) => Self {
                zone: zone.to_owned(),
                location_key: Some(key.to_owned()),
            },
            None => Self {
                zone: s.to_owned(),
                location_key: None,
            },
        })
    }
}

impl ZoneSetEntry {
    /// `zone`, or `zone:location_key` when the key is set.
    pub fn to_str(&self) -> String {
        match &self.location_key {
            Some(key) => format!("{}:{key}", self.zone),
            None => self.zone.clone(),
        }
    }
}

impl Denc for ZoneSetEntry {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> std::result::Result<(), RadosError> {
        self.to_str().encode(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> std::result::Result<Self, RadosError> {
        let Ok(entry) = String::decode(buf, features)?.parse();
        Ok(entry)
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        self.to_str().encoded_size(features)
    }
}

impl Serialize for ZoneSetEntry {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("ZoneSetEntry", 1)?;
        state.serialize_field("entry", &self.to_str())?;
        state.end()
    }
}

/// `rgw_zone_set`: the zones a change has passed through. No struct
/// header: a `u32` count then each entry's string, in entry order (the
/// entries are a `BTreeSet`, so that order is sorted).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ZoneSet {
    pub entries: BTreeSet<ZoneSetEntry>,
}

impl ZoneSet {
    pub fn insert(&mut self, zone: &str, location_key: Option<&str>) {
        self.entries.insert(ZoneSetEntry {
            zone: zone.to_owned(),
            location_key: location_key.map(str::to_owned),
        });
    }

    pub fn exists(&self, zone: &str, location_key: Option<&str>) -> bool {
        self.entries.contains(&ZoneSetEntry {
            zone: zone.to_owned(),
            location_key: location_key.map(str::to_owned),
        })
    }
}

impl Denc for ZoneSet {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> std::result::Result<(), RadosError> {
        self.entries.encode(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> std::result::Result<Self, RadosError> {
        Ok(Self {
            entries: BTreeSet::<ZoneSetEntry>::decode(buf, features)?,
        })
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        self.entries.encoded_size(features)
    }
}

impl Serialize for ZoneSet {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("ZoneSet", 1)?;
        state.serialize_field("entries", &self.entries)?;
        state.end()
    }
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

    #[test]
    fn entry_ver_packs_both_fields() {
        // Oracle rgw_bucket_entry_ver test 0: pool 123, epoch 12322.
        let v = EntryVer {
            pool: 123,
            epoch: 12_322,
        };
        assert_eq!(bytes(&v), unhex("0101040000007b822230"));
        assert_eq!(json(&v), r#"{"pool":123,"epoch":12322}"#);
        // Default: pool -1 takes the 8-byte form.
        assert_eq!(
            bytes(&EntryVer::default()),
            unhex("01010a00000088ffffffffffffffff00")
        );
        assert_eq!(
            EntryVer::decode(&mut &bytes(&EntryVer::default())[..], 0).expect("decode"),
            EntryVer::default()
        );
    }

    #[test]
    fn pending_info_is_state_time_op() {
        // Oracle rgw_bucket_pending_info test 1: complete, epoch, op del.
        let p = PendingInfo {
            state: PendingState::COMPLETE,
            timestamp: UTime::default(),
            op: 1,
        };
        assert_eq!(bytes(&p), unhex("02020a00000001000000000000000001"));
        assert_eq!(json(&p), r#"{"state":1,"timestamp":"0.000000","op":1}"#);
    }

    #[test]
    fn category_stats_floors_at_three() {
        let s = CategoryStats {
            total_size: 1024,
            total_size_rounded: 4096,
            num_entries: 2,
            actual_size: 1024,
        };
        assert_eq!(
            bytes(&s),
            unhex("0302200000000004000000000000001000000000000002000000000000000004000000000000")
        );
        assert_eq!(
            json(&s),
            r#"{"total_size":1024,"total_size_rounded":4096,"num_entries":2,"actual_size":1024}"#
        );
        // Version 2 had no actual_size; it is below the floor.
        let v2 = unhex("0202180000000004000000000000001000000000000002000000000000");
        assert!(CategoryStats::decode(&mut &v2[..], 0).is_err());
    }

    #[test]
    fn zone_set_is_bare_strings_in_entry_order() {
        // Oracle rgw_zone_set test 0.
        let mut z = ZoneSet::default();
        for zone in ["zone1", "zone2", "zone3"] {
            z.insert(zone, Some("loc_key"));
        }
        assert_eq!(
            bytes(&z),
            unhex(
                "030000000d0000007a6f6e65313a6c6f635f6b65790d0000007a6f6e65323a6c6f635f6b65790d0000007a6f6e65333a6c6f635f6b6579"
            )
        );
        assert_eq!(
            json(&z),
            r#"{"entries":[{"entry":"zone1:loc_key"},{"entry":"zone2:loc_key"},{"entry":"zone3:loc_key"}]}"#
        );
        assert_eq!(ZoneSet::decode(&mut &bytes(&z)[..], 0).expect("decode"), z);
        // A key-less zone sorts before the same zone with a key; the split is at the first colon.
        let e: ZoneSetEntry = "z:a:b".parse().expect("infallible");
        assert_eq!(
            (e.zone.as_str(), e.location_key.as_deref()),
            ("z", Some("a:b"))
        );
        assert!("z".parse::<ZoneSetEntry>().expect("infallible") < e);
        assert!(z.exists("zone2", Some("loc_key")) && !z.exists("zone2", None));
    }

    #[test]
    fn enums_keep_unknown_bytes_and_name_the_known_ones() {
        assert_eq!(ModifyOp(9).as_str(), "unknown");
        assert_eq!(ModifyOp::LINK_OLH_DM.as_str(), "link_olh_del");
        assert_eq!(BiIndexType(7).as_str(), "invalid");
        assert_eq!(ReshardStatus::IN_PROGRESS.as_str(), "in-progress");
        assert_eq!(json(&ObjCategory::MAIN), "1");
        assert_eq!(bytes(&ObjCategory::CLOUD_TIERED), [4]);
        assert_eq!(rounded_size(1), 4096);
        assert_eq!(rounded_size(4096), 4096);
        assert_eq!(rounded_size(0), 0);
    }

    #[test]
    fn obj_key_is_two_strings() {
        // Corpus cls_rgw_obj_key/eedab0b3...
        let k = ObjKey {
            name: "name".to_owned(),
            instance: "instance".to_owned(),
        };
        assert_eq!(
            bytes(&k),
            b"\x01\x01\x14\x00\x00\x00\x04\x00\x00\x00name\x08\x00\x00\x00instance"
        );
        assert_eq!(json(&k), r#"{"name":"name","instance":"instance"}"#);
    }

    #[test]
    fn obj_writes_the_name_twice_and_dumps_loc_as_key() {
        // Corpus cls_rgw_obj/bfcd58c2...: pool "mypool", key.name "myoid",
        // loc "mykey", no instance; 53 bytes, version 2 over compat 1.
        let o = Obj {
            pool: "mypool".to_owned(),
            key: ObjKey {
                name: "myoid".to_owned(),
                instance: String::new(),
            },
            loc: "mykey".to_owned(),
        };
        let wire = b"\x02\x01\x2f\x00\x00\x00\x06\x00\x00\x00mypool\x05\x00\x00\x00myoid\x05\x00\x00\x00mykey\x01\x01\x0d\x00\x00\x00\x05\x00\x00\x00myoid\x00\x00\x00\x00";
        assert_eq!(bytes(&o), wire);
        assert_eq!(Obj::decode(&mut &wire[..], 0).expect("decode"), o);
        assert_eq!(
            json(&o),
            r#"{"pool":"mypool","oid":"myoid","key":"mykey","instance":""}"#
        );

        // A version-1 writer sent only the three strings; the decoder
        // floors at version 2 and rejects it.
        let v1 = b"\x01\x01\x0f\x00\x00\x00\x01\x00\x00\x00p\x01\x00\x00\x00n\x01\x00\x00\x00l";
        assert!(Obj::decode(&mut &v1[..], 0).is_err());
    }

    #[test]
    fn gc_obj_info_matches_the_corpus_instance() {
        // Corpus cls_rgw_gc_obj_info/fd3d3891...: tag "footag", empty chain,
        // time {21, 32}; 34 bytes.
        let info = GcObjInfo {
            tag: "footag".to_owned(),
            chain: ObjChain::default(),
            time: UTime { sec: 21, nsec: 32 },
        };
        assert_eq!(
            bytes(&info),
            b"\x01\x01\x1c\x00\x00\x00\x06\x00\x00\x00footag\x01\x01\x04\x00\x00\x00\x00\x00\x00\x00\x15\x00\x00\x00\x20\x00\x00\x00"
        );
        assert_eq!(
            json(&info),
            r#"{"tag":"footag","chain":{"objs":[]},"time":"1970-01-01T00:00:21.000000+0000"}"#
        );
        assert_eq!(
            GcObjInfo::decode(&mut &bytes(&info)[..], 0).expect("decode"),
            info
        );
    }

    #[test]
    fn json_keeps_a_nul_in_a_tag() {
        // cls_rgw_gc_obj_info::dump uses the whole std::string, unlike
        // obj_refcount; the corpus tags end in a NUL and dencoder prints it.
        let info = GcObjInfo {
            tag: "t\0".to_owned(),
            ..GcObjInfo::default()
        };
        assert!(json(&info).starts_with(r#"{"tag":"t\u0000""#));
    }
}
