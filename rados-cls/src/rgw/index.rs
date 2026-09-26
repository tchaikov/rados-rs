//! `cls_rgw_types.h`: the bucket-index entry and its parts, the dir
//! header and dir, the bilog entry, and the bucket-instance and reshard
//! entries. The bucket-index class methods come with a later plan; this
//! file only holds the types the OSD class and RGW exchange.

use std::collections::BTreeMap;

use bytes::{Buf, BufMut, Bytes};
use rados::{Denc, OmapKey, RadosError, UTime, VersionedDenc, VersionedEncode};
use serde::Serialize;
use serde::ser::{SerializeSeq, SerializeStruct};

use super::olh::OlhEntry;
use super::packed;
use super::types::{
    BiIndexType, CategoryStats, EntryVer, FLAG_COMMON_PREFIX, FLAG_CURRENT, FLAG_DELETE_MARKER,
    FLAG_VER, FLAG_VER_MARKER, ModifyOp, ObjCategory, ObjKey, PendingInfo, PendingState,
    ReshardStatus, ZoneSet, rounded_size,
};

/// `rgw_bucket_dir_entry_meta`: what the bucket index caches about an
/// object's content. Squid v19.2.2 writes version 7 (compat 3); version
/// 8's restore fields are not modelled and are skipped on decode.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DirEntryMeta {
    pub category: ObjCategory,
    pub size: u64,
    pub mtime: UTime,
    pub etag: String,
    pub owner: String,
    pub owner_display_name: String,
    pub content_type: String,
    pub accounted_size: u64,
    pub user_data: String,
    pub storage_class: String,
    pub appendable: bool,
}

impl VersionedEncode for DirEntryMeta {
    const MAX_DECODE_VERSION: u8 = 8;

    fn encoding_version(&self, _features: u64) -> u8 {
        7
    }

    fn compat_version(&self, _features: u64) -> u8 {
        3
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> std::result::Result<(), RadosError> {
        self.category.encode(buf, features)?;
        self.size.encode(buf, features)?;
        self.mtime.encode(buf, features)?;
        self.etag.encode(buf, features)?;
        self.owner.encode(buf, features)?;
        self.owner_display_name.encode(buf, features)?;
        self.content_type.encode(buf, features)?;
        self.accounted_size.encode(buf, features)?;
        self.user_data.encode(buf, features)?;
        self.storage_class.encode(buf, features)?;
        self.appendable.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 7, "DirEntryMeta", "Nautilus v14+");

        let category = ObjCategory::decode(buf, features)?;
        let size = u64::decode(buf, features)?;
        let mtime = UTime::decode(buf, features)?;
        let etag = String::decode(buf, features)?;
        let owner = String::decode(buf, features)?;
        let owner_display_name = String::decode(buf, features)?;
        let content_type = String::decode(buf, features)?;
        let accounted_size = u64::decode(buf, features)?;
        let user_data = String::decode(buf, features)?;
        let storage_class = String::decode(buf, features)?;
        let appendable = bool::decode(buf, features)?;
        Ok(Self {
            category,
            size,
            mtime,
            etag,
            owner,
            owner_display_name,
            content_type,
            accounted_size,
            user_data,
            storage_class,
            appendable,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

rados::impl_denc_for_versioned!(DirEntryMeta);

/// A [`UTime`] dumped through [`crate::dump::utime`] (`utime_t::gmtime`),
/// for use where a value, not a `serialize_with` function, is needed.
struct DumpUtime<'a>(&'a UTime);

impl Serialize for DumpUtime<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        crate::dump::utime(self.0, serializer)
    }
}

/// A [`UTime`] dumped through [`crate::dump::utime_nsec`]
/// (`utime_t::gmtime_nsec`), as `rgw_bi_log_entry::timestamp` streams it.
struct DumpUtimeNsec<'a>(&'a UTime);

impl Serialize for DumpUtimeNsec<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        crate::dump::utime_nsec(self.0, serializer)
    }
}

impl Serialize for DirEntryMeta {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("DirEntryMeta", 11)?;
        state.serialize_field("category", &self.category)?;
        state.serialize_field("size", &self.size)?;
        state.serialize_field("mtime", &DumpUtime(&self.mtime))?;
        state.serialize_field("etag", &self.etag)?;
        state.serialize_field("storage_class", &self.storage_class)?;
        state.serialize_field("owner", &self.owner)?;
        state.serialize_field("owner_display_name", &self.owner_display_name)?;
        state.serialize_field("content_type", &self.content_type)?;
        state.serialize_field("accounted_size", &self.accounted_size)?;
        state.serialize_field("user_data", &self.user_data)?;
        state.serialize_field("appendable", &self.appendable)?;
        state.end()
    }
}

/// `Vec<(String, PendingInfo)>` dumped as `encode_json` dumps
/// `rgw_bucket_dir_entry::pending_map` (a `std::multimap`): an array of
/// `{"key", "val"}` objects.
fn pending_map<S: serde::Serializer>(
    m: &[(String, PendingInfo)],
    s: S,
) -> std::result::Result<S::Ok, S::Error> {
    crate::dump::map_entries(m.iter().map(|(k, v)| (k, v)), s)
}

/// `rgw_bucket_dir_entry`: one object (or object instance) in a bucket
/// index. `ver.epoch` is written twice on the wire (a legacy plain `u64`
/// right after the name, then again inside `ver` itself with `pool`);
/// the key's `name` and `instance` are split across the wire, `instance`
/// arriving only after `tag`. `index_ver` is packed and not dumped.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct DirEntry {
    #[serde(flatten)]
    pub key: ObjKey,
    pub ver: EntryVer,
    pub locator: String,
    pub exists: bool,
    pub meta: DirEntryMeta,
    pub tag: String,
    pub flags: u16,
    /// Encoded in `Vec` order: keep it sorted by tag, as the C++
    /// multimap is, or the bytes differ from Ceph's. Decode yields it
    /// sorted.
    #[serde(serialize_with = "pending_map")]
    pub pending_map: Vec<(String, PendingInfo)>,
    pub versioned_epoch: u64,
    #[serde(skip)]
    pub index_ver: u64,
}

impl VersionedEncode for DirEntry {
    const MAX_DECODE_VERSION: u8 = 8;

    fn encoding_version(&self, _features: u64) -> u8 {
        8
    }

    fn compat_version(&self, _features: u64) -> u8 {
        3
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> std::result::Result<(), RadosError> {
        self.key.name.encode(buf, features)?;
        self.ver.epoch.encode(buf, features)?;
        self.exists.encode(buf, features)?;
        self.meta.encode(buf, features)?;
        (self.pending_map.len() as u32).encode(buf, features)?;
        for (k, v) in &self.pending_map {
            k.encode(buf, features)?;
            v.encode(buf, features)?;
        }
        self.locator.encode(buf, features)?;
        self.ver.encode(buf, features)?;
        packed::encode(self.index_ver, buf);
        self.tag.encode(buf, features)?;
        self.key.instance.encode(buf, features)?;
        self.flags.encode(buf, features)?;
        self.versioned_epoch.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 8, "DirEntry", "Hammer v0.94+");

        let name = String::decode(buf, features)?;
        let _epoch_legacy = u64::decode(buf, features)?;
        let exists = bool::decode(buf, features)?;
        let meta = DirEntryMeta::decode(buf, features)?;
        let count = u32::decode(buf, features)? as usize;
        let mut pending_map = Vec::with_capacity(count.min(4096));
        for _ in 0..count {
            let k = String::decode(buf, features)?;
            let v = PendingInfo::decode(buf, features)?;
            pending_map.push((k, v));
        }
        let locator = String::decode(buf, features)?;
        let ver = EntryVer::decode(buf, features)?;
        let index_ver = packed::decode(buf)?;
        let tag = String::decode(buf, features)?;
        let instance = String::decode(buf, features)?;
        let flags = u16::decode(buf, features)?;
        let versioned_epoch = u64::decode(buf, features)?;
        Ok(Self {
            key: ObjKey { name, instance },
            ver,
            locator,
            exists,
            meta,
            pending_map,
            index_ver,
            tag,
            flags,
            versioned_epoch,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

rados::impl_denc_for_versioned!(DirEntry);

impl DirEntry {
    /// `rgw_bucket_dir_entry::is_current`.
    pub fn is_current(&self) -> bool {
        let test_flags = FLAG_VER | FLAG_CURRENT;
        (self.flags & FLAG_VER) == 0 || (self.flags & test_flags) == test_flags
    }

    /// `rgw_bucket_dir_entry::is_delete_marker`.
    pub fn is_delete_marker(&self) -> bool {
        self.flags & FLAG_DELETE_MARKER != 0
    }

    /// `rgw_bucket_dir_entry::is_visible`.
    pub fn is_visible(&self) -> bool {
        self.is_current() && !self.is_delete_marker()
    }

    /// `rgw_bucket_dir_entry::is_valid`.
    pub fn is_valid(&self) -> bool {
        self.flags & FLAG_VER_MARKER == 0
    }

    /// `rgw_bucket_dir_entry::is_common_prefix`.
    pub fn is_common_prefix(&self) -> bool {
        self.flags & FLAG_COMMON_PREFIX != 0
    }
}

/// `cls_rgw_bucket_instance_entry`: the reshard status the dir header
/// caches for the bucket's current instance. The wire also carries an
/// empty legacy string and a `-1` `i32` after the status (fields removed
/// in version 2 and restored, empty, in version 3); the decoder floors
/// at version 3 and so always reads them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BucketInstanceEntry {
    pub reshard_status: ReshardStatus,
}

impl VersionedEncode for BucketInstanceEntry {
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
        self.reshard_status.encode(buf, features)?;
        String::new().encode(buf, features)?;
        (-1i32).encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 3, "BucketInstanceEntry", "Reef v18+");

        let reshard_status = ReshardStatus::decode(buf, features)?;
        let _bucket_instance_id = String::decode(buf, features)?;
        let _num_shards = i32::decode(buf, features)?;
        Ok(Self { reshard_status })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

rados::impl_denc_for_versioned!(BucketInstanceEntry);

impl BucketInstanceEntry {
    pub fn resharding(&self) -> bool {
        self.reshard_status != ReshardStatus::NOT_RESHARDING
    }

    pub fn resharding_in_progress(&self) -> bool {
        self.reshard_status == ReshardStatus::IN_PROGRESS
    }
}

impl Serialize for BucketInstanceEntry {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("BucketInstanceEntry", 1)?;
        state.serialize_field("reshard_status", self.reshard_status.as_str())?;
        state.end()
    }
}

/// `rgw_bucket_dir_header`: the dir's running stats and bookkeeping.
/// Squid v19.2.2 writes version 7 (compat 2); version 8's
/// `reshardlog_entries` is not modelled and is skipped on decode.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DirHeader {
    pub stats: BTreeMap<ObjCategory, CategoryStats>,
    pub tag_timeout: u64,
    pub ver: u64,
    pub master_ver: u64,
    pub max_marker: String,
    pub new_instance: BucketInstanceEntry,
    pub syncstopped: bool,
}

impl VersionedEncode for DirHeader {
    const MAX_DECODE_VERSION: u8 = 8;

    fn encoding_version(&self, _features: u64) -> u8 {
        7
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
        self.stats.encode(buf, features)?;
        self.tag_timeout.encode(buf, features)?;
        self.ver.encode(buf, features)?;
        self.master_ver.encode(buf, features)?;
        self.max_marker.encode(buf, features)?;
        self.new_instance.encode(buf, features)?;
        self.syncstopped.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 7, "DirHeader", "Luminous v12+");

        let stats = BTreeMap::<ObjCategory, CategoryStats>::decode(buf, features)?;
        let tag_timeout = u64::decode(buf, features)?;
        let ver = u64::decode(buf, features)?;
        let master_ver = u64::decode(buf, features)?;
        let max_marker = String::decode(buf, features)?;
        let new_instance = BucketInstanceEntry::decode(buf, features)?;
        let syncstopped = bool::decode(buf, features)?;
        Ok(Self {
            stats,
            tag_timeout,
            ver,
            master_ver,
            max_marker,
            new_instance,
            syncstopped,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

rados::impl_denc_for_versioned!(DirHeader);

impl DirHeader {
    pub fn resharding(&self) -> bool {
        self.new_instance.resharding()
    }

    pub fn resharding_in_progress(&self) -> bool {
        self.new_instance.resharding_in_progress()
    }
}

/// `rgw_bucket_dir_header::stats` dumps as an array alternating a bare
/// category number and its stats object, not as a JSON object keyed by
/// that number.
struct StatsArray<'a>(&'a BTreeMap<ObjCategory, CategoryStats>);

impl Serialize for StatsArray<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.0.len() * 2))?;
        for (k, v) in self.0 {
            seq.serialize_element(&k.0)?;
            seq.serialize_element(v)?;
        }
        seq.end()
    }
}

impl Serialize for DirHeader {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("DirHeader", 4)?;
        state.serialize_field("ver", &self.ver)?;
        state.serialize_field("master_ver", &self.master_ver)?;
        state.serialize_field("stats", &StatsArray(&self.stats))?;
        state.serialize_field("new_instance", &self.new_instance)?;
        state.end()
    }
}

/// `rgw_bucket_dir`: a dir header and its entries, keyed by name
/// (`boost::container::flat_map`, encoded the same as a `std::map`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Dir {
    pub header: DirHeader,
    pub entries: BTreeMap<String, DirEntry>,
}

impl VersionedEncode for Dir {
    const MAX_DECODE_VERSION: u8 = 2;

    fn encoding_version(&self, _features: u64) -> u8 {
        2
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
        self.header.encode(buf, features)?;
        self.entries.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 2, "Dir", "Bobtail v0.56+");

        let header = DirHeader::decode(buf, features)?;
        let entries = BTreeMap::<String, DirEntry>::decode(buf, features)?;
        Ok(Self { header, entries })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

rados::impl_denc_for_versioned!(Dir);

/// `rgw_bucket_dir::m` dumps as an array alternating a bare key and its
/// entry object, not as a JSON object keyed by name.
struct MapArray<'a>(&'a BTreeMap<String, DirEntry>);

impl Serialize for MapArray<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.0.len() * 2))?;
        for (k, v) in self.0 {
            seq.serialize_element(k)?;
            seq.serialize_element(v)?;
        }
        seq.end()
    }
}

impl Serialize for Dir {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("Dir", 2)?;
        state.serialize_field("header", &self.header)?;
        state.serialize_field("map", &MapArray(&self.entries))?;
        state.end()
    }
}

/// `RGW_BILOG_FLAG_VERSIONED_OP`.
pub const BILOG_FLAG_VERSIONED_OP: u16 = 0x1;
/// `RGW_BILOG_NULL_VERSION` (Tentacle v20+; a v19 cluster never sets it).
pub const BILOG_NULL_VERSION: u16 = 0x2;

fn bilog_state_str(state: PendingState) -> &'static str {
    match state {
        PendingState::PENDING_MODIFY => "pending",
        PendingState::COMPLETE => "complete",
        _ => "invalid",
    }
}

/// `rgw_bi_log_entry`: one bucket-index log entry, read by multisite
/// sync. `state` prints as `pending`/`complete`/`invalid`, unlike
/// [`super::types::PendingState`]'s own convention.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BiLogEntry {
    pub id: String,
    pub object: String,
    pub instance: String,
    pub timestamp: UTime,
    pub ver: EntryVer,
    pub op: ModifyOp,
    pub state: PendingState,
    pub index_ver: u64,
    pub tag: String,
    pub bilog_flags: u16,
    pub owner: String,
    pub owner_display_name: String,
    pub zones_trace: ZoneSet,
}

impl VersionedEncode for BiLogEntry {
    const MAX_DECODE_VERSION: u8 = 4;

    fn encoding_version(&self, _features: u64) -> u8 {
        4
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
        self.id.encode(buf, features)?;
        self.object.encode(buf, features)?;
        self.timestamp.encode(buf, features)?;
        self.ver.encode(buf, features)?;
        self.tag.encode(buf, features)?;
        self.op.encode(buf, features)?;
        self.state.encode(buf, features)?;
        packed::encode(self.index_ver, buf);
        self.instance.encode(buf, features)?;
        self.bilog_flags.encode(buf, features)?;
        self.owner.encode(buf, features)?;
        self.owner_display_name.encode(buf, features)?;
        self.zones_trace.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 4, "BiLogEntry", "Luminous v12+");

        let id = String::decode(buf, features)?;
        let object = String::decode(buf, features)?;
        let timestamp = UTime::decode(buf, features)?;
        let ver = EntryVer::decode(buf, features)?;
        let tag = String::decode(buf, features)?;
        let op = ModifyOp::decode(buf, features)?;
        let state = PendingState::decode(buf, features)?;
        let index_ver = packed::decode(buf)?;
        let instance = String::decode(buf, features)?;
        let bilog_flags = u16::decode(buf, features)?;
        let owner = String::decode(buf, features)?;
        let owner_display_name = String::decode(buf, features)?;
        let zones_trace = ZoneSet::decode(buf, features)?;
        Ok(Self {
            id,
            object,
            instance,
            timestamp,
            ver,
            op,
            state,
            index_ver,
            tag,
            bilog_flags,
            owner,
            owner_display_name,
            zones_trace,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

rados::impl_denc_for_versioned!(BiLogEntry);

impl BiLogEntry {
    /// `rgw_bi_log_entry::is_versioned`.
    pub fn is_versioned(&self) -> bool {
        self.bilog_flags & BILOG_FLAG_VERSIONED_OP != 0
    }

    /// `rgw_bi_log_entry::is_null_verid`; always false on v19, which
    /// never sets the flag.
    pub fn is_null_verid(&self) -> bool {
        self.bilog_flags & BILOG_NULL_VERSION != 0
    }
}

/// `zones_trace`'s bare-array dump: `encode_json("zones_trace", zs, f)`
/// has a dedicated overload for `rgw_zone_set` that dumps `zs.entries`
/// directly, skipping the `{"entries": [...]}` wrapper [`ZoneSet`]'s own
/// dump uses.
struct ZonesTraceBare<'a>(&'a ZoneSet);

impl Serialize for ZonesTraceBare<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.0.entries.len()))?;
        for e in &self.0.entries {
            seq.serialize_element(e)?;
        }
        seq.end()
    }
}

impl Serialize for BiLogEntry {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("BiLogEntry", 14)?;
        state.serialize_field("op_id", &self.id)?;
        state.serialize_field("op_tag", &self.tag)?;
        state.serialize_field("op", self.op.as_str())?;
        state.serialize_field("object", &self.object)?;
        state.serialize_field("instance", &self.instance)?;
        state.serialize_field("state", bilog_state_str(self.state))?;
        state.serialize_field("index_ver", &self.index_ver)?;
        state.serialize_field("timestamp", &DumpUtimeNsec(&self.timestamp))?;
        state.serialize_field("ver", &self.ver)?;
        state.serialize_field("bilog_flags", &self.bilog_flags)?;
        state.serialize_field("versioned", &self.is_versioned())?;
        state.serialize_field("owner", &self.owner)?;
        state.serialize_field("owner_display_name", &self.owner_display_name)?;
        state.serialize_field("zones_trace", &ZonesTraceBare(&self.zones_trace))?;
        state.end()
    }
}

/// `cls_rgw_reshard_entry`: one bucket queued for resharding. Version 1's
/// `new_instance_id` string (removed in version 2) is not modelled;
/// version 3's `initiator` byte is not modelled either and is skipped on
/// decode.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReshardEntry {
    pub time: UTime,
    pub tenant: String,
    pub bucket_name: String,
    pub bucket_id: String,
    pub old_num_shards: u32,
    pub new_num_shards: u32,
}

impl VersionedEncode for ReshardEntry {
    const MAX_DECODE_VERSION: u8 = 3;

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
        self.time.encode(buf, features)?;
        self.tenant.encode(buf, features)?;
        self.bucket_name.encode(buf, features)?;
        self.bucket_id.encode(buf, features)?;
        self.old_num_shards.encode(buf, features)?;
        self.new_num_shards.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 2, "ReshardEntry", "Reef v18+");

        let time = UTime::decode(buf, features)?;
        let tenant = String::decode(buf, features)?;
        let bucket_name = String::decode(buf, features)?;
        let bucket_id = String::decode(buf, features)?;
        let old_num_shards = u32::decode(buf, features)?;
        let new_num_shards = u32::decode(buf, features)?;
        Ok(Self {
            time,
            tenant,
            bucket_name,
            bucket_id,
            old_num_shards,
            new_num_shards,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

rados::impl_denc_for_versioned!(ReshardEntry);

impl ReshardEntry {
    /// `cls_rgw_reshard_entry::get_key`.
    pub fn key(&self) -> String {
        format!("{}:{}", self.tenant, self.bucket_name)
    }
}

impl Serialize for ReshardEntry {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("ReshardEntry", 6)?;
        state.serialize_field("time", &DumpUtime(&self.time))?;
        state.serialize_field("tenant", &self.tenant)?;
        state.serialize_field("bucket_name", &self.bucket_name)?;
        state.serialize_field("bucket_id", &self.bucket_id)?;
        state.serialize_field("old_num_shards", &self.old_num_shards)?;
        state.serialize_field("tentative_new_num_shards", &self.new_num_shards)?;
        state.end()
    }
}

/// `rgw_cls_bi_entry::get_info`'s result: the entry's key, its category
/// when it has one (a `PLAIN`/`INSTANCE` dir entry; `None` for `OLH`),
/// the stats delta the dir entry contributes, and whether it counts
/// towards the bucket's accounted stats.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BiInfo {
    pub key: ObjKey,
    pub category: Option<ObjCategory>,
    pub stats: CategoryStats,
    pub counts: bool,
}

/// `rgw_cls_bi_entry`: one bucket-index entry, its payload encoded by
/// `kind` as a [`DirEntry`] (`PLAIN`/`INSTANCE`) or an [`OlhEntry`]
/// (`OLH`). The dump decodes that payload; [`BiEntry::get_info`] mirrors
/// the C++ accounting.
#[derive(Debug, Clone, Default, PartialEq, Eq, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct BiEntry {
    pub kind: BiIndexType,
    /// The omap key the entry lives under. Instance and OLH keys start
    /// with the `0x80` namespace byte, so the key is bytes, not a
    /// `String`; the dump renders it lossily where the C++ writes the
    /// raw bytes into its JSON.
    pub idx: OmapKey,
    pub data: Bytes,
}

impl BiEntry {
    /// `rgw_cls_bi_entry::get_info`. A payload that fails to decode is
    /// returned as an error rather than the C++'s undefined behaviour.
    pub fn get_info(&self) -> std::result::Result<BiInfo, RadosError> {
        let mut buf = self.data.clone();
        if self.kind == BiIndexType::OLH {
            let entry = OlhEntry::decode(&mut buf, 0)?;
            return Ok(BiInfo {
                key: entry.key,
                category: None,
                stats: CategoryStats::default(),
                counts: false,
            });
        }

        let entry = DirEntry::decode(&mut buf, 0)?;
        let stats = CategoryStats {
            total_size: entry.meta.accounted_size,
            total_size_rounded: rounded_size(entry.meta.accounted_size),
            num_entries: 1,
            actual_size: entry.meta.size,
        };
        let counts = match self.kind {
            BiIndexType::PLAIN => entry.exists && entry.flags == 0,
            BiIndexType::INSTANCE => entry.exists,
            _ => false,
        };
        Ok(BiInfo {
            key: entry.key,
            category: Some(entry.meta.category),
            stats,
            counts,
        })
    }
}

impl Serialize for BiEntry {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let has_entry = matches!(
            self.kind,
            BiIndexType::PLAIN | BiIndexType::INSTANCE | BiIndexType::OLH
        );
        let mut state = serializer.serialize_struct("BiEntry", if has_entry { 3 } else { 2 })?;
        state.serialize_field("type", self.kind.as_str())?;
        state.serialize_field("idx", &String::from_utf8_lossy(&self.idx))?;
        match self.kind {
            BiIndexType::PLAIN | BiIndexType::INSTANCE => {
                let mut buf = self.data.clone();
                let entry = DirEntry::decode(&mut buf, 0).map_err(serde::ser::Error::custom)?;
                state.serialize_field("entry", &entry)?;
            }
            BiIndexType::OLH => {
                let mut buf = self.data.clone();
                let entry = OlhEntry::decode(&mut buf, 0).map_err(serde::ser::Error::custom)?;
                state.serialize_field("entry", &entry)?;
            }
            _ => {}
        }
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

    fn meta_instance() -> DirEntryMeta {
        DirEntryMeta {
            category: ObjCategory::MAIN,
            size: 100,
            mtime: UTime::default(),
            etag: "etag".to_owned(),
            owner: "owner".to_owned(),
            owner_display_name: "display name".to_owned(),
            content_type: "content/type".to_owned(),
            accounted_size: 0,
            user_data: String::new(),
            storage_class: String::new(),
            appendable: false,
        }
    }

    #[test]
    fn dir_entry_meta_dump_order_differs_from_wire_order() {
        let m = meta_instance();
        assert_eq!(
            bytes(&m),
            unhex(
                "07035300000001640000000000000000000000000000000400000065746167050000006f776e65720c000000646973706c6179206e616d650c000000636f6e74656e742f747970650000000000000000000000000000000000"
            )
        );
        assert_eq!(
            json(&m),
            r#"{"category":1,"size":100,"mtime":"0.000000","etag":"etag","storage_class":"","owner":"owner","owner_display_name":"display name","content_type":"content/type","accounted_size":0,"user_data":"","appendable":false}"#
        );
        assert_eq!(
            DirEntryMeta::decode(&mut &bytes(&m)[..], 0).expect("decode"),
            m
        );
        // Version 6 lacked appendable; below the floor.
        let v6 = unhex(
            "06035200000001640000000000000000000000000000000400000065746167050000006f776e65720c000000646973706c6179206e616d650c000000636f6e74656e742f7479706500000000000000000000000000000000",
        );
        assert!(DirEntryMeta::decode(&mut &v6[..], 0).is_err());
    }

    /// `encoded` re-versioned to `version` with `tail` appended to its
    /// content, as a newer release writes it.
    fn with_tail(encoded: &[u8], version: u8, tail: &[u8]) -> Vec<u8> {
        let mut out = encoded.to_vec();
        out[0] = version;
        out.extend_from_slice(tail);
        let len = u32::try_from(out.len() - 6).expect("fits");
        out[2..6].copy_from_slice(&len.to_le_bytes());
        out
    }

    #[test]
    fn decoders_accept_mains_version_and_skip_its_tail() {
        // Ceph main writes meta v8 (restore_status u8, restore_expiry_date
        // real_time), header v8 (reshardlog_entries u32) and reshard
        // entry v3 (initiator u8); everything past v19's fields is
        // skipped, and a version past MAX_DECODE_VERSION is rejected.
        let meta = dir_entry_instance().meta;
        let v8 = with_tail(&bytes(&meta), 8, &[0; 9]);
        let mut buf = &v8[..];
        assert_eq!(DirEntryMeta::decode(&mut buf, 0).expect("meta v8"), meta);
        assert!(buf.is_empty());
        assert!(DirEntryMeta::decode(&mut &with_tail(&bytes(&meta), 9, &[])[..], 0).is_err());

        let header = dir_header_instance();
        let v8 = with_tail(&bytes(&header), 8, &[0; 4]);
        let mut buf = &v8[..];
        assert_eq!(DirHeader::decode(&mut buf, 0).expect("header v8"), header);
        assert!(buf.is_empty());

        let v2 = unhex(
            "02013200000002000000030000000600000074656e616e74070000006275636b657431090000006275636b65745f69640800000040000000",
        );
        let entry = ReshardEntry::decode(&mut &v2[..], 0).expect("reshard v2");
        let v3 = with_tail(&v2, 3, &[1]);
        let mut buf = &v3[..];
        assert_eq!(
            ReshardEntry::decode(&mut buf, 0).expect("reshard v3"),
            entry
        );
        assert!(buf.is_empty());
    }

    fn dir_entry_instance() -> DirEntry {
        DirEntry {
            key: ObjKey {
                name: "name".to_owned(),
                instance: String::new(),
            },
            ver: EntryVer {
                pool: 1,
                epoch: 1234,
            },
            locator: "locator".to_owned(),
            exists: true,
            meta: meta_instance(),
            tag: "tag".to_owned(),
            flags: 0,
            pending_map: Vec::new(),
            versioned_epoch: 0,
            index_ver: 0,
        }
    }

    #[test]
    fn dir_entry_writes_ver_epoch_twice_and_splits_the_key() {
        let e = dir_entry_instance();
        assert_eq!(
            bytes(&e),
            unhex(
                "080399000000040000006e616d65d2040000000000000107035300000001640000000000000000000000000000000400000065746167050000006f776e65720c000000646973706c6179206e616d650c000000636f6e74656e742f74797065000000000000000000000000000000000000000000070000006c6f6361746f720101040000000182d20400030000007461670000000000000000000000000000"
            )
        );
        assert_eq!(
            json(&e),
            r#"{"name":"name","instance":"","ver":{"pool":1,"epoch":1234},"locator":"locator","exists":true,"meta":{"category":1,"size":100,"mtime":"0.000000","etag":"etag","storage_class":"","owner":"owner","owner_display_name":"display name","content_type":"content/type","accounted_size":0,"user_data":"","appendable":false},"tag":"tag","flags":0,"pending_map":[],"versioned_epoch":0}"#
        );
        assert_eq!(DirEntry::decode(&mut &bytes(&e)[..], 0).expect("decode"), e);
    }

    #[test]
    fn dir_entry_round_trips_a_pending_entry_and_non_zero_flags() {
        let mut e = dir_entry_instance();
        e.flags = FLAG_VER | FLAG_CURRENT;
        e.pending_map.push((
            "marker".to_owned(),
            PendingInfo {
                state: PendingState::PENDING_MODIFY,
                timestamp: UTime::default(),
                op: 0,
            },
        ));
        let round = DirEntry::decode(&mut &bytes(&e)[..], 0).expect("decode");
        assert_eq!(round, e);
        assert!(json(&e).contains(r#""pending_map":[{"key":"marker","val":{"#));
    }

    #[test]
    fn dir_entry_flags_report_current_and_visible() {
        let mut e = dir_entry_instance();
        e.flags = FLAG_VER | FLAG_CURRENT;
        assert!(e.is_current());
        assert!(e.is_visible());
        e.flags = FLAG_VER;
        assert!(!e.is_current());
        assert!(!e.is_visible());
    }

    fn bucket_instance_entry() -> BucketInstanceEntry {
        BucketInstanceEntry {
            reshard_status: ReshardStatus::IN_PROGRESS,
        }
    }

    #[test]
    fn bucket_instance_entry_carries_two_empty_legacy_fields() {
        let e = bucket_instance_entry();
        assert_eq!(bytes(&e), unhex("0301090000000100000000ffffffff"));
        assert_eq!(json(&e), r#"{"reshard_status":"in-progress"}"#);
        assert_eq!(
            BucketInstanceEntry::decode(&mut &bytes(&e)[..], 0).expect("decode"),
            e
        );
        assert!(e.resharding());
        assert!(e.resharding_in_progress());
        // Version 2 dropped the legacy fields; below the floor.
        let v2 = unhex("0201010000000100");
        assert!(BucketInstanceEntry::decode(&mut &v2[..], 0).is_err());
    }

    fn dir_header_instance() -> DirHeader {
        let mut stats = BTreeMap::new();
        stats.insert(
            ObjCategory::NONE,
            CategoryStats {
                total_size: 1024,
                total_size_rounded: 4096,
                num_entries: 2,
                actual_size: 1024,
            },
        );
        DirHeader {
            stats,
            tag_timeout: 0,
            ver: 0,
            master_ver: 0,
            max_marker: String::new(),
            new_instance: BucketInstanceEntry::default(),
            syncstopped: false,
        }
    }

    #[test]
    fn dir_header_stats_dump_alternates_key_and_object() {
        let h = dir_header_instance();
        assert_eq!(
            bytes(&h),
            unhex(
                "07025700000001000000000302200000000004000000000000001000000000000002000000000000000004000000000000000000000000000000000000000000000000000000000000000000000301090000000000000000ffffffff00"
            )
        );
        assert_eq!(
            json(&h),
            r#"{"ver":0,"master_ver":0,"stats":[0,{"total_size":1024,"total_size_rounded":4096,"num_entries":2,"actual_size":1024}],"new_instance":{"reshard_status":"not-resharding"}}"#
        );
        assert_eq!(
            DirHeader::decode(&mut &bytes(&h)[..], 0).expect("decode"),
            h
        );
    }

    #[test]
    fn dir_wraps_the_header_and_dumps_the_map_as_pairs() {
        let d = Dir {
            header: dir_header_instance(),
            entries: BTreeMap::new(),
        };
        assert_eq!(
            bytes(&d),
            unhex(
                "02026100000007025700000001000000000302200000000004000000000000001000000000000002000000000000000004000000000000000000000000000000000000000000000000000000000000000000000301090000000000000000ffffffff0000000000"
            )
        );
        assert_eq!(Dir::decode(&mut &bytes(&d)[..], 0).expect("decode"), d);
        assert!(json(&d).contains(r#""map":[]"#));
    }

    fn bilog_entry_instance() -> BiLogEntry {
        BiLogEntry {
            id: "midf".to_owned(),
            object: "obj".to_owned(),
            instance: String::new(),
            timestamp: UTime { sec: 2, nsec: 3 },
            ver: EntryVer::default(),
            op: ModifyOp::DEL,
            state: PendingState::PENDING_MODIFY,
            index_ver: 4323,
            tag: "tagasdfds".to_owned(),
            bilog_flags: 0,
            owner: String::new(),
            owner_display_name: String::new(),
            zones_trace: ZoneSet::default(),
        }
    }

    #[test]
    fn bilog_entry_renames_and_derives_versioned_and_streams_nsec() {
        let e = bilog_entry_instance();
        assert_eq!(
            bytes(&e),
            unhex(
                "04014b000000040000006d696466030000006f626a020000000300000001010a00000088ffffffffffffffff0009000000746167617364666473010082e310000000000000000000000000000000000000"
            )
        );
        assert_eq!(
            json(&e),
            r#"{"op_id":"midf","op_tag":"tagasdfds","op":"del","object":"obj","instance":"","state":"pending","index_ver":4323,"timestamp":"2.000000","ver":{"pool":-1,"epoch":0},"bilog_flags":0,"versioned":false,"owner":"","owner_display_name":"","zones_trace":[]}"#
        );
        assert_eq!(
            BiLogEntry::decode(&mut &bytes(&e)[..], 0).expect("decode"),
            e
        );
        assert!(!e.is_versioned());
        assert!(!e.is_null_verid());
    }

    #[test]
    fn reshard_entry_renames_new_num_shards() {
        let e = ReshardEntry {
            time: UTime { sec: 2, nsec: 3 },
            tenant: "tenant".to_owned(),
            bucket_name: "bucket1".to_owned(),
            bucket_id: "bucket_id".to_owned(),
            old_num_shards: 8,
            new_num_shards: 64,
        };
        assert_eq!(
            bytes(&e),
            unhex(
                "02013200000002000000030000000600000074656e616e74070000006275636b657431090000006275636b65745f69640800000040000000"
            )
        );
        assert_eq!(
            json(&e),
            r#"{"time":"2.000000","tenant":"tenant","bucket_name":"bucket1","bucket_id":"bucket_id","old_num_shards":8,"tentative_new_num_shards":64}"#
        );
        assert_eq!(
            ReshardEntry::decode(&mut &bytes(&e)[..], 0).expect("decode"),
            e
        );
        assert_eq!(e.key(), "tenant:bucket1");
        // Version 1 carried a new_instance_id string; below the floor.
        let v1 = unhex(
            "01013200000002000000030000000600000074656e616e74070000006275636b657431090000006275636b65745f69640800000040000000",
        );
        assert!(ReshardEntry::decode(&mut &v1[..], 0).is_err());
    }

    fn olh_entry_payload() -> Vec<u8> {
        unhex(
            "01013800000001011c000000080000006b65792e6e616d650c0000006b65792e696e7374616e636501d20400000000000000000000030000007461670101",
        )
    }

    #[test]
    fn bi_entry_invalid_has_no_entry() {
        let e = BiEntry::default();
        assert_eq!(bytes(&e), unhex("010109000000000000000000000000"));
        assert_eq!(json(&e), r#"{"type":"invalid","idx":""}"#);
        assert_eq!(BiEntry::decode(&mut &bytes(&e)[..], 0).expect("decode"), e);
    }

    #[test]
    fn bi_entry_dumps_a_namespaced_key_lossily() {
        let e = BiEntry {
            kind: BiIndexType::INVALID,
            idx: Bytes::from_static(b"\x801000_obj"),
            data: Bytes::new(),
        };
        assert_eq!(
            bytes(&e),
            unhex("010112000000000900000080313030305f6f626a00000000")
        );
        assert_eq!(
            json(&e),
            "{\"type\":\"invalid\",\"idx\":\"\u{fffd}1000_obj\"}"
        );
        assert_eq!(BiEntry::decode(&mut &bytes(&e)[..], 0).expect("decode"), e);
    }

    #[test]
    fn bi_entry_olh_dumps_the_decoded_olh_entry() {
        let e = BiEntry {
            kind: BiIndexType::OLH,
            idx: Bytes::from_static(b"idx"),
            data: Bytes::from(olh_entry_payload()),
        };
        assert_eq!(
            bytes(&e),
            unhex(
                "01014a00000003030000006964783e00000001013800000001011c000000080000006b65792e6e616d650c0000006b65792e696e7374616e636501d20400000000000000000000030000007461670101"
            )
        );
        assert_eq!(
            json(&e),
            r#"{"type":"olh","idx":"idx","entry":{"key":{"name":"key.name","instance":"key.instance"},"delete_marker":true,"epoch":1234,"pending_log":[],"tag":"tag","exists":true,"pending_removal":true}}"#
        );
        assert_eq!(BiEntry::decode(&mut &bytes(&e)[..], 0).expect("decode"), e);
        let info = e.get_info().expect("get_info");
        assert_eq!(info.key.name, "key.name");
        assert_eq!(info.category, None);
        assert!(!info.counts);
    }

    #[test]
    fn bi_entry_plain_accounts_a_dir_entry() {
        let dir_entry = dir_entry_instance();
        let e = BiEntry {
            kind: BiIndexType::PLAIN,
            idx: Bytes::from_static(b"idx"),
            data: Bytes::from(bytes(&dir_entry)),
        };
        let info = e.get_info().expect("get_info");
        assert_eq!(info.key.name, "name");
        assert_eq!(info.category, Some(ObjCategory::MAIN));
        assert_eq!(info.stats.num_entries, 1);
        assert_eq!(info.stats.actual_size, dir_entry.meta.size);
        assert_eq!(info.stats.total_size, dir_entry.meta.accounted_size);
        // exists is true and flags is 0, so it counts.
        assert!(info.counts);
    }
}
