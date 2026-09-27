//! The bucket index: from `cls_rgw_types.h`, the entry and its parts,
//! the dir header and dir, the bilog entry, and the bucket-instance and
//! reshard entries; from `cls_rgw_ops.h`, the request and reply structs
//! of the bucket-index, resharding and head-object methods; and, as
//! `cls_rgw_client.h` has them, those methods' op constructors and
//! async calls.
//!
//! The index writes ([`prepare`], [`complete`], [`suggest_changes`],
//! [`rebuild_index`], [`update_stats`], [`set_tag_timeout`]) take no
//! resharding guard of their own: Ceph v19's class does not guard itself,
//! and RGW decides per call site to put [`guard_op`] in front of the
//! write in one compound operation, which then fails with
//! `OSDError { code: -ERR_BUSY_RESHARDING }` while the shard reshards.
//! The `*_op` constructors are for building that compound.
//!
//! Server facts (Ceph v19 `cls_rgw.cc`): a second [`init_index`] is
//! `EINVAL`, so RGW creates the shard with `create(exclusive)` first to
//! get `EEXIST`; every header write bumps `ver`. `bucket_list` skips
//! entries flagged `FLAG_VER_MARKER`; without `list_versions` it also
//! skips entries that are not visible and any entry named like
//! `start_obj`; it collapses names under `delimiter` into one entry
//! flagged `FLAG_COMMON_PREFIX`; it never returns the `0x80` namespace.
//! It does return entries whose `exists` is false (a delete that
//! completed while another tag is pending leaves one); RGW filters those
//! itself.

use std::collections::BTreeMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use rados::osdclient::error::Result;
use rados::osdclient::{IoCtx, OSDOp, OpReply};
use rados::{Denc, OSDClientError, OmapKey, RadosError, UTime, VersionedDenc, VersionedEncode};
use serde::Serialize;
use serde::ser::{SerializeSeq, SerializeStruct};

use super::CLASS;
use super::olh::OlhEntry;
use super::packed;
use super::types::{
    BiIndexType, CategoryStats, CheckMtimeType, EntryVer, FLAG_COMMON_PREFIX, FLAG_CURRENT,
    FLAG_DELETE_MARKER, FLAG_VER, FLAG_VER_MARKER, ModifyOp, ObjCategory, ObjKey, PendingInfo,
    PendingState, ReshardStatus, ZoneSet, rounded_size,
};
use crate::call;

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
pub(super) struct DumpUtime<'a>(pub(super) &'a UTime);

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
pub(super) struct ZonesTraceBare<'a>(pub(super) &'a ZoneSet);

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

/// `rgw_cls_tag_timeout_op`: the seconds after which a pending change no
/// longer blocks a suggestion.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct TagTimeoutOp {
    pub tag_timeout: u64,
}

/// `rgw_cls_obj_prepare_op`: record a pending change to `key` under
/// `tag`. Version 7 (compat 5); `op` is one byte and the full key comes
/// after `log_op`, where versions before 5 wrote only the name first.
/// The v19 dump prints the name only, then `log_op`, `bilog_flags` and
/// `zones_trace` as a bare array.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrepareOp {
    pub op: ModifyOp,
    pub key: ObjKey,
    pub tag: String,
    pub locator: String,
    pub log_op: bool,
    pub bilog_flags: u16,
    pub zones_trace: ZoneSet,
}

impl Default for PrepareOp {
    fn default() -> Self {
        Self {
            op: ModifyOp::UNKNOWN,
            key: ObjKey::default(),
            tag: String::new(),
            locator: String::new(),
            log_op: false,
            bilog_flags: 0,
            zones_trace: ZoneSet::default(),
        }
    }
}

impl VersionedEncode for PrepareOp {
    const MAX_DECODE_VERSION: u8 = 7;

    fn encoding_version(&self, _features: u64) -> u8 {
        7
    }

    fn compat_version(&self, _features: u64) -> u8 {
        5
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> std::result::Result<(), RadosError> {
        self.op.encode(buf, features)?;
        self.tag.encode(buf, features)?;
        self.locator.encode(buf, features)?;
        self.log_op.encode(buf, features)?;
        self.key.encode(buf, features)?;
        self.bilog_flags.encode(buf, features)?;
        self.zones_trace.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 7, "PrepareOp", "Luminous v12+");

        let op = ModifyOp::decode(buf, features)?;
        let tag = String::decode(buf, features)?;
        let locator = String::decode(buf, features)?;
        let log_op = bool::decode(buf, features)?;
        let key = ObjKey::decode(buf, features)?;
        let bilog_flags = u16::decode(buf, features)?;
        let zones_trace = ZoneSet::decode(buf, features)?;
        Ok(Self {
            op,
            key,
            tag,
            locator,
            log_op,
            bilog_flags,
            zones_trace,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

rados::impl_denc_for_versioned!(PrepareOp);

impl Serialize for PrepareOp {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("PrepareOp", 7)?;
        state.serialize_field("op", &self.op)?;
        state.serialize_field("name", &self.key.name)?;
        state.serialize_field("tag", &self.tag)?;
        state.serialize_field("locator", &self.locator)?;
        state.serialize_field("log_op", &self.log_op)?;
        state.serialize_field("bilog_flags", &self.bilog_flags)?;
        state.serialize_field("zones_trace", &ZonesTraceBare(&self.zones_trace))?;
        state.end()
    }
}

/// `rgw_cls_obj_complete_op`: finish the change prepared under `tag`
/// (C++ `op_tag`). Version 9 (compat 7); `ver.epoch` goes on the wire
/// right after `op` and again inside `ver`, and the full key comes late.
/// `remove_objs` are keys the class drops from the index unaccounted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteOp {
    pub op: ModifyOp,
    pub key: ObjKey,
    pub locator: String,
    pub ver: EntryVer,
    pub meta: DirEntryMeta,
    pub tag: String,
    pub log_op: bool,
    pub bilog_flags: u16,
    pub remove_objs: Vec<ObjKey>,
    pub zones_trace: ZoneSet,
}

impl Default for CompleteOp {
    fn default() -> Self {
        Self {
            op: ModifyOp::ADD,
            key: ObjKey::default(),
            locator: String::new(),
            ver: EntryVer::default(),
            meta: DirEntryMeta::default(),
            tag: String::new(),
            log_op: false,
            bilog_flags: 0,
            remove_objs: Vec::new(),
            zones_trace: ZoneSet::default(),
        }
    }
}

impl VersionedEncode for CompleteOp {
    const MAX_DECODE_VERSION: u8 = 9;

    fn encoding_version(&self, _features: u64) -> u8 {
        9
    }

    fn compat_version(&self, _features: u64) -> u8 {
        7
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> std::result::Result<(), RadosError> {
        self.op.encode(buf, features)?;
        self.ver.epoch.encode(buf, features)?;
        self.meta.encode(buf, features)?;
        self.tag.encode(buf, features)?;
        self.locator.encode(buf, features)?;
        self.remove_objs.encode(buf, features)?;
        self.ver.encode(buf, features)?;
        self.log_op.encode(buf, features)?;
        self.key.encode(buf, features)?;
        self.bilog_flags.encode(buf, features)?;
        self.zones_trace.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 9, "CompleteOp", "Luminous v12+");

        let op = ModifyOp::decode(buf, features)?;
        let _epoch_legacy = u64::decode(buf, features)?;
        let meta = DirEntryMeta::decode(buf, features)?;
        let tag = String::decode(buf, features)?;
        let locator = String::decode(buf, features)?;
        let remove_objs = Vec::<ObjKey>::decode(buf, features)?;
        let ver = EntryVer::decode(buf, features)?;
        let log_op = bool::decode(buf, features)?;
        let key = ObjKey::decode(buf, features)?;
        let bilog_flags = u16::decode(buf, features)?;
        let zones_trace = ZoneSet::decode(buf, features)?;
        Ok(Self {
            op,
            key,
            locator,
            ver,
            meta,
            tag,
            log_op,
            bilog_flags,
            remove_objs,
            zones_trace,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

rados::impl_denc_for_versioned!(CompleteOp);

impl Serialize for CompleteOp {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("CompleteOp", 10)?;
        state.serialize_field("op", &self.op)?;
        state.serialize_field("name", &self.key.name)?;
        state.serialize_field("instance", &self.key.instance)?;
        state.serialize_field("locator", &self.locator)?;
        state.serialize_field("ver", &self.ver)?;
        state.serialize_field("meta", &self.meta)?;
        state.serialize_field("tag", &self.tag)?;
        state.serialize_field("log_op", &self.log_op)?;
        state.serialize_field("bilog_flags", &self.bilog_flags)?;
        state.serialize_field("zones_trace", &ZonesTraceBare(&self.zones_trace))?;
        state.end()
    }
}

/// `rgw_cls_list_op`: up to `num_entries` entries after `start_obj`
/// whose names start with `filter_prefix`; zero entries asks for the
/// header alone. Version 6 (compat 4) encodes `num_entries` first; the
/// dump prints only the start name and `num_entries`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListOp {
    pub start_obj: ObjKey,
    pub num_entries: u32,
    pub filter_prefix: String,
    pub list_versions: bool,
    pub delimiter: String,
}

impl VersionedEncode for ListOp {
    const MAX_DECODE_VERSION: u8 = 6;

    fn encoding_version(&self, _features: u64) -> u8 {
        6
    }

    fn compat_version(&self, _features: u64) -> u8 {
        4
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> std::result::Result<(), RadosError> {
        self.num_entries.encode(buf, features)?;
        self.filter_prefix.encode(buf, features)?;
        self.start_obj.encode(buf, features)?;
        self.list_versions.encode(buf, features)?;
        self.delimiter.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 6, "ListOp", "Octopus v15+");

        let num_entries = u32::decode(buf, features)?;
        let filter_prefix = String::decode(buf, features)?;
        let start_obj = ObjKey::decode(buf, features)?;
        let list_versions = bool::decode(buf, features)?;
        let delimiter = String::decode(buf, features)?;
        Ok(Self {
            start_obj,
            num_entries,
            filter_prefix,
            list_versions,
            delimiter,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

rados::impl_denc_for_versioned!(ListOp);

impl Serialize for ListOp {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("ListOp", 2)?;
        state.serialize_field("start_obj", &self.start_obj.name)?;
        state.serialize_field("num_entries", &self.num_entries)?;
        state.end()
    }
}

/// `rgw_cls_list_ret`: a page of the index. `marker` is where the next
/// page starts when `is_truncated`, set even when the page is empty.
/// Version 4 (compat 2); the dump prints `is_truncated` as an int and
/// leaves `marker` out.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ListRet {
    pub dir: Dir,
    #[serde(serialize_with = "crate::dump::bool_as_int")]
    pub is_truncated: bool,
    #[serde(skip)]
    pub marker: ObjKey,
}

impl VersionedEncode for ListRet {
    const MAX_DECODE_VERSION: u8 = 4;

    fn encoding_version(&self, _features: u64) -> u8 {
        4
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
        self.dir.encode(buf, features)?;
        self.is_truncated.encode(buf, features)?;
        self.marker.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> std::result::Result<Self, RadosError> {
        rados::check_min_version!(struct_v, 4, "ListRet", "Quincy v17+");

        let dir = Dir::decode(buf, features)?;
        let is_truncated = bool::decode(buf, features)?;
        let marker = ObjKey::decode(buf, features)?;
        Ok(Self {
            dir,
            is_truncated,
            marker,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

rados::impl_denc_for_versioned!(ListRet);

/// `rgw_cls_check_index_ret`: the stored header next to one recomputed
/// from the entries.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct CheckIndexRet {
    pub existing_header: DirHeader,
    pub calculated_header: DirHeader,
}

/// `rgw_cls_bucket_update_stats_op`: add `stats` to the header's, or
/// replace them when `absolute`. Version 1 as v19 writes it (Tentacle
/// v20 writes version 2 with `dec_stats`, which this request-only type
/// does not decode); the dump prints `stats` as `{"key", "val"}` entries
/// keyed by category number.
#[derive(Debug, Clone, Default, PartialEq, Eq, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct UpdateStatsOp {
    pub absolute: bool,
    pub stats: BTreeMap<ObjCategory, CategoryStats>,
}

/// A stats map dumped as `encode_json` dumps a `std::map<int, ...>`.
struct StatsEntries<'a>(&'a BTreeMap<ObjCategory, CategoryStats>);

impl Serialize for StatsEntries<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        crate::dump::map_entries(self.0.iter().map(|(k, v)| (&k.0, v)), s)
    }
}

impl Serialize for UpdateStatsOp {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("UpdateStatsOp", 2)?;
        state.serialize_field("absolute", &self.absolute)?;
        state.serialize_field("stats", &StatsEntries(&self.stats))?;
        state.end()
    }
}

/// `rgw_cls_obj_remove_op`: remove the head object, keeping the xattrs
/// whose names start with one of `keep_attr_prefixes`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct RemoveObjOp {
    pub keep_attr_prefixes: Vec<String>,
}

/// `rgw_cls_obj_store_pg_ver_op`: the xattr to store the PG version in.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct StorePgVerOp {
    pub attr: String,
}

/// `rgw_cls_obj_check_attrs_prefix`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct CheckAttrsPrefixOp {
    pub check_prefix: String,
    pub fail_if_exist: bool,
}

/// `rgw_cls_obj_check_mtime`: version 2 (compat 1) added
/// `high_precision_time`. Ceph neither registers nor dumps it. The
/// decoder floors at version 2.
#[derive(Debug, Clone, Default, PartialEq, Eq, VersionedDenc)]
#[denc(
    crate = "rados",
    version = 2,
    compat = 1,
    min_version = 2,
    ceph_release = "Jewel v10+"
)]
pub struct CheckMtimeOp {
    pub mtime: UTime,
    pub kind: CheckMtimeType,
    pub high_precision_time: bool,
}

/// `cls_rgw_set_bucket_resharding_op`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct SetBucketReshardingOp {
    pub entry: BucketInstanceEntry,
}

/// Request structs with no fields: an empty version-1 frame on the wire
/// and `{}` in the dump.
macro_rules! empty_op {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
        pub struct $name {}

        impl VersionedEncode for $name {
            const MAX_DECODE_VERSION: u8 = 1;

            fn encoding_version(&self, _features: u64) -> u8 {
                1
            }

            fn compat_version(&self, _features: u64) -> u8 {
                1
            }

            fn encode_content<B: BufMut>(
                &self,
                _buf: &mut B,
                _features: u64,
                _version: u8,
            ) -> std::result::Result<(), RadosError> {
                Ok(())
            }

            fn decode_content<B: Buf>(
                _buf: &mut B,
                _features: u64,
                struct_v: u8,
                _compat_version: u8,
            ) -> std::result::Result<Self, RadosError> {
                rados::check_min_version!(struct_v, 1, stringify!($name), "Luminous v12+");
                Ok(Self {})
            }

            fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
                Some(0)
            }
        }

        rados::impl_denc_for_versioned!($name);
    };
}

empty_op! {
    /// `cls_rgw_clear_bucket_resharding_op`.
    ClearBucketReshardingOp
}

empty_op! {
    /// `cls_rgw_get_bucket_resharding_op`.
    GetBucketReshardingOp
}

/// `cls_rgw_guard_bucket_resharding_op`: the error the guard returns
/// while the shard is resharding.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct GuardBucketReshardingOp {
    pub ret_err: i32,
}

/// `cls_rgw_get_bucket_resharding_ret`. Ceph declares a dump but never
/// defines one; this one prints the field.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, VersionedDenc)]
#[denc(crate = "rados", version = 1, compat = 1)]
pub struct GetBucketReshardingRet {
    pub new_instance: BucketInstanceEntry,
}

/// What a [`Suggestion`] asks the class to do with its entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuggestOp {
    Remove,
    Update,
}

/// One `dir_suggest_changes` item: RGW found the head object disagreeing
/// with `entry` and proposes removing or rewriting it; `log` also writes
/// the change to the bilog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Suggestion {
    pub op: SuggestOp,
    pub log: bool,
    pub entry: DirEntry,
}

/// `CEPH_RGW_REMOVE`, `CEPH_RGW_UPDATE` and `CEPH_RGW_DIR_SUGGEST_LOG_OP`.
const SUGGEST_REMOVE: u8 = b'r';
const SUGGEST_UPDATE: u8 = b'u';
const SUGGEST_LOG_OP: u8 = 0x80;

/// The input of `dir_suggest_changes`: no struct header, each suggestion
/// its op byte followed by its encoded entry, concatenated.
pub fn encode_suggestions(suggestions: &[Suggestion]) -> std::result::Result<Bytes, RadosError> {
    let mut buf = BytesMut::new();
    for s in suggestions {
        let op = match s.op {
            SuggestOp::Remove => SUGGEST_REMOVE,
            SuggestOp::Update => SUGGEST_UPDATE,
        };
        let log = if s.log { SUGGEST_LOG_OP } else { 0 };
        buf.put_u8(op | log);
        s.entry.encode(&mut buf, 0)?;
    }
    Ok(buf.freeze())
}

/// `CLS_RGW_ERR_BUSY_RESHARDING`: RGW passes its negation to the guard,
/// and a guarded write fails with `OSDError { code: -2300 }` while the
/// shard is resharding.
pub const ERR_BUSY_RESHARDING: i32 = 2300;

/// `RGWBIAdvanceAndRetryError`: `bucket_list`'s answer when it gave up
/// before finding an entry to return.
const EFBIG: i32 = 27;

/// How many times [`list`] resends from the returned marker before
/// giving up with `EFBIG`.
const MAX_LIST_ADVANCES: usize = 64;

/// `cls_rgw_bucket_init_index`: write an empty header to a new shard.
/// `EINVAL` when the shard already has one.
pub fn init_index_op() -> Result<OSDOp> {
    call::raw_op(CLASS, "bucket_init_index", Bytes::new())
}

/// `bucket_set_tag_timeout`: store `tag_timeout` seconds in the
/// header.
pub fn set_tag_timeout_op(tag_timeout: u64) -> Result<OSDOp> {
    call::op(
        CLASS,
        "bucket_set_tag_timeout",
        &TagTimeoutOp { tag_timeout },
    )
}

/// `cls_rgw_bucket_prepare_op`: add `op.tag` to the pending map of the
/// entry for `op.key`, creating a non-existent entry if there is none.
/// The stats are untouched; an empty tag is `EINVAL`.
pub fn prepare_op(op: &PrepareOp) -> Result<OSDOp> {
    call::op(CLASS, "bucket_prepare_op", op)
}

/// `cls_rgw_bucket_complete_op`: drop `op.tag` from the entry's pending
/// map (`EINVAL` if it is not there) and apply `op.op`. A `ver` from the
/// entry's pool with a non-zero epoch not newer than the entry's turns
/// the op into a cancel, which leaves the stats alone but still stores
/// `ver` on the entry; an `ADD` replaces the entry's accounting with
/// `meta`'s;
/// a `DEL` unaccounts the entry and removes it, or keeps it with `exists`
/// false while other tags are pending. `remove_objs` are unaccounted and
/// removed whatever `op` is.
pub fn complete_op(op: &CompleteOp) -> Result<OSDOp> {
    call::op(CLASS, "bucket_complete_op", op)
}

/// `cls_rgw_bucket_list_op`: one page of entries; decode with
/// [`decode_list`]. A reply that is truncated with no entries comes with
/// result `EFBIG`, which [`list`] handles.
pub fn list_op(op: &ListOp) -> Result<OSDOp> {
    call::op(CLASS, "bucket_list", op)
}

/// `bucket_list` for zero entries, which returns the header alone;
/// decode with [`decode_dir_header`].
pub fn dir_header_op() -> Result<OSDOp> {
    list_op(&ListOp::default())
}

/// `bucket_check_index`: the stored header and one recomputed
/// from the entries; decode with [`decode_check_index`].
pub fn check_index_op() -> Result<OSDOp> {
    call::raw_op(CLASS, "bucket_check_index", Bytes::new())
}

/// `bucket_rebuild_index`: replace the header's stats with the
/// recomputed ones. `master_ver`, `max_marker` and the reshard status go
/// back to their defaults; `ver` is bumped.
pub fn rebuild_index_op() -> Result<OSDOp> {
    call::raw_op(CLASS, "bucket_rebuild_index", Bytes::new())
}

/// `cls_rgw_bucket_update_stats`: add `stats` to the header's, or replace
/// the named categories when `absolute`.
pub fn update_stats_op(
    absolute: bool,
    stats: &BTreeMap<ObjCategory, CategoryStats>,
) -> Result<OSDOp> {
    let op = UpdateStatsOp {
        absolute,
        stats: stats.clone(),
    };
    call::op(CLASS, "bucket_update_stats", &op)
}

/// `cls_rgw_suggest_changes`: apply each suggestion whose entry has no
/// pending tag younger than the tag timeout (the header's, else the
/// OSD's `rgw_pending_bucket_index_op_expiration`, else 120 s) and whose
/// `index_ver` is not older than the stored entry's; an applied update
/// stamps the entry with the header's `ver`, so a later suggestion must
/// carry the entry as listed. Suggestions for keys not in the index are
/// skipped.
pub fn suggest_changes_op(suggestions: &[Suggestion]) -> Result<OSDOp> {
    call::raw_op(
        CLASS,
        "dir_suggest_changes",
        encode_suggestions(suggestions)?,
    )
}

/// `cls_rgw_remove_obj`: on a head object, not an index shard. Removes
/// the object and, if any xattr starts with one of `keep_attr_prefixes`,
/// recreates it empty with only those xattrs. `ENOENT` when absent.
pub fn remove_obj_op(keep_attr_prefixes: &[String]) -> Result<OSDOp> {
    let op = RemoveObjOp {
        keep_attr_prefixes: keep_attr_prefixes.to_vec(),
    };
    call::op(CLASS, "obj_remove", &op)
}

/// `cls_rgw_obj_store_pg_ver`: store the PG's current version as an
/// encoded `u64` in the head object's xattr `attr`.
pub fn store_pg_ver_op(attr: &str) -> Result<OSDOp> {
    let op = StorePgVerOp {
        attr: attr.to_owned(),
    };
    call::op(CLASS, "obj_store_pg_ver", &op)
}

/// `cls_rgw_obj_check_attrs_prefix`: `ECANCELED` when whether the head
/// object has an xattr starting with `prefix` equals `fail_if_exist`;
/// an empty prefix is `EINVAL`.
pub fn check_attrs_prefix_op(prefix: &str, fail_if_exist: bool) -> Result<OSDOp> {
    let op = CheckAttrsPrefixOp {
        check_prefix: prefix.to_owned(),
        fail_if_exist,
    };
    call::op(CLASS, "obj_check_attrs_prefix", &op)
}

/// `cls_rgw_obj_check_mtime`: `ECANCELED` unless `object mtime <kind>
/// mtime` holds, compared in whole seconds unless `high_precision_time`.
/// A missing object counts as mtime 0.
pub fn check_mtime_op(
    mtime: UTime,
    kind: CheckMtimeType,
    high_precision_time: bool,
) -> Result<OSDOp> {
    let op = CheckMtimeOp {
        mtime,
        kind,
        high_precision_time,
    };
    call::op(CLASS, "obj_check_mtime", &op)
}

/// `cls_rgw_set_bucket_resharding`: store `status` in the header; the
/// rest of the entry is not stored.
pub fn set_bucket_resharding_op(status: ReshardStatus) -> Result<OSDOp> {
    let op = SetBucketReshardingOp {
        entry: BucketInstanceEntry {
            reshard_status: status,
        },
    };
    call::op(CLASS, "set_bucket_resharding", &op)
}

/// `cls_rgw_clear_bucket_resharding`: reset the header's reshard status.
pub fn clear_bucket_resharding_op() -> Result<OSDOp> {
    call::op(
        CLASS,
        "clear_bucket_resharding",
        &ClearBucketReshardingOp {},
    )
}

/// `cls_rgw_guard_bucket_resharding`: fail with `ret_err` when the
/// header's reshard status is anything but `NOT_RESHARDING`, which
/// aborts the compound operation it leads.
pub fn guard_bucket_resharding_op(ret_err: i32) -> Result<OSDOp> {
    call::op(
        CLASS,
        "guard_bucket_resharding",
        &GuardBucketReshardingOp { ret_err },
    )
}

/// The guard RGW puts in front of every index write:
/// [`guard_bucket_resharding_op`] with `-ERR_BUSY_RESHARDING`.
pub fn guard_op() -> Result<OSDOp> {
    guard_bucket_resharding_op(-ERR_BUSY_RESHARDING)
}

/// `cls_rgw_get_bucket_resharding`: decode with
/// [`decode_get_bucket_resharding`].
pub fn get_bucket_resharding_op() -> Result<OSDOp> {
    call::op(CLASS, "get_bucket_resharding", &GetBucketReshardingOp {})
}

/// Decode the reply to [`list_op`].
pub fn decode_list(reply: &OpReply) -> Result<ListRet> {
    call::decode(reply)
}

/// Decode the reply to [`dir_header_op`].
pub fn decode_dir_header(reply: &OpReply) -> Result<DirHeader> {
    Ok(call::decode::<ListRet>(reply)?.dir.header)
}

/// Decode the reply to [`check_index_op`].
pub fn decode_check_index(reply: &OpReply) -> Result<CheckIndexRet> {
    call::decode(reply)
}

/// Decode the reply to [`get_bucket_resharding_op`].
pub fn decode_get_bucket_resharding(reply: &OpReply) -> Result<BucketInstanceEntry> {
    Ok(call::decode::<GetBucketReshardingRet>(reply)?.new_instance)
}

/// See [`init_index_op`].
pub async fn init_index(ioctx: &IoCtx, oid: &str) -> Result<()> {
    call::exec_raw(ioctx, oid, CLASS, "bucket_init_index", Bytes::new())
        .await
        .map(drop)
}

/// See [`set_tag_timeout_op`].
pub async fn set_tag_timeout(ioctx: &IoCtx, oid: &str, tag_timeout: u64) -> Result<()> {
    let op = TagTimeoutOp { tag_timeout };
    call::exec(ioctx, oid, CLASS, "bucket_set_tag_timeout", &op)
        .await
        .map(drop)
}

/// See [`prepare_op`].
pub async fn prepare(ioctx: &IoCtx, oid: &str, op: &PrepareOp) -> Result<()> {
    call::exec(ioctx, oid, CLASS, "bucket_prepare_op", op)
        .await
        .map(drop)
}

/// See [`complete_op`].
pub async fn complete(ioctx: &IoCtx, oid: &str, op: &CompleteOp) -> Result<()> {
    call::exec(ioctx, oid, CLASS, "bucket_complete_op", op)
        .await
        .map(drop)
}

/// See [`list_op`]. When the class answers `EFBIG` (it read its limit of
/// entries without finding one to return), this decodes the reply and
/// sends again from its `marker`, as RGW does, up to 64 times before
/// returning the `EFBIG`. One call reads at most eight times
/// `num_entries` keys, so a small `num_entries` crosses at most
/// `520 * num_entries` skipped entries.
pub async fn list(ioctx: &IoCtx, oid: &str, op: &ListOp) -> Result<ListRet> {
    let mut op = op.clone();
    for _ in 0..=MAX_LIST_ADVANCES {
        let built = rados::OpBuilder::new().op(list_op(&op)?).build();
        let result = ioctx.execute_op_unchecked(oid, built).await?;
        let reply = result.first_reply()?;
        let code = if result.result < 0 {
            result.result
        } else {
            reply.return_code
        };
        if code >= 0 {
            return decode_list(reply);
        }
        if code != -EFBIG {
            return Err(OSDClientError::OSDError {
                code,
                message: "rgw::bucket_list failed".to_owned(),
            });
        }
        op.start_obj = decode_list(reply)?.marker;
    }
    Err(OSDClientError::OSDError {
        code: -EFBIG,
        message: format!("rgw::bucket_list found nothing in {MAX_LIST_ADVANCES} advances"),
    })
}

/// See [`dir_header_op`].
pub async fn dir_header(ioctx: &IoCtx, oid: &str) -> Result<DirHeader> {
    let out = call::exec(ioctx, oid, CLASS, "bucket_list", &ListOp::default()).await?;
    Ok(call::decode_bytes::<ListRet>(out)?.dir.header)
}

/// See [`check_index_op`].
pub async fn check_index(ioctx: &IoCtx, oid: &str) -> Result<CheckIndexRet> {
    let out = call::exec_raw(ioctx, oid, CLASS, "bucket_check_index", Bytes::new()).await?;
    call::decode_bytes(out)
}

/// See [`rebuild_index_op`].
pub async fn rebuild_index(ioctx: &IoCtx, oid: &str) -> Result<()> {
    call::exec_raw(ioctx, oid, CLASS, "bucket_rebuild_index", Bytes::new())
        .await
        .map(drop)
}

/// See [`update_stats_op`].
pub async fn update_stats(
    ioctx: &IoCtx,
    oid: &str,
    absolute: bool,
    stats: &BTreeMap<ObjCategory, CategoryStats>,
) -> Result<()> {
    let op = UpdateStatsOp {
        absolute,
        stats: stats.clone(),
    };
    call::exec(ioctx, oid, CLASS, "bucket_update_stats", &op)
        .await
        .map(drop)
}

/// See [`suggest_changes_op`].
pub async fn suggest_changes(ioctx: &IoCtx, oid: &str, suggestions: &[Suggestion]) -> Result<()> {
    let indata = encode_suggestions(suggestions)?;
    call::exec_raw(ioctx, oid, CLASS, "dir_suggest_changes", indata)
        .await
        .map(drop)
}

/// See [`remove_obj_op`].
pub async fn remove_obj(ioctx: &IoCtx, oid: &str, keep_attr_prefixes: &[String]) -> Result<()> {
    let op = RemoveObjOp {
        keep_attr_prefixes: keep_attr_prefixes.to_vec(),
    };
    call::exec(ioctx, oid, CLASS, "obj_remove", &op)
        .await
        .map(drop)
}

/// See [`store_pg_ver_op`].
pub async fn store_pg_ver(ioctx: &IoCtx, oid: &str, attr: &str) -> Result<()> {
    let op = StorePgVerOp {
        attr: attr.to_owned(),
    };
    call::exec(ioctx, oid, CLASS, "obj_store_pg_ver", &op)
        .await
        .map(drop)
}

/// See [`check_attrs_prefix_op`].
pub async fn check_attrs_prefix(
    ioctx: &IoCtx,
    oid: &str,
    prefix: &str,
    fail_if_exist: bool,
) -> Result<()> {
    let op = CheckAttrsPrefixOp {
        check_prefix: prefix.to_owned(),
        fail_if_exist,
    };
    call::exec(ioctx, oid, CLASS, "obj_check_attrs_prefix", &op)
        .await
        .map(drop)
}

/// See [`check_mtime_op`].
pub async fn check_mtime(
    ioctx: &IoCtx,
    oid: &str,
    mtime: UTime,
    kind: CheckMtimeType,
    high_precision_time: bool,
) -> Result<()> {
    let op = CheckMtimeOp {
        mtime,
        kind,
        high_precision_time,
    };
    call::exec(ioctx, oid, CLASS, "obj_check_mtime", &op)
        .await
        .map(drop)
}

/// See [`set_bucket_resharding_op`].
pub async fn set_bucket_resharding(ioctx: &IoCtx, oid: &str, status: ReshardStatus) -> Result<()> {
    let op = SetBucketReshardingOp {
        entry: BucketInstanceEntry {
            reshard_status: status,
        },
    };
    call::exec(ioctx, oid, CLASS, "set_bucket_resharding", &op)
        .await
        .map(drop)
}

/// See [`clear_bucket_resharding_op`].
pub async fn clear_bucket_resharding(ioctx: &IoCtx, oid: &str) -> Result<()> {
    let op = ClearBucketReshardingOp {};
    call::exec(ioctx, oid, CLASS, "clear_bucket_resharding", &op)
        .await
        .map(drop)
}

/// See [`guard_bucket_resharding_op`].
pub async fn guard_bucket_resharding(ioctx: &IoCtx, oid: &str, ret_err: i32) -> Result<()> {
    let op = GuardBucketReshardingOp { ret_err };
    call::exec(ioctx, oid, CLASS, "guard_bucket_resharding", &op)
        .await
        .map(drop)
}

/// See [`get_bucket_resharding_op`].
pub async fn get_bucket_resharding(ioctx: &IoCtx, oid: &str) -> Result<BucketInstanceEntry> {
    let op = GetBucketReshardingOp {};
    let out = call::exec(ioctx, oid, CLASS, "get_bucket_resharding", &op).await?;
    Ok(call::decode_bytes::<GetBucketReshardingRet>(out)?.new_instance)
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

    #[test]
    fn tag_timeout_op_matches_the_oracle() {
        let op = TagTimeoutOp {
            tag_timeout: 23_323,
        };
        assert_eq!(bytes(&op), unhex("0101080000001b5b000000000000"));
        assert_eq!(json(&op), r#"{"tag_timeout":23323}"#);
        assert_eq!(
            TagTimeoutOp::decode(&mut &bytes(&op)[..], 0).expect("decode"),
            op
        );
    }

    #[test]
    fn prepare_op_puts_the_key_after_log_op() {
        let op = PrepareOp {
            op: ModifyOp::ADD,
            key: ObjKey {
                name: "name".to_owned(),
                instance: String::new(),
            },
            tag: "tag".to_owned(),
            locator: "locator".to_owned(),
            ..PrepareOp::default()
        };
        let wire = unhex(
            "07052c0000000003000000746167070000006c6f6361746f720001010c000000040000006e616d6500000000000000000000",
        );
        assert_eq!(wire.len(), 50);
        assert_eq!(bytes(&op), wire);
        assert_eq!(
            json(&op),
            r#"{"op":0,"name":"name","tag":"tag","locator":"locator","log_op":false,"bilog_flags":0,"zones_trace":[]}"#
        );
        assert_eq!(PrepareOp::decode(&mut &wire[..], 0).expect("decode"), op);

        let default = PrepareOp::default();
        assert_eq!(default.op, ModifyOp::UNKNOWN);
        assert_eq!(
            bytes(&default),
            unhex("07051e000000030000000000000000000101080000000000000000000000000000000000")
        );
        // Version 6 had no zones_trace; below the floor.
        let mut v6 = wire.clone();
        v6[0] = 6;
        assert!(PrepareOp::decode(&mut &v6[..], 0).is_err());
    }

    #[test]
    fn complete_op_writes_the_epoch_twice_and_the_key_late() {
        let op = CompleteOp {
            op: ModifyOp::DEL,
            key: ObjKey {
                name: "name".to_owned(),
                instance: String::new(),
            },
            locator: "locator".to_owned(),
            ver: EntryVer {
                pool: 2,
                epoch: 100,
            },
            meta: meta_instance(),
            tag: "tag".to_owned(),
            ..CompleteOp::default()
        };
        let wire = unhex(
            "09079900000001640000000000000007035300000001640000000000000000000000000000000400000065746167050000006f776e65720c000000646973706c6179206e616d650c000000636f6e74656e742f74797065000000000000000000000000000000000003000000746167070000006c6f6361746f720000000001010200000002640001010c000000040000006e616d6500000000000000000000",
        );
        assert_eq!(wire.len(), 159);
        assert_eq!(bytes(&op), wire);
        assert_eq!(
            json(&op),
            r#"{"op":1,"name":"name","instance":"","locator":"locator","ver":{"pool":2,"epoch":100},"meta":{"category":1,"size":100,"mtime":"0.000000","etag":"etag","storage_class":"","owner":"owner","owner_display_name":"display name","content_type":"content/type","accounted_size":0,"user_data":"","appendable":false},"tag":"tag","log_op":false,"bilog_flags":0,"zones_trace":[]}"#
        );
        assert_eq!(CompleteOp::decode(&mut &wire[..], 0).expect("decode"), op);
        assert_eq!(CompleteOp::default().op, ModifyOp::ADD);
        assert_eq!(CompleteOp::default().ver.pool, -1);

        // The legacy epoch after `op` is ignored; the one inside `ver` wins.
        let mut stale = wire.clone();
        stale[7] = 0x63;
        assert_eq!(
            CompleteOp::decode(&mut &stale[..], 0)
                .expect("decode")
                .ver
                .epoch,
            100
        );
        let mut v8 = wire;
        v8[0] = 8;
        assert!(CompleteOp::decode(&mut &v8[..], 0).is_err());
    }

    #[test]
    fn list_op_encodes_num_entries_first() {
        let op = ListOp {
            start_obj: ObjKey {
                name: "start_obj".to_owned(),
                instance: String::new(),
            },
            num_entries: 100,
            filter_prefix: "filter_prefix".to_owned(),
            ..ListOp::default()
        };
        let wire = unhex(
            "060431000000640000000d00000066696c7465725f7072656669780101110000000900000073746172745f6f626a000000000000000000",
        );
        assert_eq!(wire.len(), 55);
        assert_eq!(bytes(&op), wire);
        assert_eq!(json(&op), r#"{"start_obj":"start_obj","num_entries":100}"#);
        assert_eq!(ListOp::decode(&mut &wire[..], 0).expect("decode"), op);
        let mut v5 = wire;
        v5[0] = 5;
        assert!(ListOp::decode(&mut &v5[..], 0).is_err());
    }

    #[test]
    fn list_ret_dumps_truncation_as_an_int_and_hides_the_marker() {
        let ret = ListRet {
            dir: Dir::default(),
            is_truncated: true,
            marker: ObjKey::default(),
        };
        let wire = unhex(
            "04024f00000002023a00000007023000000000000000000000000000000000000000000000000000000000000000000000000301090000000000000000ffffffff0000000000010101080000000000000000000000",
        );
        assert_eq!(wire.len(), 85);
        assert_eq!(bytes(&ret), wire);
        assert_eq!(
            json(&ret),
            r#"{"dir":{"header":{"ver":0,"master_ver":0,"stats":[],"new_instance":{"reshard_status":"not-resharding"}},"map":[]},"is_truncated":1}"#
        );
        assert_eq!(ListRet::decode(&mut &wire[..], 0).expect("decode"), ret);

        let with_marker = ListRet {
            marker: ObjKey {
                name: "m".to_owned(),
                instance: String::new(),
            },
            ..ret
        };
        assert_eq!(
            ListRet::decode(&mut &bytes(&with_marker)[..], 0)
                .expect("decode")
                .marker
                .name,
            "m"
        );
        // Version 3 had no marker; below the floor.
        let mut v3 = wire;
        v3[0] = 3;
        assert!(ListRet::decode(&mut &v3[..], 0).is_err());
    }

    #[test]
    fn check_index_ret_is_two_headers() {
        let ret = CheckIndexRet::default();
        let header = concat!(
            "070230000000",
            "00000000",
            "000000000000000000000000000000000000000000000000",
            "00000000",
            "0301090000000000000000ffffffff",
            "00",
        );
        let wire = unhex(&format!("01016c000000{header}{header}"));
        assert_eq!(wire.len(), 114);
        assert_eq!(bytes(&ret), wire);
        let h = r#"{"ver":0,"master_ver":0,"stats":[],"new_instance":{"reshard_status":"not-resharding"}}"#;
        assert_eq!(
            json(&ret),
            format!(r#"{{"existing_header":{h},"calculated_header":{h}}}"#)
        );
        assert_eq!(
            CheckIndexRet::decode(&mut &wire[..], 0).expect("decode"),
            ret
        );
    }

    #[test]
    fn update_stats_op_dumps_stats_as_numbered_entries() {
        let mut stats = BTreeMap::new();
        stats.insert(
            ObjCategory::NONE,
            CategoryStats {
                total_size: 1,
                total_size_rounded: 4096,
                num_entries: 1,
                actual_size: 0,
            },
        );
        let op = UpdateStatsOp {
            absolute: true,
            stats,
        };
        let wire = unhex(
            "01012c0000000101000000000302200000000100000000000000001000000000000001000000000000000000000000000000",
        );
        assert_eq!(bytes(&op), wire);
        assert_eq!(
            json(&op),
            r#"{"absolute":true,"stats":[{"key":0,"val":{"total_size":1,"total_size_rounded":4096,"num_entries":1,"actual_size":0}}]}"#
        );
        assert_eq!(
            UpdateStatsOp::decode(&mut &wire[..], 0).expect("decode"),
            op
        );
    }

    #[test]
    fn head_object_ops_match_the_oracle() {
        let remove = RemoveObjOp {
            keep_attr_prefixes: (1..=3).map(|i| format!("keep_attr_prefixes{i}")).collect(),
        };
        assert_eq!(
            bytes(&remove),
            unhex(
                "01014900000003000000130000006b6565705f617474725f707265666978657331130000006b6565705f617474725f707265666978657332130000006b6565705f617474725f707265666978657333"
            )
        );
        assert_eq!(
            json(&remove),
            r#"{"keep_attr_prefixes":["keep_attr_prefixes1","keep_attr_prefixes2","keep_attr_prefixes3"]}"#
        );

        let pg_ver = StorePgVerOp {
            attr: "attr".to_owned(),
        };
        assert_eq!(bytes(&pg_ver), unhex("0101080000000400000061747472"));
        assert_eq!(json(&pg_ver), r#"{"attr":"attr"}"#);

        let prefix = CheckAttrsPrefixOp {
            check_prefix: "prefix".to_owned(),
            fail_if_exist: true,
        };
        assert_eq!(bytes(&prefix), unhex("01010b0000000600000070726566697801"));
        assert_eq!(
            json(&prefix),
            r#"{"check_prefix":"prefix","fail_if_exist":true}"#
        );

        let mtime = CheckMtimeOp {
            mtime: UTime { sec: 1, nsec: 2 },
            kind: CheckMtimeType::LT,
            high_precision_time: true,
        };
        let wire = unhex("02010a00000001000000020000000101");
        assert_eq!(bytes(&mtime), wire);
        assert_eq!(
            CheckMtimeOp::decode(&mut &wire[..], 0).expect("decode"),
            mtime
        );
        // Version 1 had no high_precision_time; below the floor.
        let v1 = unhex("010109000000010000000200000001");
        let err = CheckMtimeOp::decode(&mut &v1[..], 0).expect_err("version 1");
        assert!(
            matches!(
                err,
                RadosError::Codec(rados::CodecError::VersionTooOld { got: 1, min: 2, .. })
            ),
            "{err:?}"
        );
    }

    #[test]
    fn resharding_ops_match_the_oracle() {
        let set = SetBucketReshardingOp::default();
        assert_eq!(
            bytes(&set),
            unhex("01010f0000000301090000000000000000ffffffff")
        );
        assert_eq!(
            json(&set),
            r#"{"entry":{"reshard_status":"not-resharding"}}"#
        );

        assert_eq!(bytes(&ClearBucketReshardingOp {}), unhex("010100000000"));
        assert_eq!(json(&ClearBucketReshardingOp {}), "{}");
        assert_eq!(bytes(&GetBucketReshardingOp {}), unhex("010100000000"));
        assert_eq!(json(&GetBucketReshardingOp {}), "{}");
        assert_eq!(
            GetBucketReshardingOp::decode(&mut &unhex("010100000000")[..], 0).expect("decode"),
            GetBucketReshardingOp {}
        );

        let guard = GuardBucketReshardingOp::default();
        assert_eq!(bytes(&guard), unhex("01010400000000000000"));
        assert_eq!(json(&guard), r#"{"ret_err":0}"#);
        let busy = GuardBucketReshardingOp { ret_err: -2300 };
        assert_eq!(bytes(&busy), unhex("01010400000004f7ffff"));
        assert_eq!(json(&busy), r#"{"ret_err":-2300}"#);

        let ret = GetBucketReshardingRet::default();
        let wire = unhex("01010f0000000301090000000000000000ffffffff");
        assert_eq!(bytes(&ret), wire);
        assert_eq!(
            json(&ret),
            r#"{"new_instance":{"reshard_status":"not-resharding"}}"#
        );
        assert_eq!(
            GetBucketReshardingRet::decode(&mut &wire[..], 0).expect("decode"),
            ret
        );
    }

    #[test]
    fn suggestions_are_an_op_byte_and_an_entry_each() {
        let entry = DirEntry::default();
        let entry_wire = unhex(concat!(
            "080370000000",
            "00000000",
            "0000000000000000",
            "00",
            "070332000000",
            "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
            "00000000",
            "00000000",
            "01010a00000088ffffffffffffffff00",
            "00",
            "00000000",
            "00000000",
            "0000",
            "0000000000000000",
        ));
        assert_eq!(bytes(&entry), entry_wire);

        let update = Suggestion {
            op: SuggestOp::Update,
            log: false,
            entry: entry.clone(),
        };
        let mut want = vec![0x75];
        want.extend_from_slice(&entry_wire);
        assert_eq!(
            encode_suggestions(std::slice::from_ref(&update))
                .expect("encode")
                .as_ref(),
            &want[..]
        );

        let logged_remove = Suggestion {
            op: SuggestOp::Remove,
            log: true,
            entry,
        };
        let logged_update = Suggestion {
            log: true,
            ..update
        };
        let mut want = vec![0xf5];
        want.extend_from_slice(&entry_wire);
        want.push(0xf2);
        want.extend_from_slice(&entry_wire);
        assert_eq!(
            encode_suggestions(&[logged_update, logged_remove])
                .expect("encode")
                .as_ref(),
            &want[..]
        );
        assert!(encode_suggestions(&[]).expect("encode").is_empty());
    }

    #[test]
    fn ops_name_the_rgw_class_and_their_method() {
        use rados::osdclient::types::OpData;

        let stats = BTreeMap::new();
        let cases = [
            (init_index_op(), "bucket_init_index"),
            (set_tag_timeout_op(1), "bucket_set_tag_timeout"),
            (prepare_op(&PrepareOp::default()), "bucket_prepare_op"),
            (complete_op(&CompleteOp::default()), "bucket_complete_op"),
            (list_op(&ListOp::default()), "bucket_list"),
            (dir_header_op(), "bucket_list"),
            (check_index_op(), "bucket_check_index"),
            (rebuild_index_op(), "bucket_rebuild_index"),
            (update_stats_op(false, &stats), "bucket_update_stats"),
            (suggest_changes_op(&[]), "dir_suggest_changes"),
            (remove_obj_op(&[]), "obj_remove"),
            (store_pg_ver_op("a"), "obj_store_pg_ver"),
            (check_attrs_prefix_op("p", true), "obj_check_attrs_prefix"),
            (
                check_mtime_op(UTime::default(), CheckMtimeType::EQ, false),
                "obj_check_mtime",
            ),
            (
                set_bucket_resharding_op(ReshardStatus::IN_PROGRESS),
                "set_bucket_resharding",
            ),
            (clear_bucket_resharding_op(), "clear_bucket_resharding"),
            (guard_bucket_resharding_op(0), "guard_bucket_resharding"),
            (guard_op(), "guard_bucket_resharding"),
            (get_bucket_resharding_op(), "get_bucket_resharding"),
        ];
        for (op, method) in cases {
            let op = op.expect("op");
            assert!(op.indata.starts_with(format!("rgw{method}").as_bytes()));
            assert!(matches!(
                op.op_data,
                OpData::Call { class_len: 3, method_len, .. } if usize::from(method_len) == method.len()
            ));
        }

        // The raw methods carry no struct; the header read is a zero-entry list.
        let op = init_index_op().expect("op");
        assert!(matches!(op.op_data, OpData::Call { indata_len: 0, .. }));
        let op = dir_header_op().expect("op");
        assert!(op.indata.ends_with(&bytes(&ListOp::default())));
        let op = set_bucket_resharding_op(ReshardStatus::IN_PROGRESS).expect("op");
        assert!(
            op.indata
                .ends_with(&unhex("01010f0000000301090000000100000000ffffffff"))
        );
    }

    #[test]
    fn guard_op_fails_with_busy_resharding() {
        let op = guard_op().expect("op");
        assert!(op.indata.ends_with(&unhex("01010400000004f7ffff")));
    }

    #[test]
    fn suggest_changes_op_carries_the_bare_concatenation() {
        let s = Suggestion {
            op: SuggestOp::Remove,
            log: false,
            entry: dir_entry_instance(),
        };
        let op = suggest_changes_op(std::slice::from_ref(&s)).expect("op");
        let mut want = b"rgwdir_suggest_changes".to_vec();
        want.push(b'r');
        want.extend_from_slice(&bytes(&s.entry));
        assert_eq!(op.indata.as_ref(), &want[..]);
    }

    fn reply<T: Denc>(v: &T) -> OpReply {
        OpReply {
            return_code: 0,
            outdata: rados::encode_with_capacity(v, 0).expect("encode"),
        }
    }

    #[test]
    fn decoders_unwrap_the_replies() {
        let mut dir = Dir {
            header: dir_header_instance(),
            entries: BTreeMap::new(),
        };
        dir.entries.insert("name".to_owned(), dir_entry_instance());
        let ret = ListRet {
            dir,
            is_truncated: true,
            marker: ObjKey {
                name: "name".to_owned(),
                instance: String::new(),
            },
        };
        assert_eq!(decode_list(&reply(&ret)).expect("decode"), ret);
        assert_eq!(
            decode_dir_header(&reply(&ret)).expect("decode"),
            dir_header_instance()
        );

        let check = CheckIndexRet {
            existing_header: dir_header_instance(),
            calculated_header: DirHeader::default(),
        };
        assert_eq!(decode_check_index(&reply(&check)).expect("decode"), check);

        let get = GetBucketReshardingRet {
            new_instance: bucket_instance_entry(),
        };
        assert_eq!(
            decode_get_bucket_resharding(&reply(&get)).expect("decode"),
            bucket_instance_entry()
        );
    }
}
