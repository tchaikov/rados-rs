//! OSD map types, decoding, and placement helpers for OSD-facing cluster state.
//!
//! This module models Ceph's `OSDMap` and related pool and OSD metadata types,
//! including the versioned decode logic needed to ingest monitor-provided maps.
//! It also provides placement helpers used by the client to map objects and PGs
//! onto OSD sets, plus serialization helpers that keep `dencoder` JSON output
//! aligned with ceph-dencoder where practical.

use super::types::PoolFlags;
use crate::crush::CrushMap;
use crate::crush::mapper::CRUSH_ITEM_NONE;
use crate::denc::{
    Denc, EntityAddr, EntityAddrvec, FixedSize, RadosError, VersionedEncode,
    mark_feature_dependent_encoding, mark_simple_encoding, mark_versioned_encoding,
};
use bytes::{Buf, BufMut, Bytes};
use libc;
use serde::Serialize;
use std::collections::BTreeMap;

/// Re-export basic types from denc
pub use crate::denc::{EVersion, Epoch, FsId, UTime, UuidD, Version};

/// Re-export PgId from rados-crush
pub use crate::crush::PgId;

/// Default capacity for the CRUSH placement result LRU cache.
const CRUSH_CACHE_SIZE: std::num::NonZeroUsize = match std::num::NonZeroUsize::new(1000) {
    Some(v) => v,
    None => unreachable!(),
};

/// Varint encoding/decoding utilities (matching C++ denc_varint)
fn decode_varint<B: Buf>(buf: &mut B) -> Result<u64, RadosError> {
    if buf.remaining() < 1 {
        return Err(RadosError::Protocol(
            "Insufficient bytes for varint".to_string(),
        ));
    }

    let mut byte = buf.get_u8();
    let mut v = (byte & 0x7f) as u64;
    let mut shift = 7;

    while (byte & 0x80) != 0 {
        if buf.remaining() < 1 {
            return Err(RadosError::Protocol("Incomplete varint".to_string()));
        }
        byte = buf.get_u8();
        v |= ((byte & 0x7f) as u64) << shift;
        shift += 7;
    }

    Ok(v)
}

fn encode_varint<B: BufMut>(mut v: u64, buf: &mut B) -> Result<(), RadosError> {
    let mut byte = (v & 0x7f) as u8;
    v >>= 7;

    while v != 0 {
        byte |= 0x80;
        buf.put_u8(byte);
        byte = (v & 0x7f) as u8;
        v >>= 7;
    }
    buf.put_u8(byte);
    Ok(())
}

/// Shard ID type (shard_id_t in C++)
/// Wraps an i8 value representing a shard identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Default)]
pub struct ShardId(pub i8);

impl ShardId {
    pub const NO_SHARD: ShardId = ShardId(-1);

    pub fn new(id: i8) -> Self {
        ShardId(id)
    }
}

impl Denc for ShardId {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        buf.put_i8(self.0);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 1 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for ShardId".to_string(),
            ));
        }
        Ok(ShardId(buf.get_i8()))
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(1)
    }
}

impl FixedSize for ShardId {
    const SIZE: usize = 1;
}

/// Shard ID Set (shard_id_set in C++)
/// A bitset that can store up to 128 shard IDs efficiently
/// Encoded as 2 u64 words using varint encoding
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub struct ShardIdSet {
    // Two 64-bit words to represent 128 bits
    words: [u64; 2],
}

impl ShardIdSet {
    pub fn insert(&mut self, shard_id: ShardId) {
        let id = shard_id.0;
        if id >= 0 {
            let uid = id as usize;
            if uid < 128 {
                let word_idx = uid / 64;
                let bit_idx = uid % 64;
                self.words[word_idx] |= 1u64 << bit_idx;
            }
        }
    }

    pub fn contains(&self, shard_id: ShardId) -> bool {
        let id = shard_id.0;
        if id >= 0 {
            let uid = id as usize;
            if uid < 128 {
                let word_idx = uid / 64;
                let bit_idx = uid % 64;
                return (self.words[word_idx] & (1u64 << bit_idx)) != 0;
            }
        }
        false
    }

    pub fn clear(&mut self) {
        self.words = [0, 0];
    }

    pub fn is_empty(&self) -> bool {
        self.words[0] == 0 && self.words[1] == 0
    }

    pub fn iter(&self) -> ShardIdSetIter<'_> {
        ShardIdSetIter {
            set: self,
            current: 0,
        }
    }
}

pub struct ShardIdSetIter<'a> {
    set: &'a ShardIdSet,
    current: i16,
}

impl<'a> Iterator for ShardIdSetIter<'a> {
    type Item = ShardId;

    fn next(&mut self) -> Option<Self::Item> {
        while self.current < 128 {
            let shard_id = ShardId(self.current as i8);
            self.current += 1;
            if self.set.contains(shard_id) {
                return Some(shard_id);
            }
        }
        None
    }
}

impl Denc for ShardIdSet {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        // Encode each word using varint
        encode_varint(self.words[0], buf)?;
        encode_varint(self.words[1], buf)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        // Decode each word using varint
        let word0 = decode_varint(buf)?;
        let word1 = decode_varint(buf)?;
        Ok(ShardIdSet {
            words: [word0, word1],
        })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        // Varint size is variable, so we can't determine it without encoding
        None
    }
}

/// Format a UTime as a timestamp string matching ceph-dencoder's output.
///
/// Mirrors `utime_t::localtime()` in `src/include/utime.cc`:
/// - If sec < 10 years (315,360,000 s): format as `"<sec>.<usec:06>"` (relative time)
/// - Otherwise: ISO 8601 local time with timezone offset, e.g. `"2024-09-29T14:51:58.799272+0800"`
fn format_utime_as_timestamp(utime: &UTime) -> String {
    // Zero timestamp
    if utime.sec == 0 && utime.nsec == 0 {
        return "0.000000".to_string();
    }

    // C++ threshold: 60*60*24*365*10 (10 years). Treat as relative time.
    if utime.sec < 60 * 60 * 24 * 365 * 10 {
        let microseconds = utime.nsec / 1000;
        return format!("{}.{:06}", utime.sec, microseconds);
    }

    // Absolute time: convert via localtime_r, matching C++ localtime() call.
    let microseconds = utime.nsec / 1000;
    unsafe {
        let tt = utime.sec as libc::time_t;
        let mut tm: libc::tm = std::mem::zeroed();
        libc::localtime_r(&tt, &mut tm);

        let year = tm.tm_year + 1900;
        let month = tm.tm_mon + 1;
        let day = tm.tm_mday;
        let hour = tm.tm_hour;
        let min = tm.tm_min;
        let sec = tm.tm_sec;
        let gmtoff = tm.tm_gmtoff; // offset from UTC in seconds

        let tz_sign = if gmtoff >= 0 { '+' } else { '-' };
        let tz_hours = gmtoff.unsigned_abs() / 3600;
        let tz_mins = (gmtoff.unsigned_abs() % 3600) / 60;

        format!(
            "{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}.{microseconds:06}{tz_sign}{tz_hours:02}{tz_mins:02}"
        )
    }
}

/// Explicit Hash HitSet parameters - no additional parameters needed
#[derive(Debug, Clone, Default, Serialize, crate::VersionedDenc)]
#[denc(crate = "crate", version = 1, compat = 1)]
pub struct ExplicitHashHitSetParams;

/// Explicit Object HitSet parameters - no additional parameters needed
#[derive(Debug, Clone, Default, Serialize, crate::VersionedDenc)]
#[denc(crate = "crate", version = 1, compat = 1)]
pub struct ExplicitObjectHitSetParams;

/// BloomHitSet parameters - separate VersionedEncode implementation
#[derive(Debug, Clone, Default, Serialize, crate::VersionedDenc)]
#[denc(crate = "crate", version = 1, compat = 1)]
pub struct BloomHitSetParams {
    pub fpp_micro: u32,
    pub target_size: u64,
    pub seed: u64,
}

/// HitSet parameter types - using dedicated types for each variant
/// Type discriminants match C++ HitSet::Params::TYPE_* constants
const HITSET_TYPE_NONE: u8 = 0;
const HITSET_TYPE_EXPLICIT_HASH: u8 = 1;
const HITSET_TYPE_EXPLICIT_OBJECT: u8 = 2;
const HITSET_TYPE_BLOOM: u8 = 3;

#[derive(Debug, Clone, Default)]
pub enum HitSetParams {
    #[default]
    None,
    ExplicitHash(ExplicitHashHitSetParams),
    ExplicitObject(ExplicitObjectHitSetParams),
    Bloom(BloomHitSetParams),
}

// Custom Serialize implementation to match ceph-dencoder format
// Reference: ~/dev/ceph/src/osd/HitSet.cc BloomHitSet::Params::dump()
impl Serialize for HitSetParams {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        // Use max field count (4 for Bloom) as hint; Serde treats it as advisory
        let mut state = serializer.serialize_struct("HitSetParams", 4)?;

        match self {
            HitSetParams::None => {
                state.serialize_field("type", "none")?;
            }
            HitSetParams::ExplicitHash(_params) => {
                // ExplicitHashHitSetParams is a unit struct — no additional fields
                state.serialize_field("type", "explicit_hash")?;
            }
            HitSetParams::ExplicitObject(_params) => {
                // ExplicitObjectHitSetParams is a unit struct — no additional fields
                state.serialize_field("type", "explicit_object")?;
            }
            HitSetParams::Bloom(params) => {
                // Matches BloomHitSet::Params::dump(): false_positive_probability, target_size, seed
                state.serialize_field("type", "bloom")?;
                // fpp_micro stores FPP * 1_000_000 as an integer; dump as float
                let fpp = params.fpp_micro as f64 / 1_000_000.0;
                state.serialize_field("false_positive_probability", &fpp)?;
                state.serialize_field("target_size", &params.target_size)?;
                state.serialize_field("seed", &params.seed)?;
            }
        }

        state.end()
    }
}

impl VersionedEncode for HitSetParams {
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
    ) -> Result<(), RadosError> {
        // Write directly to buf parameter

        match self {
            HitSetParams::None => {
                buf.put_u8(HITSET_TYPE_NONE);
            }
            HitSetParams::ExplicitHash(params) => {
                buf.put_u8(HITSET_TYPE_EXPLICIT_HASH);
                params.encode_versioned(buf, features)?;
            }
            HitSetParams::ExplicitObject(params) => {
                buf.put_u8(HITSET_TYPE_EXPLICIT_OBJECT);
                params.encode_versioned(buf, features)?;
            }
            HitSetParams::Bloom(params) => {
                buf.put_u8(HITSET_TYPE_BLOOM);
                params.encode_versioned(buf, features)?;
            }
        }

        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        _version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        if buf.remaining() < 1 {
            return Err(RadosError::Protocol(
                "Insufficient buf for HitSetParams type".to_string(),
            ));
        }

        let hit_set_type = buf.get_u8();

        match hit_set_type {
            HITSET_TYPE_NONE => Ok(HitSetParams::None),
            HITSET_TYPE_EXPLICIT_HASH => {
                let params = ExplicitHashHitSetParams::decode_versioned(buf, features)?;
                Ok(HitSetParams::ExplicitHash(params))
            }
            HITSET_TYPE_EXPLICIT_OBJECT => {
                let params = ExplicitObjectHitSetParams::decode_versioned(buf, features)?;
                Ok(HitSetParams::ExplicitObject(params))
            }
            HITSET_TYPE_BLOOM => {
                let params = BloomHitSetParams::decode_versioned(buf, features)?;
                Ok(HitSetParams::Bloom(params))
            }
            _ => Err(RadosError::Protocol(format!(
                "Unknown HitSet type: {hit_set_type}"
            ))),
        }
    }

    fn encoded_size_content(&self, features: u64, _version: u8) -> Option<usize> {
        let mut size = 1; // type byte
        size += match self {
            HitSetParams::None => 0,
            HitSetParams::ExplicitHash(params) => params.encoded_size(features)?,
            HitSetParams::ExplicitObject(params) => params.encoded_size(features)?,
            HitSetParams::Bloom(params) => params.encoded_size(features)?,
        };
        Some(size)
    }
}

crate::denc::impl_denc_for_versioned!(HitSetParams);

/// Ceph UUID type (uuid_d in C++)
/// OSD extended information structure (osd_xinfo_t in C++)
#[derive(Debug, Clone, Default)]
pub struct OsdXInfo {
    pub down_stamp: UTime,              // timestamp when we were last marked down
    pub laggy_probability: f32,         // 0.0 = definitely not laggy, 1.0 = definitely laggy
    pub laggy_interval: u32, // average interval between being marked laggy and recovering
    pub features: u64,       // features supported by this osd we should know about
    pub old_weight: u32,     // weight prior to being auto marked out
    pub last_purged_snaps_scrub: UTime, // last scrub of purged_snaps
    pub dead_epoch: Epoch,   // last epoch we were confirmed dead (not just down)
}

// Custom Serialize implementation to match ceph-dencoder timestamp format
impl Serialize for OsdXInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("OsdXInfo", 7)?;

        // Format UTime fields as ISO 8601 timestamp strings to match ceph-dencoder
        let down_stamp_str = format_utime_as_timestamp(&self.down_stamp);
        state.serialize_field("down_stamp", &down_stamp_str)?;

        // Serialize laggy_probability as integer 0 when it's 0.0, otherwise as float
        if self.laggy_probability == 0.0 {
            state.serialize_field("laggy_probability", &0)?;
        } else {
            state.serialize_field("laggy_probability", &self.laggy_probability)?;
        }

        state.serialize_field("laggy_interval", &self.laggy_interval)?;
        state.serialize_field("features", &self.features)?;
        state.serialize_field("old_weight", &self.old_weight)?;

        let last_purged_snaps_scrub_str = format_utime_as_timestamp(&self.last_purged_snaps_scrub);
        state.serialize_field("last_purged_snaps_scrub", &last_purged_snaps_scrub_str)?;

        state.serialize_field("dead_epoch", &self.dead_epoch)?;
        state.end()
    }
}

impl VersionedEncode for OsdXInfo {
    // Quincy+ peers always use version 4 on encode. Older versions remain decode-compatible.

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
    ) -> Result<(), RadosError> {
        self.down_stamp.encode(buf, features)?;

        // laggy_probability is stored as a fixed-point u32 on the wire
        let lp = (self.laggy_probability * 0xffffffffu32 as f32) as u32;
        lp.encode(buf, features)?;
        self.laggy_interval.encode(buf, features)?;
        self.features.encode(buf, features)?;
        self.old_weight.encode(buf, features)?;
        self.last_purged_snaps_scrub.encode(buf, features)?;
        self.dead_epoch.encode(buf, features)?;

        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Quincy always emits v4 (SERVER_OCTOPUS guaranteed); all fields present.
        crate::denc::check_min_version!(version, 4, "OsdXInfo", "Quincy v17+");
        let down_stamp = UTime::decode(buf, features)?;
        let lp = u32::decode(buf, features)?;
        let laggy_probability = lp as f32 / 0xffffffffu32 as f32;
        let laggy_interval = u32::decode(buf, features)?;
        let osd_features = u64::decode(buf, features)?;
        let old_weight = u32::decode(buf, features)?;
        let last_purged_snaps_scrub = UTime::decode(buf, features)?;
        let dead_epoch = Epoch::decode(buf, features)?;

        Ok(OsdXInfo {
            down_stamp,
            laggy_probability,
            laggy_interval,
            features: osd_features,
            old_weight,
            last_purged_snaps_scrub,
            dead_epoch,
        })
    }

    fn encoded_size_content(&self, features: u64, _version: u8) -> Option<usize> {
        Some(
            self.down_stamp.encoded_size(features)?
                + 4
                + 4
                + 8
                + 4
                + self.last_purged_snaps_scrub.encoded_size(features)?
                + 4,
        )
    }
}

crate::denc::impl_denc_for_versioned!(OsdXInfo);

/// OSD information structure (osd_info_t in C++)
#[derive(Debug, Clone, Serialize, crate::StructVDenc)]
#[denc(crate = "crate", struct_v = 1)]
pub struct OsdInfo {
    #[serde(skip)]
    pub struct_v: u8,
    pub last_clean_begin: Epoch, // last interval that ended with a clean osd shutdown
    pub last_clean_end: Epoch,
    pub up_from: Epoch, // epoch osd marked up
    pub up_thru: Epoch, // lower bound on actual osd death (if > up_from)
    pub down_at: Epoch, // upper bound on actual osd death (if > up_from)
    pub lost_at: Epoch, // last epoch we decided data was "lost"
}

impl OsdInfo {
    const STRUCT_V: u8 = 1;
}

impl Default for OsdInfo {
    fn default() -> Self {
        Self {
            struct_v: Self::STRUCT_V,
            last_clean_begin: Epoch::default(),
            last_clean_end: Epoch::default(),
            up_from: Epoch::default(),
            up_thru: Epoch::default(),
            down_at: Epoch::default(),
            lost_at: Epoch::default(),
        }
    }
}

impl crate::FixedSize for OsdInfo {
    const SIZE: usize = 25;
}

/// Pool type constants matching Ceph
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum PoolType {
    Replicated = 1,
    Erasure = 3,
}

impl TryFrom<u8> for PoolType {
    type Error = crate::RadosError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(PoolType::Replicated),
            3 => Ok(PoolType::Erasure),
            _ => Err(crate::RadosError::InvalidData(format!(
                "Invalid PoolType value: {value}"
            ))),
        }
    }
}

/// Simplified pg_pool_t implementation based on actual Ceph encoding
/// This follows the exact field order from osd_types.cc
#[derive(Debug, Clone, Default)]
pub struct PgPool {
    // Basic pool configuration (always present)
    /// `pg_pool_t::TYPE_REPLICATED` (1) or `pg_pool_t::TYPE_ERASURE` (3).
    pub pool_type: u8,
    pub size: u8,
    pub crush_rule: u8,  // Fixed: should be u8, not i32
    pub object_hash: u8, // Fixed: should be u8, not u32

    // PG numbers
    pub pg_num: u32,
    pub pgp_num: u32,

    // Legacy localized pg nums (always 0)
    pub lpg_num: u32,  // always 0
    pub lpgp_num: u32, // always 0

    // Change tracking
    pub last_change: Epoch,

    // Snapshot info
    pub snap_seq: u64,
    pub snap_epoch: Epoch,

    // Snaps and removed snaps - use generic maps/sets
    pub snaps: BTreeMap<u64, PoolSnapInfo>,
    pub removed_snaps: Vec<SnapInterval>,

    // Auth UID
    pub auid: u64,

    // Flags and crash_replay_interval (version dependent)
    pub flags: u64,
    pub min_size: u8,

    // Quota
    pub quota_max_bytes: u64,
    pub quota_max_objects: u64,

    // Tier info
    pub tiers: Vec<u64>,
    pub tier_of: i64,
    pub cache_mode: u8,
    pub read_tier: i64,
    pub write_tier: i64,

    // Properties
    pub properties: BTreeMap<String, String>,

    // Hit set (using proper HitSetParams type)
    pub hit_set_params: HitSetParams,
    pub hit_set_period: u32,
    pub hit_set_count: u32,

    // More fields that come in different versions
    pub stripe_width: u32,
    pub target_max_bytes: u64,
    pub target_max_objects: u64,
    pub cache_target_dirty_ratio_micro: u32,
    pub cache_target_full_ratio_micro: u32,
    pub cache_min_flush_age: u32,
    pub cache_min_evict_age: u32,
    pub erasure_code_profile: String,
    pub last_force_op_resend_preluminous: Epoch,
    pub min_read_recency_for_promote: i32,
    pub expected_num_objects: u64,

    // Version 19+
    pub cache_target_dirty_high_ratio_micro: u32,

    // Version 20+
    pub min_write_recency_for_promote: i32,

    // Version 21+
    pub use_gmt_hitset: bool,

    // Version 22+
    pub fast_read: bool,

    // Version 23+
    pub hit_set_grade_decay_rate: i32,
    pub hit_set_search_last_n: u32,

    // Version 24+ - pool options (simplified)
    pub opts_data: Vec<u8>,

    // Version 25+
    pub last_force_op_resend_prenautilus: Epoch,

    // Version 26+ - application metadata
    pub application_metadata: BTreeMap<String, BTreeMap<String, String>>,

    // Version 27+
    pub create_time: UTime,

    // Version 28+ fields
    pub pg_num_target: u32,
    pub pgp_num_target: u32,
    pub pg_num_pending: u32,
    pub last_force_op_resend: Epoch,
    pub pg_autoscale_mode: u8,

    // Version 29+
    pub last_pg_merge_meta: PgMergeMeta,

    // Version 30+
    pub peering_crush_bucket_count: u32,
    pub peering_crush_bucket_target: u32,
    pub peering_crush_bucket_barrier: u32,
    pub peering_crush_mandatory_member: i32,

    // Version 32+
    pub nonprimary_shards: ShardIdSet,
}

// Custom Serialize implementation for PgPool to format create_time as timestamp
impl Serialize for PgPool {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("PgPool", 40)?; // approximate field count

        // Application metadata first (matches ceph order)
        state.serialize_field("application_metadata", &self.application_metadata)?;
        state.serialize_field("auid", &self.auid)?;
        state.serialize_field("cache_min_evict_age", &self.cache_min_evict_age)?;
        state.serialize_field("cache_min_flush_age", &self.cache_min_flush_age)?;

        // cache_mode as string
        let cache_mode_str = match self.cache_mode {
            0 => "none",
            1 => "writeback",
            2 => "forward",
            3 => "readonly",
            4 => "readforward",
            5 => "readproxy",
            _ => "unknown",
        };
        state.serialize_field("cache_mode", &cache_mode_str)?;

        state.serialize_field(
            "cache_target_dirty_high_ratio_micro",
            &self.cache_target_dirty_high_ratio_micro,
        )?;
        state.serialize_field(
            "cache_target_dirty_ratio_micro",
            &self.cache_target_dirty_ratio_micro,
        )?;
        state.serialize_field(
            "cache_target_full_ratio_micro",
            &self.cache_target_full_ratio_micro,
        )?;

        // Format create_time as timestamp string
        let create_time_str = format_utime_as_timestamp(&self.create_time);
        state.serialize_field("create_time", &create_time_str)?;

        state.serialize_field("crush_rule", &self.crush_rule)?;
        state.serialize_field("erasure_code_profile", &self.erasure_code_profile)?;
        state.serialize_field("expected_num_objects", &self.expected_num_objects)?;
        state.serialize_field("fast_read", &self.fast_read)?;
        state.serialize_field("flags", &self.flags)?;
        state.serialize_field("hit_set_count", &self.hit_set_count)?;
        state.serialize_field("hit_set_grade_decay_rate", &self.hit_set_grade_decay_rate)?;
        state.serialize_field("hit_set_params", &self.hit_set_params)?;
        state.serialize_field("hit_set_period", &self.hit_set_period)?;
        state.serialize_field("hit_set_search_last_n", &self.hit_set_search_last_n)?;

        // last_change as string
        state.serialize_field("last_change", &self.last_change.to_string())?;
        state.serialize_field(
            "last_force_op_resend",
            &self.last_force_op_resend.to_string(),
        )?;
        state.serialize_field(
            "last_force_op_resend_preluminous",
            &self.last_force_op_resend_preluminous.to_string(),
        )?;
        state.serialize_field(
            "last_force_op_resend_prenautilus",
            &self.last_force_op_resend_prenautilus.to_string(),
        )?;

        state.serialize_field("last_pg_merge_meta", &self.last_pg_merge_meta)?;
        state.serialize_field(
            "min_read_recency_for_promote",
            &self.min_read_recency_for_promote,
        )?;
        state.serialize_field("min_size", &self.min_size)?;
        state.serialize_field(
            "min_write_recency_for_promote",
            &self.min_write_recency_for_promote,
        )?;
        state.serialize_field("object_hash", &self.object_hash)?;

        // Note: ceph-dencoder adds computed fields like "options", "flags_names", "is_stretch_pool", etc.
        // We skip lpg_num, lpgp_num (legacy, always 0), opts_data (not in ceph output)

        // pg_autoscale_mode as string
        let pg_autoscale_mode_str = match self.pg_autoscale_mode {
            0 => "off",
            1 => "warn",
            2 => "on",
            _ => "unknown",
        };
        state.serialize_field("pg_autoscale_mode", &pg_autoscale_mode_str)?;

        state.serialize_field("pg_num", &self.pg_num)?;
        state.serialize_field("pg_num_pending", &self.pg_num_pending)?;
        state.serialize_field("pg_num_target", &self.pg_num_target)?;

        // Use ceph's field name
        state.serialize_field("pg_placement_num", &self.pgp_num)?;
        state.serialize_field("pg_placement_num_target", &self.pgp_num_target)?;

        // pool_snaps - use field name from ceph
        state.serialize_field("pool_snaps", &self.snaps)?;

        state.serialize_field("quota_max_bytes", &self.quota_max_bytes)?;
        state.serialize_field("quota_max_objects", &self.quota_max_objects)?;
        state.serialize_field("read_tier", &self.read_tier)?;

        // removed_snaps using ceph's field name
        state.serialize_field("removed_snaps", &self.removed_snaps)?;

        state.serialize_field("size", &self.size)?;
        state.serialize_field("snap_epoch", &self.snap_epoch)?;
        state.serialize_field("snap_seq", &self.snap_seq)?;
        state.serialize_field("stripe_width", &self.stripe_width)?;
        state.serialize_field("target_max_bytes", &self.target_max_bytes)?;
        state.serialize_field("target_max_objects", &self.target_max_objects)?;
        state.serialize_field("tier_of", &self.tier_of)?;
        state.serialize_field("tiers", &self.tiers)?;
        state.serialize_field("use_gmt_hitset", &self.use_gmt_hitset)?;
        state.serialize_field("write_tier", &self.write_tier)?;

        state.end()
    }
}

/// Pool snapshot information structure
#[derive(Debug, Clone, Default)]
pub struct PoolSnapInfo {
    pub snapid: u64,
    pub stamp: UTime,
    pub name: String,
}

// Custom Serialize implementation to match ceph-dencoder timestamp format
impl Serialize for PoolSnapInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("PoolSnapInfo", 3)?;

        state.serialize_field("snapid", &self.snapid)?;

        // Format UTime as ISO 8601 timestamp string to match ceph-dencoder
        let timestamp_str = format_utime_as_timestamp(&self.stamp);
        state.serialize_field("stamp", &timestamp_str)?;

        state.serialize_field("name", &self.name)?;
        state.end()
    }
}

impl VersionedEncode for PoolSnapInfo {
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
    ) -> Result<(), RadosError> {
        self.snapid.encode(buf, features)?;
        self.stamp.encode(buf, features)?;
        self.name.encode(buf, features)?;
        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        crate::denc::check_min_version!(version, 2, "PoolSnapInfo", "Quincy v17+");
        let snapid = u64::decode(buf, features)?;
        let stamp = UTime::decode(buf, features)?;
        let name = String::decode(buf, features)?;
        Ok(PoolSnapInfo {
            snapid,
            stamp,
            name,
        })
    }

    fn encoded_size_content(&self, features: u64, _version: u8) -> Option<usize> {
        // snapid (u64=8) + stamp (UTime=8) + name (u32 len + bytes)
        Some(8 + self.stamp.encoded_size(features)? + self.name.encoded_size(features)?)
    }
}
crate::denc::impl_denc_for_versioned!(PoolSnapInfo);

/// Snapshot interval for removed snaps
#[derive(Debug, Clone, Default, Serialize, crate::Denc)]
#[denc(crate = "crate")]
pub struct SnapInterval {
    pub start: u64,
    pub len: u64,
}

impl crate::FixedSize for SnapInterval {
    const SIZE: usize = 16;
}

impl PgPool {
    // Version constants from Ceph
    const VERSION_CURRENT: u8 = 32; // Latest
    const COMPAT_VERSION: u8 = 5;
    const VERSION_OCTOPUS: u8 = 29;
    const VERSION_OCTOPUS_STRETCH: u8 = 30;

    /// `pg_pool_t::TYPE_REPLICATED`
    pub const TYPE_REPLICATED: u8 = 1;
    /// `pg_pool_t::TYPE_ERASURE`
    pub const TYPE_ERASURE: u8 = 3;

    /// Mirrors `pg_pool_t::FLAG_EC_OPTIMIZATIONS = 1<<19`.  When set, the
    /// monitor reorders pg_temp into "primaryfirst" form and the client
    /// has to reverse the transform to compute the right `spg_t` shard.
    pub const FLAG_EC_OPTIMIZATIONS: u64 = 1 << 19;

    pub fn is_replicated(&self) -> bool {
        self.pool_type == Self::TYPE_REPLICATED
    }

    pub fn is_erasure(&self) -> bool {
        self.pool_type == Self::TYPE_ERASURE
    }

    /// Mirrors `pg_pool_t::allows_ecoptimizations()` in C++.
    pub fn allows_ecoptimizations(&self) -> bool {
        self.flags & Self::FLAG_EC_OPTIMIZATIONS != 0
    }

    /// Mirrors `pg_pool_t::is_nonprimary_shard(shard)` — true if the
    /// given shard is in the pool's `nonprimary_shards` set (only
    /// non-empty for optimized EC pools).
    pub fn is_nonprimary_shard(&self, shard: ShardId) -> bool {
        !self.nonprimary_shards.is_empty() && self.nonprimary_shards.contains(shard)
    }

    /// Reorder a shard-ID-indexed acting set into "primaryfirst" form
    /// for an optimized EC pool, mirroring the vector form of C++
    /// `OSDMap::pgtemp_primaryfirst`.  Walks each input position as a
    /// shard id; entries whose shard id is in `nonprimary_shards` are
    /// moved to the back, otherwise they keep their relative order at
    /// the front.  No-op for non-optimized EC and replicated pools.
    ///
    /// The OSDMonitor pre-applies this transform before storing
    /// `pg_temp` in incremental updates, so for typical optimized 4+2
    /// EC pools (`nonprimary_shards = {4,5}`) the transform is the
    /// identity and `pg_temp` already-stored is "right" without any
    /// further reshuffling.  For asymmetric layouts the transform
    /// genuinely moves entries.
    pub fn pgtemp_primaryfirst_vec(&self, pg_temp: &[i32]) -> Vec<i32> {
        if !self.allows_ecoptimizations() || self.nonprimary_shards.is_empty() {
            return pg_temp.to_vec();
        }
        let mut result = Vec::with_capacity(pg_temp.len());
        let mut nonprimary = Vec::new();
        for (idx, &osd) in pg_temp.iter().enumerate() {
            if self.is_nonprimary_shard(ShardId::new(idx as i8)) {
                nonprimary.push(osd);
            } else {
                result.push(osd);
            }
        }
        result.extend(nonprimary);
        result
    }

    /// Reverse `pgtemp_primaryfirst_vec` when the PG has a stored temp set.
    /// Returns an error if the optimized EC layout exceeds 128 shards,
    /// has out-of-range nonprimary shards, or its temp length differs from size.
    pub fn pgtemp_undo_primaryfirst_vec(
        &self,
        has_pg_temp: bool,
        acting: &[i32],
    ) -> Result<Vec<i32>, RadosError> {
        if !self.allows_ecoptimizations() || !has_pg_temp {
            return Ok(acting.to_vec());
        }
        let size = usize::from(self.size);
        if size > i8::MAX as usize + 1 || acting.len() != size {
            return Err(RadosError::Protocol(format!(
                "Invalid optimized EC pg_temp: pool size {size}, acting length {} (maximum 128 shards)",
                acting.len()
            )));
        }
        if self
            .nonprimary_shards
            .iter()
            .any(|shard| shard.0 as usize >= size)
        {
            return Err(RadosError::Protocol(
                "Optimized EC nonprimary shard is outside pool size".into(),
            ));
        }
        let mut primary = 0;
        let mut nonprimary = size - self.nonprimary_shards.iter().count();
        Ok((0..size)
            .map(|shard| {
                let index = if self.is_nonprimary_shard(ShardId::new(shard as i8)) {
                    let index = nonprimary;
                    nonprimary += 1;
                    index
                } else {
                    let index = primary;
                    primary += 1;
                    index
                };
                acting[index]
            })
            .collect())
    }

    /// Convert an original shard id to its primaryfirst position.
    pub fn pgtemp_primaryfirst_shard(&self, has_pg_temp: bool, shard: ShardId) -> ShardId {
        if shard == ShardId::NO_SHARD
            || shard.0 == 0
            || !self.allows_ecoptimizations()
            || !has_pg_temp
        {
            return shard;
        }
        let nonprimary_count = self.nonprimary_shards.iter().count() as i32;
        let num_parity_shards = self.size as i32 - nonprimary_count - 1;
        let shard = shard.0 as i32;
        ShardId::new(if shard >= self.size as i32 - num_parity_shards {
            shard + num_parity_shards + 1 - self.size as i32
        } else {
            shard + num_parity_shards
        } as i8)
    }

    /// Inverse of `pgtemp_primaryfirst_vec` — convert a "primaryfirst"
    /// position back to the actual shard ID.  Mirrors the `shard_id_t`
    /// form of C++ `OSDMap::pgtemp_undo_primaryfirst`.
    ///
    /// Returns the input unchanged for shard 0, `NO_SHARD`, or pools
    /// that are not optimized EC, or when no `pg_temp` is in effect
    /// for the PG (the C++ function checks `has_pgtemp` internally
    /// because the reorder only happens when pg_temp gets stored).
    /// The math itself is byte-for-byte identical to C++ so we route
    /// to the same shard the OSD expects.
    pub fn pgtemp_undo_primaryfirst_shard(&self, has_pg_temp: bool, pos: ShardId) -> ShardId {
        if pos == ShardId::NO_SHARD || pos.0 == 0 {
            return pos;
        }
        if !self.allows_ecoptimizations() || !has_pg_temp {
            return pos;
        }
        let size = self.size as i32;
        let nonprimary_count = self.nonprimary_shards.iter().count() as i32;
        let num_parity_shards = size - nonprimary_count - 1;
        let p = pos.0 as i32;
        let result = if p > num_parity_shards {
            p - num_parity_shards
        } else {
            p + size - num_parity_shards - 1
        };
        ShardId::new(result as i8)
    }

    pub fn is_stretch_pool(&self) -> bool {
        // For now, assume no stretch pools (like the default case in Ceph)
        // This would be based on crush rules and other pool configuration
        false
    }

    pub(crate) fn canonical_last_force_op_resend(&self) -> Epoch {
        if self.last_force_op_resend.as_u32() != 0 {
            self.last_force_op_resend
        } else if self.last_force_op_resend_prenautilus.as_u32() != 0 {
            self.last_force_op_resend_prenautilus
        } else {
            self.last_force_op_resend_preluminous
        }
    }
}

impl VersionedEncode for PgPool {
    // Quincy+ peers only need the modern 29/30/32 encode variants.
    const FEATURE_DEPENDENT: bool = true;

    fn encoding_version(&self, features: u64) -> u8 {
        use crate::denc::features::{CephFeatures, has_significant_feature};

        if has_significant_feature(features, CephFeatures::MASK_SERVER_TENTACLE) {
            Self::VERSION_CURRENT
        } else if self.is_stretch_pool() {
            Self::VERSION_OCTOPUS_STRETCH
        } else {
            Self::VERSION_OCTOPUS
        }
    }

    fn compat_version(&self, _features: u64) -> u8 {
        Self::COMPAT_VERSION
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        version: u8,
    ) -> Result<(), RadosError> {
        debug_assert!(matches!(
            version,
            Self::VERSION_OCTOPUS | Self::VERSION_OCTOPUS_STRETCH | Self::VERSION_CURRENT
        ));
        let canonical_last_force_op_resend = self.canonical_last_force_op_resend();

        // Basic fields (always present in ENCODE_START format)
        self.pool_type.encode(buf, features)?;
        self.size.encode(buf, features)?;
        self.crush_rule.encode(buf, features)?;
        self.object_hash.encode(buf, features)?;
        self.pg_num.encode(buf, features)?;
        self.pgp_num.encode(buf, features)?;
        self.lpg_num.encode(buf, features)?;
        self.lpgp_num.encode(buf, features)?;
        self.last_change.encode(buf, features)?;
        self.snap_seq.encode(buf, features)?;
        self.snap_epoch.encode(buf, features)?;
        self.snaps.encode(buf, features)?;
        self.removed_snaps.encode(buf, features)?;
        self.auid.encode(buf, features)?;
        self.flags.encode(buf, features)?;
        0u32.encode(buf, features)?; // crash_replay_interval - always 0, not stored in struct
        self.min_size.encode(buf, features)?;
        self.quota_max_bytes.encode(buf, features)?;
        self.quota_max_objects.encode(buf, features)?;
        self.tiers.encode(buf, features)?;
        self.tier_of.encode(buf, features)?;
        self.cache_mode.encode(buf, features)?;
        self.read_tier.encode(buf, features)?;
        self.write_tier.encode(buf, features)?;
        self.properties.encode(buf, features)?;
        self.hit_set_params.encode(buf, features)?;
        self.hit_set_period.encode(buf, features)?;
        self.hit_set_count.encode(buf, features)?;
        self.stripe_width.encode(buf, features)?;
        self.target_max_bytes.encode(buf, features)?;
        self.target_max_objects.encode(buf, features)?;
        self.cache_target_dirty_ratio_micro.encode(buf, features)?;
        self.cache_target_full_ratio_micro.encode(buf, features)?;
        self.cache_min_flush_age.encode(buf, features)?;
        self.cache_min_evict_age.encode(buf, features)?;
        self.erasure_code_profile.encode(buf, features)?;
        canonical_last_force_op_resend.encode(buf, features)?;
        self.min_read_recency_for_promote.encode(buf, features)?;
        self.expected_num_objects.encode(buf, features)?;
        self.cache_target_dirty_high_ratio_micro
            .encode(buf, features)?;
        self.min_write_recency_for_promote.encode(buf, features)?;
        self.use_gmt_hitset.encode(buf, features)?;
        self.fast_read.encode(buf, features)?;
        self.hit_set_grade_decay_rate.encode(buf, features)?;
        self.hit_set_search_last_n.encode(buf, features)?;

        // Pool opts - encoded with an inline version header (not a VersionedEncode container)
        buf.put_u8(2); // opts struct_v
        buf.put_u8(1); // opts compat_version
        let opts_len = self.opts_data.len() as u32;
        opts_len.encode(buf, features)?;
        buf.put_slice(&self.opts_data);

        canonical_last_force_op_resend.encode(buf, features)?;
        self.application_metadata.encode(buf, features)?;
        self.create_time.encode(buf, features)?;
        self.pg_num_target.encode(buf, features)?;
        self.pgp_num_target.encode(buf, features)?;
        self.pg_num_pending.encode(buf, features)?;
        0u32.encode(buf, features)?; // pg_num_dec_last_epoch_started (always 0)
        0u32.encode(buf, features)?; // pg_num_dec_last_epoch_clean (always 0)
        canonical_last_force_op_resend.encode(buf, features)?;
        self.pg_autoscale_mode.encode(buf, features)?;
        self.last_pg_merge_meta.encode(buf, features)?;

        if version == Self::VERSION_OCTOPUS_STRETCH {
            self.peering_crush_bucket_count.encode(buf, features)?;
            self.peering_crush_bucket_target.encode(buf, features)?;
            self.peering_crush_bucket_barrier.encode(buf, features)?;
            self.peering_crush_mandatory_member.encode(buf, features)?;
        } else if version == Self::VERSION_CURRENT {
            // Encode optional peering_crush_data (only if stretch pool)
            if self.is_stretch_pool() {
                true.encode(buf, features)?; // Some
                self.peering_crush_bucket_count.encode(buf, features)?;
                self.peering_crush_bucket_target.encode(buf, features)?;
                self.peering_crush_bucket_barrier.encode(buf, features)?;
                self.peering_crush_mandatory_member.encode(buf, features)?;
            } else {
                false.encode(buf, features)?; // None
            }
            self.nonprimary_shards.encode(buf, features)?;
        }

        Ok(())
    }

    #[allow(clippy::field_reassign_with_default)]
    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Quincy always emits v29 (non-stretch) or v30 (stretch pool); all fields through
        // v28 are unconditional, v29+ remain guarded for newer-cluster compatibility.
        crate::denc::check_min_version!(version, 29, "pg_pool_t", "Quincy v17+");

        let mut pool = PgPool::default();

        pool.pool_type = u8::decode(buf, features)?;
        pool.size = u8::decode(buf, features)?;
        pool.crush_rule = u8::decode(buf, features)?;
        pool.object_hash = u8::decode(buf, features)?;
        pool.pg_num = u32::decode(buf, features)?;
        pool.pgp_num = u32::decode(buf, features)?;
        pool.lpg_num = u32::decode(buf, features)?;
        pool.lpgp_num = u32::decode(buf, features)?;
        pool.last_change = Epoch::decode(buf, features)?;
        pool.snap_seq = u64::decode(buf, features)?;
        pool.snap_epoch = Epoch::decode(buf, features)?;
        // v3+
        pool.snaps = BTreeMap::decode(buf, features)?;
        pool.removed_snaps = Vec::decode(buf, features)?;
        pool.auid = u64::decode(buf, features)?;
        // v4+
        pool.flags = u64::decode(buf, features)?;
        let _crash_replay_interval = u32::decode(buf, features)?;
        // v7+
        pool.min_size = u8::decode(buf, features)?;
        // v8+
        pool.quota_max_bytes = u64::decode(buf, features)?;
        pool.quota_max_objects = u64::decode(buf, features)?;
        // v9+
        pool.tiers = Vec::decode(buf, features)?;
        pool.tier_of = i64::decode(buf, features)?;
        pool.cache_mode = u8::decode(buf, features)?;
        pool.read_tier = i64::decode(buf, features)?;
        pool.write_tier = i64::decode(buf, features)?;
        // v10+
        pool.properties = BTreeMap::decode(buf, features)?;
        // v11+
        pool.hit_set_params = HitSetParams::decode_versioned(buf, features)?;
        pool.hit_set_period = u32::decode(buf, features)?;
        pool.hit_set_count = u32::decode(buf, features)?;
        // v12+
        pool.stripe_width = u32::decode(buf, features)?;
        // v13+
        pool.target_max_bytes = u64::decode(buf, features)?;
        pool.target_max_objects = u64::decode(buf, features)?;
        pool.cache_target_dirty_ratio_micro = u32::decode(buf, features)?;
        pool.cache_target_full_ratio_micro = u32::decode(buf, features)?;
        pool.cache_min_flush_age = u32::decode(buf, features)?;
        pool.cache_min_evict_age = u32::decode(buf, features)?;
        // v14+
        pool.erasure_code_profile = String::decode(buf, features)?;
        // v15+
        pool.last_force_op_resend_preluminous = Epoch::decode(buf, features)?;
        // v16+
        pool.min_read_recency_for_promote = i32::decode(buf, features)?;
        // v17+
        pool.expected_num_objects = u64::decode(buf, features)?;
        // v19+
        pool.cache_target_dirty_high_ratio_micro = u32::decode(buf, features)?;
        // v20+
        pool.min_write_recency_for_promote = i32::decode(buf, features)?;
        // v21+
        pool.use_gmt_hitset = bool::decode(buf, features)?;
        // v22+
        pool.fast_read = bool::decode(buf, features)?;
        // v23+
        pool.hit_set_grade_decay_rate = i32::decode(buf, features)?;
        pool.hit_set_search_last_n = u32::decode(buf, features)?;
        // v24+: opts (versioned sub-encoding stored as raw bytes)
        {
            let _opts_version = buf.get_u8();
            let _opts_compat_version = buf.get_u8();
            let opts_len = buf.get_u32_le() as usize;
            if opts_len <= buf.remaining() {
                pool.opts_data = vec![0u8; opts_len];
                buf.copy_to_slice(&mut pool.opts_data);
            }
        }
        // v25+
        pool.last_force_op_resend_prenautilus = Epoch::decode(buf, features)?;
        // v26+
        pool.application_metadata = BTreeMap::decode(buf, features)?;
        // v27+
        pool.create_time = UTime::decode(buf, features)?;
        // v28+
        pool.pg_num_target = u32::decode(buf, features)?;
        pool.pgp_num_target = u32::decode(buf, features)?;
        pool.pg_num_pending = u32::decode(buf, features)?;
        let _pg_num_dec_last_epoch_started = u32::decode(buf, features)?;
        let _pg_num_dec_last_epoch_clean = u32::decode(buf, features)?;
        pool.last_force_op_resend = Epoch::decode(buf, features)?;
        pool.pg_autoscale_mode = u8::decode(buf, features)?;

        // v29+: always present at Quincy floor
        pool.last_pg_merge_meta = PgMergeMeta::decode_versioned(buf, features)?;

        // Version 30 fields (special handling)
        if version == 30 {
            pool.peering_crush_bucket_count = u32::decode(buf, features)?;
            pool.peering_crush_bucket_target = u32::decode(buf, features)?;
            pool.peering_crush_bucket_barrier = u32::decode(buf, features)?;
            pool.peering_crush_mandatory_member = i32::decode(buf, features)?;
        }

        // Version 31+ fields (optional tuple)
        if version >= 31 {
            // Decode Option<(u32, u32, u32, i32)>
            let peering_data: Option<(u32, u32, u32, i32)> = Option::decode(buf, features)?;
            if let Some((count, target, barrier, mandatory)) = peering_data {
                pool.peering_crush_bucket_count = count;
                pool.peering_crush_bucket_target = target;
                pool.peering_crush_bucket_barrier = barrier;
                pool.peering_crush_mandatory_member = mandatory;
            }
        }

        // Version 32+ fields
        if version >= 32 {
            pool.nonprimary_shards = ShardIdSet::decode(buf, features)?;
        }

        Ok(pool)
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None // Complex type - size computed by encoding
    }
}
// Manual Denc implementation for PgPool (uses VersionedEncode)
impl crate::Denc for PgPool {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.encode_versioned(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Self::decode_versioned(buf, features)
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        None
    }
}

/// PG Merge Metadata (pg_merge_meta_t in C++)
#[derive(Debug, Clone, Default, crate::VersionedDenc)]
#[denc(crate = "crate", version = 1, compat = 1)]
pub struct PgMergeMeta {
    pub source_pgid: PgId,
    pub ready_epoch: Epoch,
    pub last_epoch_started: Epoch,
    pub last_epoch_clean: Epoch,
    pub source_version: EVersion,
    pub target_version: EVersion,
}

// Custom Serialize implementation to match ceph-dencoder format
impl Serialize for PgMergeMeta {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("PgMergeMeta", 6)?;

        // Format source_pgid as "pool.seed" string
        let source_pgid_str = format!("{}.{}", self.source_pgid.pool, self.source_pgid.seed);
        state.serialize_field("source_pgid", &source_pgid_str)?;

        state.serialize_field("ready_epoch", &self.ready_epoch)?;
        state.serialize_field("last_epoch_started", &self.last_epoch_started)?;
        state.serialize_field("last_epoch_clean", &self.last_epoch_clean)?;

        // Format versions as "epoch'version" string
        let source_version_str = format!(
            "{}'{}",
            self.source_version.epoch, self.source_version.version
        );
        state.serialize_field("source_version", &source_version_str)?;

        let target_version_str = format!(
            "{}'{}",
            self.target_version.epoch, self.target_version.version
        );
        state.serialize_field("target_version", &target_version_str)?;

        state.end()
    }
}

/// Snapshot interval set (snap_interval_set_t in C++)
/// A set of snapshot intervals represented as a vector of SnapInterval
#[derive(Debug, Clone, Default, Serialize, crate::Denc)]
#[denc(crate = "crate")]
pub struct SnapIntervalSet {
    pub intervals: Vec<SnapInterval>,
}

/// Ceph release type (ceph_release_t in C++)
pub type CephRelease = u8;

/// Incremental OSDMap update (OSDMap::Incremental in C++)
/// Represents a diff from epoch-1 to epoch
#[derive(Debug, Clone, Serialize, Default)]
pub struct OSDMapIncremental {
    /// Feature bits used for encoding
    pub encode_features: u64,

    /// Filesystem UUID
    pub fsid: UuidD,

    /// New epoch (this is a diff from epoch-1 to epoch)
    pub epoch: Epoch,

    /// Modification timestamp
    pub modified: UTime,

    /// New pool max (incremented on each pool create)
    pub new_pool_max: i64,

    /// New flags
    pub new_flags: i32,

    /// Full map (if this is a full update instead of incremental)
    pub fullmap: Bytes,

    /// CRUSH map update
    pub crush: Bytes,

    // Incremental changes
    pub new_max_osd: i32,
    pub new_pools: BTreeMap<u64, PgPool>,
    pub new_pool_names: BTreeMap<u64, String>,
    pub old_pools: Vec<u64>,

    pub new_up_client: BTreeMap<i32, EntityAddrvec>,
    pub new_state: BTreeMap<i32, u32>,
    pub new_weight: BTreeMap<i32, u32>,
    pub new_pg_temp: BTreeMap<PgId, Vec<i32>>,
    pub new_primary_temp: BTreeMap<PgId, i32>,
    pub new_primary_affinity: BTreeMap<i32, u32>,

    pub new_erasure_code_profiles: BTreeMap<String, BTreeMap<String, String>>,
    pub old_erasure_code_profiles: Vec<String>,

    pub new_pg_upmap: BTreeMap<PgId, Vec<i32>>,
    pub old_pg_upmap: Vec<PgId>,
    pub new_pg_upmap_items: BTreeMap<PgId, Vec<(i32, i32)>>,
    pub old_pg_upmap_items: Vec<PgId>,

    pub new_removed_snaps: BTreeMap<u64, SnapIntervalSet>,
    pub new_purged_snaps: BTreeMap<u64, SnapIntervalSet>,

    pub new_last_up_change: UTime,
    pub new_last_in_change: UTime,

    pub new_pg_upmap_primary: BTreeMap<PgId, i32>,
    pub old_pg_upmap_primary: Vec<PgId>,

    // Extended OSD-only data
    pub new_hb_back_up: BTreeMap<i32, EntityAddrvec>,
    pub new_up_thru: BTreeMap<i32, Epoch>,
    pub new_last_clean_interval: BTreeMap<i32, (Epoch, Epoch)>,
    pub new_lost: BTreeMap<i32, Epoch>,
    pub new_blocklist: BTreeMap<EntityAddr, UTime>,
    pub old_blocklist: Vec<EntityAddr>,
    pub new_up_cluster: BTreeMap<i32, EntityAddrvec>,
    pub cluster_snapshot: String,
    pub new_uuid: BTreeMap<i32, UuidD>,
    pub new_xinfo: BTreeMap<i32, OsdXInfo>,
    pub new_hb_front_up: BTreeMap<i32, EntityAddrvec>,

    pub new_nearfull_ratio: f32,
    pub new_full_ratio: f32,
    pub new_backfillfull_ratio: f32,

    pub new_require_min_compat_client: CephRelease,
    pub new_require_osd_release: CephRelease,

    pub new_crush_node_flags: BTreeMap<i32, u32>,
    pub new_device_class_flags: BTreeMap<i32, u32>,

    pub change_stretch_mode: bool,
    pub new_stretch_bucket_count: u32,
    pub new_degraded_stretch_mode: u32,
    pub new_recovering_stretch_mode: u32,
    pub new_stretch_mode_bucket: i32,
    pub stretch_mode_enabled: bool,

    pub new_range_blocklist: BTreeMap<EntityAddr, UTime>,
    pub old_range_blocklist: Vec<EntityAddr>,

    pub mutate_allow_crimson: u8,

    // CRC fields
    pub have_crc: bool,
    pub full_crc: u32,
    pub inc_crc: u32,
}

const OSDMAP_NESTED_HEADER_SIZE: usize = 6;
const OSDMAP_INCREMENTAL_CLIENT_VERSION: u8 = 9;
const OSDMAP_INCREMENTAL_OSD_VERSION: u8 = 12;
const OSDMAP_CLIENT_VERSION: u8 = 10;
const OSDMAP_OSD_VERSION: u8 = 12;

#[derive(Default)]
struct OSDMapIncrementalClientSection {
    fsid: UuidD,
    epoch: Epoch,
    modified: UTime,
    new_pool_max: i64,
    new_flags: i32,
    fullmap: Bytes,
    crush: Bytes,
    new_max_osd: i32,
    new_pools: BTreeMap<u64, PgPool>,
    new_pool_names: BTreeMap<u64, String>,
    old_pools: Vec<u64>,
    new_up_client: BTreeMap<i32, EntityAddrvec>,
    new_state: BTreeMap<i32, u32>,
    new_weight: BTreeMap<i32, u32>,
    new_pg_temp: BTreeMap<PgId, Vec<i32>>,
    new_primary_temp: BTreeMap<PgId, i32>,
    new_primary_affinity: BTreeMap<i32, u32>,
    new_erasure_code_profiles: BTreeMap<String, BTreeMap<String, String>>,
    old_erasure_code_profiles: Vec<String>,
    new_pg_upmap: BTreeMap<PgId, Vec<i32>>,
    old_pg_upmap: Vec<PgId>,
    new_pg_upmap_items: BTreeMap<PgId, Vec<(i32, i32)>>,
    old_pg_upmap_items: Vec<PgId>,
    new_removed_snaps: BTreeMap<u64, SnapIntervalSet>,
    new_purged_snaps: BTreeMap<u64, SnapIntervalSet>,
    new_last_up_change: UTime,
    new_last_in_change: UTime,
    new_pg_upmap_primary: BTreeMap<PgId, i32>,
    old_pg_upmap_primary: Vec<PgId>,
}

impl VersionedEncode for OSDMapIncrementalClientSection {
    const MAX_DECODE_VERSION: u8 = OSDMAP_INCREMENTAL_CLIENT_VERSION;

    fn encoding_version(&self, _features: u64) -> u8 {
        OSDMAP_INCREMENTAL_CLIENT_VERSION
    }

    fn compat_version(&self, _features: u64) -> u8 {
        OSDMAP_INCREMENTAL_CLIENT_VERSION
    }

    fn encode_content<B: BufMut>(
        &self,
        _buf: &mut B,
        _features: u64,
        _version: u8,
    ) -> Result<(), RadosError> {
        Err(RadosError::Protocol(
            "OSDMapIncremental client section encoding not yet implemented".to_string(),
        ))
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Quincy always emits v8 (SERVER_NAUTILUS guaranteed); all fields through v8 present.
        crate::denc::check_min_version!(
            version,
            8,
            "OSDMapIncremental::ClientSection",
            "Quincy v17+"
        );
        let mut section = Self {
            fsid: UuidD::decode(buf, features)?,
            epoch: Epoch::decode(buf, features)?,
            modified: UTime::decode(buf, features)?,
            new_pool_max: i64::decode(buf, features)?,
            new_flags: i32::decode(buf, features)?,
            fullmap: Bytes::decode(buf, features)?,
            crush: Bytes::decode(buf, features)?,
            new_max_osd: i32::decode(buf, features)?,
            new_pools: BTreeMap::decode(buf, features)?,
            new_pool_names: BTreeMap::decode(buf, features)?,
            old_pools: Vec::decode(buf, features)?,
            new_up_client: BTreeMap::decode(buf, features)?,
            new_state: BTreeMap::decode(buf, features)?,
            new_weight: BTreeMap::decode(buf, features)?,
            new_pg_temp: BTreeMap::decode(buf, features)?,
            new_primary_temp: BTreeMap::decode(buf, features)?,
            new_primary_affinity: BTreeMap::decode(buf, features)?,
            new_erasure_code_profiles: BTreeMap::decode(buf, features)?,
            old_erasure_code_profiles: Vec::decode(buf, features)?,
            new_pg_upmap: BTreeMap::decode(buf, features)?,
            old_pg_upmap: Vec::decode(buf, features)?,
            new_pg_upmap_items: BTreeMap::decode(buf, features)?,
            old_pg_upmap_items: Vec::decode(buf, features)?,
            new_removed_snaps: BTreeMap::decode(buf, features)?,
            new_purged_snaps: BTreeMap::decode(buf, features)?,
            new_last_up_change: UTime::decode(buf, features)?,
            new_last_in_change: UTime::decode(buf, features)?,
            ..Default::default()
        };

        if version >= 9 {
            section.new_pg_upmap_primary = BTreeMap::decode(buf, features)?;
            section.old_pg_upmap_primary = Vec::decode(buf, features)?;
        }

        Ok(section)
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

#[derive(Default)]
struct OSDMapIncrementalOsdSection {
    new_hb_back_up: BTreeMap<i32, EntityAddrvec>,
    new_up_thru: BTreeMap<i32, Epoch>,
    new_last_clean_interval: BTreeMap<i32, (Epoch, Epoch)>,
    new_lost: BTreeMap<i32, Epoch>,
    new_blocklist: BTreeMap<EntityAddr, UTime>,
    old_blocklist: Vec<EntityAddr>,
    new_up_cluster: BTreeMap<i32, EntityAddrvec>,
    cluster_snapshot: String,
    new_uuid: BTreeMap<i32, UuidD>,
    new_xinfo: BTreeMap<i32, OsdXInfo>,
    new_hb_front_up: BTreeMap<i32, EntityAddrvec>,
    encode_features: u64,
    new_nearfull_ratio: f32,
    new_full_ratio: f32,
    new_backfillfull_ratio: f32,
    new_require_min_compat_client: CephRelease,
    new_require_osd_release: CephRelease,
    new_crush_node_flags: BTreeMap<i32, u32>,
    new_device_class_flags: BTreeMap<i32, u32>,
    change_stretch_mode: bool,
    new_stretch_bucket_count: u32,
    new_degraded_stretch_mode: u32,
    new_recovering_stretch_mode: u32,
    new_stretch_mode_bucket: i32,
    stretch_mode_enabled: bool,
    new_range_blocklist: BTreeMap<EntityAddr, UTime>,
    old_range_blocklist: Vec<EntityAddr>,
    mutate_allow_crimson: u8,
}

impl VersionedEncode for OSDMapIncrementalOsdSection {
    const MAX_DECODE_VERSION: u8 = OSDMAP_INCREMENTAL_OSD_VERSION;

    fn encoding_version(&self, _features: u64) -> u8 {
        OSDMAP_INCREMENTAL_OSD_VERSION
    }

    fn compat_version(&self, _features: u64) -> u8 {
        OSDMAP_INCREMENTAL_OSD_VERSION
    }

    fn encode_content<B: BufMut>(
        &self,
        _buf: &mut B,
        _features: u64,
        _version: u8,
    ) -> Result<(), RadosError> {
        Err(RadosError::Protocol(
            "OSDMapIncremental OSD section encoding not yet implemented".to_string(),
        ))
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Quincy always emits v9 (SERVER_NAUTILUS guaranteed); all fields through v9 present.
        crate::denc::check_min_version!(version, 9, "OSDMapIncremental::OsdSection", "Quincy v17+");
        let mut section = Self {
            new_hb_back_up: BTreeMap::decode(buf, features)?,
            new_up_thru: BTreeMap::decode(buf, features)?,
            new_last_clean_interval: BTreeMap::decode(buf, features)?,
            new_lost: BTreeMap::decode(buf, features)?,
            new_blocklist: BTreeMap::decode(buf, features)?,
            old_blocklist: Vec::decode(buf, features)?,
            new_up_cluster: BTreeMap::decode(buf, features)?,
            cluster_snapshot: String::decode(buf, features)?,
            new_uuid: BTreeMap::decode(buf, features)?,
            new_xinfo: BTreeMap::decode(buf, features)?,
            new_hb_front_up: BTreeMap::decode(buf, features)?,
            encode_features: u64::decode(buf, features)?,
            new_nearfull_ratio: f32::decode(buf, features)?,
            new_full_ratio: f32::decode(buf, features)?,
            new_backfillfull_ratio: f32::decode(buf, features)?,
            new_require_min_compat_client: u8::decode(buf, features)?,
            new_require_osd_release: u8::decode(buf, features)?,
            new_crush_node_flags: BTreeMap::decode(buf, features)?,
            new_device_class_flags: BTreeMap::decode(buf, features)?,
            ..Default::default()
        };

        if version >= 10 {
            section.change_stretch_mode = bool::decode(buf, features)?;
            section.new_stretch_bucket_count = u32::decode(buf, features)?;
            section.new_degraded_stretch_mode = u32::decode(buf, features)?;
            section.new_recovering_stretch_mode = u32::decode(buf, features)?;
            section.new_stretch_mode_bucket = i32::decode(buf, features)?;
            section.stretch_mode_enabled = bool::decode(buf, features)?;
        }

        if version >= 11 {
            section.new_range_blocklist = BTreeMap::decode(buf, features)?;
            section.old_range_blocklist = Vec::decode(buf, features)?;
        }

        if version >= 12 {
            section.mutate_allow_crimson = u8::decode(buf, features)?;
        }

        Ok(section)
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

#[derive(Default)]
struct OSDMapClientSection {
    fsid: UuidD,
    epoch: Epoch,
    created: UTime,
    modified: UTime,
    pools: BTreeMap<u64, PgPool>,
    pool_name: BTreeMap<u64, String>,
    pool_max: i32,
    flags: u32,
    max_osd: i32,
    osd_state: Vec<u32>,
    osd_weight: Vec<u32>,
    osd_addrs_client: Vec<EntityAddrvec>,
    pg_temp: BTreeMap<PgId, Vec<i32>>,
    primary_temp: BTreeMap<PgId, i32>,
    osd_primary_affinity: Vec<u32>,
    crush: Option<CrushMap>,
    erasure_code_profiles: BTreeMap<String, BTreeMap<String, String>>,
    pg_upmap: BTreeMap<PgId, Vec<i32>>,
    pg_upmap_items: BTreeMap<PgId, Vec<(i32, i32)>>,
    crush_version: i32,
    new_removed_snaps: BTreeMap<u64, SnapIntervalSet>,
    new_purged_snaps: BTreeMap<u64, SnapIntervalSet>,
    last_up_change: UTime,
    last_in_change: UTime,
    pg_upmap_primaries: BTreeMap<PgId, i32>,
}

impl VersionedEncode for OSDMapClientSection {
    const MAX_DECODE_VERSION: u8 = OSDMAP_CLIENT_VERSION;

    fn encoding_version(&self, _features: u64) -> u8 {
        OSDMAP_CLIENT_VERSION
    }

    fn compat_version(&self, _features: u64) -> u8 {
        OSDMAP_CLIENT_VERSION
    }

    fn encode_content<B: BufMut>(
        &self,
        _buf: &mut B,
        _features: u64,
        _version: u8,
    ) -> Result<(), RadosError> {
        Err(RadosError::Protocol(
            "OSDMap client section encoding not yet implemented".to_string(),
        ))
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Quincy always emits v9 (SERVER_NAUTILUS guaranteed); all fields through v9 present.
        crate::denc::check_min_version!(version, 9, "OSDMapClientSection", "Quincy v17+");
        let mut section = Self {
            fsid: UuidD::decode(buf, features)?,
            epoch: Epoch::decode(buf, features)?,
            created: UTime::decode(buf, features)?,
            modified: UTime::decode(buf, features)?,
            pools: BTreeMap::decode(buf, features)?,
            pool_name: BTreeMap::decode(buf, features)?,
            pool_max: i32::decode(buf, features)?,
            flags: u32::decode(buf, features)?,
            max_osd: i32::decode(buf, features)?,
            osd_state: Vec::decode(buf, features)?,
            osd_weight: Vec::decode(buf, features)?,
            osd_addrs_client: Vec::decode(buf, features)?,
            pg_temp: BTreeMap::decode(buf, features)?,
            primary_temp: BTreeMap::decode(buf, features)?,
            osd_primary_affinity: Vec::decode(buf, features)?,
            ..Default::default()
        };

        let mut crush_bytes = Bytes::decode(buf, features)?;
        if !crush_bytes.is_empty() {
            section.crush = Some(
                CrushMap::decode(&mut crush_bytes)
                    .map_err(|error| RadosError::Crush(Box::new(error)))?,
            );
        }

        section.erasure_code_profiles = BTreeMap::decode(buf, features)?;
        section.pg_upmap = BTreeMap::decode(buf, features)?;
        section.pg_upmap_items = BTreeMap::decode(buf, features)?;
        section.crush_version = i32::decode(buf, features)?;
        section.new_removed_snaps = BTreeMap::decode(buf, features)?;
        section.new_purged_snaps = BTreeMap::decode(buf, features)?;
        section.last_up_change = UTime::decode(buf, features)?;
        section.last_in_change = UTime::decode(buf, features)?;

        if version >= 10 {
            section.pg_upmap_primaries = BTreeMap::decode(buf, features)?;
        }

        Ok(section)
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

#[derive(Default)]
struct OSDMapOsdSection {
    osd_addrs_hb_back: Vec<EntityAddrvec>,
    osd_info: Vec<OsdInfo>,
    blocklist: BTreeMap<EntityAddr, UTime>,
    osd_addrs_cluster: Vec<EntityAddrvec>,
    cluster_snapshot_epoch: Epoch,
    cluster_snapshot: String,
    osd_uuid: Vec<UuidD>,
    osd_xinfo: Vec<OsdXInfo>,
    osd_addrs_hb_front: Vec<EntityAddrvec>,
    nearfull_ratio: f32,
    full_ratio: f32,
    backfillfull_ratio: f32,
    require_min_compat_client: CephRelease,
    require_osd_release: CephRelease,
    removed_snaps_queue: Vec<(i64, SnapIntervalSet)>,
    crush_node_flags: BTreeMap<i32, u32>,
    device_class_flags: BTreeMap<i32, u32>,
    stretch_mode_enabled: bool,
    stretch_bucket_count: u32,
    degraded_stretch_mode: u32,
    recovering_stretch_mode: u32,
    stretch_mode_bucket: i32,
    range_blocklist: BTreeMap<EntityAddr, UTime>,
    allow_crimson: bool,
}

impl VersionedEncode for OSDMapOsdSection {
    const MAX_DECODE_VERSION: u8 = OSDMAP_OSD_VERSION;

    fn encoding_version(&self, _features: u64) -> u8 {
        OSDMAP_OSD_VERSION
    }

    fn compat_version(&self, _features: u64) -> u8 {
        OSDMAP_OSD_VERSION
    }

    fn encode_content<B: BufMut>(
        &self,
        _buf: &mut B,
        _features: u64,
        _version: u8,
    ) -> Result<(), RadosError> {
        Err(RadosError::Protocol(
            "OSDMap OSD section encoding not yet implemented".to_string(),
        ))
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Quincy always emits v9 (SERVER_NAUTILUS guaranteed); all fields through v9 present.
        crate::denc::check_min_version!(version, 9, "OSDMapOsdSection", "Quincy v17+");
        let mut section = Self {
            osd_addrs_hb_back: Vec::decode(buf, features)?,
            osd_info: Vec::decode(buf, features)?,
            blocklist: BTreeMap::decode(buf, features)?,
            osd_addrs_cluster: Vec::decode(buf, features)?,
            cluster_snapshot_epoch: Epoch::decode(buf, features)?,
            cluster_snapshot: String::decode(buf, features)?,
            osd_uuid: Vec::decode(buf, features)?,
            osd_xinfo: Vec::decode(buf, features)?,
            osd_addrs_hb_front: Vec::decode(buf, features)?,
            nearfull_ratio: f32::decode(buf, features)?,
            full_ratio: f32::decode(buf, features)?,
            backfillfull_ratio: f32::decode(buf, features)?,
            require_min_compat_client: CephRelease::decode(buf, features)?,
            require_osd_release: CephRelease::decode(buf, features)?,
            removed_snaps_queue: Vec::decode(buf, features)?,
            crush_node_flags: BTreeMap::decode(buf, features)?,
            device_class_flags: BTreeMap::decode(buf, features)?,
            ..Default::default()
        };

        if version >= 10 {
            section.stretch_mode_enabled = bool::decode(buf, features)?;
            section.stretch_bucket_count = u32::decode(buf, features)?;
            section.degraded_stretch_mode = u32::decode(buf, features)?;
            section.recovering_stretch_mode = u32::decode(buf, features)?;
            section.stretch_mode_bucket = i32::decode(buf, features)?;
        }

        if version >= 11 {
            section.range_blocklist = BTreeMap::decode(buf, features)?;
        }

        if version >= 12 {
            section.allow_crimson = bool::decode(buf, features)?;
        }

        Ok(section)
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None
    }
}

impl OSDMapIncremental {
    pub fn new(epoch: Epoch) -> Self {
        Self {
            epoch,
            new_pool_max: -1,
            new_flags: -1,
            new_max_osd: -1,
            new_nearfull_ratio: -1.0,
            new_full_ratio: -1.0,
            new_backfillfull_ratio: -1.0,
            new_require_min_compat_client: 0xff, // Unknown
            new_require_osd_release: 0xff,       // Unknown,
            ..Default::default()
        }
    }

    /// Decodes a versioned incremental OSDMap update from Ceph wire data.
    ///
    /// `OSDMapIncremental` is intentionally decode-only for now because the
    /// encoder has not been implemented yet.
    pub fn decode_versioned<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        // Outer wrapper (version 8, legacy compat 7)
        if buf.remaining() < OSDMAP_NESTED_HEADER_SIZE {
            return Err(RadosError::Codec(crate::CodecError::InsufficientData {
                needed: OSDMAP_NESTED_HEADER_SIZE,
                available: buf.remaining(),
            }));
        }

        // Save the original 6 header bytes for CRC calculation
        // (CRC includes the header as encoded by Ceph)
        let header_bytes = {
            let chunk = buf.chunk();
            if chunk.len() < 6 {
                return Err(RadosError::Protocol(
                    "Cannot read header bytes for CRC".into(),
                ));
            }
            bytes::Bytes::copy_from_slice(&chunk[..6])
        };

        let struct_v = buf.get_u8();
        let _struct_compat = buf.get_u8();
        let struct_len = buf.get_u32_le() as usize;

        if buf.remaining() < struct_len {
            return Err(RadosError::Codec(crate::CodecError::InsufficientData {
                needed: struct_len,
                available: buf.remaining(),
            }));
        }

        crate::denc::check_min_version!(struct_v, 7, "OSDMapIncremental", "Quincy v17+");

        // Extract outer content into Bytes
        let outer_bytes = buf.copy_to_bytes(struct_len);
        let mut outer_cursor = &outer_bytes[..];
        let client = OSDMapIncrementalClientSection::decode_versioned(&mut outer_cursor, features)?;
        let osd = OSDMapIncrementalOsdSection::decode_versioned(&mut outer_cursor, features)?;
        let mut inc = OSDMapIncremental::new(client.epoch);

        inc.fsid = client.fsid;
        inc.modified = client.modified;
        inc.new_pool_max = client.new_pool_max;
        inc.new_flags = client.new_flags;
        inc.fullmap = client.fullmap;
        inc.crush = client.crush;
        inc.new_max_osd = client.new_max_osd;
        inc.new_pools = client.new_pools;
        inc.new_pool_names = client.new_pool_names;
        inc.old_pools = client.old_pools;
        inc.new_up_client = client.new_up_client;
        inc.new_state = client.new_state;
        inc.new_weight = client.new_weight;
        inc.new_pg_temp = client.new_pg_temp;
        inc.new_primary_temp = client.new_primary_temp;
        inc.new_primary_affinity = client.new_primary_affinity;
        inc.new_erasure_code_profiles = client.new_erasure_code_profiles;
        inc.old_erasure_code_profiles = client.old_erasure_code_profiles;
        inc.new_pg_upmap = client.new_pg_upmap;
        inc.old_pg_upmap = client.old_pg_upmap;
        inc.new_pg_upmap_items = client.new_pg_upmap_items;
        inc.old_pg_upmap_items = client.old_pg_upmap_items;
        inc.new_removed_snaps = client.new_removed_snaps;
        inc.new_purged_snaps = client.new_purged_snaps;
        inc.new_last_up_change = client.new_last_up_change;
        inc.new_last_in_change = client.new_last_in_change;
        inc.new_pg_upmap_primary = client.new_pg_upmap_primary;
        inc.old_pg_upmap_primary = client.old_pg_upmap_primary;

        inc.new_hb_back_up = osd.new_hb_back_up;
        inc.new_up_thru = osd.new_up_thru;
        inc.new_last_clean_interval = osd.new_last_clean_interval;
        inc.new_lost = osd.new_lost;
        inc.new_blocklist = osd.new_blocklist;
        inc.old_blocklist = osd.old_blocklist;
        inc.new_up_cluster = osd.new_up_cluster;
        inc.cluster_snapshot = osd.cluster_snapshot;
        inc.new_uuid = osd.new_uuid;
        inc.new_xinfo = osd.new_xinfo;
        inc.new_hb_front_up = osd.new_hb_front_up;
        inc.encode_features = osd.encode_features;
        inc.new_nearfull_ratio = osd.new_nearfull_ratio;
        inc.new_full_ratio = osd.new_full_ratio;
        inc.new_backfillfull_ratio = osd.new_backfillfull_ratio;
        inc.new_require_min_compat_client = osd.new_require_min_compat_client;
        inc.new_require_osd_release = osd.new_require_osd_release;
        inc.new_crush_node_flags = osd.new_crush_node_flags;
        inc.new_device_class_flags = osd.new_device_class_flags;
        inc.change_stretch_mode = osd.change_stretch_mode;
        inc.new_stretch_bucket_count = osd.new_stretch_bucket_count;
        inc.new_degraded_stretch_mode = osd.new_degraded_stretch_mode;
        inc.new_recovering_stretch_mode = osd.new_recovering_stretch_mode;
        inc.new_stretch_mode_bucket = osd.new_stretch_mode_bucket;
        inc.stretch_mode_enabled = osd.stretch_mode_enabled;
        inc.new_range_blocklist = osd.new_range_blocklist;
        inc.old_range_blocklist = osd.old_range_blocklist;
        inc.mutate_allow_crimson = osd.mutate_allow_crimson;

        // CRC validation (version 8+)
        if struct_v >= 8 {
            if outer_cursor.remaining() < 8 {
                return Err(RadosError::Protocol(format!(
                    "OSDMapIncremental v{} is missing CRC trailer: need 8 bytes, have {}",
                    struct_v,
                    outer_cursor.remaining()
                )));
            }

            inc.have_crc = true;

            // Calculate CRC position (how many bytes consumed from outer_bytes)
            let crc_offset = outer_bytes.len() - outer_cursor.remaining();

            // Read CRC fields
            inc.inc_crc = u32::decode(&mut outer_cursor, features)?;
            inc.full_crc = u32::decode(&mut outer_cursor, features)?;

            // Validate CRC following C++ pattern from OSDMap.cc:1029-1057
            //
            // C++ encoding (OSDMap.cc:745-762):
            //   start_offset = bl.length();              // Before outer ENCODE_START
            //   ENCODE_START(8, 7, bl);                  // Adds 6-byte header
            //   <encode client section>
            //   <encode osd section>
            //   crc_offset = bl.length();
            //   crc_filler = bl.append_hole(4);          // Reserve for inc_crc
            //   tail_offset = bl.length();
            //   encode(full_crc, bl);                    // Write 4 bytes
            //   ENCODE_FINISH(bl);
            //
            //   front.substr_of(bl, start_offset, crc_offset - start_offset);
            //   inc_crc = front.crc32c(-1);              // CRC header + client + osd
            //   tail.substr_of(bl, tail_offset, bl.length() - tail_offset);
            //   inc_crc = tail.crc32c(inc_crc);          // Append CRC of full_crc
            //
            // So CRC covers: [header][client][osd][full_crc], excluding [inc_crc]
            //
            // Calculate CRC32C using pure Rust implementation
            //
            // Ceph uses ceph_crc32c_sctp (RFC 3720 SCTP/iSCSI variant). The CRC covers:
            // [header][client_section][osd_section][full_crc], excluding only the 4-byte inc_crc field.
            //
            // Note: The crc32c crate returns inverted CRC values (XOR with 0xFFFFFFFF),
            // but Ceph stores non-inverted values, so we need to invert the result.

            let crc_field_size = 4; // Size of inc_crc field
            let crc_tail_offset = crc_offset + crc_field_size;

            // Stream the covered regions through a single crc-fast Digest.
            // OSDMap incremental payloads can be tens of KB to MB during
            // startup, so the PCLMULQDQ path matters here too even though
            // this is not a per-op hot path.
            let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
            digest.update(&header_bytes);
            if crc_offset > 0 {
                digest.update(&outer_bytes[..crc_offset]);
            }
            if crc_tail_offset < outer_bytes.len() {
                digest.update(&outer_bytes[crc_tail_offset..]);
            }
            // Invert the CRC to match Ceph's convention.
            let actual_crc = !(digest.finalize() as u32);

            // Verify CRC matches
            if actual_crc != inc.inc_crc {
                return Err(RadosError::Protocol(format!(
                    "OSDMapIncremental CRC mismatch: expected 0x{:08x}, got 0x{:08x}",
                    inc.inc_crc, actual_crc
                )));
            }
        }

        // DECODE_FINISH: Forward compatibility - remaining bytes are ignored

        Ok(inc)
    }

    /// Apply this incremental update to an OSDMap
    pub fn apply_to(&self, base: &mut OSDMap) -> Result<(), RadosError> {
        // Exhaustive field manifest.  Every field of `OSDMapIncremental`
        // must be named here, either because `apply_to` below writes it
        // into `base` or because it is deliberately not applied.  Adding
        // a new field to the struct will fail to compile until it is
        // classified here — the compile-time checklist that would have
        // caught the silently-dropped blocklist deltas.
        let Self {
            // -------- Applied below --------
            epoch: _,
            modified: _,
            new_pool_max: _,
            new_flags: _,
            crush: _,
            new_max_osd: _,
            new_pools: _,
            new_pool_names: _,
            old_pools: _,
            new_up_client: _,
            new_state: _,
            new_weight: _,
            new_pg_temp: _,
            new_primary_temp: _,
            new_primary_affinity: _,
            new_erasure_code_profiles: _,
            old_erasure_code_profiles: _,
            new_pg_upmap: _,
            old_pg_upmap: _,
            new_pg_upmap_items: _,
            old_pg_upmap_items: _,
            new_removed_snaps: _,
            new_purged_snaps: _,
            new_last_up_change: _,
            new_last_in_change: _,
            new_pg_upmap_primary: _,
            old_pg_upmap_primary: _,
            new_blocklist: _,
            old_blocklist: _,
            new_range_blocklist: _,
            old_range_blocklist: _,
            new_nearfull_ratio: _,
            new_full_ratio: _,
            new_backfillfull_ratio: _,

            // -------- Intentionally not applied --------
            encode_features: _,               // wire-decode metadata
            fsid: _,                          // invariant within a session
            fullmap: _,                       // TODO: handle full-map replacement
            new_hb_back_up: _,                // server-side heartbeat addrs
            new_up_thru: _,                   // server-side OSD epoch history
            new_last_clean_interval: _,       // server-side OSD history
            new_lost: _,                      // admin OSD lost flag
            new_up_cluster: _,                // cluster-facing addrs (OSD-to-OSD)
            cluster_snapshot: _,              // cephfs snapshot name
            new_uuid: _,                      // per-OSD fsid (admin repair)
            new_xinfo: _,                     // extended OSD info (server-side)
            new_hb_front_up: _,               // server-side heartbeat addrs
            new_require_min_compat_client: _, // mon-enforced feature gate
            new_require_osd_release: _,       // mon-enforced feature gate
            new_crush_node_flags: _,
            new_device_class_flags: _, // carried in the CRUSH map bytes
            change_stretch_mode: _,    // stretch mode (rare deployment)
            new_stretch_bucket_count: _,
            new_degraded_stretch_mode: _,
            new_recovering_stretch_mode: _,
            new_stretch_mode_bucket: _,
            stretch_mode_enabled: _,
            mutate_allow_crimson: _, // crimson experimental
            have_crc: _,             // decode-time CRC flag
            full_crc: _,             // CRC of the carried full map
            inc_crc: _,              // CRC of the incremental
        } = self;

        // Sanity-check the incremental against the base before mutating
        // anything.  C++ `OSDMap::apply_incremental` does the same checks
        // (the fsid clause adopts the inc's fsid on epoch == 1 and
        // returns -EINVAL otherwise; the epoch clause asserts).  An
        // out-of-order or wrong-cluster incremental landing in our base
        // would silently corrupt routing state, so we refuse instead.
        if base.epoch.as_u32() != 0 && self.fsid != base.fsid {
            return Err(RadosError::Protocol(format!(
                "OSDMap incremental fsid {:?} does not match base {:?}; \
                 refusing to apply (wrong cluster?)",
                self.fsid, base.fsid
            )));
        }
        if self.epoch.as_u32() != base.epoch.as_u32() + 1 {
            return Err(RadosError::Protocol(format!(
                "OSDMap incremental epoch {} is not base epoch {} + 1; \
                 out-of-order incremental application would corrupt state",
                self.epoch.as_u32(),
                base.epoch.as_u32()
            )));
        }

        // Full-map replacement.  C++ apply_incremental short-circuits
        // here: if `inc.fullmap` is non-empty, decode it as a complete
        // OSDMap and ignore every other field.  We do not yet decode a
        // full OSDMap from raw bytes inside apply_to, so refuse the
        // incremental rather than apply the other fields on top of a
        // stale base — that would silently corrupt routing state.  The
        // caller is expected to retry with a fresh full-map fetch from
        // the monitor.
        if !self.fullmap.is_empty() {
            return Err(RadosError::Protocol(format!(
                "OSDMap incremental epoch {} carries a {}-byte full-map \
                 replacement; full-map application is not yet implemented \
                 (refusing to apply diff fields on top of stale base)",
                self.epoch.as_u32(),
                self.fullmap.len()
            )));
        }

        // Decode before changing the base: a bad topology must not leave
        // new pool/state data paired with the previous CRUSH map.
        let crush = if self.crush.is_empty() {
            None
        } else {
            Some(
                CrushMap::decode(&mut self.crush.clone())
                    .map_err(|error| RadosError::Crush(Box::new(error)))?,
            )
        };

        // Invalidate placement caches only after the update is validated.
        base.lock_acting_cache()?.clear();
        base.lock_crush_cache()?.clear();
        if base.epoch.as_u32() == 0 {
            base.fsid = self.fsid;
        }

        // Update epoch
        base.epoch = self.epoch;
        base.modified = self.modified;

        // Update pool max
        if self.new_pool_max >= 0 {
            base.pool_max = self.new_pool_max as i32;
        }

        // Update flags
        if self.new_flags >= 0 {
            base.flags = self.new_flags as u32;
        }

        // Update max_osd
        if self.new_max_osd >= 0 {
            base.max_osd = self.new_max_osd;
            // Resize vectors if needed
            if base.max_osd as usize > base.osd_state.len() {
                base.osd_state.resize(base.max_osd as usize, 0);
                base.osd_weight.resize(base.max_osd as usize, 0);
            }
        }

        // Apply pool changes
        for pool_id in &self.old_pools {
            base.pools.remove(pool_id);
            base.pool_name.remove(pool_id);
        }

        for (pool_id, pool) in &self.new_pools {
            base.pools.insert(*pool_id, pool.clone());
        }

        for (pool_id, name) in &self.new_pool_names {
            base.pool_name.insert(*pool_id, name.clone());
        }

        for (osd, weight) in &self.new_weight {
            let idx = *osd as usize;
            if idx < base.osd_weight.len() {
                base.osd_weight[idx] = *weight;
            }
        }

        // Apply primary_affinity changes.  C++ stores this as a parallel
        // vector indexed by osd_id; we mirror that shape.
        for (osd, affinity) in &self.new_primary_affinity {
            let idx = *osd as usize;
            if idx >= base.osd_primary_affinity.len() {
                base.osd_primary_affinity
                    .resize(idx + 1, CEPH_OSD_DEFAULT_PRIMARY_AFFINITY);
            }
            base.osd_primary_affinity[idx] = *affinity;
        }

        // Apply state XORs before new_up_client, which forces EXISTS|UP.
        // A zero delta toggles UP; toggling EXISTS off destroys the OSD.
        for (osd, state) in &self.new_state {
            let idx = *osd as usize;
            if idx >= base.osd_state.len() {
                continue;
            }
            let state = if *state == 0 { CEPH_OSD_UP } else { *state };
            if base.osd_state[idx] & state & CEPH_OSD_EXISTS != 0 {
                base.osd_state[idx] = 0;
                if let Some(affinity) = base.osd_primary_affinity.get_mut(idx) {
                    *affinity = CEPH_OSD_DEFAULT_PRIMARY_AFFINITY;
                }
                for addrs in [
                    &mut base.osd_addrs_client,
                    &mut base.osd_addrs_cluster,
                    &mut base.osd_addrs_hb_front,
                    &mut base.osd_addrs_hb_back,
                ] {
                    if let Some(addr) = addrs.get_mut(idx) {
                        *addr = EntityAddrvec::default();
                    }
                }
                if let Some(uuid) = base.osd_uuid.get_mut(idx) {
                    *uuid = UuidD::default();
                }
                if let Some(info) = base.osd_info.get_mut(idx) {
                    *info = OsdInfo::default();
                }
                if let Some(xinfo) = base.osd_xinfo.get_mut(idx) {
                    *xinfo = OsdXInfo::default();
                }
            } else {
                base.osd_state[idx] ^= state;
            }
        }

        for (osd, addrvec) in &self.new_up_client {
            let idx = *osd as usize;
            if idx >= base.osd_addrs_client.len() {
                base.osd_addrs_client
                    .resize(idx + 1, EntityAddrvec::default());
            }
            base.osd_addrs_client[idx] = addrvec.clone();
            if idx < base.osd_state.len() {
                base.osd_state[idx] |= CEPH_OSD_EXISTS | CEPH_OSD_UP;
                base.osd_state[idx] &= !CEPH_OSD_STOP;
            }
        }

        // Apply pg_temp changes
        if !self.new_pg_temp.is_empty() {
            tracing::debug!(
                target: "rados::osdclient::crush",
                "incremental epoch={} applying {} pg_temp changes",
                self.epoch.as_u32(),
                self.new_pg_temp.len(),
            );
        }
        for (pgid, osds) in &self.new_pg_temp {
            if osds.is_empty() {
                base.pg_temp.remove(pgid);
            } else {
                base.pg_temp.insert(*pgid, osds.clone());
            }
        }

        // Apply primary_temp changes.  -1 means "remove the primary_temp
        // override for this pg".
        for (pgid, osd) in &self.new_primary_temp {
            if *osd == -1 {
                base.primary_temp.remove(pgid);
            } else {
                base.primary_temp.insert(*pgid, *osd);
            }
        }

        // Apply pg_upmap (full acting-set override) changes.
        for pgid in &self.old_pg_upmap {
            base.pg_upmap.remove(pgid);
        }
        for (pgid, osds) in &self.new_pg_upmap {
            base.pg_upmap.insert(*pgid, osds.clone());
        }

        // Apply pg_upmap_items (per-osd swap) changes.
        for pgid in &self.old_pg_upmap_items {
            base.pg_upmap_items.remove(pgid);
        }
        for (pgid, items) in &self.new_pg_upmap_items {
            base.pg_upmap_items.insert(*pgid, items.clone());
        }

        // Apply pg_upmap_primaries (primary override) changes.
        for pgid in &self.old_pg_upmap_primary {
            base.pg_upmap_primaries.remove(pgid);
        }
        for (pgid, primary) in &self.new_pg_upmap_primary {
            base.pg_upmap_primaries.insert(*pgid, *primary);
        }

        for (node, flags) in &self.new_crush_node_flags {
            if *flags == 0 {
                base.crush_node_flags.remove(node);
            } else {
                base.crush_node_flags.insert(*node, *flags);
            }
        }

        // Apply erasure-code profile changes.
        for name in &self.old_erasure_code_profiles {
            base.erasure_code_profiles.remove(name);
        }
        for (name, profile) in &self.new_erasure_code_profiles {
            base.erasure_code_profiles
                .insert(name.clone(), profile.clone());
        }

        // Snap tracking: C++ apply_incremental REPLACES `new_removed_snaps`
        // wholesale (it is a per-epoch delta, not an accumulator), then
        // unions deltas into the cumulative `removed_snaps_queue` and
        // subtracts purged intervals from that queue.  The client uses
        // `new_removed_snaps` (the per-epoch delta) for snap-context
        // pruning; the queue is for OSD bookkeeping.  Match those
        // semantics exactly so callers reading `new_removed_snaps` see
        // only the latest epoch's removals.
        base.new_removed_snaps = self.new_removed_snaps.clone();
        base.new_purged_snaps = self.new_purged_snaps.clone();

        if let Some(crush) = crush {
            tracing::debug!(
                target: "rados::osdclient::crush",
                epoch = self.epoch.as_u32(),
                bytes = self.crush.len(),
                "Applying new CRUSH map",
            );
            base.crush = Some(crush);
        }

        // Update timestamps
        if self.new_last_up_change.sec != 0 {
            base.last_up_change = self.new_last_up_change;
        }

        if self.new_last_in_change.sec != 0 {
            base.last_in_change = self.new_last_in_change;
        }

        // Blocklist deltas: remove old entries first, then merge new ones.
        for addr in &self.old_blocklist {
            base.blocklist.remove(addr);
        }
        for (addr, expire) in &self.new_blocklist {
            base.blocklist.insert(*addr, *expire);
        }
        for addr in &self.old_range_blocklist {
            base.range_blocklist.remove(addr);
        }
        for (addr, expire) in &self.new_range_blocklist {
            base.range_blocklist.insert(*addr, *expire);
        }

        // `-1.0` is the "unchanged" sentinel in the incremental encoding.
        if self.new_full_ratio >= 0.0 {
            base.full_ratio = self.new_full_ratio;
        }
        if self.new_nearfull_ratio >= 0.0 {
            base.nearfull_ratio = self.new_nearfull_ratio;
        }
        if self.new_backfillfull_ratio >= 0.0 {
            base.backfillfull_ratio = self.new_backfillfull_ratio;
        }

        Ok(())
    }
}

/// OSDMap - the main OSD cluster map structure
/// This is a simplified version that focuses on decoding the essential fields
#[derive(Debug, Serialize)]
pub struct OSDMap {
    // Meta-encoding wrapper (version 8, compat 7)
    // Client-usable data section
    pub fsid: UuidD,
    pub epoch: Epoch,
    pub created: UTime,
    pub modified: UTime,

    pub pools: BTreeMap<u64, PgPool>,
    pub pool_name: BTreeMap<u64, String>,
    pub pool_max: i32,

    pub flags: u32,
    pub max_osd: i32,
    pub osd_state: Vec<u32>,
    pub osd_weight: Vec<u32>,
    pub osd_addrs_client: Vec<EntityAddrvec>,

    pub pg_temp: BTreeMap<PgId, Vec<i32>>,
    pub primary_temp: BTreeMap<PgId, i32>,
    pub osd_primary_affinity: Vec<u32>,

    // CRUSH map (parsed from encoded bytes)
    #[serde(skip)]
    pub crush: Option<CrushMap>,
    pub erasure_code_profiles: BTreeMap<String, BTreeMap<String, String>>,

    // CRUSH calculation cache: (pool_id, pg_seed) -> Vec<i32> (OSD list)
    #[serde(skip)]
    crush_cache: std::sync::Mutex<lru::LruCache<(u64, u32), Vec<i32>>>,

    // Acting set cache: (pool_id, pg_seed) -> Vec<i32> (final OSD list after
    // CRUSH + upmap + pg_temp overrides).  Keyed identically to crush_cache but
    // the value includes all placement overrides, so callers on the I/O hot path
    // can skip the full CRUSH walk + override application on repeated lookups.
    #[serde(skip)]
    acting_cache: std::sync::Mutex<lru::LruCache<(u64, u32), PgPlacement>>,

    // Version 4+ fields
    pub pg_upmap: BTreeMap<PgId, Vec<i32>>,
    pub pg_upmap_items: BTreeMap<PgId, Vec<(i32, i32)>>,

    // Version 6+ fields
    pub crush_version: i32,

    // Version 7+ fields
    pub new_removed_snaps: BTreeMap<u64, SnapIntervalSet>,
    pub new_purged_snaps: BTreeMap<u64, SnapIntervalSet>,

    // Version 9+ fields
    pub last_up_change: UTime,
    pub last_in_change: UTime,

    // Version 10+ fields
    pub pg_upmap_primaries: BTreeMap<PgId, i32>,

    // OSD-only data section
    pub osd_addrs_hb_back: Vec<EntityAddrvec>,
    pub osd_info: Vec<OsdInfo>,
    pub blocklist: BTreeMap<EntityAddr, UTime>,
    pub osd_addrs_cluster: Vec<EntityAddrvec>,
    pub cluster_snapshot_epoch: Epoch,
    pub cluster_snapshot: String,
    pub osd_uuid: Vec<UuidD>,
    pub osd_xinfo: Vec<OsdXInfo>,
    pub osd_addrs_hb_front: Vec<EntityAddrvec>,

    // Version 2+ fields
    pub nearfull_ratio: f32,
    pub full_ratio: f32,
    pub backfillfull_ratio: f32,

    // Version 5+ fields
    pub require_min_compat_client: CephRelease,
    pub require_osd_release: CephRelease,

    // Version 6+ fields (osd-only section)
    pub removed_snaps_queue: Vec<(i64, SnapIntervalSet)>,

    // Version 8+ fields
    pub crush_node_flags: BTreeMap<i32, u32>,

    // Version 9+ fields (osd-only section)
    pub device_class_flags: BTreeMap<i32, u32>,

    // Version 10+ fields (stretch mode)
    pub stretch_mode_enabled: bool,
    pub stretch_bucket_count: u32,
    pub degraded_stretch_mode: u32,
    pub recovering_stretch_mode: u32,
    pub stretch_mode_bucket: i32,

    // Version 11+ fields
    pub range_blocklist: BTreeMap<EntityAddr, UTime>,

    // Version 12+ fields
    pub allow_crimson: bool,
}

/// OSD state flag: OSD exists in the cluster.
///
/// Reference: Ceph `include/ceph_fs.h` `CEPH_OSD_EXISTS`
const CEPH_OSD_EXISTS: u32 = 1 << 0;

/// OSD state flag: OSD is currently up (running and reachable).
///
/// Reference: Ceph `include/ceph_fs.h` `CEPH_OSD_UP`
const CEPH_OSD_UP: u32 = 1 << 1;

/// OSD state flag: OSD has been administratively stopped.
///
/// Reference: Ceph `include/ceph_fs.h` `CEPH_OSD_STOP`.
/// Cleared whenever a mon emits a `new_up_client` entry for the OSD.
const CEPH_OSD_STOP: u32 = 1 << 9;

/// Maximum (and default) value for `osd_primary_affinity`.
///
/// Reference: `CEPH_OSD_MAX_PRIMARY_AFFINITY` /
/// `CEPH_OSD_DEFAULT_PRIMARY_AFFINITY` in `include/rados.h`.
/// `0x10000` means "always primary if acting[0]".
const CEPH_OSD_DEFAULT_PRIMARY_AFFINITY: u32 = 0x10000;

const CEPH_FEATURE_CRUSH_TUNABLES: u64 = 1 << 18;
const CEPH_FEATURE_CRUSH_TUNABLES2: u64 = 1 << 25;
const CEPH_FEATURE_OSDHASHPSPOOL: u64 = 1 << 30;
const CEPH_FEATURE_OSD_CACHEPOOL: u64 = 1 << 35;
const CEPH_FEATURE_CRUSH_V2: u64 = 1 << 36;
const CEPH_FEATURE_CRUSH_TUNABLES3: u64 = 1 << 41;
const CEPH_FEATURE_CRUSH_V4: u64 = 1 << 48;
const CEPH_FEATURE_CRUSH_TUNABLES5: u64 = 1 << 58;
const CEPH_FEATUREMASK_CRUSH_CHOOSE_ARGS: u64 = (1 << 21) | (1 << 57);
const CEPH_FEATUREMASK_CRUSH_MSR: u64 = (1 << 38) | (1 << 57);
const CEPH_FEATUREMASK_OSDMAP_PG_UPMAP: u64 = (1 << 21) | (1 << 57);
const CEPH_FEATUREMASK_SERVER_REEF: u64 = (1 << 31) | (1 << 28) | (1 << 57);
const CEPH_FEATURE_SERVER_JEWEL: u64 = 1 << 57;
const CEPH_FEATUREMASK_SERVER_KRAKEN: u64 = (1 << 14) | (1 << 57);
const CEPH_FEATURE_MSG_ADDR2: u64 = 1 << 59;
const CEPH_FEATURE_CEPHX_V2: u64 = 1 << 61;
const CEPH_FEATUREMASK_STRETCH_MODE: u64 = (1 << 32) | (1 << 28) | (1 << 57);
const CEPH_FEATURES_CRUSH_QUINCY: u64 = CEPH_FEATURE_CRUSH_TUNABLES
    | CEPH_FEATURE_CRUSH_TUNABLES2
    | CEPH_FEATURE_CRUSH_TUNABLES3
    | CEPH_FEATURE_CRUSH_TUNABLES5
    | CEPH_FEATURE_CRUSH_V2
    | CEPH_FEATURE_CRUSH_V4
    | CEPH_FEATUREMASK_CRUSH_CHOOSE_ARGS;
const CEPH_FEATURES_CRUSH_TENTACLE: u64 = CEPH_FEATURE_CRUSH_TUNABLES
    | CEPH_FEATURE_CRUSH_TUNABLES2
    | CEPH_FEATURE_CRUSH_TUNABLES3
    | CEPH_FEATURE_CRUSH_TUNABLES5
    | CEPH_FEATURE_CRUSH_V2
    | CEPH_FEATURE_CRUSH_V4
    | CEPH_FEATUREMASK_CRUSH_MSR;

/// Ceph release whose [`OSDMap::get_features_for_release`] rules should apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OSDMapFeatureRelease {
    Quincy,
    Tentacle,
}

impl Default for OSDMap {
    fn default() -> Self {
        Self {
            fsid: UuidD::default(),
            epoch: Epoch::default(),
            created: UTime::default(),
            modified: UTime::default(),
            pools: BTreeMap::default(),
            pool_name: BTreeMap::default(),
            pool_max: 0,
            flags: 0,
            max_osd: 0,
            osd_state: Vec::default(),
            osd_weight: Vec::default(),
            osd_addrs_client: Vec::default(),
            pg_temp: BTreeMap::default(),
            primary_temp: BTreeMap::default(),
            osd_primary_affinity: Vec::default(),
            crush: None,
            erasure_code_profiles: BTreeMap::default(),
            crush_cache: std::sync::Mutex::new(lru::LruCache::new(CRUSH_CACHE_SIZE)),
            acting_cache: std::sync::Mutex::new(lru::LruCache::new(CRUSH_CACHE_SIZE)),
            pg_upmap: BTreeMap::default(),
            pg_upmap_items: BTreeMap::default(),
            crush_version: 0,
            new_removed_snaps: BTreeMap::default(),
            new_purged_snaps: BTreeMap::default(),
            last_up_change: UTime::default(),
            last_in_change: UTime::default(),
            pg_upmap_primaries: BTreeMap::default(),
            osd_addrs_hb_back: Vec::default(),
            osd_info: Vec::default(),
            blocklist: BTreeMap::default(),
            osd_addrs_cluster: Vec::default(),
            cluster_snapshot_epoch: Epoch::default(),
            cluster_snapshot: String::default(),
            osd_uuid: Vec::default(),
            osd_xinfo: Vec::default(),
            osd_addrs_hb_front: Vec::default(),
            nearfull_ratio: 0.0,
            full_ratio: 0.0,
            backfillfull_ratio: 0.0,
            require_min_compat_client: 0,
            require_osd_release: 0,
            removed_snaps_queue: Vec::default(),
            crush_node_flags: BTreeMap::default(),
            device_class_flags: BTreeMap::default(),
            stretch_mode_enabled: false,
            stretch_bucket_count: 0,
            degraded_stretch_mode: 0,
            recovering_stretch_mode: 0,
            stretch_mode_bucket: 0,
            range_blocklist: BTreeMap::default(),
            allow_crimson: false,
        }
    }
}

impl Clone for OSDMap {
    fn clone(&self) -> Self {
        Self {
            fsid: self.fsid,
            epoch: self.epoch,
            created: self.created,
            modified: self.modified,
            pools: self.pools.clone(),
            pool_name: self.pool_name.clone(),
            pool_max: self.pool_max,
            flags: self.flags,
            max_osd: self.max_osd,
            osd_state: self.osd_state.clone(),
            osd_weight: self.osd_weight.clone(),
            osd_addrs_client: self.osd_addrs_client.clone(),
            pg_temp: self.pg_temp.clone(),
            primary_temp: self.primary_temp.clone(),
            osd_primary_affinity: self.osd_primary_affinity.clone(),
            crush: self.crush.clone(),
            erasure_code_profiles: self.erasure_code_profiles.clone(),
            // Create new empty caches for the clone
            crush_cache: std::sync::Mutex::new(lru::LruCache::new(CRUSH_CACHE_SIZE)),
            acting_cache: std::sync::Mutex::new(lru::LruCache::new(CRUSH_CACHE_SIZE)),
            pg_upmap: self.pg_upmap.clone(),
            pg_upmap_items: self.pg_upmap_items.clone(),
            crush_version: self.crush_version,
            new_removed_snaps: self.new_removed_snaps.clone(),
            new_purged_snaps: self.new_purged_snaps.clone(),
            last_up_change: self.last_up_change,
            last_in_change: self.last_in_change,
            pg_upmap_primaries: self.pg_upmap_primaries.clone(),
            osd_addrs_hb_back: self.osd_addrs_hb_back.clone(),
            osd_info: self.osd_info.clone(),
            blocklist: self.blocklist.clone(),
            osd_addrs_cluster: self.osd_addrs_cluster.clone(),
            cluster_snapshot_epoch: self.cluster_snapshot_epoch,
            cluster_snapshot: self.cluster_snapshot.clone(),
            osd_uuid: self.osd_uuid.clone(),
            osd_xinfo: self.osd_xinfo.clone(),
            osd_addrs_hb_front: self.osd_addrs_hb_front.clone(),
            nearfull_ratio: self.nearfull_ratio,
            full_ratio: self.full_ratio,
            backfillfull_ratio: self.backfillfull_ratio,
            require_min_compat_client: self.require_min_compat_client,
            require_osd_release: self.require_osd_release,
            removed_snaps_queue: self.removed_snaps_queue.clone(),
            crush_node_flags: self.crush_node_flags.clone(),
            device_class_flags: self.device_class_flags.clone(),
            stretch_mode_enabled: self.stretch_mode_enabled,
            stretch_bucket_count: self.stretch_bucket_count,
            degraded_stretch_mode: self.degraded_stretch_mode,
            recovering_stretch_mode: self.recovering_stretch_mode,
            stretch_mode_bucket: self.stretch_mode_bucket,
            range_blocklist: self.range_blocklist.clone(),
            allow_crimson: self.allow_crimson,
        }
    }
}

type RawPlacementCache = lru::LruCache<(u64, u32), Vec<i32>>;
type PlacementCache = lru::LruCache<(u64, u32), PgPlacement>;

/// The independently observable results of PG placement.
///
/// `raw` is the CRUSH result before OSDMap overrides. `up` applies upmaps and
/// liveness, while `acting` additionally applies `pg_temp`. Primary IDs are
/// deliberately separate: `primary_temp` does not reorder shard positions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgPlacement {
    pub raw: Vec<i32>,
    pub up: Vec<i32>,
    pub acting: Vec<i32>,
    pub up_primary: i32,
    pub acting_primary: i32,
}

impl OSDMap {
    fn lock_crush_cache(&self) -> Result<std::sync::MutexGuard<'_, RawPlacementCache>, RadosError> {
        self.crush_cache
            .lock()
            .map_err(|_| RadosError::Protocol("crush_cache lock poisoned".into()))
    }

    fn lock_acting_cache(&self) -> Result<std::sync::MutexGuard<'_, PlacementCache>, RadosError> {
        self.acting_cache
            .lock()
            .map_err(|_| RadosError::Protocol("acting_cache lock poisoned".into()))
    }

    /// Map a PG to its raw, up, and acting OSD sets via CRUSH + OSDMap state.
    ///
    /// Results are cached per `(pool_id, pg_seed)` so repeated lookups for
    /// the same PG within the same OSDMap epoch skip the CRUSH tree walk.
    ///
    /// This is the method the I/O hot path should use.
    ///
    /// Cache tiering (`pg_pool_t::read_tier` / `write_tier`) is the one
    /// `Objecter::_calc_target` behaviour we deliberately do not mirror.
    /// The feature is deprecated upstream and we have no plan to
    /// resurrect support for it.
    pub fn pg_to_placement(&self, pg: &PgId) -> Result<PgPlacement, RadosError> {
        let pool = match self.pools.get(&pg.pool) {
            Some(pool) => pool,
            _ => {
                return Ok(PgPlacement {
                    raw: Vec::new(),
                    up: Vec::new(),
                    acting: Vec::new(),
                    up_primary: -1,
                    acting_primary: -1,
                });
            }
        };

        let cache_key = (pg.pool, pg.seed);
        {
            let mut cache = self.lock_acting_cache()?;
            if let Some(cached) = cache.get(&cache_key) {
                return Ok(cached.clone());
            }
        }

        let raw = self.pg_to_osds(pg)?;
        let placement = self.placement_from_raw(pool, pg, raw)?;

        {
            let mut cache = self.lock_acting_cache()?;
            cache.put(cache_key, placement.clone());
        }
        Ok(placement)
    }

    fn placement_from_raw(
        &self,
        pool: &PgPool,
        pg: &PgId,
        raw: Vec<i32>,
    ) -> Result<PgPlacement, RadosError> {
        let mut up = raw.clone();
        // Ceph removes nonexistent CRUSH results before applying upmaps.
        if pool.is_replicated() {
            up.retain(|&osd| self.exists(osd));
        } else {
            for osd in &mut up {
                if !self.exists(*osd) {
                    *osd = CRUSH_ITEM_NONE;
                }
            }
        }
        self.apply_upmap(pg, &mut up);
        if pool.is_replicated() {
            up.retain(|&osd| osd >= 0 && self.is_up(osd));
        } else {
            for osd in &mut up {
                if *osd < 0 || self.is_down(*osd) {
                    *osd = CRUSH_ITEM_NONE;
                }
            }
        }

        let hashpspool = PoolFlags::from_bits_truncate(pool.flags).contains(PoolFlags::HASHPSPOOL);
        let pps = crate::crush::placement::pg_to_pps(*pg, pool.pgp_num, hashpspool);
        let up_primary = self
            .select_primary_index(pps, &up)
            .map_or(-1, |pos| up[pos]);
        if pool.is_replicated()
            && let Some(pos) = up.iter().position(|&osd| osd == up_primary)
        {
            up[..=pos].rotate_right(1);
        }

        let mut acting = self.pg_temp.get(pg).cloned().unwrap_or_default();
        let mut temp_primary = None;
        let mut has_nonempty_temp = false;
        if self.pg_temp.contains_key(pg) {
            if pool.is_replicated() {
                acting.retain(|&osd| osd >= 0 && self.is_up(osd));
            } else {
                for osd in &mut acting {
                    if *osd < 0 || self.is_down(*osd) {
                        *osd = CRUSH_ITEM_NONE;
                    }
                }
            }
            has_nonempty_temp = !acting.is_empty();
            temp_primary = acting.iter().copied().find(|&osd| osd != CRUSH_ITEM_NONE);
            if pool.is_erasure() {
                acting = pool.pgtemp_undo_primaryfirst_vec(has_nonempty_temp, &acting)?;
            }
        }
        if acting.is_empty() {
            acting = up.clone();
        }
        let acting_primary = self.primary_temp.get(pg).copied().unwrap_or_else(|| {
            if has_nonempty_temp {
                temp_primary.unwrap_or(-1)
            } else {
                up_primary
            }
        });
        Ok(PgPlacement {
            raw,
            up,
            acting,
            up_primary,
            acting_primary,
        })
    }

    /// Map a PG to its final acting OSD set via CRUSH + all overrides.
    pub fn pg_to_acting_osds(&self, pg: &PgId) -> Result<Vec<i32>, RadosError> {
        Ok(self.pg_to_placement(pg)?.acting)
    }

    /// Compute the `spg_t` shard for a PG.
    ///
    /// Returns `ShardId::NO_SHARD` (-1) for replicated pools — there is
    /// no per-shard concept and the wire encoding uses `-1`.
    ///
    /// For erasure-coded pools the shard is the position of the acting
    /// primary in the (post-`pgtemp_primaryfirst`) acting set,
    /// mirroring the EC branch of C++ `Objecter::_calc_target`.
    /// Optimized EC pools (`FLAG_EC_OPTIMIZATIONS`) additionally apply
    /// `pgtemp_undo_primaryfirst` to convert from primaryfirst position
    /// back to actual shard ID; the math is byte-for-byte identical to
    /// C++ so the wire shard matches what the OSD expects.
    ///
    /// The OSD verifies the shard against its own per-PG `shard_id`
    /// and rejects mismatches as misdirected ops, so getting this
    /// right is a hard correctness requirement for EC pool I/O.
    pub fn pg_to_spg_shard(&self, pg: &PgId) -> Result<ShardId, RadosError> {
        let pool = self
            .pools
            .get(&pg.pool)
            .ok_or_else(|| RadosError::Protocol(format!("Pool {} not found", pg.pool)))?;

        if !pool.is_erasure() {
            return Ok(ShardId::NO_SHARD);
        }

        self.ec_shard_from_placement(pool, pg, &self.pg_to_placement(pg)?)
    }

    /// Compute the EC `spg_t` shard from a hand-supplied raw CRUSH
    /// output.  Caller must already know the pool exists and is
    /// erasure-coded.  Mirrors the post-CRUSH portion of the EC branch
    /// of C++ `Objecter::_calc_target` — applies upmap, builds the EC
    /// up set, picks the acting primary, and returns the spg_t shard.
    ///
    /// For non-optimized EC and for optimized EC without a `pg_temp`
    /// override, the shard is just the primary's position in the
    /// acting set.  For optimized EC + `pg_temp`, the C++ code re-runs
    /// `pgtemp_primaryfirst` on `acting` to get the primaryfirst-view
    /// vector, walks IT to find the primary, then runs
    /// `pgtemp_undo_primaryfirst` on the position.  We mirror the
    /// same call sequence so we land on the same shard the OSD picks.
    #[cfg(test)]
    fn ec_shard_from_raw(&self, pg: &PgId, raw: Vec<i32>) -> Result<ShardId, RadosError> {
        let pool = self
            .pools
            .get(&pg.pool)
            .ok_or_else(|| RadosError::Protocol(format!("Pool {} not found", pg.pool)))?;
        let placement = self.placement_from_raw(pool, pg, raw)?;
        self.ec_shard_from_placement(pool, pg, &placement)
    }

    fn ec_shard_from_placement(
        &self,
        pool: &PgPool,
        pg: &PgId,
        placement: &PgPlacement,
    ) -> Result<ShardId, RadosError> {
        let acting = &placement.acting;
        let acting_primary = placement.acting_primary;

        if acting_primary < 0 {
            return Err(RadosError::Protocol(format!(
                "EC pool {} pg {}.{:x}: no acting primary",
                pg.pool, pg.pool, pg.seed
            )));
        }

        // Match C++ Objecter::_calc_target: re-apply pgtemp_primaryfirst
        // on acting if pg_temp is set.  For non-optimized EC this is a
        // no-op (the helper short-circuits), for optimized EC with the
        // typical `nonprimary_shards = {4,5}` layout it's idempotent on
        // monitor-stored pg_temp, and for asymmetric layouts it produces
        // the primaryfirst view we need to walk.
        let has_pg_temp = self.pg_temp.get(pg).is_some_and(|temp| !temp.is_empty());
        let primaryfirst_view: Vec<i32> = if has_pg_temp {
            pool.pgtemp_primaryfirst_vec(acting)
        } else {
            acting.to_vec()
        };

        let pos = primaryfirst_view
            .iter()
            .position(|&o| o == acting_primary)
            .ok_or_else(|| {
                RadosError::Protocol(format!(
                    "EC pool {} pg {}.{:x}: acting primary {} not in primaryfirst view {:?}",
                    pg.pool, pg.pool, pg.seed, acting_primary, primaryfirst_view
                ))
            })?;

        if pos > i8::MAX as usize {
            return Err(RadosError::Protocol(format!(
                "EC pool {} pg {}.{:x}: shard position {} exceeds i8 range",
                pg.pool, pg.pool, pg.seed, pos
            )));
        }
        // Convert the primaryfirst position back to the actual shard
        // ID.  No-op for non-optimized or non-pg_temp; runs the
        // pgtemp_undo_primaryfirst arithmetic for optimized EC + pg_temp.
        let shard = pool.pgtemp_undo_primaryfirst_shard(has_pg_temp, ShardId::new(pos as i8));
        Ok(shard)
    }

    /// Walk a candidate acting set and pick the position that should
    /// become primary, honouring `osd_primary_affinity`.
    ///
    /// Returns `Some(position)` of the chosen primary, or `None` if no
    /// usable up OSD is in the set.  Mirrors the leaf logic of C++
    /// `OSDMap::_pick_primary` + `_apply_primary_affinity`:
    ///
    ///   - Skip `CRUSH_ITEM_NONE` and down OSDs.
    ///   - If every up OSD has the default affinity, return the first
    ///     non-skipped position (the cheap path — no hash, no walk past
    ///     the first up OSD).
    ///   - Otherwise walk the set and reject each up OSD with
    ///     probability `1 - affinity / MAX`, using the same rjenkins1
    ///     hash of `(pps, osd_id)` C++ uses.  Take the first non-rejected
    ///     OSD; if every up OSD gets rejected, fall back to the first
    ///     up OSD we saw.
    ///
    /// `pps` is the post-`raw_pg_to_pps` seed — the same value CRUSH
    /// took as input.  Drift between this seed and the OSD's view would
    /// produce a different primary on the client and OSD sides, so
    /// callers must use `placement::pg_to_pps`.
    fn select_primary_index(&self, pps: u32, osds: &[i32]) -> Option<usize> {
        let any_non_default = osds.iter().any(|&o| {
            o >= 0
                && self.is_up(o)
                && self
                    .osd_primary_affinity
                    .get(o as usize)
                    .copied()
                    .unwrap_or(CEPH_OSD_DEFAULT_PRIMARY_AFFINITY)
                    != CEPH_OSD_DEFAULT_PRIMARY_AFFINITY
        });

        let mut fallback: Option<usize> = None;
        for (i, &o) in osds.iter().enumerate() {
            if o == CRUSH_ITEM_NONE || o < 0 || self.is_down(o) {
                continue;
            }
            if !any_non_default {
                return Some(i);
            }
            let a = self
                .osd_primary_affinity
                .get(o as usize)
                .copied()
                .unwrap_or(CEPH_OSD_DEFAULT_PRIMARY_AFFINITY);
            if a < CEPH_OSD_DEFAULT_PRIMARY_AFFINITY
                && (crate::crush::hash::crush_hash32_2(pps, o as u32) >> 16) >= a
            {
                fallback.get_or_insert(i);
            } else {
                return Some(i);
            }
        }
        fallback
    }

    /// Apply `pg_upmap`, `pg_upmap_items`, and `pg_upmap_primary` to the
    /// raw CRUSH result. This is deliberately before down filtering, matching
    /// C++ `OSDMap::_apply_upmap`.
    fn apply_upmap(&self, pg: &PgId, osds: &mut Vec<i32>) {
        if let Some(upmap) = self.pg_upmap.get(pg) {
            if upmap.iter().any(|&osd| self.upmap_target_is_out(osd)) {
                return;
            }
            *osds = upmap.clone();
        }
        if let Some(items) = self.pg_upmap_items.get(pg) {
            for &(from, to) in items {
                if osds.contains(&to) || self.upmap_target_is_out(to) {
                    continue;
                }
                if let Some(pos) = osds.iter().position(|&osd| osd == from) {
                    osds[pos] = to;
                }
            }
        }
        if let Some(&primary) = self.pg_upmap_primaries.get(pg)
            && primary != CRUSH_ITEM_NONE
            && primary >= 0
            && primary < self.max_osd
            && self.osd_weight.get(primary as usize).copied().unwrap_or(0) != 0
            && let Some(pos) = osds.iter().skip(1).position(|&osd| osd == primary)
        {
            osds.swap(0, pos + 1);
        }
    }

    fn upmap_target_is_out(&self, osd: i32) -> bool {
        osd != CRUSH_ITEM_NONE
            && osd >= 0
            && osd < self.max_osd
            && self.osd_weight.get(osd as usize).copied() == Some(0)
    }

    pub fn new() -> Self {
        Self::default()
    }

    /// Return the feature and possible-feature masks for a Ceph entity type.
    ///
    /// The selected source release controls release-specific CRUSH, upmap, and
    /// server feature masks. This reads OSDMap state; it does not advertise a
    /// messenger session's features.
    pub fn get_features_for_release(
        &self,
        release: OSDMapFeatureRelease,
        entity_type: crate::EntityType,
    ) -> (u64, u64) {
        let mut features = 0;
        if let Some(crush) = &self.crush {
            if crush.choose_local_tries != 2
                || crush.choose_local_fallback_tries != 5
                || crush.choose_total_tries != 19
            {
                features |= CEPH_FEATURE_CRUSH_TUNABLES;
            }
            if crush.chooseleaf_descend_once != 0 {
                features |= CEPH_FEATURE_CRUSH_TUNABLES2;
            }
            if crush.chooseleaf_vary_r != 0 {
                features |= CEPH_FEATURE_CRUSH_TUNABLES3;
            }
            if crush.chooseleaf_stable != 0 {
                features |= CEPH_FEATURE_CRUSH_TUNABLES5;
            }
            if crush
                .buckets
                .iter()
                .flatten()
                .any(|bucket| bucket.alg == crate::crush::BucketAlgorithm::Straw2)
            {
                features |= CEPH_FEATURE_CRUSH_V4;
            }
            if !crush.choose_args.is_empty()
                && (crush.choose_args.len() != 1
                    || !crush.choose_args.contains_key(&-1)
                    || crush
                        .choose_args
                        .values()
                        .flatten()
                        .flatten()
                        .any(|arg| arg.weight_set.len() > 1 || !arg.ids.is_empty()))
            {
                features |= CEPH_FEATUREMASK_CRUSH_CHOOSE_ARGS;
            }
            if release == OSDMapFeatureRelease::Tentacle
                && (crush.msr_descents != 100
                    || crush.msr_collision_tries != 100
                    || crush.rules.iter().flatten().any(|rule| {
                        matches!(
                            rule.rule_type,
                            crate::crush::RuleType::MsrFirstN | crate::crush::RuleType::MsrIndep
                        )
                    }))
            {
                features |= CEPH_FEATUREMASK_CRUSH_MSR;
            }
            for pool in self.pools.values() {
                if let Ok(rule) = crush.get_rule(pool.crush_rule as u32) {
                    for step in &rule.steps {
                        match step.op {
                            crate::crush::RuleOp::ChooseIndep
                            | crate::crush::RuleOp::ChooseLeafIndep
                            | crate::crush::RuleOp::SetChooseTries
                            | crate::crush::RuleOp::SetChooseLeafTries => {
                                features |= CEPH_FEATURE_CRUSH_V2;
                            }
                            crate::crush::RuleOp::SetChooseLeafVaryR => {
                                features |= CEPH_FEATURE_CRUSH_TUNABLES3;
                            }
                            crate::crush::RuleOp::SetChooseLeafStable => {
                                features |= CEPH_FEATURE_CRUSH_TUNABLES5;
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        for pool in self.pools.values() {
            if PoolFlags::from_bits_truncate(pool.flags).contains(PoolFlags::HASHPSPOOL) {
                features |= CEPH_FEATURE_OSDHASHPSPOOL;
            }
            if !pool.tiers.is_empty() || pool.tier_of >= 0 {
                features |= CEPH_FEATURE_OSD_CACHEPOOL;
            }
        }
        if self
            .osd_primary_affinity
            .iter()
            .take(self.max_osd.max(0) as usize)
            .any(|&affinity| affinity != CEPH_OSD_DEFAULT_PRIMARY_AFFINITY)
        {
            features |= CEPH_FEATURE_CRUSH_TUNABLES3;
        }
        if !self.pg_upmap.is_empty() || !self.pg_upmap_items.is_empty() {
            features |= CEPH_FEATUREMASK_OSDMAP_PG_UPMAP;
        }
        if release == OSDMapFeatureRelease::Tentacle && !self.pg_upmap_primaries.is_empty() {
            features |= CEPH_FEATUREMASK_SERVER_REEF;
        }
        let mut mask = match release {
            OSDMapFeatureRelease::Quincy => CEPH_FEATURES_CRUSH_QUINCY,
            OSDMapFeatureRelease::Tentacle => {
                CEPH_FEATURES_CRUSH_TENTACLE | CEPH_FEATUREMASK_SERVER_REEF
            }
        } | CEPH_FEATUREMASK_OSDMAP_PG_UPMAP
            | CEPH_FEATURE_OSDHASHPSPOOL
            | CEPH_FEATURE_OSD_CACHEPOOL
            | CEPH_FEATURE_CRUSH_TUNABLES3;
        if entity_type == crate::EntityType::OSD {
            let jewel = self.require_osd_release >= 10;
            let kraken = self.require_osd_release >= 11;
            if jewel {
                features |= CEPH_FEATURE_SERVER_JEWEL;
            }
            if kraken {
                features |= CEPH_FEATUREMASK_SERVER_KRAKEN | CEPH_FEATURE_MSG_ADDR2;
            }
            mask |=
                CEPH_FEATURE_SERVER_JEWEL | CEPH_FEATUREMASK_SERVER_KRAKEN | CEPH_FEATURE_MSG_ADDR2;
            if self.stretch_mode_enabled {
                features |= CEPH_FEATUREMASK_STRETCH_MODE;
                mask |= CEPH_FEATUREMASK_STRETCH_MODE;
            }
        }
        if self.require_min_compat_client >= 14
            || (self.require_osd_release >= 14 && entity_type == crate::EntityType::OSD)
        {
            features |= CEPH_FEATURE_CEPHX_V2;
        }
        mask |= CEPH_FEATURE_CEPHX_V2;
        (features, mask)
    }

    /// Return the flags inherited by an OSD from all named CRUSH ancestors.
    pub fn get_osd_crush_node_flags(&self, osd: i32) -> u32 {
        let Some(crush) = &self.crush else {
            return 0;
        };
        crush
            .location_ordered(osd)
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|(_, name)| {
                crush
                    .names
                    .iter()
                    .find_map(|(&id, candidate)| (candidate == &name).then_some(id))
            })
            .filter_map(|id| self.crush_node_flags.get(&id))
            .fold(0, |flags, inherited| flags | inherited)
    }

    /// Check if an OSD is marked UP in the OSDMap.
    ///
    /// Returns `false` if the OSD ID is out of range or the OSD is not UP.
    ///
    /// Reference: Ceph `OSDMap::is_up(int osd)` in `src/osd/OSDMap.h`
    pub fn is_up(&self, osd_id: i32) -> bool {
        self.exists(osd_id) && self.osd_state[osd_id as usize] & CEPH_OSD_UP != 0
    }

    /// Check if an OSD is marked DOWN in the OSDMap.
    ///
    /// An OSD is down if it exists but is not marked UP, or if it does not
    /// exist in the map at all.
    ///
    /// Reference: Ceph `OSDMap::is_down(int osd)` in `src/osd/OSDMap.h`
    pub fn is_down(&self, osd_id: i32) -> bool {
        !self.is_up(osd_id)
    }

    /// True if an OSD is marked OUT (weight == 0).
    ///
    /// Out-of-range or negative IDs are treated as "out" so callers can use
    /// this as a "not a valid routing target" check without bounds-checking
    /// separately.  Mirrors the `osd_weight[osd] == 0` checks in C++
    /// `OSDMap::_apply_upmap` which reject upmap rules targeting OUT OSDs.
    pub fn is_out(&self, osd_id: i32) -> bool {
        if osd_id < 0 {
            return true;
        }
        let idx = osd_id as usize;
        if idx >= self.osd_weight.len() {
            return true;
        }
        self.osd_weight[idx] == 0
    }

    // OSDMap-level flag constants (from include/rados.h)
    const FLAG_FULL: u32 = 1 << 1; // cluster full (deprecated since Mimic)
    const FLAG_PAUSERD: u32 = 1 << 2; // pause all reads
    const FLAG_PAUSEWR: u32 = 1 << 3; // pause all writes

    /// True if read operations should be queued cluster-wide (`CEPH_OSDMAP_PAUSERD`).
    pub fn is_pauserd(&self) -> bool {
        self.flags & Self::FLAG_PAUSERD != 0
    }

    /// True if write operations should be queued cluster-wide (`CEPH_OSDMAP_PAUSEWR` or
    /// the legacy `CEPH_OSDMAP_FULL` flag).
    pub fn is_pausewr(&self) -> bool {
        self.flags & (Self::FLAG_PAUSEWR | Self::FLAG_FULL) != 0
    }

    /// Returns the parsed `PoolFlags` for the given pool, or empty if unknown.
    fn pool_flags(&self, pool_id: u64) -> PoolFlags {
        self.pools.get(&pool_id).map_or(PoolFlags::empty(), |p| {
            PoolFlags::from_bits_truncate(p.flags)
        })
    }

    /// True if the given pool is full (no writes accepted).
    ///
    /// Checks both the `FULL` and `FULL_QUOTA` pool flags, matching
    /// `Objecter::_osdmap_pool_full()` in C++.
    pub fn is_pool_full(&self, pool_id: u64) -> bool {
        self.pool_flags(pool_id)
            .intersects(PoolFlags::FULL | PoolFlags::FULL_QUOTA)
    }

    /// Returns true if the pool has the EIO flag set.
    ///
    /// Mirrors `pg_pool_t::FLAG_EIO` (`1<<16`) checked in
    /// `Objecter::_calc_target()` → `RECALC_OP_TARGET_POOL_EIO`.
    /// All client ops targeting this pool must fail immediately with -EIO.
    pub fn is_pool_eio(&self, pool_id: u64) -> bool {
        self.pool_flags(pool_id).contains(PoolFlags::EIO)
    }

    /// Return the pool's snap context: `(snap_seq, sorted-desc snap IDs)`.
    ///
    /// Returns `(0, vec![])` when the pool has no snaps (the common case for
    /// plain RBD/CephFS usage).  The list is already stored in descending
    /// order by the OSD, matching what `Objecter::_calc_target` sends.
    pub fn pool_snap_context(&self, pool_id: u64) -> (u64, Vec<u64>) {
        match self.pools.get(&pool_id) {
            Some(pool) if pool.snap_seq != 0 => {
                // BTreeMap keys are ascending; .rev() gives descending without a sort.
                let snaps: Vec<u64> = pool.snaps.keys().copied().rev().collect();
                (pool.snap_seq, snaps)
            }
            _ => (0, vec![]),
        }
    }

    /// Prune snap IDs removed by `new_removed_snaps` from `snaps`.
    ///
    /// Mirrors `Objecter::_prune_snapc()`: removes any snap ID that falls
    /// inside the removed-snaps interval set for the given pool.  Called
    /// after each OSDMap update while an op is waiting in the retry loop.
    pub fn prune_snap_context(&self, pool_id: u64, snaps: &mut Vec<u64>) {
        if snaps.is_empty() {
            return;
        }
        if let Some(interval_set) = self.new_removed_snaps.get(&pool_id) {
            snaps.retain(|&s| {
                !interval_set
                    .intervals
                    .iter()
                    .any(|iv| s >= iv.start && s < iv.start.saturating_add(iv.len))
            });
        }
    }

    /// Check if an entity address is in the cluster's blocklist.
    ///
    /// Mirrors `OSDMap::is_blocklisted()` in `src/osd/OSDMap.cc`:
    /// 1. Normalise type to ANY (Nautilus+ stores all entries as TYPE_ANY).
    /// 2. Check for an exact match (same ip:port:nonce).
    /// 3. Check for a whole-IP match (port=0, nonce=0 entry).
    ///
    /// Range blocklist (CIDR) is not yet implemented.
    pub fn is_blocklisted(&self, addr: &EntityAddr) -> bool {
        if self.blocklist.is_empty() {
            return false;
        }
        let normalised = addr.as_type_any();
        if self.blocklist.contains_key(&normalised) {
            return true;
        }
        let ip_only = addr.as_ip_only();
        self.blocklist.contains_key(&ip_only)
    }

    /// Check if an OSD exists in the cluster.
    ///
    /// Reference: Ceph `OSDMap::exists(int osd)` in `src/osd/OSDMap.h`
    pub fn exists(&self, osd_id: i32) -> bool {
        if osd_id < 0 || osd_id >= self.max_osd {
            return false;
        }
        let idx = osd_id as usize;
        if idx >= self.osd_state.len() {
            return false;
        }
        self.osd_state[idx] & CEPH_OSD_EXISTS != 0
    }

    /// Get the client-facing address for an OSD.
    ///
    /// Returns `None` if the OSD ID is out of range.
    pub fn get_osd_addr(&self, osd_id: i32) -> Option<&EntityAddrvec> {
        if osd_id < 0 {
            return None;
        }
        self.osd_addrs_client.get(osd_id as usize)
    }

    /// Get the CRUSH map, if available
    pub fn get_crush_map(&self) -> Option<&CrushMap> {
        self.crush.as_ref()
    }

    /// Get a pool by ID
    pub fn get_pool(&self, pool_id: u64) -> Option<&PgPool> {
        self.pools.get(&pool_id)
    }

    /// Get a pool name by ID
    pub fn get_pool_name(&self, pool_id: u64) -> Option<&String> {
        self.pool_name.get(&pool_id)
    }

    /// Look up a pool ID by name. Linear scan — fine for realistic pool counts.
    pub fn pool_id_by_name(&self, name: &str) -> Option<u64> {
        self.pool_name
            .iter()
            .find_map(|(&id, n)| (n == name).then_some(id))
    }

    /// Get the CRUSH rule ID for a pool
    pub fn get_pool_crush_rule(&self, pool_id: u64) -> Option<u8> {
        self.pools.get(&pool_id).map(|p| p.crush_rule)
    }

    /// Simplified object → PG mapping (no namespace, no locator key).
    ///
    /// **Not for production I/O.** Use `OSDClient::execute_op` which handles
    /// namespace hashing, hashpspool, upmap, and pg_temp overrides.
    pub fn object_to_pg(&self, pool_id: u64, object_name: &str) -> Result<PgId, RadosError> {
        // Get the pool
        let pool = self
            .get_pool(pool_id)
            .ok_or_else(|| RadosError::Protocol(format!("Pool {pool_id} not found")))?;

        // Create a simple object locator
        let locator = crate::crush::ObjectLocator::new(pool_id);

        // Calculate the PG using the CRUSH placement function
        let pg = crate::crush::object_to_pg(object_name, &locator, pool.pg_num)
            .map_err(|e| RadosError::Protocol(format!("PG calculation failed: {e}")))?;

        Ok(PgId {
            pool: pg.pool as u64,
            seed: pg.seed,
        })
    }

    /// Simplified object → OSD mapping (CRUSH only, no overrides).
    ///
    /// **Not for production I/O.** Skips upmap, pg_temp, and
    /// pg_upmap_primaries overrides. Use `OSDClient::execute_op` instead.
    pub fn object_to_osds(&self, pool_id: u64, object_name: &str) -> Result<Vec<i32>, RadosError> {
        // First, map object to PG
        let pg = self.object_to_pg(pool_id, object_name)?;

        // Then, map PG to OSDs using CRUSH
        self.pg_to_osds(&pg)
    }

    /// Raw CRUSH PG → OSD mapping (no upmap / pg_temp overrides).
    ///
    /// **Not for production I/O.** Prefer `OSDMap::pg_to_acting_osds`
    /// which applies all OSDMap overrides.
    pub fn pg_to_osds(&self, pg: &PgId) -> Result<Vec<i32>, RadosError> {
        let cache_key = (pg.pool, pg.seed);
        {
            let mut cache = self.lock_crush_cache()?;
            if let Some(cached_osds) = cache.get(&cache_key) {
                return Ok(cached_osds.clone());
            }
        }

        let pool = self
            .get_pool(pg.pool)
            .ok_or_else(|| RadosError::Protocol(format!("Pool {} not found", pg.pool)))?;

        let crush_map = self
            .get_crush_map()
            .ok_or_else(|| RadosError::Protocol("CRUSH map not available".to_string()))?;

        let hashpspool = PoolFlags::from_bits_truncate(pool.flags).contains(PoolFlags::HASHPSPOOL);
        let result = crate::crush::placement::pg_to_osds(
            crush_map,
            *pg,
            pool.pgp_num,
            pool.crush_rule as u32,
            &self.osd_weight,
            pool.size as usize,
            hashpspool,
        )
        .map_err(|e| RadosError::Protocol(format!("CRUSH mapping failed: {e}")))?;

        {
            let mut cache = self.lock_crush_cache()?;
            cache.put(cache_key, result.clone());
        }

        Ok(result)
    }
}

impl VersionedEncode for OSDMap {
    const FEATURE_DEPENDENT: bool = true;

    fn encoding_version(&self, _features: u64) -> u8 {
        8 // Meta-encoding version
    }

    fn compat_version(&self, _features: u64) -> u8 {
        7
    }

    fn encode_content<B: BufMut>(
        &self,
        _buf: &mut B,
        _features: u64,
        _version: u8,
    ) -> Result<(), RadosError> {
        // For now, we'll focus on decoding. Encoding can be implemented later.
        Err(RadosError::Protocol(
            "OSDMap encoding not yet implemented".to_string(),
        ))
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        crate::denc::check_min_version!(version, 7, "OSDMap", "Quincy v17+");
        let client = OSDMapClientSection::decode_versioned(buf, features)?;
        let osd = OSDMapOsdSection::decode_versioned(buf, features)?;

        Ok(OSDMap {
            fsid: client.fsid,
            epoch: client.epoch,
            created: client.created,
            modified: client.modified,
            pools: client.pools,
            pool_name: client.pool_name,
            pool_max: client.pool_max,
            flags: client.flags,
            max_osd: client.max_osd,
            osd_state: client.osd_state,
            osd_weight: client.osd_weight,
            osd_addrs_client: client.osd_addrs_client,
            pg_temp: client.pg_temp,
            primary_temp: client.primary_temp,
            osd_primary_affinity: client.osd_primary_affinity,
            crush: client.crush,
            erasure_code_profiles: client.erasure_code_profiles,
            crush_cache: std::sync::Mutex::new(lru::LruCache::new(CRUSH_CACHE_SIZE)),
            acting_cache: std::sync::Mutex::new(lru::LruCache::new(CRUSH_CACHE_SIZE)),
            pg_upmap: client.pg_upmap,
            pg_upmap_items: client.pg_upmap_items,
            crush_version: client.crush_version,
            new_removed_snaps: client.new_removed_snaps,
            new_purged_snaps: client.new_purged_snaps,
            last_up_change: client.last_up_change,
            last_in_change: client.last_in_change,
            pg_upmap_primaries: client.pg_upmap_primaries,
            osd_addrs_hb_back: osd.osd_addrs_hb_back,
            osd_info: osd.osd_info,
            blocklist: osd.blocklist,
            osd_addrs_cluster: osd.osd_addrs_cluster,
            cluster_snapshot_epoch: osd.cluster_snapshot_epoch,
            cluster_snapshot: osd.cluster_snapshot,
            osd_uuid: osd.osd_uuid,
            osd_xinfo: osd.osd_xinfo,
            osd_addrs_hb_front: osd.osd_addrs_hb_front,
            nearfull_ratio: osd.nearfull_ratio,
            full_ratio: osd.full_ratio,
            backfillfull_ratio: osd.backfillfull_ratio,
            require_min_compat_client: osd.require_min_compat_client,
            require_osd_release: osd.require_osd_release,
            removed_snaps_queue: osd.removed_snaps_queue,
            crush_node_flags: osd.crush_node_flags,
            device_class_flags: osd.device_class_flags,
            stretch_mode_enabled: osd.stretch_mode_enabled,
            stretch_bucket_count: osd.stretch_bucket_count,
            degraded_stretch_mode: osd.degraded_stretch_mode,
            recovering_stretch_mode: osd.recovering_stretch_mode,
            stretch_mode_bucket: osd.stretch_mode_bucket,
            range_blocklist: osd.range_blocklist,
            allow_crimson: osd.allow_crimson,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None // Complex type - size computed by encoding
    }
}

// ============= Encoding Metadata Registration =============
//
// Mark all types with their compile-time encoding properties
// This allows tools to detect versioning and feature dependency at compile time

// Level 1: Simple types (no versioning, no feature dependency)
mark_simple_encoding!(OsdInfo);
mark_simple_encoding!(SnapInterval);
mark_simple_encoding!(SnapIntervalSet);

// Level 1/2: Versioned but not feature-dependent
mark_versioned_encoding!(PoolSnapInfo);
mark_versioned_encoding!(PgMergeMeta);
mark_versioned_encoding!(ExplicitHashHitSetParams);
mark_versioned_encoding!(ExplicitObjectHitSetParams);
mark_versioned_encoding!(BloomHitSetParams);
mark_versioned_encoding!(HitSetParams);

// Level 1/2: Versioned but not feature-dependent under the Quincy+ encode contract
mark_versioned_encoding!(OsdXInfo);
// Level 2/3: Feature-dependent types (encoding changes based on features)
mark_feature_dependent_encoding!(PgPool);
mark_feature_dependent_encoding!(OSDMap);

#[cfg(test)]
#[path = "tests/osdmap_correctness.rs"]
mod correctness_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crush::{
        BucketAlgorithm, BucketData, CrushBucket, CrushMap, CrushRule, CrushRuleStep, RuleOp,
        RuleType,
    };
    use bytes::Bytes;
    use serde_json::json;
    use sha2::{Digest, Sha256};

    const CRUSH_TUNABLES: u64 = 1 << 18;
    const CRUSH_TUNABLES2: u64 = 1 << 25;
    const OSDHASHPSPOOL: u64 = 1 << 30;
    const CRUSH_V2: u64 = 1 << 36;
    const CRUSH_TUNABLES3: u64 = 1 << 41;
    const OSD_PRIMARY_AFFINITY: u64 = 1 << 41;
    const CRUSH_TUNABLES5: u64 = 1 << 58;
    const OSDMAP_PG_UPMAP: u64 = (1 << 21) | (1 << 57);
    const SERVER_REEF: u64 = (1 << 31) | (1 << 28) | (1 << 57);
    const STRETCH_MODE: u64 = 1 << 32;
    const CRUSH_CHOOSE_ARGS: u64 = 1 << 21;
    const CRUSH_FEATURE_MASK_QUINCY: u64 = CRUSH_TUNABLES
        | CRUSH_TUNABLES2
        | CRUSH_TUNABLES3
        | CRUSH_TUNABLES5
        | CRUSH_V2
        | (1 << 48)
        | ((1 << 21) | (1 << 57));
    const CRUSH_FEATURE_MASK_TENTACLE: u64 = CRUSH_TUNABLES
        | CRUSH_TUNABLES2
        | CRUSH_TUNABLES3
        | CRUSH_TUNABLES5
        | CRUSH_V2
        | (1 << 48)
        | ((1 << 38) | (1 << 57));

    fn store_bucket(map: &mut CrushMap, bucket: CrushBucket) {
        let index = (-1 - bucket.id) as usize;
        if map.buckets.len() <= index {
            map.buckets.resize(index + 1, None);
        }
        map.buckets[index] = Some(bucket);
    }

    fn straw2_bucket(id: i32, bucket_type: i32, items: Vec<i32>) -> CrushBucket {
        let size = items.len();
        let weights = vec![0x1_0000; items.len()];
        CrushBucket {
            id,
            bucket_type,
            alg: BucketAlgorithm::Straw2,
            hash: 0,
            weight: weights.iter().sum(),
            size: size as u32,
            items,
            data: BucketData::Straw2 {
                item_weights: weights,
            },
        }
    }

    fn six_osd_crush_map() -> CrushMap {
        let mut crush = CrushMap::new();
        crush.chooseleaf_stable = 1;
        crush.allowed_bucket_algs = (1 << 1) | (1 << 2) | (1 << 4) | (1 << 5);
        crush.type_names = [
            (0, "osd"),
            (1, "host"),
            (2, "chassis"),
            (3, "rack"),
            (4, "row"),
            (5, "pdu"),
            (6, "pod"),
            (7, "room"),
            (8, "datacenter"),
            (9, "zone"),
            (10, "region"),
            (11, "root"),
        ]
        .into_iter()
        .map(|(id, name)| (id, name.to_owned()))
        .collect();
        crush.names = [
            (-1, "default"),
            (-2, "localhost"),
            (-3, "localrack"),
            (0, "osd.0"),
            (1, "osd.1"),
            (2, "osd.2"),
            (3, "osd.3"),
            (4, "osd.4"),
            (5, "osd.5"),
        ]
        .into_iter()
        .map(|(id, name)| (id, name.to_owned()))
        .collect();
        store_bucket(&mut crush, straw2_bucket(-1, 11, vec![-3]));
        store_bucket(&mut crush, straw2_bucket(-2, 1, (0..6).collect()));
        store_bucket(&mut crush, straw2_bucket(-3, 3, vec![-2]));
        crush.max_buckets = 3;
        crush.max_devices = 6;
        crush
    }

    #[test]
    fn six_osd_source_fixture_ids_names_and_types() {
        let crush = six_osd_crush_map();
        let triples: Vec<_> = [-1, -2, -3]
            .into_iter()
            .map(|id| {
                (
                    id,
                    crush.names.get(&id).map(String::as_str),
                    crush.buckets[(-id - 1) as usize]
                        .as_ref()
                        .map(|bucket| bucket.bucket_type),
                )
            })
            .collect();
        assert_eq!(
            triples,
            [
                (-1, Some("default"), Some(11)),
                (-2, Some("localhost"), Some(1)),
                (-3, Some("localrack"), Some(3)),
            ]
        );
    }

    fn source_six_osd_map() -> OSDMap {
        let mut map = OSDMap::new();
        let mut crush = six_osd_crush_map();
        crush.rules = vec![
            Some(CrushRule {
                rule_id: 0,
                rule_type: RuleType::Replicated,
                steps: vec![
                    CrushRuleStep {
                        op: RuleOp::Take,
                        arg1: -1,
                        arg2: 0,
                    },
                    CrushRuleStep {
                        op: RuleOp::ChooseFirstN,
                        arg1: 0,
                        arg2: 0,
                    },
                    CrushRuleStep {
                        op: RuleOp::Emit,
                        arg1: 0,
                        arg2: 0,
                    },
                ],
            }),
            Some(CrushRule {
                rule_id: 1,
                rule_type: RuleType::Erasure,
                steps: vec![
                    CrushRuleStep {
                        op: RuleOp::SetChooseLeafTries,
                        arg1: 5,
                        arg2: 0,
                    },
                    CrushRuleStep {
                        op: RuleOp::SetChooseTries,
                        arg1: 100,
                        arg2: 0,
                    },
                    CrushRuleStep {
                        op: RuleOp::Take,
                        arg1: -1,
                        arg2: 0,
                    },
                    CrushRuleStep {
                        op: RuleOp::ChooseIndep,
                        arg1: 0,
                        arg2: 0,
                    },
                    CrushRuleStep {
                        op: RuleOp::Emit,
                        arg1: 0,
                        arg2: 0,
                    },
                ],
            }),
        ];
        map.crush = Some(crush);

        let mut osds = OSDMapIncremental::new(Epoch::new(1));
        osds.new_max_osd = 6;
        for osd in 0..6 {
            osds.new_state.insert(osd, CEPH_OSD_EXISTS | (1 << 3));
            osds.new_up_client.insert(osd, EntityAddrvec::default());
            osds.new_weight.insert(osd, 0x1_0000);
        }
        osds.apply_to(&mut map).unwrap();

        let mut pools = OSDMapIncremental::new(Epoch::new(2));
        pools.new_pool_max = 2;
        pools.new_pools.insert(
            1,
            PgPool {
                pool_type: PgPool::TYPE_ERASURE,
                size: 3,
                pg_num: 64,
                pgp_num: 64,
                crush_rule: 1,
                tier_of: -1,
                ..Default::default()
            },
        );
        pools.new_pool_names.insert(1, "ec".into());
        pools.new_pools.insert(
            2,
            PgPool {
                pool_type: PgPool::TYPE_REPLICATED,
                size: 3,
                pg_num: 64,
                pgp_num: 64,
                crush_rule: 0,
                flags: PoolFlags::HASHPSPOOL.bits(),
                tier_of: -1,
                ..Default::default()
            },
        );
        pools.new_pool_names.insert(2, "reppool".into());
        pools.apply_to(&mut map).unwrap();
        map
    }

    // Upstream: v17.2.7/src/test/osd/TestOSDMap.cc::OSDMapTest.MapPG,
    // MapFunctionsMatch, PrimaryIsFirst, PGTempRespected, PrimaryTempRespected
    // Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/osd/TestOSDMap.cc#L261
    // Upstream: v20.2.4/src/test/osd/TestOSDMap.cc::OSDMapTest.MapPG,
    // MapFunctionsMatch, PrimaryIsFirst, PGTempRespected, PrimaryTempRespected
    // Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/osd/TestOSDMap.cc#L502
    #[test]
    fn placement_ports_source_map_and_temp_primaries() {
        let mut map = source_six_osd_map();
        let pg = PgId::new(2, 0);
        let placement = map.pg_to_placement(&pg).unwrap();
        // Pinned C++ Jewel/STRAW2 setup maps raw PG 2.0 to this order.
        assert_eq!(placement.raw, vec![3, 5, 1]);
        assert_eq!(placement.up, vec![3, 5, 1]);
        assert_eq!(placement.acting, vec![3, 5, 1]);
        assert_eq!(placement.up_primary, 3);
        assert_eq!(placement.acting_primary, 3);
        assert_eq!(map.pg_to_osds(&pg).unwrap(), vec![3, 5, 1]);
        assert_eq!(map.pg_to_acting_osds(&pg).unwrap(), vec![3, 5, 1]);
        assert_eq!(map.pg_to_placement(&pg).unwrap().acting_primary, 3);
        assert_eq!(map.lock_acting_cache().unwrap().len(), 1);

        let mut pg_temp = OSDMapIncremental::new(Epoch::new(3));
        pg_temp.new_pg_temp.insert(pg, vec![1, 5, 3]);
        pg_temp.apply_to(&mut map).unwrap();
        let after_temp = map.pg_to_placement(&pg).unwrap();
        assert_eq!(after_temp.raw, vec![3, 5, 1]);
        assert_eq!(after_temp.up, vec![3, 5, 1]);
        assert_eq!(after_temp.acting, vec![1, 5, 3]);
        assert_eq!(after_temp.up_primary, 3);
        assert_eq!(after_temp.acting_primary, 1);
        assert_eq!(map.lock_acting_cache().unwrap().len(), 1);

        let mut primary_temp = OSDMapIncremental::new(Epoch::new(4));
        primary_temp.new_primary_temp.insert(pg, 5);
        primary_temp.apply_to(&mut map).unwrap();
        let after_primary = map.pg_to_placement(&pg).unwrap();
        assert_eq!(after_primary.raw, vec![3, 5, 1]);
        assert_eq!(after_primary.up, vec![3, 5, 1]);
        assert_eq!(after_primary.acting, vec![1, 5, 3]);
        assert_eq!(after_primary.up_primary, 3);
        assert_eq!(after_primary.acting_primary, 5);
    }

    #[test]
    fn optimized_ec_pg_temp_preserves_stored_primary_and_none_primary() {
        let mut map = source_six_osd_map();
        map.max_osd = 11;
        map.osd_state.resize(11, CEPH_OSD_EXISTS | CEPH_OSD_UP);
        map.osd_weight.resize(11, 0x1_0000);
        map.osd_primary_affinity.resize(11, 0x1_0000);
        let pool = map.pools.get_mut(&1).unwrap();
        pool.size = 6;
        pool.flags = PgPool::FLAG_EC_OPTIMIZATIONS;
        pool.nonprimary_shards.insert(ShardId::new(1));
        pool.nonprimary_shards.insert(ShardId::new(2));
        pool.nonprimary_shards.insert(ShardId::new(3));

        let pg = PgId::new(1, 0);
        let mut temp = OSDMapIncremental::new(Epoch::new(3));
        temp.new_pg_temp
            .insert(pg, vec![CRUSH_ITEM_NONE, 9, 10, 6, 7, 8]);
        temp.apply_to(&mut map).unwrap();

        let placement = map.pg_to_placement(&pg).unwrap();
        // Pinned build_simple + EC rule 1, x=pool 1 (no HASHPSPOOL).
        assert_eq!(placement.raw, vec![5, 0, 2, 4, 3, 1]);
        assert_eq!(placement.up, vec![5, 0, 2, 4, 3, 1]);
        assert_eq!(placement.up_primary, 5);
        assert_eq!(map.pg_to_osds(&pg).unwrap(), vec![5, 0, 2, 4, 3, 1]);
        assert_eq!(placement.acting, vec![CRUSH_ITEM_NONE, 6, 7, 8, 9, 10]);
        assert_eq!(placement.acting_primary, 9);
        assert_eq!(map.pg_to_spg_shard(&pg).unwrap(), ShardId::new(4));

        let mut all_none = OSDMapIncremental::new(Epoch::new(4));
        all_none.new_pg_temp.insert(pg, vec![CRUSH_ITEM_NONE; 6]);
        all_none.apply_to(&mut map).unwrap();
        let placement = map.pg_to_placement(&pg).unwrap();
        assert_eq!(placement.acting, vec![CRUSH_ITEM_NONE; 6]);
        assert_eq!(placement.acting_primary, -1);
    }

    fn source_affinity_counts(map: &OSDMap, pool: u64) -> (Vec<usize>, Vec<usize>, Vec<usize>) {
        let mut any = vec![0; 6];
        let mut first = vec![0; 6];
        let mut primary = vec![0; 6];
        for seed in 0..10_000 {
            let pg = PgId::new(pool, seed);
            let placement = map.pg_to_placement(&pg).unwrap();
            for &osd in &placement.acting {
                if (0..6).contains(&osd) {
                    any[osd as usize] += 1;
                }
            }
            if let Some(&osd) = placement.acting.first()
                && (0..6).contains(&osd)
            {
                first[osd as usize] += 1;
            }
            if (0..6).contains(&placement.acting_primary) {
                primary[placement.acting_primary as usize] += 1;
            }
        }
        (any, first, primary)
    }

    fn update_placement_digest(digest: &mut Sha256, placement: &PgPlacement) {
        for osds in [&placement.up, &placement.acting] {
            digest.update((osds.len() as u32).to_be_bytes());
            for osd in osds {
                digest.update(osd.to_be_bytes());
            }
        }
        digest.update(placement.up_primary.to_be_bytes());
        digest.update(placement.acting_primary.to_be_bytes());
    }

    fn placement_digests(map: &OSDMap, pool: u64) -> (String, String) {
        let mut direct = Sha256::new();
        let mut cached = Sha256::new();
        for seed in 0..10_000 {
            let pg = PgId::new(pool, seed);
            let cache_key = (pg.pool, pg.seed);
            assert!(!map.lock_acting_cache().unwrap().contains(&cache_key));
            let miss = map.pg_to_placement(&pg).unwrap();
            let inserted = map
                .lock_acting_cache()
                .unwrap()
                .peek(&cache_key)
                .cloned()
                .expect("miss inserts placement cache entry");
            let hit = map.pg_to_placement(&pg).unwrap();
            assert_eq!(hit, inserted, "cache hit seed {seed}");
            update_placement_digest(&mut direct, &miss);
            update_placement_digest(&mut cached, &hit);
        }
        (
            hex::encode(direct.finalize()),
            hex::encode(cached.finalize()),
        )
    }

    fn pinned_placement_digest(pool: u64, state: &str) -> String {
        let values: Vec<_> =
            include_str!("../../tests/crush/reference/osdmap-primary-affinity-vectors.txt")
                .lines()
                .filter(|line| !line.starts_with('#'))
                .filter_map(|line| {
                    let fields: Vec<_> = line.split_whitespace().collect();
                    (fields.len() == 5 && fields[1] == pool.to_string() && fields[2] == state)
                        .then(|| fields[4])
                })
                .collect();
        assert_eq!(
            values.len(),
            2,
            "pinned releases for pool {pool}, state {state}"
        );
        assert_eq!(values[0], values[1], "pinned releases diverge");
        values[0].to_owned()
    }

    fn assert_placement_digests(map: &OSDMap, pool: u64, state: &str) {
        let expected = pinned_placement_digest(pool, state);
        let (miss, hit) = placement_digests(map, pool);
        assert_eq!(
            miss, expected,
            "direct cache miss, pool {pool}, state {state}"
        );
        assert_eq!(
            hit, expected,
            "immediate cache hit, pool {pool}, state {state}"
        );
    }

    #[test]
    fn primary_affinity_matches_pinned_placement_digests() {
        let mut map = source_six_osd_map();
        for pool in [1, 2] {
            assert_placement_digests(&map, pool, "default");

            let mut zero = OSDMapIncremental::new(Epoch::new(map.epoch.as_u32() + 1));
            zero.new_primary_affinity.insert(0, 0);
            zero.new_primary_affinity.insert(1, 0);
            zero.apply_to(&mut map).unwrap();
            assert_placement_digests(&map, pool, "zero");

            let mut half = OSDMapIncremental::new(Epoch::new(map.epoch.as_u32() + 1));
            half.new_primary_affinity.insert(0, 0x8000);
            half.new_primary_affinity.insert(1, 0);
            half.apply_to(&mut map).unwrap();
            assert_placement_digests(&map, pool, "half");

            let mut restore = OSDMapIncremental::new(Epoch::new(map.epoch.as_u32() + 1));
            restore
                .new_primary_affinity
                .insert(0, CEPH_OSD_DEFAULT_PRIMARY_AFFINITY);
            restore
                .new_primary_affinity
                .insert(1, CEPH_OSD_DEFAULT_PRIMARY_AFFINITY);
            restore.apply_to(&mut map).unwrap();
        }
    }

    // Upstream: v17.2.7/src/test/osd/TestOSDMap.cc::OSDMapTest.PrimaryAffinity
    // Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/osd/TestOSDMap.cc#L462
    // Upstream: v20.2.4/src/test/osd/TestOSDMap.cc::OSDMapTest.PrimaryAffinity
    // Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/osd/TestOSDMap.cc#L703
    #[test]
    fn primary_affinity_ports_all_source_states_for_both_pools() {
        let mut map = source_six_osd_map();
        for pool in [1, 2] {
            let (any, first, primary) = source_affinity_counts(&map, pool);
            for osd in 0..6 {
                assert!(any[osd] > 0, "pool {pool}, osd {osd}");
                assert!(first[osd] > 0, "pool {pool}, osd {osd}");
                assert!(primary[osd] > 0, "pool {pool}, osd {osd}");
            }

            let mut zero = OSDMapIncremental::new(Epoch::new(map.epoch.as_u32() + 1));
            zero.new_primary_affinity.insert(0, 0);
            zero.new_primary_affinity.insert(1, 0);
            zero.apply_to(&mut map).unwrap();
            let (any, first, primary) = source_affinity_counts(&map, pool);
            for osd in 0..6 {
                assert!(any[osd] > 0, "pool {pool}, osd {osd}");
                if osd >= 2 {
                    assert!(first[osd] > 0, "pool {pool}, osd {osd}");
                    assert!(primary[osd] > 0, "pool {pool}, osd {osd}");
                } else {
                    if pool == 2 {
                        assert_eq!(first[osd], 0, "pool {pool}, osd {osd}");
                    }
                    assert_eq!(primary[osd], 0, "pool {pool}, osd {osd}");
                }
            }

            let mut half = OSDMapIncremental::new(Epoch::new(map.epoch.as_u32() + 1));
            half.new_primary_affinity.insert(0, 0x8000);
            half.new_primary_affinity.insert(1, 0);
            half.apply_to(&mut map).unwrap();
            let (any, first, primary) = source_affinity_counts(&map, pool);
            let expected = (10_000 / (6 - 2)) / 2;
            for osd in 0..6 {
                assert!(any[osd] > 0, "pool {pool}, osd {osd}");
                if osd >= 2 {
                    assert!(first[osd] > 0, "pool {pool}, osd {osd}");
                    assert!(primary[osd] > 0, "pool {pool}, osd {osd}");
                } else if osd == 1 {
                    if pool == 2 {
                        assert_eq!(first[osd], 0, "pool {pool}, osd {osd}");
                    }
                    assert_eq!(primary[osd], 0, "pool {pool}, osd {osd}");
                } else {
                    assert!(primary[0] > expected * 2 / 3, "pool {pool}");
                    assert!(primary[0] < expected * 4 / 3, "pool {pool}");
                }
            }

            let mut restore = OSDMapIncremental::new(Epoch::new(map.epoch.as_u32() + 1));
            restore
                .new_primary_affinity
                .insert(0, CEPH_OSD_DEFAULT_PRIMARY_AFFINITY);
            restore
                .new_primary_affinity
                .insert(1, CEPH_OSD_DEFAULT_PRIMARY_AFFINITY);
            restore.apply_to(&mut map).unwrap();
        }
    }

    #[test]
    fn upmap_replays_source_valid_out_and_invalid_target_transitions() {
        let pg = PgId::new(2, 0);
        let mut map = source_six_osd_map();
        assert_eq!(map.pg_to_placement(&pg).unwrap().raw, vec![3, 5, 1]);

        let mut invalid = OSDMapIncremental::new(Epoch::new(3));
        invalid.new_pg_upmap.insert(pg, vec![99, 2, 4]);
        invalid.apply_to(&mut map).unwrap();
        assert_eq!(map.pg_upmap.get(&pg), Some(&vec![99, 2, 4]));
        let mut raw = vec![3, 5, 1];
        map.apply_upmap(&pg, &mut raw);
        assert_eq!(raw, vec![99, 2, 4]);
        let placement = map.pg_to_placement(&pg).unwrap();
        assert_eq!(placement.raw, vec![3, 5, 1]);
        assert_eq!(placement.up, vec![2, 4]);
        assert_eq!(placement.acting, vec![2, 4]);
        assert_eq!(placement.up_primary, 2);
        assert_eq!(placement.acting_primary, 2);

        let mut remove = OSDMapIncremental::new(Epoch::new(4));
        remove.old_pg_upmap.push(pg);
        remove.apply_to(&mut map).unwrap();
        assert_eq!(map.pg_to_placement(&pg).unwrap().raw, vec![3, 5, 1]);

        let mut out = OSDMapIncremental::new(Epoch::new(5));
        out.new_weight.insert(4, 0);
        out.new_pg_upmap.insert(pg, vec![0, 4, 2]);
        out.apply_to(&mut map).unwrap();
        let placement = map.pg_to_placement(&pg).unwrap();
        assert_eq!(placement.raw, vec![3, 5, 1]);
        assert_eq!(placement.up, vec![3, 5, 1]);
        assert_eq!(placement.acting, vec![3, 5, 1]);
        assert_eq!(placement.up_primary, 3);
        assert_eq!(placement.acting_primary, 3);
    }

    // Client application of the CleanTemps removal sentinels. The monitor
    // cleanup producer remains outside this crate.
    #[test]
    fn client_replays_temp_removal_sentinels_and_keeps_useful_temps() {
        let pg = PgId::new(2, 0);
        let mut map = source_six_osd_map();
        let mut add = OSDMapIncremental::new(Epoch::new(3));
        add.new_pg_temp.insert(pg, vec![3, 5, 1]);
        add.new_primary_temp.insert(pg, 3);
        add.apply_to(&mut map).unwrap();

        let placement = map.pg_to_placement(&pg).unwrap();
        assert_eq!(placement.raw, vec![3, 5, 1]);
        assert_eq!(placement.up, vec![3, 5, 1]);
        assert_eq!(placement.acting, vec![3, 5, 1]);
        assert_eq!(placement.up_primary, 3);
        assert_eq!(placement.acting_primary, 3);

        let mut remove = OSDMapIncremental::new(Epoch::new(4));
        remove.new_pg_temp.insert(pg, vec![]);
        remove.new_primary_temp.insert(pg, -1);
        remove.apply_to(&mut map).unwrap();

        assert!(!map.pg_temp.contains_key(&pg));
        assert!(!map.primary_temp.contains_key(&pg));
        assert_eq!(
            map.pg_to_placement(&pg).unwrap(),
            PgPlacement {
                raw: vec![3, 5, 1],
                up: vec![3, 5, 1],
                acting: vec![3, 5, 1],
                up_primary: 3,
                acting_primary: 3,
            }
        );

        let mut useful = OSDMapIncremental::new(Epoch::new(5));
        useful.new_pg_temp.insert(pg, vec![3, 0, 1]);
        useful.new_primary_temp.insert(pg, 0);
        useful.apply_to(&mut map).unwrap();
        let placement = map.pg_to_placement(&pg).unwrap();
        assert_eq!(placement.raw, vec![3, 5, 1]);
        assert_eq!(placement.up, vec![3, 5, 1]);
        assert_eq!(placement.acting, vec![3, 0, 1]);
        assert_eq!(placement.up_primary, 3);
        assert_eq!(placement.acting_primary, 0);
    }

    // Client-side removal sentinels consumed by CleanPGUpmaps and
    // CleanPGUpmapPrimaries; their monitor producers are not implemented.
    #[test]
    fn client_replays_full_item_and_primary_upmap_removals() {
        let pg = PgId::new(2, 0);
        let mut map = source_six_osd_map();
        let mut add = OSDMapIncremental::new(Epoch::new(3));
        add.new_pg_upmap.insert(pg, vec![0, 2, 4]);
        add.new_pg_upmap_items.insert(pg, vec![(2, 1)]);
        add.new_pg_upmap_primary.insert(pg, 4);
        add.apply_to(&mut map).unwrap();
        assert_eq!(
            map.pg_to_placement(&pg).unwrap(),
            PgPlacement {
                raw: vec![3, 5, 1],
                up: vec![4, 1, 0],
                acting: vec![4, 1, 0],
                up_primary: 4,
                acting_primary: 4,
            }
        );

        let mut remove = OSDMapIncremental::new(Epoch::new(4));
        remove.old_pg_upmap.push(pg);
        remove.old_pg_upmap_items.push(pg);
        remove.old_pg_upmap_primary.push(pg);
        remove.apply_to(&mut map).unwrap();
        assert_eq!(
            map.pg_to_placement(&pg).unwrap(),
            PgPlacement {
                raw: vec![3, 5, 1],
                up: vec![3, 5, 1],
                acting: vec![3, 5, 1],
                up_primary: 3,
                acting_primary: 3,
            }
        );
    }

    #[test]
    fn upmap_items_keep_source_collision_and_negative_target_semantics() {
        let pg = PgId::new(2, 0);
        let mut map = source_six_osd_map();
        let mut items = OSDMapIncremental::new(Epoch::new(3));
        items
            .new_pg_upmap_items
            .insert(pg, vec![(5, 3), (5, 99), (1, -2)]);
        items.apply_to(&mut map).unwrap();

        let mut raw = vec![3, 5, 1];
        map.apply_upmap(&pg, &mut raw);
        assert_eq!(raw, vec![3, 99, -2]);
        let placement = map.pg_to_placement(&pg).unwrap();
        assert_eq!(placement.raw, vec![3, 5, 1]);
        assert_eq!(placement.up, vec![3]);
        assert_eq!(placement.acting, vec![3]);
        assert_eq!(placement.up_primary, 3);
        assert_eq!(placement.acting_primary, 3);

        let mut remove = OSDMapIncremental::new(Epoch::new(4));
        remove.old_pg_upmap_items.push(pg);
        remove.apply_to(&mut map).unwrap();
        assert_eq!(map.pg_to_placement(&pg).unwrap().up, vec![3, 5, 1]);
    }

    // Upstream: v17.2.7/src/osd/OSDMap.cc::_apply_upmap
    // Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/osd/OSDMap.cc#L2432
    // Upstream: v20.2.4/src/osd/OSDMap.cc::_apply_upmap
    // Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/OSDMap.cc#L2803
    #[test]
    fn upmap_special_values_follow_source_validity_predicates() {
        let pg = PgId::new(2, 0);
        let mut map = source_six_osd_map();

        let mut full_none = OSDMapIncremental::new(Epoch::new(3));
        full_none
            .new_pg_upmap
            .insert(pg, vec![CRUSH_ITEM_NONE, 2, 4]);
        full_none.apply_to(&mut map).unwrap();
        let mut raw = vec![3, 5, 1];
        map.apply_upmap(&pg, &mut raw);
        assert_eq!(raw, vec![CRUSH_ITEM_NONE, 2, 4]);
        assert_eq!(
            map.pg_to_placement(&pg).unwrap(),
            PgPlacement {
                raw: vec![3, 5, 1],
                up: vec![2, 4],
                acting: vec![2, 4],
                up_primary: 2,
                acting_primary: 2,
            }
        );

        let mut item_none = OSDMapIncremental::new(Epoch::new(4));
        item_none.old_pg_upmap.push(pg);
        item_none
            .new_pg_upmap_items
            .insert(pg, vec![(5, CRUSH_ITEM_NONE)]);
        item_none.apply_to(&mut map).unwrap();
        let mut raw = vec![3, 5, 1];
        map.apply_upmap(&pg, &mut raw);
        assert_eq!(raw, vec![3, CRUSH_ITEM_NONE, 1]);
        assert_eq!(
            map.pg_to_placement(&pg).unwrap(),
            PgPlacement {
                raw: vec![3, 5, 1],
                up: vec![3, 1],
                acting: vec![3, 1],
                up_primary: 3,
                acting_primary: 3,
            }
        );

        let mut no_items = OSDMapIncremental::new(Epoch::new(5));
        no_items.old_pg_upmap_items.push(pg);
        no_items.apply_to(&mut map).unwrap();
        for (epoch, primary) in [(6, 99), (7, -2), (8, CRUSH_ITEM_NONE)] {
            let mut ignored = OSDMapIncremental::new(Epoch::new(epoch));
            ignored.new_pg_upmap_primary.insert(pg, primary);
            ignored.apply_to(&mut map).unwrap();
            assert_eq!(
                map.pg_to_placement(&pg).unwrap(),
                PgPlacement {
                    raw: vec![3, 5, 1],
                    up: vec![3, 5, 1],
                    acting: vec![3, 5, 1],
                    up_primary: 3,
                    acting_primary: 3,
                },
                "primary {primary}"
            );
        }

        let mut out = OSDMapIncremental::new(Epoch::new(9));
        out.new_weight.insert(5, 0);
        out.new_pg_upmap_primary.insert(pg, 5);
        out.apply_to(&mut map).unwrap();
        assert_eq!(
            map.pg_to_placement(&pg).unwrap(),
            PgPlacement {
                raw: vec![3, 1, 0],
                up: vec![3, 1, 0],
                acting: vec![3, 1, 0],
                up_primary: 3,
                acting_primary: 3,
            }
        );

        let mut valid = OSDMapIncremental::new(Epoch::new(10));
        valid.new_weight.insert(5, 0x1_0000);
        valid.new_pg_upmap_primary.insert(pg, 5);
        valid.apply_to(&mut map).unwrap();
        assert_eq!(
            map.pg_to_placement(&pg).unwrap(),
            PgPlacement {
                raw: vec![3, 5, 1],
                up: vec![5, 3, 1],
                acting: vec![5, 3, 1],
                up_primary: 5,
                acting_primary: 5,
            }
        );
    }

    // Upstream: v20.2.4/src/test/osd/TestOSDMap.cc::OSDMapTest.pgtemp_primaryfirst
    // Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/osd/TestOSDMap.cc#L3234
    #[test]
    fn pgtemp_primaryfirst_ports_every_mask_and_codec_direction() {
        let set = vec![0, 1, 2, 3, 4, 5];
        let pgid = PgId::new(1, 0);
        let mut map = OSDMap::new();
        let mut pool = PgPool {
            pool_type: PgPool::TYPE_ERASURE,
            size: 6,
            ..Default::default()
        };

        assert_eq!(pool.pgtemp_primaryfirst_vec(&set), set);
        assert_eq!(pool.pgtemp_undo_primaryfirst_vec(false, &set).unwrap(), set);

        pool.flags = PgPool::FLAG_EC_OPTIMIZATIONS;
        assert_eq!(pool.pgtemp_primaryfirst_vec(&set), set);
        assert_eq!(pool.pgtemp_undo_primaryfirst_vec(false, &set).unwrap(), set);

        pool.flags = 0;
        let mut inc = OSDMapIncremental::new(Epoch::new(1));
        inc.new_pg_temp.insert(pgid, set.clone());
        inc.apply_to(&mut map).unwrap();
        assert_eq!(pool.pgtemp_primaryfirst_vec(&set), set);
        assert_eq!(
            pool.pgtemp_undo_primaryfirst_vec(map.pg_temp.contains_key(&pgid), &set)
                .unwrap(),
            set
        );

        pool.flags = PgPool::FLAG_EC_OPTIMIZATIONS;
        assert_eq!(pool.pgtemp_primaryfirst_vec(&set), set);
        assert_eq!(
            pool.pgtemp_undo_primaryfirst_vec(map.pg_temp.contains_key(&pgid), &set)
                .unwrap(),
            set
        );

        for mask in (2..64).step_by(2) {
            pool.nonprimary_shards.clear();
            for shard in 0..6 {
                if mask & (1 << shard) != 0 {
                    pool.nonprimary_shards.insert(ShardId::new(shard));
                }
            }
            let encoded = pool.pgtemp_primaryfirst_vec(&set);
            let primary_count = pool.size as usize - pool.nonprimary_shards.iter().count();
            for (position, &osd) in encoded.iter().enumerate() {
                assert_eq!(
                    position >= primary_count,
                    pool.is_nonprimary_shard(ShardId::new(osd as i8)),
                    "mask {mask}, position {position}"
                );
            }
            for shard in 0..6 {
                let shard = ShardId::new(shard);
                let encoded_shard = pool.pgtemp_primaryfirst_shard(true, shard);
                assert_eq!(
                    pool.pgtemp_undo_primaryfirst_shard(true, encoded_shard),
                    shard,
                    "mask {mask}, shard {}",
                    shard.0
                );
                assert_eq!(
                    encoded_shard == shard,
                    shard.0 == 0 || mask == 62,
                    "mask {mask}, shard {}",
                    shard.0
                );
            }
            assert_eq!(
                pool.pgtemp_undo_primaryfirst_vec(map.pg_temp.contains_key(&pgid), &encoded)
                    .unwrap(),
                set,
                "mask {mask}"
            );
        }
    }

    // Upstream: v17.2.7/src/test/osd/TestOSDMap.cc::OSDMapTest.Features
    // Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/osd/TestOSDMap.cc#L222
    // Upstream: v20.2.4/src/test/osd/TestOSDMap.cc::OSDMapTest.Features
    // Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/osd/TestOSDMap.cc#L463
    #[test]
    fn get_features_derives_pool_and_rule_bits() {
        let mut map = make_osdmap_with_states(vec![CEPH_OSD_EXISTS | CEPH_OSD_UP; 6]);
        let mut crush = six_osd_crush_map();
        crush.choose_local_tries = 0;
        crush.choose_local_fallback_tries = 0;
        crush.choose_total_tries = 50;
        crush.chooseleaf_descend_once = 1;
        crush.chooseleaf_vary_r = 1;
        crush.rules = vec![
            Some(CrushRule {
                rule_id: 0,
                rule_type: RuleType::Replicated,
                steps: vec![],
            }),
            Some(CrushRule {
                rule_id: 1,
                rule_type: RuleType::Erasure,
                steps: vec![CrushRuleStep {
                    op: RuleOp::ChooseIndep,
                    arg1: 0,
                    arg2: 0,
                }],
            }),
        ];
        map.crush = Some(crush);
        map.pools.insert(
            1,
            PgPool {
                pool_type: PgPool::TYPE_ERASURE,
                crush_rule: 1,
                tier_of: -1,
                ..Default::default()
            },
        );
        map.pools.insert(
            2,
            PgPool {
                pool_type: PgPool::TYPE_REPLICATED,
                crush_rule: 0,
                flags: PoolFlags::HASHPSPOOL.bits(),
                tier_of: -1,
                ..Default::default()
            },
        );
        map.osd_primary_affinity = vec![CEPH_OSD_DEFAULT_PRIMARY_AFFINITY; 6];

        let expected = CRUSH_TUNABLES
            | CRUSH_TUNABLES2
            | CRUSH_TUNABLES3
            | CRUSH_TUNABLES5
            | CRUSH_V2
            | (1 << 48)
            | OSDHASHPSPOOL
            | OSD_PRIMARY_AFFINITY;
        for release in [OSDMapFeatureRelease::Quincy, OSDMapFeatureRelease::Tentacle] {
            let base_mask = match release {
                OSDMapFeatureRelease::Quincy => CRUSH_FEATURE_MASK_QUINCY,
                OSDMapFeatureRelease::Tentacle => CRUSH_FEATURE_MASK_TENTACLE | SERVER_REEF,
            } | OSDMAP_PG_UPMAP
                | OSDHASHPSPOOL
                | (1 << 35)
                | OSD_PRIMARY_AFFINITY
                | (1 << 61);
            assert_eq!(
                map.get_features_for_release(release, crate::EntityType::CLIENT),
                (expected, base_mask)
            );
            assert_eq!(
                map.get_features_for_release(release, crate::EntityType::OSD),
                (
                    expected,
                    base_mask | (1 << 57) | ((1 << 14) | (1 << 57)) | (1 << 59)
                )
            );
        }

        let mut inc = OSDMapIncremental::new(Epoch::new(1));
        inc.old_pools.push(1);
        inc.new_primary_affinity.insert(0, 0x8000);
        inc.apply_to(&mut map).unwrap();
        for release in [OSDMapFeatureRelease::Quincy, OSDMapFeatureRelease::Tentacle] {
            let base_mask = match release {
                OSDMapFeatureRelease::Quincy => CRUSH_FEATURE_MASK_QUINCY,
                OSDMapFeatureRelease::Tentacle => CRUSH_FEATURE_MASK_TENTACLE | SERVER_REEF,
            } | OSDMAP_PG_UPMAP
                | OSDHASHPSPOOL
                | (1 << 35)
                | OSD_PRIMARY_AFFINITY
                | (1 << 61);
            assert_eq!(
                map.get_features_for_release(release, crate::EntityType::MON),
                (expected & !CRUSH_V2, base_mask)
            );
        }
        map.stretch_mode_enabled = true;
        for release in [OSDMapFeatureRelease::Quincy, OSDMapFeatureRelease::Tentacle] {
            let (osd_features, osd_mask) =
                map.get_features_for_release(release, crate::EntityType::OSD);
            let (client_features, client_mask) =
                map.get_features_for_release(release, crate::EntityType::CLIENT);
            assert_eq!(osd_features & STRETCH_MODE, STRETCH_MODE);
            assert_eq!(osd_mask & STRETCH_MODE, STRETCH_MODE);
            assert_eq!(client_features & STRETCH_MODE, 0);
            assert_eq!(client_mask & STRETCH_MODE, 0);
        }
    }

    #[test]
    fn get_features_ignores_empty_default_choose_arg() {
        let mut map = OSDMap::new();
        let mut crush = six_osd_crush_map();
        // This is the decoder's wire shape for index -1, bucket 0, with zero
        // weight-set positions and zero IDs.
        crush.choose_args.insert(
            -1,
            vec![Some(crate::crush::CrushChooseArg::default()), None, None],
        );
        map.crush = Some(crush);

        for release in [OSDMapFeatureRelease::Quincy, OSDMapFeatureRelease::Tentacle] {
            assert_eq!(
                map.get_features_for_release(release, crate::EntityType::CLIENT)
                    .0
                    & CRUSH_CHOOSE_ARGS,
                0
            );
        }

        map.crush
            .as_mut()
            .unwrap()
            .choose_args
            .get_mut(&-1)
            .unwrap()[0] = Some(crate::crush::CrushChooseArg {
            weight_set: Vec::new(),
            ids: vec![-2],
        });
        for release in [OSDMapFeatureRelease::Quincy, OSDMapFeatureRelease::Tentacle] {
            assert_eq!(
                map.get_features_for_release(release, crate::EntityType::CLIENT)
                    .0
                    & CRUSH_CHOOSE_ARGS,
                CRUSH_CHOOSE_ARGS
            );
        }
    }

    // Upstream: v17.2.7/src/test/osd/TestOSDMap.cc::OSDMapTest.get_osd_crush_node_flags
    // Source: https://github.com/ceph/ceph/blob/b12291d110049b2f35e32e0de30d70e9a4c060d2/src/test/osd/TestOSDMap.cc#L537
    // Upstream: v20.2.4/src/test/osd/TestOSDMap.cc::OSDMapTest.get_osd_crush_node_flags
    // Source: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/test/osd/TestOSDMap.cc#L778
    #[test]
    fn get_osd_crush_node_flags_inherits_and_replaces_root_flags() {
        let mut map = make_osdmap_with_states(vec![CEPH_OSD_EXISTS | CEPH_OSD_UP; 6]);
        map.crush = Some(six_osd_crush_map());
        for osd in 0..6 {
            assert_eq!(map.get_osd_crush_node_flags(osd), 0);
        }

        let mut first = OSDMapIncremental::new(Epoch::new(1));
        first.new_crush_node_flags.insert(-1, 123);
        first.apply_to(&mut map).unwrap();
        for osd in 0..6 {
            assert_eq!(map.get_osd_crush_node_flags(osd), 123);
        }
        assert_eq!(map.get_osd_crush_node_flags(1000), 0);

        let mut second = OSDMapIncremental::new(Epoch::new(2));
        second.new_crush_node_flags.insert(-1, 456);
        second.apply_to(&mut map).unwrap();
        for osd in 0..6 {
            assert_eq!(map.get_osd_crush_node_flags(osd), 456);
        }
        assert_eq!(map.get_osd_crush_node_flags(1000), 0);

        let mut third = OSDMapIncremental::new(Epoch::new(3));
        third.new_crush_node_flags.insert(-1, 0);
        third.apply_to(&mut map).unwrap();
        for osd in 0..6 {
            assert_eq!(map.get_osd_crush_node_flags(osd), 0);
        }
    }

    fn make_osdmap_with_states(states: Vec<u32>) -> OSDMap {
        let max_osd = states.len() as i32;
        let mut map = OSDMap::new();
        map.max_osd = max_osd;
        map.osd_state = states;
        map
    }

    #[test]
    fn test_is_up_returns_true_for_up_osd() {
        // CEPH_OSD_EXISTS | CEPH_OSD_UP = 0x3
        let map = make_osdmap_with_states(vec![0x3, 0x1, 0x0]);
        assert!(map.is_up(0));
    }

    #[test]
    fn test_is_up_returns_false_for_down_osd() {
        // CEPH_OSD_EXISTS only (no UP bit) = 0x1
        let map = make_osdmap_with_states(vec![0x3, 0x1, 0x0]);
        assert!(!map.is_up(1));
    }

    #[test]
    fn test_is_up_returns_false_for_nonexistent_osd() {
        let map = make_osdmap_with_states(vec![0x3, 0x1, 0x0]);
        assert!(!map.is_up(2));
    }

    #[test]
    fn test_is_up_returns_false_for_out_of_range_osd() {
        let map = make_osdmap_with_states(vec![0x3]);
        assert!(!map.is_up(5));
    }

    #[test]
    fn test_is_up_returns_false_for_negative_osd_id() {
        let map = make_osdmap_with_states(vec![0x3]);
        assert!(!map.is_up(-1));
    }

    #[test]
    fn test_is_down_is_inverse_of_is_up() {
        let map = make_osdmap_with_states(vec![0x3, 0x1, 0x0]);
        assert!(!map.is_down(0)); // UP => not down
        assert!(map.is_down(1)); // EXISTS but not UP => down
        assert!(map.is_down(2)); // neither EXISTS nor UP => down
        assert!(map.is_down(99)); // out of range => down
    }

    #[test]
    fn test_exists_returns_true_for_existing_osd() {
        let map = make_osdmap_with_states(vec![0x3, 0x1, 0x0]);
        assert!(map.exists(0)); // EXISTS | UP
        assert!(map.exists(1)); // EXISTS only
    }

    #[test]
    fn test_exists_returns_false_for_removed_osd() {
        let map = make_osdmap_with_states(vec![0x3, 0x1, 0x0]);
        assert!(!map.exists(2)); // state == 0, no EXISTS bit
    }

    #[test]
    fn test_exists_returns_false_for_out_of_range() {
        let map = make_osdmap_with_states(vec![0x3]);
        assert!(!map.exists(5));
        assert!(!map.exists(-1));
    }

    #[test]
    fn test_get_osd_addr_returns_none_for_invalid_id() {
        let map = make_osdmap_with_states(vec![0x3]);
        assert!(map.get_osd_addr(-1).is_none());
        assert!(map.get_osd_addr(5).is_none());
    }

    #[test]
    fn test_get_osd_addr_returns_addrvec() {
        let mut map = make_osdmap_with_states(vec![0x3]);
        map.osd_addrs_client = vec![EntityAddrvec {
            addrs: vec![EntityAddr::default()],
        }];
        assert!(map.get_osd_addr(0).is_some());
    }

    #[test]
    fn test_osdmap_incremental_decode_versioned_is_inherent_decode_only_api() {
        let mut bytes = Bytes::new();
        let err = OSDMapIncremental::decode_versioned(&mut bytes, 0)
            .expect_err("empty input should fail through inherent decode-only API");

        assert!(
            matches!(err, RadosError::Codec(ref ce) if matches!(ce, crate::CodecError::InsufficientData { needed: 6, .. })),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_osdmap_incremental_decode_versioned_rejects_legacy_versions() {
        let mut bytes = Bytes::from_static(&[6, 6, 0, 0, 0, 0]);
        let err = OSDMapIncremental::decode_versioned(&mut bytes, 0)
            .expect_err("legacy incremental encoding should be rejected");

        assert!(
            matches!(err, RadosError::Codec(ref ce) if matches!(ce, crate::CodecError::VersionTooOld { type_name: "OSDMapIncremental", .. })),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_select_primary_index_default_affinity_picks_first_up() {
        // All up, all default affinity → first non-NONE wins.
        let map = make_osdmap_with_states(vec![0x3; 4]);
        let osds = vec![0, 1, 2, 3];
        assert_eq!(map.select_primary_index(0, &osds), Some(0));
    }

    #[test]
    fn test_select_primary_index_skips_down_with_default_affinity() {
        let mut states = vec![0x3; 4];
        states[0] = 0x1; // down
        let map = make_osdmap_with_states(states);
        let osds = vec![0, 1, 2, 3];
        assert_eq!(map.select_primary_index(0, &osds), Some(1));
    }

    #[test]
    fn test_select_primary_index_zero_affinity_is_rejected() {
        // Set OSD 0's affinity to 0 — should always be rejected.  All
        // other OSDs at default → first non-rejected (OSD 1) wins.
        let mut map = make_osdmap_with_states(vec![0x3; 4]);
        map.osd_primary_affinity = vec![
            0,
            CEPH_OSD_DEFAULT_PRIMARY_AFFINITY,
            CEPH_OSD_DEFAULT_PRIMARY_AFFINITY,
            CEPH_OSD_DEFAULT_PRIMARY_AFFINITY,
        ];
        let osds = vec![0, 1, 2, 3];
        assert_eq!(map.select_primary_index(0, &osds), Some(1));
    }

    #[test]
    fn test_select_primary_index_all_zero_affinity_falls_back_to_first() {
        // When every up OSD is rejected, the fallback path takes the
        // first up OSD in the input order.  This matches C++
        // `_apply_primary_affinity` which remembers `pos = i` on the
        // first rejection but keeps walking, returning that fallback
        // if nothing else passes.
        let mut map = make_osdmap_with_states(vec![0x3; 3]);
        map.osd_primary_affinity = vec![0, 0, 0];
        let osds = vec![0, 1, 2];
        assert_eq!(map.select_primary_index(0, &osds), Some(0));
    }

    #[test]
    fn test_select_primary_index_all_none_or_down_returns_none() {
        let map = make_osdmap_with_states(vec![0x1, 0x1, 0x1]);
        let osds = vec![0, 1, CRUSH_ITEM_NONE];
        assert_eq!(map.select_primary_index(0, &osds), None);
    }

    fn make_optimized_ec_pool(size: u8, nonprimary: &[i8]) -> PgPool {
        let mut pool = PgPool {
            pool_type: PgPool::TYPE_ERASURE,
            flags: PgPool::FLAG_EC_OPTIMIZATIONS,
            size,
            ..Default::default()
        };
        for &s in nonprimary {
            pool.nonprimary_shards.insert(ShardId::new(s));
        }
        pool
    }

    #[test]
    fn test_pgtemp_primaryfirst_undo_roundtrip_typical_4_2() {
        // Standard 4+2 EC: nonprimary = {4, 5}.  For this layout the
        // C++ math is the identity, so every shard maps to itself.
        let pool = make_optimized_ec_pool(6, &[4, 5]);
        for i in 0..6 {
            let s = ShardId::new(i);
            let pf = pool.pgtemp_primaryfirst_shard(true, s);
            let back = pool.pgtemp_undo_primaryfirst_shard(true, pf);
            assert_eq!(s, back, "roundtrip failed for shard {i}");
        }
    }

    #[test]
    fn test_pgtemp_primaryfirst_undo_roundtrip_asymmetric() {
        // Asymmetric: nonprimary = {2, 3}.  The C++ math is
        // genuinely non-identity here.  Verifies the inverse property
        // for every shard.
        let pool = make_optimized_ec_pool(6, &[2, 3]);
        for i in 0..6 {
            let s = ShardId::new(i);
            let pf = pool.pgtemp_primaryfirst_shard(true, s);
            let back = pool.pgtemp_undo_primaryfirst_shard(true, pf);
            assert_eq!(s, back, "roundtrip failed for shard {i}");
        }
    }

    #[test]
    fn test_pgtemp_primaryfirst_scalar_roundtrip_counts_decoded_shard_127() {
        let mut encoded = bytes::BytesMut::new();
        let mut nonprimary = ShardIdSet::default();
        nonprimary.insert(ShardId::new(2));
        nonprimary.insert(ShardId::new(3));
        nonprimary.insert(ShardId::new(127));
        nonprimary.encode(&mut encoded, 0).unwrap();
        let mut encoded = encoded.freeze();
        let nonprimary = ShardIdSet::decode(&mut encoded, 0).unwrap();
        assert_eq!(
            nonprimary.iter().collect::<Vec<_>>(),
            vec![ShardId::new(2), ShardId::new(3), ShardId::new(127)]
        );

        let pool = PgPool {
            pool_type: PgPool::TYPE_ERASURE,
            flags: PgPool::FLAG_EC_OPTIMIZATIONS,
            size: 6,
            nonprimary_shards: nonprimary,
            ..Default::default()
        };
        let shard = ShardId::new(1);
        assert_eq!(
            pool.pgtemp_undo_primaryfirst_shard(true, pool.pgtemp_primaryfirst_shard(true, shard)),
            shard
        );
    }

    #[test]
    fn test_pgtemp_undo_identity_when_not_optimized() {
        // Non-optimized EC: undo must be the identity even with
        // non-empty nonprimary_shards, because allows_ecoptimizations
        // returns false.
        let mut pool = PgPool {
            pool_type: PgPool::TYPE_ERASURE,
            size: 6,
            ..Default::default()
        };
        pool.nonprimary_shards.insert(ShardId::new(4));
        pool.nonprimary_shards.insert(ShardId::new(5));
        for i in 0..6 {
            let s = ShardId::new(i);
            assert_eq!(pool.pgtemp_undo_primaryfirst_shard(true, s), s);
        }
    }

    #[test]
    fn test_pgtemp_undo_identity_without_pg_temp() {
        // Optimized EC but no pg_temp on the PG: undo is identity
        // because the C++ function checks has_pgtemp internally.
        let pool = make_optimized_ec_pool(6, &[4, 5]);
        for i in 0..6 {
            let s = ShardId::new(i);
            assert_eq!(pool.pgtemp_undo_primaryfirst_shard(false, s), s);
        }
    }

    #[test]
    fn test_pgtemp_primaryfirst_vec_idempotent_typical_4_2() {
        // For nonprimary={4,5}, pgtemp_primaryfirst on a shard-id
        // vector is the identity, matching the assumption that the
        // OSDMonitor stores already-primaryfirst pg_temp.
        let pool = make_optimized_ec_pool(6, &[4, 5]);
        let input = vec![10, 11, 12, 13, 14, 15];
        let out = pool.pgtemp_primaryfirst_vec(&input);
        assert_eq!(out, input);
    }

    fn make_ec_pool_map() -> (OSDMap, PgPool) {
        let mut map = make_osdmap_with_states(vec![0x3; 20]);
        let pool = PgPool {
            pool_type: PgPool::TYPE_ERASURE,
            size: 6,
            ..Default::default()
        };
        map.pools.insert(7, pool.clone());
        (map, pool)
    }

    #[test]
    fn test_ec_shard_from_raw_simple_case() {
        // 4+2 EC, all OSDs up, no overrides.  Acting matches raw, primary
        // is acting[0] = osd 10, shard = 0.
        let (map, _) = make_ec_pool_map();
        let pg = PgId::new(7, 1);
        let raw = vec![10, 11, 12, 13, 14, 15];
        let shard = map.ec_shard_from_raw(&pg, raw).expect("ec_shard_from_raw");
        assert_eq!(shard, ShardId::new(0));
    }

    #[test]
    fn test_ec_shard_from_raw_skips_down_primary() {
        // Same 4+2, but OSD 10 is down. C++ EC `_raw_to_up_osds`
        // replaces it with NONE, primary becomes acting[1]=11, shard=1.
        let (mut map, _) = make_ec_pool_map();
        map.osd_state[10] = 0x1; // EXISTS but not UP
        let pg = PgId::new(7, 1);
        let raw = vec![10, 11, 12, 13, 14, 15];
        let shard = map.ec_shard_from_raw(&pg, raw).expect("ec_shard_from_raw");
        assert_eq!(shard, ShardId::new(1), "down primary → shard rolls forward");
    }

    #[test]
    fn test_ec_shard_from_raw_honours_primary_temp() {
        // primary_temp pins primary to OSD 12, which is acting[2]. shard=2.
        let (mut map, _) = make_ec_pool_map();
        let pg = PgId::new(7, 1);
        map.primary_temp.insert(pg, 12);
        let raw = vec![10, 11, 12, 13, 14, 15];
        let shard = map.ec_shard_from_raw(&pg, raw).expect("ec_shard_from_raw");
        assert_eq!(shard, ShardId::new(2));
    }

    #[test]
    fn test_ec_shard_from_raw_honours_pg_temp() {
        // pg_temp completely replaces the acting set. Primary is
        // pg_temp[0], shard reflects pg_temp positions.
        let (mut map, _) = make_ec_pool_map();
        let pg = PgId::new(7, 1);
        map.pg_temp.insert(pg, vec![15, 14, 13, 12, 11, 10]);
        let raw = vec![10, 11, 12, 13, 14, 15];
        let shard = map.ec_shard_from_raw(&pg, raw).expect("ec_shard_from_raw");
        assert_eq!(
            shard,
            ShardId::new(0),
            "pg_temp[0] is the primary, shard = 0"
        );
    }

    #[test]
    fn test_ec_shard_from_raw_optimized_typical_layout() {
        // Optimized EC with the typical nonprimary={4,5} layout.
        // pgtemp_undo_primaryfirst is the identity for this layout, so
        // shard = position of primary in acting.
        let mut map = make_osdmap_with_states(vec![0x3; 20]);
        let pool = make_optimized_ec_pool(6, &[4, 5]);
        map.pools.insert(7, pool);
        let pg = PgId::new(7, 1);
        // Insert a pg_temp so the optimized branch fires.
        map.pg_temp.insert(pg, vec![10, 11, 12, 13, 14, 15]);
        let raw = vec![10, 11, 12, 13, 14, 15];
        let shard = map.ec_shard_from_raw(&pg, raw).expect("ec_shard_from_raw");
        assert_eq!(shard, ShardId::new(0));
    }

    #[test]
    fn test_ec_shard_from_raw_optimized_no_pg_temp() {
        // Optimized EC without pg_temp on the PG: the C++ undo function
        // short-circuits because has_pgtemp is false, so shard = position.
        let mut map = make_osdmap_with_states(vec![0x3; 20]);
        let pool = make_optimized_ec_pool(6, &[4, 5]);
        map.pools.insert(7, pool);
        let pg = PgId::new(7, 1);
        let raw = vec![10, 11, 12, 13, 14, 15];
        let shard = map.ec_shard_from_raw(&pg, raw).expect("ec_shard_from_raw");
        assert_eq!(shard, ShardId::new(0));
    }

    #[test]
    fn test_pg_to_spg_shard_replicated_returns_no_shard() {
        // Replicated pool: must short-circuit to NO_SHARD without
        // touching CRUSH (which we have not set up in this test).
        let states: Vec<u32> = vec![0x3, 0x3, 0x3];
        let mut map = make_osdmap_with_states(states);
        let pool = PgPool {
            pool_type: PgPool::TYPE_REPLICATED,
            ..Default::default()
        };
        map.pools.insert(7, pool);
        let pg = PgId::new(7, 1);
        let shard = map.pg_to_spg_shard(&pg).expect("pg_to_spg_shard");
        assert_eq!(shard, ShardId::NO_SHARD);
    }

    #[test]
    fn test_ec_shard_from_raw_optimized_with_pg_temp_runs_undo() {
        // Asymmetric optimized EC layout (nonprimary={2,3}) with a
        // pg_temp.  Verifies that the undo math runs and produces a
        // consistent shard via the roundtrip property: whatever shard
        // we return must be the position the OSD picked when it
        // primaryfirst-stored the pg_temp.
        let mut map = make_osdmap_with_states(vec![0x3; 20]);
        let pool = make_optimized_ec_pool(6, &[2, 3]);
        map.pools.insert(7, pool);
        let pg = PgId::new(7, 1);
        map.pg_temp.insert(pg, vec![10, 11, 12, 13, 14, 15]);
        let raw = vec![10, 11, 12, 13, 14, 15];
        let shard = map.ec_shard_from_raw(&pg, raw).expect("ec_shard_from_raw");
        // The acting primary is pg_temp[0]=10, which is at position 0
        // of the primaryfirst view.  pgtemp_undo_primaryfirst(0) is
        // the early-return short circuit, so shard = 0.
        assert_eq!(shard, ShardId::new(0));
    }

    #[test]
    fn test_apply_to_replaces_new_removed_snaps_wholesale() {
        // First apply lays down a removal for pool 7 covering snap 100..105.
        // Second apply lays down a removal for pool 7 covering snap 200.
        // After the second apply, base.new_removed_snaps for pool 7 must
        // reflect ONLY the second epoch's deltas — matching C++ which
        // assigns wholesale rather than merging.
        let mut base = OSDMap::new();

        let mut inc1 = OSDMapIncremental::new(Epoch::new(1));
        let mut snaps1 = SnapIntervalSet::default();
        snaps1.intervals.push(SnapInterval { start: 100, len: 5 });
        inc1.new_removed_snaps.insert(7, snaps1);
        inc1.apply_to(&mut base).expect("apply 1");

        let mut inc2 = OSDMapIncremental::new(Epoch::new(2));
        let mut snaps2 = SnapIntervalSet::default();
        snaps2.intervals.push(SnapInterval { start: 200, len: 1 });
        inc2.new_removed_snaps.insert(7, snaps2);
        inc2.apply_to(&mut base).expect("apply 2");

        let pool_snaps = base.new_removed_snaps.get(&7).expect("snaps for pool 7");
        assert_eq!(
            pool_snaps.intervals.len(),
            1,
            "wholesale replace should leave only the second epoch's interval"
        );
        assert_eq!(pool_snaps.intervals[0].start, 200);
    }

    #[test]
    fn test_prune_snap_context_drops_removed_ids() {
        // Verify the pruning math: an op carrying snap context [99, 100,
        // 101, 200] against a pool that has removed [100..104] should
        // come back with [99, 200].
        let mut base = OSDMap::new();
        let mut inc = OSDMapIncremental::new(Epoch::new(1));
        let mut removed = SnapIntervalSet::default();
        removed.intervals.push(SnapInterval { start: 100, len: 4 });
        inc.new_removed_snaps.insert(7, removed);
        inc.apply_to(&mut base).expect("apply");

        let mut snaps = vec![99u64, 100, 101, 103, 200];
        base.prune_snap_context(7, &mut snaps);
        assert_eq!(snaps, vec![99, 200]);
    }

    #[test]
    fn test_apply_to_refuses_out_of_order_epoch() {
        let mut base = OSDMap::new();
        base.epoch = Epoch::new(5);
        let inc = OSDMapIncremental::new(Epoch::new(7)); // skip epoch 6
        let err = inc
            .apply_to(&mut base)
            .expect_err("out-of-order epoch should be refused");
        assert!(
            matches!(err, RadosError::Protocol(ref m) if m.contains("out-of-order")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_apply_to_refuses_wrong_fsid() {
        let mut base = OSDMap::new();
        base.epoch = Epoch::new(1);
        // base.fsid is default
        let mut inc = OSDMapIncremental::new(Epoch::new(2));
        // Set inc.fsid to a non-default value via UuidD::new
        inc.fsid = UuidD::from_bytes([0xAA; 16]);
        let err = inc
            .apply_to(&mut base)
            .expect_err("fsid mismatch should be refused");
        assert!(
            matches!(err, RadosError::Protocol(ref m) if m.contains("fsid")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_apply_to_adopts_fsid_on_first_apply() {
        let mut base = OSDMap::new();
        // base.epoch is 0 → first apply should adopt the inc's fsid.
        let mut inc = OSDMapIncremental::new(Epoch::new(1));
        inc.fsid = UuidD::from_bytes([0xBB; 16]);
        inc.apply_to(&mut base).expect("first apply should succeed");
        assert_eq!(base.fsid, UuidD::from_bytes([0xBB; 16]));
    }

    #[test]
    fn test_apply_to_refuses_fullmap_replacement() {
        let mut base = OSDMap::new();
        let mut inc = OSDMapIncremental::new(Epoch::new(1));
        inc.fullmap = Bytes::from_static(b"some non-empty payload");
        let err = inc
            .apply_to(&mut base)
            .expect_err("fullmap-bearing inc should be refused");
        assert!(
            matches!(err, RadosError::Protocol(ref m) if m.contains("full-map replacement")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_apply_to_new_state_zero_toggles_up_bit() {
        let mut base = make_osdmap_with_states(vec![0x3]); // EXISTS|UP
        let mut inc = OSDMapIncremental::new(Epoch::new(1));
        inc.new_state.insert(0, 0); // 0 should toggle UP per C++
        inc.apply_to(&mut base).expect("apply_to");
        assert!(!base.is_up(0), "OSD 0 UP bit should have been toggled off");
    }

    #[test]
    fn test_apply_to_new_up_client_sets_up_and_clears_stop() {
        let mut base = make_osdmap_with_states(vec![CEPH_OSD_STOP]);
        // A previously-stopped OSD that is now coming back up.
        let mut inc = OSDMapIncremental::new(Epoch::new(1));
        let addr = EntityAddrvec::default();
        inc.new_up_client.insert(0, addr);
        inc.apply_to(&mut base).expect("apply_to");
        assert!(base.is_up(0), "UP bit should have been set");
        assert_eq!(
            base.osd_state[0] & CEPH_OSD_STOP,
            0,
            "STOP bit should have been cleared"
        );
        assert_ne!(
            base.osd_state[0] & CEPH_OSD_EXISTS,
            0,
            "EXISTS bit should be set"
        );
    }

    #[test]
    fn test_apply_upmap_rejects_pg_upmap_with_out_target() {
        // CRUSH gives [0, 1, 2]. Admin upmap targets [3, 4, 5] but OSD 4
        // is OUT (weight 0). C++ rejects the entire operation, including
        // otherwise-valid item and primary overrides.
        let mut map = make_osdmap_with_states(vec![0x3, 0x3, 0x3, 0x3, 0x3, 0x3]);
        map.osd_weight = vec![0x10000, 0x10000, 0x10000, 0x10000, 0, 0x10000];
        let pg = PgId::new(1, 42);
        map.pg_upmap.insert(pg, vec![3, 4, 5]);
        map.pg_upmap_items.insert(pg, vec![(1, 3)]);
        map.pg_upmap_primaries.insert(pg, 2);
        let mut osds = vec![0, 1, 2];
        map.apply_upmap(&pg, &mut osds);
        assert_eq!(
            osds,
            vec![0, 1, 2],
            "invalid pg_upmap should stop all upmap processing"
        );
    }

    #[test]
    fn test_apply_upmap_items_skips_duplicate() {
        // CRUSH gives [1, 2, 3]. upmap_items wants to swap 2 → 3 but 3
        // is already in the set; would create a duplicate. Skip the swap.
        let map = make_osdmap_with_states(vec![0x3, 0x3, 0x3, 0x3]);
        let pg = PgId::new(1, 42);
        let mut map = map;
        map.osd_weight = vec![0x10000; 4];
        map.pg_upmap_items.insert(pg, vec![(2, 3)]);
        let mut osds = vec![1, 2, 3];
        map.apply_upmap(&pg, &mut osds);
        assert_eq!(
            osds,
            vec![1, 2, 3],
            "duplicate-creating swap should be skipped"
        );
    }

    #[test]
    fn test_apply_upmap_items_skips_out_target() {
        // CRUSH gives [1, 2, 3]. upmap_items wants 2 → 4 but 4 is OUT.
        // Skip the swap.
        let mut map = make_osdmap_with_states(vec![0x3, 0x3, 0x3, 0x3, 0x3]);
        map.osd_weight = vec![0x10000, 0x10000, 0x10000, 0x10000, 0];
        let pg = PgId::new(1, 42);
        map.pg_upmap_items.insert(pg, vec![(2, 4)]);
        let mut osds = vec![1, 2, 3];
        map.apply_upmap(&pg, &mut osds);
        assert_eq!(osds, vec![1, 2, 3], "OUT-target swap should be skipped");
    }

    #[test]
    fn test_apply_upmap_primaries_rejects_out_target() {
        // CRUSH gives [1, 2, 3]. Admin sets primary_upmap to OSD 2, but
        // OSD 2 is OUT. C++ skips the override entirely.
        let mut map = make_osdmap_with_states(vec![0x3, 0x3, 0x3, 0x3]);
        map.osd_weight = vec![0x10000, 0x10000, 0, 0x10000];
        let pg = PgId::new(1, 42);
        map.pg_upmap_primaries.insert(pg, 2);
        let mut osds = vec![1, 2, 3];
        map.apply_upmap(&pg, &mut osds);
        assert_eq!(
            osds[0], 1,
            "OUT primary override should not promote OSD 2 to primary"
        );
    }

    #[test]
    fn test_apply_to_applies_blocklist_deltas() {
        let mut base = OSDMap::new();
        let stale = EntityAddr::default();
        let fresh = EntityAddr {
            nonce: 42,
            ..EntityAddr::default()
        };
        base.blocklist.insert(stale, UTime::new(1, 0));

        let mut inc = OSDMapIncremental::new(Epoch::new(1));
        inc.old_blocklist.push(stale);
        inc.new_blocklist.insert(fresh, UTime::new(99, 0));

        inc.apply_to(&mut base).expect("apply_to");

        assert!(
            !base.blocklist.contains_key(&stale),
            "old entry not removed"
        );
        assert_eq!(
            base.blocklist.get(&fresh).copied(),
            Some(UTime::new(99, 0)),
            "new entry not inserted"
        );
    }

    #[test]
    fn test_osd_info_json_omits_struct_v() {
        let info = OsdInfo {
            struct_v: 1,
            last_clean_begin: Epoch::new(10),
            last_clean_end: Epoch::new(11),
            up_from: Epoch::new(12),
            up_thru: Epoch::new(13),
            down_at: Epoch::new(14),
            lost_at: Epoch::new(15),
        };

        let value = serde_json::to_value(info).expect("osd_info JSON should serialize");

        assert_eq!(
            value,
            json!({
                "last_clean_begin": 10,
                "last_clean_end": 11,
                "up_from": 12,
                "up_thru": 13,
                "down_at": 14,
                "lost_at": 15
            })
        );
    }
}
