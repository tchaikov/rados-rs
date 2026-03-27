//! OSD map types, decoding, and placement helpers for OSD-facing cluster state.
//!
//! This module models Ceph's `OSDMap` and related pool and OSD metadata types,
//! including the versioned decode logic needed to ingest monitor-provided maps.
//! It also provides placement helpers used by the client to map objects and PGs
//! onto OSD sets, plus serialization helpers that keep `dencoder` JSON output
//! aligned with ceph-dencoder where practical.

use crate::crush::CrushMap;
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
const CRUSH_CACHE_SIZE: std::num::NonZeroUsize = std::num::NonZeroUsize::new(1000).unwrap();

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
    current: i8,
}

impl<'a> Iterator for ShardIdSetIter<'a> {
    type Item = ShardId;

    fn next(&mut self) -> Option<Self::Item> {
        while self.current >= 0 && (self.current as usize) < 128 {
            let shard_id = ShardId(self.current);
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
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}{}{:02}{:02}",
            year, month, day, hour, min, sec, microseconds, tz_sign, tz_hours, tz_mins
        )
    }
}

// String encoding implementation to match Ceph's string handling
// Generic BTreeMap encoding - matches Ceph's map encoding

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
                "Unknown HitSet type: {}",
                hit_set_type
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
    const FEATURE_DEPENDENT: bool = false;

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
                "Invalid PoolType value: {}",
                value
            ))),
        }
    }
}

/// Simplified pg_pool_t implementation based on actual Ceph encoding
/// This follows the exact field order from osd_types.cc
#[derive(Debug, Clone, Default)]
pub struct PgPool {
    // Basic pool configuration (always present)
    pub pool_type: u8, // TYPE_REPLICATED=1, TYPE_ERASURE=3
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
        _version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
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

    pub fn is_stretch_pool(&self) -> bool {
        // For now, assume no stretch pools (like the default case in Ceph)
        // This would be based on crush rules and other pool configuration
        false
    }

    fn canonical_last_force_op_resend(&self) -> Epoch {
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

        if version >= 29 {
            pool.last_pg_merge_meta = PgMergeMeta::decode_versioned(buf, features)?;
        }

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
    const USES_VERSIONING: bool = true;
    const FEATURE_DEPENDENT: bool = <PgPool as VersionedEncode>::FEATURE_DEPENDENT;

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
#[derive(Debug, Clone, Default)]
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

impl VersionedEncode for PgMergeMeta {
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
        self.source_pgid.encode(buf, features)?;
        self.ready_epoch.encode(buf, features)?;
        self.last_epoch_started.encode(buf, features)?;
        self.last_epoch_clean.encode(buf, features)?;
        self.source_version.encode(buf, features)?;
        self.target_version.encode(buf, features)?;
        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        _version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        let source_pgid = PgId::decode(buf, features)?;
        let ready_epoch = Epoch::decode(buf, features)?;
        let last_epoch_started = Epoch::decode(buf, features)?;
        let last_epoch_clean = Epoch::decode(buf, features)?;
        let source_version = EVersion::decode(buf, features)?;
        let target_version = EVersion::decode(buf, features)?;
        Ok(PgMergeMeta {
            source_pgid,
            ready_epoch,
            last_epoch_started,
            last_epoch_clean,
            source_version,
            target_version,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None // Complex type - size computed by encoding
    }
}
crate::denc::impl_denc_for_versioned!(PgMergeMeta);

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

        let crush_bytes = Bytes::decode(buf, features)?;
        if !crush_bytes.is_empty() {
            let mut crush_buf = crush_bytes.clone();
            match CrushMap::decode(&mut crush_buf) {
                Ok(crush_map) => section.crush = Some(crush_map),
                Err(e) => {
                    tracing::warn!("Failed to parse CRUSH map: {:?}", e);
                    section.crush = None;
                }
            }
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

            // Calculate CRC using streaming approach (avoids large buffer allocation)
            let mut actual_crc = crc32c::crc32c(&header_bytes);

            // CRC the client and OSD sections (everything before inc_crc)
            if crc_offset > 0 {
                actual_crc = crc32c::crc32c_append(actual_crc, &outer_bytes[..crc_offset]);
            }

            // CRC the tail (full_crc field after inc_crc)
            if crc_tail_offset < outer_bytes.len() {
                actual_crc = crc32c::crc32c_append(actual_crc, &outer_bytes[crc_tail_offset..]);
            }

            // Invert the CRC to match Ceph's convention
            actual_crc = !actual_crc;

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
        // Invalidate CRUSH cache on any OSDMap update
        {
            let mut cache = base.lock_crush_cache()?;
            cache.clear();
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

        // Apply OSD state changes
        for (osd, addrvec) in &self.new_up_client {
            let idx = *osd as usize;
            if idx >= base.osd_addrs_client.len() {
                base.osd_addrs_client
                    .resize(idx + 1, EntityAddrvec::default());
            }
            base.osd_addrs_client[idx] = addrvec.clone();
        }

        for (osd, state) in &self.new_state {
            let idx = *osd as usize;
            if idx < base.osd_state.len() {
                base.osd_state[idx] ^= state; // XOR the state
            }
        }

        for (osd, weight) in &self.new_weight {
            let idx = *osd as usize;
            if idx < base.osd_weight.len() {
                base.osd_weight[idx] = *weight;
            }
        }

        // Apply pg_temp changes
        for (pgid, osds) in &self.new_pg_temp {
            if osds.is_empty() {
                base.pg_temp.remove(pgid);
            } else {
                base.pg_temp.insert(*pgid, osds.clone());
            }
        }

        // Apply primary_temp changes
        for (pgid, osd) in &self.new_primary_temp {
            if *osd == -1 {
                base.primary_temp.remove(pgid);
            } else {
                base.primary_temp.insert(*pgid, *osd);
            }
        }

        // Update timestamps
        if self.new_last_up_change.sec != 0 {
            base.last_up_change = self.new_last_up_change;
        }

        if self.new_last_in_change.sec != 0 {
            base.last_in_change = self.new_last_in_change;
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
            // Create a new empty cache for the clone
            crush_cache: std::sync::Mutex::new(lru::LruCache::new(CRUSH_CACHE_SIZE)),
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

type CrushCache = lru::LruCache<(u64, u32), Vec<i32>>;

impl OSDMap {
    fn lock_crush_cache(&self) -> Result<std::sync::MutexGuard<'_, CrushCache>, RadosError> {
        self.crush_cache
            .lock()
            .map_err(|_| RadosError::Protocol("crush_cache lock poisoned".into()))
    }

    pub fn new() -> Self {
        Self {
            crush_cache: std::sync::Mutex::new(lru::LruCache::new(CRUSH_CACHE_SIZE)),
            ..Default::default()
        }
    }

    /// Check if an OSD is marked UP in the OSDMap.
    ///
    /// Returns `false` if the OSD ID is out of range or the OSD is not UP.
    ///
    /// Reference: Ceph `OSDMap::is_up(int osd)` in `src/osd/OSDMap.h`
    pub fn is_up(&self, osd_id: i32) -> bool {
        if osd_id < 0 {
            return false;
        }
        let idx = osd_id as usize;
        if idx >= self.osd_state.len() {
            return false;
        }
        self.osd_state[idx] & CEPH_OSD_UP != 0
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

    /// True if the given pool is full (no writes accepted).
    ///
    /// Checks both the `FULL` and `FULL_QUOTA` pool flags, matching
    /// `Objecter::_osdmap_pool_full()` in C++.
    pub fn is_pool_full(&self, pool_id: u64) -> bool {
        use crate::osdclient::types::PoolFlags;
        self.pools.get(&pool_id).is_some_and(|p| {
            let flags = PoolFlags::from_bits_truncate(p.flags);
            flags.intersects(PoolFlags::FULL | PoolFlags::FULL_QUOTA)
        })
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
        if osd_id < 0 {
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

    /// Get the CRUSH rule ID for a pool
    pub fn get_pool_crush_rule(&self, pool_id: u64) -> Option<u8> {
        self.pools.get(&pool_id).map(|p| p.crush_rule)
    }

    /// Calculate object to PG mapping
    /// Maps an object name to its placement group within a pool
    pub fn object_to_pg(&self, pool_id: u64, object_name: &str) -> Result<PgId, RadosError> {
        // Get the pool
        let pool = self
            .get_pool(pool_id)
            .ok_or_else(|| RadosError::Protocol(format!("Pool {} not found", pool_id)))?;

        // Create a simple object locator
        let locator = crate::crush::ObjectLocator::new(pool_id);

        // Calculate the PG using the CRUSH placement function
        let pg = crate::crush::object_to_pg(object_name, &locator, pool.pg_num);

        Ok(PgId {
            pool: pg.pool as u64,
            seed: pg.seed,
        })
    }

    /// Calculate object to OSD mapping
    /// This is the complete object placement function that combines:
    /// 1. Object name -> PG mapping (hashing)
    /// 2. PG -> OSD set mapping (CRUSH)
    pub fn object_to_osds(&self, pool_id: u64, object_name: &str) -> Result<Vec<i32>, RadosError> {
        // First, map object to PG
        let pg = self.object_to_pg(pool_id, object_name)?;

        // Then, map PG to OSDs using CRUSH
        self.pg_to_osds(&pg)
    }

    /// Calculate PG to OSD mapping using CRUSH
    /// Returns the ordered list of OSDs that should store replicas for this PG
    pub fn pg_to_osds(&self, pg: &PgId) -> Result<Vec<i32>, RadosError> {
        // Check cache first
        let cache_key = (pg.pool, pg.seed);
        {
            let mut cache = self.lock_crush_cache()?;
            if let Some(cached_osds) = cache.get(&cache_key) {
                return Ok(cached_osds.clone());
            }
        }

        // Cache miss - calculate using CRUSH
        // Get the pool
        let pool = self
            .get_pool(pg.pool)
            .ok_or_else(|| RadosError::Protocol(format!("Pool {} not found", pg.pool)))?;

        // Get the CRUSH map
        let crush_map = self
            .get_crush_map()
            .ok_or_else(|| RadosError::Protocol("CRUSH map not available".to_string()))?;

        // Verify the CRUSH rule exists
        let _rule = crush_map
            .get_rule(pool.crush_rule as u32)
            .map_err(|e| RadosError::Protocol(format!("Failed to get CRUSH rule: {e}")))?;

        // Use CRUSH to calculate the OSD set
        let result_max = pool.size as usize;
        let mut result = Vec::with_capacity(result_max);

        // Use PG seed as the input to CRUSH
        let x = pg.seed;

        // Call CRUSH mapper
        crate::crush::mapper::crush_do_rule(
            crush_map,
            pool.crush_rule as u32,
            x,
            &mut result,
            result_max,
            &self.osd_weight,
        )
        .map_err(|e| RadosError::Protocol(format!("CRUSH mapping failed: {e}")))?;

        // Store in cache
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
mod tests {
    use super::*;
    use bytes::Bytes;
    use serde_json::json;

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
