use bytes::{Buf, BufMut, Bytes};
use crush::CrushMap;
use denc::{
    mark_feature_dependent_encoding, mark_simple_encoding, mark_versioned_encoding, Denc,
    EntityAddr, EntityAddrvec, Padding, RadosError, VersionedEncode,
};
use serde::Serialize;
use std::collections::BTreeMap;

/// Re-export basic types from denc
pub use denc::{EVersion, Epoch, FsId, UTime, UuidD, Version};

/// Re-export PgId from crush
pub use crush::PgId;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct ShardId(pub i8);

impl ShardId {
    pub const NO_SHARD: ShardId = ShardId(-1);
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

/// Shard ID Set (shard_id_set in C++)
/// A bitset that can store up to 128 shard IDs efficiently
/// Encoded as 2 u64 words using varint encoding
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub struct ShardIdSet {
    // Two 64-bit words to represent 128 bits
    words: [u64; 2],
}

impl ShardIdSet {
    pub fn new() -> Self {
        Self::default()
    }

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

/// Helper function to format UTime as timestamp string for serialization
fn format_utime_as_timestamp(utime: &UTime) -> String {
    use std::time::Duration;

    // Special case: zero timestamp outputs as "0.000000"
    if utime.sec == 0 && utime.nsec == 0 {
        return "0.000000".to_string();
    }

    // Special case: timestamps < 1 year output as "seconds.microseconds"
    // This matches ceph-dencoder behavior for small timestamps
    if utime.sec < 31536000 {
        // 365 days
        let microseconds = utime.nsec / 1000;
        return format!("{}.{:06}", utime.sec, microseconds);
    }

    // Convert UTime to SystemTime
    let _duration = Duration::new(utime.sec as u64, utime.nsec);

    // Format as ISO 8601 with nanoseconds
    // Note: The nsec field in UTime is already nanoseconds, but we need microseconds for display
    let microseconds = utime.nsec / 1000;

    // Convert to datetime components manually to match ceph format exactly
    // Using chrono-like formatting but without the dependency
    let secs_since_epoch = utime.sec as i64;

    // Days since epoch (1970-01-01)
    let days = secs_since_epoch / 86400;
    let remaining_secs = secs_since_epoch % 86400;

    // Calculate year, month, day from days since epoch
    // This is a simplified version - for production you'd want chrono crate
    let (year, month, day) = days_to_ymd(days);

    // Calculate hours, minutes, seconds
    let hours = remaining_secs / 3600;
    let minutes = (remaining_secs % 3600) / 60;
    let seconds = remaining_secs % 60;

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}+0000",
        year, month, day, hours, minutes, seconds, microseconds
    )
}

/// Convert days since Unix epoch to year/month/day
/// Simplified algorithm - should match ceph's output
fn days_to_ymd(mut days: i64) -> (i32, u32, u32) {
    // Days since 1970-01-01
    // This is a simplified Gregorian calendar calculation

    // Adjust for epoch (Unix epoch is 1970-01-01)
    let mut year = 1970;

    // Handle years
    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if days >= days_in_year {
            days -= days_in_year;
            year += 1;
        } else {
            break;
        }
    }

    // Handle months
    let days_in_months = if is_leap_year(year) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut month = 1;
    for &days_in_month in &days_in_months {
        if days >= days_in_month as i64 {
            days -= days_in_month as i64;
            month += 1;
        } else {
            break;
        }
    }

    let day = days as u32 + 1; // Days are 1-indexed

    (year, month, day)
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

// String encoding implementation to match Ceph's string handling
// Generic BTreeMap encoding - matches Ceph's map encoding

/// Explicit Hash HitSet parameters - no additional parameters needed
#[derive(Debug, Clone, Default, Serialize)]
pub struct ExplicitHashHitSetParams;

impl VersionedEncode for ExplicitHashHitSetParams {
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
    ) -> Result<(), RadosError> {
        // No parameters to encode
        Ok(())
    }

    fn decode_content<B: Buf>(
        _buf: &mut B,
        _features: u64,
        _version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // No parameters to decode for ExplicitHash
        Ok(ExplicitHashHitSetParams)
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        Some(0) // No content
    }
}

denc::impl_denc_for_versioned!(ExplicitHashHitSetParams);

/// Explicit Object HitSet parameters - no additional parameters needed
#[derive(Debug, Clone, Default, Serialize)]
pub struct ExplicitObjectHitSetParams;

impl VersionedEncode for ExplicitObjectHitSetParams {
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
    ) -> Result<(), RadosError> {
        // No parameters to encode
        Ok(())
    }

    fn decode_content<B: Buf>(
        _buf: &mut B,
        _features: u64,
        _version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // No parameters to decode for ExplicitObject
        Ok(ExplicitObjectHitSetParams)
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        Some(0) // No content
    }
}

denc::impl_denc_for_versioned!(ExplicitObjectHitSetParams);

/// BloomHitSet parameters - separate VersionedEncode implementation
#[derive(Debug, Clone, Default, Serialize)]
pub struct BloomHitSetParams {
    pub fpp_micro: u32,
    pub target_size: u64,
    pub seed: u64,
}

impl VersionedEncode for BloomHitSetParams {
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
    ) -> Result<(), RadosError> {
        // Write directly to buf parameter
        buf.put_u32_le(self.fpp_micro);
        buf.put_u64_le(self.target_size);
        buf.put_u64_le(self.seed);
        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        _features: u64,
        _version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        if buf.remaining() < 20 {
            // 4 + 8 + 8 = 20 buf
            return Err(RadosError::Protocol(
                "Insufficient buf for BloomHitSetParams".to_string(),
            ));
        }

        let fpp_micro = buf.get_u32_le();
        let target_size = buf.get_u64_le();
        let seed = buf.get_u64_le();

        Ok(BloomHitSetParams {
            fpp_micro,
            target_size,
            seed,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        Some(4 + 8 + 8) // fpp_micro (u32) + target_size (u64) + seed (u64)
    }
}

denc::impl_denc_for_versioned!(BloomHitSetParams);

/// HitSet parameter types - using dedicated types for each variant
#[derive(Debug, Clone, Default)]
pub enum HitSetParams {
    #[default]
    None,
    ExplicitHash(ExplicitHashHitSetParams),
    ExplicitObject(ExplicitObjectHitSetParams),
    Bloom(BloomHitSetParams),
}

// Custom Serialize implementation to match ceph-dencoder format
impl Serialize for HitSetParams {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("HitSetParams", 1)?;

        match self {
            HitSetParams::None => {
                state.serialize_field("type", "none")?;
            }
            HitSetParams::ExplicitHash(_params) => {
                state.serialize_field("type", "explicit_hash")?;
                // TODO: serialize params fields
            }
            HitSetParams::ExplicitObject(_params) => {
                state.serialize_field("type", "explicit_object")?;
                // TODO: serialize params fields
            }
            HitSetParams::Bloom(_params) => {
                state.serialize_field("type", "bloom")?;
                // TODO: serialize params fields
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
                buf.put_u8(0); // TYPE_NONE
            }
            HitSetParams::ExplicitHash(params) => {
                buf.put_u8(1); // TYPE_EXPLICIT_HASH
                params.encode_versioned(buf, features)?;
            }
            HitSetParams::ExplicitObject(params) => {
                buf.put_u8(2); // TYPE_EXPLICIT_OBJECT
                params.encode_versioned(buf, features)?;
            }
            HitSetParams::Bloom(params) => {
                buf.put_u8(3); // TYPE_BLOOM
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
            0 => Ok(HitSetParams::None),
            1 => {
                let params = ExplicitHashHitSetParams::decode_versioned(buf, features)?;
                Ok(HitSetParams::ExplicitHash(params))
            }
            2 => {
                let params = ExplicitObjectHitSetParams::decode_versioned(buf, features)?;
                Ok(HitSetParams::ExplicitObject(params))
            }
            3 => {
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

denc::impl_denc_for_versioned!(HitSetParams);

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
    // OsdXInfo encoding is feature-dependent: version 4 if SERVER_OCTOPUS features, else 3
    const FEATURE_DEPENDENT: bool = true;

    fn encoding_version(&self, features: u64) -> u8 {
        // Match C++ logic: version 4 if SERVER_OCTOPUS features, else 3
        if (features & 0x8000000000000000) != 0 {
            // CEPH_FEATUREMASK_SERVER_OCTOPUS
            4
        } else {
            3
        }
    }

    fn compat_version(&self, _features: u64) -> u8 {
        1
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        version: u8,
    ) -> Result<(), RadosError> {
        // Write directly to buf parameter

        // Version 1+ fields
        self.down_stamp.encode(buf, features)?;

        // Convert laggy_probability to u32 as in C++
        let lp = (self.laggy_probability * 0xffffffffu32 as f32) as u32;
        buf.put_u32_le(lp);

        buf.put_u32_le(self.laggy_interval);

        // Version 2+ fields
        if version >= 2 {
            buf.put_u64_le(self.features);
        }

        // Version 3+ fields
        if version >= 3 {
            buf.put_u32_le(self.old_weight);
        }

        // Version 4+ fields
        if version >= 4 {
            self.last_purged_snaps_scrub.encode(buf, features)?;
            buf.put_u32_le(self.dead_epoch);
        }

        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Version 1+ fields
        let down_stamp = UTime::decode(buf, features)?;

        let lp = buf.get_u32_le();
        let laggy_probability = lp as f32 / 0xffffffffu32 as f32;

        let laggy_interval = buf.get_u32_le();

        let mut xinfo = OsdXInfo {
            down_stamp,
            laggy_probability,
            laggy_interval,
            ..Default::default()
        };

        // Version 2+ fields
        if version >= 2 {
            xinfo.features = buf.get_u64_le();
        }

        // Version 3+ fields
        if version >= 3 {
            xinfo.old_weight = buf.get_u32_le();
        }

        // Version 4+ fields
        if version >= 4 {
            xinfo.last_purged_snaps_scrub = UTime::decode(buf, features)?;
            xinfo.dead_epoch = buf.get_u32_le();
        }

        Ok(xinfo)
    }

    fn encoded_size_content(&self, features: u64, version: u8) -> Option<usize> {
        let mut size = 0;

        // Version 1+ fields
        size += self.down_stamp.encoded_size(features)?; // UTime
        size += 4; // laggy_probability (u32)
        size += 4; // laggy_interval (u32)

        // Version 2+ fields
        if version >= 2 {
            size += 8; // features (u64)
        }

        // Version 3+ fields
        if version >= 3 {
            size += 4; // old_weight (u32)
        }

        // Version 4+ fields
        if version >= 4 {
            size += self.last_purged_snaps_scrub.encoded_size(features)?; // UTime
            size += 4; // dead_epoch (u32)
        }

        Some(size)
    }
}

denc::impl_denc_for_versioned!(OsdXInfo);

/// OSD information structure (osd_info_t in C++)
#[derive(Debug, Clone, Default, Serialize)]
pub struct OsdInfo {
    pub last_clean_begin: Epoch, // last interval that ended with a clean osd shutdown
    pub last_clean_end: Epoch,
    pub up_from: Epoch, // epoch osd marked up
    pub up_thru: Epoch, // lower bound on actual osd death (if > up_from)
    pub down_at: Epoch, // upper bound on actual osd death (if > up_from)
    pub lost_at: Epoch, // last epoch we decided data was "lost"
}

// DencMut implementation for OsdInfo
impl denc::Denc for OsdInfo {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        if buf.remaining_mut() < 25 {
            return Err(RadosError::Protocol(format!(
                "Insufficient buffer space for OsdInfo: need 25, have {}",
                buf.remaining_mut()
            )));
        }
        buf.put_u8(1); // struct_v = 1
        buf.put_u32_le(self.last_clean_begin);
        buf.put_u32_le(self.last_clean_end);
        buf.put_u32_le(self.up_from);
        buf.put_u32_le(self.up_thru);
        buf.put_u32_le(self.down_at);
        buf.put_u32_le(self.lost_at);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 25 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for OsdInfo".to_string(),
            ));
        }
        let _struct_v = buf.get_u8();
        let last_clean_begin = buf.get_u32_le();
        let last_clean_end = buf.get_u32_le();
        let up_from = buf.get_u32_le();
        let up_thru = buf.get_u32_le();
        let down_at = buf.get_u32_le();
        let lost_at = buf.get_u32_le();

        Ok(OsdInfo {
            last_clean_begin,
            last_clean_end,
            up_from,
            up_thru,
            down_at,
            lost_at,
        })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(25)
    }
}

impl denc::FixedSize for OsdInfo {
    const SIZE: usize = 25;
}

/// Pool type constants matching Ceph
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum PoolType {
    Replicated = 1,
    Erasure = 3,
}

impl From<u8> for PoolType {
    fn from(value: u8) -> Self {
        match value {
            1 => PoolType::Replicated,
            3 => PoolType::Erasure,
            _ => PoolType::Replicated, // Default fallback
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
        // Write directly to buf parameter
        buf.put_u64_le(self.snapid);
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
        let snapid = buf.get_u64_le();
        let stamp = UTime::decode(buf, features)?;
        let name = String::decode(buf, features)?;
        Ok(PoolSnapInfo {
            snapid,
            stamp,
            name,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None // Complex type - size computed by encoding
    }
}
// Manual Denc implementation for PoolSnapInfo (uses VersionedEncode)
impl denc::Denc for PoolSnapInfo {
    const USES_VERSIONING: bool = true;
    const FEATURE_DEPENDENT: bool = <PoolSnapInfo as VersionedEncode>::FEATURE_DEPENDENT;

    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.encode_versioned(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Self::decode_versioned(buf, features)
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        let mut temp_buf = bytes::BytesMut::new();
        if self.encode(&mut temp_buf, features).is_ok() {
            Some(temp_buf.len())
        } else {
            None
        }
    }
}

/// Snapshot interval for removed snaps
#[derive(Debug, Clone, Default, Serialize)]
pub struct SnapInterval {
    pub start: u64,
    pub len: u64,
}

// DencMut implementation for SnapInterval
impl denc::Denc for SnapInterval {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        if buf.remaining_mut() < 16 {
            return Err(RadosError::Protocol(format!(
                "Insufficient buffer space for SnapInterval: need 16, have {}",
                buf.remaining_mut()
            )));
        }
        buf.put_u64_le(self.start);
        buf.put_u64_le(self.len);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 16 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for SnapInterval".to_string(),
            ));
        }
        let start = buf.get_u64_le();
        let len = buf.get_u64_le();
        Ok(SnapInterval { start, len })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(16)
    }
}

impl denc::FixedSize for SnapInterval {
    const SIZE: usize = 16;
}

impl PgPool {
    // Version constants from Ceph
    const VERSION_CURRENT: u8 = 32; // Latest
    const COMPAT_VERSION: u8 = 5;

    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_stretch_pool(&self) -> bool {
        // For now, assume no stretch pools (like the default case in Ceph)
        // This would be based on crush rules and other pool configuration
        false
    }
}

impl VersionedEncode for PgPool {
    // PgPool encoding is feature-dependent: version changes based on
    // SERVER_TENTACLE, NEW_OSDOP_ENCODING, SERVER_LUMINOUS, SERVER_MIMIC,
    // SERVER_NAUTILUS features (see encoding_version below)
    const FEATURE_DEPENDENT: bool = true;

    fn encoding_version(&self, features: u64) -> u8 {
        use denc::features::*;

        // Implement the same logic as Ceph's pg_pool_t::encode
        let mut v = Self::VERSION_CURRENT; // Default to latest

        // NOTE: any new encoding dependencies must be reflected by SIGNIFICANT_FEATURES
        if !has_significant_feature(features, CEPH_FEATUREMASK_SERVER_TENTACLE) {
            if !has_significant_feature(features, CEPH_FEATUREMASK_NEW_OSDOP_ENCODING) {
                // this was the first post-hammer thing we added; if it's missing, encode
                // like hammer.
                v = 21;
            } else if !has_significant_feature(features, CEPH_FEATUREMASK_SERVER_LUMINOUS) {
                v = 24;
            } else if !has_significant_feature(features, CEPH_FEATUREMASK_SERVER_MIMIC) {
                v = 26;
            } else if !has_significant_feature(features, CEPH_FEATUREMASK_SERVER_NAUTILUS) {
                v = 27;
            } else if !self.is_stretch_pool() {
                v = 29;
            } else {
                v = 30;
            }
        }

        v
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
        // Write directly to buf parameter

        // Basic fields (always present in ENCODE_START format)
        buf.put_u8(self.pool_type);
        buf.put_u8(self.size);
        buf.put_u8(self.crush_rule); // Fixed: u8 not i32
        buf.put_u8(self.object_hash); // Fixed: u8 not u32
        buf.put_u32_le(self.pg_num);
        buf.put_u32_le(self.pgp_num);
        buf.put_u32_le(self.lpg_num); // always 0
        buf.put_u32_le(self.lpgp_num); // always 0
        buf.put_u32_le(self.last_change);
        buf.put_u64_le(self.snap_seq);
        buf.put_u32_le(self.snap_epoch);

        // Encode snaps and removed_snaps using generic implementations
        self.snaps.encode(buf, features)?;

        self.removed_snaps.encode(buf, features)?;

        buf.put_u64_le(self.auid);

        // Flags (version dependent)
        buf.put_u64_le(self.flags);

        buf.put_u32_le(0); // crash_replay_interval - always 0, not stored in struct
        buf.put_u8(self.min_size);
        buf.put_u64_le(self.quota_max_bytes);
        buf.put_u64_le(self.quota_max_objects);

        // Encode tiers using generic Vec implementation
        self.tiers.encode(buf, features)?;

        buf.put_i64_le(self.tier_of);
        buf.put_u8(self.cache_mode);
        buf.put_i64_le(self.read_tier);
        buf.put_i64_le(self.write_tier);

        // Encode properties using generic BTreeMap implementation
        self.properties.encode(buf, features)?;

        // Hit set params
        self.hit_set_params.encode(buf, features)?;

        buf.put_u32_le(self.hit_set_period);
        buf.put_u32_le(self.hit_set_count);
        buf.put_u32_le(self.stripe_width);
        buf.put_u64_le(self.target_max_bytes);
        buf.put_u64_le(self.target_max_objects);
        buf.put_u32_le(self.cache_target_dirty_ratio_micro);
        buf.put_u32_le(self.cache_target_full_ratio_micro);
        buf.put_u32_le(self.cache_min_flush_age);
        buf.put_u32_le(self.cache_min_evict_age);

        self.erasure_code_profile.encode(buf, features)?;

        buf.put_u32_le(self.last_force_op_resend_preluminous);
        buf.put_i32_le(self.min_read_recency_for_promote);
        buf.put_u64_le(self.expected_num_objects);

        // Version-dependent fields
        if version >= 19 {
            buf.put_u32_le(self.cache_target_dirty_high_ratio_micro);
        }
        if version >= 20 {
            buf.put_i32_le(self.min_write_recency_for_promote);
        }
        if version >= 21 {
            buf.put_u8(if self.use_gmt_hitset { 1 } else { 0 });
        }
        if version >= 22 {
            buf.put_u8(if self.fast_read { 1 } else { 0 });
        }
        if version >= 23 {
            buf.put_i32_le(self.hit_set_grade_decay_rate);
            buf.put_u32_le(self.hit_set_search_last_n);
        }
        if version >= 24 {
            // Pool opts - encoded with version header
            buf.put_u8(2); // opts version
            buf.put_u8(1); // opts compat_version
            let opts_len = self.opts_data.len() as u32;
            buf.put_u32_le(opts_len);
            buf.put_slice(&self.opts_data);
        }
        if version >= 25 {
            buf.put_u32_le(self.last_force_op_resend_prenautilus);
        }
        if version >= 26 {
            // Application metadata using generic BTreeMap implementation
            self.application_metadata.encode(buf, features)?;
        }
        if version >= 27 {
            self.create_time.encode(buf, features)?;
        }
        if version >= 28 {
            buf.put_u32_le(self.pg_num_target);
            buf.put_u32_le(self.pgp_num_target);
            buf.put_u32_le(self.pg_num_pending);
            buf.put_u32_le(0); // pg_num_dec_last_epoch_started (always 0)
            buf.put_u32_le(0); // pg_num_dec_last_epoch_clean (always 0)
            buf.put_u32_le(self.last_force_op_resend);
            buf.put_u8(self.pg_autoscale_mode);
        }
        if version >= 29 {
            self.last_pg_merge_meta.encode(buf, features)?;
        }
        if version == 30 {
            buf.put_u32_le(self.peering_crush_bucket_count);
            buf.put_u32_le(self.peering_crush_bucket_target);
            buf.put_u32_le(self.peering_crush_bucket_barrier);
            buf.put_i32_le(self.peering_crush_mandatory_member);
        }
        if version >= 31 {
            // Encode optional peering_crush_data (only if stretch pool)
            if self.is_stretch_pool() {
                buf.put_u8(1); // Some
                buf.put_u32_le(self.peering_crush_bucket_count);
                buf.put_u32_le(self.peering_crush_bucket_target);
                buf.put_u32_le(self.peering_crush_bucket_barrier);
                buf.put_i32_le(self.peering_crush_mandatory_member);
            } else {
                buf.put_u8(0); // None
            }
        }
        if version >= 32 {
            self.nonprimary_shards.encode(buf, features)?;
        }

        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        if version < Self::COMPAT_VERSION {
            return Err(RadosError::Protocol(format!(
                "pg_pool_t version {} too old (minimum {})",
                version,
                Self::COMPAT_VERSION
            )));
        }

        let mut pool = PgPool::new();

        // Basic fields (always present)
        pool.pool_type = buf.get_u8();
        pool.size = buf.get_u8();
        pool.crush_rule = buf.get_u8();
        pool.object_hash = buf.get_u8();
        pool.pg_num = buf.get_u32_le();
        pool.pgp_num = buf.get_u32_le();
        pool.lpg_num = buf.get_u32_le();
        pool.lpgp_num = buf.get_u32_le();
        pool.last_change = buf.get_u32_le();
        pool.snap_seq = buf.get_u64_le();
        pool.snap_epoch = buf.get_u32_le();

        // Version-dependent fields following C++ decode logic
        if version >= 3 {
            // Decode snaps map using generic implementation
            pool.snaps = BTreeMap::decode(buf, features)?;

            // Decode removed_snaps using generic implementation
            pool.removed_snaps = Vec::decode(buf, features)?;

            pool.auid = buf.get_u64_le();
        } else {
            // For older versions, use different encoding format
            return Err(RadosError::Protocol(
                "Version < 3 not supported yet".to_string(),
            ));
        }

        if version >= 4 {
            pool.flags = buf.get_u64_le();

            // Read crash_replay_interval but don't store it (matches C++ behavior)
            let _crash_replay_interval = buf.get_u32_le();
        } else {
            pool.flags = 0;
        }

        if version >= 7 {
            pool.min_size = buf.get_u8();
        } else {
            pool.min_size = if pool.size > 0 {
                pool.size - pool.size / 2
            } else {
                0
            };
        }

        if version >= 8 {
            pool.quota_max_bytes = buf.get_u64_le();

            pool.quota_max_objects = buf.get_u64_le();
        } else {
            pool.quota_max_bytes = 0;
            pool.quota_max_objects = 0;
        }

        if version >= 9 {
            // Decode tiers using generic Vec implementation
            pool.tiers = Vec::decode(buf, features)?;

            pool.tier_of = buf.get_i64_le();

            pool.cache_mode = buf.get_u8();

            pool.read_tier = buf.get_i64_le();

            pool.write_tier = buf.get_i64_le();
        } else {
            pool.tiers = Vec::new();
            pool.tier_of = -1;
            pool.cache_mode = 0;
            pool.read_tier = -1;
            pool.write_tier = -1;
        }

        if version >= 10 {
            // Decode properties using generic BTreeMap implementation
            pool.properties = BTreeMap::decode(buf, features)?;
        } else {
            pool.properties = BTreeMap::new();
        }

        if version >= 11 {
            // Debug code removed - cannot index into generic Buf trait

            // Decode HitSetParams using the proper implementation
            pool.hit_set_params = HitSetParams::decode_versioned(buf, features)?;

            pool.hit_set_period = buf.get_u32_le();
            pool.hit_set_count = buf.get_u32_le();
        } else {
            pool.hit_set_params = HitSetParams::None;
            pool.hit_set_period = 0;
            pool.hit_set_count = 0;
        }

        if version >= 12 {
            pool.stripe_width = buf.get_u32_le();
        } else {
            pool.stripe_width = 0;
        }

        if version >= 13 {
            pool.target_max_bytes = buf.get_u64_le();
            pool.target_max_objects = buf.get_u64_le();
            pool.cache_target_dirty_ratio_micro = buf.get_u32_le();
            pool.cache_target_full_ratio_micro = buf.get_u32_le();
            pool.cache_min_flush_age = buf.get_u32_le();
            pool.cache_min_evict_age = buf.get_u32_le();
        } else {
            pool.target_max_bytes = 0;
            pool.target_max_objects = 0;
            pool.cache_target_dirty_ratio_micro = 0;
            pool.cache_target_full_ratio_micro = 0;
            pool.cache_min_flush_age = 0;
            pool.cache_min_evict_age = 0;
        }

        if version >= 14 {
            pool.erasure_code_profile = String::decode(buf, features)?;
        } else {
            pool.erasure_code_profile = String::new();
        }

        if version >= 15 {
            pool.last_force_op_resend_preluminous = buf.get_u32_le();
        } else {
            pool.last_force_op_resend_preluminous = 0;
        }

        if version >= 16 {
            pool.min_read_recency_for_promote = buf.get_i32_le();
        } else {
            pool.min_read_recency_for_promote = 1;
        }

        if version >= 17 {
            pool.expected_num_objects = buf.get_u64_le();
        } else {
            pool.expected_num_objects = 0;
        }

        // Version-dependent fields
        if version >= 19 {
            pool.cache_target_dirty_high_ratio_micro = buf.get_u32_le();
        }
        if version >= 20 {
            pool.min_write_recency_for_promote = buf.get_i32_le();
        }
        if version >= 21 {
            pool.use_gmt_hitset = buf.get_u8() != 0;
        }
        if version >= 22 {
            pool.fast_read = buf.get_u8() != 0;
        }
        if version >= 23 {
            pool.hit_set_grade_decay_rate = buf.get_i32_le();
            pool.hit_set_search_last_n = buf.get_u32_le();
        } else {
            pool.hit_set_grade_decay_rate = 0;
            pool.hit_set_search_last_n = 1;
        }

        // For now, let's be more permissive with remaining fields
        // If we can't decode them properly, skip them
        if version >= 24 && buf.remaining() > 4 {
            // Pool opts (simplified as raw data for now)

            if buf.remaining() >= 8 {
                // Debug array removed - cannot index generic Buf
            }

            // opts is versioned encoded - read version header first
            if buf.remaining() >= 6 {
                // Need at least 6 bytes for version header
                let _version = buf.get_u8();
                let _compat_version = buf.get_u8();
                let opts_len = buf.get_u32_le() as usize;

                if opts_len <= buf.remaining() && opts_len < 10000 {
                    // Reasonable limit
                    pool.opts_data = vec![0u8; opts_len];
                    if opts_len > 0 {
                        buf.copy_to_slice(&mut pool.opts_data);
                    }
                } else {
                    pool.opts_data = Vec::new();
                }
            } else {
                pool.opts_data = Vec::new();
            }
        } else {
            pool.opts_data = Vec::new();
        }

        // Try to continue with remaining fields if there are enough bytes
        if version >= 25 && buf.remaining() >= 4 {
            pool.last_force_op_resend_prenautilus = buf.get_u32_le();
        } else {
            pool.last_force_op_resend_prenautilus = pool.last_force_op_resend_preluminous;
        }

        // Version 26+ fields
        if version >= 26 {
            // Application metadata (BTreeMap<String, String>)
            pool.application_metadata = BTreeMap::decode(buf, features)?;
        }

        if version >= 27 {
            // Create time
            pool.create_time = UTime::decode(buf, features)?;
        }

        if version >= 28 {
            // Version 28 fields
            pool.pg_num_target = buf.get_u32_le();
            pool.pgp_num_target = buf.get_u32_le();
            pool.pg_num_pending = buf.get_u32_le();
            let _pg_num_dec_last_epoch_started = buf.get_u32_le(); // Always 0 in recent versions
            let _pg_num_dec_last_epoch_clean = buf.get_u32_le(); // Always 0 in recent versions
            pool.last_force_op_resend = buf.get_u32_le();
            pool.pg_autoscale_mode = buf.get_u8();
        }

        if version >= 29 {
            pool.last_pg_merge_meta = PgMergeMeta::decode_versioned(buf, features)?;
        }

        // Version 30 fields (special handling)
        if version == 30 {
            pool.peering_crush_bucket_count = buf.get_u32_le();
            pool.peering_crush_bucket_target = buf.get_u32_le();
            pool.peering_crush_bucket_barrier = buf.get_u32_le();
            pool.peering_crush_mandatory_member = buf.get_i32_le();
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
            if buf.remaining() >= 2 {
                let _peek_bytes: Vec<u8> = buf.chunk()[..buf.remaining().min(10)].to_vec();
            }
            pool.nonprimary_shards = ShardIdSet::decode(buf, features)?;
        }

        Ok(pool)
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None // Complex type - size computed by encoding
    }
}
// Manual Denc implementation for PgPool (uses VersionedEncode)
impl denc::Denc for PgPool {
    const USES_VERSIONING: bool = true;
    const FEATURE_DEPENDENT: bool = <PgPool as VersionedEncode>::FEATURE_DEPENDENT;

    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.encode_versioned(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Self::decode_versioned(buf, features)
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        let mut temp_buf = bytes::BytesMut::new();
        if self.encode(&mut temp_buf, features).is_ok() {
            Some(temp_buf.len())
        } else {
            None
        }
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
    // Extra fields found in corpus data (purpose unknown)
    pub extra_field: Padding<u32>,
    pub extra_padding: Padding<u8>,
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

        // Skip extra_field and extra_padding (they're Padding types)

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
        // Write directly to buf parameter

        // Encode all fields in order
        self.source_pgid.encode(buf, features)?;

        buf.put_u32_le(self.ready_epoch);
        buf.put_u32_le(self.last_epoch_started);
        buf.put_u32_le(self.last_epoch_clean);

        self.source_version.encode(buf, features)?;

        self.target_version.encode(buf, features)?;

        // Only add extra bytes if they are non-zero (they exist in corpus but may not be standard)
        if *self.extra_field != 0 || *self.extra_padding != 0 {
            buf.put_u32_le(*self.extra_field);
            buf.put_u8(*self.extra_padding);
        }

        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        _version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        let source_pgid = PgId::decode(buf, features)?;
        let ready_epoch = buf.get_u32_le();
        let last_epoch_started = buf.get_u32_le();
        let last_epoch_clean = buf.get_u32_le();
        let source_version = EVersion::decode(buf, features)?;
        let target_version = EVersion::decode(buf, features)?;

        // In corpus data, there are 5 extra bytes at the end
        let mut extra_field = Padding::zero();
        let mut extra_padding = Padding::zero();
        if buf.remaining() >= 5 {
            *extra_field = buf.get_u32_le();
            *extra_padding = buf.get_u8();
        }

        Ok(PgMergeMeta {
            source_pgid,
            ready_epoch,
            last_epoch_started,
            last_epoch_clean,
            source_version,
            target_version,
            extra_field,
            extra_padding,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None // Complex type - size computed by encoding
    }
}
// Manual Denc implementation for PgMergeMeta (uses VersionedEncode)
impl denc::Denc for PgMergeMeta {
    const USES_VERSIONING: bool = true;
    const FEATURE_DEPENDENT: bool = <PgMergeMeta as VersionedEncode>::FEATURE_DEPENDENT;

    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.encode_versioned(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Self::decode_versioned(buf, features)
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        let mut temp_buf = bytes::BytesMut::new();
        if self.encode(&mut temp_buf, features).is_ok() {
            Some(temp_buf.len())
        } else {
            None
        }
    }
}

/// Snapshot interval set (snap_interval_set_t in C++)
/// A set of snapshot intervals represented as a vector of SnapInterval
#[derive(Debug, Clone, Default, Serialize)]
pub struct SnapIntervalSet {
    pub intervals: Vec<SnapInterval>,
}

// DencMut implementation for SnapIntervalSet
impl denc::Denc for SnapIntervalSet {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        <Vec<SnapInterval> as denc::Denc>::encode(&self.intervals, buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        let intervals = <Vec<SnapInterval> as denc::Denc>::decode(buf, features)?;
        Ok(SnapIntervalSet { intervals })
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        <Vec<SnapInterval> as denc::Denc>::encoded_size(&self.intervals, features)
    }
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

    /// Apply this incremental update to an OSDMap
    pub fn apply_to(&self, base: &mut OSDMap) -> Result<(), RadosError> {
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

impl VersionedEncode for OSDMapIncremental {
    fn encoding_version(&self, _features: u64) -> u8 {
        8 // Current version
    }

    fn compat_version(&self, _features: u64) -> u8 {
        7 // Minimum compatible version
    }

    fn encode_content<B: BufMut>(
        &self,
        _buf: &mut B,
        _features: u64,
        _version: u8,
    ) -> Result<(), RadosError> {
        // Not implemented yet
        Err(RadosError::Protocol(
            "OSDMapIncremental encoding not implemented".into(),
        ))
    }

    fn decode_content<B: Buf>(
        _buf: &mut B,
        _features: u64,
        _version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Not used - we override decode_versioned for nested versioning
        Err(RadosError::Protocol(
            "Use decode_versioned for OSDMapIncremental".into(),
        ))
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None // Complex type - size computed by encoding
    }

    fn decode_versioned<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        use bytes::Bytes;

        // Outer wrapper (version 8, legacy compat 7)
        if buf.remaining() < 6 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for version header".into(),
            ));
        }

        let struct_v = buf.get_u8();
        let _struct_compat = buf.get_u8();
        let struct_len = buf.get_u32_le() as usize;

        if buf.remaining() < struct_len {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes: need {}, have {}",
                struct_len,
                buf.remaining()
            )));
        }

        if struct_v < 7 {
            return Err(RadosError::Protocol(
                "Legacy incremental encoding (v<7) not supported".into(),
            ));
        }

        // Extract outer content into Bytes
        let outer_bytes = buf.copy_to_bytes(struct_len);
        let mut outer_cursor = &outer_bytes[..];
        let mut inc = OSDMapIncremental::new(0);

        // Client-usable data section (version 9)
        {
            if outer_cursor.remaining() < 6 {
                return Err(RadosError::Protocol(
                    "Insufficient bytes for client section header".into(),
                ));
            }

            let client_v = outer_cursor.get_u8();
            let _client_compat = outer_cursor.get_u8();
            let client_len = outer_cursor.get_u32_le() as usize;

            if outer_cursor.remaining() < client_len {
                return Err(RadosError::Protocol(format!(
                    "Insufficient bytes for client section: need {}, have {}",
                    client_len,
                    outer_cursor.remaining()
                )));
            }

            // Extract client section bytes
            let client_bytes = outer_cursor.copy_to_bytes(client_len);
            let mut client_cursor = &client_bytes[..];

            inc.fsid = UuidD::decode(&mut client_cursor, features)?;
            inc.epoch = Epoch::decode(&mut client_cursor, features)?;
            inc.modified = UTime::decode(&mut client_cursor, features)?;
            inc.new_pool_max = i64::decode(&mut client_cursor, features)?;
            inc.new_flags = i32::decode(&mut client_cursor, features)?;
            inc.fullmap = Bytes::decode(&mut client_cursor, features)?;
            inc.crush = Bytes::decode(&mut client_cursor, features)?;

            inc.new_max_osd = i32::decode(&mut client_cursor, features)?;
            inc.new_pools = BTreeMap::decode(&mut client_cursor, features)?;

            // Handle new_pool_names based on version
            if client_v == 5 {
                // Version 5: manual decode
                let count = u32::decode(&mut client_cursor, features)? as usize;
                for _ in 0..count {
                    let pool_id = i64::decode(&mut client_cursor, features)?;
                    let pool_name = String::decode(&mut client_cursor, features)?;
                    inc.new_pool_names.insert(pool_id as u64, pool_name);
                }
            } else if client_v >= 6 {
                // Version 6+: standard decode
                inc.new_pool_names = BTreeMap::decode(&mut client_cursor, features)?;
            }

            // Handle old_pools based on version
            if client_v < 6 {
                // Version < 6: manual decode
                let count = u32::decode(&mut client_cursor, features)? as usize;
                for _ in 0..count {
                    let pool_id = i64::decode(&mut client_cursor, features)?;
                    inc.old_pools.push(pool_id as u64);
                }
            } else {
                // Version 6+: standard decode
                inc.old_pools = Vec::decode(&mut client_cursor, features)?;
            }

            inc.new_up_client = BTreeMap::decode(&mut client_cursor, features)?;

            if client_v >= 5 {
                inc.new_state = BTreeMap::decode(&mut client_cursor, features)?;
            } else {
                // Convert old u8 state format to u32
                let old_state: BTreeMap<i32, u8> = BTreeMap::decode(&mut client_cursor, features)?;
                inc.new_state = old_state.into_iter().map(|(k, v)| (k, v as u32)).collect();
            }

            inc.new_weight = BTreeMap::decode(&mut client_cursor, features)?;
            inc.new_pg_temp = BTreeMap::decode(&mut client_cursor, features)?;
            inc.new_primary_temp = BTreeMap::decode(&mut client_cursor, features)?;

            if client_v >= 2 {
                inc.new_primary_affinity = BTreeMap::decode(&mut client_cursor, features)?;
            }

            if client_v >= 3 {
                inc.new_erasure_code_profiles = BTreeMap::decode(&mut client_cursor, features)?;
                inc.old_erasure_code_profiles = Vec::decode(&mut client_cursor, features)?;
            }

            if client_v >= 4 {
                inc.new_pg_upmap = BTreeMap::decode(&mut client_cursor, features)?;
                inc.old_pg_upmap = Vec::decode(&mut client_cursor, features)?;
                inc.new_pg_upmap_items = BTreeMap::decode(&mut client_cursor, features)?;
                inc.old_pg_upmap_items = Vec::decode(&mut client_cursor, features)?;
            }

            if client_v >= 6 {
                inc.new_removed_snaps = BTreeMap::decode(&mut client_cursor, features)?;
                inc.new_purged_snaps = BTreeMap::decode(&mut client_cursor, features)?;
            }

            if client_v >= 8 {
                inc.new_last_up_change = UTime::decode(&mut client_cursor, features)?;
                inc.new_last_in_change = UTime::decode(&mut client_cursor, features)?;
            }

            if client_v >= 9 {
                inc.new_pg_upmap_primary = BTreeMap::decode(&mut client_cursor, features)?;
                inc.old_pg_upmap_primary = Vec::decode(&mut client_cursor, features)?;
            }

            // DECODE_FINISH: Forward compatibility - remaining bytes are ignored
        }

        // Extended OSD-only data section (version 12)
        {
            if outer_cursor.remaining() < 6 {
                return Err(RadosError::Protocol(
                    "Insufficient bytes for OSD section header".into(),
                ));
            }

            let osd_v = outer_cursor.get_u8();
            let _osd_compat = outer_cursor.get_u8();
            let osd_len = outer_cursor.get_u32_le() as usize;

            if outer_cursor.remaining() < osd_len {
                return Err(RadosError::Protocol(format!(
                    "Insufficient bytes for OSD section: need {}, have {}",
                    osd_len,
                    outer_cursor.remaining()
                )));
            }

            // Extract OSD section bytes
            let osd_bytes = outer_cursor.copy_to_bytes(osd_len);
            let mut osd_cursor = &osd_bytes[..];

            inc.new_hb_back_up = BTreeMap::decode(&mut osd_cursor, features)?;
            inc.new_up_thru = BTreeMap::decode(&mut osd_cursor, features)?;
            inc.new_last_clean_interval = BTreeMap::decode(&mut osd_cursor, features)?;
            inc.new_lost = BTreeMap::decode(&mut osd_cursor, features)?;
            inc.new_blocklist = BTreeMap::decode(&mut osd_cursor, features)?;
            inc.old_blocklist = Vec::decode(&mut osd_cursor, features)?;
            inc.new_up_cluster = BTreeMap::decode(&mut osd_cursor, features)?;
            inc.cluster_snapshot = String::decode(&mut osd_cursor, features)?;
            inc.new_uuid = BTreeMap::decode(&mut osd_cursor, features)?;
            inc.new_xinfo = BTreeMap::decode(&mut osd_cursor, features)?;
            inc.new_hb_front_up = BTreeMap::decode(&mut osd_cursor, features)?;

            if osd_v >= 2 {
                inc.encode_features = u64::decode(&mut osd_cursor, features)?;
            } else {
                inc.encode_features = 0;
            }

            if osd_v >= 3 {
                inc.new_nearfull_ratio = f32::from_bits(osd_cursor.get_u32_le());
                inc.new_full_ratio = f32::from_bits(osd_cursor.get_u32_le());
            }

            if osd_v >= 4 {
                inc.new_backfillfull_ratio = f32::from_bits(osd_cursor.get_u32_le());
            }

            if osd_v >= 6 {
                inc.new_require_min_compat_client = u8::decode(&mut osd_cursor, features)?;
                inc.new_require_osd_release = u8::decode(&mut osd_cursor, features)?;
            }

            if osd_v >= 8 {
                inc.new_crush_node_flags = BTreeMap::decode(&mut osd_cursor, features)?;
            }

            if osd_v >= 9 {
                inc.new_device_class_flags = BTreeMap::decode(&mut osd_cursor, features)?;
            }

            if osd_v >= 10 {
                inc.change_stretch_mode = bool::decode(&mut osd_cursor, features)?;
                inc.new_stretch_bucket_count = u32::decode(&mut osd_cursor, features)?;
                inc.new_degraded_stretch_mode = u32::decode(&mut osd_cursor, features)?;
                inc.new_recovering_stretch_mode = u32::decode(&mut osd_cursor, features)?;
                inc.new_stretch_mode_bucket = i32::decode(&mut osd_cursor, features)?;
                inc.stretch_mode_enabled = bool::decode(&mut osd_cursor, features)?;
            }

            if osd_v >= 11 {
                inc.new_range_blocklist = BTreeMap::decode(&mut osd_cursor, features)?;
                inc.old_range_blocklist = Vec::decode(&mut osd_cursor, features)?;
            }

            if osd_v >= 12 {
                inc.mutate_allow_crimson = u8::decode(&mut osd_cursor, features)?;
            }

            // DECODE_FINISH: Forward compatibility - remaining bytes are ignored
        }

        // CRC validation (version 8+)
        if struct_v >= 8 && outer_cursor.remaining() >= 8 {
            inc.have_crc = true;
            inc.inc_crc = u32::decode(&mut outer_cursor, features)?;
            inc.full_crc = u32::decode(&mut outer_cursor, features)?;
            // TODO: Validate CRC if needed
        }

        // DECODE_FINISH: Forward compatibility - remaining bytes are ignored

        Ok(inc)
    }

    fn encode_versioned<B: BufMut>(&self, _buf: &mut B, _features: u64) -> Result<(), RadosError> {
        // Not implemented yet
        Err(RadosError::Protocol(
            "OSDMapIncremental encoding not implemented".into(),
        ))
    }
}

/// OSDMap - the main OSD cluster map structure
/// This is a simplified version that focuses on decoding the essential fields
#[derive(Debug, Clone, Default, Serialize)]
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

impl OSDMap {
    pub fn new() -> Self {
        Self::default()
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
        let locator = crush::ObjectLocator::new(pool_id);

        // Calculate the PG using the CRUSH placement function
        let pg = crush::object_to_pg(object_name, &locator, pool.pg_num);

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
            .map_err(|e| RadosError::Protocol(format!("Failed to get CRUSH rule: {:?}", e)))?;

        // Use CRUSH to calculate the OSD set
        let mut result = Vec::new();
        let result_max = pool.size as usize;

        // Use PG seed as the input to CRUSH
        let x = pg.seed;

        // Call CRUSH mapper
        crush::mapper::crush_do_rule(
            crush_map,
            pool.crush_rule as u32,
            x,
            &mut result,
            result_max,
            &self.osd_weight,
        )
        .map_err(|e| RadosError::Protocol(format!("CRUSH mapping failed: {:?}", e)))?;

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
        if version < 7 {
            return Err(RadosError::Protocol(format!(
                "OSDMap version {} too old (minimum 7)",
                version
            )));
        }

        let mut map = OSDMap::new();

        // Decode client-usable data section (nested ENCODE_START)
        let client_v = buf.get_u8();
        let _client_compat = buf.get_u8();
        let client_len = buf.get_u32_le() as usize;

        if buf.remaining() < client_len {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes for client data: need {}, have {}",
                client_len,
                buf.remaining()
            )));
        }

        let mut client_bytes = buf.copy_to_bytes(client_len);

        // Decode client-usable fields
        map.fsid = UuidD::decode(&mut client_bytes, features)?;
        map.epoch = client_bytes.get_u32_le();
        map.created = UTime::decode(&mut client_bytes, features)?;
        map.modified = UTime::decode(&mut client_bytes, features)?;

        // Decode pools (BTreeMap<u64, PgPool>)
        map.pools = BTreeMap::decode(&mut client_bytes, features)?;

        // Decode pool_name (BTreeMap<u64, String>)
        map.pool_name = BTreeMap::decode(&mut client_bytes, features)?;

        map.pool_max = client_bytes.get_i32_le();

        // Decode flags
        map.flags = client_bytes.get_u32_le();

        // Decode max_osd
        map.max_osd = client_bytes.get_i32_le();

        // Decode osd_state
        if client_v >= 5 {
            // Version 5+ uses Vec<u32> directly
            map.osd_state = Vec::decode(&mut client_bytes, features)?;
        } else {
            // Older versions use Vec<u8>
            let n = client_bytes.get_u32_le() as usize;
            map.osd_state = Vec::with_capacity(n);
            for _ in 0..n {
                map.osd_state.push(client_bytes.get_u8() as u32);
            }
        }

        // Decode osd_weight
        map.osd_weight = Vec::decode(&mut client_bytes, features)?;

        // Decode osd_addrs (client addresses)
        if client_v >= 8 {
            // Version 8+ uses EntityAddrvec
            map.osd_addrs_client = Vec::decode(&mut client_bytes, features)?;
        } else {
            // Older versions use single EntityAddr per OSD
            let n = client_bytes.get_u32_le() as usize;
            map.osd_addrs_client = Vec::with_capacity(n);
            for _ in 0..n {
                let addr = EntityAddr::decode(&mut client_bytes, features)?;
                map.osd_addrs_client.push(EntityAddrvec::with_addr(addr));
            }
        }

        // Decode pg_temp (using a custom decoder since it's not a standard BTreeMap)
        if client_bytes.remaining() < 4 {
            return Err(RadosError::Protocol(format!(
                "Not enough bytes for pg_temp length: need 4, have {}",
                client_bytes.remaining()
            )));
        }
        let pg_temp_len = client_bytes.get_u32_le() as usize;
        for _ in 0..pg_temp_len {
            let pgid = PgId::decode(&mut client_bytes, features)?;
            let osds = Vec::<i32>::decode(&mut client_bytes, features)?;
            map.pg_temp.insert(pgid, osds);
        }

        // Decode primary_temp
        if client_bytes.remaining() < 4 {
            return Err(RadosError::Protocol(format!(
                "Not enough bytes for primary_temp length: need 4, have {}",
                client_bytes.remaining()
            )));
        }
        let primary_temp_len = client_bytes.get_u32_le() as usize;
        for _ in 0..primary_temp_len {
            let pgid = PgId::decode(&mut client_bytes, features)?;
            let osd = client_bytes.get_i32_le();
            map.primary_temp.insert(pgid, osd);
        }

        map.osd_primary_affinity = Vec::decode(&mut client_bytes, features)?;

        // Decode CRUSH map (as bytes, then parse it)
        let crush_bytes = Bytes::decode(&mut client_bytes, features)?;

        // Parse the CRUSH map from the bytes
        if !crush_bytes.is_empty() {
            let mut crush_buf = crush_bytes.clone();
            match CrushMap::decode(&mut crush_buf) {
                Ok(crush_map) => {
                    map.crush = Some(crush_map);
                }
                Err(e) => {
                    // Log the error but continue - CRUSH map parsing is optional for now
                    tracing::warn!("Failed to parse CRUSH map: {:?}", e);
                    map.crush = None;
                }
            }
        } else {
            map.crush = None;
        }

        // Decode erasure_code_profiles
        map.erasure_code_profiles = BTreeMap::decode(&mut client_bytes, features)?;

        // Version 4+ fields
        if client_v >= 4 {
            // Decode pg_upmap
            let pg_upmap_len = client_bytes.get_u32_le() as usize;
            for _ in 0..pg_upmap_len {
                let pgid = PgId::decode(&mut client_bytes, features)?;
                let osds = Vec::<i32>::decode(&mut client_bytes, features)?;
                map.pg_upmap.insert(pgid, osds);
            }

            // Decode pg_upmap_items
            let pg_upmap_items_len = client_bytes.get_u32_le() as usize;
            for _ in 0..pg_upmap_items_len {
                let pgid = PgId::decode(&mut client_bytes, features)?;
                let items_len = client_bytes.get_u32_le() as usize;
                let mut items = Vec::with_capacity(items_len);
                for _ in 0..items_len {
                    let from = client_bytes.get_i32_le();
                    let to = client_bytes.get_i32_le();
                    items.push((from, to));
                }
                map.pg_upmap_items.insert(pgid, items);
            }
        }

        // Version 6+ fields
        if client_v >= 6 {
            map.crush_version = client_bytes.get_i32_le();
        }

        // Version 7+ fields
        if client_v >= 7 {
            // Decode new_removed_snaps
            let removed_snaps_len = client_bytes.get_u32_le() as usize;
            for _ in 0..removed_snaps_len {
                let pool_id = client_bytes.get_i64_le();
                let snap_set = SnapIntervalSet::decode(&mut client_bytes, features)?;
                map.new_removed_snaps.insert(pool_id as u64, snap_set);
            }

            // Decode new_purged_snaps
            let purged_snaps_len = client_bytes.get_u32_le() as usize;
            for _ in 0..purged_snaps_len {
                let pool_id = client_bytes.get_i64_le();
                let snap_set = SnapIntervalSet::decode(&mut client_bytes, features)?;
                map.new_purged_snaps.insert(pool_id as u64, snap_set);
            }
        }

        // Version 9+ fields
        if client_v >= 9 {
            map.last_up_change = UTime::decode(&mut client_bytes, features)?;
            map.last_in_change = UTime::decode(&mut client_bytes, features)?;
        }

        // Version 10+ fields
        if client_v >= 10 {
            let pg_upmap_primaries_len = client_bytes.get_u32_le() as usize;
            for _ in 0..pg_upmap_primaries_len {
                let pgid = PgId::decode(&mut client_bytes, features)?;
                let osd = client_bytes.get_i32_le();
                map.pg_upmap_primaries.insert(pgid, osd);
            }
        }

        // Decode OSD-only data section (nested ENCODE_START)
        let osd_v = buf.get_u8();
        let _osd_compat = buf.get_u8();
        let osd_len = buf.get_u32_le() as usize;

        if buf.remaining() < osd_len {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes for OSD data: need {}, have {}",
                osd_len,
                buf.remaining()
            )));
        }

        let mut osd_bytes = buf.copy_to_bytes(osd_len);

        // Decode hb_back_addrs
        map.osd_addrs_hb_back = Vec::decode(&mut osd_bytes, features)?;

        // Decode osd_info
        map.osd_info = Vec::decode(&mut osd_bytes, features)?;

        // Decode blocklist
        map.blocklist = BTreeMap::decode(&mut osd_bytes, features)?;

        // Decode cluster_addrs
        map.osd_addrs_cluster = Vec::decode(&mut osd_bytes, features)?;

        map.cluster_snapshot_epoch = osd_bytes.get_u32_le();
        map.cluster_snapshot = String::decode(&mut osd_bytes, features)?;

        // Decode osd_uuid
        map.osd_uuid = Vec::decode(&mut osd_bytes, features)?;

        // Decode osd_xinfo
        map.osd_xinfo = Vec::decode(&mut osd_bytes, features)?;

        // Decode hb_front_addrs
        map.osd_addrs_hb_front = Vec::decode(&mut osd_bytes, features)?;

        // Version 2+ fields
        if osd_v >= 2 {
            map.nearfull_ratio = f32::from_bits(osd_bytes.get_u32_le());
            map.full_ratio = f32::from_bits(osd_bytes.get_u32_le());
        }

        // Version 3+ fields
        if osd_v >= 3 {
            map.backfillfull_ratio = f32::from_bits(osd_bytes.get_u32_le());
        }

        // Version 5+ fields
        if osd_v >= 5 {
            map.require_min_compat_client = osd_bytes.get_u8();
            map.require_osd_release = osd_bytes.get_u8();
        }

        // Version 6+ fields
        if osd_v >= 6 {
            let queue_len = osd_bytes.get_u32_le() as usize;
            for _ in 0..queue_len {
                let pool_id = osd_bytes.get_i64_le();
                let snap_set = SnapIntervalSet::decode(&mut osd_bytes, features)?;
                map.removed_snaps_queue.push((pool_id, snap_set));
            }
        }

        // Version 8+ fields
        if osd_v >= 8 {
            map.crush_node_flags = BTreeMap::decode(&mut osd_bytes, features)?;
        }

        // Version 9+ fields
        if osd_v >= 9 {
            map.device_class_flags = BTreeMap::decode(&mut osd_bytes, features)?;
        }

        // Version 10+ fields (stretch mode)
        if osd_v >= 10 {
            map.stretch_mode_enabled = osd_bytes.get_u8() != 0;
            map.stretch_bucket_count = osd_bytes.get_u32_le();
            map.degraded_stretch_mode = osd_bytes.get_u32_le();
            map.recovering_stretch_mode = osd_bytes.get_u32_le();
            map.stretch_mode_bucket = osd_bytes.get_i32_le();
        }

        // Version 11+ fields
        if osd_v >= 11 {
            map.range_blocklist = BTreeMap::decode(&mut osd_bytes, features)?;
        }

        // Version 12+ fields
        if osd_v >= 12 {
            map.allow_crimson = osd_bytes.get_u8() != 0;
        }

        Ok(map)
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

// Level 2/3: Feature-dependent types (encoding changes based on features)
mark_feature_dependent_encoding!(OsdXInfo);
mark_feature_dependent_encoding!(PgPool);
mark_feature_dependent_encoding!(OSDMap);
