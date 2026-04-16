// PGMap and related types from Ceph's mon/PGMap.h

use crate::denc::{Denc, EVersion, Epoch, FixedSize, RadosError, UTime, Version, VersionedEncode};
use crate::osdclient::osdmap::PgId;
use bytes::{Buf, BufMut};
use std::collections::BTreeMap;

/// Wire size of ObjectStatSum at version 20: 36 × i64 + 4 × i32
const OBJECT_STAT_SUM_ENCODED_SIZE: usize =
    36 * std::mem::size_of::<i64>() + 4 * std::mem::size_of::<i32>();

/// Number of u32 fields in OsdStatInterfaces (1 + 3×6 + 1 = 21 fields for
/// last_update, back_{pingtime,min,max}[3], back_last, front_{pingtime,min,max}[3], front_last)
const OSD_STAT_INTERFACES_NUM_FIELDS: usize = 21;

/// Wire size of OsdStatInterfaces: 21 × u32
const OSD_STAT_INTERFACES_SIZE: usize = OSD_STAT_INTERFACES_NUM_FIELDS * std::mem::size_of::<u32>();

/// PG count statistics for an OSD
/// C++ definition: PGMapDigest::pg_count in mon/PGMap.h
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Default, crate::Denc)]
#[denc(crate = "crate")]
pub(crate) struct PgCount {
    /// Number of PGs for which this OSD is in the acting set
    pub acting: i32,
    /// Number of PGs for which this OSD is in the up set but not acting
    pub up_not_acting: i32,
    /// Number of PGs for which this OSD is the primary
    pub primary: i32,
}

impl FixedSize for PgCount {
    const SIZE: usize = 12;
}

/// Filesystem statistics from the object store
/// C++ definition: store_statfs_t in osd/osd_types.h
#[derive(Debug, Clone, PartialEq, Eq, Default, crate::VersionedDenc, serde::Serialize)]
#[denc(crate = "crate", version = 1, compat = 1)]
pub struct StoreStatfs {
    /// Total bytes
    pub total: u64,
    /// Free bytes available
    pub available: u64,
    /// Bytes reserved for internal purposes
    pub internally_reserved: u64,
    /// Bytes allocated by the store
    pub allocated: i64,
    /// Bytes actually stored by the user
    pub data_stored: i64,
    /// Bytes stored after compression
    pub data_compressed: i64,
    /// Bytes allocated for compressed data
    pub data_compressed_allocated: i64,
    /// Bytes that were compressed
    pub data_compressed_original: i64,
    /// Approx usage of omap data
    pub omap_allocated: i64,
    /// Approx usage of internal metadata
    pub internal_metadata: i64,
}

impl StoreStatfs {
    pub fn is_zero(&self) -> bool {
        *self == Self::default()
    }
}

/// Object statistics summary
/// C++ definition: object_stat_sum_t in osd/osd_types.h
/// Version 20, encodes 40 fields (36 i64 + 4 i32)
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize)]
pub struct ObjectStatSum {
    pub num_bytes: i64,
    pub num_objects: i64,
    pub num_object_clones: i64,
    pub num_object_copies: i64,
    pub num_objects_missing_on_primary: i64,
    pub num_objects_degraded: i64,
    pub num_objects_unfound: i64,
    #[serde(rename = "num_read")]
    pub num_rd: i64,
    #[serde(rename = "num_read_kb")]
    pub num_rd_kb: i64,
    #[serde(rename = "num_write")]
    pub num_wr: i64,
    #[serde(rename = "num_write_kb")]
    pub num_wr_kb: i64,
    pub num_scrub_errors: i64,
    pub num_objects_recovered: i64,
    pub num_bytes_recovered: i64,
    pub num_keys_recovered: i64,
    pub num_shallow_scrub_errors: i64,
    pub num_deep_scrub_errors: i64,
    pub num_objects_dirty: i64,
    pub num_whiteouts: i64,
    pub num_objects_omap: i64,
    pub num_objects_hit_set_archive: i64,
    pub num_objects_misplaced: i64,
    pub num_bytes_hit_set_archive: i64,
    pub num_flush: i64,
    pub num_flush_kb: i64,
    pub num_evict: i64,
    pub num_evict_kb: i64,
    pub num_promote: i64,
    pub num_flush_mode_high: i32,
    pub num_flush_mode_low: i32,
    pub num_evict_mode_some: i32,
    pub num_evict_mode_full: i32,
    pub num_objects_pinned: i64,
    pub num_objects_missing: i64,
    pub num_legacy_snapsets: i64,
    pub num_large_omap_objects: i64,
    pub num_objects_manifest: i64,
    pub num_omap_bytes: i64,
    pub num_omap_keys: i64,
    pub num_objects_repaired: i64,
}

impl ObjectStatSum {
    pub fn is_zero(&self) -> bool {
        *self == Self::default()
    }
}

impl VersionedEncode for ObjectStatSum {
    fn encoding_version(&self, _features: u64) -> u8 {
        20
    }

    fn compat_version(&self, _features: u64) -> u8 {
        14
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> Result<(), RadosError> {
        // Encode all 40 fields in order (36 × i64 + 4 × i32)
        self.num_bytes.encode(buf, features)?;
        self.num_objects.encode(buf, features)?;
        self.num_object_clones.encode(buf, features)?;
        self.num_object_copies.encode(buf, features)?;
        self.num_objects_missing_on_primary.encode(buf, features)?;
        self.num_objects_degraded.encode(buf, features)?;
        self.num_objects_unfound.encode(buf, features)?;
        self.num_rd.encode(buf, features)?;
        self.num_rd_kb.encode(buf, features)?;
        self.num_wr.encode(buf, features)?;
        self.num_wr_kb.encode(buf, features)?;
        self.num_scrub_errors.encode(buf, features)?;
        self.num_objects_recovered.encode(buf, features)?;
        self.num_bytes_recovered.encode(buf, features)?;
        self.num_keys_recovered.encode(buf, features)?;
        self.num_shallow_scrub_errors.encode(buf, features)?;
        self.num_deep_scrub_errors.encode(buf, features)?;
        self.num_objects_dirty.encode(buf, features)?;
        self.num_whiteouts.encode(buf, features)?;
        self.num_objects_omap.encode(buf, features)?;
        self.num_objects_hit_set_archive.encode(buf, features)?;
        self.num_objects_misplaced.encode(buf, features)?;
        self.num_bytes_hit_set_archive.encode(buf, features)?;
        self.num_flush.encode(buf, features)?;
        self.num_flush_kb.encode(buf, features)?;
        self.num_evict.encode(buf, features)?;
        self.num_evict_kb.encode(buf, features)?;
        self.num_promote.encode(buf, features)?;
        self.num_flush_mode_high.encode(buf, features)?;
        self.num_flush_mode_low.encode(buf, features)?;
        self.num_evict_mode_some.encode(buf, features)?;
        self.num_evict_mode_full.encode(buf, features)?;
        self.num_objects_pinned.encode(buf, features)?;
        self.num_objects_missing.encode(buf, features)?;
        self.num_legacy_snapsets.encode(buf, features)?;
        self.num_large_omap_objects.encode(buf, features)?;
        self.num_objects_manifest.encode(buf, features)?;
        self.num_omap_bytes.encode(buf, features)?;
        self.num_omap_keys.encode(buf, features)?;
        self.num_objects_repaired.encode(buf, features)?;

        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Quincy always emits v20; all 40 fields are always present.
        crate::denc::check_min_version!(version, 20, "ObjectStatSum", "Quincy v17+");

        let num_bytes = i64::decode(buf, features)?;
        let num_objects = i64::decode(buf, features)?;
        let num_object_clones = i64::decode(buf, features)?;
        let num_object_copies = i64::decode(buf, features)?;
        let num_objects_missing_on_primary = i64::decode(buf, features)?;
        let num_objects_degraded = i64::decode(buf, features)?;
        let num_objects_unfound = i64::decode(buf, features)?;
        let num_rd = i64::decode(buf, features)?;
        let num_rd_kb = i64::decode(buf, features)?;
        let num_wr = i64::decode(buf, features)?;
        let num_wr_kb = i64::decode(buf, features)?;
        let num_scrub_errors = i64::decode(buf, features)?;
        let num_objects_recovered = i64::decode(buf, features)?;
        let num_bytes_recovered = i64::decode(buf, features)?;
        let num_keys_recovered = i64::decode(buf, features)?;
        let num_shallow_scrub_errors = i64::decode(buf, features)?;
        let num_deep_scrub_errors = i64::decode(buf, features)?;
        let num_objects_dirty = i64::decode(buf, features)?;
        let num_whiteouts = i64::decode(buf, features)?;
        let num_objects_omap = i64::decode(buf, features)?;
        let num_objects_hit_set_archive = i64::decode(buf, features)?;
        let num_objects_misplaced = i64::decode(buf, features)?;
        let num_bytes_hit_set_archive = i64::decode(buf, features)?;
        let num_flush = i64::decode(buf, features)?;
        let num_flush_kb = i64::decode(buf, features)?;
        let num_evict = i64::decode(buf, features)?;
        let num_evict_kb = i64::decode(buf, features)?;
        let num_promote = i64::decode(buf, features)?;
        let num_flush_mode_high = i32::decode(buf, features)?;
        let num_flush_mode_low = i32::decode(buf, features)?;
        let num_evict_mode_some = i32::decode(buf, features)?;
        let num_evict_mode_full = i32::decode(buf, features)?;
        let num_objects_pinned = i64::decode(buf, features)?;
        let num_objects_missing = i64::decode(buf, features)?;

        // Fields present since v16-v20; Quincy always emits v20 so all are unconditional.
        let num_legacy_snapsets = i64::decode(buf, features)?;
        let num_large_omap_objects = i64::decode(buf, features)?;
        let num_objects_manifest = i64::decode(buf, features)?;
        let num_omap_bytes = i64::decode(buf, features)?;
        let num_omap_keys = i64::decode(buf, features)?;
        let num_objects_repaired = i64::decode(buf, features)?;

        Ok(ObjectStatSum {
            num_bytes,
            num_objects,
            num_object_clones,
            num_object_copies,
            num_objects_missing_on_primary,
            num_objects_degraded,
            num_objects_unfound,
            num_rd,
            num_rd_kb,
            num_wr,
            num_wr_kb,
            num_scrub_errors,
            num_objects_recovered,
            num_bytes_recovered,
            num_keys_recovered,
            num_shallow_scrub_errors,
            num_deep_scrub_errors,
            num_objects_dirty,
            num_whiteouts,
            num_objects_omap,
            num_objects_hit_set_archive,
            num_objects_misplaced,
            num_bytes_hit_set_archive,
            num_flush,
            num_flush_kb,
            num_evict,
            num_evict_kb,
            num_promote,
            num_flush_mode_high,
            num_flush_mode_low,
            num_evict_mode_some,
            num_evict_mode_full,
            num_objects_pinned,
            num_objects_missing,
            num_legacy_snapsets,
            num_large_omap_objects,
            num_objects_manifest,
            num_omap_bytes,
            num_omap_keys,
            num_objects_repaired,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        Some(OBJECT_STAT_SUM_ENCODED_SIZE)
    }
}

impl Denc for ObjectStatSum {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.encode_versioned(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Self::decode_versioned(buf, features)
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(6 + OBJECT_STAT_SUM_ENCODED_SIZE) // 6 bytes for ENCODE_START header + content
    }
}

/// Collection of object statistics
/// C++ definition: object_stat_collection_t in osd/osd_types.h
/// Version 2, wraps ObjectStatSum
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ObjectStatCollection {
    pub sum: ObjectStatSum,
}

impl ObjectStatCollection {
    pub fn is_zero(&self) -> bool {
        self.sum.is_zero()
    }
}

impl VersionedEncode for ObjectStatCollection {
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
        // Encode the sum
        self.sum.encode(buf, features)?;
        // Encode dummy u32 (legacy field)
        0u32.encode(buf, features)?;
        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        crate::denc::check_min_version!(version, 2, "ObjectStatCollection", "Quincy v17+");

        let sum = ObjectStatSum::decode(buf, features)?;
        let _dummy = u32::decode(buf, features)?; // legacy field

        Ok(ObjectStatCollection { sum })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        // ObjectStatSum (versioned header + content) + legacy u32
        Some(6 + OBJECT_STAT_SUM_ENCODED_SIZE + 4)
    }
}

impl Denc for ObjectStatCollection {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.encode_versioned(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Self::decode_versioned(buf, features)
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        // Header (6) + sum (6 + 288) + dummy u32 (4)
        let sum_size = self.sum.encoded_size(features)?;
        Some(6 + sum_size + 4)
    }
}

/// Pool statistics
/// C++ definition: pool_stat_t in osd/osd_types.h
/// Active encode always uses version 7 for Quincy+ peers.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PoolStat {
    pub stats: ObjectStatCollection,
    pub store_stats: StoreStatfs,
    pub log_size: i64,
    pub ondisk_log_size: i64,
    pub up: i32,
    pub acting: i32,
    pub num_store_stats: i32,
}

impl PoolStat {
    pub fn is_zero(&self) -> bool {
        self.stats.is_zero()
            && self.store_stats.is_zero()
            && self.log_size == 0
            && self.ondisk_log_size == 0
            && self.up == 0
            && self.acting == 0
            && self.num_store_stats == 0
    }
}

impl VersionedEncode for PoolStat {
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
    ) -> Result<(), RadosError> {
        // Encode all fields
        self.stats.encode(buf, features)?;
        self.log_size.encode(buf, features)?;
        self.ondisk_log_size.encode(buf, features)?;
        self.up.encode(buf, features)?;
        self.acting.encode(buf, features)?;
        self.store_stats.encode(buf, features)?;
        self.num_store_stats.encode(buf, features)?;
        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Quincy always emits v7; all fields are always present.
        crate::denc::check_min_version!(version, 7, "PoolStat", "Quincy v17+");
        let stats = ObjectStatCollection::decode(buf, features)?;
        let log_size = i64::decode(buf, features)?;
        let ondisk_log_size = i64::decode(buf, features)?;
        let up = i32::decode(buf, features)?;
        let acting = i32::decode(buf, features)?;
        let store_stats = StoreStatfs::decode(buf, features)?;
        let num_store_stats = i32::decode(buf, features)?;

        Ok(PoolStat {
            stats,
            store_stats,
            log_size,
            ondisk_log_size,
            up,
            acting,
            num_store_stats,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None // Complex type - size computed by encoding
    }
}

impl Denc for PoolStat {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.encode_versioned(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Self::decode_versioned(buf, features)
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        let stats_size = self.stats.encoded_size(features)?;
        let store_stats_size = self.store_stats.encoded_size(features)?;
        let content_size = stats_size + 16 + 4 + 4 + store_stats_size + 4;
        Some(6 + content_size)
    }
}

impl serde::Serialize for PoolStat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        let mut state = serializer.serialize_struct("PoolStat", 7)?;
        state.serialize_field("stat_sum", &self.stats.sum)?;
        state.serialize_field("store_stats", &self.store_stats)?;
        state.serialize_field("log_size", &self.log_size)?;
        state.serialize_field("ondisk_log_size", &self.ondisk_log_size)?;
        state.serialize_field("up", &self.up)?;
        state.serialize_field("acting", &self.acting)?;
        state.serialize_field("num_store_stats", &self.num_store_stats)?;
        state.end()
    }
}

/// Power-of-2 histogram
/// C++ definition: pow2_hist_t in common/histogram.h
/// Version 1, wraps vector<int32_t>
#[derive(Debug, Clone, PartialEq, Eq, Default, crate::VersionedDenc)]
#[denc(crate = "crate", version = 1, compat = 1)]
#[allow(dead_code)]
pub(crate) struct Pow2Hist {
    pub values: Vec<i32>,
}

/// Object store performance statistics
/// C++ definition: objectstore_perf_stat_t in osd/osd_types.h
/// Active encode always uses version 2 nanosecond counters for Quincy+ peers.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ObjectstorePerfStat {
    /// Commit latency in nanoseconds
    pub os_commit_latency_ns: u64,
    /// Apply latency in nanoseconds
    pub os_apply_latency_ns: u64,
}

impl serde::Serialize for ObjectstorePerfStat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        let mut state = serializer.serialize_struct("ObjectstorePerfStat", 4)?;
        state.serialize_field(
            "commit_latency_ms",
            &(self.os_commit_latency_ns as f64 / 1_000_000.0),
        )?;
        state.serialize_field(
            "apply_latency_ms",
            &(self.os_apply_latency_ns as f64 / 1_000_000.0),
        )?;
        state.serialize_field("commit_latency_ns", &self.os_commit_latency_ns)?;
        state.serialize_field("apply_latency_ns", &self.os_apply_latency_ns)?;
        state.end()
    }
}

impl VersionedEncode for ObjectstorePerfStat {
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
    ) -> Result<(), RadosError> {
        self.os_commit_latency_ns.encode(buf, features)?;
        self.os_apply_latency_ns.encode(buf, features)?;
        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        crate::denc::check_min_version!(version, 2, "ObjectstorePerfStat", "Quincy v17+");
        let os_commit_latency_ns = u64::decode(buf, features)?;
        let os_apply_latency_ns = u64::decode(buf, features)?;
        Ok(ObjectstorePerfStat {
            os_commit_latency_ns,
            os_apply_latency_ns,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        Some(16)
    }
}

crate::denc::impl_denc_for_versioned!(ObjectstorePerfStat);

/// OSD heartbeat interface statistics
/// C++ definition: osd_stat_t::Interfaces in osd/osd_types.h
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[allow(dead_code)]
pub(crate) struct OsdStatInterfaces {
    pub last_update: u32, // in seconds
    pub back_pingtime: [u32; 3],
    pub back_min: [u32; 3],
    pub back_max: [u32; 3],
    pub back_last: u32,
    pub front_pingtime: [u32; 3],
    pub front_min: [u32; 3],
    pub front_max: [u32; 3],
    pub front_last: u32,
}

impl Denc for OsdStatInterfaces {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.last_update.encode(buf, features)?;
        self.back_pingtime.encode(buf, features)?;
        self.back_min.encode(buf, features)?;
        self.back_max.encode(buf, features)?;
        self.back_last.encode(buf, features)?;
        self.front_pingtime.encode(buf, features)?;
        self.front_min.encode(buf, features)?;
        self.front_max.encode(buf, features)?;
        self.front_last.encode(buf, features)?;
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        let last_update = u32::decode(buf, features)?;
        let back_pingtime = <[u32; 3]>::decode(buf, features)?;
        let back_min = <[u32; 3]>::decode(buf, features)?;
        let back_max = <[u32; 3]>::decode(buf, features)?;
        let back_last = u32::decode(buf, features)?;
        let front_pingtime = <[u32; 3]>::decode(buf, features)?;
        let front_min = <[u32; 3]>::decode(buf, features)?;
        let front_max = <[u32; 3]>::decode(buf, features)?;
        let front_last = u32::decode(buf, features)?;
        Ok(OsdStatInterfaces {
            last_update,
            back_pingtime,
            back_min,
            back_max,
            back_last,
            front_pingtime,
            front_min,
            front_max,
            front_last,
        })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(OSD_STAT_INTERFACES_SIZE)
    }
}

impl FixedSize for OsdStatInterfaces {
    const SIZE: usize = OSD_STAT_INTERFACES_SIZE;
}

/// OSD statistics
/// C++ definition: osd_stat_t in osd/osd_types.h
/// Version 14, feature-dependent
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[allow(dead_code)]
pub(crate) struct OsdStat {
    pub statfs: StoreStatfs,
    pub hb_peers: Vec<i32>,
    pub snap_trim_queue_len: i32,
    pub num_snap_trimming: i32,
    pub num_shards_repaired: u64,
    pub op_queue_age_hist: Pow2Hist,
    pub os_perf_stat: ObjectstorePerfStat,
    pub os_alerts: BTreeMap<i32, BTreeMap<String, String>>,
    pub up_from: u32, // epoch_t
    pub seq: u64,
    pub num_pgs: u32,
    pub num_osds: u32,
    pub num_per_pool_osds: u32,
    pub num_per_pool_omap_osds: u32,
    pub hb_pingtime: BTreeMap<i32, OsdStatInterfaces>,
}

#[allow(dead_code)]
impl VersionedEncode for OsdStat {
    const FEATURE_DEPENDENT: bool = true;

    fn encoding_version(&self, _features: u64) -> u8 {
        14
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
        // For compatibility, encode kb/kb_used/kb_avail (computed from statfs)
        // Note: We'll use dummy values for now since we need statfs methods
        let kb: i64 = (self.statfs.total / 1024) as i64;
        let kb_used: i64 = ((self.statfs.total - self.statfs.available) / 1024) as i64;
        let kb_avail: i64 = (self.statfs.available / 1024) as i64;
        kb.encode(buf, features)?;
        kb_used.encode(buf, features)?;
        kb_avail.encode(buf, features)?;

        self.snap_trim_queue_len.encode(buf, features)?;
        self.num_snap_trimming.encode(buf, features)?;

        self.hb_peers.encode(buf, features)?;

        // Legacy field: num_hb_out (always empty vector)
        0u32.encode(buf, features)?;

        self.op_queue_age_hist.encode(buf, features)?;
        self.os_perf_stat.encode(buf, features)?;

        self.up_from.encode(buf, features)?;
        self.seq.encode(buf, features)?;
        self.num_pgs.encode(buf, features)?;

        // More compatibility fields (computed from statfs)
        let kb_used_data = self.statfs.data_stored / 1024;
        let kb_used_omap = self.statfs.omap_allocated / 1024;
        let kb_used_meta = self.statfs.internal_metadata / 1024;
        kb_used_data.encode(buf, features)?;
        kb_used_omap.encode(buf, features)?;
        kb_used_meta.encode(buf, features)?;

        self.statfs.encode(buf, features)?;

        self.os_alerts.encode(buf, features)?;

        self.num_shards_repaired.encode(buf, features)?;
        self.num_osds.encode(buf, features)?;
        self.num_per_pool_osds.encode(buf, features)?;
        self.num_per_pool_omap_osds.encode(buf, features)?;

        // Encode hb_pingtime map (BTreeMap<i32, OsdStatInterfaces> with i32 length prefix)
        (self.hb_pingtime.len() as i32).encode(buf, features)?;
        for (osd_id, interfaces) in &self.hb_pingtime {
            osd_id.encode(buf, features)?;
            interfaces.encode(buf, features)?;
        }

        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Quincy always encodes osd_stat_t at v14 (ENCODE_START(14, 2, bl)
        // in osd_types.cc). Every `if version >= N` guard below the floor
        // is always-true; decode unconditionally.
        crate::denc::check_min_version!(version, 14, "OsdStat", "Quincy v17+");

        // Legacy compatibility fields (always present; values ignored)
        let _kb = i64::decode(buf, features)?;
        let _kb_used = i64::decode(buf, features)?;
        let _kb_avail = i64::decode(buf, features)?;

        let snap_trim_queue_len = i32::decode(buf, features)?;
        let num_snap_trimming = i32::decode(buf, features)?;

        let hb_peers = Vec::<i32>::decode(buf, features)?;

        // Legacy num_hb_out vector
        let _num_hb_out = Vec::<i32>::decode(buf, features)?;

        let op_queue_age_hist = Pow2Hist::decode(buf, features)?;
        let os_perf_stat = ObjectstorePerfStat::decode(buf, features)?;
        let up_from = u32::decode(buf, features)?;
        let seq = u64::decode(buf, features)?;
        let num_pgs = u32::decode(buf, features)?;

        // v10: more legacy compatibility fields (ignored) + statfs
        let _kb_used_data = i64::decode(buf, features)?;
        let _kb_used_omap = i64::decode(buf, features)?;
        let _kb_used_meta = i64::decode(buf, features)?;
        let statfs = StoreStatfs::decode(buf, features)?;

        let os_alerts = BTreeMap::decode(buf, features)?;
        let num_shards_repaired = u64::decode(buf, features)?;
        let num_osds = u32::decode(buf, features)?;
        let num_per_pool_osds = u32::decode(buf, features)?;
        let num_per_pool_omap_osds = u32::decode(buf, features)?;

        // Decode hb_pingtime map
        let map_size: usize = i32::decode(buf, features)?
            .try_into()
            .map_err(|_| crate::RadosError::InvalidData("negative map size".into()))?;
        let mut hb_pingtime = BTreeMap::new();
        for _ in 0..map_size {
            let osd_id = i32::decode(buf, features)?;
            let interfaces = OsdStatInterfaces::decode(buf, features)?;
            hb_pingtime.insert(osd_id, interfaces);
        }

        Ok(OsdStat {
            statfs,
            hb_peers,
            snap_trim_queue_len,
            num_snap_trimming,
            num_shards_repaired,
            op_queue_age_hist,
            os_perf_stat,
            os_alerts,
            up_from,
            seq,
            num_pgs,
            num_osds,
            num_per_pool_osds,
            num_per_pool_omap_osds,
            hb_pingtime,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None // Complex type - size computed by encoding
    }
}

impl Denc for OsdStat {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.encode_versioned(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Self::decode_versioned(buf, features)
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        // This is complex due to variable-sized fields, so we'll compute it
        let mut size = 6; // ENCODE_START header

        // Compatibility fields
        size += 3 * 8; // 3 * i64

        size += 4; // snap_trim_queue_len
        size += 4; // num_snap_trimming

        size += self.hb_peers.encoded_size(features)?;
        size += 4; // num_hb_out (empty vec)

        size += self.op_queue_age_hist.encoded_size(features)?;
        size += self.os_perf_stat.encoded_size(features)?;

        size += 4; // up_from
        size += 8; // seq
        size += 4; // num_pgs

        // More compatibility fields
        size += 3 * 8; // 3 * i64

        size += self.statfs.encoded_size(features)?;
        size += self.os_alerts.encoded_size(features)?;

        size += 8; // num_shards_repaired
        size += 4; // num_osds
        size += 4; // num_per_pool_osds
        size += 4; // num_per_pool_omap_osds

        // hb_pingtime map
        size += 4; // map length
        size += self.hb_pingtime.len() * (4 + 84); // osd_id + Interfaces

        Some(size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_pg_count_roundtrip() {
        let original = PgCount {
            acting: 100,
            up_not_acting: 5,
            primary: 50,
        };

        let mut buf = BytesMut::new();
        original.encode(&mut buf, 0).unwrap();

        assert_eq!(buf.len(), 12);

        let decoded = PgCount::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_pg_count_default() {
        let pg_count = PgCount::default();
        assert_eq!(pg_count.acting, 0);
        assert_eq!(pg_count.up_not_acting, 0);
        assert_eq!(pg_count.primary, 0);
    }

    #[test]
    fn test_pg_count_insufficient_buffer() {
        let pg_count = PgCount::default();
        let buf = BytesMut::with_capacity(8);
        // Use limit to actually restrict the buffer
        let mut limited = buf.limit(8);
        assert!(pg_count.encode(&mut limited, 0).is_err());
    }

    #[test]
    fn test_pg_count_decode_insufficient() {
        let mut buf = BytesMut::new();
        buf.put_i32_le(1);
        buf.put_i32_le(2);
        // Missing third field
        assert!(PgCount::decode(&mut buf, 0).is_err());
    }

    #[test]
    fn test_store_statfs_roundtrip() {
        let original = StoreStatfs {
            total: 1000000,
            available: 500000,
            internally_reserved: 10000,
            allocated: 400000,
            data_stored: 350000,
            data_compressed: 100000,
            data_compressed_allocated: 120000,
            data_compressed_original: 150000,
            omap_allocated: 5000,
            internal_metadata: 3000,
        };

        let mut buf = BytesMut::new();
        original.encode(&mut buf, 0).unwrap();

        // Check that encoding includes version header (6 bytes) + content (80 bytes)
        assert_eq!(buf.len(), 86);

        let decoded = StoreStatfs::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_store_statfs_default() {
        let statfs = StoreStatfs::default();
        assert!(statfs.is_zero());

        let mut buf = BytesMut::new();
        statfs.encode(&mut buf, 0).unwrap();

        let decoded = StoreStatfs::decode(&mut buf, 0).unwrap();
        assert!(decoded.is_zero());
    }

    #[test]
    fn test_object_stat_sum_roundtrip() {
        let original = ObjectStatSum {
            num_bytes: 1024000,
            num_objects: 100,
            num_object_clones: 5,
            num_object_copies: 300,
            num_objects_missing_on_primary: 0,
            num_objects_degraded: 2,
            num_objects_unfound: 0,
            num_rd: 1000,
            num_rd_kb: 500,
            num_wr: 2000,
            num_wr_kb: 1000,
            num_scrub_errors: 0,
            num_objects_recovered: 10,
            num_bytes_recovered: 50000,
            num_keys_recovered: 20,
            num_shallow_scrub_errors: 0,
            num_deep_scrub_errors: 0,
            num_objects_dirty: 30,
            num_whiteouts: 0,
            num_objects_omap: 15,
            num_objects_hit_set_archive: 0,
            num_objects_misplaced: 1,
            num_bytes_hit_set_archive: 0,
            num_flush: 0,
            num_flush_kb: 0,
            num_evict: 0,
            num_evict_kb: 0,
            num_promote: 0,
            num_flush_mode_high: 0,
            num_flush_mode_low: 0,
            num_evict_mode_some: 0,
            num_evict_mode_full: 0,
            num_objects_pinned: 5,
            num_objects_missing: 0,
            num_legacy_snapsets: 3,
            num_large_omap_objects: 2,
            num_objects_manifest: 0,
            num_omap_bytes: 10000,
            num_omap_keys: 100,
            num_objects_repaired: 1,
        };

        let mut buf = BytesMut::new();
        original.encode(&mut buf, 0).unwrap();

        // Check size: header (6) + content (304 = 36*8 + 4*4)
        assert_eq!(buf.len(), 310);

        let decoded = ObjectStatSum::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_object_stat_sum_default() {
        let stats = ObjectStatSum::default();
        assert!(stats.is_zero());

        let mut buf = BytesMut::new();
        stats.encode(&mut buf, 0).unwrap();

        let decoded = ObjectStatSum::decode(&mut buf, 0).unwrap();
        assert!(decoded.is_zero());
    }

    #[test]
    fn test_object_stat_collection_roundtrip() {
        let original = ObjectStatCollection {
            sum: ObjectStatSum {
                num_bytes: 2048000,
                num_objects: 200,
                ..Default::default()
            },
        };

        let mut buf = BytesMut::new();
        original.encode(&mut buf, 0).unwrap();

        // Check size: outer header (6) + inner encoding (6 + 304) + dummy u32 (4)
        assert_eq!(buf.len(), 320);

        let decoded = ObjectStatCollection::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_object_stat_collection_default() {
        let collection = ObjectStatCollection::default();
        assert!(collection.is_zero());

        let mut buf = BytesMut::new();
        collection.encode(&mut buf, 0).unwrap();

        let decoded = ObjectStatCollection::decode(&mut buf, 0).unwrap();
        assert!(decoded.is_zero());
    }

    #[test]
    fn test_pool_stat_roundtrip() {
        let original = PoolStat {
            stats: ObjectStatCollection {
                sum: ObjectStatSum {
                    num_bytes: 1024000,
                    num_objects: 50,
                    ..Default::default()
                },
            },
            store_stats: StoreStatfs {
                total: 1000000,
                available: 500000,
                ..Default::default()
            },
            log_size: 1000,
            ondisk_log_size: 1200,
            up: 3,
            acting: 3,
            num_store_stats: 3,
        };

        let mut buf = BytesMut::new();
        original.encode(&mut buf, 0).unwrap();
        assert_eq!(&buf[..2], &[7, 5]);

        let decoded = PoolStat::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_pool_stat_default() {
        let pool_stat = PoolStat::default();
        assert!(pool_stat.is_zero());

        let mut buf = BytesMut::new();
        pool_stat.encode(&mut buf, 0).unwrap();
        assert_eq!(&buf[..2], &[7, 5]);

        let decoded = PoolStat::decode(&mut buf, 0).unwrap();
        assert!(decoded.is_zero());
    }

    #[test]
    fn test_pow2_hist_roundtrip() {
        let original = Pow2Hist {
            values: vec![1, 2, 4, 8, 16, 32],
        };

        let mut buf = BytesMut::new();
        original.encode(&mut buf, 0).unwrap();

        let decoded = Pow2Hist::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_pow2_hist_default() {
        let hist = Pow2Hist::default();
        assert!(hist.values.is_empty());

        let mut buf = BytesMut::new();
        hist.encode(&mut buf, 0).unwrap();

        let decoded = Pow2Hist::decode(&mut buf, 0).unwrap();
        assert!(decoded.values.is_empty());
    }

    #[test]
    fn test_objectstore_perf_stat_roundtrip_v2() {
        let original = ObjectstorePerfStat {
            os_commit_latency_ns: 5_000_000, // 5ms in ns
            os_apply_latency_ns: 3_000_000,  // 3ms in ns
        };

        let mut buf = BytesMut::new();
        original.encode(&mut buf, 0).unwrap();

        assert_eq!(buf.len(), 22);
        assert_eq!(&buf[..6], &[2, 2, 16, 0, 0, 0]);

        let decoded = ObjectstorePerfStat::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_objectstore_perf_stat_rejects_v1() {
        // v1 used millisecond fields; Quincy always encodes v2 (nanoseconds).
        // The check_min_version! floor now rejects v1.
        let mut buf = BytesMut::new();
        buf.put_u8(1); // version = 1
        buf.put_u8(1); // compat_version = 1
        buf.put_u32_le(8); // length
        buf.put_u32_le(5);
        buf.put_u32_le(3);

        assert!(ObjectstorePerfStat::decode(&mut buf, 0).is_err());
    }

    #[test]
    fn test_objectstore_perf_stat_default() {
        let perf_stat = ObjectstorePerfStat::default();
        assert_eq!(perf_stat.os_commit_latency_ns, 0);
        assert_eq!(perf_stat.os_apply_latency_ns, 0);

        let mut buf = BytesMut::new();
        perf_stat.encode(&mut buf, 0).unwrap();
        assert_eq!(&buf[..6], &[2, 2, 16, 0, 0, 0]);

        let decoded = ObjectstorePerfStat::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, perf_stat);
    }

    #[test]
    fn test_osd_stat_interfaces_roundtrip() {
        let original = OsdStatInterfaces {
            last_update: 123456,
            back_pingtime: [10, 20, 30],
            back_min: [5, 10, 15],
            back_max: [15, 25, 35],
            back_last: 25,
            front_pingtime: [8, 16, 24],
            front_min: [4, 8, 12],
            front_max: [12, 20, 28],
            front_last: 20,
        };

        let mut buf = BytesMut::new();
        original.encode(&mut buf, 0).unwrap();

        // 21 u32 fields = 84 bytes
        assert_eq!(buf.len(), 84);

        let decoded = OsdStatInterfaces::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_osd_stat_interfaces_default() {
        let interfaces = OsdStatInterfaces::default();
        assert_eq!(interfaces.last_update, 0);
        assert_eq!(interfaces.back_pingtime, [0, 0, 0]);
        assert_eq!(interfaces.front_last, 0);

        let mut buf = BytesMut::new();
        interfaces.encode(&mut buf, 0).unwrap();

        let decoded = OsdStatInterfaces::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, interfaces);
    }

    #[test]
    fn test_osd_stat_roundtrip() {
        let mut os_alerts = BTreeMap::new();
        let mut alert_list = BTreeMap::new();
        alert_list.insert("alert1".to_string(), "value1".to_string());
        os_alerts.insert(0, alert_list);

        let mut hb_pingtime = BTreeMap::new();
        hb_pingtime.insert(
            1,
            OsdStatInterfaces {
                last_update: 100,
                back_pingtime: [1, 2, 3],
                ..Default::default()
            },
        );

        let original = OsdStat {
            statfs: StoreStatfs {
                total: 1000000,
                available: 500000,
                ..Default::default()
            },
            hb_peers: vec![1, 2, 3],
            snap_trim_queue_len: 5,
            num_snap_trimming: 2,
            num_shards_repaired: 10,
            op_queue_age_hist: Pow2Hist {
                values: vec![1, 2, 4],
            },
            os_perf_stat: ObjectstorePerfStat {
                os_commit_latency_ns: 5_000_000, // 5ms - won't lose precision
                os_apply_latency_ns: 3_000_000,  // 3ms - won't lose precision
            },
            os_alerts,
            up_from: 100,
            seq: 200,
            num_pgs: 50,
            num_osds: 3,
            num_per_pool_osds: 3,
            num_per_pool_omap_osds: 3,
            hb_pingtime,
        };

        let mut buf = BytesMut::new();
        original.encode(&mut buf, 0).unwrap();

        let decoded = OsdStat::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_osd_stat_default() {
        let osd_stat = OsdStat::default();
        assert_eq!(osd_stat.num_pgs, 0);
        assert_eq!(osd_stat.num_osds, 0);
        assert!(osd_stat.hb_peers.is_empty());
        assert!(osd_stat.os_alerts.is_empty());

        let mut buf = BytesMut::new();
        osd_stat.encode(&mut buf, 0).unwrap();

        let decoded = OsdStat::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, osd_stat);
    }
}

// ========== Supporting types for pg_stat_t ==========

// Re-export ShardId from osdmap module (canonical definition)
use crate::osdclient::osdmap::ShardId;

/// PG shard identifier (pg_shard_t in C++)
/// Identifies a specific shard of a PG on a specific OSD
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[allow(dead_code)]
pub(crate) struct PgShard {
    pub osd: i32,
    pub shard: ShardId,
}

#[allow(dead_code)]
impl PgShard {
    /// NO_OSD constant
    pub const NO_OSD: i32 = 0x7fffffff;

    pub fn new(osd: i32, shard: ShardId) -> Self {
        PgShard { osd, shard }
    }

    pub fn is_undefined(&self) -> bool {
        self.osd == -1
    }
}

impl VersionedEncode for PgShard {
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
        self.osd.encode(buf, features)?;
        self.shard.encode(buf, features)?;
        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        crate::denc::check_min_version!(version, 1, "PgShard", "Quincy v17+");
        let osd = i32::decode(buf, features)?;
        let shard = ShardId::decode(buf, features)?;
        Ok(PgShard { osd, shard })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        Some(5) // i32 + i8
    }
}

impl Denc for PgShard {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.encode_versioned(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Self::decode_versioned(buf, features)
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(6 + 5) // 6 bytes for ENCODE_START header + 5 bytes for content (i32 + i8)
    }
}

/// Interval set (interval_set<T> in C++)
/// Represents a set of non-overlapping intervals
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[allow(dead_code)]
pub(crate) struct IntervalSet<T>
where
    T: Ord,
{
    /// Map of interval start -> length
    pub intervals: BTreeMap<T, T>,
}

impl<T> Denc for IntervalSet<T>
where
    T: Denc + Ord,
{
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.intervals.encode(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        let intervals = BTreeMap::<T, T>::decode(buf, features)?;
        Ok(IntervalSet { intervals })
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        self.intervals.encoded_size(features)
    }
}

/// Scrubbing schedule status for a PG
/// C++ definition: pg_scrubbing_status_t in osd/osd_types.h
///
/// Note: not encoded as a standalone unit — its fields are split across
/// two positions inside PgStat's versioned encoding (first 6 fields at
/// version 27+, last 3 at version 30+). Access them individually through
/// `PgStat::encode_content` / `decode_content`.
#[derive(Debug, Clone, PartialEq, Default)]
#[allow(dead_code)]
pub(crate) struct PgScrubbingStatus {
    pub scheduled_at: UTime,
    pub duration_seconds: i32,
    pub sched_status: u16,
    pub is_active: bool,
    pub is_deep: bool,
    pub is_periodic: bool,
    // Only present in PgStat encoding v30+:
    pub osd_to_respond: u16,
    pub ordinal_of_requested_replica: u8,
    pub num_to_reserve: u8,
}

/// PG statistics (pg_stat_t in C++)
/// Aggregate stats for a single placement group
/// Version 30, compat 22
#[derive(Debug, Clone, PartialEq, Default)]
#[allow(dead_code)]
pub(crate) struct PgStat {
    pub version: EVersion,
    pub reported_seq: Version,
    pub reported_epoch: Epoch,
    pub state: u64,
    pub last_fresh: UTime,
    pub last_change: UTime,
    pub last_active: UTime,
    pub last_peered: UTime,
    pub last_clean: UTime,
    pub last_unstale: UTime,
    pub last_undegraded: UTime,
    pub last_fullsized: UTime,

    pub log_start: EVersion,
    pub ondisk_log_start: EVersion,

    pub created: Epoch,
    pub last_epoch_clean: Epoch,
    pub parent: PgId,
    pub parent_split_bits: u32,

    pub last_scrub: EVersion,
    pub last_deep_scrub: EVersion,
    pub last_scrub_stamp: UTime,
    pub last_deep_scrub_stamp: UTime,
    pub last_clean_scrub_stamp: UTime,
    pub last_scrub_duration: i32,

    pub stats: ObjectStatCollection,

    pub log_size: i64,
    pub log_dups_size: i64,
    pub ondisk_log_size: i64,
    pub objects_scrubbed: i64,
    pub scrub_duration: f64,

    pub up: Vec<i32>,
    pub acting: Vec<i32>,
    pub avail_no_missing: Vec<PgShard>,
    pub object_location_counts: BTreeMap<Vec<PgShard>, i32>,
    pub mapping_epoch: Epoch,

    pub blocked_by: Vec<i32>,
    pub purged_snaps: IntervalSet<u64>,

    pub last_became_active: UTime,
    pub last_became_peered: UTime,

    pub up_primary: i32,
    pub acting_primary: i32,

    pub snaptrimq_len: u32,
    pub objects_trimmed: i64,
    pub snaptrim_duration: f64,

    pub scrub_sched_status: PgScrubbingStatus,

    // Boolean flags packed in C++ bitfields
    pub stats_invalid: bool,
    pub dirty_stats_invalid: bool,
    pub omap_stats_invalid: bool,
    pub hitset_stats_invalid: bool,
    pub hitset_bytes_stats_invalid: bool,
    pub pin_stats_invalid: bool,
    pub manifest_stats_invalid: bool,
}

impl VersionedEncode for PgStat {
    fn encoding_version(&self, _features: u64) -> u8 {
        30
    }

    fn compat_version(&self, _features: u64) -> u8 {
        22
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> Result<(), RadosError> {
        // Follow exact order from pg_stat_t::encode in osd_types.cc
        self.version.encode(buf, features)?;
        self.reported_seq.encode(buf, features)?;
        self.reported_epoch.encode(buf, features)?;

        // state is u64, but encode lower 32 bits first
        let state_lower = (self.state & 0xFFFFFFFF) as u32;
        state_lower.encode(buf, features)?;

        self.log_start.encode(buf, features)?;
        self.ondisk_log_start.encode(buf, features)?;
        self.created.encode(buf, features)?;
        self.last_epoch_clean.encode(buf, features)?;
        self.parent.encode(buf, features)?;
        self.parent_split_bits.encode(buf, features)?;
        self.last_scrub.encode(buf, features)?;
        self.last_scrub_stamp.encode(buf, features)?;
        self.stats.encode(buf, features)?;
        self.log_size.encode(buf, features)?;
        self.ondisk_log_size.encode(buf, features)?;
        self.up.encode(buf, features)?;
        self.acting.encode(buf, features)?;
        self.last_fresh.encode(buf, features)?;
        self.last_change.encode(buf, features)?;
        self.last_active.encode(buf, features)?;
        self.last_clean.encode(buf, features)?;
        self.last_unstale.encode(buf, features)?;
        self.mapping_epoch.encode(buf, features)?;
        self.last_deep_scrub.encode(buf, features)?;
        self.last_deep_scrub_stamp.encode(buf, features)?;
        self.stats_invalid.encode(buf, features)?;
        self.last_clean_scrub_stamp.encode(buf, features)?;
        self.last_became_active.encode(buf, features)?;
        self.dirty_stats_invalid.encode(buf, features)?;
        self.up_primary.encode(buf, features)?;
        self.acting_primary.encode(buf, features)?;
        self.omap_stats_invalid.encode(buf, features)?;
        self.hitset_stats_invalid.encode(buf, features)?;
        self.blocked_by.encode(buf, features)?;
        self.last_undegraded.encode(buf, features)?;
        self.last_fullsized.encode(buf, features)?;
        self.hitset_bytes_stats_invalid.encode(buf, features)?;
        self.last_peered.encode(buf, features)?;
        self.last_became_peered.encode(buf, features)?;
        self.pin_stats_invalid.encode(buf, features)?;
        self.snaptrimq_len.encode(buf, features)?;

        // Encode upper 32 bits of state
        let state_upper = (self.state >> 32) as u32;
        state_upper.encode(buf, features)?;

        self.purged_snaps.encode(buf, features)?;
        self.manifest_stats_invalid.encode(buf, features)?;
        self.avail_no_missing.encode(buf, features)?;
        self.object_location_counts.encode(buf, features)?;
        self.last_scrub_duration.encode(buf, features)?;

        // Encode pg_scrubbing_status_t fields (first 6, v27+)
        self.scrub_sched_status.scheduled_at.encode(buf, features)?;
        self.scrub_sched_status
            .duration_seconds
            .encode(buf, features)?;
        self.scrub_sched_status.sched_status.encode(buf, features)?;
        self.scrub_sched_status.is_active.encode(buf, features)?;
        self.scrub_sched_status.is_deep.encode(buf, features)?;
        self.scrub_sched_status.is_periodic.encode(buf, features)?;

        self.objects_scrubbed.encode(buf, features)?;
        self.scrub_duration.encode(buf, features)?;
        self.objects_trimmed.encode(buf, features)?;
        self.snaptrim_duration.encode(buf, features)?;
        self.log_dups_size.encode(buf, features)?;

        // Encode pg_scrubbing_status_t fields (last 3, v30+)
        self.scrub_sched_status
            .osd_to_respond
            .encode(buf, features)?;
        self.scrub_sched_status
            .ordinal_of_requested_replica
            .encode(buf, features)?;
        self.scrub_sched_status
            .num_to_reserve
            .encode(buf, features)?;

        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Quincy always emits v29; all fields decoded below are always present.
        crate::denc::check_min_version!(version, 29, "PgStat", "Quincy v17+");

        let pg_version = EVersion::decode(buf, features)?;
        let reported_seq = Version::decode(buf, features)?;
        let reported_epoch = Epoch::decode(buf, features)?;

        // Decode lower 32 bits of state
        let state_lower = u32::decode(buf, features)? as u64;

        let log_start = EVersion::decode(buf, features)?;
        let ondisk_log_start = EVersion::decode(buf, features)?;
        let created = Epoch::decode(buf, features)?;
        let last_epoch_clean = Epoch::decode(buf, features)?;
        let parent = PgId::decode(buf, features)?;
        let parent_split_bits = u32::decode(buf, features)?;
        let last_scrub = EVersion::decode(buf, features)?;
        let last_scrub_stamp = UTime::decode(buf, features)?;
        let stats = ObjectStatCollection::decode(buf, features)?;
        let log_size = i64::decode(buf, features)?;
        let ondisk_log_size = i64::decode(buf, features)?;
        let up = Vec::<i32>::decode(buf, features)?;
        let acting = Vec::<i32>::decode(buf, features)?;
        let last_fresh = UTime::decode(buf, features)?;
        let last_change = UTime::decode(buf, features)?;
        let last_active = UTime::decode(buf, features)?;
        let last_clean = UTime::decode(buf, features)?;
        let last_unstale = UTime::decode(buf, features)?;
        let mapping_epoch = Epoch::decode(buf, features)?;
        let last_deep_scrub = EVersion::decode(buf, features)?;
        let last_deep_scrub_stamp = UTime::decode(buf, features)?;
        let stats_invalid = bool::decode(buf, features)?;
        let last_clean_scrub_stamp = UTime::decode(buf, features)?;
        let last_became_active = UTime::decode(buf, features)?;
        let dirty_stats_invalid = bool::decode(buf, features)?;
        let up_primary = i32::decode(buf, features)?;
        let acting_primary = i32::decode(buf, features)?;
        let omap_stats_invalid = bool::decode(buf, features)?;
        let hitset_stats_invalid = bool::decode(buf, features)?;
        let blocked_by = Vec::<i32>::decode(buf, features)?;
        let last_undegraded = UTime::decode(buf, features)?;
        let last_fullsized = UTime::decode(buf, features)?;
        let hitset_bytes_stats_invalid = bool::decode(buf, features)?;
        let last_peered = UTime::decode(buf, features)?;
        let last_became_peered = UTime::decode(buf, features)?;
        let pin_stats_invalid = bool::decode(buf, features)?;
        let snaptrimq_len = u32::decode(buf, features)?;

        // Decode upper 32 bits of state
        let state_upper = u32::decode(buf, features)? as u64;
        let state = state_lower | (state_upper << 32);

        let purged_snaps = IntervalSet::<u64>::decode(buf, features)?;
        let manifest_stats_invalid = bool::decode(buf, features)?;
        let avail_no_missing = Vec::<PgShard>::decode(buf, features)?;
        let object_location_counts = BTreeMap::<Vec<PgShard>, i32>::decode(buf, features)?;
        let last_scrub_duration = i32::decode(buf, features)?;

        // Decode pg_scrubbing_status_t fields (first 6, v27+)
        let scrub_scheduled_at = UTime::decode(buf, features)?;
        let scrub_duration_seconds = i32::decode(buf, features)?;
        let scrub_status = u16::decode(buf, features)?;
        let scrub_is_active = bool::decode(buf, features)?;
        let scrub_is_deep = bool::decode(buf, features)?;
        let scrub_is_periodic = bool::decode(buf, features)?;

        let objects_scrubbed = i64::decode(buf, features)?;
        let scrub_duration = f64::decode(buf, features)?;
        let objects_trimmed = i64::decode(buf, features)?;
        let snaptrim_duration = f64::decode(buf, features)?;
        let log_dups_size = i64::decode(buf, features)?;

        // Decode pg_scrubbing_status_t fields (last 3, v30+)
        let scrub_osd_to_respond = u16::decode(buf, features)?;
        let scrub_ordinal_of_requested_replica = u8::decode(buf, features)?;
        let scrub_num_to_reserve = u8::decode(buf, features)?;

        Ok(PgStat {
            version: pg_version,
            reported_seq,
            reported_epoch,
            state,
            last_fresh,
            last_change,
            last_active,
            last_peered,
            last_clean,
            last_unstale,
            last_undegraded,
            last_fullsized,
            log_start,
            ondisk_log_start,
            created,
            last_epoch_clean,
            parent,
            parent_split_bits,
            last_scrub,
            last_deep_scrub,
            last_scrub_stamp,
            last_deep_scrub_stamp,
            last_clean_scrub_stamp,
            last_scrub_duration,
            stats,
            log_size,
            log_dups_size,
            ondisk_log_size,
            objects_scrubbed,
            scrub_duration,
            up,
            acting,
            avail_no_missing,
            object_location_counts,
            mapping_epoch,
            blocked_by,
            purged_snaps,
            last_became_active,
            last_became_peered,
            up_primary,
            acting_primary,
            snaptrimq_len,
            objects_trimmed,
            snaptrim_duration,
            scrub_sched_status: PgScrubbingStatus {
                scheduled_at: scrub_scheduled_at,
                duration_seconds: scrub_duration_seconds,
                sched_status: scrub_status,
                is_active: scrub_is_active,
                is_deep: scrub_is_deep,
                is_periodic: scrub_is_periodic,
                osd_to_respond: scrub_osd_to_respond,
                ordinal_of_requested_replica: scrub_ordinal_of_requested_replica,
                num_to_reserve: scrub_num_to_reserve,
            },
            stats_invalid,
            dirty_stats_invalid,
            omap_stats_invalid,
            hitset_stats_invalid,
            hitset_bytes_stats_invalid,
            pin_stats_invalid,
            manifest_stats_invalid,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None // Complex type - size computed by encoding
    }
}

crate::denc::impl_denc_for_versioned!(PgStat);

/// PGMapDigest (PGMapDigest class in C++)
/// Aggregate statistics for all PGs in the cluster
/// Version 5, compat 1
#[derive(Debug, Clone, PartialEq, Default)]
#[allow(dead_code)]
pub(crate) struct PgMapDigest {
    pub num_pg: i64,
    pub num_pg_active: i64,
    pub num_pg_unknown: i64,
    pub num_osd: i64,
    pub pg_pool_sum: BTreeMap<i32, PoolStat>,
    pub pg_sum: PoolStat,
    pub osd_sum: OsdStat,
    pub num_pg_by_state: BTreeMap<u64, i32>,
    pub num_pg_by_osd: BTreeMap<i32, PgCount>,
    pub num_pg_by_pool: BTreeMap<i64, i64>,
    pub osd_last_seq: Vec<u64>,
    pub per_pool_sum_delta: BTreeMap<i64, (PoolStat, UTime)>,
    pub per_pool_sum_deltas_stamps: BTreeMap<i64, UTime>,
    pub pg_sum_delta: PoolStat,
    pub stamp_delta: UTime,
    pub avail_space_by_rule: BTreeMap<i32, i64>,
    pub purged_snaps: BTreeMap<i64, IntervalSet<u64>>,
    pub osd_sum_by_class: BTreeMap<String, OsdStat>,
    pub pool_pg_unavailable_map: BTreeMap<u64, Vec<PgId>>,
}

impl VersionedEncode for PgMapDigest {
    const FEATURE_DEPENDENT: bool = true;

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
    ) -> Result<(), RadosError> {
        // Follow exact order from PGMapDigest::encode in PGMap.cc
        self.num_pg.encode(buf, features)?;
        self.num_pg_active.encode(buf, features)?;
        self.num_pg_unknown.encode(buf, features)?;
        self.num_osd.encode(buf, features)?;

        // Feature-dependent fields
        self.pg_pool_sum.encode(buf, features)?;
        self.pg_sum.encode(buf, features)?;
        self.osd_sum.encode(buf, features)?;

        self.num_pg_by_state.encode(buf, features)?;
        self.num_pg_by_osd.encode(buf, features)?;
        self.num_pg_by_pool.encode(buf, features)?;
        self.osd_last_seq.encode(buf, features)?;

        // Feature-dependent fields
        self.per_pool_sum_delta.encode(buf, features)?;
        self.per_pool_sum_deltas_stamps.encode(buf, features)?;
        self.pg_sum_delta.encode(buf, features)?;

        self.stamp_delta.encode(buf, features)?;
        self.avail_space_by_rule.encode(buf, features)?;
        self.purged_snaps.encode(buf, features)?;

        // Feature-dependent field
        self.osd_sum_by_class.encode(buf, features)?;

        // Version 5+ field
        self.pool_pg_unavailable_map.encode(buf, features)?;

        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Quincy's PGMapDigest::decode asserts struct_v >= 4.
        crate::denc::check_min_version!(version, 4, "PgMapDigest", "Quincy v17+");

        let num_pg = i64::decode(buf, features)?;
        let num_pg_active = i64::decode(buf, features)?;
        let num_pg_unknown = i64::decode(buf, features)?;
        let num_osd = i64::decode(buf, features)?;

        let pg_pool_sum = BTreeMap::<i32, PoolStat>::decode(buf, features)?;
        let pg_sum = PoolStat::decode(buf, features)?;
        let osd_sum = OsdStat::decode(buf, features)?;

        let num_pg_by_state = BTreeMap::<u64, i32>::decode(buf, features)?;
        let num_pg_by_osd = BTreeMap::<i32, PgCount>::decode(buf, features)?;
        let num_pg_by_pool = BTreeMap::<i64, i64>::decode(buf, features)?;
        let osd_last_seq = Vec::<u64>::decode(buf, features)?;

        let per_pool_sum_delta = BTreeMap::<i64, (PoolStat, UTime)>::decode(buf, features)?;
        let per_pool_sum_deltas_stamps = BTreeMap::<i64, UTime>::decode(buf, features)?;
        let pg_sum_delta = PoolStat::decode(buf, features)?;

        let stamp_delta = UTime::decode(buf, features)?;
        let avail_space_by_rule = BTreeMap::<i32, i64>::decode(buf, features)?;
        let purged_snaps = BTreeMap::<i64, IntervalSet<u64>>::decode(buf, features)?;

        let osd_sum_by_class = BTreeMap::<String, OsdStat>::decode(buf, features)?;

        // Version 5+ field
        let pool_pg_unavailable_map = if version >= 5 {
            BTreeMap::<u64, Vec<PgId>>::decode(buf, features)?
        } else {
            BTreeMap::new()
        };

        Ok(PgMapDigest {
            num_pg,
            num_pg_active,
            num_pg_unknown,
            num_osd,
            pg_pool_sum,
            pg_sum,
            osd_sum,
            num_pg_by_state,
            num_pg_by_osd,
            num_pg_by_pool,
            osd_last_seq,
            per_pool_sum_delta,
            per_pool_sum_deltas_stamps,
            pg_sum_delta,
            stamp_delta,
            avail_space_by_rule,
            purged_snaps,
            osd_sum_by_class,
            pool_pg_unavailable_map,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None // Complex type - size computed by encoding
    }
}

crate::denc::impl_denc_for_versioned!(PgMapDigest);

/// PGMap (PGMap class in C++)
/// The complete PG map including all PG stats
/// Version 8, compat 8
#[derive(Debug, Clone, PartialEq, Default, crate::VersionedDenc)]
#[denc(crate = "crate", version = 8, compat = 8, feature_dependent)]
#[allow(dead_code)]
pub(crate) struct PgMap {
    pub version: Version,
    pub pg_stat: BTreeMap<PgId, PgStat>,
    pub osd_stat: BTreeMap<i32, OsdStat>,
    pub last_osdmap_epoch: Epoch,
    pub last_pg_scan: Epoch,
    pub stamp: UTime,
    pub pool_statfs: BTreeMap<(i64, i32), StoreStatfs>,
}

#[cfg(test)]
mod pg_stat_support_tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_shard_id() {
        let shard = ShardId::new(5);
        assert_eq!(shard.0, 5);

        let mut buf = BytesMut::new();
        shard.encode(&mut buf, 0).unwrap();
        assert_eq!(buf.len(), 1);

        let decoded = ShardId::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, shard);
    }

    #[test]
    fn test_shard_id_no_shard() {
        let shard = ShardId::NO_SHARD;
        assert_eq!(shard.0, -1);

        let mut buf = BytesMut::new();
        shard.encode(&mut buf, 0).unwrap();

        let decoded = ShardId::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, shard);
    }

    #[test]
    fn test_pg_shard() {
        let pg_shard = PgShard::new(10, ShardId::new(2));

        let mut buf = BytesMut::new();
        pg_shard.encode(&mut buf, 0).unwrap();

        let decoded = PgShard::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, pg_shard);
        assert_eq!(decoded.osd, 10);
        assert_eq!(decoded.shard.0, 2);
    }

    #[test]
    fn test_pg_shard_undefined() {
        let pg_shard = PgShard::new(-1, ShardId::NO_SHARD);
        assert!(pg_shard.is_undefined());

        let mut buf = BytesMut::new();
        pg_shard.encode(&mut buf, 0).unwrap();

        let decoded = PgShard::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, pg_shard);
        assert!(decoded.is_undefined());
    }

    #[test]
    fn test_interval_set_u64() {
        let mut intervals = BTreeMap::new();
        intervals.insert(0u64, 10u64); // [0, 10)
        intervals.insert(20u64, 5u64); // [20, 25)

        let interval_set = IntervalSet { intervals };

        let mut buf = BytesMut::new();
        interval_set.encode(&mut buf, 0).unwrap();

        let decoded = IntervalSet::<u64>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, interval_set);
        assert_eq!(decoded.intervals.len(), 2);
    }

    #[test]
    fn test_interval_set_empty() {
        let interval_set = IntervalSet::<u64>::default();

        let mut buf = BytesMut::new();
        interval_set.encode(&mut buf, 0).unwrap();

        let decoded = IntervalSet::<u64>::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, interval_set);
        assert!(decoded.intervals.is_empty());
    }

    #[test]
    fn test_pg_stat_default() {
        let pg_stat = PgStat::default();

        let mut buf = BytesMut::new();
        pg_stat.encode(&mut buf, 0).unwrap();

        let decoded = PgStat::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, pg_stat);
        assert_eq!(decoded.state, 0);
        assert_eq!(decoded.up_primary, 0);
        assert_eq!(decoded.acting_primary, 0);
    }

    #[test]
    fn test_pg_stat_with_data() {
        use crate::osdclient::osdmap::EVersion;

        let pg_stat = PgStat {
            version: EVersion {
                version: 100,
                epoch: crate::Epoch::new(10),
            },
            reported_seq: 50,
            reported_epoch: crate::Epoch::new(10),
            state: 0x123456789ABCDEF0u64, // Test full 64-bit state
            up_primary: 1,
            acting_primary: 2,
            up: vec![1, 2, 3],
            acting: vec![2, 3, 4],
            scrub_duration: 1.5,
            snaptrim_duration: 2.5,
            ..Default::default()
        };

        let mut buf = BytesMut::new();
        pg_stat.encode(&mut buf, 0).unwrap();

        let decoded = PgStat::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded.version, pg_stat.version);
        assert_eq!(decoded.reported_seq, 50);
        assert_eq!(decoded.reported_epoch, crate::Epoch::new(10));
        assert_eq!(decoded.state, 0x123456789ABCDEF0u64);
        assert_eq!(decoded.up_primary, 1);
        assert_eq!(decoded.acting_primary, 2);
        assert_eq!(decoded.up, vec![1, 2, 3]);
        assert_eq!(decoded.acting, vec![2, 3, 4]);
        assert!((decoded.scrub_duration - 1.5).abs() < 0.0001);
        assert!((decoded.snaptrim_duration - 2.5).abs() < 0.0001);
    }

    #[test]
    fn test_pg_map_digest_default() {
        let pg_map_digest = PgMapDigest::default();

        let mut buf = BytesMut::new();
        pg_map_digest.encode(&mut buf, 0).unwrap();

        let decoded = PgMapDigest::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, pg_map_digest);
        assert_eq!(decoded.num_pg, 0);
        assert_eq!(decoded.num_osd, 0);
    }

    #[test]
    fn test_pg_map_digest_with_data() {
        let mut pg_map_digest = PgMapDigest {
            num_pg: 100,
            num_pg_active: 95,
            num_pg_unknown: 5,
            num_osd: 10,
            ..Default::default()
        };

        // Add some data to maps
        pg_map_digest.num_pg_by_state.insert(1, 50);
        pg_map_digest.num_pg_by_state.insert(2, 50);
        pg_map_digest.num_pg_by_pool.insert(1, 100);

        let mut buf = BytesMut::new();
        pg_map_digest.encode(&mut buf, 0).unwrap();

        let decoded = PgMapDigest::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded.num_pg, 100);
        assert_eq!(decoded.num_pg_active, 95);
        assert_eq!(decoded.num_pg_unknown, 5);
        assert_eq!(decoded.num_osd, 10);
        assert_eq!(decoded.num_pg_by_state.len(), 2);
        assert_eq!(decoded.num_pg_by_pool.len(), 1);
    }

    #[test]
    fn test_pg_map_default() {
        let pg_map = PgMap::default();

        let mut buf = BytesMut::new();
        pg_map.encode(&mut buf, 0).unwrap();

        let decoded = PgMap::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded, pg_map);
        assert_eq!(decoded.version, 0);
        assert!(decoded.pg_stat.is_empty());
        assert!(decoded.osd_stat.is_empty());
    }

    #[test]
    fn test_pg_map_with_data() {
        use crate::osdclient::osdmap::{EVersion, PgId, UTime};

        let pg_id = PgId { pool: 1, seed: 100 };
        let pg_stat = PgStat {
            version: EVersion {
                version: 10,
                epoch: crate::Epoch::new(5),
            },
            ..Default::default()
        };

        let mut pg_map = PgMap {
            version: 123,
            last_osdmap_epoch: crate::Epoch::new(50),
            last_pg_scan: crate::Epoch::new(49),
            stamp: UTime {
                sec: 1000,
                nsec: 500,
            },
            ..Default::default()
        };

        pg_map.pg_stat.insert(pg_id, pg_stat);

        let mut buf = BytesMut::new();
        pg_map.encode(&mut buf, 0).unwrap();

        let decoded = PgMap::decode(&mut buf, 0).unwrap();
        assert_eq!(decoded.version, 123);
        assert_eq!(decoded.last_osdmap_epoch, crate::Epoch::new(50));
        assert_eq!(decoded.last_pg_scan, crate::Epoch::new(49));
        assert_eq!(decoded.stamp.sec, 1000);
        assert_eq!(decoded.stamp.nsec, 500);
        assert_eq!(decoded.pg_stat.len(), 1);
        assert!(decoded.pg_stat.contains_key(&pg_id));
    }
}
