// PGMap and related types from Ceph's mon/PGMap.h

use crate::osdmap::PgId;
use bytes::{Buf, BufMut};
use denc::{Denc, EVersion, Epoch, FixedSize, RadosError, UTime, Version, VersionedEncode};

/// PG count statistics for an OSD
/// C++ definition: PGMapDigest::pg_count in mon/PGMap.h
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PgCount {
    /// Number of PGs for which this OSD is in the acting set
    pub acting: i32,
    /// Number of PGs for which this OSD is in the up set but not acting
    pub up_not_acting: i32,
    /// Number of PGs for which this OSD is the primary
    pub primary: i32,
}

impl PgCount {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Denc for PgCount {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        if buf.remaining_mut() < 12 {
            return Err(RadosError::Protocol(format!(
                "Insufficient buffer space for PgCount: need 12, have {}",
                buf.remaining_mut()
            )));
        }
        buf.put_i32_le(self.acting);
        buf.put_i32_le(self.up_not_acting);
        buf.put_i32_le(self.primary);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 12 {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes for PgCount: need 12, have {}",
                buf.remaining()
            )));
        }
        let acting = buf.get_i32_le();
        let up_not_acting = buf.get_i32_le();
        let primary = buf.get_i32_le();
        Ok(PgCount {
            acting,
            up_not_acting,
            primary,
        })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(12)
    }
}

impl FixedSize for PgCount {
    const SIZE: usize = 12;
}

/// Filesystem statistics from the object store
/// C++ definition: store_statfs_t in osd/osd_types.h
#[derive(Debug, Clone, PartialEq, Eq, Default)]
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
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_zero(&self) -> bool {
        *self == Self::default()
    }
}

impl VersionedEncode for StoreStatfs {
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
        if buf.remaining_mut() < 80 {
            return Err(RadosError::Protocol(format!(
                "Insufficient buffer space for StoreStatfs: need 80, have {}",
                buf.remaining_mut()
            )));
        }
        buf.put_u64_le(self.total);
        buf.put_u64_le(self.available);
        buf.put_u64_le(self.internally_reserved);
        buf.put_i64_le(self.allocated);
        buf.put_i64_le(self.data_stored);
        buf.put_i64_le(self.data_compressed);
        buf.put_i64_le(self.data_compressed_allocated);
        buf.put_i64_le(self.data_compressed_original);
        buf.put_i64_le(self.omap_allocated);
        buf.put_i64_le(self.internal_metadata);
        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        _features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        if version != 1 {
            return Err(RadosError::Protocol(format!(
                "Unsupported StoreStatfs version: {}",
                version
            )));
        }
        if buf.remaining() < 80 {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes for StoreStatfs: need 80, have {}",
                buf.remaining()
            )));
        }
        Ok(StoreStatfs {
            total: buf.get_u64_le(),
            available: buf.get_u64_le(),
            internally_reserved: buf.get_u64_le(),
            allocated: buf.get_i64_le(),
            data_stored: buf.get_i64_le(),
            data_compressed: buf.get_i64_le(),
            data_compressed_allocated: buf.get_i64_le(),
            data_compressed_original: buf.get_i64_le(),
            omap_allocated: buf.get_i64_le(),
            internal_metadata: buf.get_i64_le(),
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None // Complex type - size computed by encoding
    }
}

impl Denc for StoreStatfs {
    const USES_VERSIONING: bool = true;

    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.encode_versioned(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Self::decode_versioned(buf, features)
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(6 + 80) // 6 bytes for ENCODE_START header + 80 bytes for content
    }
}

/// Object statistics summary
/// C++ definition: object_stat_sum_t in osd/osd_types.h
/// Version 20, encodes 38 fields (34 i64 + 4 i32)
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ObjectStatSum {
    pub num_bytes: i64,
    pub num_objects: i64,
    pub num_object_clones: i64,
    pub num_object_copies: i64,
    pub num_objects_missing_on_primary: i64,
    pub num_objects_degraded: i64,
    pub num_objects_unfound: i64,
    pub num_rd: i64,
    pub num_rd_kb: i64,
    pub num_wr: i64,
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
    pub fn new() -> Self {
        Self::default()
    }

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
        _features: u64,
        _version: u8,
    ) -> Result<(), RadosError> {
        // Encode all 38 fields in order
        // 36 i64 fields + 4 i32 fields = 36*8 + 4*4 = 288 + 16 = 304 bytes
        if buf.remaining_mut() < 304 {
            return Err(RadosError::Protocol(format!(
                "Insufficient buffer space for ObjectStatSum: need 304, have {}",
                buf.remaining_mut()
            )));
        }

        buf.put_i64_le(self.num_bytes);
        buf.put_i64_le(self.num_objects);
        buf.put_i64_le(self.num_object_clones);
        buf.put_i64_le(self.num_object_copies);
        buf.put_i64_le(self.num_objects_missing_on_primary);
        buf.put_i64_le(self.num_objects_degraded);
        buf.put_i64_le(self.num_objects_unfound);
        buf.put_i64_le(self.num_rd);
        buf.put_i64_le(self.num_rd_kb);
        buf.put_i64_le(self.num_wr);
        buf.put_i64_le(self.num_wr_kb);
        buf.put_i64_le(self.num_scrub_errors);
        buf.put_i64_le(self.num_objects_recovered);
        buf.put_i64_le(self.num_bytes_recovered);
        buf.put_i64_le(self.num_keys_recovered);
        buf.put_i64_le(self.num_shallow_scrub_errors);
        buf.put_i64_le(self.num_deep_scrub_errors);
        buf.put_i64_le(self.num_objects_dirty);
        buf.put_i64_le(self.num_whiteouts);
        buf.put_i64_le(self.num_objects_omap);
        buf.put_i64_le(self.num_objects_hit_set_archive);
        buf.put_i64_le(self.num_objects_misplaced);
        buf.put_i64_le(self.num_bytes_hit_set_archive);
        buf.put_i64_le(self.num_flush);
        buf.put_i64_le(self.num_flush_kb);
        buf.put_i64_le(self.num_evict);
        buf.put_i64_le(self.num_evict_kb);
        buf.put_i64_le(self.num_promote);
        buf.put_i32_le(self.num_flush_mode_high);
        buf.put_i32_le(self.num_flush_mode_low);
        buf.put_i32_le(self.num_evict_mode_some);
        buf.put_i32_le(self.num_evict_mode_full);
        buf.put_i64_le(self.num_objects_pinned);
        buf.put_i64_le(self.num_objects_missing);
        buf.put_i64_le(self.num_legacy_snapsets);
        buf.put_i64_le(self.num_large_omap_objects);
        buf.put_i64_le(self.num_objects_manifest);
        buf.put_i64_le(self.num_omap_bytes);
        buf.put_i64_le(self.num_omap_keys);
        buf.put_i64_le(self.num_objects_repaired);

        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        _features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        if !(14..=20).contains(&version) {
            return Err(RadosError::Protocol(format!(
                "Unsupported ObjectStatSum version: {} (expected 14-20)",
                version
            )));
        }

        // Base fields for version 14: 28 i64 + 4 i32 + 2 i64 = 30 i64 + 4 i32 = 256 bytes
        let min_bytes = 256;
        if buf.remaining() < min_bytes {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes for ObjectStatSum v{}: need at least {}, have {}",
                version,
                min_bytes,
                buf.remaining()
            )));
        }

        let num_bytes = buf.get_i64_le();
        let num_objects = buf.get_i64_le();
        let num_object_clones = buf.get_i64_le();
        let num_object_copies = buf.get_i64_le();
        let num_objects_missing_on_primary = buf.get_i64_le();
        let num_objects_degraded = buf.get_i64_le();
        let num_objects_unfound = buf.get_i64_le();
        let num_rd = buf.get_i64_le();
        let num_rd_kb = buf.get_i64_le();
        let num_wr = buf.get_i64_le();
        let num_wr_kb = buf.get_i64_le();
        let num_scrub_errors = buf.get_i64_le();
        let num_objects_recovered = buf.get_i64_le();
        let num_bytes_recovered = buf.get_i64_le();
        let num_keys_recovered = buf.get_i64_le();
        let num_shallow_scrub_errors = buf.get_i64_le();
        let num_deep_scrub_errors = buf.get_i64_le();
        let num_objects_dirty = buf.get_i64_le();
        let num_whiteouts = buf.get_i64_le();
        let num_objects_omap = buf.get_i64_le();
        let num_objects_hit_set_archive = buf.get_i64_le();
        let num_objects_misplaced = buf.get_i64_le();
        let num_bytes_hit_set_archive = buf.get_i64_le();
        let num_flush = buf.get_i64_le();
        let num_flush_kb = buf.get_i64_le();
        let num_evict = buf.get_i64_le();
        let num_evict_kb = buf.get_i64_le();
        let num_promote = buf.get_i64_le();
        let num_flush_mode_high = buf.get_i32_le();
        let num_flush_mode_low = buf.get_i32_le();
        let num_evict_mode_some = buf.get_i32_le();
        let num_evict_mode_full = buf.get_i32_le();
        let num_objects_pinned = buf.get_i64_le();
        let num_objects_missing = buf.get_i64_le();

        // Version 16+ has num_legacy_snapsets
        let num_legacy_snapsets = if version >= 16 {
            buf.get_i64_le()
        } else {
            num_object_clones // upper bound fallback
        };

        // Version 17+ has num_large_omap_objects
        let num_large_omap_objects = if version >= 17 { buf.get_i64_le() } else { 0 };

        // Version 18+ has num_objects_manifest
        let num_objects_manifest = if version >= 18 { buf.get_i64_le() } else { 0 };

        // Version 19+ has num_omap_bytes and num_omap_keys
        let (num_omap_bytes, num_omap_keys) = if version >= 19 {
            (buf.get_i64_le(), buf.get_i64_le())
        } else {
            (0, 0)
        };

        // Version 20+ has num_objects_repaired
        let num_objects_repaired = if version >= 20 { buf.get_i64_le() } else { 0 };

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
        None // Complex type - size computed by encoding
    }
}

impl Denc for ObjectStatSum {
    const USES_VERSIONING: bool = true;

    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.encode_versioned(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Self::decode_versioned(buf, features)
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(6 + 304) // 6 bytes for ENCODE_START header + 304 bytes for content
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
    pub fn new() -> Self {
        Self::default()
    }

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
        buf.put_u32_le(0);
        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        if version != 2 {
            return Err(RadosError::Protocol(format!(
                "Unsupported ObjectStatCollection version: {} (expected 2)",
                version
            )));
        }

        let sum = ObjectStatSum::decode(buf, features)?;
        let _dummy = buf.get_u32_le(); // Ignore legacy field

        Ok(ObjectStatCollection { sum })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None // Complex type - size computed by encoding
    }
}

impl Denc for ObjectStatCollection {
    const USES_VERSIONING: bool = true;

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
/// Version 7 (with features), feature-dependent
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
    pub fn new() -> Self {
        Self::default()
    }

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
    const FEATURE_DEPENDENT: bool = true;

    fn encoding_version(&self, features: u64) -> u8 {
        // Check if CEPH_FEATURE_OSDENC is present
        const CEPH_FEATURE_OSDENC: u64 = 1 << 13;
        if (features & CEPH_FEATURE_OSDENC) != 0 {
            7 // Version 7 with ENCODE_START
        } else {
            4 // Legacy version without ENCODE_START
        }
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
        buf.put_i64_le(self.log_size);
        buf.put_i64_le(self.ondisk_log_size);
        buf.put_i32_le(self.up);
        buf.put_i32_le(self.acting);
        self.store_stats.encode(buf, features)?;
        buf.put_i32_le(self.num_store_stats);
        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        let stats = ObjectStatCollection::decode(buf, features)?;
        let log_size = buf.get_i64_le();
        let ondisk_log_size = buf.get_i64_le();

        // Version 5+ has up and acting fields
        let (up, acting) = if version >= 5 {
            (buf.get_i32_le(), buf.get_i32_le())
        } else {
            (0, 0)
        };

        // Version 6+ has store_stats
        let store_stats = if version >= 6 {
            StoreStatfs::decode(buf, features)?
        } else {
            StoreStatfs::default()
        };

        // Version 7+ has num_store_stats
        let num_store_stats = if version >= 7 { buf.get_i32_le() } else { 0 };

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
    const USES_VERSIONING: bool = true;
    const FEATURE_DEPENDENT: bool = true;

    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        const CEPH_FEATURE_OSDENC: u64 = 1 << 13;

        if (features & CEPH_FEATURE_OSDENC) == 0 {
            // Legacy encoding without ENCODE_START - version 4 only has stats + 2 i64
            buf.put_u8(4); // Version byte
            self.stats.encode(buf, features)?;
            buf.put_i64_le(self.log_size);
            buf.put_i64_le(self.ondisk_log_size);
            Ok(())
        } else {
            // Modern encoding with ENCODE_START
            self.encode_versioned(buf, features)
        }
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        const CEPH_FEATURE_OSDENC: u64 = 1 << 13;

        if (features & CEPH_FEATURE_OSDENC) == 0 {
            // Legacy decoding
            let version = buf.get_u8();
            Self::decode_content(buf, features, version, 0)
        } else {
            // Modern decoding with DECODE_START
            Self::decode_versioned(buf, features)
        }
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        const CEPH_FEATURE_OSDENC: u64 = 1 << 13;

        let stats_size = self.stats.encoded_size(features)?;
        let store_stats_size = self.store_stats.encoded_size(features)?;
        let content_size = stats_size + 16 + 4 + 4 + store_stats_size + 4; // stats + 2*i64 + 2*i32 + store_stats + i32

        if (features & CEPH_FEATURE_OSDENC) == 0 {
            // Legacy: version byte + content
            Some(1 + content_size)
        } else {
            // Modern: ENCODE_START header + content
            Some(6 + content_size)
        }
    }
}

/// Power-of-2 histogram
/// C++ definition: pow2_hist_t in common/histogram.h
/// Version 1, wraps vector<int32_t>
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Pow2Hist {
    pub values: Vec<i32>,
}

impl Pow2Hist {
    pub fn new() -> Self {
        Self::default()
    }
}

impl VersionedEncode for Pow2Hist {
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
        self.values.encode(buf, features)?;
        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        if version != 1 {
            return Err(RadosError::Protocol(format!(
                "Unsupported Pow2Hist version: {} (expected 1)",
                version
            )));
        }

        let values = Vec::<i32>::decode(buf, features)?;
        Ok(Pow2Hist { values })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None // Complex type - size computed by encoding
    }
}

impl Denc for Pow2Hist {
    const USES_VERSIONING: bool = true;

    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.encode_versioned(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Self::decode_versioned(buf, features)
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        let values_size = self.values.encoded_size(features)?;
        Some(6 + values_size) // 6 bytes for ENCODE_START header + values
    }
}

/// Object store performance statistics
/// C++ definition: objectstore_perf_stat_t in osd/osd_types.h
/// Version 2, feature-dependent (OS_PERF_STAT_NS)
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ObjectstorePerfStat {
    /// Commit latency in nanoseconds
    pub os_commit_latency_ns: u64,
    /// Apply latency in nanoseconds
    pub os_apply_latency_ns: u64,
}

impl ObjectstorePerfStat {
    pub fn new() -> Self {
        Self::default()
    }
}

impl VersionedEncode for ObjectstorePerfStat {
    const FEATURE_DEPENDENT: bool = true;

    fn encoding_version(&self, features: u64) -> u8 {
        // Check if OS_PERF_STAT_NS feature is present
        const CEPH_FEATURE_OS_PERF_STAT_NS: u64 = 1 << 34;
        if (features & CEPH_FEATURE_OS_PERF_STAT_NS) != 0 {
            2 // Version 2 with nanoseconds
        } else {
            1 // Version 1 with milliseconds
        }
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
        const CEPH_FEATURE_OS_PERF_STAT_NS: u64 = 1 << 34;

        if (features & CEPH_FEATURE_OS_PERF_STAT_NS) != 0 {
            // Version 2: encode as u64 nanoseconds
            buf.put_u64_le(self.os_commit_latency_ns);
            buf.put_u64_le(self.os_apply_latency_ns);
        } else {
            // Version 1: encode as u32 milliseconds
            let commit_ms = (self.os_commit_latency_ns / 1_000_000) as u32;
            let apply_ms = (self.os_apply_latency_ns / 1_000_000) as u32;
            buf.put_u32_le(commit_ms);
            buf.put_u32_le(apply_ms);
        }
        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        const CEPH_FEATURE_OS_PERF_STAT_NS: u64 = 1 << 34;

        if version == 2 || (features & CEPH_FEATURE_OS_PERF_STAT_NS) != 0 {
            // Version 2: decode from u64 nanoseconds
            let os_commit_latency_ns = buf.get_u64_le();
            let os_apply_latency_ns = buf.get_u64_le();
            Ok(ObjectstorePerfStat {
                os_commit_latency_ns,
                os_apply_latency_ns,
            })
        } else {
            // Version 1: decode from u32 milliseconds
            let commit_ms = buf.get_u32_le();
            let apply_ms = buf.get_u32_le();
            Ok(ObjectstorePerfStat {
                os_commit_latency_ns: commit_ms as u64 * 1_000_000,
                os_apply_latency_ns: apply_ms as u64 * 1_000_000,
            })
        }
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None // Complex type - size computed by encoding
    }
}

impl Denc for ObjectstorePerfStat {
    const USES_VERSIONING: bool = true;
    const FEATURE_DEPENDENT: bool = true;

    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.encode_versioned(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Self::decode_versioned(buf, features)
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        const CEPH_FEATURE_OS_PERF_STAT_NS: u64 = 1 << 34;
        let content_size = if (features & CEPH_FEATURE_OS_PERF_STAT_NS) != 0 {
            16 // 2 * u64
        } else {
            8 // 2 * u32
        };
        Some(6 + content_size)
    }
}

/// OSD heartbeat interface statistics
/// C++ definition: osd_stat_t::Interfaces in osd/osd_types.h
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OsdStatInterfaces {
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

impl OsdStatInterfaces {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Denc for OsdStatInterfaces {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        // 21 u32 fields = 84 bytes
        if buf.remaining_mut() < 84 {
            return Err(RadosError::Protocol(format!(
                "Insufficient buffer space for OsdStatInterfaces: need 84, have {}",
                buf.remaining_mut()
            )));
        }

        buf.put_u32_le(self.last_update);
        for &val in &self.back_pingtime {
            buf.put_u32_le(val);
        }
        for &val in &self.back_min {
            buf.put_u32_le(val);
        }
        for &val in &self.back_max {
            buf.put_u32_le(val);
        }
        buf.put_u32_le(self.back_last);
        for &val in &self.front_pingtime {
            buf.put_u32_le(val);
        }
        for &val in &self.front_min {
            buf.put_u32_le(val);
        }
        for &val in &self.front_max {
            buf.put_u32_le(val);
        }
        buf.put_u32_le(self.front_last);

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 84 {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes for OsdStatInterfaces: need 84, have {}",
                buf.remaining()
            )));
        }

        let last_update = buf.get_u32_le();
        let mut back_pingtime = [0u32; 3];
        for val in &mut back_pingtime {
            *val = buf.get_u32_le();
        }
        let mut back_min = [0u32; 3];
        for val in &mut back_min {
            *val = buf.get_u32_le();
        }
        let mut back_max = [0u32; 3];
        for val in &mut back_max {
            *val = buf.get_u32_le();
        }
        let back_last = buf.get_u32_le();
        let mut front_pingtime = [0u32; 3];
        for val in &mut front_pingtime {
            *val = buf.get_u32_le();
        }
        let mut front_min = [0u32; 3];
        for val in &mut front_min {
            *val = buf.get_u32_le();
        }
        let mut front_max = [0u32; 3];
        for val in &mut front_max {
            *val = buf.get_u32_le();
        }
        let front_last = buf.get_u32_le();

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
        Some(84) // 21 * u32
    }
}

impl FixedSize for OsdStatInterfaces {
    const SIZE: usize = 84;
}

/// OSD statistics
/// C++ definition: osd_stat_t in osd/osd_types.h
/// Version 14, feature-dependent
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OsdStat {
    pub statfs: StoreStatfs,
    pub hb_peers: Vec<i32>,
    pub snap_trim_queue_len: i32,
    pub num_snap_trimming: i32,
    pub num_shards_repaired: u64,
    pub op_queue_age_hist: Pow2Hist,
    pub os_perf_stat: ObjectstorePerfStat,
    pub os_alerts: std::collections::BTreeMap<i32, std::collections::BTreeMap<String, String>>,
    pub up_from: u32, // epoch_t
    pub seq: u64,
    pub num_pgs: u32,
    pub num_osds: u32,
    pub num_per_pool_osds: u32,
    pub num_per_pool_omap_osds: u32,
    pub hb_pingtime: std::collections::BTreeMap<i32, OsdStatInterfaces>,
}

impl OsdStat {
    pub fn new() -> Self {
        Self::default()
    }
}

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
        let kb = (self.statfs.total / 1024) as i64;
        let kb_used = ((self.statfs.total - self.statfs.available) / 1024) as i64;
        let kb_avail = (self.statfs.available / 1024) as i64;
        buf.put_i64_le(kb);
        buf.put_i64_le(kb_used);
        buf.put_i64_le(kb_avail);

        buf.put_i32_le(self.snap_trim_queue_len);
        buf.put_i32_le(self.num_snap_trimming);

        self.hb_peers.encode(buf, features)?;

        // Legacy field: num_hb_out (always empty vector)
        buf.put_u32_le(0);

        self.op_queue_age_hist.encode(buf, features)?;
        self.os_perf_stat.encode(buf, features)?;

        buf.put_u32_le(self.up_from);
        buf.put_u64_le(self.seq);
        buf.put_u32_le(self.num_pgs);

        // More compatibility fields (computed from statfs)
        let kb_used_data = self.statfs.data_stored / 1024;
        let kb_used_omap = self.statfs.omap_allocated / 1024;
        let kb_used_meta = self.statfs.internal_metadata / 1024;
        buf.put_i64_le(kb_used_data);
        buf.put_i64_le(kb_used_omap);
        buf.put_i64_le(kb_used_meta);

        self.statfs.encode(buf, features)?;

        self.os_alerts.encode(buf, features)?;

        buf.put_u64_le(self.num_shards_repaired);
        buf.put_u32_le(self.num_osds);
        buf.put_u32_le(self.num_per_pool_osds);
        buf.put_u32_le(self.num_per_pool_omap_osds);

        // Encode hb_pingtime map
        buf.put_i32_le(self.hb_pingtime.len() as i32);
        for (osd_id, interfaces) in &self.hb_pingtime {
            buf.put_i32_le(*osd_id);
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
        // Legacy compatibility fields
        let _kb = buf.get_i64_le();
        let _kb_used = buf.get_i64_le();
        let _kb_avail = buf.get_i64_le();

        let snap_trim_queue_len = buf.get_i32_le();
        let num_snap_trimming = buf.get_i32_le();

        let hb_peers = Vec::<i32>::decode(buf, features)?;

        // Legacy num_hb_out vector
        let _num_hb_out = Vec::<i32>::decode(buf, features)?;

        let op_queue_age_hist = if version >= 4 {
            Pow2Hist::decode(buf, features)?
        } else {
            Pow2Hist::default()
        };

        let os_perf_stat = if version >= 6 {
            ObjectstorePerfStat::decode(buf, features)?
        } else {
            ObjectstorePerfStat::default()
        };

        let up_from = if version >= 7 { buf.get_u32_le() } else { 0 };

        let seq = if version >= 8 { buf.get_u64_le() } else { 0 };

        let num_pgs = if version >= 9 { buf.get_u32_le() } else { 0 };

        // More compatibility fields (ignored)
        let (_kb_used_data, _kb_used_omap, _kb_used_meta) = if version >= 10 {
            (buf.get_i64_le(), buf.get_i64_le(), buf.get_i64_le())
        } else {
            (0, 0, 0)
        };

        let statfs = if version >= 10 {
            StoreStatfs::decode(buf, features)?
        } else {
            StoreStatfs::default()
        };

        let os_alerts = if version >= 11 {
            std::collections::BTreeMap::decode(buf, features)?
        } else {
            std::collections::BTreeMap::new()
        };

        let num_shards_repaired = if version >= 12 { buf.get_u64_le() } else { 0 };

        let num_osds = if version >= 13 { buf.get_u32_le() } else { 0 };

        let num_per_pool_osds = if version >= 13 { buf.get_u32_le() } else { 0 };

        let num_per_pool_omap_osds = if version >= 14 { buf.get_u32_le() } else { 0 };

        // Decode hb_pingtime map
        let hb_pingtime = if version >= 13 {
            let map_size = buf.get_i32_le() as usize;
            let mut map = std::collections::BTreeMap::new();
            for _ in 0..map_size {
                let osd_id = buf.get_i32_le();
                let interfaces = OsdStatInterfaces::decode(buf, features)?;
                map.insert(osd_id, interfaces);
            }
            map
        } else {
            std::collections::BTreeMap::new()
        };

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
    const USES_VERSIONING: bool = true;
    const FEATURE_DEPENDENT: bool = true;

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
    use std::collections::BTreeMap;

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
        const CEPH_FEATURE_OSDENC: u64 = 1 << 13;

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

        // Test with features (modern encoding)
        let mut buf = BytesMut::new();
        original.encode(&mut buf, CEPH_FEATURE_OSDENC).unwrap();

        let decoded = PoolStat::decode(&mut buf, CEPH_FEATURE_OSDENC).unwrap();
        assert_eq!(decoded, original);

        // Test without features (legacy encoding - version 4 only has stats + 2 i64)
        let mut buf_legacy = BytesMut::new();
        original.encode(&mut buf_legacy, 0).unwrap();

        let decoded_legacy = PoolStat::decode(&mut buf_legacy, 0).unwrap();
        // Version 4 doesn't include up, acting, store_stats, num_store_stats
        assert_eq!(decoded_legacy.stats, original.stats);
        assert_eq!(decoded_legacy.log_size, original.log_size);
        assert_eq!(decoded_legacy.ondisk_log_size, original.ondisk_log_size);
        assert_eq!(decoded_legacy.up, 0); // Not in version 4
        assert_eq!(decoded_legacy.acting, 0); // Not in version 4
        assert!(decoded_legacy.store_stats.is_zero()); // Not in version 4
        assert_eq!(decoded_legacy.num_store_stats, 0); // Not in version 4
    }

    #[test]
    fn test_pool_stat_default() {
        let pool_stat = PoolStat::default();
        assert!(pool_stat.is_zero());

        let mut buf = BytesMut::new();
        pool_stat.encode(&mut buf, 0).unwrap();

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
        const CEPH_FEATURE_OS_PERF_STAT_NS: u64 = 1 << 34;

        let original = ObjectstorePerfStat {
            os_commit_latency_ns: 5_000_000, // 5ms in ns
            os_apply_latency_ns: 3_000_000,  // 3ms in ns
        };

        // Test with feature flag (version 2, nanoseconds)
        let mut buf = BytesMut::new();
        original
            .encode(&mut buf, CEPH_FEATURE_OS_PERF_STAT_NS)
            .unwrap();

        let decoded = ObjectstorePerfStat::decode(&mut buf, CEPH_FEATURE_OS_PERF_STAT_NS).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_objectstore_perf_stat_roundtrip_v1() {
        let original = ObjectstorePerfStat {
            os_commit_latency_ns: 5_000_000, // 5ms in ns
            os_apply_latency_ns: 3_000_000,  // 3ms in ns
        };

        // Test without feature flag (version 1, milliseconds)
        let mut buf = BytesMut::new();
        original.encode(&mut buf, 0).unwrap();

        // Should be 6 bytes header + 8 bytes (2*u32)
        assert_eq!(buf.len(), 14);

        let decoded = ObjectstorePerfStat::decode(&mut buf, 0).unwrap();
        // Values should match within millisecond precision
        assert_eq!(decoded.os_commit_latency_ns, 5_000_000);
        assert_eq!(decoded.os_apply_latency_ns, 3_000_000);
    }

    #[test]
    fn test_objectstore_perf_stat_default() {
        let perf_stat = ObjectstorePerfStat::default();
        assert_eq!(perf_stat.os_commit_latency_ns, 0);
        assert_eq!(perf_stat.os_apply_latency_ns, 0);

        let mut buf = BytesMut::new();
        perf_stat.encode(&mut buf, 0).unwrap();

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

/// Shard ID (shard_id_t in C++)
/// Represents a shard identifier for erasure-coded pools
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ShardId(pub i8);

impl ShardId {
    /// NO_SHARD constant indicates no shard (replicated pool)
    pub const NO_SHARD: ShardId = ShardId(-1);

    pub fn new(id: i8) -> Self {
        ShardId(id)
    }
}

impl Denc for ShardId {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        if buf.remaining_mut() < 1 {
            return Err(RadosError::Protocol(
                "Insufficient buffer space for ShardId".to_string(),
            ));
        }
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

/// PG shard identifier (pg_shard_t in C++)
/// Identifies a specific shard of a PG on a specific OSD
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct PgShard {
    pub osd: i32,
    pub shard: ShardId,
}

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
        if version != 1 {
            return Err(RadosError::Protocol(format!(
                "Unsupported PgShard version: {} (expected 1)",
                version
            )));
        }
        let osd = i32::decode(buf, features)?;
        let shard = ShardId::decode(buf, features)?;
        Ok(PgShard { osd, shard })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None // Complex type - size computed by encoding
    }
}

impl Denc for PgShard {
    const USES_VERSIONING: bool = true;

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
pub struct IntervalSet<T>
where
    T: Ord,
{
    /// Map of interval start -> length
    pub intervals: std::collections::BTreeMap<T, T>,
}

impl<T> IntervalSet<T>
where
    T: Ord,
{
    pub fn new() -> Self {
        IntervalSet {
            intervals: std::collections::BTreeMap::new(),
        }
    }
}

impl<T> Denc for IntervalSet<T>
where
    T: Denc + Ord,
{
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.intervals.encode(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        let intervals = std::collections::BTreeMap::<T, T>::decode(buf, features)?;
        Ok(IntervalSet { intervals })
    }

    fn encoded_size(&self, features: u64) -> Option<usize> {
        self.intervals.encoded_size(features)
    }
}

/// PG statistics (pg_stat_t in C++)
/// Aggregate stats for a single placement group
/// Version 30, compat 22
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PgStat {
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
    pub object_location_counts: std::collections::BTreeMap<Vec<PgShard>, i32>,
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

    // pg_scrubbing_status_t fields inlined
    pub scrub_sched_scheduled_at: UTime,
    pub scrub_sched_duration_seconds: i32,
    pub scrub_sched_status: u16,
    pub scrub_sched_is_active: bool,
    pub scrub_sched_is_deep: bool,
    pub scrub_sched_is_periodic: bool,
    pub scrub_sched_osd_to_respond: u16,
    pub scrub_sched_ordinal_of_requested_replica: u8,
    pub scrub_sched_num_to_reserve: u8,

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

        // Encode pg_scrubbing_status_t fields
        self.scrub_sched_scheduled_at.encode(buf, features)?;
        self.scrub_sched_duration_seconds.encode(buf, features)?;
        self.scrub_sched_status.encode(buf, features)?;
        self.scrub_sched_is_active.encode(buf, features)?;
        self.scrub_sched_is_deep.encode(buf, features)?;
        self.scrub_sched_is_periodic.encode(buf, features)?;

        self.objects_scrubbed.encode(buf, features)?;
        buf.put_f64_le(self.scrub_duration);
        self.objects_trimmed.encode(buf, features)?;
        buf.put_f64_le(self.snaptrim_duration);
        self.log_dups_size.encode(buf, features)?;

        self.scrub_sched_osd_to_respond.encode(buf, features)?;
        self.scrub_sched_ordinal_of_requested_replica
            .encode(buf, features)?;
        self.scrub_sched_num_to_reserve.encode(buf, features)?;

        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        if !(22..=30).contains(&version) {
            return Err(RadosError::Protocol(format!(
                "Unsupported PgStat version: {} (expected 22-30)",
                version
            )));
        }

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
        let object_location_counts =
            std::collections::BTreeMap::<Vec<PgShard>, i32>::decode(buf, features)?;
        let last_scrub_duration = i32::decode(buf, features)?;

        // Decode pg_scrubbing_status_t fields
        let scrub_sched_scheduled_at = UTime::decode(buf, features)?;
        let scrub_sched_duration_seconds = i32::decode(buf, features)?;
        let scrub_sched_status = u16::decode(buf, features)?;
        let scrub_sched_is_active = bool::decode(buf, features)?;
        let scrub_sched_is_deep = bool::decode(buf, features)?;
        let scrub_sched_is_periodic = bool::decode(buf, features)?;

        let objects_scrubbed = i64::decode(buf, features)?;
        let scrub_duration = buf.get_f64_le();
        let objects_trimmed = i64::decode(buf, features)?;
        let snaptrim_duration = buf.get_f64_le();
        let log_dups_size = i64::decode(buf, features)?;

        let scrub_sched_osd_to_respond = u16::decode(buf, features)?;
        let scrub_sched_ordinal_of_requested_replica = u8::decode(buf, features)?;
        let scrub_sched_num_to_reserve = u8::decode(buf, features)?;

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
            scrub_sched_scheduled_at,
            scrub_sched_duration_seconds,
            scrub_sched_status,
            scrub_sched_is_active,
            scrub_sched_is_deep,
            scrub_sched_is_periodic,
            scrub_sched_osd_to_respond,
            scrub_sched_ordinal_of_requested_replica,
            scrub_sched_num_to_reserve,
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

impl Denc for PgStat {
    const USES_VERSIONING: bool = true;

    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.encode_versioned(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Self::decode_versioned(buf, features)
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        None // Variable size due to vectors and maps
    }
}

/// PGMapDigest (PGMapDigest class in C++)
/// Aggregate statistics for all PGs in the cluster
/// Version 5, compat 1
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PgMapDigest {
    pub num_pg: i64,
    pub num_pg_active: i64,
    pub num_pg_unknown: i64,
    pub num_osd: i64,
    pub pg_pool_sum: std::collections::BTreeMap<i32, PoolStat>,
    pub pg_sum: PoolStat,
    pub osd_sum: OsdStat,
    pub num_pg_by_state: std::collections::BTreeMap<u64, i32>,
    pub num_pg_by_osd: std::collections::BTreeMap<i32, PgCount>,
    pub num_pg_by_pool: std::collections::BTreeMap<i64, i64>,
    pub osd_last_seq: Vec<u64>,
    pub per_pool_sum_delta: std::collections::BTreeMap<i64, (PoolStat, UTime)>,
    pub per_pool_sum_deltas_stamps: std::collections::BTreeMap<i64, UTime>,
    pub pg_sum_delta: PoolStat,
    pub stamp_delta: UTime,
    pub avail_space_by_rule: std::collections::BTreeMap<i32, i64>,
    pub purged_snaps: std::collections::BTreeMap<i64, IntervalSet<u64>>,
    pub osd_sum_by_class: std::collections::BTreeMap<String, OsdStat>,
    pub pool_pg_unavailable_map: std::collections::BTreeMap<u64, Vec<PgId>>,
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
        if !(1..=5).contains(&version) {
            return Err(RadosError::Protocol(format!(
                "Unsupported PgMapDigest version: {} (expected 1-5)",
                version
            )));
        }

        let num_pg = i64::decode(buf, features)?;
        let num_pg_active = i64::decode(buf, features)?;
        let num_pg_unknown = i64::decode(buf, features)?;
        let num_osd = i64::decode(buf, features)?;

        let pg_pool_sum = std::collections::BTreeMap::<i32, PoolStat>::decode(buf, features)?;
        let pg_sum = PoolStat::decode(buf, features)?;
        let osd_sum = OsdStat::decode(buf, features)?;

        let num_pg_by_state = std::collections::BTreeMap::<u64, i32>::decode(buf, features)?;
        let num_pg_by_osd = std::collections::BTreeMap::<i32, PgCount>::decode(buf, features)?;
        let num_pg_by_pool = std::collections::BTreeMap::<i64, i64>::decode(buf, features)?;
        let osd_last_seq = Vec::<u64>::decode(buf, features)?;

        let per_pool_sum_delta =
            std::collections::BTreeMap::<i64, (PoolStat, UTime)>::decode(buf, features)?;
        let per_pool_sum_deltas_stamps =
            std::collections::BTreeMap::<i64, UTime>::decode(buf, features)?;
        let pg_sum_delta = PoolStat::decode(buf, features)?;

        let stamp_delta = UTime::decode(buf, features)?;
        let avail_space_by_rule = std::collections::BTreeMap::<i32, i64>::decode(buf, features)?;
        let purged_snaps =
            std::collections::BTreeMap::<i64, IntervalSet<u64>>::decode(buf, features)?;

        let osd_sum_by_class =
            std::collections::BTreeMap::<String, OsdStat>::decode(buf, features)?;

        // Version 5+ field
        let pool_pg_unavailable_map = if version >= 5 {
            std::collections::BTreeMap::<u64, Vec<PgId>>::decode(buf, features)?
        } else {
            std::collections::BTreeMap::new()
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

impl Denc for PgMapDigest {
    const USES_VERSIONING: bool = true;

    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.encode_versioned(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Self::decode_versioned(buf, features)
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        None // Variable size due to maps and vectors
    }
}

/// PGMap (PGMap class in C++)
/// The complete PG map including all PG stats
/// Version 8, compat 8
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PgMap {
    pub version: Version,
    pub pg_stat: std::collections::BTreeMap<PgId, PgStat>,
    pub osd_stat: std::collections::BTreeMap<i32, OsdStat>,
    pub last_osdmap_epoch: Epoch,
    pub last_pg_scan: Epoch,
    pub stamp: UTime,
    pub pool_statfs: std::collections::BTreeMap<(i64, i32), StoreStatfs>,
}

impl VersionedEncode for PgMap {
    const FEATURE_DEPENDENT: bool = true;

    fn encoding_version(&self, _features: u64) -> u8 {
        8
    }

    fn compat_version(&self, _features: u64) -> u8 {
        8
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        _version: u8,
    ) -> Result<(), RadosError> {
        // Follow exact order from PGMap::encode in PGMap.cc
        self.version.encode(buf, features)?;
        self.pg_stat.encode(buf, features)?;
        self.osd_stat.encode(buf, features)?; // Feature-dependent
        self.last_osdmap_epoch.encode(buf, features)?;
        self.last_pg_scan.encode(buf, features)?;
        self.stamp.encode(buf, features)?;
        self.pool_statfs.encode(buf, features)?; // Feature-dependent
        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        if version != 8 {
            return Err(RadosError::Protocol(format!(
                "Unsupported PgMap version: {} (expected 8)",
                version
            )));
        }

        let pg_version = Version::decode(buf, features)?;
        let pg_stat = std::collections::BTreeMap::<PgId, PgStat>::decode(buf, features)?;
        let osd_stat = std::collections::BTreeMap::<i32, OsdStat>::decode(buf, features)?;
        let last_osdmap_epoch = Epoch::decode(buf, features)?;
        let last_pg_scan = Epoch::decode(buf, features)?;
        let stamp = UTime::decode(buf, features)?;
        let pool_statfs =
            std::collections::BTreeMap::<(i64, i32), StoreStatfs>::decode(buf, features)?;

        Ok(PgMap {
            version: pg_version,
            pg_stat,
            osd_stat,
            last_osdmap_epoch,
            last_pg_scan,
            stamp,
            pool_statfs,
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        None // Complex type - size computed by encoding
    }
}

impl Denc for PgMap {
    const USES_VERSIONING: bool = true;

    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        self.encode_versioned(buf, features)
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Self::decode_versioned(buf, features)
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        None // Variable size due to maps
    }
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
        let mut intervals = std::collections::BTreeMap::new();
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
        let interval_set = IntervalSet::<u64>::new();

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
        use crate::osdmap::EVersion;

        let pg_stat = PgStat {
            version: EVersion {
                version: 100,
                epoch: 10,
            },
            reported_seq: 50,
            reported_epoch: 10,
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
        assert_eq!(decoded.reported_epoch, 10);
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
        use crate::osdmap::{EVersion, PgId, UTime};

        let pg_id = PgId { pool: 1, seed: 100 };
        let pg_stat = PgStat {
            version: EVersion {
                version: 10,
                epoch: 5,
            },
            ..Default::default()
        };

        let mut pg_map = PgMap {
            version: 123,
            last_osdmap_epoch: 50,
            last_pg_scan: 49,
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
        assert_eq!(decoded.last_osdmap_epoch, 50);
        assert_eq!(decoded.last_pg_scan, 49);
        assert_eq!(decoded.stamp.sec, 1000);
        assert_eq!(decoded.stamp.nsec, 500);
        assert_eq!(decoded.pg_stat.len(), 1);
        assert!(decoded.pg_stat.contains_key(&pg_id));
    }
}
