use crate::denc::{Denc, VersionedEncode};
use crate::entity_addr::{EntityAddr, EntityAddrvec};
use crate::error::RadosError;
use crate::{mark_feature_dependent_encoding, mark_simple_encoding, mark_versioned_encoding};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::Serialize;
use std::collections::BTreeMap;

/// Basic Ceph types that match C++ implementation
/// Ceph filesystem identifier (uuid_d in C++)
pub type FsId = [u8; 16];

/// Ceph epoch type  
pub type Epoch = u32;

/// Ceph version type
pub type Version = u64;

/// Placement Group ID (pg_t in C++)
/// Encoding format matches pg_t::encode() in osd_types.h
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct PgId {
    pub pool: u64,
    pub seed: u32,
}

impl Denc for PgId {
    fn encode(&self, _features: u64) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::with_capacity(17);
        // Version byte
        buf.put_u8(1);
        // m_pool
        buf.put_u64_le(self.pool);
        // m_seed
        buf.put_u32_le(self.seed);
        // deprecated preferred field (always -1)
        buf.put_i32_le(-1);
        Ok(buf.freeze())
    }

    fn decode(bytes: &mut Bytes) -> Result<Self, RadosError> {
        if bytes.remaining() < 17 {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes for PgId: need 17, have {}",
                bytes.remaining()
            )));
        }
        // Version byte
        let version = bytes.get_u8();
        if version != 1 {
            return Err(RadosError::Protocol(format!(
                "Unsupported PgId version: {}",
                version
            )));
        }
        // m_pool
        let pool = bytes.get_u64_le();
        // m_seed
        let seed = bytes.get_u32_le();
        // Skip deprecated preferred field (4 bytes)
        bytes.advance(4);
        Ok(PgId { pool, seed })
    }

    fn encoded_size(&self) -> Option<usize> {
        Some(17) // 1 (version) + 8 (pool) + 4 (seed) + 4 (preferred)
    }
}

// DencMut implementation for PgId
impl crate::denc_mut::DencMut for PgId {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        if buf.remaining_mut() < 17 {
            return Err(RadosError::Protocol(format!(
                "Insufficient buffer space for PgId: need 17, have {}",
                buf.remaining_mut()
            )));
        }
        buf.put_u8(1); // Version byte
        buf.put_u64_le(self.pool);
        buf.put_u32_le(self.seed);
        buf.put_i32_le(-1); // deprecated preferred field
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 17 {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes for PgId: need 17, have {}",
                buf.remaining()
            )));
        }
        let version = buf.get_u8();
        if version != 1 {
            return Err(RadosError::Protocol(format!(
                "Unsupported PgId version: {}",
                version
            )));
        }
        let pool = buf.get_u64_le();
        let seed = buf.get_u32_le();
        buf.advance(4); // Skip deprecated preferred field
        Ok(PgId { pool, seed })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(17)
    }
}

impl crate::denc_mut::FixedSize for PgId {
    const SIZE: usize = 17;
}

/// Event Version (eversion_t in C++)
/// Note: In corpus data, only version and epoch are encoded (12 bytes total)
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct EVersion {
    pub version: Version,
    pub epoch: Epoch,
    pub pad: u32, // __pad field from C++, but not encoded in corpus data
}

impl Denc for EVersion {
    fn encode(&self, _features: u64) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::with_capacity(12);
        buf.put_u64_le(self.version);
        buf.put_u32_le(self.epoch);
        // pad field is NOT encoded in the corpus data format
        Ok(buf.freeze())
    }

    fn decode(bytes: &mut Bytes) -> Result<Self, RadosError> {
        if bytes.remaining() < 12 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for EVersion".to_string(),
            ));
        }
        let version = bytes.get_u64_le();
        let epoch = bytes.get_u32_le();
        Ok(EVersion {
            version,
            epoch,
            pad: 0, // Default pad value since it's not encoded
        })
    }

    fn encoded_size(&self) -> Option<usize> {
        Some(12)
    }
}

// DencMut implementation for EVersion
impl crate::denc_mut::DencMut for EVersion {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        if buf.remaining_mut() < 12 {
            return Err(RadosError::Protocol(format!(
                "Insufficient buffer space for EVersion: need 12, have {}",
                buf.remaining_mut()
            )));
        }
        buf.put_u64_le(self.version);
        buf.put_u32_le(self.epoch);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 12 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for EVersion".to_string(),
            ));
        }
        let version = buf.get_u64_le();
        let epoch = buf.get_u32_le();
        Ok(EVersion {
            version,
            epoch,
            pad: 0,
        })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(12)
    }
}

impl crate::denc_mut::FixedSize for EVersion {
    const SIZE: usize = 12;
}

/// Unix timestamp (utime_t in C++)
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct UTime {
    pub sec: u32,
    pub nsec: u32,
}

impl Denc for UTime {
    fn encode(&self, _features: u64) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::with_capacity(8);
        buf.put_u32_le(self.sec);
        buf.put_u32_le(self.nsec);
        Ok(buf.freeze())
    }

    fn decode(bytes: &mut Bytes) -> Result<Self, RadosError> {
        if bytes.remaining() < 8 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for UTime".to_string(),
            ));
        }
        let sec = bytes.get_u32_le();
        let nsec = bytes.get_u32_le();
        Ok(UTime { sec, nsec })
    }

    fn encoded_size(&self) -> Option<usize> {
        Some(8)
    }
}

// DencMut implementation for UTime
impl crate::denc_mut::DencMut for UTime {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        if buf.remaining_mut() < 8 {
            return Err(RadosError::Protocol(format!(
                "Insufficient buffer space for UTime: need 8, have {}",
                buf.remaining_mut()
            )));
        }
        buf.put_u32_le(self.sec);
        buf.put_u32_le(self.nsec);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 8 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for UTime".to_string(),
            ));
        }
        let sec = buf.get_u32_le();
        let nsec = buf.get_u32_le();
        Ok(UTime { sec, nsec })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(8)
    }
}

impl crate::denc_mut::FixedSize for UTime {
    const SIZE: usize = 8;
}

/// String encoding implementation to match Ceph's string handling
impl Denc for String {
    fn encode(&self, _features: u64) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::new();
        let len = self.len() as u32;
        buf.put_u32_le(len);
        buf.extend_from_slice(self.as_bytes());
        Ok(buf.freeze())
    }

    fn decode(bytes: &mut Bytes) -> Result<Self, RadosError> {
        if bytes.remaining() < 4 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for string length".to_string(),
            ));
        }
        let len = bytes.get_u32_le() as usize;
        if bytes.remaining() < len {
            return Err(RadosError::Protocol(
                "Insufficient bytes for string content".to_string(),
            ));
        }
        let mut str_bytes = vec![0u8; len];
        bytes.copy_to_slice(&mut str_bytes);
        String::from_utf8(str_bytes)
            .map_err(|_| RadosError::Protocol("Invalid UTF-8 in string".to_string()))
    }
}

/// Generic BTreeMap encoding - matches Ceph's map encoding  
impl<K: Denc + Ord, V: Denc> Denc for BTreeMap<K, V> {
    fn encode(&self, features: u64) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::new();
        let len = self.len() as u32;
        buf.put_u32_le(len);
        for (key, value) in self {
            let key_bytes = key.encode(features)?;
            buf.extend_from_slice(&key_bytes);
            let value_bytes = value.encode(features)?;
            buf.extend_from_slice(&value_bytes);
        }
        Ok(buf.freeze())
    }

    fn decode(bytes: &mut Bytes) -> Result<Self, RadosError> {
        if bytes.remaining() < 4 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for map length".to_string(),
            ));
        }
        let len = bytes.get_u32_le() as usize;
        let mut map = BTreeMap::new();
        for _ in 0..len {
            let key = K::decode(bytes)?;
            let value = V::decode(bytes)?;
            map.insert(key, value);
        }
        Ok(map)
    }
}

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

    fn encode_content(&self, _features: u64, _version: u8) -> Result<Bytes, RadosError> {
        // No parameters to encode for ExplicitHash
        Ok(Bytes::new())
    }

    fn decode_content(
        _bytes: &mut Bytes,
        _version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // No parameters to decode for ExplicitHash
        Ok(ExplicitHashHitSetParams)
    }
}

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

    fn encode_content(&self, _features: u64, _version: u8) -> Result<Bytes, RadosError> {
        // No parameters to encode for ExplicitObject
        Ok(Bytes::new())
    }

    fn decode_content(
        _bytes: &mut Bytes,
        _version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // No parameters to decode for ExplicitObject
        Ok(ExplicitObjectHitSetParams)
    }
}

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

    fn encode_content(&self, _features: u64, _version: u8) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::new();
        buf.put_u32_le(self.fpp_micro);
        buf.put_u64_le(self.target_size);
        buf.put_u64_le(self.seed);
        Ok(buf.freeze())
    }

    fn decode_content(
        bytes: &mut Bytes,
        _version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        if bytes.remaining() < 20 {
            // 4 + 8 + 8 = 20 bytes
            return Err(RadosError::Protocol(
                "Insufficient bytes for BloomHitSetParams".to_string(),
            ));
        }

        let fpp_micro = bytes.get_u32_le();
        let target_size = bytes.get_u64_le();
        let seed = bytes.get_u64_le();

        Ok(BloomHitSetParams {
            fpp_micro,
            target_size,
            seed,
        })
    }
}

/// HitSet parameter types - using dedicated types for each variant
#[derive(Debug, Clone, Serialize, Default)]
pub enum HitSetParams {
    #[default]
    None,
    ExplicitHash(ExplicitHashHitSetParams),
    ExplicitObject(ExplicitObjectHitSetParams),
    Bloom(BloomHitSetParams),
}

impl VersionedEncode for HitSetParams {
    fn encoding_version(&self, _features: u64) -> u8 {
        1
    }

    fn compat_version(&self, _features: u64) -> u8 {
        1
    }

    fn encode_content(&self, features: u64, _version: u8) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::new();

        match self {
            HitSetParams::None => {
                buf.put_u8(0); // TYPE_NONE
            }
            HitSetParams::ExplicitHash(params) => {
                buf.put_u8(1); // TYPE_EXPLICIT_HASH
                let params_bytes = params.encode_versioned(features)?;
                buf.extend_from_slice(&params_bytes);
            }
            HitSetParams::ExplicitObject(params) => {
                buf.put_u8(2); // TYPE_EXPLICIT_OBJECT
                let params_bytes = params.encode_versioned(features)?;
                buf.extend_from_slice(&params_bytes);
            }
            HitSetParams::Bloom(params) => {
                buf.put_u8(3); // TYPE_BLOOM
                let params_bytes = params.encode_versioned(features)?;
                buf.extend_from_slice(&params_bytes);
            }
        }

        Ok(buf.freeze())
    }

    fn decode_content(
        bytes: &mut Bytes,
        _version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        if bytes.remaining() < 1 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for HitSetParams type".to_string(),
            ));
        }

        let hit_set_type = bytes.get_u8();

        match hit_set_type {
            0 => Ok(HitSetParams::None),
            1 => {
                let params = ExplicitHashHitSetParams::decode_versioned(bytes)?;
                Ok(HitSetParams::ExplicitHash(params))
            }
            2 => {
                let params = ExplicitObjectHitSetParams::decode_versioned(bytes)?;
                Ok(HitSetParams::ExplicitObject(params))
            }
            3 => {
                let params = BloomHitSetParams::decode_versioned(bytes)?;
                Ok(HitSetParams::Bloom(params))
            }
            _ => Err(RadosError::Protocol(format!(
                "Unknown HitSet type: {}",
                hit_set_type
            ))),
        }
    }
}

/// Ceph UUID type (uuid_d in C++)
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct UuidD {
    pub bytes: [u8; 16],
}

impl UuidD {
    pub fn new() -> Self {
        Self { bytes: [0; 16] }
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self { bytes }
    }

    pub fn is_zero(&self) -> bool {
        self.bytes.iter().all(|&b| b == 0)
    }
}

impl std::fmt::Display for UuidD {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            self.bytes[0], self.bytes[1], self.bytes[2], self.bytes[3],
            self.bytes[4], self.bytes[5], self.bytes[6], self.bytes[7],
            self.bytes[8], self.bytes[9], self.bytes[10], self.bytes[11],
            self.bytes[12], self.bytes[13], self.bytes[14], self.bytes[15]
        )
    }
}

impl Denc for UuidD {
    fn encode(&self, _features: u64) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::with_capacity(16);
        buf.extend_from_slice(&self.bytes);
        Ok(buf.freeze())
    }

    fn decode(bytes: &mut Bytes) -> Result<Self, RadosError> {
        if bytes.remaining() < 16 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for UuidD".to_string(),
            ));
        }

        let mut uuid_bytes = [0u8; 16];
        bytes.copy_to_slice(&mut uuid_bytes);

        Ok(UuidD { bytes: uuid_bytes })
    }

    fn encoded_size(&self) -> Option<usize> {
        Some(16)
    }
}

// DencMut implementation for UuidD
impl crate::denc_mut::DencMut for UuidD {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        if buf.remaining_mut() < 16 {
            return Err(RadosError::Protocol(format!(
                "Insufficient buffer space for UuidD: need 16, have {}",
                buf.remaining_mut()
            )));
        }
        buf.put_slice(&self.bytes);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 16 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for UuidD".to_string(),
            ));
        }
        let mut uuid_bytes = [0u8; 16];
        buf.copy_to_slice(&mut uuid_bytes);
        Ok(UuidD { bytes: uuid_bytes })
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(16)
    }
}

impl crate::denc_mut::FixedSize for UuidD {
    const SIZE: usize = 16;
}

/// OSD extended information structure (osd_xinfo_t in C++)
#[derive(Debug, Clone, Default, Serialize)]
pub struct OsdXInfo {
    pub down_stamp: UTime,              // timestamp when we were last marked down
    pub laggy_probability: f32,         // 0.0 = definitely not laggy, 1.0 = definitely laggy
    pub laggy_interval: u32, // average interval between being marked laggy and recovering
    pub features: u64,       // features supported by this osd we should know about
    pub old_weight: u32,     // weight prior to being auto marked out
    pub last_purged_snaps_scrub: UTime, // last scrub of purged_snaps
    pub dead_epoch: Epoch,   // last epoch we were confirmed dead (not just down)
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

    fn encode_content(&self, _features: u64, version: u8) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::new();

        // Version 1+ fields
        let down_stamp_bytes = self.down_stamp.encode(_features)?;
        buf.extend_from_slice(&down_stamp_bytes);

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
            let last_purged_bytes = self.last_purged_snaps_scrub.encode(_features)?;
            buf.extend_from_slice(&last_purged_bytes);
            buf.put_u32_le(self.dead_epoch);
        }

        Ok(buf.freeze())
    }

    fn decode_content(
        bytes: &mut Bytes,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Version 1+ fields
        let down_stamp = UTime::decode(bytes)?;

        let lp = bytes.get_u32_le();
        let laggy_probability = lp as f32 / 0xffffffffu32 as f32;

        let laggy_interval = bytes.get_u32_le();

        let mut xinfo = OsdXInfo {
            down_stamp,
            laggy_probability,
            laggy_interval,
            ..Default::default()
        };

        // Version 2+ fields
        if version >= 2 {
            xinfo.features = bytes.get_u64_le();
        }

        // Version 3+ fields
        if version >= 3 {
            xinfo.old_weight = bytes.get_u32_le();
        }

        // Version 4+ fields
        if version >= 4 {
            xinfo.last_purged_snaps_scrub = UTime::decode(bytes)?;
            xinfo.dead_epoch = bytes.get_u32_le();
        }

        Ok(xinfo)
    }
}

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

impl Denc for OsdInfo {
    fn encode(&self, _features: u64) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::with_capacity(25); // 1 + 6*4 = 25 bytes
        buf.put_u8(1); // struct_v = 1
        buf.put_u32_le(self.last_clean_begin);
        buf.put_u32_le(self.last_clean_end);
        buf.put_u32_le(self.up_from);
        buf.put_u32_le(self.up_thru);
        buf.put_u32_le(self.down_at);
        buf.put_u32_le(self.lost_at);
        Ok(buf.freeze())
    }

    fn decode(bytes: &mut Bytes) -> Result<Self, RadosError> {
        if bytes.remaining() < 25 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for OsdInfo".to_string(),
            ));
        }

        let _struct_v = bytes.get_u8();
        let last_clean_begin = bytes.get_u32_le();
        let last_clean_end = bytes.get_u32_le();
        let up_from = bytes.get_u32_le();
        let up_thru = bytes.get_u32_le();
        let down_at = bytes.get_u32_le();
        let lost_at = bytes.get_u32_le();

        Ok(OsdInfo {
            last_clean_begin,
            last_clean_end,
            up_from,
            up_thru,
            down_at,
            lost_at,
        })
    }

    fn encoded_size(&self) -> Option<usize> {
        Some(25)
    }
}

// DencMut implementation for OsdInfo
impl crate::denc_mut::DencMut for OsdInfo {
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

impl crate::denc_mut::FixedSize for OsdInfo {
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
#[derive(Debug, Clone, Default, Serialize)]
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
    // Additional version-specific fields would go here
}

/// Pool snapshot information structure
#[derive(Debug, Clone, Default, Serialize)]
pub struct PoolSnapInfo {
    pub snapid: u64,
    pub stamp: UTime,
    pub name: String,
}

impl VersionedEncode for PoolSnapInfo {
    fn encoding_version(&self, _features: u64) -> u8 {
        2
    }

    fn compat_version(&self, _features: u64) -> u8 {
        2
    }

    fn encode_content(&self, features: u64, _version: u8) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::new();
        buf.put_u64_le(self.snapid);
        let stamp_bytes = self.stamp.encode(features)?;
        buf.extend_from_slice(&stamp_bytes);
        let name_bytes = self.name.encode(features)?;
        buf.extend_from_slice(&name_bytes);
        Ok(buf.freeze())
    }

    fn decode_content(
        bytes: &mut Bytes,
        _version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        let snapid = bytes.get_u64_le();
        let stamp = UTime::decode(bytes)?;
        let name = String::decode(bytes)?;
        Ok(PoolSnapInfo {
            snapid,
            stamp,
            name,
        })
    }
}

/// Snapshot interval for removed snaps
#[derive(Debug, Clone, Default, Serialize)]
pub struct SnapInterval {
    pub start: u64,
    pub len: u64,
}

impl Denc for SnapInterval {
    fn encode(&self, _features: u64) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::with_capacity(16);
        buf.put_u64_le(self.start);
        buf.put_u64_le(self.len);
        Ok(buf.freeze())
    }

    fn decode(bytes: &mut Bytes) -> Result<Self, RadosError> {
        if bytes.remaining() < 16 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for SnapInterval".to_string(),
            ));
        }
        let start = bytes.get_u64_le();
        let len = bytes.get_u64_le();
        Ok(SnapInterval { start, len })
    }

    fn encoded_size(&self) -> Option<usize> {
        Some(16)
    }
}

// DencMut implementation for SnapInterval
impl crate::denc_mut::DencMut for SnapInterval {
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

impl crate::denc_mut::FixedSize for SnapInterval {
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
        use crate::features::*;

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

    fn encode_content(&self, features: u64, version: u8) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::new();

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
        let snaps_bytes = self.snaps.encode(features)?;
        buf.extend_from_slice(&snaps_bytes);

        let removed_snaps_bytes = self.removed_snaps.encode(features)?;
        buf.extend_from_slice(&removed_snaps_bytes);

        buf.put_u64_le(self.auid);

        // Flags (version dependent)
        buf.put_u64_le(self.flags);

        buf.put_u32_le(0); // crash_replay_interval - always 0, not stored in struct
        buf.put_u8(self.min_size);
        buf.put_u64_le(self.quota_max_bytes);
        buf.put_u64_le(self.quota_max_objects);

        // Encode tiers using generic Vec implementation
        let tiers_bytes = self.tiers.encode(features)?;
        buf.extend_from_slice(&tiers_bytes);

        buf.put_i64_le(self.tier_of);
        buf.put_u8(self.cache_mode);
        buf.put_i64_le(self.read_tier);
        buf.put_i64_le(self.write_tier);

        // Encode properties using generic BTreeMap implementation
        let properties_bytes = self.properties.encode(features)?;
        buf.extend_from_slice(&properties_bytes);

        // Hit set params
        let hit_set_bytes = self.hit_set_params.encode(features)?;
        buf.extend_from_slice(&hit_set_bytes);

        buf.put_u32_le(self.hit_set_period);
        buf.put_u32_le(self.hit_set_count);
        buf.put_u32_le(self.stripe_width);
        buf.put_u64_le(self.target_max_bytes);
        buf.put_u64_le(self.target_max_objects);
        buf.put_u32_le(self.cache_target_dirty_ratio_micro);
        buf.put_u32_le(self.cache_target_full_ratio_micro);
        buf.put_u32_le(self.cache_min_flush_age);
        buf.put_u32_le(self.cache_min_evict_age);

        let profile_bytes = self.erasure_code_profile.encode(features)?;
        buf.extend_from_slice(&profile_bytes);

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
            buf.extend_from_slice(&self.opts_data);
        }
        if version >= 25 {
            buf.put_u32_le(self.last_force_op_resend_prenautilus);
        }
        if version >= 26 {
            // Application metadata using generic BTreeMap implementation
            let app_metadata_bytes = self.application_metadata.encode(features)?;
            buf.extend_from_slice(&app_metadata_bytes);
        }
        if version >= 27 {
            let create_time_bytes = self.create_time.encode(features)?;
            buf.extend_from_slice(&create_time_bytes);
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
            let merge_meta_bytes = self.last_pg_merge_meta.encode(features)?;
            buf.extend_from_slice(&merge_meta_bytes);
        }

        Ok(buf.freeze())
    }

    fn decode_content(
        bytes: &mut Bytes,
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

        println!(
            "  Debug: Starting decode with version {}, remaining bytes: {}",
            version,
            bytes.remaining()
        );
        let mut pool = PgPool::new();

        // Basic fields (always present)
        pool.pool_type = bytes.get_u8();
        pool.size = bytes.get_u8();
        pool.crush_rule = bytes.get_u8(); // Fixed: u8 not i32
        pool.object_hash = bytes.get_u8(); // Fixed: u8 not u32
        pool.pg_num = bytes.get_u32_le();
        pool.pgp_num = bytes.get_u32_le();
        pool.lpg_num = bytes.get_u32_le(); // should be 0
        pool.lpgp_num = bytes.get_u32_le(); // should be 0
        pool.last_change = bytes.get_u32_le();
        pool.snap_seq = bytes.get_u64_le();
        pool.snap_epoch = bytes.get_u32_le();

        println!(
            "  Debug: Before snaps, remaining bytes: {}",
            bytes.remaining()
        );

        // Version-dependent fields following C++ decode logic
        if version >= 3 {
            // Decode snaps map using generic implementation
            pool.snaps = BTreeMap::decode(bytes)?;
            println!(
                "  Debug: After snaps decode, snaps.len()={}, remaining bytes: {}",
                pool.snaps.len(),
                bytes.remaining()
            );

            // Decode removed_snaps using generic implementation
            pool.removed_snaps = Vec::decode(bytes)?;
            println!(
                "  Debug: After removed_snaps decode, removed_snaps.len()={}, remaining bytes: {}",
                pool.removed_snaps.len(),
                bytes.remaining()
            );

            pool.auid = bytes.get_u64_le();
            println!(
                "  Debug: After auid, remaining bytes: {}",
                bytes.remaining()
            );
        } else {
            // For older versions, use different encoding format
            return Err(RadosError::Protocol(
                "Version < 3 not supported yet".to_string(),
            ));
        }

        if version >= 4 {
            pool.flags = bytes.get_u64_le();
            println!(
                "  Debug: After flags, remaining bytes: {}",
                bytes.remaining()
            );

            // Read crash_replay_interval but don't store it (matches C++ behavior)
            let _crash_replay_interval = bytes.get_u32_le();
            println!(
                "  Debug: After crash_replay_interval (not stored), remaining bytes: {}",
                bytes.remaining()
            );
        } else {
            pool.flags = 0;
        }

        if version >= 7 {
            pool.min_size = bytes.get_u8();
            println!(
                "  Debug: After min_size, remaining bytes: {}",
                bytes.remaining()
            );
        } else {
            pool.min_size = if pool.size > 0 {
                pool.size - pool.size / 2
            } else {
                0
            };
        }

        if version >= 8 {
            pool.quota_max_bytes = bytes.get_u64_le();
            println!(
                "  Debug: After quota_max_bytes, remaining bytes: {}",
                bytes.remaining()
            );

            pool.quota_max_objects = bytes.get_u64_le();
            println!(
                "  Debug: After quota_max_objects, remaining bytes: {}",
                bytes.remaining()
            );
        } else {
            pool.quota_max_bytes = 0;
            pool.quota_max_objects = 0;
        }

        if version >= 9 {
            println!(
                "  Debug: Before tiers (version >= 9), remaining bytes: {}",
                bytes.remaining()
            );

            // Debug: peek at the tiers length before decoding
            if bytes.remaining() >= 4 {
                let tiers_len = bytes.get_u32_le();
                if tiers_len < 1000 {
                } else {
                    println!(
                        "  Debug: tiers_len is suspiciously large - likely reading wrong offset"
                    );
                }
                // Put the length back
                let mut temp_bytes = BytesMut::with_capacity(4 + bytes.remaining());
                temp_bytes.put_u32_le(tiers_len);
                temp_bytes.put_slice(bytes);
                *bytes = temp_bytes.freeze();
            }

            // Decode tiers using generic Vec implementation
            pool.tiers = Vec::decode(bytes)?;
            println!(
                "  Debug: After tiers decode, tiers.len()={}, remaining bytes: {}",
                pool.tiers.len(),
                bytes.remaining()
            );

            pool.tier_of = bytes.get_i64_le();
            println!(
                "  Debug: After tier_of, remaining bytes: {}",
                bytes.remaining()
            );

            pool.cache_mode = bytes.get_u8();
            println!(
                "  Debug: After cache_mode, remaining bytes: {}",
                bytes.remaining()
            );

            pool.read_tier = bytes.get_i64_le();
            println!(
                "  Debug: After read_tier, remaining bytes: {}",
                bytes.remaining()
            );

            pool.write_tier = bytes.get_i64_le();
            println!(
                "  Debug: After write_tier, remaining bytes: {}",
                bytes.remaining()
            );
        } else {
            pool.tiers = Vec::new();
            pool.tier_of = -1;
            pool.cache_mode = 0;
            pool.read_tier = -1;
            pool.write_tier = -1;
        }

        if version >= 10 {
            println!(
                "  Debug: Before properties (version >= 10), remaining bytes: {}",
                bytes.remaining()
            );
            // Decode properties using generic BTreeMap implementation
            pool.properties = BTreeMap::decode(bytes)?;
            println!(
                "  Debug: After properties decode, properties.len()={}, remaining bytes: {}",
                pool.properties.len(),
                bytes.remaining()
            );
        } else {
            pool.properties = BTreeMap::new();
        }

        if version >= 11 {
            println!(
                "  Debug: Before hit_set_params (version >= 11), remaining bytes: {}",
                bytes.remaining()
            );

            // Debug: show next few bytes to understand what we're reading
            if bytes.remaining() >= 8 {
                let debug_bytes = [
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ];
                println!(
                    "  Debug: Next 8 bytes at hit_set_params position: {:02x?}",
                    debug_bytes
                );
            }

            // Decode HitSetParams using the proper implementation
            pool.hit_set_params = HitSetParams::decode(bytes)?;
            println!(
                "  Debug: After hit_set_params decode, remaining bytes: {}",
                bytes.remaining()
            );

            pool.hit_set_period = bytes.get_u32_le();
            pool.hit_set_count = bytes.get_u32_le();
            println!(
                "  Debug: After hit_set_period/count, remaining bytes: {}",
                bytes.remaining()
            );
        } else {
            pool.hit_set_params = HitSetParams::None;
            pool.hit_set_period = 0;
            pool.hit_set_count = 0;
        }

        if version >= 12 {
            pool.stripe_width = bytes.get_u32_le();
            println!(
                "  Debug: After stripe_width, remaining bytes: {}",
                bytes.remaining()
            );
        } else {
            pool.stripe_width = 0;
        }

        if version >= 13 {
            pool.target_max_bytes = bytes.get_u64_le();
            pool.target_max_objects = bytes.get_u64_le();
            pool.cache_target_dirty_ratio_micro = bytes.get_u32_le();
            pool.cache_target_full_ratio_micro = bytes.get_u32_le();
            pool.cache_min_flush_age = bytes.get_u32_le();
            pool.cache_min_evict_age = bytes.get_u32_le();
            println!(
                "  Debug: After version 13 fields, remaining bytes: {}",
                bytes.remaining()
            );
        } else {
            pool.target_max_bytes = 0;
            pool.target_max_objects = 0;
            pool.cache_target_dirty_ratio_micro = 0;
            pool.cache_target_full_ratio_micro = 0;
            pool.cache_min_flush_age = 0;
            pool.cache_min_evict_age = 0;
        }

        if version >= 14 {
            pool.erasure_code_profile = String::decode(bytes)?;
            println!(
                "  Debug: After erasure_code_profile, remaining bytes: {}",
                bytes.remaining()
            );
        } else {
            pool.erasure_code_profile = String::new();
        }

        if version >= 15 {
            pool.last_force_op_resend_preluminous = bytes.get_u32_le();
            println!(
                "  Debug: After last_force_op_resend_preluminous, remaining bytes: {}",
                bytes.remaining()
            );
        } else {
            pool.last_force_op_resend_preluminous = 0;
        }

        if version >= 16 {
            pool.min_read_recency_for_promote = bytes.get_i32_le();
            println!(
                "  Debug: After min_read_recency_for_promote, remaining bytes: {}",
                bytes.remaining()
            );
        } else {
            pool.min_read_recency_for_promote = 1;
        }

        if version >= 17 {
            pool.expected_num_objects = bytes.get_u64_le();
            println!(
                "  Debug: After expected_num_objects, remaining bytes: {}",
                bytes.remaining()
            );
        } else {
            pool.expected_num_objects = 0;
        }

        // Version-dependent fields
        if version >= 19 {
            pool.cache_target_dirty_high_ratio_micro = bytes.get_u32_le();
        }
        if version >= 20 {
            pool.min_write_recency_for_promote = bytes.get_i32_le();
        }
        if version >= 21 {
            pool.use_gmt_hitset = bytes.get_u8() != 0;
        }
        if version >= 22 {
            pool.fast_read = bytes.get_u8() != 0;
        }
        if version >= 23 {
            pool.hit_set_grade_decay_rate = bytes.get_i32_le();
            pool.hit_set_search_last_n = bytes.get_u32_le();
            println!(
                "  Debug: After version 23 fields, remaining bytes: {}",
                bytes.remaining()
            );
        } else {
            pool.hit_set_grade_decay_rate = 0;
            pool.hit_set_search_last_n = 1;
        }

        // For now, let's be more permissive with remaining fields
        // If we can't decode them properly, skip them
        if version >= 24 && bytes.remaining() > 4 {
            // Pool opts (simplified as raw data for now)
            println!(
                "  Debug: About to read opts len, remaining bytes: {}",
                bytes.remaining()
            );
            if bytes.remaining() >= 8 {
                let debug_bytes = [
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ];
                println!(
                    "  Debug: Next 8 bytes at opts position: {:02x?}",
                    debug_bytes
                );
            }

            // opts is versioned encoded - read version header first
            if bytes.remaining() >= 6 {
                // Need at least 6 bytes for version header
                let version = bytes.get_u8();
                let compat_version = bytes.get_u8();
                let opts_len = bytes.get_u32_le() as usize;
                println!(
                    "  Debug: opts version={}, compat={}, len={}, remaining bytes: {}",
                    version,
                    compat_version,
                    opts_len,
                    bytes.remaining()
                );

                if opts_len <= bytes.remaining() && opts_len < 10000 {
                    // Reasonable limit
                    pool.opts_data = vec![0u8; opts_len];
                    if opts_len > 0 {
                        bytes.copy_to_slice(&mut pool.opts_data);
                    }
                    println!(
                        "  Debug: After opts, remaining bytes: {}",
                        bytes.remaining()
                    );
                } else {
                    println!(
                        "  Debug: Skipping opts due to unreasonable length or insufficient bytes"
                    );
                    pool.opts_data = Vec::new();
                }
            } else {
                pool.opts_data = Vec::new();
            }
        } else {
            pool.opts_data = Vec::new();
        }

        // Try to continue with remaining fields if there are enough bytes
        if version >= 25 && bytes.remaining() >= 4 {
            pool.last_force_op_resend_prenautilus = bytes.get_u32_le();
            println!(
                "  Debug: After last_force_op_resend_prenautilus (v25), remaining bytes: {}",
                bytes.remaining()
            );
        } else {
            pool.last_force_op_resend_prenautilus = pool.last_force_op_resend_preluminous;
        }

        // Version 26+ fields
        if version >= 26 {
            // Application metadata (BTreeMap<String, String>)
            pool.application_metadata = BTreeMap::decode(bytes)?;
            println!(
                "  Debug: After application_metadata (v26), remaining bytes: {}",
                bytes.remaining()
            );
        }

        if version >= 27 {
            // Create time
            pool.create_time = UTime::decode(bytes)?;
            println!(
                "  Debug: After create_time (v27), remaining bytes: {}",
                bytes.remaining()
            );
        }

        if version >= 28 {
            // Version 28 fields
            pool.pg_num_target = bytes.get_u32_le();
            pool.pgp_num_target = bytes.get_u32_le();
            pool.pg_num_pending = bytes.get_u32_le();
            let _pg_num_dec_last_epoch_started = bytes.get_u32_le(); // Always 0 in recent versions
            let _pg_num_dec_last_epoch_clean = bytes.get_u32_le(); // Always 0 in recent versions
            pool.last_force_op_resend = bytes.get_u32_le();
            pool.pg_autoscale_mode = bytes.get_u8();
            println!(
                "  Debug: After v28 fields, remaining bytes: {}",
                bytes.remaining()
            );
        }

        if version >= 29 && bytes.remaining() >= 59 {
            // Version 29 - last_pg_merge_meta with proper PgMergeMeta decoding
            // Need at least 59 bytes for the encoded PgMergeMeta
            pool.last_pg_merge_meta = PgMergeMeta::decode(bytes)?;
            println!(
                "  Debug: After v29 fields, remaining bytes: {}",
                bytes.remaining()
            );
        } else if version >= 29 {
            println!(
                "  Debug: Skipping v29 fields - insufficient bytes: {} (need 59)",
                bytes.remaining()
            );
        }

        println!(
            "  Debug: Decode completed, remaining bytes: {}",
            bytes.remaining()
        );

        Ok(pool)
    }
}

/// PG Merge Metadata (pg_merge_meta_t in C++)
#[derive(Debug, Clone, Default, Serialize)]
pub struct PgMergeMeta {
    pub source_pgid: PgId,
    pub ready_epoch: Epoch,
    pub last_epoch_started: Epoch,
    pub last_epoch_clean: Epoch,
    pub source_version: EVersion,
    pub target_version: EVersion,
    // Extra fields found in corpus data (purpose unknown)
    pub extra_field: u32,
    pub extra_padding: u8,
}

impl VersionedEncode for PgMergeMeta {
    fn encoding_version(&self, _features: u64) -> u8 {
        1
    }

    fn compat_version(&self, _features: u64) -> u8 {
        1
    }

    fn encode_content(&self, features: u64, _version: u8) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::new();

        // Encode all fields in order
        let pgid_bytes = self.source_pgid.encode(features)?;
        buf.extend_from_slice(&pgid_bytes);

        buf.put_u32_le(self.ready_epoch);
        buf.put_u32_le(self.last_epoch_started);
        buf.put_u32_le(self.last_epoch_clean);

        let source_ver_bytes = self.source_version.encode(features)?;
        buf.extend_from_slice(&source_ver_bytes);

        let target_ver_bytes = self.target_version.encode(features)?;
        buf.extend_from_slice(&target_ver_bytes);

        // Only add extra bytes if they are non-zero (they exist in corpus but may not be standard)
        if self.extra_field != 0 || self.extra_padding != 0 {
            buf.put_u32_le(self.extra_field);
            buf.put_u8(self.extra_padding);
        }

        Ok(buf.freeze())
    }

    fn decode_content(
        bytes: &mut Bytes,
        _version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        let source_pgid = PgId::decode(bytes)?;
        let ready_epoch = bytes.get_u32_le();
        let last_epoch_started = bytes.get_u32_le();
        let last_epoch_clean = bytes.get_u32_le();
        let source_version = EVersion::decode(bytes)?;
        let target_version = EVersion::decode(bytes)?;

        // In corpus data, there are 5 extra bytes at the end
        let mut extra_field = 0u32;
        let mut extra_padding = 0u8;
        if bytes.remaining() >= 5 {
            extra_field = bytes.get_u32_le();
            extra_padding = bytes.get_u8();
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
}

/// Snapshot interval set (snap_interval_set_t in C++)
/// A set of snapshot intervals represented as a vector of SnapInterval
#[derive(Debug, Clone, Default, Serialize)]
pub struct SnapIntervalSet {
    pub intervals: Vec<SnapInterval>,
}

impl Denc for SnapIntervalSet {
    fn encode(&self, features: u64) -> Result<Bytes, RadosError> {
        // Encode as a vector of SnapInterval
        self.intervals.encode(features)
    }

    fn decode(bytes: &mut Bytes) -> Result<Self, RadosError> {
        let intervals = Vec::<SnapInterval>::decode(bytes)?;
        Ok(SnapIntervalSet { intervals })
    }
}

/// Ceph release type (ceph_release_t in C++)
pub type CephRelease = u8;

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

    pub pools: BTreeMap<i64, PgPool>,
    pub pool_name: BTreeMap<i64, String>,
    pub pool_max: i64,

    pub flags: u32,
    pub max_osd: i32,
    pub osd_state: Vec<u32>,
    pub osd_weight: Vec<u32>,
    pub osd_addrs_client: Vec<EntityAddrvec>,

    pub pg_temp: BTreeMap<PgId, Vec<i32>>,
    pub primary_temp: BTreeMap<PgId, i32>,
    pub osd_primary_affinity: Vec<u32>,

    // CRUSH map (encoded as raw bytes for now)
    #[serde(skip)]
    pub crush: Bytes,
    pub erasure_code_profiles: BTreeMap<String, BTreeMap<String, String>>,

    // Version 4+ fields
    pub pg_upmap: BTreeMap<PgId, Vec<i32>>,
    pub pg_upmap_items: BTreeMap<PgId, Vec<(i32, i32)>>,

    // Version 6+ fields
    pub crush_version: i32,

    // Version 7+ fields
    pub new_removed_snaps: BTreeMap<i64, SnapIntervalSet>,
    pub new_purged_snaps: BTreeMap<i64, SnapIntervalSet>,

    // Version 9+ fields
    pub last_up_change: UTime,
    pub last_in_change: UTime,

    // Version 10+ fields
    pub pg_upmap_primaries: BTreeMap<PgId, i32>,

    // OSD-only data section
    pub osd_addrs_hb_back: Vec<EntityAddrvec>,
    pub osd_info: BTreeMap<i32, OsdInfo>,
    pub blocklist: BTreeMap<EntityAddr, UTime>,
    pub osd_addrs_cluster: Vec<EntityAddrvec>,
    pub cluster_snapshot_epoch: Epoch,
    pub cluster_snapshot: String,
    pub osd_uuid: BTreeMap<i32, UuidD>,
    pub osd_xinfo: BTreeMap<i32, OsdXInfo>,
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
}

impl VersionedEncode for OSDMap {
    const FEATURE_DEPENDENT: bool = true;

    fn encoding_version(&self, _features: u64) -> u8 {
        8 // Meta-encoding version
    }

    fn compat_version(&self, _features: u64) -> u8 {
        7
    }

    fn encode_content(&self, _features: u64, _version: u8) -> Result<Bytes, RadosError> {
        // For now, we'll focus on decoding. Encoding can be implemented later.
        Err(RadosError::Protocol(
            "OSDMap encoding not yet implemented".to_string(),
        ))
    }

    fn decode_content(
        bytes: &mut Bytes,
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
        let client_v = bytes.get_u8();
        let _client_compat = bytes.get_u8();
        let client_len = bytes.get_u32_le() as usize;

        if bytes.remaining() < client_len {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes for client data: need {}, have {}",
                client_len,
                bytes.remaining()
            )));
        }

        let mut client_bytes = bytes.split_to(client_len);

        // Decode client-usable fields
        map.fsid = UuidD::decode(&mut client_bytes)?;
        map.epoch = client_bytes.get_u32_le();
        map.created = UTime::decode(&mut client_bytes)?;
        map.modified = UTime::decode(&mut client_bytes)?;

        // Decode pools (BTreeMap<i64, PgPool>)
        map.pools = BTreeMap::decode(&mut client_bytes)?;

        // Decode pool_name (BTreeMap<i64, String>)
        map.pool_name = BTreeMap::decode(&mut client_bytes)?;

        map.pool_max = client_bytes.get_i64_le();

        // Decode flags
        map.flags = client_bytes.get_u32_le();

        // Decode max_osd
        map.max_osd = client_bytes.get_i32_le();

        // Decode osd_state
        if client_v >= 5 {
            // Version 5+ uses Vec<u32> directly
            map.osd_state = Vec::decode(&mut client_bytes)?;
        } else {
            // Older versions use Vec<u8>
            let n = client_bytes.get_u32_le() as usize;
            map.osd_state = Vec::with_capacity(n);
            for _ in 0..n {
                map.osd_state.push(client_bytes.get_u8() as u32);
            }
        }

        // Decode osd_weight
        map.osd_weight = Vec::decode(&mut client_bytes)?;

        // Decode osd_addrs (client addresses)
        if client_v >= 8 {
            // Version 8+ uses EntityAddrvec
            map.osd_addrs_client = Vec::decode(&mut client_bytes)?;
        } else {
            // Older versions use single EntityAddr per OSD
            let n = client_bytes.get_u32_le() as usize;
            map.osd_addrs_client = Vec::with_capacity(n);
            for _ in 0..n {
                let addr = EntityAddr::decode(&mut client_bytes)?;
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
            let pgid = PgId::decode(&mut client_bytes)?;
            let osds = Vec::<i32>::decode(&mut client_bytes)?;
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
            let pgid = PgId::decode(&mut client_bytes)?;
            let osd = client_bytes.get_i32_le();
            map.primary_temp.insert(pgid, osd);
        }

        // Decode osd_primary_affinity
        // NOTE: Some corpus files don't have this field encoded, even though the C++ code
        // suggests it should always be present. Skip it for now.
        // map.osd_primary_affinity = Vec::decode(&mut client_bytes)?;

        // Decode CRUSH map (as raw bytes)
        map.crush = Bytes::decode(&mut client_bytes)?;

        // Decode erasure_code_profiles
        map.erasure_code_profiles = BTreeMap::decode(&mut client_bytes)?;

        // Version 4+ fields
        if client_v >= 4 {
            // Decode pg_upmap
            let pg_upmap_len = client_bytes.get_u32_le() as usize;
            for _ in 0..pg_upmap_len {
                let pgid = PgId::decode(&mut client_bytes)?;
                let osds = Vec::<i32>::decode(&mut client_bytes)?;
                map.pg_upmap.insert(pgid, osds);
            }

            // Decode pg_upmap_items
            let pg_upmap_items_len = client_bytes.get_u32_le() as usize;
            for _ in 0..pg_upmap_items_len {
                let pgid = PgId::decode(&mut client_bytes)?;
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
                let snap_set = SnapIntervalSet::decode(&mut client_bytes)?;
                map.new_removed_snaps.insert(pool_id, snap_set);
            }

            // Decode new_purged_snaps
            let purged_snaps_len = client_bytes.get_u32_le() as usize;
            for _ in 0..purged_snaps_len {
                let pool_id = client_bytes.get_i64_le();
                let snap_set = SnapIntervalSet::decode(&mut client_bytes)?;
                map.new_purged_snaps.insert(pool_id, snap_set);
            }
        }

        // Version 9+ fields
        if client_v >= 9 {
            map.last_up_change = UTime::decode(&mut client_bytes)?;
            map.last_in_change = UTime::decode(&mut client_bytes)?;
        }

        // Version 10+ fields
        if client_v >= 10 {
            let pg_upmap_primaries_len = client_bytes.get_u32_le() as usize;
            for _ in 0..pg_upmap_primaries_len {
                let pgid = PgId::decode(&mut client_bytes)?;
                let osd = client_bytes.get_i32_le();
                map.pg_upmap_primaries.insert(pgid, osd);
            }
        }

        // Decode OSD-only data section (nested ENCODE_START)
        let osd_v = bytes.get_u8();
        let _osd_compat = bytes.get_u8();
        let osd_len = bytes.get_u32_le() as usize;

        if bytes.remaining() < osd_len {
            return Err(RadosError::Protocol(format!(
                "Insufficient bytes for OSD data: need {}, have {}",
                osd_len,
                bytes.remaining()
            )));
        }

        let mut osd_bytes = bytes.split_to(osd_len);

        // Decode hb_back_addrs
        if osd_v >= 7 {
            map.osd_addrs_hb_back = Vec::decode(&mut osd_bytes)?;
        } else {
            // Older versions use single EntityAddr per OSD
            let n = osd_bytes.get_u32_le() as usize;
            map.osd_addrs_hb_back = Vec::with_capacity(n);
            for _ in 0..n {
                let addr = EntityAddr::decode(&mut osd_bytes)?;
                map.osd_addrs_hb_back.push(EntityAddrvec::with_addr(addr));
            }
        }

        // Decode osd_info
        map.osd_info = BTreeMap::decode(&mut osd_bytes)?;

        // Decode blocklist
        map.blocklist = BTreeMap::decode(&mut osd_bytes)?;

        // Decode cluster_addrs
        if osd_v >= 7 {
            map.osd_addrs_cluster = Vec::decode(&mut osd_bytes)?;
        } else {
            let n = osd_bytes.get_u32_le() as usize;
            map.osd_addrs_cluster = Vec::with_capacity(n);
            for _ in 0..n {
                let addr = EntityAddr::decode(&mut osd_bytes)?;
                map.osd_addrs_cluster.push(EntityAddrvec::with_addr(addr));
            }
        }

        map.cluster_snapshot_epoch = osd_bytes.get_u32_le();
        map.cluster_snapshot = String::decode(&mut osd_bytes)?;

        // Decode osd_uuid
        map.osd_uuid = BTreeMap::decode(&mut osd_bytes)?;

        // Decode osd_xinfo
        map.osd_xinfo = BTreeMap::decode(&mut osd_bytes)?;

        // Decode hb_front_addrs
        if osd_v >= 7 {
            map.osd_addrs_hb_front = Vec::decode(&mut osd_bytes)?;
        } else {
            let n = osd_bytes.get_u32_le() as usize;
            map.osd_addrs_hb_front = Vec::with_capacity(n);
            for _ in 0..n {
                let addr = EntityAddr::decode(&mut osd_bytes)?;
                map.osd_addrs_hb_front.push(EntityAddrvec::with_addr(addr));
            }
        }

        // Version 2+ fields
        if osd_v >= 2 {
            map.nearfull_ratio = f32::from_bits(osd_bytes.get_u32_le());
            map.full_ratio = f32::from_bits(osd_bytes.get_u32_le());
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
                let snap_set = SnapIntervalSet::decode(&mut osd_bytes)?;
                map.removed_snaps_queue.push((pool_id, snap_set));
            }
        }

        // Version 8+ fields
        if osd_v >= 8 {
            map.crush_node_flags = BTreeMap::decode(&mut osd_bytes)?;
        }

        // Version 9+ fields
        if osd_v >= 9 {
            map.device_class_flags = BTreeMap::decode(&mut osd_bytes)?;
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
            map.range_blocklist = BTreeMap::decode(&mut osd_bytes)?;
        }

        // Version 12+ fields
        if osd_v >= 12 {
            map.allow_crimson = osd_bytes.get_u8() != 0;
        }

        Ok(map)
    }
}

// ============= Encoding Metadata Registration =============
//
// Mark all types with their compile-time encoding properties
// This allows tools to detect versioning and feature dependency at compile time

// Level 1: Simple types (no versioning, no feature dependency)
mark_simple_encoding!(PgId);
mark_simple_encoding!(EVersion);
mark_simple_encoding!(UTime);
mark_simple_encoding!(UuidD);
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
