//! MonMap and related types
//!
//! This module implements the Ceph monitor map encoding/decoding.
//! See MonMap.h and MonMap.cc in the Ceph source for the C++ implementation.

use crate::denc::{Denc, VersionedEncode};
use crate::entity_addr::EntityAddrvec;
use crate::error::RadosError;
use bytes::{Buf, BufMut};
use serde::Serialize;
use std::collections::BTreeMap;

// Import specific types from types module
use crate::types::{Epoch, FsId, UTime};

/// Monitor feature flags (mon_feature_t in C++)
/// Uses versioned encoding (ENCODE_START/DECODE_START)
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct MonFeature {
    pub features: u64,
}

// MonFeature content size - directly tied to the field type
const MON_FEATURE_CONTENT_SIZE: usize = std::mem::size_of::<u64>(); // features field

impl VersionedEncode for MonFeature {
    const FEATURE_DEPENDENT: bool = false;

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
        if buf.remaining_mut() < 8 {
            return Err(RadosError::Protocol(format!(
                "Insufficient buffer space for MonFeature: need 8, have {}",
                buf.remaining_mut()
            )));
        }
        buf.put_u64_le(self.features);
        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        _features: u64,
        _version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        if buf.remaining() < 8 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for MonFeature".to_string(),
            ));
        }
        Ok(MonFeature {
            features: buf.get_u64_le(),
        })
    }

    fn encoded_size_content(&self, _features: u64, _version: u8) -> Option<usize> {
        Some(MON_FEATURE_CONTENT_SIZE)
    }
}

crate::impl_denc_for_versioned!(MonFeature);

/// Ceph release version for MonMap (ceph_release_t in C++)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[repr(u8)]
pub enum MonCephRelease {
    #[default]
    Unknown = 0,
    Argonaut = 1,
    Bobtail = 2,
    Cuttlefish = 3,
    Dumpling = 4,
    Emperor = 5,
    Firefly = 6,
    Giant = 7,
    Hammer = 8,
    Infernalis = 9,
    Jewel = 10,
    Kraken = 11,
    Luminous = 12,
    Mimic = 13,
    Nautilus = 14,
    Octopus = 15,
    Pacific = 16,
    Quincy = 17,
    Reef = 18,
    Squid = 19,
    MAX = 20,
}

impl From<u8> for MonCephRelease {
    fn from(v: u8) -> Self {
        match v {
            1 => MonCephRelease::Argonaut,
            2 => MonCephRelease::Bobtail,
            3 => MonCephRelease::Cuttlefish,
            4 => MonCephRelease::Dumpling,
            5 => MonCephRelease::Emperor,
            6 => MonCephRelease::Firefly,
            7 => MonCephRelease::Giant,
            8 => MonCephRelease::Hammer,
            9 => MonCephRelease::Infernalis,
            10 => MonCephRelease::Jewel,
            11 => MonCephRelease::Kraken,
            12 => MonCephRelease::Luminous,
            13 => MonCephRelease::Mimic,
            14 => MonCephRelease::Nautilus,
            15 => MonCephRelease::Octopus,
            16 => MonCephRelease::Pacific,
            17 => MonCephRelease::Quincy,
            18 => MonCephRelease::Reef,
            19 => MonCephRelease::Squid,
            20 => MonCephRelease::MAX,
            _ => MonCephRelease::Unknown,
        }
    }
}

impl Denc for MonCephRelease {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        if buf.remaining_mut() < 1 {
            return Err(RadosError::Protocol(
                "Insufficient buffer space for MonCephRelease".to_string(),
            ));
        }
        buf.put_u8(*self as u8);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 1 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for MonCephRelease".to_string(),
            ));
        }
        Ok(MonCephRelease::from(buf.get_u8()))
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(1)
    }
}

impl crate::denc::FixedSize for MonCephRelease {
    const SIZE: usize = 1;
}

/// Election strategy (election_strategy in C++)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[repr(u8)]
pub enum ElectionStrategy {
    #[default]
    Classic = 1,
    Disallow = 2,
    Connectivity = 3,
}

impl From<u8> for ElectionStrategy {
    fn from(v: u8) -> Self {
        match v {
            1 => ElectionStrategy::Classic,
            2 => ElectionStrategy::Disallow,
            3 => ElectionStrategy::Connectivity,
            _ => ElectionStrategy::Classic,
        }
    }
}

impl Denc for ElectionStrategy {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        if buf.remaining_mut() < 1 {
            return Err(RadosError::Protocol(
                "Insufficient buffer space for ElectionStrategy".to_string(),
            ));
        }
        buf.put_u8(*self as u8);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        if buf.remaining() < 1 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for ElectionStrategy".to_string(),
            ));
        }
        Ok(ElectionStrategy::from(buf.get_u8()))
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(1)
    }
}

impl crate::denc::FixedSize for ElectionStrategy {
    const SIZE: usize = 1;
}

/// Monitor information (mon_info_t in C++)
/// Version 6 encoding format
#[derive(Debug, Clone, Default, Serialize)]
pub struct MonInfo {
    pub name: String,
    pub public_addrs: EntityAddrvec,
    pub priority: u16,
    pub weight: u16,
    pub crush_loc: BTreeMap<String, String>,
    pub time_added: UTime,
}

impl VersionedEncode for MonInfo {
    const FEATURE_DEPENDENT: bool = true;

    fn encoding_version(&self, features: u64) -> u8 {
        // Check for SERVER_NAUTILUS feature
        const SERVER_NAUTILUS: u64 = 1 << 28; // CEPH_FEATURE_SERVER_NAUTILUS
        if (features & SERVER_NAUTILUS) == 0 {
            2
        } else {
            6
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
        // Encode name
        Denc::encode(&self.name, buf, features)?;

        // Encode public_addrs (v3+ uses entity_addrvec_t, v1-v2 use entity_addr_t)
        if version < 3 {
            // For v1-v2, encode as legacy addr (simplified - just use first addr)
            Denc::encode(&self.public_addrs, buf, features)?;
        } else {
            Denc::encode(&self.public_addrs, buf, features)?;
        }

        // Encode priority (v2+)
        if version >= 2 {
            Denc::encode(&self.priority, buf, features)?;
        }

        // Encode weight (v4+)
        if version >= 4 {
            Denc::encode(&self.weight, buf, features)?;
        }

        // Encode crush_loc (v5+)
        if version >= 5 {
            Denc::encode(&self.crush_loc, buf, features)?;
        }

        // Encode time_added (v6+)
        if version >= 6 {
            Denc::encode(&self.time_added, buf, features)?;
        }

        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        let name = <String as Denc>::decode(buf, features)?;
        let public_addrs = <EntityAddrvec as Denc>::decode(buf, features)?;

        let priority = if version >= 2 {
            <u16 as Denc>::decode(buf, features)?
        } else {
            0
        };

        let weight = if version >= 4 {
            <u16 as Denc>::decode(buf, features)?
        } else {
            0
        };

        let crush_loc = if version >= 5 {
            <BTreeMap<String, String> as Denc>::decode(buf, features)?
        } else {
            BTreeMap::new()
        };

        let time_added = if version >= 6 {
            <UTime as Denc>::decode(buf, features)?
        } else {
            UTime::default()
        };

        Ok(MonInfo {
            name,
            public_addrs,
            priority,
            weight,
            crush_loc,
            time_added,
        })
    }

    fn encoded_size_content(&self, features: u64, version: u8) -> Option<usize> {
        let mut size = 0;

        // name (string)
        size += self.name.encoded_size(features)?;

        // public_addrs (entity_addrvec_t)
        size += self.public_addrs.encoded_size(features)?;

        // priority (v2+, u16)
        if version >= 2 {
            size += 2;
        }

        // weight (v4+, u16)
        if version >= 4 {
            size += 2;
        }

        // crush_loc (v5+, map)
        if version >= 5 {
            size += self.crush_loc.encoded_size(features)?;
        }

        // time_added (v6+, UTime)
        if version >= 6 {
            size += self.time_added.encoded_size(features)?;
        }

        Some(size)
    }
}

crate::impl_denc_for_versioned!(MonInfo);

/// Monitor map (MonMap in C++)
/// Version 9 encoding format
///
/// ## Supported Versions
/// - **Encoding**: v6-v9 (modern format with mon_info and ranks)
/// - **Decoding**: v2-v9 (with limited legacy support)
///
/// ## Version History
/// - v1: Legacy format with entity_inst_t (not supported)
/// - v2: Legacy format with mon_addr map (basic support)
/// - v3-v5: Legacy format with ENCODE_START (basic support)
/// - v6: Added ranks field
/// - v7: Added min_mon_release
/// - v8: Added removed_ranks, strategy, and disallowed_leaders
/// - v9: Added stretch mode fields (stretch_mode_enabled, tiebreaker_mon, stretch_marked_down_mons)
///
/// The implementation focuses on modern formats (v6+) as these are used in
/// all actively supported Ceph releases (Nautilus and later).
#[derive(Debug, Clone, Default, Serialize)]
pub struct MonMap {
    pub fsid: FsId,
    pub epoch: Epoch,
    pub last_changed: UTime,
    pub created: UTime,
    pub persistent_features: MonFeature,
    pub optional_features: MonFeature,
    pub mon_info: BTreeMap<String, MonInfo>,
    pub ranks: Vec<String>,
    pub min_mon_release: MonCephRelease,
    pub removed_ranks: Vec<u32>,
    pub strategy: ElectionStrategy,
    pub disallowed_leaders: Vec<String>,
    pub stretch_mode_enabled: bool,
    pub tiebreaker_mon: String,
    pub stretch_marked_down_mons: Vec<String>,
}

impl VersionedEncode for MonMap {
    const FEATURE_DEPENDENT: bool = true;

    fn encoding_version(&self, features: u64) -> u8 {
        // Check for SERVER_NAUTILUS feature
        const SERVER_NAUTILUS: u64 = 1 << 28; // CEPH_FEATURE_SERVER_NAUTILUS
        const MONENC: u64 = 1 << 57; // CEPH_FEATURE_MONENC

        if (features & MONENC) == 0 {
            // Legacy encoding without MONENC
            return 2;
        }

        if (features & SERVER_NAUTILUS) == 0 {
            5
        } else {
            9
        }
    }

    fn compat_version(&self, features: u64) -> u8 {
        const MONENC: u64 = 1 << 57;
        if (features & MONENC) == 0 {
            2
        } else {
            3
        }
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        version: u8,
    ) -> Result<(), RadosError> {
        // Encode fsid (raw 16 bytes)
        if buf.remaining_mut() < 16 {
            return Err(RadosError::Protocol(
                "Insufficient buffer space for fsid".to_string(),
            ));
        }
        buf.put_slice(&self.fsid);

        // Encode epoch
        Denc::encode(&self.epoch, buf, features)?;

        // For v1-v2, encode legacy mon_addr map
        // For v3-v5, encode legacy_mon_addr
        // For v6+, we don't encode legacy maps

        if version < 6 {
            // Build legacy mon_addr map from mon_info
            // Note: We currently only support encoding version 6+ (modern format)
            // Legacy versions 1-5 would require encoding old format maps
            // which is not commonly needed for new implementations
            return Err(RadosError::Protocol(format!(
                "MonMap encoding version {} not supported (only v6+ supported)",
                version
            )));
        }

        // Encode timestamps
        Denc::encode(&self.last_changed, buf, features)?;
        Denc::encode(&self.created, buf, features)?;

        // Encode features (v4+)
        if version >= 4 {
            Denc::encode(&self.persistent_features, buf, features)?;
            Denc::encode(&self.optional_features, buf, features)?;
        }

        // Encode mon_info (v5+)
        if version >= 5 {
            Denc::encode(&self.mon_info, buf, features)?;
        }

        // Encode ranks (v6+)
        if version >= 6 {
            Denc::encode(&self.ranks, buf, features)?;
        }

        // Encode min_mon_release (v7+)
        if version >= 7 {
            Denc::encode(&self.min_mon_release, buf, features)?;
        }

        // Encode removed_ranks and strategy (v8+)
        if version >= 8 {
            Denc::encode(&self.removed_ranks, buf, features)?;
            Denc::encode(&self.strategy, buf, features)?;
            Denc::encode(&self.disallowed_leaders, buf, features)?;
        }

        // Encode stretch mode fields (v9+)
        if version >= 9 {
            Denc::encode(&self.stretch_mode_enabled, buf, features)?;
            Denc::encode(&self.tiebreaker_mon, buf, features)?;
            Denc::encode(&self.stretch_marked_down_mons, buf, features)?;
        }

        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Decode fsid (raw 16 bytes)
        if buf.remaining() < 16 {
            return Err(RadosError::Protocol(
                "Insufficient bytes for fsid".to_string(),
            ));
        }
        let mut fsid = [0u8; 16];
        buf.copy_to_slice(&mut fsid);

        // Decode epoch
        let epoch = <Epoch as Denc>::decode(buf, features)?;

        // For v1, decode vector<entity_inst_t> and build mon_addr
        // For v2-v5, decode legacy mon_addr map
        let mut legacy_mon_addr: BTreeMap<String, crate::entity_addr::EntityAddr> = BTreeMap::new();

        if version == 1 {
            // v1: decode vector<entity_inst_t>
            // This legacy format is rarely encountered in modern Ceph deployments
            // and requires special handling of entity_inst_t structures
            return Err(RadosError::Protocol(
                "MonMap decoding version 1 not supported (legacy format)".to_string(),
            ));
        } else if version < 6 {
            // v2-v5: decode map<string, entity_addr_t>
            legacy_mon_addr =
                <BTreeMap<String, crate::entity_addr::EntityAddr> as Denc>::decode(buf, features)?;
        }

        // Decode timestamps
        let last_changed = <UTime as Denc>::decode(buf, features)?;
        let created = <UTime as Denc>::decode(buf, features)?;

        // Decode features (v4+)
        let (persistent_features, optional_features) = if version >= 4 {
            (
                <MonFeature as Denc>::decode(buf, features)?,
                <MonFeature as Denc>::decode(buf, features)?,
            )
        } else {
            (MonFeature::default(), MonFeature::default())
        };

        // Decode mon_info (v5+) or build from legacy
        let mon_info = if version < 5 {
            // Build mon_info from legacy_mon_addr
            let mut info_map = BTreeMap::new();
            for (name, addr) in legacy_mon_addr {
                let mon = MonInfo {
                    name: name.clone(),
                    public_addrs: {
                        let mut addrvec = crate::entity_addr::EntityAddrvec::new();
                        addrvec.addrs.push(addr);
                        addrvec
                    },
                    ..Default::default()
                };
                info_map.insert(name, mon);
            }
            info_map
        } else {
            <BTreeMap<String, MonInfo> as Denc>::decode(buf, features)?
        };

        // Decode ranks (v6+) or calculate from mon_info
        let ranks = if version < 6 {
            // Would need to call calc_legacy_ranks()
            // For now, just extract names from mon_info
            mon_info.keys().cloned().collect()
        } else {
            <Vec<String> as Denc>::decode(buf, features)?
        };

        // Decode min_mon_release (v7+)
        let min_mon_release = if version >= 7 {
            <MonCephRelease as Denc>::decode(buf, features)?
        } else {
            MonCephRelease::Unknown
        };

        // Decode removed_ranks and strategy (v8+)
        let (removed_ranks, strategy, disallowed_leaders) = if version >= 8 {
            (
                <Vec<u32> as Denc>::decode(buf, features)?,
                <ElectionStrategy as Denc>::decode(buf, features)?,
                <Vec<String> as Denc>::decode(buf, features)?,
            )
        } else {
            (Vec::new(), ElectionStrategy::default(), Vec::new())
        };

        // Decode stretch mode fields (v9+)
        let (stretch_mode_enabled, tiebreaker_mon, stretch_marked_down_mons) = if version >= 9 {
            (
                <bool as Denc>::decode(buf, features)?,
                <String as Denc>::decode(buf, features)?,
                <Vec<String> as Denc>::decode(buf, features)?,
            )
        } else {
            (false, String::new(), Vec::new())
        };

        Ok(MonMap {
            fsid,
            epoch,
            last_changed,
            created,
            persistent_features,
            optional_features,
            mon_info,
            ranks,
            min_mon_release,
            removed_ranks,
            strategy,
            disallowed_leaders,
            stretch_mode_enabled,
            tiebreaker_mon,
            stretch_marked_down_mons,
        })
    }

    fn encoded_size_content(&self, features: u64, version: u8) -> Option<usize> {
        if version < 6 {
            // Legacy versions not supported
            return None;
        }

        let mut size = 0;

        // fsid (raw 16 bytes)
        size += 16;

        // epoch (u32)
        size += 4;

        // timestamps (UTime each)
        size += self.last_changed.encoded_size(features)?;
        size += self.created.encoded_size(features)?;

        // features (v4+)
        if version >= 4 {
            size += self.persistent_features.encoded_size(features)?;
            size += self.optional_features.encoded_size(features)?;
        }

        // mon_info (v5+)
        if version >= 5 {
            size += self.mon_info.encoded_size(features)?;
        }

        // ranks (v6+)
        if version >= 6 {
            size += self.ranks.encoded_size(features)?;
        }

        // min_mon_release (v7+)
        if version >= 7 {
            size += 1; // MonCephRelease is u8
        }

        // removed_ranks and strategy (v8+)
        if version >= 8 {
            size += self.removed_ranks.encoded_size(features)?;
            size += 1; // ElectionStrategy is u8
            size += self.disallowed_leaders.encoded_size(features)?;
        }

        // stretch mode fields (v9+)
        if version >= 9 {
            size += 1; // bool
            size += self.tiebreaker_mon.encoded_size(features)?;
            size += self.stretch_marked_down_mons.encoded_size(features)?;
        }

        Some(size)
    }
}

crate::impl_denc_for_versioned!(MonMap);

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_mon_feature_roundtrip() {
        let feature = MonFeature {
            features: 0x12345678,
        };
        let mut buf = BytesMut::new();
        feature.encode(&mut buf, 0).unwrap();
        let decoded = MonFeature::decode(&mut buf.freeze(), 0).unwrap();
        assert_eq!(feature, decoded);
    }

    #[test]
    fn test_ceph_release_roundtrip() {
        let release = MonCephRelease::Quincy;
        let mut buf = BytesMut::new();
        release.encode(&mut buf, 0).unwrap();
        let decoded = MonCephRelease::decode(&mut buf.freeze(), 0).unwrap();
        assert_eq!(release, decoded);
    }

    #[test]
    fn test_election_strategy_roundtrip() {
        let strategy = ElectionStrategy::Connectivity;
        let mut buf = BytesMut::new();
        strategy.encode(&mut buf, 0).unwrap();
        let decoded = ElectionStrategy::decode(&mut buf.freeze(), 0).unwrap();
        assert_eq!(strategy, decoded);
    }

    #[test]
    fn test_mon_info_roundtrip() {
        let mon_info = MonInfo {
            name: "mon.a".to_string(),
            priority: 100,
            weight: 200,
            ..Default::default()
        };

        let mut buf = BytesMut::new();
        mon_info.encode(&mut buf, u64::MAX).unwrap();
        let decoded = MonInfo::decode(&mut buf.freeze(), u64::MAX).unwrap();
        assert_eq!(mon_info.name, decoded.name);
        assert_eq!(mon_info.priority, decoded.priority);
        assert_eq!(mon_info.weight, decoded.weight);
    }

    #[test]
    fn test_monmap_roundtrip() {
        let monmap = MonMap {
            epoch: 1,
            fsid: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            ..Default::default()
        };

        let mut buf = BytesMut::new();
        monmap.encode(&mut buf, u64::MAX).unwrap();
        let decoded = MonMap::decode(&mut buf.freeze(), u64::MAX).unwrap();
        assert_eq!(monmap.epoch, decoded.epoch);
        assert_eq!(monmap.fsid, decoded.fsid);
    }
}
