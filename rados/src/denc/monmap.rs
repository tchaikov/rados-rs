//! MonMap and related types
//!
//! This module implements the Ceph monitor map encoding/decoding.
//! See MonMap.h and MonMap.cc in the Ceph source for the C++ implementation.

use crate::denc::codec::{Denc, VersionedEncode};
use crate::denc::entity_addr::EntityAddrvec;
use crate::denc::error::RadosError;
use bytes::{Buf, BufMut};
use serde::Serialize;
use std::collections::BTreeMap;

use crate::denc::ids::Epoch;
use crate::denc::types::{FsId, UTime};

const MONMAP_ENCODING_VERSION: u8 = 9;
const MONMAP_COMPAT_VERSION: u8 = 3;
const MONMAP_MIN_DECODE_VERSION: u8 = 6;

/// Monitor feature flags (mon_feature_t in C++)
/// Uses versioned encoding (ENCODE_START/DECODE_START)
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct MonFeature {
    pub features: u64,
}

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
        features: u64,
        _version: u8,
    ) -> Result<(), RadosError> {
        self.features.encode(buf, features)
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        crate::denc::check_min_version!(version, 1, "MonFeature", "Quincy v17+");
        Ok(MonFeature {
            features: u64::decode(buf, features)?,
        })
    }

    fn encoded_size_content(&self, features: u64, _version: u8) -> Option<usize> {
        self.features.encoded_size(features)
    }
}

crate::denc::impl_denc_for_versioned!(MonFeature);

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

crate::impl_denc_u8_enum!(MonCephRelease);

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

crate::impl_denc_u8_enum!(ElectionStrategy);

/// Monitor information (mon_info_t in C++)
/// Encodes at v6 (always, since Quincy requires SERVER_NAUTILUS); decodes v5–v6+
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

    fn encoding_version(&self, _features: u64) -> u8 {
        // Quincy (v17) and later always have SERVER_NAUTILUS; v2 encode
        // would produce wire data that our decode_content rejects (min v5).
        6
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
        // We always encode at v6 (see `encoding_version`); older layouts are
        // dead on the wire for Quincy+ peers.
        debug_assert_eq!(version, 6);
        Denc::encode(&self.name, buf, features)?;
        Denc::encode(&self.public_addrs, buf, features)?;
        Denc::encode(&self.priority, buf, features)?;
        Denc::encode(&self.weight, buf, features)?;
        Denc::encode(&self.crush_loc, buf, features)?;
        Denc::encode(&self.time_added, buf, features)?;
        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Quincy (v17) always emits ENCODE_START(5, 1, bl), encoding name,
        // public_addrs, priority, weight, and crush_loc as a unit.
        crate::denc::check_min_version!(version, 5, "MonInfo", "Quincy v17+");

        let name = <String as Denc>::decode(buf, features)?;
        let public_addrs = <EntityAddrvec as Denc>::decode(buf, features)?;
        let priority = <u16 as Denc>::decode(buf, features)?;
        let weight = <u16 as Denc>::decode(buf, features)?;
        let crush_loc = <BTreeMap<String, String> as Denc>::decode(buf, features)?;

        // v6+: time_added (not present in Quincy, added in a later release)
        let time_added =
            crate::denc::decode_if_version!(buf, features, version, >= 6, UTime, UTime::default());

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
        debug_assert_eq!(version, 6);
        Some(
            self.name.encoded_size(features)?
                + self.public_addrs.encoded_size(features)?
                + 2 // priority (u16)
                + 2 // weight (u16)
                + self.crush_loc.encoded_size(features)?
                + self.time_added.encoded_size(features)?,
        )
    }
}

crate::denc::impl_denc_for_versioned!(MonInfo);

/// Monitor map (MonMap in C++)
/// Version 9 encoding format
///
/// ## Supported Versions
/// - **Encoding**: v9 (current Quincy+ layout)
/// - **Decoding**: v6-v9 (modern format with ranks)
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
/// all actively supported Ceph releases (Quincy and later).
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

    fn encoding_version(&self, _features: u64) -> u8 {
        MONMAP_ENCODING_VERSION
    }

    fn compat_version(&self, _features: u64) -> u8 {
        MONMAP_COMPAT_VERSION
    }

    fn encode_content<B: BufMut>(
        &self,
        buf: &mut B,
        features: u64,
        version: u8,
    ) -> Result<(), RadosError> {
        Denc::encode(&self.fsid, buf, features)?;
        Denc::encode(&self.epoch, buf, features)?;

        debug_assert_eq!(version, MONMAP_ENCODING_VERSION);

        Denc::encode(&self.last_changed, buf, features)?;
        Denc::encode(&self.created, buf, features)?;

        Denc::encode(&self.persistent_features, buf, features)?;
        Denc::encode(&self.optional_features, buf, features)?;
        Denc::encode(&self.mon_info, buf, features)?;
        Denc::encode(&self.ranks, buf, features)?;
        Denc::encode(&self.min_mon_release, buf, features)?;
        Denc::encode(&self.removed_ranks, buf, features)?;
        Denc::encode(&self.strategy, buf, features)?;
        Denc::encode(&self.disallowed_leaders, buf, features)?;
        Denc::encode(&self.stretch_mode_enabled, buf, features)?;
        Denc::encode(&self.tiebreaker_mon, buf, features)?;
        Denc::encode(&self.stretch_marked_down_mons, buf, features)?;

        Ok(())
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        version: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Minimum supported project release boundary (Quincy v17+)
        // Version 6 remains the wire-format floor we decode for modern MonMap layouts.
        crate::denc::check_min_version!(
            version,
            MONMAP_MIN_DECODE_VERSION,
            "MonMap",
            "Quincy v17+"
        );

        let fsid = <[u8; 16] as Denc>::decode(buf, features)?;
        let epoch = <Epoch as Denc>::decode(buf, features)?;
        let last_changed = <UTime as Denc>::decode(buf, features)?;
        let created = <UTime as Denc>::decode(buf, features)?;
        let persistent_features = <MonFeature as Denc>::decode(buf, features)?;
        let optional_features = <MonFeature as Denc>::decode(buf, features)?;
        let mon_info = <BTreeMap<String, MonInfo> as Denc>::decode(buf, features)?;
        let ranks = <Vec<String> as Denc>::decode(buf, features)?;

        // Decode min_mon_release (v7+)
        let min_mon_release = crate::denc::decode_if_version!(buf, features, version, >= 7, MonCephRelease, MonCephRelease::Unknown);

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
        debug_assert_eq!(version, MONMAP_ENCODING_VERSION);

        Some(
            self.fsid.encoded_size(features)?
                + self.epoch.encoded_size(features)?
                + self.last_changed.encoded_size(features)?
                + self.created.encoded_size(features)?
                + self.persistent_features.encoded_size(features)?
                + self.optional_features.encoded_size(features)?
                + self.mon_info.encoded_size(features)?
                + self.ranks.encoded_size(features)?
                + self.min_mon_release.encoded_size(features)?
                + self.removed_ranks.encoded_size(features)?
                + self.strategy.encoded_size(features)?
                + self.disallowed_leaders.encoded_size(features)?
                + self.stretch_mode_enabled.encoded_size(features)?
                + self.tiebreaker_mon.encoded_size(features)?
                + self.stretch_marked_down_mons.encoded_size(features)?,
        )
    }
}

crate::denc::impl_denc_for_versioned!(MonMap);

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
            epoch: crate::denc::ids::Epoch::new(1),
            fsid: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            ..Default::default()
        };

        let mut buf = BytesMut::new();
        monmap.encode(&mut buf, u64::MAX).unwrap();
        let decoded = MonMap::decode(&mut buf.freeze(), u64::MAX).unwrap();
        assert_eq!(monmap.epoch, decoded.epoch);
        assert_eq!(monmap.fsid, decoded.fsid);
    }

    #[test]
    fn test_monmap_encode_ignores_legacy_feature_negotiation() {
        let monmap = MonMap {
            epoch: crate::denc::ids::Epoch::new(1),
            fsid: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            ..Default::default()
        };

        let mut buf = BytesMut::new();
        monmap.encode(&mut buf, 0).unwrap();

        assert_eq!(buf[0], MONMAP_ENCODING_VERSION);
        assert_eq!(buf[1], MONMAP_COMPAT_VERSION);
    }
}
