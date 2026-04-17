//! Monitor map structures
//!
//! The MonMapState contains information about all monitors in the cluster.

use crate::denc::{EntityAddr, EntityAddrType, EntityAddrvec};
use crate::monclient::error::{MonClientError, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

/// Milliseconds per second for timestamp conversions
const MS_PER_SEC: u64 = 1000;
/// Nanoseconds per millisecond for timestamp conversions
const NSEC_PER_MSEC: u64 = 1_000_000;

/// Monitor map
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonMapState {
    /// Cluster FSID
    pub fsid: Uuid,

    /// MonMapState epoch
    pub epoch: u32,

    /// Creation time (Unix timestamp)
    pub created: u64,

    /// Last modified time (Unix timestamp)
    pub modified: u64,

    /// List of monitors
    pub monitors: Vec<MonInfo>,

    /// Monitor name to index mapping
    #[serde(skip)]
    name_to_rank: HashMap<String, usize>,

    /// Monitor rank to vector index mapping
    #[serde(skip)]
    rank_to_index: HashMap<usize, usize>,

    /// Address to monitor mapping
    #[serde(skip)]
    addr_to_rank: HashMap<String, usize>,

    /// Decoded MonInfo fields that are not surfaced in MonInfo yet
    #[serde(skip)]
    mon_info_extras: HashMap<String, MonInfoExtras>,

    /// Decoded mon_info entries that do not have an active rank
    #[serde(skip)]
    unranked_mon_info: BTreeMap<String, crate::MonInfo>,

    /// Additional decoded MonMapState state preserved for re-encoding
    #[serde(skip)]
    persistent_features: crate::MonFeature,

    /// Additional decoded MonMapState state preserved for re-encoding
    #[serde(skip)]
    optional_features: crate::MonFeature,

    /// Minimum monitor release advertised by the cluster
    #[serde(skip)]
    min_mon_release: crate::MonCephRelease,

    /// Removed monitor ranks
    #[serde(skip)]
    removed_ranks: Vec<u32>,

    /// Election strategy
    #[serde(skip)]
    strategy: crate::ElectionStrategy,

    /// Monitor names that are disallowed as leaders
    #[serde(skip)]
    disallowed_leaders: Vec<String>,

    /// Stretch mode flag
    #[serde(skip)]
    stretch_mode_enabled: bool,

    /// Stretch mode tiebreaker monitor
    #[serde(skip)]
    tiebreaker_mon: String,

    /// Stretch mode marked down monitors
    #[serde(skip)]
    stretch_marked_down_mons: Vec<String>,
}

impl MonMapState {
    pub fn new(fsid: Uuid) -> Self {
        Self {
            fsid,
            epoch: 0,
            created: 0,
            modified: 0,
            monitors: Vec::new(),
            name_to_rank: HashMap::new(),
            rank_to_index: HashMap::new(),
            addr_to_rank: HashMap::new(),
            mon_info_extras: HashMap::new(),
            unranked_mon_info: BTreeMap::new(),
            persistent_features: crate::MonFeature::default(),
            optional_features: crate::MonFeature::default(),
            min_mon_release: crate::MonCephRelease::default(),
            removed_ranks: Vec::new(),
            strategy: crate::ElectionStrategy::default(),
            disallowed_leaders: Vec::new(),
            stretch_mode_enabled: false,
            tiebreaker_mon: String::new(),
            stretch_marked_down_mons: Vec::new(),
        }
    }

    /// Build initial monmap from configuration.
    ///
    /// Preserves per-monitor grouping from `mon_host` entries, including cases
    /// where multiple monitors share the same IP address. When callers provide a
    /// flattened list of addresses, consecutive same-IP addresses are grouped
    /// until the protocol type repeats, which matches the common
    /// `[v2:IP:PORT,v1:IP:PORT] [v2:IP:PORT,v1:IP:PORT]` layout used by Ceph.
    pub fn build_initial(mon_addrs: &[String]) -> Result<Self> {
        let mut monitor_groups = collect_initial_monitor_groups(mon_addrs)?;
        monitor_groups.sort_by_key(|group| {
            group
                .first()
                .and_then(EntityAddr::to_socket_addr)
                .map(|addr| addr.ip())
        });

        let mut monmap = Self::new(Uuid::nil());
        monmap.epoch = 0;

        for (rank, addrs) in monitor_groups.iter_mut().enumerate() {
            let mut addrs = std::mem::take(addrs);

            // Sort addresses: v2 (Msgr2) first, then v1 (Legacy)
            // This ensures get_msgr2() finds v2 addresses first
            addrs.sort_by_key(|a| if a.is_msgr2() { 0 } else { 1 });

            let mon_info = MonInfo {
                name: format!("mon.{rank}"),
                rank,
                addrs: EntityAddrvec { addrs },
                priority: 0,
                weight: 0,
            };

            monmap.add_monitor(mon_info);
        }

        Ok(monmap)
    }

    /// Add a monitor to the map
    pub fn add_monitor(&mut self, mon: MonInfo) {
        let rank = mon.rank;
        let name = mon.name.clone();
        let index = self.monitors.len();

        // Build address mapping
        for addr in &mon.addrs.addrs {
            self.addr_to_rank.insert(addr.to_string(), rank);
        }

        self.name_to_rank.insert(name.clone(), rank);
        self.rank_to_index.insert(rank, index);
        self.mon_info_extras.entry(name).or_default();
        self.monitors.push(mon);
    }

    /// Get number of monitors
    pub fn len(&self) -> usize {
        self.monitors.len()
    }

    /// Returns true if the monmap has no monitors
    pub fn is_empty(&self) -> bool {
        self.monitors.is_empty()
    }

    /// Get monitor by rank
    pub fn get_mon(&self, rank: usize) -> Option<&MonInfo> {
        self.rank_to_index
            .get(&rank)
            .and_then(|&index| self.monitors.get(index))
    }

    /// Get monitor addresses by rank
    pub fn get_addrs(&self, rank: usize) -> Option<&EntityAddrvec> {
        self.get_mon(rank).map(|m| &m.addrs)
    }

    /// Get monitor name by rank
    pub fn get_name(&self, rank: usize) -> Option<&str> {
        self.get_mon(rank).map(|m| m.name.as_str())
    }

    /// Get monitor rank by name
    pub fn get_rank(&self, name: &str) -> Option<usize> {
        self.name_to_rank.get(name).copied()
    }

    /// Get monitor rank by address
    pub fn get_rank_by_addr(&self, addr: &str) -> Option<usize> {
        self.addr_to_rank.get(addr).copied()
    }

    /// Check if monitor exists by name
    pub fn contains(&self, name: &str) -> bool {
        self.name_to_rank.contains_key(name)
    }

    /// Get all monitor ranks
    pub fn get_all_ranks(&self) -> Vec<usize> {
        let mut ranks: Vec<_> = self.monitors.iter().map(|mon| mon.rank).collect();
        ranks.sort_unstable();
        ranks
    }

    /// Get monitors grouped by priority (lowest priority first)
    /// Returns monitors with the same priority grouped together
    pub fn get_monitors_by_priority(&self) -> Vec<Vec<usize>> {
        // Group monitors by priority
        let mut priority_groups: BTreeMap<i32, Vec<usize>> = BTreeMap::new();
        for mon in &self.monitors {
            priority_groups
                .entry(mon.priority)
                .or_default()
                .push(mon.rank);
        }

        for ranks in priority_groups.values_mut() {
            ranks.sort_unstable();
        }

        // Return groups in priority order (lowest priority first)
        priority_groups.into_values().collect()
    }

    /// Get the weight of a monitor by rank
    pub fn get_weight(&self, rank: usize) -> u16 {
        self.get_mon(rank).map_or(0, |m| m.weight)
    }

    /// Rebuild internal indices after deserialization
    pub fn rebuild_indices(&mut self) {
        self.name_to_rank.clear();
        self.rank_to_index.clear();
        self.addr_to_rank.clear();

        for (index, mon) in self.monitors.iter().enumerate() {
            let name = mon.name.clone();
            self.name_to_rank.insert(name.clone(), mon.rank);
            self.rank_to_index.insert(mon.rank, index);
            self.mon_info_extras.entry(name).or_default();
            for addr in &mon.addrs.addrs {
                self.addr_to_rank.insert(addr.to_string(), mon.rank);
            }
        }
    }

    /// Encode monmap to bytes (for sending in messages)
    pub fn encode(&self) -> Result<bytes::Bytes> {
        use crate::Denc;
        use bytes::BytesMut;

        // Convert to rados-denc MonMapState
        let denc_monmap = self.to_denc_monmap();

        // Encode with full features
        let mut buf = BytesMut::with_capacity(denc_monmap.encoded_size(u64::MAX).unwrap_or(1024));
        denc_monmap.encode(&mut buf, u64::MAX)?;

        Ok(buf.freeze())
    }

    /// Decode monmap from bytes
    pub fn decode(data: &[u8]) -> Result<Self> {
        use crate::Denc;

        // Decode using rados-denc
        let mut bytes = bytes::Bytes::copy_from_slice(data);
        let denc_monmap = crate::MonMap::decode(&mut bytes, u64::MAX)?;

        // Convert to monclient MonMapState
        Self::from_denc_monmap(denc_monmap)
    }

    /// Convert to rados-denc MonMapState for encoding
    fn to_denc_monmap(&self) -> crate::MonMap {
        use crate::UTime;

        // Convert fsid from Uuid to [u8; 16]
        let fsid = *self.fsid.as_bytes();

        // Convert monitors to mon_info BTreeMap while preserving extra decoded fields.
        let mut mon_info = self.unranked_mon_info.clone();
        for mon in &self.monitors {
            let extras = self
                .mon_info_extras
                .get(&mon.name)
                .cloned()
                .unwrap_or_default();
            let denc_mon = crate::MonInfo {
                name: mon.name.clone(),
                public_addrs: mon.addrs.clone(),
                priority: mon.priority as u16,
                weight: mon.weight,
                crush_loc: extras.crush_loc,
                time_added: extras.time_added,
            };
            mon_info.insert(mon.name.clone(), denc_mon);
        }

        // Build ranks from the stored rank values rather than monitor vector order.
        let mut ranked_monitors: Vec<&MonInfo> = self.monitors.iter().collect();
        ranked_monitors.sort_by_key(|mon| mon.rank);
        let ranks: Vec<String> = ranked_monitors
            .into_iter()
            .map(|mon| mon.name.clone())
            .collect();

        crate::MonMap {
            fsid,
            epoch: crate::Epoch::new(self.epoch),
            last_changed: UTime {
                sec: (self.modified / MS_PER_SEC) as u32,
                nsec: ((self.modified % MS_PER_SEC) * NSEC_PER_MSEC) as u32,
            },
            created: UTime {
                sec: (self.created / MS_PER_SEC) as u32,
                nsec: ((self.created % MS_PER_SEC) * NSEC_PER_MSEC) as u32,
            },
            persistent_features: self.persistent_features.clone(),
            optional_features: self.optional_features.clone(),
            mon_info,
            ranks,
            min_mon_release: self.min_mon_release,
            removed_ranks: self.removed_ranks.clone(),
            strategy: self.strategy,
            disallowed_leaders: self.disallowed_leaders.clone(),
            stretch_mode_enabled: self.stretch_mode_enabled,
            tiebreaker_mon: self.tiebreaker_mon.clone(),
            stretch_marked_down_mons: self.stretch_marked_down_mons.clone(),
        }
    }

    /// Convert from rados-denc MonMapState after decoding
    fn from_denc_monmap(denc_monmap: crate::MonMap) -> Result<Self> {
        let crate::MonMap {
            fsid,
            epoch,
            last_changed,
            created,
            persistent_features,
            optional_features,
            mut mon_info,
            ranks,
            min_mon_release,
            removed_ranks,
            strategy,
            disallowed_leaders,
            stretch_mode_enabled,
            tiebreaker_mon,
            stretch_marked_down_mons,
        } = denc_monmap;

        // Convert fsid from [u8; 16] to Uuid
        let fsid = uuid::Uuid::from_bytes(fsid);

        // Convert UTime to Unix timestamp (milliseconds)
        let created = (created.sec as u64) * MS_PER_SEC + (created.nsec as u64) / NSEC_PER_MSEC;
        let modified =
            (last_changed.sec as u64) * MS_PER_SEC + (last_changed.nsec as u64) / NSEC_PER_MSEC;

        // Reconstruct monitor ranks from the ranks vector rather than map iteration order.
        let mut monitors = Vec::with_capacity(ranks.len());
        let mut mon_info_extras = HashMap::with_capacity(ranks.len());
        for (rank, name) in ranks.iter().enumerate() {
            let mon = mon_info.remove(name).ok_or_else(|| {
                MonClientError::InvalidMonMap(format!(
                    "MonMapState ranks referenced missing monitor {name}"
                ))
            })?;

            mon_info_extras.insert(
                name.clone(),
                MonInfoExtras {
                    crush_loc: mon.crush_loc,
                    time_added: mon.time_added,
                },
            );
            monitors.push(MonInfo {
                name: name.clone(),
                rank,
                addrs: mon.public_addrs,
                priority: mon.priority as i32,
                weight: mon.weight,
            });
        }

        let n = ranks.len();
        let mut monmap = Self {
            fsid,
            epoch: epoch.as_u32(),
            created,
            modified,
            monitors,
            name_to_rank: HashMap::with_capacity(n),
            rank_to_index: HashMap::with_capacity(n),
            addr_to_rank: HashMap::with_capacity(n),
            mon_info_extras,
            unranked_mon_info: mon_info,
            persistent_features,
            optional_features,
            min_mon_release,
            removed_ranks,
            strategy,
            disallowed_leaders,
            stretch_mode_enabled,
            tiebreaker_mon,
            stretch_marked_down_mons,
        };

        monmap.rebuild_indices();
        Ok(monmap)
    }
}

impl Default for MonMapState {
    fn default() -> Self {
        Self::new(Uuid::nil())
    }
}

/// Monitor information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonInfo {
    /// Monitor name (e.g., "mon.a")
    pub name: String,

    /// Monitor rank
    pub rank: usize,

    /// Monitor addresses
    pub addrs: EntityAddrvec,

    /// Priority (for election)
    pub priority: i32,

    /// Weight (for client selection)
    pub weight: u16,
}

#[derive(Debug, Clone, Default)]
struct MonInfoExtras {
    crush_loc: BTreeMap<String, String>,
    time_added: crate::UTime,
}

/// Parse monitor address string
fn parse_mon_addr(addr_str: &str) -> Result<EntityAddr> {
    use std::net::SocketAddr;

    // Format: "v2:127.0.0.1:3300" or "127.0.0.1:3300"
    let (addr_type, addr_part) = if let Some(stripped) = addr_str.strip_prefix("v2:") {
        (EntityAddrType::Msgr2, stripped)
    } else if let Some(stripped) = addr_str.strip_prefix("v1:") {
        (EntityAddrType::Legacy, stripped)
    } else {
        // Default to msgr2
        (EntityAddrType::Msgr2, addr_str)
    };

    let socket_addr: SocketAddr = addr_part
        .parse()
        .map_err(|e| MonClientError::InvalidMonMap(format!("Invalid address {addr_part}: {e}")))?;

    Ok(EntityAddr::from_socket_addr(addr_type, socket_addr))
}

fn collect_initial_monitor_groups(mon_addrs: &[String]) -> Result<Vec<Vec<EntityAddr>>> {
    use std::collections::HashSet;

    let mut monitor_groups = Vec::new();
    let mut current_group = Vec::new();
    let mut current_ip = None;
    let mut current_types = HashSet::new();

    for entry in mon_addrs {
        let parsed_group = parse_mon_addr_group(entry)?;

        if parsed_group.len() > 1 {
            if !current_group.is_empty() {
                monitor_groups.push(std::mem::take(&mut current_group));
                current_ip = None;
                current_types.clear();
            }
            monitor_groups.push(parsed_group);
            continue;
        }

        let addr = parsed_group
            .into_iter()
            .next()
            .ok_or_else(|| MonClientError::InvalidMonMap("Empty monitor address group".into()))?;
        let ip = addr
            .to_socket_addr()
            .ok_or_else(|| MonClientError::InvalidMonMap("Could not parse socket addr".into()))?
            .ip();

        if current_group.is_empty() {
            current_ip = Some(ip);
            current_types.insert(addr.addr_type);
            current_group.push(addr);
            continue;
        }

        if current_ip == Some(ip) && !current_types.contains(&addr.addr_type) {
            current_types.insert(addr.addr_type);
            current_group.push(addr);
            continue;
        }

        monitor_groups.push(std::mem::take(&mut current_group));
        current_types.clear();
        current_ip = Some(ip);
        current_types.insert(addr.addr_type);
        current_group.push(addr);
    }

    if !current_group.is_empty() {
        monitor_groups.push(current_group);
    }

    Ok(monitor_groups)
}

fn parse_mon_addr_group(addr_str: &str) -> Result<Vec<EntityAddr>> {
    let trimmed = addr_str.trim();
    let inner = trimmed
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(trimmed);

    inner
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(parse_mon_addr)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn test_build_initial_monmap() {
        // Test with multiple IPs - each IP becomes one monitor
        let addrs = vec![
            "v2:127.0.0.1:3300".to_string(),
            "v2:127.0.0.2:3301".to_string(),
            "v2:127.0.0.3:3302".to_string(),
        ];

        let monmap = MonMapState::build_initial(&addrs).unwrap();
        assert_eq!(monmap.len(), 3);
        assert_eq!(monmap.get_name(0), Some("mon.0"));
        assert!(monmap.get_addrs(0).is_some());
    }

    #[test]
    fn test_build_initial_monmap_groups_v1_v2_pairs_per_monitor() {
        let addrs = vec![
            "v2:192.168.1.1:6789".to_string(),
            "v1:192.168.1.1:6790".to_string(),
            "v2:192.168.1.2:6789".to_string(),
            "v1:192.168.1.2:6790".to_string(),
        ];

        let monmap = MonMapState::build_initial(&addrs).unwrap();

        // Should create 2 monitors (one per unique IP)
        assert_eq!(monmap.len(), 2);

        // Each monitor should have 2 addresses (v2 and v1)
        let mon0_addrs = monmap.get_addrs(0).unwrap();
        assert_eq!(mon0_addrs.addrs.len(), 2);

        // v2 should be sorted first
        assert!(mon0_addrs.addrs[0].is_msgr2());
        assert!(mon0_addrs.addrs[1].is_legacy());

        // get_msgr2() should find the v2 address
        assert!(mon0_addrs.get_msgr2().is_some());

        // Check second monitor
        let mon1_addrs = monmap.get_addrs(1).unwrap();
        assert_eq!(mon1_addrs.addrs.len(), 2);
        assert!(mon1_addrs.get_msgr2().is_some());
    }

    #[test]
    fn test_build_initial_monmap_preserves_same_ip_monitor_groups() {
        let addrs = vec![
            "v2:192.168.1.43:40472".to_string(),
            "v1:192.168.1.43:40473".to_string(),
            "v2:192.168.1.43:40474".to_string(),
            "v1:192.168.1.43:40475".to_string(),
        ];

        let monmap =
            MonMapState::build_initial(&addrs).expect("same-IP monitor groups should parse");

        assert_eq!(
            monmap.len(),
            2,
            "distinct same-IP monitors must not collapse"
        );

        let mon0_addrs = monmap.get_addrs(0).expect("rank 0 should exist");
        assert_eq!(mon0_addrs.addrs.len(), 2, "rank 0 should keep its pair");
        assert_eq!(
            monmap.get_rank_by_addr("v2:192.168.1.43:40472"),
            Some(0),
            "first v2 address should map to rank 0"
        );
        assert_eq!(
            monmap.get_rank_by_addr("v1:192.168.1.43:40473"),
            Some(0),
            "first v1 address should map to rank 0"
        );

        let mon1_addrs = monmap.get_addrs(1).expect("rank 1 should exist");
        assert_eq!(mon1_addrs.addrs.len(), 2, "rank 1 should keep its pair");
        assert_eq!(
            monmap.get_rank_by_addr("v2:192.168.1.43:40474"),
            Some(1),
            "second v2 address should map to rank 1"
        );
        assert_eq!(
            monmap.get_rank_by_addr("v1:192.168.1.43:40475"),
            Some(1),
            "second v1 address should map to rank 1"
        );
    }

    #[test]
    fn test_build_initial_monmap_preserves_explicit_bracket_groups() {
        let addrs = vec![
            "[v2:192.168.1.43:40472,v1:192.168.1.43:40473]".to_string(),
            "[v2:192.168.1.43:40474,v1:192.168.1.43:40475]".to_string(),
        ];

        let monmap = MonMapState::build_initial(&addrs)
            .expect("explicit monitor address groups should parse");

        assert_eq!(monmap.len(), 2);
        assert_eq!(monmap.get_rank_by_addr("v2:192.168.1.43:40472"), Some(0));
        assert_eq!(monmap.get_rank_by_addr("v2:192.168.1.43:40474"), Some(1));
    }

    #[test]
    fn test_monmap_lookup() {
        let mut monmap = MonMapState::new(Uuid::nil());

        let mon = MonInfo {
            name: "mon.a".to_string(),
            rank: 0,
            addrs: EntityAddrvec::new(),
            priority: 0,
            weight: 0,
        };

        monmap.add_monitor(mon);

        assert!(monmap.contains("mon.a"));
        assert_eq!(monmap.get_rank("mon.a"), Some(0));
        assert_eq!(monmap.get_name(0), Some("mon.a"));
    }

    #[test]
    fn test_rank_lookup_uses_rank_mapping() {
        let mut monmap = MonMapState::new(Uuid::nil());

        monmap.add_monitor(MonInfo {
            name: "mon.e".to_string(),
            rank: 5,
            addrs: EntityAddrvec::new(),
            priority: 1,
            weight: 9,
        });
        monmap.add_monitor(MonInfo {
            name: "mon.c".to_string(),
            rank: 2,
            addrs: EntityAddrvec::new(),
            priority: 1,
            weight: 7,
        });

        assert_eq!(monmap.get_name(5), Some("mon.e"));
        assert_eq!(monmap.get_name(2), Some("mon.c"));
        assert_eq!(monmap.get_weight(5), 9);
        assert_eq!(monmap.get_all_ranks(), vec![2, 5]);
    }

    #[test]
    fn test_from_denc_monmap_uses_ranks_vector() {
        let mut mon_info = BTreeMap::new();
        mon_info.insert(
            "mon.a".to_string(),
            crate::MonInfo {
                name: "mon.a".to_string(),
                public_addrs: EntityAddrvec::new(),
                priority: 1,
                weight: 2,
                crush_loc: BTreeMap::new(),
                time_added: crate::UTime::default(),
            },
        );
        mon_info.insert(
            "mon.b".to_string(),
            crate::MonInfo {
                name: "mon.b".to_string(),
                public_addrs: EntityAddrvec::new(),
                priority: 3,
                weight: 4,
                crush_loc: BTreeMap::new(),
                time_added: crate::UTime::default(),
            },
        );

        let monmap = MonMapState::from_denc_monmap(crate::MonMap {
            fsid: *Uuid::nil().as_bytes(),
            epoch: crate::Epoch::new(7),
            last_changed: crate::UTime::default(),
            created: crate::UTime::default(),
            persistent_features: crate::MonFeature::default(),
            optional_features: crate::MonFeature::default(),
            mon_info,
            ranks: vec!["mon.b".to_string(), "mon.a".to_string()],
            min_mon_release: crate::MonCephRelease::default(),
            removed_ranks: Vec::new(),
            strategy: crate::ElectionStrategy::default(),
            disallowed_leaders: Vec::new(),
            stretch_mode_enabled: false,
            tiebreaker_mon: String::new(),
            stretch_marked_down_mons: Vec::new(),
        })
        .unwrap();

        assert_eq!(monmap.get_name(0), Some("mon.b"));
        assert_eq!(monmap.get_name(1), Some("mon.a"));
        assert_eq!(monmap.get_rank("mon.b"), Some(0));
        assert_eq!(monmap.get_rank("mon.a"), Some(1));
    }

    #[test]
    fn test_to_denc_monmap_preserves_decoded_fields() {
        let mut mon_info = BTreeMap::new();
        mon_info.insert(
            "mon.a".to_string(),
            crate::MonInfo {
                name: "mon.a".to_string(),
                public_addrs: EntityAddrvec::new(),
                priority: 7,
                weight: 8,
                crush_loc: BTreeMap::from([("datacenter".to_string(), "dc1".to_string())]),
                time_added: crate::UTime { sec: 11, nsec: 12 },
            },
        );
        mon_info.insert(
            "mon.orphan".to_string(),
            crate::MonInfo {
                name: "mon.orphan".to_string(),
                public_addrs: EntityAddrvec::new(),
                priority: 9,
                weight: 10,
                crush_loc: BTreeMap::from([("rack".to_string(), "r1".to_string())]),
                time_added: crate::UTime { sec: 21, nsec: 22 },
            },
        );

        let monmap = MonMapState::from_denc_monmap(crate::MonMap {
            fsid: *Uuid::nil().as_bytes(),
            epoch: crate::Epoch::new(3),
            last_changed: crate::UTime { sec: 4, nsec: 5 },
            created: crate::UTime { sec: 6, nsec: 7 },
            persistent_features: crate::MonFeature { features: 0x11 },
            optional_features: crate::MonFeature { features: 0x22 },
            mon_info,
            ranks: vec!["mon.a".to_string()],
            min_mon_release: crate::MonCephRelease::Reef,
            removed_ranks: vec![4, 5],
            strategy: crate::ElectionStrategy::Disallow,
            disallowed_leaders: vec!["mon.a".to_string()],
            stretch_mode_enabled: true,
            tiebreaker_mon: "mon.a".to_string(),
            stretch_marked_down_mons: vec!["mon.c".to_string()],
        })
        .unwrap();

        let denc_monmap = monmap.to_denc_monmap();
        let mon_a = denc_monmap.mon_info.get("mon.a").unwrap();
        let mon_orphan = denc_monmap.mon_info.get("mon.orphan").unwrap();

        assert_eq!(denc_monmap.persistent_features.features, 0x11);
        assert_eq!(denc_monmap.optional_features.features, 0x22);
        assert_eq!(denc_monmap.min_mon_release, crate::MonCephRelease::Reef);
        assert_eq!(denc_monmap.removed_ranks, vec![4, 5]);
        assert_eq!(denc_monmap.strategy, crate::ElectionStrategy::Disallow);
        assert_eq!(denc_monmap.disallowed_leaders, vec!["mon.a".to_string()]);
        assert!(denc_monmap.stretch_mode_enabled);
        assert_eq!(denc_monmap.tiebreaker_mon, "mon.a");
        assert_eq!(
            denc_monmap.stretch_marked_down_mons,
            vec!["mon.c".to_string()]
        );
        assert_eq!(denc_monmap.ranks, vec!["mon.a".to_string()]);
        assert_eq!(
            mon_a.crush_loc.get("datacenter").map(String::as_str),
            Some("dc1")
        );
        assert_eq!(mon_a.time_added.sec, 11);
        assert_eq!(
            mon_orphan.crush_loc.get("rack").map(String::as_str),
            Some("r1")
        );
        assert_eq!(mon_orphan.time_added.sec, 21);
    }

    #[test]
    fn test_parse_mon_addr() {
        let addr = parse_mon_addr("v2:127.0.0.1:3300").unwrap();
        assert!(addr.is_msgr2());

        let addr = parse_mon_addr("127.0.0.1:3300").unwrap();
        assert!(addr.is_msgr2()); // defaults to msgr2

        let addr = parse_mon_addr("v1:127.0.0.1:6789").unwrap();
        assert!(addr.is_legacy());
    }
}
