//! Monitor map structures
//!
//! The MonMap contains information about all monitors in the cluster.

use crate::error::{MonClientError, Result};
use crate::types::EntityAddrVec;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Monitor map
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonMap {
    /// Cluster FSID
    pub fsid: Uuid,

    /// MonMap epoch
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

    /// Address to monitor mapping
    #[serde(skip)]
    addr_to_rank: HashMap<String, usize>,
}

impl MonMap {
    pub fn new(fsid: Uuid) -> Self {
        Self {
            fsid,
            epoch: 0,
            created: 0,
            modified: 0,
            monitors: Vec::new(),
            name_to_rank: HashMap::new(),
            addr_to_rank: HashMap::new(),
        }
    }

    /// Build initial monmap from configuration
    ///
    /// Groups addresses by IP address (host), creating one monitor per unique IP.
    /// Each monitor's EntityAddrVec contains all protocol versions (v1, v2) for that IP.
    ///
    /// This matches C++ Objecter behavior where monitor addresses in ceph.conf like:
    ///   `[v2:192.168.1.43:40472,v1:192.168.1.43:40473] [v2:192.168.1.43:40474,v1:192.168.1.43:40475]`
    /// are parsed as 2 monitors, each with both v1 and v2 addresses.
    pub fn build_initial(mon_addrs: &[String]) -> Result<Self> {
        use crate::types::AddrType;
        use std::net::IpAddr;

        // Group addresses by IP address (host)
        let mut ip_to_addrs: HashMap<IpAddr, Vec<crate::types::EntityAddr>> = HashMap::new();

        for addr_str in mon_addrs {
            let addr = parse_mon_addr(addr_str)?;
            let ip = addr.addr.ip();
            ip_to_addrs.entry(ip).or_default().push(addr);
        }

        // Sort IPs for deterministic ordering
        let mut ips: Vec<IpAddr> = ip_to_addrs.keys().copied().collect();
        ips.sort();

        let mut monmap = Self::new(Uuid::nil());
        monmap.epoch = 0;

        for (rank, ip) in ips.iter().enumerate() {
            let mut addrs = ip_to_addrs.remove(ip).unwrap();

            // Sort addresses: v2 (Msgr2) first, then v1 (Legacy)
            // This ensures get_msgr2() finds v2 addresses first
            addrs.sort_by_key(|a| match a.addr_type {
                AddrType::Msgr2 => 0,
                AddrType::Legacy => 1,
            });

            let mon_info = MonInfo {
                name: format!("mon.{}", rank),
                rank,
                addrs: EntityAddrVec { addrs },
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

        // Build address mapping
        for addr in &mon.addrs.addrs {
            self.addr_to_rank.insert(addr.to_string(), rank);
        }

        self.name_to_rank.insert(name, rank);
        self.monitors.push(mon);
    }

    /// Get number of monitors
    pub fn size(&self) -> usize {
        self.monitors.len()
    }

    /// Get monitor by rank
    pub fn get_mon(&self, rank: usize) -> Option<&MonInfo> {
        self.monitors.get(rank)
    }

    /// Get monitor addresses by rank
    pub fn get_addrs(&self, rank: usize) -> Option<&EntityAddrVec> {
        self.monitors.get(rank).map(|m| &m.addrs)
    }

    /// Get monitor name by rank
    pub fn get_name(&self, rank: usize) -> Option<&str> {
        self.monitors.get(rank).map(|m| m.name.as_str())
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
        (0..self.monitors.len()).collect()
    }

    /// Get monitors grouped by priority (lowest priority first)
    /// Returns monitors with the same priority grouped together
    pub fn get_monitors_by_priority(&self) -> Vec<Vec<usize>> {
        use std::collections::BTreeMap;

        // Group monitors by priority
        let mut priority_groups: BTreeMap<i32, Vec<usize>> = BTreeMap::new();
        for mon in &self.monitors {
            priority_groups
                .entry(mon.priority)
                .or_default()
                .push(mon.rank);
        }

        // Return groups in priority order (lowest priority first)
        priority_groups.into_values().collect()
    }

    /// Get the weight of a monitor by rank
    pub fn get_weight(&self, rank: usize) -> u16 {
        self.monitors.get(rank).map(|m| m.weight).unwrap_or(0)
    }

    /// Rebuild internal indices after deserialization
    pub fn rebuild_indices(&mut self) {
        self.name_to_rank.clear();
        self.addr_to_rank.clear();

        for mon in &self.monitors {
            self.name_to_rank.insert(mon.name.clone(), mon.rank);
            for addr in &mon.addrs.addrs {
                self.addr_to_rank.insert(addr.to_string(), mon.rank);
            }
        }
    }

    /// Encode monmap to bytes (for sending in messages)
    pub fn encode(&self) -> Result<bytes::Bytes> {
        use bytes::BytesMut;
        use denc::Denc;

        // Convert to denc MonMap
        let denc_monmap = self.to_denc_monmap();

        // Encode with full features
        let mut buf = BytesMut::new();
        denc_monmap
            .encode(&mut buf, u64::MAX)
            .map_err(|e| MonClientError::DecodingError(e.to_string()))?;

        Ok(buf.freeze())
    }

    /// Decode monmap from bytes
    pub fn decode(data: &[u8]) -> Result<Self> {
        use denc::Denc;

        // Decode using denc
        let mut bytes = bytes::Bytes::from(data.to_vec());
        let denc_monmap = denc::MonMap::decode(&mut bytes, u64::MAX)
            .map_err(|e| MonClientError::DecodingError(e.to_string()))?;

        // Convert to monclient MonMap
        Self::from_denc_monmap(denc_monmap)
    }

    /// Convert to denc MonMap for encoding
    fn to_denc_monmap(&self) -> denc::MonMap {
        use denc::UTime;
        use std::collections::BTreeMap;

        // Convert fsid from Uuid to [u8; 16]
        let fsid = *self.fsid.as_bytes();

        // Convert monitors to mon_info BTreeMap
        let mut mon_info = BTreeMap::new();
        for mon in &self.monitors {
            // Convert EntityAddrVec to EntityAddrvec
            let mut public_addrs = denc::EntityAddrvec::new();
            for addr in &mon.addrs.addrs {
                let denc_addr_type = match addr.addr_type {
                    crate::types::AddrType::Legacy => denc::EntityAddrType::Legacy,
                    crate::types::AddrType::Msgr2 => denc::EntityAddrType::Msgr2,
                };
                let denc_addr = denc::EntityAddr::from_socket_addr(denc_addr_type, addr.addr);
                public_addrs.addrs.push(denc_addr);
            }

            let denc_mon = denc::MonInfo {
                name: mon.name.clone(),
                public_addrs,
                priority: mon.priority as u16,
                weight: mon.weight,
                crush_loc: BTreeMap::new(),
                time_added: UTime::default(),
            };
            mon_info.insert(mon.name.clone(), denc_mon);
        }

        // Build ranks from monitors
        let ranks: Vec<String> = self.monitors.iter().map(|m| m.name.clone()).collect();

        denc::MonMap {
            fsid,
            epoch: self.epoch,
            last_changed: UTime {
                sec: (self.modified / 1000) as u32,
                nsec: ((self.modified % 1000) * 1_000_000) as u32,
            },
            created: UTime {
                sec: (self.created / 1000) as u32,
                nsec: ((self.created % 1000) * 1_000_000) as u32,
            },
            persistent_features: denc::MonFeature::default(),
            optional_features: denc::MonFeature::default(),
            mon_info,
            ranks,
            min_mon_release: denc::MonCephRelease::default(),
            removed_ranks: Vec::new(),
            strategy: denc::ElectionStrategy::default(),
            disallowed_leaders: Vec::new(),
            stretch_mode_enabled: false,
            tiebreaker_mon: String::new(),
            stretch_marked_down_mons: Vec::new(),
        }
    }

    /// Convert from denc MonMap after decoding
    fn from_denc_monmap(denc_monmap: denc::MonMap) -> Result<Self> {
        use crate::types::{AddrType, EntityAddr, EntityAddrVec};

        // Convert fsid from [u8; 16] to Uuid
        let fsid = uuid::Uuid::from_bytes(denc_monmap.fsid);

        // Convert UTime to Unix timestamp (milliseconds)
        let created =
            (denc_monmap.created.sec as u64) * 1000 + (denc_monmap.created.nsec as u64) / 1_000_000;
        let modified = (denc_monmap.last_changed.sec as u64) * 1000
            + (denc_monmap.last_changed.nsec as u64) / 1_000_000;

        // Convert mon_info to monitors
        let mut monitors = Vec::new();
        for (rank, (name, mon)) in denc_monmap.mon_info.iter().enumerate() {
            // Convert EntityAddrvec to EntityAddrVec
            let mut addrs = EntityAddrVec::new();
            for denc_addr in &mon.public_addrs.addrs {
                if let Some(socket_addr) = denc_addr.to_socket_addr() {
                    let addr_type = match denc_addr.addr_type {
                        denc::EntityAddrType::Legacy => AddrType::Legacy,
                        denc::EntityAddrType::Msgr2 => AddrType::Msgr2,
                        denc::EntityAddrType::None
                        | denc::EntityAddrType::Any
                        | denc::EntityAddrType::Cidr => {
                            // These types should not appear in monitor addresses
                            // Default to Msgr2 for robustness
                            AddrType::Msgr2
                        }
                    };
                    let addr = EntityAddr {
                        addr_type,
                        nonce: denc_addr.nonce,
                        addr: socket_addr,
                    };
                    addrs.addrs.push(addr);
                }
            }

            monitors.push(MonInfo {
                name: name.clone(),
                rank,
                addrs,
                priority: mon.priority as i32,
                weight: mon.weight,
            });
        }

        let mut monmap = Self {
            fsid,
            epoch: denc_monmap.epoch,
            created,
            modified,
            monitors,
            name_to_rank: HashMap::new(),
            addr_to_rank: HashMap::new(),
        };

        monmap.rebuild_indices();
        Ok(monmap)
    }
}

impl Default for MonMap {
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
    pub addrs: EntityAddrVec,

    /// Priority (for election)
    pub priority: i32,

    /// Weight (for client selection)
    pub weight: u16,
}

/// Parse monitor address string
fn parse_mon_addr(addr_str: &str) -> Result<crate::types::EntityAddr> {
    use crate::types::{AddrType, EntityAddr};
    use std::net::SocketAddr;

    // Format: "v2:127.0.0.1:3300" or "127.0.0.1:3300"
    let (addr_type, addr_part) = if let Some(stripped) = addr_str.strip_prefix("v2:") {
        (AddrType::Msgr2, stripped)
    } else if let Some(stripped) = addr_str.strip_prefix("v1:") {
        (AddrType::Legacy, stripped)
    } else {
        // Default to msgr2
        (AddrType::Msgr2, addr_str)
    };

    let socket_addr: SocketAddr = addr_part.parse().map_err(|e| {
        MonClientError::InvalidMonMap(format!("Invalid address {}: {}", addr_part, e))
    })?;

    Ok(EntityAddr::new(addr_type, socket_addr))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_initial_monmap() {
        // Test with multiple IPs - each IP becomes one monitor
        let addrs = vec![
            "v2:127.0.0.1:3300".to_string(),
            "v2:127.0.0.2:3301".to_string(),
            "v2:127.0.0.3:3302".to_string(),
        ];

        let monmap = MonMap::build_initial(&addrs).unwrap();
        assert_eq!(monmap.size(), 3);
        assert_eq!(monmap.get_name(0), Some("mon.0"));
        assert!(monmap.get_addrs(0).is_some());
    }

    #[test]
    fn test_build_initial_monmap_groups_by_ip() {
        // Test that addresses with same IP are grouped into one monitor
        // This matches Ceph's behavior: [v2:IP:PORT1,v1:IP:PORT2] = one monitor with 2 addrs
        let addrs = vec![
            "v2:192.168.1.1:6789".to_string(),
            "v1:192.168.1.1:6790".to_string(),
            "v2:192.168.1.2:6789".to_string(),
            "v1:192.168.1.2:6790".to_string(),
        ];

        let monmap = MonMap::build_initial(&addrs).unwrap();

        // Should create 2 monitors (one per unique IP)
        assert_eq!(monmap.size(), 2);

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
    fn test_monmap_lookup() {
        let mut monmap = MonMap::new(Uuid::nil());

        let mon = MonInfo {
            name: "mon.a".to_string(),
            rank: 0,
            addrs: EntityAddrVec::new(),
            priority: 0,
            weight: 0,
        };

        monmap.add_monitor(mon);

        assert!(monmap.contains("mon.a"));
        assert_eq!(monmap.get_rank("mon.a"), Some(0));
        assert_eq!(monmap.get_name(0), Some("mon.a"));
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
