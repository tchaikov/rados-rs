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
    pub fn build_initial(mon_addrs: &[String]) -> Result<Self> {
        let mut monmap = Self::new(Uuid::nil());
        monmap.epoch = 0;

        for (rank, addr_str) in mon_addrs.iter().enumerate() {
            // Parse address string (e.g., "v2:127.0.0.1:3300")
            let addr = parse_mon_addr(addr_str)?;

            let mon_info = MonInfo {
                name: format!("mon.{}", rank),
                rank,
                addrs: EntityAddrVec::with_addr(addr),
                priority: 0,
                weight: 0,
            };

            monmap.add_monitor(mon_info);
        }

        Ok(monmap)
    }

    /// Build initial monmap from CEPH_MON_ADDR environment variable
    pub fn build_from_env() -> Result<Self> {
        let mon_addr = std::env::var("CEPH_MON_ADDR")
            .map_err(|_| MonClientError::InvalidMonMap("CEPH_MON_ADDR not set".into()))?;

        // CEPH_MON_ADDR can be a single address or comma-separated list
        let addrs: Vec<String> = mon_addr
            .split(',')
            .map(|s| {
                let s = s.trim();
                // If no protocol prefix, assume v2
                if s.starts_with("v1:") || s.starts_with("v2:") {
                    s.to_string()
                } else {
                    format!("v2:{}", s)
                }
            })
            .collect();

        Self::build_initial(&addrs)
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
        // TODO: Implement proper Ceph encoding
        // For now, use JSON as placeholder
        let json = serde_json::to_vec(self)
            .map_err(|e: serde_json::Error| MonClientError::EncodingError(e.to_string()))?;
        Ok(bytes::Bytes::from(json))
    }

    /// Decode monmap from bytes
    pub fn decode(data: &[u8]) -> Result<Self> {
        // TODO: Implement proper Ceph decoding
        // For now, use JSON as placeholder
        let mut monmap: Self = serde_json::from_slice(data)
            .map_err(|e: serde_json::Error| MonClientError::DecodingError(e.to_string()))?;
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
        let addrs = vec![
            "v2:127.0.0.1:3300".to_string(),
            "v2:127.0.0.1:3301".to_string(),
            "v2:127.0.0.1:3302".to_string(),
        ];

        let monmap = MonMap::build_initial(&addrs).unwrap();
        assert_eq!(monmap.size(), 3);
        assert_eq!(monmap.get_name(0), Some("mon.0"));
        assert!(monmap.get_addrs(0).is_some());
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
