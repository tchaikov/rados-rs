use crate::auth::Keyring;
use crate::error::{Error, Result};
use crate::types::{EntityAddr, EntityName, FeatureSet};
use configparser::ini::Ini;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use tracing::debug;

#[derive(Debug, Clone)]
pub struct CephConfig {
    pub fsid: String,
    pub keyring_path: Option<PathBuf>,
    pub mon_host: Vec<EntityAddr>,
    pub auth_cluster_required: String,
    pub auth_service_required: String,
    pub auth_client_required: String,
    pub public_network: Option<String>,
    pub cluster_network: Option<String>,
    pub msgr2_enabled: bool,
    pub port_range: (u16, u16),
    pub sections: HashMap<String, HashMap<String, String>>,
}

impl CephConfig {
    pub fn new() -> Self {
        Self {
            fsid: String::new(),
            keyring_path: None,
            mon_host: Vec::new(),
            auth_cluster_required: "cephx".to_string(),
            auth_service_required: "cephx".to_string(),
            auth_client_required: "cephx".to_string(),
            public_network: None,
            cluster_network: None,
            msgr2_enabled: true,
            port_range: (6800, 7300),
            sections: HashMap::new(),
        }
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut ini = Ini::new();
        let path_str = path.as_ref().to_string_lossy();
        ini.load(&path_str)
            .map_err(|e| Error::InvalidConfig(format!("Failed to load config: {}", e)))?;

        let mut config = Self::new();
        let mut sections = HashMap::new();

        for section_name in ini.sections() {
            let section_map = ini.get_map_ref().get(&section_name).cloned().unwrap_or_default();
            let mut current_section = HashMap::new();

            for (key, value) in section_map.iter() {
                let value_str = value.clone().unwrap_or_default();
                current_section.insert(key.clone(), value_str.clone());

                // Parse global configuration
                if section_name == "global" {
                    match key.as_str() {
                        "fsid" => config.fsid = value_str,
                        "keyring" => config.keyring_path = Some(PathBuf::from(value_str)),
                        "mon host" | "mon_host" => {
                            config.mon_host = parse_mon_hosts(&value_str)?;
                        }
                        "auth_cluster_required" => config.auth_cluster_required = value_str,
                        "auth_service_required" => config.auth_service_required = value_str,
                        "auth_client_required" => config.auth_client_required = value_str,
                        "public_network" => config.public_network = Some(value_str),
                        "cluster_network" => config.cluster_network = Some(value_str),
                        "ms bind msgr2" | "ms_bind_msgr2" => {
                            config.msgr2_enabled = parse_bool(&value_str);
                        }
                        "ms bind port min" | "ms_bind_port_min" => {
                            if let Ok(port) = value_str.parse::<u16>() {
                                config.port_range.0 = port;
                            }
                        }
                        "ms bind port max" | "ms_bind_port_max" => {
                            if let Ok(port) = value_str.parse::<u16>() {
                                config.port_range.1 = port;
                            }
                        }
                        _ => {}
                    }
                }
            }

            sections.insert(section_name, current_section);
        }

        config.sections = sections;
        Ok(config)
    }

    pub fn load_keyring(&self) -> Result<Keyring> {
        if let Some(keyring_path) = &self.keyring_path {
            Keyring::from_file(keyring_path)
        } else {
            // Try default locations
            let default_paths = [
                "/etc/ceph/ceph.client.admin.keyring",
                "~/.ceph/ceph.client.admin.keyring",
                "./ceph.client.admin.keyring",
            ];

            for path_str in &default_paths {
                let path = PathBuf::from(path_str);
                if path.exists() {
                    return Keyring::from_file(path);
                }
            }

            Err(Error::InvalidConfig("No keyring file found".into()))
        }
    }

    pub fn get_section_value(&self, section: &str, key: &str) -> Option<&str> {
        self.sections.get(section)?.get(key).map(|s| s.as_str())
    }

    pub fn is_cephx_enabled(&self) -> bool {
        self.auth_client_required == "cephx" || self.auth_service_required == "cephx"
    }

    pub fn get_mon_addresses(&self) -> Vec<SocketAddr> {
        self.mon_host.iter().map(|addr| addr.addr).collect()
    }

    pub fn get_features(&self) -> FeatureSet {
        let mut features = FeatureSet::EMPTY;
        
        if self.msgr2_enabled {
            features = features.union(FeatureSet::MSGR2);
        }

        // Try with minimal features first - just MSGR2
        debug!("Using minimal feature set for troubleshooting");
        features
        
        // Original feature set - commented out for now
        // // Add other features based on configuration
        // features = features.union(FeatureSet::SERVER_QUINCY);
        // 
        // features
    }
}

impl Default for CephConfig {
    fn default() -> Self {
        Self::new()
    }
}

fn parse_mon_hosts(value: &str) -> Result<Vec<EntityAddr>> {
    let mut addresses = Vec::new();
    
    for host_spec in value.split(',') {
        let host_spec = host_spec.trim();
        
        // Handle v2: prefix for msgr2
        let addr_part = if host_spec.starts_with("v2:") {
            &host_spec[3..]
        } else if host_spec.starts_with("v1:") {
            &host_spec[3..]
        } else {
            host_spec
        };

        let socket_addr = addr_part.parse::<SocketAddr>()
            .map_err(|e| Error::InvalidConfig(format!("Invalid monitor address '{}': {}", addr_part, e)))?;

        addresses.push(EntityAddr::new_with_random_nonce(socket_addr));
    }

    Ok(addresses)
}

fn parse_bool(value: &str) -> bool {
    match value.to_lowercase().as_str() {
        "true" | "yes" | "1" | "on" => true,
        "false" | "no" | "0" | "off" => false,
        _ => false,
    }
}

#[derive(Debug, Clone)]
pub struct CephContext {
    pub config: CephConfig,
    pub keyring: Keyring,
    pub entity_name: EntityName,
}

impl CephContext {
    pub fn new(config_path: impl AsRef<Path>, entity_name: EntityName) -> Result<Self> {
        let config = CephConfig::from_file(config_path)?;
        let keyring = config.load_keyring()?;

        Ok(Self {
            config,
            keyring,
            entity_name,
        })
    }

    pub fn from_config_and_keyring(
        config: CephConfig,
        keyring: Keyring,
        entity_name: EntityName,
    ) -> Self {
        Self {
            config,
            keyring,
            entity_name,
        }
    }

    pub fn get_entity_key(&self) -> Option<&crate::auth::CephXKey> {
        let entity_str = format!("{}", self.entity_name);
        self.keyring.get_key(&entity_str)
    }

    pub fn get_features(&self) -> FeatureSet {
        self.config.get_features()
    }

    pub fn get_mon_addresses(&self) -> Vec<SocketAddr> {
        self.config.get_mon_addresses()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_mon_hosts() {
        let hosts = "v2:127.0.0.1:6789,v2:192.168.1.1:6789";
        let addresses = parse_mon_hosts(hosts).unwrap();
        assert_eq!(addresses.len(), 2);
        assert_eq!(addresses[0].addr.port(), 6789);
        assert_eq!(addresses[1].addr.port(), 6789);
    }

    #[test]
    fn test_parse_bool() {
        assert!(parse_bool("true"));
        assert!(parse_bool("yes"));
        assert!(parse_bool("1"));
        assert!(parse_bool("on"));
        assert!(!parse_bool("false"));
        assert!(!parse_bool("no"));
        assert!(!parse_bool("0"));
        assert!(!parse_bool("off"));
    }
}