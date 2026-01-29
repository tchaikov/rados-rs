//! Ceph configuration file parser
//!
//! This crate provides functionality to parse Ceph configuration files (ceph.conf)
//! and extract configuration options.
//!
//! # Example
//!
//! ```no_run
//! use cephconfig::CephConfig;
//!
//! let config = CephConfig::from_file("/etc/ceph/ceph.conf").unwrap();
//! let mon_addrs = config.mon_addrs();
//! let keyring = config.keyring();
//! ```

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Failed to read config file: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Failed to parse config file: {0}")]
    ParseError(String),

    #[error("Missing required option: {0}")]
    MissingOption(String),
}

/// Represents a parsed Ceph configuration
#[derive(Debug, Clone)]
pub struct CephConfig {
    sections: HashMap<String, HashMap<String, String>>,
}

impl CephConfig {
    /// Parse a Ceph configuration file from the given path
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let content = fs::read_to_string(path)?;
        Self::parse(&content)
    }

    /// Parse a Ceph configuration from a string
    pub fn parse(content: &str) -> Result<Self, ConfigError> {
        let mut sections: HashMap<String, HashMap<String, String>> = HashMap::new();
        let mut current_section = String::from("global");

        for line in content.lines() {
            let line = line.trim();

            // Skip empty lines and comments
            if line.is_empty() || line.starts_with(';') || line.starts_with('#') {
                continue;
            }

            // Section header
            if line.starts_with('[') && line.ends_with(']') {
                current_section = line[1..line.len() - 1].to_string();
                sections.entry(current_section.clone()).or_default();
                continue;
            }

            // Key-value pair
            if let Some(eq_pos) = line.find('=') {
                let key = line[..eq_pos].trim().to_string();
                let value = line[eq_pos + 1..].trim().to_string();

                sections
                    .entry(current_section.clone())
                    .or_default()
                    .insert(key, value);
            }
        }

        Ok(Self { sections })
    }

    /// Get a configuration value from a specific section
    pub fn get(&self, section: &str, key: &str) -> Option<&str> {
        self.sections
            .get(section)
            .and_then(|s| s.get(key))
            .map(|v| v.as_str())
    }

    /// Get a configuration value, checking multiple sections in order
    /// Typically checks: specific section -> client -> global
    pub fn get_with_fallback(&self, sections: &[&str], key: &str) -> Option<&str> {
        for section in sections {
            if let Some(value) = self.get(section, key) {
                return Some(value);
            }
        }
        None
    }

    /// Get monitor addresses
    ///
    /// Parses the "mon host" configuration option and returns a list of monitor addresses.
    /// Supports both v2 and v1 protocol addresses.
    pub fn mon_addrs(&self) -> Result<Vec<String>, ConfigError> {
        let mon_host = self
            .get_with_fallback(&["global", "client"], "mon host")
            .ok_or_else(|| ConfigError::MissingOption("mon host".to_string()))?;

        // Parse monitor addresses from the format:
        // [v2:192.168.1.43:40472,v1:192.168.1.43:40473] [v2:192.168.1.43:40474,v1:192.168.1.43:40475]
        let mut addrs = Vec::new();

        for part in mon_host.split_whitespace() {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            // Remove brackets if present
            let part = part.trim_start_matches('[').trim_end_matches(']');

            // Split by comma to get individual addresses
            for addr in part.split(',') {
                let addr = addr.trim();
                if !addr.is_empty() {
                    addrs.push(addr.to_string());
                }
            }
        }

        if addrs.is_empty() {
            return Err(ConfigError::ParseError(
                "No monitor addresses found in 'mon host'".to_string(),
            ));
        }

        Ok(addrs)
    }

    /// Get the first v2 monitor address
    ///
    /// This is a convenience method for getting a single v2 monitor address,
    /// which is commonly needed for initial connection.
    pub fn first_v2_mon_addr(&self) -> Result<String, ConfigError> {
        let addrs = self.mon_addrs()?;

        addrs
            .into_iter()
            .find(|addr| addr.starts_with("v2:"))
            .ok_or_else(|| ConfigError::ParseError("No v2 monitor address found".to_string()))
    }

    /// Get keyring file path
    pub fn keyring(&self) -> Result<String, ConfigError> {
        self.get_with_fallback(&["client", "global"], "keyring")
            .map(|s| s.to_string())
            .ok_or_else(|| ConfigError::MissingOption("keyring".to_string()))
    }

    /// Get entity name (defaults to "client.admin" if not specified)
    pub fn entity_name(&self) -> String {
        self.get_with_fallback(&["client", "global"], "entity name")
            .unwrap_or("client.admin")
            .to_string()
    }

    /// Get all sections in the configuration
    pub fn sections(&self) -> Vec<&str> {
        self.sections.keys().map(|s| s.as_str()).collect()
    }

    /// Get all keys in a section
    pub fn keys(&self, section: &str) -> Vec<&str> {
        self.sections
            .get(section)
            .map(|s| s.keys().map(|k| k.as_str()).collect())
            .unwrap_or_default()
    }
}

/// Configuration builder with macro support for defining options
///
/// This provides a convenient way to define and access configuration options
/// with type safety and default values.
#[macro_export]
macro_rules! define_config {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $(
                $(#[$field_meta:meta])*
                $field:ident: $ty:ty = $default:expr,
            )*
        }
    ) => {
        $(#[$meta])*
        $vis struct $name {
            $(
                $(#[$field_meta])*
                pub $field: $ty,
            )*
        }

        impl $name {
            /// Create a new configuration with default values
            pub fn new() -> Self {
                Self {
                    $(
                        $field: $default,
                    )*
                }
            }

            /// Load configuration from a Ceph config file
            pub fn from_ceph_config(config: &$crate::CephConfig) -> Result<Self, $crate::ConfigError> {
                let mut result = Self::new();
                result.apply_ceph_config(config)?;
                Ok(result)
            }

            /// Apply values from a Ceph config file
            pub fn apply_ceph_config(&mut self, _config: &$crate::CephConfig) -> Result<(), $crate::ConfigError> {
                // This is a placeholder - specific implementations should override
                Ok(())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_CONFIG: &str = r#"
; Test configuration
[global]
fsid = 7150dbe1-1803-44b9-9a3d-b893308fd02e
mon host = [v2:192.168.1.43:40472,v1:192.168.1.43:40473] [v2:192.168.1.43:40474,v1:192.168.1.43:40475]

[client]
keyring = /home/kefu/dev/ceph/build/keyring
log file = /home/kefu/dev/ceph/build/out/$name.$pid.log

[mon]
debug mon = 20
"#;

    #[test]
    fn test_parse_config() {
        let config = CephConfig::parse(TEST_CONFIG).unwrap();

        assert_eq!(
            config.get("global", "fsid"),
            Some("7150dbe1-1803-44b9-9a3d-b893308fd02e")
        );
        assert_eq!(
            config.get("client", "keyring"),
            Some("/home/kefu/dev/ceph/build/keyring")
        );
        assert_eq!(config.get("mon", "debug mon"), Some("20"));
    }

    #[test]
    fn test_mon_addrs() {
        let config = CephConfig::parse(TEST_CONFIG).unwrap();
        let addrs = config.mon_addrs().unwrap();

        assert_eq!(addrs.len(), 4);
        assert!(addrs.contains(&"v2:192.168.1.43:40472".to_string()));
        assert!(addrs.contains(&"v1:192.168.1.43:40473".to_string()));
        assert!(addrs.contains(&"v2:192.168.1.43:40474".to_string()));
        assert!(addrs.contains(&"v1:192.168.1.43:40475".to_string()));
    }

    #[test]
    fn test_first_v2_mon_addr() {
        let config = CephConfig::parse(TEST_CONFIG).unwrap();
        let addr = config.first_v2_mon_addr().unwrap();

        assert!(addr.starts_with("v2:"));
        assert!(addr.contains("192.168.1.43"));
    }

    #[test]
    fn test_keyring() {
        let config = CephConfig::parse(TEST_CONFIG).unwrap();
        let keyring = config.keyring().unwrap();

        assert_eq!(keyring, "/home/kefu/dev/ceph/build/keyring");
    }

    #[test]
    fn test_entity_name_default() {
        let config = CephConfig::parse(TEST_CONFIG).unwrap();
        let entity_name = config.entity_name();

        assert_eq!(entity_name, "client.admin");
    }

    #[test]
    fn test_get_with_fallback() {
        let config = CephConfig::parse(TEST_CONFIG).unwrap();

        // Should find in global
        assert_eq!(
            config.get_with_fallback(&["client", "global"], "fsid"),
            Some("7150dbe1-1803-44b9-9a3d-b893308fd02e")
        );

        // Should find in client
        assert_eq!(
            config.get_with_fallback(&["client", "global"], "keyring"),
            Some("/home/kefu/dev/ceph/build/keyring")
        );

        // Should not find
        assert_eq!(
            config.get_with_fallback(&["client", "global"], "nonexistent"),
            None
        );
    }

    #[test]
    fn test_sections() {
        let config = CephConfig::parse(TEST_CONFIG).unwrap();
        let sections = config.sections();

        assert!(sections.contains(&"global"));
        assert!(sections.contains(&"client"));
        assert!(sections.contains(&"mon"));
    }

    #[test]
    #[allow(dead_code)]
    fn test_define_config_macro() {
        define_config! {
            /// Test configuration
            pub struct TestConfig {
                /// Monitor addresses
                mon_addrs: Vec<String> = vec![],
                /// Keyring path
                keyring: String = "/etc/ceph/keyring".to_string(),
            }
        }

        let config = TestConfig::new();
        assert_eq!(config.mon_addrs.len(), 0);
        assert_eq!(config.keyring, "/etc/ceph/keyring");

        let config = TestConfig::default();
        assert_eq!(config.keyring, "/etc/ceph/keyring");
    }
}
