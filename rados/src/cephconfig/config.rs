//! Ceph configuration file parser and accessor.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::cephconfig::ConfigError;

use crate::auth::protocol::{CEPH_AUTH_CEPHX, CEPH_AUTH_GSS, CEPH_AUTH_NONE};

/// Represents a parsed Ceph configuration.
#[derive(Debug, Clone)]
pub struct CephConfig {
    sections: HashMap<String, HashMap<String, String>>,
}

impl CephConfig {
    /// Parse a Ceph configuration file from the given path.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let content = fs::read_to_string(path)?;
        Self::parse(&content)
    }

    /// Parse a Ceph configuration from a string.
    pub fn parse(content: &str) -> Result<Self, ConfigError> {
        let mut sections: HashMap<String, HashMap<String, String>> = HashMap::new();
        let mut current_section = String::from("global");

        for line in content.lines() {
            let line = line.trim();

            if line.is_empty() || line.starts_with(';') || line.starts_with('#') {
                continue;
            }

            if line.starts_with('[') && line.ends_with(']') {
                current_section = line[1..line.len() - 1].to_string();
                sections.entry(current_section.clone()).or_default();
                continue;
            }

            if let Some(eq_pos) = line.find('=') {
                // Normalize key: trim whitespace and replace spaces with underscores.
                // Mirrors C++ ConfUtils::normalize_key_name().
                let key = line[..eq_pos].trim().replace(' ', "_");
                // Strip inline comments (';' or '#') — mirrors C++ ConfUtils.
                let raw_value = &line[eq_pos + 1..];
                let value = raw_value
                    .find(';')
                    .or_else(|| raw_value.find('#'))
                    .map_or(raw_value, |pos| &raw_value[..pos])
                    .trim()
                    .to_string();

                sections
                    .entry(current_section.clone())
                    .or_default()
                    .insert(key, value);
            }
        }

        Ok(Self { sections })
    }

    /// Get a configuration value from a specific section.
    pub fn get(&self, section: &str, key: &str) -> Option<&str> {
        self.sections
            .get(section)
            .and_then(|s| s.get(key))
            .map(|v| v.as_str())
    }

    /// Get a configuration value, checking multiple sections in order.
    /// Typically checks: specific section -> client -> global.
    pub fn get_with_fallback(&self, sections: &[&str], key: &str) -> Option<&str> {
        sections.iter().find_map(|section| self.get(section, key))
    }

    /// Get monitor addresses.
    ///
    /// Parses the "mon_host" configuration option and returns a list of monitor addresses.
    /// Supports both v2 and v1 protocol addresses.
    pub fn mon_addrs(&self) -> Result<Vec<String>, ConfigError> {
        let mon_host = self
            .get_with_fallback(&["global", "client"], "mon_host")
            .ok_or_else(|| ConfigError::MissingOption("mon_host".to_string()))?;

        let addrs: Vec<String> = mon_host
            .split_whitespace()
            .flat_map(|part| {
                let part = part.trim_start_matches('[').trim_end_matches(']');
                part.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(String::from)
            })
            .collect();

        if addrs.is_empty() {
            return Err(ConfigError::ParseError(
                "No monitor addresses found in 'mon host'".to_string(),
            ));
        }

        Ok(addrs)
    }

    /// Get the first v2 monitor address.
    ///
    /// This is a convenience method for getting a single v2 monitor address,
    /// which is commonly needed for initial connection.
    pub fn first_v2_mon_addr(&self) -> Result<String, ConfigError> {
        self.mon_addrs()?
            .into_iter()
            .find(|addr| addr.starts_with("v2:"))
            .ok_or_else(|| ConfigError::ParseError("No v2 monitor address found".to_string()))
    }

    /// Get keyring file path.
    pub fn keyring(&self) -> Result<String, ConfigError> {
        self.get_with_fallback(&["client", "global"], "keyring")
            .map(|s| s.to_string())
            .ok_or_else(|| ConfigError::MissingOption("keyring".to_string()))
    }

    /// Get entity name (defaults to "client.admin" if not specified).
    pub fn entity_name(&self) -> String {
        self.get_with_fallback(&["client", "global"], "entity_name")
            .unwrap_or("client.admin")
            .to_string()
    }

    /// Get required authentication methods for clients.
    ///
    /// Checks "auth_supported" first (if set, applies to all connections),
    /// otherwise checks "auth_client_required". Returns list of supported
    /// auth method constants (CEPH_AUTH_NONE=1, CEPH_AUTH_CEPHX=2, etc.).
    ///
    /// Defaults to [CEPH_AUTH_CEPHX] if not specified.
    ///
    /// Reference: AuthRegistry::refresh_config() in src/auth/AuthRegistry.cc
    pub fn get_auth_client_required(&self) -> Vec<u32> {
        self.resolve_auth_methods("auth_client_required", &["global", "client"])
    }

    /// Shared helper for `get_auth_client_required`.
    ///
    /// First checks "auth_supported" in global. If that yields results, returns them.
    /// Otherwise looks up `fallback_key` in the given `fallback_sections`.
    /// Defaults to `[CEPH_AUTH_CEPHX]` if nothing is configured.
    fn resolve_auth_methods(&self, fallback_key: &str, fallback_sections: &[&str]) -> Vec<u32> {
        // Check auth_supported first (applies to all if set)
        if let Some(auth_supported) = self.get("global", "auth_supported") {
            let methods = parse_auth_methods(auth_supported);
            if !methods.is_empty() {
                return methods;
            }
        }

        let auth_value = self
            .get_with_fallback(fallback_sections, fallback_key)
            .unwrap_or("cephx");

        let methods = parse_auth_methods(auth_value);
        if methods.is_empty() {
            vec![CEPH_AUTH_CEPHX]
        } else {
            methods
        }
    }

    /// Get the DNS SRV service name for monitor discovery.
    ///
    /// Returns the value of the `mon_dns_srv_name` configuration option,
    /// which defaults to `"ceph-mon"` if not specified.
    /// The name may include a domain suffix separated by `_`,
    /// e.g., `"ceph-mon_example.com"`.
    pub fn mon_dns_srv_name(&self) -> String {
        self.get_with_fallback(&["global", "client"], "mon_dns_srv_name")
            .unwrap_or("ceph-mon")
            .to_string()
    }

    /// Get all sections in the configuration.
    pub fn sections(&self) -> Vec<&str> {
        self.sections.keys().map(|s| s.as_str()).collect()
    }

    /// Get all keys in a section.
    pub fn keys(&self, section: &str) -> Vec<&str> {
        self.sections
            .get(section)
            .map(|s| s.keys().map(|k| k.as_str()).collect())
            .unwrap_or_default()
    }
}

/// Parse comma-separated auth method names into method constants.
///
/// Recognizes "cephx", "none", and "gss". Unknown names are silently skipped.
fn parse_auth_methods(methods_str: &str) -> Vec<u32> {
    methods_str
        .split(',')
        .filter_map(|s| match s.trim().to_lowercase().as_str() {
            "none" => Some(CEPH_AUTH_NONE),
            "cephx" => Some(CEPH_AUTH_CEPHX),
            "gss" => Some(CEPH_AUTH_GSS),
            _ => None,
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Tests for config parsing and access
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_CONFIG: &str = r#"
; Test configuration
[global]
fsid = 7150dbe1-1803-44b9-9a3d-b893308fd02e
mon host = [v2:192.168.1.43:40472,v1:192.168.1.43:40473] [v2:192.168.1.43:40474,v1:192.168.1.43:40475]
ms_dispatch_throttle_bytes = 50M
ms_max_connections = 200
ms_connection_timeout = 60s
ms_compression_enabled = true
ms_compression_ratio = 0.75

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
        // Keys are normalized: spaces → underscores (mirrors C++ ConfUtils).
        assert_eq!(config.get("mon", "debug_mon"), Some("20"));
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
        assert_eq!(config.entity_name(), "client.admin");
    }

    #[test]
    fn test_mon_dns_srv_name_default() {
        let config = CephConfig::parse(TEST_CONFIG).unwrap();
        assert_eq!(config.mon_dns_srv_name(), "ceph-mon");
    }

    #[test]
    fn test_mon_dns_srv_name_configured() {
        let config_str = r#"
[global]
mon_dns_srv_name = ceph-mon_example.com
"#;
        let config = CephConfig::parse(config_str).unwrap();
        assert_eq!(config.mon_dns_srv_name(), "ceph-mon_example.com");
    }

    #[test]
    fn test_get_with_fallback() {
        let config = CephConfig::parse(TEST_CONFIG).unwrap();

        assert_eq!(
            config.get_with_fallback(&["client", "global"], "fsid"),
            Some("7150dbe1-1803-44b9-9a3d-b893308fd02e")
        );
        assert_eq!(
            config.get_with_fallback(&["client", "global"], "keyring"),
            Some("/home/kefu/dev/ceph/build/keyring")
        );
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
}
