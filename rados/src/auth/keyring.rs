//! Ceph keyring file parsing and management
//!
//! Parses Ceph keyring files to extract authentication keys for entities.

use crate::auth::error::{CephXError, Result};
use crate::auth::types::CryptoKey;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tracing::{debug, warn};

/// Represents a parsed Ceph keyring file
#[derive(Debug, Clone)]
pub struct Keyring {
    /// Map of entity names to their authentication keys
    keys: HashMap<String, CryptoKey>,
    /// Map of entity names to their capabilities
    caps: HashMap<String, HashMap<String, String>>,
}

impl Keyring {
    pub fn new() -> Self {
        Self {
            keys: HashMap::new(),
            caps: HashMap::new(),
        }
    }

    /// Load keyring from file path
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path.as_ref())
            .map_err(|e| CephXError::InvalidKey(format!("Failed to read keyring file: {}", e)))?;

        Self::from_string(&content)
    }

    /// Parse keyring from string content
    pub fn from_string(content: &str) -> Result<Self> {
        let mut keyring = Keyring::new();
        let mut current_entity: Option<String> = None;

        for line in content.lines() {
            let line = line.trim();

            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if line.starts_with('[') && line.ends_with(']') {
                let entity = line[1..line.len() - 1].to_string();
                debug!("Found entity: {}", entity);
                current_entity = Some(entity);
                continue;
            }

            if let Some(entity) = &current_entity
                && let Some((key, value)) = line.split_once('=')
            {
                let key = key.trim();
                let value = value.trim();

                match key {
                    "key" => {
                        debug!("Loading key for entity: {}", entity);
                        let crypto_key = CryptoKey::from_base64(value)?;
                        keyring.keys.insert(entity.clone(), crypto_key);
                    }
                    key if key.starts_with("caps ") => {
                        let service = &key[5..];
                        // Strip surrounding quotes — Ceph's KeyRing::decode_plaintext
                        // strips them, and cap strings are compared unquoted.
                        let cap_value = value
                            .strip_prefix('"')
                            .and_then(|s| s.strip_suffix('"'))
                            .unwrap_or(value);
                        keyring
                            .caps
                            .entry(entity.clone())
                            .or_default()
                            .insert(service.to_string(), cap_value.to_string());
                    }
                    _ => {
                        warn!("Unknown keyring field: {} = {}", key, value);
                    }
                }
            }
        }

        debug!("Loaded {} keys from keyring", keyring.keys.len());
        Ok(keyring)
    }

    pub fn get_key(&self, entity: &str) -> Option<&CryptoKey> {
        self.keys.get(entity)
    }

    pub fn get_caps(&self, entity: &str, service: &str) -> Option<&str> {
        self.caps.get(entity)?.get(service).map(String::as_str)
    }

    pub fn entities(&self) -> impl Iterator<Item = &String> {
        self.keys.keys()
    }

    pub fn has_entity(&self, entity: &str) -> bool {
        self.keys.contains_key(entity)
    }
}

impl Default for Keyring {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_keyring() {
        let content = r#"
[client.admin]
    key = AQD8J8JoSpspNhAAU49nK6K8fO4MgTYFnrk+HQ==
    caps mgr = "allow *"
    caps mon = "allow *"
    caps osd = "allow *"

[client.test]
    key = AQABCDEFghijklmnU49nK6K8fO4MgTYFnrk+HQ==
    caps mon = "allow r"
"#;

        let keyring = Keyring::from_string(content).expect("Failed to parse keyring");

        assert!(keyring.has_entity("client.admin"));
        assert!(keyring.has_entity("client.test"));
        assert!(!keyring.has_entity("client.nonexistent"));

        let admin_key = keyring
            .get_key("client.admin")
            .expect("admin key not found");
        // CryptoKey stores the full encoded structure (12-byte header + 16-byte key = 28 bytes)
        assert_eq!(admin_key.secret.len(), 28);

        let admin_caps = keyring
            .get_caps("client.admin", "mon")
            .expect("admin mon caps");
        assert_eq!(admin_caps, "allow *");

        let test_caps = keyring
            .get_caps("client.test", "mon")
            .expect("test mon caps");
        assert_eq!(test_caps, "allow r");

        let entities: Vec<_> = keyring.entities().collect();
        assert_eq!(entities.len(), 2);
    }
}
