use crate::auth::CephXKey;
use crate::error::{Error, Result};
use crate::types::EntityName;
use configparser::ini::Ini;
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct KeyringEntry {
    pub entity_name: EntityName,
    pub key: CephXKey,
    pub caps: HashMap<String, String>,
}

impl KeyringEntry {
    pub fn new(entity_name: EntityName, key: CephXKey) -> Self {
        Self {
            entity_name,
            key,
            caps: HashMap::new(),
        }
    }

    pub fn with_cap(mut self, service: String, capability: String) -> Self {
        self.caps.insert(service, capability);
        self
    }

    pub fn get_cap(&self, service: &str) -> Option<&str> {
        self.caps.get(service).map(|s| s.as_str())
    }
}

#[derive(Debug, Clone)]
pub struct Keyring {
    entries: HashMap<String, KeyringEntry>,
}

impl Keyring {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut ini = Ini::new();
        let path_str = path.as_ref().to_string_lossy();
        ini.load(&path_str)
            .map_err(|e| Error::InvalidConfig(format!("Failed to load keyring: {}", e)))?;

        let mut keyring = Self::new();

        for section_name in ini.sections() {
            // Skip global section
            if section_name == "DEFAULT" {
                continue;
            }

            // Parse entity name from section (e.g., "client.admin")
            let entity_name = parse_entity_name(&section_name)?;

            // Extract key
            let key_str = ini.get(&section_name, "key")
                .ok_or_else(|| Error::InvalidConfig(format!("No key found for {}", section_name)))?;
            let key = CephXKey::from_base64(&key_str)?;

            let mut entry = KeyringEntry::new(entity_name, key);

            // Extract capabilities
            for (prop_key, prop_value) in ini.get_map_ref().get(&section_name).unwrap_or(&HashMap::new()).iter() {
                if let Some(service) = prop_key.strip_prefix("caps ") {
                    entry.caps.insert(service.to_string(), prop_value.clone().unwrap_or_default());
                }
            }

            keyring.entries.insert(section_name, entry);
        }

        Ok(keyring)
    }

    pub fn get_entry(&self, entity_name: &str) -> Option<&KeyringEntry> {
        self.entries.get(entity_name)
    }

    pub fn get_key(&self, entity_name: &str) -> Option<&CephXKey> {
        self.get_entry(entity_name).map(|entry| &entry.key)
    }

    pub fn add_entry(&mut self, section_name: String, entry: KeyringEntry) {
        self.entries.insert(section_name, entry);
    }

    pub fn entries(&self) -> &HashMap<String, KeyringEntry> {
        &self.entries
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let mut ini = Ini::new();

        for (section_name, entry) in &self.entries {
            // Add key
            let key_base64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, entry.key.data());
            ini.set(section_name, "key", Some(key_base64));

            // Add capabilities
            for (service, capability) in &entry.caps {
                let caps_key = format!("caps {}", service);
                ini.set(section_name, &caps_key, Some(capability.clone()));
            }
        }

        let path_str = path.as_ref().to_string_lossy();
        ini.write(&path_str)
            .map_err(|e| Error::InvalidConfig(format!("Failed to save keyring: {}", e)))
    }
}

impl Default for Keyring {
    fn default() -> Self {
        Self::new()
    }
}

fn parse_entity_name(section_name: &str) -> Result<EntityName> {
    let parts: Vec<&str> = section_name.split('.').collect();
    if parts.len() != 2 {
        return Err(Error::InvalidConfig(format!(
            "Invalid entity name format: {}", section_name
        )));
    }

    let entity_type_str = parts[0];
    let entity_num_str = parts[1];

    let entity_type = match entity_type_str {
        "client" => crate::types::EntityType::TYPE_CLIENT,
        "mon" => crate::types::EntityType::TYPE_MON,
        "osd" => crate::types::EntityType::TYPE_OSD,
        "mds" => crate::types::EntityType::TYPE_MDS,
        "mgr" => crate::types::EntityType::TYPE_MGR,
        _ => return Err(Error::InvalidConfig(format!(
            "Unknown entity type: {}", entity_type_str
        ))),
    };

    let entity_num = if entity_num_str == "admin" && entity_type_str == "client" {
        // Special case for client.admin
        0
    } else {
        entity_num_str.parse::<u64>()
            .map_err(|_| Error::InvalidConfig(format!(
                "Invalid entity number: {}", entity_num_str
            )))?
    };

    Ok(EntityName::new(entity_type, entity_num))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_entity_name() {
        assert!(parse_entity_name("client.admin").is_ok());
        assert!(parse_entity_name("client.0").is_ok());
        assert!(parse_entity_name("mon.0").is_ok());
        assert!(parse_entity_name("osd.1").is_ok());
        assert!(parse_entity_name("invalid").is_err());
        assert!(parse_entity_name("client.invalid").is_err());
    }

    #[test]
    fn test_keyring_creation() {
        let keyring = Keyring::new();
        assert_eq!(keyring.entries.len(), 0);
    }
}