/// Ceph configuration file parser
///
/// This module provides functionality to parse Ceph configuration files (ceph.conf)
/// and extract authentication settings.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Parse a ceph.conf file and return a map of sections to key-value pairs
pub fn parse_ceph_conf(path: &Path) -> Result<HashMap<String, HashMap<String, String>>, std::io::Error> {
    let content = fs::read_to_string(path)?;
    let mut config = HashMap::new();
    let mut current_section = String::from("global");
    
    for line in content.lines() {
        let line = line.trim();
        
        // Skip empty lines and comments
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        
        // Check for section headers [section_name]
        if line.starts_with('[') && line.ends_with(']') {
            current_section = line[1..line.len()-1].trim().to_string();
            config.entry(current_section.clone()).or_insert_with(HashMap::new);
            continue;
        }
        
        // Parse key-value pairs
        // The value can be separated by '=', ':', or whitespace
        if let Some((key, value)) = parse_config_line(line) {
            config
                .entry(current_section.clone())
                .or_insert_with(HashMap::new)
                .insert(key, value);
        }
    }
    
    Ok(config)
}

/// Parse a single configuration line into key-value pair
/// Supports multiple delimiters: '=', ':', and whitespace
fn parse_config_line(line: &str) -> Option<(String, String)> {
    // Helper to split by delimiter and extract key-value
    fn try_split_by(line: &str, delim: char) -> Option<(String, String)> {
        line.find(delim).map(|pos| {
            let key = line[..pos].trim().to_string();
            let value = line[pos+1..].trim().to_string();
            (key, value)
        })
    }
    
    // Try '=' delimiter first
    if let Some(result) = try_split_by(line, '=') {
        return Some(result);
    }
    
    // Try ':' delimiter
    if let Some(result) = try_split_by(line, ':') {
        return Some(result);
    }
    
    // Try whitespace delimiter
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() >= 2 {
        let key = parts[0].to_string();
        let value = parts[1..].join(" ");
        return Some((key, value));
    }
    
    None
}

/// Parse the 'auth client required' setting value
/// The value can be a comma, semicolon, equals, space, or tab separated list of auth methods
/// Example values: "none", "cephx", "cephx,none", "cephx;none", "cephx = none", "cephx\tnone"
pub fn parse_auth_methods(value: &str) -> Vec<String> {
    value
        .split(&[',', ';', '=', ' ', '\t'][..])
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
        .collect()
}

/// Get the 'auth client required' setting from the global section
/// Returns None if not found
/// 
/// Supports multiple key name variations for compatibility with different Ceph versions:
/// - "auth client required" (standard format with spaces)
/// - "auth_client_required" (underscore format, common in older versions)
/// - "authclientrequired" (no separator, rare but supported)
pub fn get_auth_client_required(config: &HashMap<String, HashMap<String, String>>) -> Option<Vec<String>> {
    // Try different common variations of the key name
    let key_variations = [
        "auth client required",      // Standard format
        "auth_client_required",      // Underscore format
        "authclientrequired",        // No separator (rare)
    ];
    
    if let Some(global) = config.get("global") {
        for key in &key_variations {
            if let Some(value) = global.get(*key) {
                return Some(parse_auth_methods(value));
            }
        }
    }
    
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_parse_auth_methods() {
        assert_eq!(parse_auth_methods("none"), vec!["none"]);
        assert_eq!(parse_auth_methods("cephx"), vec!["cephx"]);
        assert_eq!(parse_auth_methods("cephx,none"), vec!["cephx", "none"]);
        assert_eq!(parse_auth_methods("cephx;none"), vec!["cephx", "none"]);
        assert_eq!(parse_auth_methods("cephx = none"), vec!["cephx", "none"]);
        assert_eq!(parse_auth_methods("cephx\tnone"), vec!["cephx", "none"]);
        assert_eq!(parse_auth_methods("cephx, none"), vec!["cephx", "none"]);
    }

    #[test]
    fn test_parse_ceph_conf() {
        let conf_content = r#"
[global]
fsid = 12345
auth_client_required = cephx
mon_host = v2:127.0.0.1:6789

[mon]
mon_allow_pool_delete = true

[osd]
osd_pool_default_size = 1
"#;
        
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(conf_content.as_bytes()).unwrap();
        
        let config = parse_ceph_conf(temp_file.path()).unwrap();
        
        assert!(config.contains_key("global"));
        assert!(config.contains_key("mon"));
        assert!(config.contains_key("osd"));
        
        let global = config.get("global").unwrap();
        assert_eq!(global.get("fsid"), Some(&"12345".to_string()));
        assert_eq!(global.get("auth_client_required"), Some(&"cephx".to_string()));
    }

    #[test]
    fn test_get_auth_client_required() {
        let mut config = HashMap::new();
        let mut global = HashMap::new();
        global.insert("auth_client_required".to_string(), "cephx".to_string());
        config.insert("global".to_string(), global);
        
        let auth_methods = get_auth_client_required(&config).unwrap();
        assert_eq!(auth_methods, vec!["cephx"]);
    }

    #[test]
    fn test_get_auth_client_required_multiple() {
        let mut config = HashMap::new();
        let mut global = HashMap::new();
        global.insert("auth client required".to_string(), "cephx,none".to_string());
        config.insert("global".to_string(), global);
        
        let auth_methods = get_auth_client_required(&config).unwrap();
        assert_eq!(auth_methods, vec!["cephx", "none"]);
    }
}
