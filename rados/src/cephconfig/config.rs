//! Ceph configuration file parser and accessor.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::cephconfig::ConfigError;

use crate::auth::protocol::{CEPH_AUTH_CEPHX, CEPH_AUTH_GSS, CEPH_AUTH_NONE};

/// Represents a parsed Ceph configuration.
///
/// The option accessors read an option the way a C++ client does: from the
/// entity's own section, then its type's, then `[global]`, so `client.admin`
/// reads `[client.admin]`, `[client]`, `[global]` (see
/// [`sections_for`](Self::sections_for)). The `_for` accessors take the
/// entity; the others read as [`DEFAULT_ENTITY_NAME`](Self::DEFAULT_ENTITY_NAME).
#[derive(Debug, Clone)]
pub struct CephConfig {
    sections: HashMap<String, HashMap<String, String>>,
}

impl CephConfig {
    /// The entity a client runs as when it is given none, as with librados'
    /// `rados_create(cluster, NULL)`.
    pub const DEFAULT_ENTITY_NAME: &'static str = "client.admin";

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
                let key = normalize_key_name(&line[..eq_pos]).into_owned();
                // C++ only treats ';' or '#' as a comment start when preceded
                // by whitespace (so paths like /foo#bar are safe).
                let raw_value = &line[eq_pos + 1..];
                let comment_pos = [" ;", "\t;", " #", "\t#"]
                    .iter()
                    .filter_map(|pat| raw_value.find(pat))
                    .min();
                let value = comment_pos
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

    /// Get a configuration value from a specific section. `key` is
    /// normalized as the parser normalizes the file's keys, so `mon host`
    /// and `mon_host` find the same option.
    pub fn get(&self, section: &str, key: &str) -> Option<&str> {
        self.sections
            .get(section)
            .and_then(|s| s.get(&*normalize_key_name(key)))
            .map(|v| v.as_str())
    }

    /// Get a configuration value, checking multiple sections in order.
    /// Typically checks: specific section -> client -> global.
    pub fn get_with_fallback(&self, sections: &[&str], key: &str) -> Option<&str> {
        sections.iter().find_map(|section| self.get(section, key))
    }

    /// The sections `entity` reads, highest priority first: its own name,
    /// its type, then `global`.
    ///
    /// Reference: `md_config_t::get_my_sections()` in src/common/config.cc
    pub fn sections_for(entity: &str) -> Vec<&str> {
        let mut sections = vec![entity];
        if let Some((entity_type, _)) = entity.split_once('.') {
            sections.push(entity_type);
        }
        sections.push("global");
        sections
    }

    /// Get a configuration value as `entity` reads it.
    pub fn get_for(&self, entity: &str, key: &str) -> Option<&str> {
        self.get_with_fallback(&Self::sections_for(entity), key)
    }

    /// Get monitor addresses as [`DEFAULT_ENTITY_NAME`](Self::DEFAULT_ENTITY_NAME) reads them.
    pub fn mon_addrs(&self) -> Result<Vec<String>, ConfigError> {
        self.mon_addrs_for(Self::DEFAULT_ENTITY_NAME)
    }

    /// Get monitor addresses as `entity` reads them.
    ///
    /// Parses the "mon_host" configuration option and returns a list of monitor addresses.
    /// Supports both v2 and v1 protocol addresses.
    pub fn mon_addrs_for(&self, entity: &str) -> Result<Vec<String>, ConfigError> {
        let mon_host = self
            .get_for(entity, "mon_host")
            .ok_or_else(|| ConfigError::MissingOption("mon_host".to_string()))?;

        let addrs: Vec<String> = mon_host
            .split_whitespace()
            .flat_map(|part| {
                // Strip at most one pair of brackets (trim_*_matches strips ALL).
                let part = part.strip_prefix('[').unwrap_or(part);
                let part = part.strip_suffix(']').unwrap_or(part);
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

    /// Get the first v2 monitor address as
    /// [`DEFAULT_ENTITY_NAME`](Self::DEFAULT_ENTITY_NAME) reads it.
    pub fn first_v2_mon_addr(&self) -> Result<String, ConfigError> {
        self.first_v2_mon_addr_for(Self::DEFAULT_ENTITY_NAME)
    }

    /// Get the first v2 monitor address as `entity` reads it.
    ///
    /// This is a convenience method for getting a single v2 monitor address,
    /// which is commonly needed for initial connection.
    pub fn first_v2_mon_addr_for(&self, entity: &str) -> Result<String, ConfigError> {
        self.mon_addrs_for(entity)?
            .into_iter()
            .find(|addr| addr.starts_with("v2:"))
            .ok_or_else(|| ConfigError::ParseError("No v2 monitor address found".to_string()))
    }

    /// Get keyring file path as [`DEFAULT_ENTITY_NAME`](Self::DEFAULT_ENTITY_NAME) reads it.
    pub fn keyring(&self) -> Result<String, ConfigError> {
        self.keyring_for(Self::DEFAULT_ENTITY_NAME)
    }

    /// Get keyring file path as `entity` reads it.
    pub fn keyring_for(&self, entity: &str) -> Result<String, ConfigError> {
        self.get_for(entity, "keyring")
            .map(|s| s.to_string())
            .ok_or_else(|| ConfigError::MissingOption("keyring".to_string()))
    }

    /// Get entity name (defaults to "client.admin" if not specified).
    ///
    /// C++ has no such option: a C++ client takes its name from `--name` or
    /// `rados_create()`. The name picks the entity's own section, so this one
    /// option is read from `[client]` then `[global]`.
    pub fn entity_name(&self) -> String {
        self.get_with_fallback(&["client", "global"], "entity_name")
            .unwrap_or(Self::DEFAULT_ENTITY_NAME)
            .to_string()
    }

    /// Get required authentication methods for clients as
    /// [`DEFAULT_ENTITY_NAME`](Self::DEFAULT_ENTITY_NAME) reads them.
    pub fn get_auth_client_required(&self) -> Vec<u32> {
        self.get_auth_client_required_for(Self::DEFAULT_ENTITY_NAME)
    }

    /// Get required authentication methods for clients as `entity` reads them.
    ///
    /// Checks "auth_supported" in `[global]` first (if set, applies to all
    /// connections), otherwise checks "auth_client_required" as `entity`
    /// reads it. Returns list of supported auth method constants
    /// (CEPH_AUTH_NONE=1, CEPH_AUTH_CEPHX=2, etc.).
    ///
    /// Defaults to [CEPH_AUTH_CEPHX] if not specified.
    ///
    /// Reference: AuthRegistry::refresh_config() in src/auth/AuthRegistry.cc
    pub fn get_auth_client_required_for(&self, entity: &str) -> Vec<u32> {
        // Squid removed auth_supported, so a Squid client ignores it wherever
        // it is set. rados-rs keeps its pre-Squid reading, from [global]
        // only, for compatibility alone. Reading it from the entity's
        // sections too, as Reef did, would let `[client] auth_supported =
        // none` turn off cephx where a Squid client keeps it.
        if let Some(auth_supported) = self.get("global", "auth_supported") {
            let methods = parse_auth_methods(auth_supported);
            if !methods.is_empty() {
                return methods;
            }
        }
        let auth_value = self
            .get_for(entity, "auth_client_required")
            .unwrap_or("cephx");
        let methods = parse_auth_methods(auth_value);
        if methods.is_empty() {
            vec![CEPH_AUTH_CEPHX]
        } else {
            methods
        }
    }

    /// Get the DNS SRV service name for monitor discovery as
    /// [`DEFAULT_ENTITY_NAME`](Self::DEFAULT_ENTITY_NAME) reads it.
    pub fn mon_dns_srv_name(&self) -> String {
        self.mon_dns_srv_name_for(Self::DEFAULT_ENTITY_NAME)
    }

    /// Get the DNS SRV service name for monitor discovery as `entity` reads it.
    ///
    /// Returns the value of the `mon_dns_srv_name` configuration option,
    /// which defaults to `"ceph-mon"` if not specified.
    /// The name may include a domain suffix separated by `_`,
    /// e.g., `"ceph-mon_example.com"`.
    pub fn mon_dns_srv_name_for(&self, entity: &str) -> String {
        self.get_for(entity, "mon_dns_srv_name")
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

/// Normalize an option name as C++ `ConfFile::normalize_key_name()` does:
/// trim it and turn each run of whitespace inside it into one underscore.
fn normalize_key_name(key: &str) -> Cow<'_, str> {
    if key.contains(char::is_whitespace) {
        Cow::Owned(key.split_whitespace().collect::<Vec<_>>().join("_"))
    } else {
        Cow::Borrowed(key)
    }
}

/// Parse auth method names into method constants, splitting the list on
/// any of `;,= \t` as C++ `get_str_list()` does.
///
/// Recognizes "cephx", "none", and "gss". Unknown names are silently skipped.
///
/// Unlike C++, it lowercases each name before matching it, and
/// [`CephConfig::get_auth_client_required_for`] falls back to cephx alone
/// when the list is unset, empty or names no known method. C++ matches
/// names case-sensitively, defaults an unset `auth_client_required` to
/// `cephx, none`, and is left with no method from an empty or all-unknown
/// list (v19.2.6 src/auth/AuthRegistry.cc:76-87,
/// src/common/options/global.yaml.in:2146).
fn parse_auth_methods(methods_str: &str) -> Vec<u32> {
    methods_str
        .split([';', ',', '=', ' ', '\t'])
        .filter_map(|s| match s.trim().to_lowercase().as_str() {
            "none" => Some(CEPH_AUTH_NONE),
            "cephx" => Some(CEPH_AUTH_CEPHX),
            "gss" => Some(CEPH_AUTH_GSS),
            _ => None,
        })
        .collect()
}

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

    #[test]
    fn test_auth_method_list_delimiters() {
        assert_eq!(
            parse_auth_methods("cephx, none"),
            [CEPH_AUTH_CEPHX, CEPH_AUTH_NONE]
        );
        assert_eq!(
            parse_auth_methods("none;cephx"),
            [CEPH_AUTH_NONE, CEPH_AUTH_CEPHX]
        );
        assert_eq!(
            parse_auth_methods("cephx none\tgss"),
            [CEPH_AUTH_CEPHX, CEPH_AUTH_NONE, CEPH_AUTH_GSS]
        );
    }

    #[test]
    fn test_lookup_key_is_normalized() {
        let config = CephConfig::parse(
            r#"
[global]
mon host = v2:10.0.0.1:3300
auth_client_required = none
"#,
        )
        .unwrap();

        assert_eq!(config.get("global", "mon_host"), Some("v2:10.0.0.1:3300"));
        assert_eq!(config.get("global", "mon host"), Some("v2:10.0.0.1:3300"));
        assert_eq!(
            config.get("global", " auth  client\trequired "),
            Some("none")
        );
        assert_eq!(
            config.get_for("client.admin", "auth client required"),
            Some("none")
        );
    }

    #[test]
    fn test_sections_for() {
        assert_eq!(
            CephConfig::sections_for("client.admin"),
            ["client.admin", "client", "global"]
        );
        assert_eq!(
            CephConfig::sections_for("client.rgw.x"),
            ["client.rgw.x", "client", "global"]
        );
    }

    #[test]
    fn test_entity_section_beats_type_beats_global() {
        let config = CephConfig::parse(
            r#"
[global]
a = global
b = global
c = global
[client]
a = client
b = client
[client.admin]
a = client.admin
"#,
        )
        .unwrap();

        assert_eq!(config.get_for("client.admin", "a"), Some("client.admin"));
        assert_eq!(config.get_for("client.admin", "b"), Some("client"));
        assert_eq!(config.get_for("client.admin", "c"), Some("global"));
    }

    #[test]
    fn test_other_entity_section_ignored() {
        let config = CephConfig::parse(
            r#"
[client]
keyring = /etc/ceph/client.keyring
[client.rgw.x]
keyring = /etc/ceph/rgw.keyring
"#,
        )
        .unwrap();

        assert_eq!(config.keyring().unwrap(), "/etc/ceph/client.keyring");
        assert_eq!(
            config.keyring_for("client.admin").unwrap(),
            "/etc/ceph/client.keyring"
        );
        assert_eq!(
            config.keyring_for("client.rgw.x").unwrap(),
            "/etc/ceph/rgw.keyring"
        );

        let config =
            CephConfig::parse("[client.rgw.x]\nkeyring = /etc/ceph/rgw.keyring\n").unwrap();
        assert!(matches!(
            config.keyring(),
            Err(ConfigError::MissingOption(ref key)) if key == "keyring"
        ));
    }

    #[test]
    fn test_keyring_only_in_entity_section() {
        // The shape of the conf rooket's ceph-config writes.
        let config = CephConfig::parse(
            r#"
[global]
mon_host = v2:10.0.0.1:3300
[client.admin]
keyring = /etc/ceph/ceph.client.admin.keyring
"#,
        )
        .unwrap();

        assert_eq!(
            config.keyring().unwrap(),
            "/etc/ceph/ceph.client.admin.keyring"
        );
        assert_eq!(config.mon_addrs().unwrap(), ["v2:10.0.0.1:3300"]);
    }

    #[test]
    fn test_client_beats_global_for_every_accessor() {
        let config = CephConfig::parse(
            r#"
[global]
mon_host = v2:10.0.0.1:3300
auth_client_required = none
mon_dns_srv_name = global-mon
keyring = /etc/ceph/global.keyring
[client]
mon_host = v2:10.0.0.2:3300
auth_client_required = cephx
mon_dns_srv_name = client-mon
keyring = /etc/ceph/client.keyring
"#,
        )
        .unwrap();

        assert_eq!(config.mon_addrs().unwrap(), ["v2:10.0.0.2:3300"]);
        assert_eq!(config.first_v2_mon_addr().unwrap(), "v2:10.0.0.2:3300");
        assert_eq!(config.get_auth_client_required(), [CEPH_AUTH_CEPHX]);
        assert_eq!(config.mon_dns_srv_name(), "client-mon");
        assert_eq!(config.keyring().unwrap(), "/etc/ceph/client.keyring");
    }

    #[test]
    fn test_auth_for_entity() {
        let config = CephConfig::parse(
            r#"
[client]
auth_client_required = cephx
[client.guest]
auth_client_required = none
"#,
        )
        .unwrap();

        assert_eq!(config.get_auth_client_required(), [CEPH_AUTH_CEPHX]);
        assert_eq!(
            config.get_auth_client_required_for("client.guest"),
            [CEPH_AUTH_NONE]
        );
    }

    #[test]
    fn test_auth_supported_is_read_from_global_only() {
        let config = CephConfig::parse(
            r#"
[global]
auth_client_required = cephx
[client]
auth_supported = none
[client.guest]
auth_supported = none
"#,
        )
        .unwrap();

        assert_eq!(config.get_auth_client_required(), [CEPH_AUTH_CEPHX]);
        assert_eq!(
            config.get_auth_client_required_for("client.guest"),
            [CEPH_AUTH_CEPHX]
        );

        let config = CephConfig::parse(
            r#"
[global]
auth_supported = none
[client]
auth_client_required = cephx
"#,
        )
        .unwrap();

        assert_eq!(config.get_auth_client_required(), [CEPH_AUTH_NONE]);
    }
}
