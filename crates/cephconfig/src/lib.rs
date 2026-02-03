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

/// Trait for types that can be parsed from ceph.conf values
pub trait ConfigValue: Sized + Clone {
    /// Parse from a string value in ceph.conf
    fn parse_config_value(s: &str) -> Result<Self, ConfigError>;

    /// Get the type name for error messages
    fn type_name() -> &'static str;
}

/// Size value in bytes (supports SI/IEC prefixes: K, M, G, T, KB, MB, GB, TB)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Size(pub u64);

impl ConfigValue for Size {
    fn parse_config_value(s: &str) -> Result<Self, ConfigError> {
        parse_size(s).map(Size)
    }

    fn type_name() -> &'static str {
        "size"
    }
}

/// Duration value (supports time units: s, ms, us, m, h, d)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Duration(pub std::time::Duration);

impl ConfigValue for Duration {
    fn parse_config_value(s: &str) -> Result<Self, ConfigError> {
        parse_duration(s).map(Duration)
    }

    fn type_name() -> &'static str {
        "duration"
    }
}

/// Count value (plain integer)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Count(pub u64);

impl ConfigValue for Count {
    fn parse_config_value(s: &str) -> Result<Self, ConfigError> {
        s.parse()
            .map(Count)
            .map_err(|_| ConfigError::ParseError(format!("Invalid count: {}", s)))
    }

    fn type_name() -> &'static str {
        "count"
    }
}

/// Ratio value (0.0 to 1.0)
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Ratio(pub f64);

impl ConfigValue for Ratio {
    fn parse_config_value(s: &str) -> Result<Self, ConfigError> {
        let val: f64 = s
            .parse()
            .map_err(|_| ConfigError::ParseError(format!("Invalid ratio: {}", s)))?;
        if !(0.0..=1.0).contains(&val) {
            return Err(ConfigError::ParseError(
                "ratio must be between 0.0 and 1.0".to_string(),
            ));
        }
        Ok(Ratio(val))
    }

    fn type_name() -> &'static str {
        "ratio"
    }
}

impl ConfigValue for bool {
    fn parse_config_value(s: &str) -> Result<Self, ConfigError> {
        match s.to_lowercase().as_str() {
            "true" | "yes" | "1" | "on" => Ok(true),
            "false" | "no" | "0" | "off" => Ok(false),
            _ => Err(ConfigError::ParseError(format!("Invalid bool: {}", s))),
        }
    }

    fn type_name() -> &'static str {
        "bool"
    }
}

impl ConfigValue for String {
    fn parse_config_value(s: &str) -> Result<Self, ConfigError> {
        Ok(s.to_string())
    }

    fn type_name() -> &'static str {
        "string"
    }
}

/// A configuration option with name, type, and default value
pub struct ConfigOption<T: ConfigValue> {
    /// The option name (used in ceph.conf)
    name: &'static str,
    /// The default value
    default: T,
    /// Optional description
    description: std::option::Option<&'static str>,
}

impl<T: ConfigValue> ConfigOption<T> {
    pub const fn new(name: &'static str, default: T) -> Self {
        Self {
            name,
            default,
            description: None,
        }
    }

    pub const fn with_description(mut self, desc: &'static str) -> Self {
        self.description = Some(desc);
        self
    }

    /// Get the value from config, falling back to default
    pub fn get(&self, config: &CephConfig, sections: &[&str]) -> T {
        config
            .get_with_fallback(sections, self.name)
            .and_then(|s| T::parse_config_value(s).ok())
            .unwrap_or_else(|| self.default.clone())
    }

    /// Get the option name
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Get the default value
    pub fn default_value(&self) -> &T {
        &self.default
    }

    /// Get the description
    pub fn description(&self) -> std::option::Option<&'static str> {
        self.description
    }
}

/// Parse size string with SI/IEC prefixes
fn parse_size(s: &str) -> Result<u64, ConfigError> {
    let s = s.trim().replace('_', "");

    let mut num_end = s.len();
    for (i, c) in s.chars().enumerate() {
        if !c.is_ascii_digit() && c != '.' {
            num_end = i;
            break;
        }
    }

    let num_str = &s[..num_end];
    let unit = &s[num_end..].to_uppercase();

    let num: f64 = num_str
        .parse()
        .map_err(|_| ConfigError::ParseError(format!("Invalid number: {}", num_str)))?;

    let multiplier: u64 = match unit.as_str() {
        "" | "B" => 1,
        "K" | "KB" => 1024,
        "M" | "MB" => 1024 * 1024,
        "G" | "GB" => 1024 * 1024 * 1024,
        "T" | "TB" => 1024 * 1024 * 1024 * 1024,
        _ => {
            return Err(ConfigError::ParseError(format!(
                "Unknown size unit: {}",
                unit
            )))
        }
    };

    Ok((num * multiplier as f64) as u64)
}

/// Parse duration string with time units
fn parse_duration(s: &str) -> Result<std::time::Duration, ConfigError> {
    let s = s.trim();

    let mut num_end = s.len();
    for (i, c) in s.chars().enumerate() {
        if !c.is_ascii_digit() && c != '.' {
            num_end = i;
            break;
        }
    }

    let num_str = &s[..num_end];
    let unit = &s[num_end..].trim().to_lowercase();

    let num: f64 = num_str
        .parse()
        .map_err(|_| ConfigError::ParseError(format!("Invalid number: {}", num_str)))?;

    let seconds = match unit.as_str() {
        "" | "s" | "sec" | "second" | "seconds" => num,
        "ms" | "msec" | "millisecond" | "milliseconds" => num / 1000.0,
        "us" | "usec" | "microsecond" | "microseconds" => num / 1_000_000.0,
        "m" | "min" | "minute" | "minutes" => num * 60.0,
        "h" | "hr" | "hour" | "hours" => num * 3600.0,
        "d" | "day" | "days" => num * 86400.0,
        _ => {
            return Err(ConfigError::ParseError(format!(
                "Unknown time unit: {}",
                unit
            )))
        }
    };

    Ok(std::time::Duration::from_secs_f64(seconds))
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
    pub fn get(&self, section: &str, key: &str) -> std::option::Option<&str> {
        self.sections
            .get(section)
            .and_then(|s| s.get(key))
            .map(|v| v.as_str())
    }

    /// Get a configuration value, checking multiple sections in order
    /// Typically checks: specific section -> client -> global
    pub fn get_with_fallback(&self, sections: &[&str], key: &str) -> std::option::Option<&str> {
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

/// Define a configuration struct with typed options
///
/// # Example
///
/// ```
/// use cephconfig::{define_options, CephConfig, Size, Duration, Count, Ratio};
///
/// define_options! {
///     /// Monitor client configuration
///     pub struct MonitorConfig {
///         /// Maximum number of monitor connections
///         ms_max_connections: Count = Count(100),
///
///         /// Dispatch throttle in bytes (100MB default)
///         ms_dispatch_throttle_bytes: Size = Size(100 * 1024 * 1024),
///
///         /// Connection timeout (30 seconds)
///         ms_connection_timeout: Duration = Duration(std::time::Duration::from_secs(30)),
///
///         /// Enable compression
///         ms_compression_enabled: bool = false,
///
///         /// Compression ratio threshold
///         ms_compression_ratio: Ratio = Ratio(0.8),
///
///         /// Monitor host addresses
///         mon_host: String = String::new(),
///     }
/// }
///
/// let config_str = r#"
/// [global]
/// ms_dispatch_throttle_bytes = 50M
/// ms_max_connections = 200
/// "#;
/// let ceph_config = CephConfig::parse(config_str).unwrap();
/// let mon_config = MonitorConfig::from_ceph_config(&ceph_config, &["global"]);
///
/// assert_eq!(mon_config.ms_dispatch_throttle_bytes.0, 50 * 1024 * 1024);
/// assert_eq!(mon_config.ms_max_connections.0, 200);
/// ```
#[macro_export]
macro_rules! define_options {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $(
                $(#[$field_meta:meta])*
                $field:ident: $ty:ty = $default:expr
            ),* $(,)?
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
            /// Create with default values
            pub fn new() -> Self {
                Self {
                    $(
                        $field: $default,
                    )*
                }
            }

            /// Load from ceph.conf with section fallback
            pub fn from_ceph_config(
                config: &$crate::CephConfig,
                sections: &[&str],
            ) -> Self {
                Self {
                    $(
                        $field: {
                            let opt = $crate::ConfigOption::new(stringify!($field), $default);
                            opt.get(config, sections)
                        },
                    )*
                }
            }

            /// Get option names (for introspection)
            pub fn option_names() -> &'static [&'static str] {
                &[
                    $(stringify!($field),)*
                ]
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}

/// Configuration builder with macro support for defining options (deprecated)
///
/// This is the old macro, kept for backward compatibility.
/// Use `define_options!` instead for better type safety.
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

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("100").unwrap(), 100);
        assert_eq!(parse_size("100B").unwrap(), 100);
        assert_eq!(parse_size("1K").unwrap(), 1024);
        assert_eq!(parse_size("1KB").unwrap(), 1024);
        assert_eq!(parse_size("100M").unwrap(), 100 * 1024 * 1024);
        assert_eq!(parse_size("100MB").unwrap(), 100 * 1024 * 1024);
        assert_eq!(parse_size("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size("1GB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size("1T").unwrap(), 1024 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("100_M").unwrap(), 100 * 1024 * 1024);
        assert_eq!(parse_size("1.5M").unwrap(), (1.5 * 1024.0 * 1024.0) as u64);
    }

    #[test]
    fn test_parse_duration() {
        assert_eq!(
            parse_duration("30").unwrap(),
            std::time::Duration::from_secs(30)
        );
        assert_eq!(
            parse_duration("30s").unwrap(),
            std::time::Duration::from_secs(30)
        );
        assert_eq!(
            parse_duration("30sec").unwrap(),
            std::time::Duration::from_secs(30)
        );
        assert_eq!(
            parse_duration("5m").unwrap(),
            std::time::Duration::from_secs(300)
        );
        assert_eq!(
            parse_duration("5min").unwrap(),
            std::time::Duration::from_secs(300)
        );
        assert_eq!(
            parse_duration("1h").unwrap(),
            std::time::Duration::from_secs(3600)
        );
        assert_eq!(
            parse_duration("1d").unwrap(),
            std::time::Duration::from_secs(86400)
        );
        assert_eq!(
            parse_duration("500ms").unwrap(),
            std::time::Duration::from_millis(500)
        );
    }

    #[test]
    fn test_size_config_value() {
        assert_eq!(
            Size::parse_config_value("100M").unwrap().0,
            100 * 1024 * 1024
        );
        assert_eq!(
            Size::parse_config_value("1G").unwrap().0,
            1024 * 1024 * 1024
        );
        assert_eq!(Size::type_name(), "size");
    }

    #[test]
    fn test_duration_config_value() {
        assert_eq!(
            Duration::parse_config_value("30s").unwrap().0,
            std::time::Duration::from_secs(30)
        );
        assert_eq!(
            Duration::parse_config_value("5m").unwrap().0,
            std::time::Duration::from_secs(300)
        );
        assert_eq!(Duration::type_name(), "duration");
    }

    #[test]
    fn test_count_config_value() {
        assert_eq!(Count::parse_config_value("100").unwrap().0, 100);
        assert_eq!(Count::parse_config_value("0").unwrap().0, 0);
        assert!(Count::parse_config_value("abc").is_err());
        assert_eq!(Count::type_name(), "count");
    }

    #[test]
    fn test_ratio_config_value() {
        assert_eq!(Ratio::parse_config_value("0.5").unwrap().0, 0.5);
        assert_eq!(Ratio::parse_config_value("0.0").unwrap().0, 0.0);
        assert_eq!(Ratio::parse_config_value("1.0").unwrap().0, 1.0);
        assert!(Ratio::parse_config_value("1.5").is_err()); // Out of range
        assert!(Ratio::parse_config_value("-0.1").is_err()); // Out of range
        assert_eq!(Ratio::type_name(), "ratio");
    }

    #[test]
    fn test_bool_config_value() {
        assert!(bool::parse_config_value("true").unwrap());
        assert!(bool::parse_config_value("True").unwrap());
        assert!(bool::parse_config_value("yes").unwrap());
        assert!(bool::parse_config_value("1").unwrap());
        assert!(bool::parse_config_value("on").unwrap());
        assert!(!bool::parse_config_value("false").unwrap());
        assert!(!bool::parse_config_value("False").unwrap());
        assert!(!bool::parse_config_value("no").unwrap());
        assert!(!bool::parse_config_value("0").unwrap());
        assert!(!bool::parse_config_value("off").unwrap());
        assert!(bool::parse_config_value("maybe").is_err());
        assert_eq!(bool::type_name(), "bool");
    }

    #[test]
    fn test_string_config_value() {
        assert_eq!(
            String::parse_config_value("hello").unwrap(),
            "hello".to_string()
        );
        assert_eq!(String::type_name(), "string");
    }

    #[test]
    fn test_config_option() {
        let opt = ConfigOption::new("test_option", Count(100));
        assert_eq!(opt.name(), "test_option");
        assert_eq!(opt.default_value().0, 100);
        assert_eq!(opt.description(), None);

        let opt = opt.with_description("Test option description");
        assert_eq!(opt.description(), Some("Test option description"));
    }

    #[test]
    fn test_config_option_get() {
        let config = CephConfig::parse(TEST_CONFIG).unwrap();

        // Test Size option
        let opt = ConfigOption::new("ms_dispatch_throttle_bytes", Size(100 * 1024 * 1024));
        let value = opt.get(&config, &["global"]);
        assert_eq!(value.0, 50 * 1024 * 1024);

        // Test Count option
        let opt = ConfigOption::new("ms_max_connections", Count(100));
        let value = opt.get(&config, &["global"]);
        assert_eq!(value.0, 200);

        // Test Duration option
        let opt = ConfigOption::new(
            "ms_connection_timeout",
            Duration(std::time::Duration::from_secs(30)),
        );
        let value = opt.get(&config, &["global"]);
        assert_eq!(value.0, std::time::Duration::from_secs(60));

        // Test bool option
        let opt = ConfigOption::new("ms_compression_enabled", false);
        let value = opt.get(&config, &["global"]);
        assert!(value);

        // Test Ratio option
        let opt = ConfigOption::new("ms_compression_ratio", Ratio(0.8));
        let value = opt.get(&config, &["global"]);
        assert_eq!(value.0, 0.75);

        // Test default fallback
        let opt = ConfigOption::new("nonexistent_option", Count(999));
        let value = opt.get(&config, &["global"]);
        assert_eq!(value.0, 999);
    }

    #[test]
    fn test_define_options_macro() {
        define_options! {
            /// Monitor client configuration
            pub struct MonitorConfig {
                /// Maximum number of monitor connections
                ms_max_connections: Count = Count(100),

                /// Dispatch throttle in bytes (100MB default)
                ms_dispatch_throttle_bytes: Size = Size(100 * 1024 * 1024),

                /// Connection timeout (30 seconds)
                ms_connection_timeout: Duration = Duration(std::time::Duration::from_secs(30)),

                /// Enable compression
                ms_compression_enabled: bool = false,

                /// Compression ratio threshold
                ms_compression_ratio: Ratio = Ratio(0.8),

                /// Monitor host addresses
                mon_host: String = String::new(),
            }
        }

        // Test default values
        let config = MonitorConfig::new();
        assert_eq!(config.ms_max_connections.0, 100);
        assert_eq!(config.ms_dispatch_throttle_bytes.0, 100 * 1024 * 1024);
        assert_eq!(
            config.ms_connection_timeout.0,
            std::time::Duration::from_secs(30)
        );
        assert!(!config.ms_compression_enabled);
        assert_eq!(config.ms_compression_ratio.0, 0.8);
        assert_eq!(config.mon_host, "");

        // Test loading from ceph config
        let ceph_config = CephConfig::parse(TEST_CONFIG).unwrap();
        let config = MonitorConfig::from_ceph_config(&ceph_config, &["global"]);

        assert_eq!(config.ms_max_connections.0, 200);
        assert_eq!(config.ms_dispatch_throttle_bytes.0, 50 * 1024 * 1024);
        assert_eq!(
            config.ms_connection_timeout.0,
            std::time::Duration::from_secs(60)
        );
        assert!(config.ms_compression_enabled);
        assert_eq!(config.ms_compression_ratio.0, 0.75);

        // Test option_names
        let names = MonitorConfig::option_names();
        assert_eq!(names.len(), 6);
        assert!(names.contains(&"ms_max_connections"));
        assert!(names.contains(&"ms_dispatch_throttle_bytes"));
        assert!(names.contains(&"ms_connection_timeout"));
        assert!(names.contains(&"ms_compression_enabled"));
        assert!(names.contains(&"ms_compression_ratio"));
        assert!(names.contains(&"mon_host"));

        // Test Default trait
        let config = MonitorConfig::default();
        assert_eq!(config.ms_max_connections.0, 100);
    }

    #[test]
    fn test_define_options_with_fallback() {
        define_options! {
            pub struct TestConfig {
                keyring: String = String::from("/etc/ceph/keyring"),
            }
        }

        let ceph_config = CephConfig::parse(TEST_CONFIG).unwrap();

        // Should find in client section
        let config = TestConfig::from_ceph_config(&ceph_config, &["client", "global"]);
        assert_eq!(config.keyring, "/home/kefu/dev/ceph/build/keyring");

        // Should use default if not found
        let config = TestConfig::from_ceph_config(&ceph_config, &["mon"]);
        assert_eq!(config.keyring, "/etc/ceph/keyring");

        // Test option_names
        let names = TestConfig::option_names();
        assert_eq!(names.len(), 1);
        assert!(names.contains(&"keyring"));
    }
}
