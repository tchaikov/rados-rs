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

mod config;
mod types;

pub use config::CephConfig;
pub use types::{
    ConfigError, ConfigOption, ConfigValue, Count, Duration, Ratio, RuntimeOptionValue, Size,
};

/// Define a configuration struct with typed options.
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
            /// Create with default values.
            pub fn new() -> Self {
                Self {
                    $(
                        $field: $default,
                    )*
                }
            }

            /// Load from ceph.conf with section fallback.
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

            /// Get option names (for introspection).
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

/// Define a runtime-updatable config struct where field names map directly to option names.
#[macro_export]
macro_rules! runtime_config_options {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $(
                $field:ident: $ty:ty
            ),* $(,)?
        }
    ) => {
        $(#[$meta])*
        $vis struct $name {
            $(
                pub $field: $ty,
            )*
        }

        impl $name {
            pub fn update_from_map(
                &mut self,
                config: &std::collections::HashMap<String, String>,
            ) {
                for (key, value) in config {
                    match key.as_str() {
                        $(
                            stringify!($field) => {
                                if let Some(parsed) = Self::parse_option::<$ty>(value) {
                                    self.$field = parsed;
                                }
                            }
                        )*
                        _ => {}
                    }
                }
            }

            pub fn parse_option<T: $crate::RuntimeOptionValue>(value: &str) -> Option<T> {
                T::parse_runtime_option(value)
            }
        }
    };
}

// ---------------------------------------------------------------------------
// Integration tests that exercise the full public API
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
    fn test_config_option_get() {
        let config = CephConfig::parse(TEST_CONFIG).unwrap();

        let opt = ConfigOption::new("ms_dispatch_throttle_bytes", Size(100 * 1024 * 1024));
        assert_eq!(opt.get(&config, &["global"]).0, 50 * 1024 * 1024);

        let opt = ConfigOption::new("ms_max_connections", Count(100));
        assert_eq!(opt.get(&config, &["global"]).0, 200);

        let opt = ConfigOption::new(
            "ms_connection_timeout",
            Duration(std::time::Duration::from_secs(30)),
        );
        assert_eq!(
            opt.get(&config, &["global"]).0,
            std::time::Duration::from_secs(60)
        );

        let opt = ConfigOption::new("ms_compression_enabled", false);
        assert!(opt.get(&config, &["global"]));

        let opt = ConfigOption::new("ms_compression_ratio", Ratio(0.8));
        assert_eq!(opt.get(&config, &["global"]).0, 0.75);

        let opt = ConfigOption::new("nonexistent_option", Count(999));
        assert_eq!(opt.get(&config, &["global"]).0, 999);
    }

    #[test]
    fn test_define_options_macro() {
        define_options! {
            /// Monitor client configuration
            pub struct MonitorConfig {
                ms_max_connections: Count = Count(100),
                ms_dispatch_throttle_bytes: Size = Size(100 * 1024 * 1024),
                ms_connection_timeout: Duration = Duration(std::time::Duration::from_secs(30)),
                ms_compression_enabled: bool = false,
                ms_compression_ratio: Ratio = Ratio(0.8),
                mon_host: String = String::new(),
            }
        }

        // Default values
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

        // Loading from ceph config
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

        // Introspection
        let names = MonitorConfig::option_names();
        assert_eq!(names.len(), 6);
        assert!(names.contains(&"ms_max_connections"));
        assert!(names.contains(&"ms_dispatch_throttle_bytes"));
        assert!(names.contains(&"ms_connection_timeout"));
        assert!(names.contains(&"ms_compression_enabled"));
        assert!(names.contains(&"ms_compression_ratio"));
        assert!(names.contains(&"mon_host"));

        // Default trait
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

        let config = TestConfig::from_ceph_config(&ceph_config, &["client", "global"]);
        assert_eq!(config.keyring, "/home/kefu/dev/ceph/build/keyring");

        let config = TestConfig::from_ceph_config(&ceph_config, &["mon"]);
        assert_eq!(config.keyring, "/etc/ceph/keyring");

        let names = TestConfig::option_names();
        assert_eq!(names.len(), 1);
        assert!(names.contains(&"keyring"));
    }
}
