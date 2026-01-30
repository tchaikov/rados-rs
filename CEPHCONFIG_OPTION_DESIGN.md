# Generic Option Type Design for cephconfig

## Requirements Analysis

Based on the user's request, we need a generic Option type system that:

1. **Type-safe value parsing**: Different types (size, duration, count, ratio, etc.)
2. **Default values**: Each option has a default
3. **Name uniqueness**: Option name serves as both config key and struct field name
4. **Elegant macro-based definition**: Use macros to avoid repetition
5. **Extensible**: Easy to add new value types

## Design

### Core Trait: ConfigValue

```rust
/// Trait for types that can be parsed from ceph.conf values
pub trait ConfigValue: Sized {
    /// Parse from a string value in ceph.conf
    fn parse_config_value(s: &str) -> Result<Self, ConfigError>;

    /// Get the type name for error messages
    fn type_name() -> &'static str;
}
```

### Built-in Value Types

```rust
// 1. Size (bytes with SI/IEC prefixes)
pub struct Size(pub u64);

impl ConfigValue for Size {
    fn parse_config_value(s: &str) -> Result<Self, ConfigError> {
        parse_size(s).map(Size)
    }
    fn type_name() -> &'static str { "size" }
}

// 2. Duration (seconds with time units)
pub struct Duration(pub std::time::Duration);

impl ConfigValue for Duration {
    fn parse_config_value(s: &str) -> Result<Self, ConfigError> {
        parse_duration(s).map(Duration)
    }
    fn type_name() -> &'static str { "duration" }
}

// 3. Count (plain integer)
pub struct Count(pub u64);

impl ConfigValue for Count {
    fn parse_config_value(s: &str) -> Result<Self, ConfigError> {
        s.parse().map(Count).map_err(|_| ConfigError::ParseError(...))
    }
    fn type_name() -> &'static str { "count" }
}

// 4. Ratio (0.0 to 1.0)
pub struct Ratio(pub f64);

impl ConfigValue for Ratio {
    fn parse_config_value(s: &str) -> Result<Self, ConfigError> {
        let val: f64 = s.parse().map_err(...)?;
        if val < 0.0 || val > 1.0 {
            return Err(ConfigError::ParseError("ratio must be 0.0-1.0"));
        }
        Ok(Ratio(val))
    }
    fn type_name() -> &'static str { "ratio" }
}

// 5. Bool
impl ConfigValue for bool {
    fn parse_config_value(s: &str) -> Result<Self, ConfigError> {
        match s.to_lowercase().as_str() {
            "true" | "yes" | "1" | "on" => Ok(true),
            "false" | "no" | "0" | "off" => Ok(false),
            _ => Err(ConfigError::ParseError(...))
        }
    }
    fn type_name() -> &'static str { "bool" }
}

// 6. String
impl ConfigValue for String {
    fn parse_config_value(s: &str) -> Result<Self, ConfigError> {
        Ok(s.to_string())
    }
    fn type_name() -> &'static str { "string" }
}
```

### Option Definition

```rust
/// A configuration option with name, type, and default value
pub struct Option<T: ConfigValue> {
    /// The option name (used in ceph.conf)
    name: &'static str,
    /// The default value
    default: T,
    /// Optional description
    description: Option<&'static str>,
}

impl<T: ConfigValue> Option<T> {
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
    pub fn get(&self, config: &CephConfig, sections: &[&str]) -> T
    where
        T: Clone,
    {
        config
            .get_with_fallback(sections, self.name)
            .and_then(|s| T::parse_config_value(s).ok())
            .unwrap_or_else(|| self.default.clone())
    }
}
```

### Macro for Defining Options

```rust
/// Define a configuration struct with typed options
///
/// # Example
///
/// ```
/// define_options! {
///     pub struct MonitorConfig {
///         /// Maximum number of monitor connections
///         ms_max_connections: Count = Count(100),
///
///         /// Dispatch throttle in bytes
///         ms_dispatch_throttle_bytes: Size = Size(100 * 1024 * 1024),
///
///         /// Connection timeout
///         ms_connection_timeout: Duration = Duration(std::time::Duration::from_secs(30)),
///
///         /// Enable compression
///         ms_compression_enabled: bool = false,
///
///         /// Compression ratio threshold
///         ms_compression_ratio: Ratio = Ratio(0.8),
///     }
/// }
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
            /// Option definitions (const for compile-time checking)
            $(
                const fn [<$field _option>]() -> $crate::Option<$ty> {
                    $crate::Option::new(
                        stringify!($field),
                        $default,
                    )
                }
            )*

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
                        $field: Self::[<$field _option>]().get(config, sections),
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
```

## Usage Examples

### Example 1: Monitor Configuration

```rust
use cephconfig::{define_options, Size, Duration, Count, Ratio};

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

// Usage
let config = CephConfig::from_file("/etc/ceph/ceph.conf")?;
let mon_config = MonitorConfig::from_ceph_config(&config, &["global", "client"]);

println!("Throttle: {} bytes", mon_config.ms_dispatch_throttle_bytes.0);
println!("Timeout: {:?}", mon_config.ms_connection_timeout.0);
println!("Max connections: {}", mon_config.ms_max_connections.0);
```

### Example 2: OSD Configuration

```rust
define_options! {
    /// OSD client configuration
    pub struct OsdConfig {
        /// OSD operation timeout
        osd_op_timeout: Duration = Duration(std::time::Duration::from_secs(30)),

        /// Maximum OSD connections per pool
        osd_pool_max_connections: Count = Count(50),

        /// OSD heartbeat interval
        osd_heartbeat_interval: Duration = Duration(std::time::Duration::from_secs(6)),

        /// Enable OSD compression
        osd_compression: bool = false,

        /// OSD compression algorithm
        osd_compression_algorithm: String = String::from("snappy"),
    }
}
```

### Example 3: Custom Value Type

```rust
// Define a custom value type for IP addresses
pub struct IpAddr(pub std::net::IpAddr);

impl ConfigValue for IpAddr {
    fn parse_config_value(s: &str) -> Result<Self, ConfigError> {
        s.parse()
            .map(IpAddr)
            .map_err(|e| ConfigError::ParseError(format!("Invalid IP: {}", e)))
    }

    fn type_name() -> &'static str {
        "ip_addr"
    }
}

// Use in config
define_options! {
    pub struct NetworkConfig {
        /// Public network address
        public_addr: IpAddr = IpAddr(std::net::IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0))),

        /// Cluster network address
        cluster_addr: IpAddr = IpAddr(std::net::IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0))),
    }
}
```

## Implementation Details

### Size Parsing (Already Implemented)

```rust
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

    let num: f64 = num_str.parse()
        .map_err(|_| ConfigError::ParseError(format!("Invalid number: {}", num_str)))?;

    let multiplier: u64 = match unit.as_str() {
        "" | "B" => 1,
        "K" | "KB" => 1024,
        "M" | "MB" => 1024 * 1024,
        "G" | "GB" => 1024 * 1024 * 1024,
        "T" | "TB" => 1024 * 1024 * 1024 * 1024,
        _ => return Err(ConfigError::ParseError(format!("Unknown unit: {}", unit))),
    };

    Ok((num * multiplier as f64) as u64)
}
```

### Duration Parsing

```rust
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

    let num: f64 = num_str.parse()
        .map_err(|_| ConfigError::ParseError(format!("Invalid number: {}", num_str)))?;

    let seconds = match unit.as_str() {
        "" | "s" | "sec" | "second" | "seconds" => num,
        "ms" | "msec" | "millisecond" | "milliseconds" => num / 1000.0,
        "us" | "usec" | "microsecond" | "microseconds" => num / 1_000_000.0,
        "m" | "min" | "minute" | "minutes" => num * 60.0,
        "h" | "hr" | "hour" | "hours" => num * 3600.0,
        "d" | "day" | "days" => num * 86400.0,
        _ => return Err(ConfigError::ParseError(format!("Unknown time unit: {}", unit))),
    };

    Ok(std::time::Duration::from_secs_f64(seconds))
}
```

## Advantages of This Design

### 1. Type Safety
```rust
// Compile-time type checking
let config = MonitorConfig::from_ceph_config(&ceph_config, &["global"]);
let bytes: u64 = config.ms_dispatch_throttle_bytes.0;  // Type-safe access
```

### 2. No Name Repetition
```rust
// Option name is automatically the field name
define_options! {
    pub struct Config {
        ms_dispatch_throttle_bytes: Size = Size(100 * 1024 * 1024),
        //  ^^^^^^^^^^^^^^^^^^^^^^^^^ This becomes both:
        //  1. Field name in struct
        //  2. Key name in ceph.conf
    }
}
```

### 3. Extensibility
```rust
// Easy to add new value types
impl ConfigValue for MyCustomType {
    fn parse_config_value(s: &str) -> Result<Self, ConfigError> {
        // Custom parsing logic
    }
    fn type_name() -> &'static str { "my_type" }
}
```

### 4. Default Values
```rust
// Defaults are part of the definition
define_options! {
    pub struct Config {
        timeout: Duration = Duration(std::time::Duration::from_secs(30)),
        //                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        //                  Default value, used if not in ceph.conf
    }
}
```

### 5. Documentation
```rust
// Doc comments become part of the struct
define_options! {
    pub struct Config {
        /// Maximum number of connections (default: 100)
        ///
        /// This controls how many concurrent connections are allowed.
        max_connections: Count = Count(100),
    }
}
```

## Migration Path

### Current Code
```rust
// Current approach in msgr2/src/lib.rs
let throttle_bytes_str = ceph_config.get_with_fallback(&sections, "ms_dispatch_throttle_bytes");
match parse_size(throttle_bytes_str) {
    Ok(throttle_bytes) => {
        config.throttle_config = Some(ThrottleConfig::with_byte_rate(throttle_bytes));
    }
    Err(e) => {
        tracing::warn!("Failed to parse: {}", e);
    }
}
```

### New Approach
```rust
// With define_options!
define_options! {
    pub struct MessengerConfig {
        ms_dispatch_throttle_bytes: Size = Size(100 * 1024 * 1024),
        ms_max_connections: Count = Count(100),
        ms_connection_timeout: Duration = Duration(std::time::Duration::from_secs(30)),
    }
}

// Usage
let msg_config = MessengerConfig::from_ceph_config(&ceph_config, &["global", "client"]);
config.throttle_config = Some(ThrottleConfig::with_byte_rate(msg_config.ms_dispatch_throttle_bytes.0));
```

## Testing

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_parsing() {
        assert_eq!(Size::parse_config_value("100M").unwrap().0, 100 * 1024 * 1024);
        assert_eq!(Size::parse_config_value("1G").unwrap().0, 1024 * 1024 * 1024);
        assert_eq!(Size::parse_config_value("100_M").unwrap().0, 100 * 1024 * 1024);
    }

    #[test]
    fn test_duration_parsing() {
        assert_eq!(
            Duration::parse_config_value("30s").unwrap().0,
            std::time::Duration::from_secs(30)
        );
        assert_eq!(
            Duration::parse_config_value("5m").unwrap().0,
            std::time::Duration::from_secs(300)
        );
    }

    #[test]
    fn test_config_loading() {
        let config_str = r#"
[global]
ms_dispatch_throttle_bytes = 50M
ms_max_connections = 200
ms_connection_timeout = 60s
"#;
        let ceph_config = CephConfig::parse(config_str).unwrap();
        let msg_config = MessengerConfig::from_ceph_config(&ceph_config, &["global"]);

        assert_eq!(msg_config.ms_dispatch_throttle_bytes.0, 50 * 1024 * 1024);
        assert_eq!(msg_config.ms_max_connections.0, 200);
        assert_eq!(msg_config.ms_connection_timeout.0, std::time::Duration::from_secs(60));
    }

    #[test]
    fn test_default_values() {
        let ceph_config = CephConfig::parse("[global]\n").unwrap();
        let msg_config = MessengerConfig::from_ceph_config(&ceph_config, &["global"]);

        // Should use defaults
        assert_eq!(msg_config.ms_dispatch_throttle_bytes.0, 100 * 1024 * 1024);
        assert_eq!(msg_config.ms_max_connections.0, 100);
    }
}
```

## Summary

This design provides:

✅ **Type-safe value parsing** - Different types with proper validation
✅ **Default values** - Built into the definition
✅ **Name uniqueness** - Field name = config key name
✅ **Elegant macros** - No repetition, clean syntax
✅ **Extensible** - Easy to add new value types
✅ **Documentation** - Doc comments preserved
✅ **Compile-time checking** - Catch errors early
✅ **Ceph-compatible** - Matches Ceph's parsing behavior

The implementation is ready to be added to the cephconfig crate!
