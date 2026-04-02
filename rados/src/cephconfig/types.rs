//! Configuration value types, parsing, and typed option definitions.

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

/// Trait for types that can be parsed from ceph.conf values.
pub trait ConfigValue: Sized + Clone {
    /// Parse from a string value in ceph.conf.
    fn parse_config_value(s: &str) -> Result<Self, ConfigError>;

    /// Get the type name for error messages.
    fn type_name() -> &'static str;
}

/// Size value in bytes (supports SI/IEC prefixes: K, M, G, T, KB, MB, GB, TB).
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

/// Duration value (supports time units: s, ms, us, m, h, d).
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

/// Count value (plain integer).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Count(pub u64);

impl ConfigValue for Count {
    fn parse_config_value(s: &str) -> Result<Self, ConfigError> {
        s.parse()
            .map(Count)
            .map_err(|e| ConfigError::ParseError(format!("Invalid count '{}': {}", s, e)))
    }

    fn type_name() -> &'static str {
        "count"
    }
}

/// Ratio value (0.0 to 1.0).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Ratio(pub f64);

impl ConfigValue for Ratio {
    fn parse_config_value(s: &str) -> Result<Self, ConfigError> {
        let val: f64 = s
            .parse()
            .map_err(|e| ConfigError::ParseError(format!("Invalid ratio '{}': {}", s, e)))?;
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

/// Trait for values parsed from runtime monitor config updates.
pub trait RuntimeOptionValue: Sized {
    fn parse_runtime_option(value: &str) -> Option<Self>;
}

impl RuntimeOptionValue for std::time::Duration {
    /// Parse runtime duration option values in seconds (e.g. `5`, `5.5`, `5s`, `5.5s`).
    fn parse_runtime_option(value: &str) -> Option<Self> {
        let trimmed = value.trim();
        let numeric = trimmed.strip_suffix('s').unwrap_or(trimmed);
        let seconds = numeric.parse::<f64>().ok()?;
        if !seconds.is_finite() || seconds < 0.0 {
            return None;
        }
        Some(std::time::Duration::from_secs_f64(seconds))
    }
}

impl RuntimeOptionValue for u64 {
    fn parse_runtime_option(value: &str) -> Option<Self> {
        value.trim().parse::<u64>().ok()
    }
}

/// A configuration option with name, type, and default value.
pub struct ConfigOption<T: ConfigValue> {
    name: &'static str,
    default: T,
}

impl<T: ConfigValue> ConfigOption<T> {
    pub const fn new(name: &'static str, default: T) -> Self {
        Self { name, default }
    }

    /// Get the value from config, falling back to default.
    pub fn get(&self, config: &crate::cephconfig::CephConfig, sections: &[&str]) -> T {
        config
            .get_with_fallback(sections, self.name)
            .and_then(|s| T::parse_config_value(s).ok())
            .unwrap_or_else(|| self.default.clone())
    }
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

/// Split a string into a leading numeric part and a trailing unit suffix.
///
/// Returns `(numeric_str, unit_str)` where `numeric_str` contains only digits
/// and dots, and `unit_str` is the remainder.
fn split_number_and_unit(s: &str) -> (&str, &str) {
    let num_end = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());
    (&s[..num_end], &s[num_end..])
}

/// Parse size string with SI/IEC prefixes.
fn parse_size(s: &str) -> Result<u64, ConfigError> {
    let s = s.trim().replace('_', "");
    let (num_str, unit_part) = split_number_and_unit(&s);
    let unit = unit_part.to_uppercase();

    let num: f64 = num_str
        .parse()
        .map_err(|e| ConfigError::ParseError(format!("Invalid number '{}': {}", num_str, e)))?;

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
            )));
        }
    };

    Ok((num * multiplier as f64) as u64)
}

/// Parse duration string with time units.
fn parse_duration(s: &str) -> Result<std::time::Duration, ConfigError> {
    let s = s.trim();
    let (num_str, unit_part) = split_number_and_unit(s);
    let unit = unit_part.trim().to_lowercase();

    let num: f64 = num_str
        .parse()
        .map_err(|e| ConfigError::ParseError(format!("Invalid number '{}': {}", num_str, e)))?;

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
            )));
        }
    };

    if !seconds.is_finite() || seconds < 0.0 {
        return Err(ConfigError::ParseError(format!(
            "Duration value out of range: {}",
            seconds
        )));
    }
    Ok(std::time::Duration::from_secs_f64(seconds))
}

// ---------------------------------------------------------------------------
// Tests for types and parsing
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(Ratio::parse_config_value("1.5").is_err());
        assert!(Ratio::parse_config_value("-0.1").is_err());
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
        let config_str = "[global]\ntest_option = 42\n";
        let config = crate::cephconfig::CephConfig::parse(config_str).unwrap();

        let opt = ConfigOption::new("test_option", Count(100));
        assert_eq!(opt.get(&config, &["global"]).0, 42);

        let opt = ConfigOption::new("missing_option", Count(100));
        assert_eq!(opt.get(&config, &["global"]).0, 100);
    }
}
