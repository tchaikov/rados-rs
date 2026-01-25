# Quick Reference: Using cephconfig

## Installation

The `cephconfig` crate is already part of the workspace. To use it in your crate:

```toml
# In your Cargo.toml
[dependencies]
cephconfig = { path = "../cephconfig" }

# Or for tests only
[dev-dependencies]
cephconfig = { path = "../cephconfig" }
```

## Basic Usage

### Parse a Configuration File

```rust
use cephconfig::CephConfig;

// Parse from file
let config = CephConfig::from_file("/etc/ceph/ceph.conf")?;

// Parse from string
let config = CephConfig::parse(config_string)?;
```

### Get Configuration Values

```rust
// Get monitor addresses (all v1 and v2)
let mon_addrs = config.mon_addrs()?;
// Returns: Vec<String> like ["v2:192.168.1.43:3300", "v1:192.168.1.43:6789", ...]

// Get first v2 monitor address
let v2_addr = config.first_v2_mon_addr()?;
// Returns: String like "v2:192.168.1.43:3300"

// Get keyring path
let keyring = config.keyring()?;
// Returns: String like "/etc/ceph/keyring"

// Get entity name (defaults to "client.admin")
let entity = config.entity_name();
// Returns: String like "client.admin"

// Get specific value from a section
let fsid = config.get("global", "fsid");
// Returns: Option<&str>

// Get value with fallback across sections
let keyring = config.get_with_fallback(&["client", "global"], "keyring");
// Returns: Option<&str>
```

### List Sections and Keys

```rust
// Get all sections
let sections = config.sections();
// Returns: Vec<&str>

// Get all keys in a section
let keys = config.keys("client");
// Returns: Vec<&str>
```

## Running Tests with ceph.conf

### Method 1: Using CEPH_CONF (Recommended)

```bash
# Set CEPH_CONF environment variable
CEPH_CONF=/home/kefu/dev/ceph/build/ceph.conf cargo test -p osdclient

# With pool ID
CEPH_CONF=/home/kefu/dev/ceph/build/ceph.conf CEPH_POOL_ID=1 \
  cargo test -p osdclient --test integration_test
```

### Method 2: Using Environment Variables (Legacy)

```bash
CEPH_MON_ADDR="v2:192.168.1.43:40472" \
CEPH_KEYRING="/home/kefu/dev/ceph/build/keyring" \
CEPH_ENTITY_NAME="client.admin" \
CEPH_POOL_ID=1 \
  cargo test -p osdclient --test integration_test
```

## Integration Test Pattern

```rust
use cephconfig::CephConfig;
use std::env;

struct TestConfig {
    mon_addrs: Vec<String>,
    keyring_path: String,
    entity_name: String,
    pool_id: i64,
}

impl TestConfig {
    fn from_env() -> Self {
        // Try to load from ceph.conf first
        if let Ok(conf_path) = env::var("CEPH_CONF") {
            if let Ok(config) = Self::from_ceph_conf(&conf_path) {
                return config;
            }
        }

        // Fall back to environment variables
        Self {
            mon_addrs: vec![env::var("CEPH_MON_ADDR").unwrap()],
            keyring_path: env::var("CEPH_KEYRING").unwrap(),
            entity_name: env::var("CEPH_ENTITY_NAME")
                .unwrap_or_else(|_| "client.admin".to_string()),
            pool_id: env::var("CEPH_POOL_ID")
                .unwrap_or_else(|_| "1".to_string())
                .parse()
                .unwrap(),
        }
    }

    fn from_ceph_conf(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let config = CephConfig::from_file(path)?;

        Ok(Self {
            mon_addrs: config.mon_addrs()?,
            keyring_path: config.keyring()?,
            entity_name: config.entity_name(),
            pool_id: env::var("CEPH_POOL_ID")
                .unwrap_or_else(|_| "1".to_string())
                .parse()
                .unwrap_or(1),
        })
    }
}
```

## Example Program

Run the included example to see configuration parsing in action:

```bash
# Use default path
cargo run --example parse_config -p cephconfig

# Use custom path
cargo run --example parse_config -p cephconfig /path/to/ceph.conf
```

## Error Handling

```rust
use cephconfig::{CephConfig, ConfigError};

match CephConfig::from_file("/etc/ceph/ceph.conf") {
    Ok(config) => {
        // Use config
    }
    Err(ConfigError::IoError(e)) => {
        eprintln!("Failed to read file: {}", e);
    }
    Err(ConfigError::ParseError(e)) => {
        eprintln!("Failed to parse config: {}", e);
    }
    Err(ConfigError::MissingOption(opt)) => {
        eprintln!("Missing required option: {}", opt);
    }
}
```

## Common Configuration File Locations

- `/etc/ceph/ceph.conf` - System-wide configuration
- `~/.ceph/config` - User-specific configuration
- `/home/kefu/dev/ceph/build/ceph.conf` - Development cluster configuration

## Tips

1. **Always use CEPH_CONF for tests**: It's more convenient than setting multiple environment variables
2. **Check the example**: Run `cargo run --example parse_config -p cephconfig` to see what's in your config
3. **Fallback pattern**: Support both CEPH_CONF and environment variables for flexibility
4. **Error handling**: Always handle errors properly - missing config files are common

## Monitor Address Format

The crate handles complex monitor address formats:

```ini
[global]
mon host = [v2:192.168.1.43:3300,v1:192.168.1.43:6789] [v2:192.168.1.43:3301,v1:192.168.1.43:6790]
```

This is parsed into individual addresses:
- `v2:192.168.1.43:3300`
- `v1:192.168.1.43:6789`
- `v2:192.168.1.43:3301`
- `v1:192.168.1.43:6790`

Use `first_v2_mon_addr()` to get just the first v2 address for initial connection.
