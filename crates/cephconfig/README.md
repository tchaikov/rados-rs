# cephconfig

A Rust library for parsing Ceph configuration files (ceph.conf).

## Features

- Parse standard INI-style Ceph configuration files
- Extract common configuration options (monitor addresses, keyring path, entity name)
- Support for section-based configuration with fallback
- Macro support for defining custom configuration structs

## Usage

### Basic Usage

```rust
use cephconfig::CephConfig;

// Parse from file
let config = CephConfig::from_file("/etc/ceph/ceph.conf")?;

// Get monitor addresses
let mon_addrs = config.mon_addrs()?;
println!("Monitor addresses: {:?}", mon_addrs);

// Get keyring path
let keyring = config.keyring()?;
println!("Keyring: {}", keyring);

// Get entity name (defaults to "client.admin")
let entity_name = config.entity_name();
println!("Entity: {}", entity_name);
```

### Getting Specific Values

```rust
// Get a value from a specific section
let fsid = config.get("global", "fsid");

// Get a value with fallback across sections
let keyring = config.get_with_fallback(&["client", "global"], "keyring");
```

### Using in Tests

The cephconfig crate is particularly useful for integration tests:

```rust
use cephconfig::CephConfig;
use std::env;

fn load_test_config() -> TestConfig {
    // Load from CEPH_CONF environment variable
    let conf_path = env::var("CEPH_CONF")
        .expect("CEPH_CONF environment variable must be set");
    let config = CephConfig::from_file(conf_path).unwrap();

    TestConfig {
        mon_addrs: config.mon_addrs().unwrap(),
        keyring: config.keyring().unwrap(),
        entity_name: config.entity_name(),
    }
}
```

Then run tests with:

```bash
# Using ceph.conf
CEPH_CONF=/path/to/ceph.conf cargo test
```

### Custom Configuration Structs

You can define custom configuration structs using the `define_config!` macro:

```rust
use cephconfig::define_config;

define_config! {
    /// My application configuration
    pub struct MyConfig {
        /// Monitor addresses
        mon_addrs: Vec<String> = vec![],
        /// Keyring path
        keyring: String = "/etc/ceph/keyring".to_string(),
        /// Connection timeout in seconds
        timeout: u64 = 30,
    }
}

// Use with defaults
let config = MyConfig::new();

// Or load from CephConfig
let ceph_config = CephConfig::from_file("/etc/ceph/ceph.conf")?;
let mut config = MyConfig::new();
config.apply_ceph_config(&ceph_config)?;
```

## Configuration File Format

The cephconfig crate parses standard INI-style Ceph configuration files:

```ini
[global]
fsid = 7150dbe1-1803-44b9-9a3d-b893308fd02e
mon host = [v2:192.168.1.43:3300,v1:192.168.1.43:6789] [v2:192.168.1.43:3301,v1:192.168.1.43:6790]

[client]
keyring = /etc/ceph/keyring
log file = /var/log/ceph/$name.$pid.log

[mon]
debug mon = 20
```

### Monitor Address Parsing

The `mon_addrs()` method parses the `mon host` configuration option and returns all addresses:

```rust
let addrs = config.mon_addrs()?;
// Returns: ["v2:192.168.1.43:3300", "v1:192.168.1.43:6789", "v2:192.168.1.43:3301", "v1:192.168.1.43:6790"]
```

To get just the first v2 address:

```rust
let addr = config.first_v2_mon_addr()?;
// Returns: "v2:192.168.1.43:3300"
```

## Error Handling

The crate uses `thiserror` for error handling:

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

## License

This crate is part of the rados-rs project.
