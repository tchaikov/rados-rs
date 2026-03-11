# RADOS-RS

A Rust native implementation of the RADOS (Reliable Autonomic Distributed Object Store) client library, designed to provide modern async/await access to Ceph storage clusters.

## 🚀 Features

- **Async/Await Support**: Built on Tokio for high-performance async I/O
- **Modern Rust**: Leverages Rust's memory safety and performance
- **Full Protocol Stack**: Complete implementation of Ceph's messenger protocol v2, CephX authentication, monitor client, and OSD client
- **Type Safety**: Strong typing for all Ceph entities and operations
- **Native Performance**: No C library dependencies
- **Ceph Octopus+**: Supported on Ceph Octopus (v15) and all later releases

## 🏗️ Current Implementation Status

### ✅ Completed Components

- **Core Types & Encoding** (`denc`): EntityName, EntityAddr, OSDMap, PgPool, CRUSH structures — corpus-validated against ceph-dencoder
- **Messenger Protocol** (`msgr2`): Complete msgr2.1 implementation — banner exchange, connection negotiation, AES-128-GCM encryption, CRC32C, compression (Snappy/Zstandard/LZ4/gzip), message priority queues
- **CephX Authentication** (`auth`): Full client/server challenge-response authentication, keyring management, service ticket handling
- **Configuration Parsing** (`cephconfig`): INI-style `ceph.conf` parser with monitor address and keyring extraction
- **CRUSH Algorithm** (`crush`): All bucket types (Uniform, List, Tree, Straw, Straw2), device class support (ssd, hdd, nvme), object→PG→OSD placement
- **Monitor Client** (`monclient`): Connection management, MonMap/OSDMap/CRUSH map subscriptions, Paxos support, DNS SRV resolution, automatic keepalive
- **OSD Client** (`osdclient`): Full CRUD operations (read, write, stat, delete), pool management, sparse reads, snapshot support, CRUSH-based placement
- **CLI Tool** (`rados`): Command-line interface for object and pool operations

## 🚀 Quick Start

### Running Examples

The project includes examples demonstrating various components:

```bash
# Write, read, stat, and delete an object (requires a running Ceph cluster)
CEPH_CONF=/etc/ceph/ceph.conf cargo run --example simple_write -p osdclient

# Decode an OSD map from a binary corpus file
cargo run --example test_osdmap_decode -p osdclient

# Decode a CRUSH map
cargo run --example test_crush_decode -p crush

# Parse a ceph.conf file
cargo run --example parse_config -p cephconfig
```

### Using the CLI

```bash
# Put an object
cargo run -p rados -- --pool mypool put myobject /path/to/file

# Get an object
cargo run -p rados -- --pool mypool get myobject /path/to/output

# List objects in a pool
cargo run -p rados -- --pool mypool ls

# Get object statistics
cargo run -p rados -- --pool mypool stat myobject
```

### Using the Library

```rust
use monclient::{AuthConfig, MonClient, MonClientConfig, MonService};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load auth from ceph.conf
    let auth = AuthConfig::from_ceph_conf("/etc/ceph/ceph.conf")?;

    let (osdmap_tx, _osdmap_rx) = msgr2::map_channel::<monclient::MOSDMap>(64);

    let mon_client = MonClient::new(
        MonClientConfig {
            mon_addrs: vec!["v2:127.0.0.1:3300".to_string()],
            auth: Some(auth),
            ..Default::default()
        },
        Some(osdmap_tx),
    )
    .await?;
    mon_client.init().await?;

    // Subscribe to OSDMap updates
    mon_client.subscribe(MonService::OsdMap, 0, 0).await?;

    // Get OSDMap version
    let (newest, oldest) = mon_client.get_version(MonService::OsdMap).await?;
    println!("OSDMap version: {} (oldest: {})", newest, oldest);

    Ok(())
}
```

See the [`osdclient/examples/simple_write.rs`](crates/osdclient/examples/simple_write.rs) example for a complete end-to-end workflow including object operations.

## 🔧 Architecture

The project is organized as a Cargo workspace:

### `denc` - Encoding/Decoding
- **Ceph Encoding**: Implementation of Ceph's DENC encoding/decoding protocol
- **Entity Types**: Strongly typed entities (MON, OSD, CLIENT, MDS, MGR)
- **Addresses**: Network addresses with nonces for unique identification
- **Features**: Capability negotiation and version compatibility
- **Complex Types**: OSDMap, PgPool, CRUSH map structures

### `denc-macros` - Procedural Macros
- Derive macros: `Denc`, `DencMut`, `VersionedDenc`, `ZeroCopyDencode`

### `dencoder` - Encoding Inspector
- Binary tool to decode, inspect, and dump Ceph binary structures to JSON
- Corpus comparison against C++ `ceph-dencoder`

### `msgr2` - Messenger Protocol v2
- **Protocol Handling**: Complete implementation of Ceph messenger protocol v2.1
- **State Machine**: State pattern-based protocol state machine
- **Frame Protocol**: Frame encoding/decoding with encryption and compression
- **Session Management**: Connection negotiation, authentication, and session handling
- **Crypto**: AES-128-GCM encryption and CRC32C checksums

### `auth` - Authentication
- **CephX Protocol**: CephX authentication implementation
- **Keyring Management**: Keyring parsing and key management
- **Ticket System**: Service ticket handling

### `cephconfig` - Configuration
- **Config Parsing**: INI-style `ceph.conf` parser
- **Address Extraction**: Monitor v2/v1 address parsing
- **Type-Safe Options**: Size, Duration, Count, Ratio option types

### `crush` - CRUSH Algorithm
- **Object Placement**: Controlled Replication Under Scalable Hashing
- **Bucket Types**: Uniform, List, Tree, Straw, Straw2
- **Device Classes**: ssd, hdd, nvme support

### `monclient` - Monitor Client
- **Cluster Maps**: MonMap, OSDMap, CRUSH map retrieval and updates
- **Subscriptions**: Event-driven map change notifications
- **Discovery**: Monitor hunting and DNS SRV resolution

### `osdclient` - OSD Client
- **Object Operations**: read, write, write_full, sparse_read, stat, delete
- **Pool Management**: list_pools, create_pool, delete_pool
- **Advanced Features**: Snapshots, locks, sparse reads, operation builders

### `rados` - CLI Tool
- Command-line interface for object put/get/delete/stat and pool management

## 🛠️ Development

### Building
```bash
cargo build --workspace
```

### Testing
```bash
# Run all unit tests
cargo test --workspace --all-targets

# Run integration tests (requires a running Ceph cluster)
CEPH_CONF=/etc/ceph/ceph.conf cargo test --workspace --tests -- --ignored --nocapture
```

### Linting
```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
```

## 🧪 Testing

### Docker-based Test Cluster

A Docker Compose setup is provided for local integration testing:

```bash
# Start a single-node Ceph cluster (Reef v18)
docker compose -f docker/docker-compose.ceph.yml up -d
```

The cluster exposes msgr2 on port 3300 and msgr1 on port 6789. A keyring for
`client.admin` is generated automatically and written into the container.

### Running Integration Tests

```bash
# Run all integration tests against the cluster
CEPH_CONF=/path/to/ceph.conf cargo test --workspace --tests -- --ignored --nocapture

# Run integration tests for a specific crate
CEPH_CONF=/path/to/ceph.conf cargo test -p osdclient --tests -- --ignored --nocapture
CEPH_CONF=/path/to/ceph.conf cargo test -p monclient --tests -- --ignored --nocapture
CEPH_CONF=/path/to/ceph.conf cargo test -p msgr2 --tests -- --ignored --nocapture
```

## 📚 Documentation

- **Compatibility**: See [COMPATIBILITY.md](COMPATIBILITY.md) for detailed version information
- **CRUSH Algorithm**: See [CRUSH_DOC_GUIDE.md](CRUSH_DOC_GUIDE.md)
- **Encoding System**: See [ENCODING_METADATA.md](ENCODING_METADATA.md)
- **Monitor Client Design**: See [docs/MONCLIENT_DESIGN.md](docs/MONCLIENT_DESIGN.md)
- **Crate READMEs**: `crates/cephconfig/README.md`, `crates/osdclient/README.md`, `crates/msgr2/README.md`

### Compatibility

**Minimum Ceph Version**: Octopus (v15, released March 2020)

rados-rs supports Ceph Octopus or later. Nautilus and older releases are outside the supported compatibility boundary. See [COMPATIBILITY.md](COMPATIBILITY.md) for detailed version compatibility information.

**Supported Ceph Releases**:
- ✅ Octopus (v15) - March 2020
- ✅ Pacific (v16) - March 2021
- ✅ Quincy (v17) - April 2022
- ✅ Reef (v18) - September 2023
- ✅ Squid (v19) - September 2024

## 🤝 Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bugs and feature requests.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🔗 Related Projects

- [Ceph](https://ceph.io/) - The original distributed storage system
- [librados](https://docs.ceph.com/en/latest/rados/api/librados/) - Official C/C++ client library
- [rust-ceph](https://github.com/ceph/ceph-rust) - Official Rust bindings (FFI-based)