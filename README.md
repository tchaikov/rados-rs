# RADOS-RS

A Rust native implementation of the RADOS (Reliable Autonomic Distributed Object Store) client library, designed to provide modern async/await access to Ceph storage clusters.

## 🚀 Features

- **Async/Await Support**: Built on Tokio for high-performance async I/O
- **Modern Rust**: Leverages Rust's memory safety and performance
- **Messenger Protocol**: Complete implementation of Ceph's messenger protocol v2
- **Type Safety**: Strong typing for all Ceph entities and operations
- **Native Performance**: No C library dependencies

## 🏗️ Current Implementation Status

### ✅ Completed Components

- **Core Types**: EntityName, EntityAddr, FeatureSet, ConnectionInfo
- **Messenger Protocol**: Banner exchange, connection negotiation, feature negotiation  
- **Message System**: Message encoding/decoding, routing, dispatching
- **Async Connections**: Tokio-based connection handling with automatic keepalive
- **Frame Protocol**: Complete frame encoding/decoding for messenger v2
- **Error Handling**: Comprehensive error types and handling

### 🚧 In Progress

- Basic connection testing and validation
- Message handler framework
- CephX authentication system (partial)

### 📋 Planned Features

- **Monitor Client**: Connection and communication with Ceph monitors (**CRITICAL FIRST STEP**)
  - MonMap, OSD Map, and CRUSH map retrieval and processing
  - Object placement calculation (PG mapping)
- **CephX Authentication**: Complete secure authentication with Ceph clusters
- **OSD Client**: Direct communication with Object Storage Daemons (depends on Monitor client)
- **Object Operations**: PUT, GET, DELETE operations on RADOS objects
- **Pool Management**: Pool creation, deletion, and configuration

## 🚀 Quick Start

### Running Examples

The project includes examples demonstrating various components:

```bash
# Test msgr2 session connecting
cargo run --example test_session_connecting -p msgr2

# Test OSD map decoding
cargo run --example test_osdmap_decode -p denc
```

### Running Integration Tests

Integration tests use the external ceph-object-corpus:

```bash
# Run denc corpus tests (requires ceph-object-corpus)
CEPH_CONF=~/dev/ceph/build/ceph.conf cargo test -p denc --tests -- --ignored
```

### Using the Library

```rust
use msgr2::{SessionConnecting, ConnectionInfo};
use denc::{EntityName, EntityAddr};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create connection info
    let local_name = EntityName::client(12345);
    let peer_addr = EntityAddr::parse("v2:10.0.0.1:6789/0")?;

    let conn_info = ConnectionInfo {
        local_name,
        peer_addr,
        // ... other fields
    };

    // Connect to a Ceph monitor
    // See examples for complete usage

    Ok(())
}
```

## 🔧 Architecture

The project is organized as a Cargo workspace with three main crates:

### `denc` - Encoding/Decoding
- **Ceph Encoding**: Implementation of Ceph's DENC encoding/decoding protocol
- **Entity Types**: Strongly typed entities (MON, OSD, CLIENT, MDS, MGR)
- **Addresses**: Network addresses with nonces for unique identification
- **Features**: Capability negotiation and version compatibility
- **Complex Types**: OSDMap, PgPool, CRUSH map structures

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

## 🛠️ Development

### Building
```bash
cargo build
```

### Testing
```bash
cargo test
```

### Linting
```bash 
cargo clippy
```

## 🧪 Testing

### Available Test Cluster

A Docker-based Ceph cluster is available for testing purposes:

- **Config Path**: `/home/kefu/dev/rust-app-ceres/docker/ceph-config/ceph.conf`
- **Test Account**: `client.admin`
- **Keyring**: Available in the same directory as `ceph.client.admin.keyring`

Run tests against this cluster:
```bash
# Test connection to Ceph cluster
cargo run --bin rados-client ceph-test

# Run with debug logging
RUST_LOG=debug cargo run --bin rados-client ceph-test
```

## 📚 Documentation

This implementation is based on the Ceph messenger protocol as implemented in:
- **Ceph source**: `src/msg/` 
- **Crimson (SeaStore)**: `src/crimson/net/`
- **Official msgr2 protocol documentation**: `ceph/doc/dev/msgr2.rst` (**CRITICAL REFERENCE**)
- **Protocol documentation**: Ceph developer docs

## 🤝 Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bugs and feature requests.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🔗 Related Projects

- [Ceph](https://ceph.io/) - The original distributed storage system
- [librados](https://docs.ceph.com/en/latest/rados/api/librados/) - Official C/C++ client library
- [rust-ceph](https://github.com/ceph/ceph-rust) - Official Rust bindings (FFI-based)