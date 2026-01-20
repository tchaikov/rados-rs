# RADOS-RS

A Rust native implementation of the RADOS (Reliable Autonomic Distributed Object Store) client library, designed to provide modern async/await access to Ceph storage clusters.

## 📋 Repository Rewrite Plan

This repository is undergoing a structured rewrite to improve reviewability and maintainability. 

**👉 Start here: [REWRITE_SUMMARY.md](./REWRITE_SUMMARY.md)** - Quick reference and overview

### Planning Documents

- **[REWRITE_SUMMARY.md](./REWRITE_SUMMARY.md)** - Quick reference and overview (start here!)
- **[COMMIT_REWRITE_PLAN.md](./COMMIT_REWRITE_PLAN.md)** - Complete 49-commit sequence plan
- **[DEPENDENCIES.md](./DEPENDENCIES.md)** - Dependency graph and build order
- **[IMPLEMENTATION_GUIDE.md](./IMPLEMENTATION_GUIDE.md)** - Practical implementation guidance

The rewrite follows a bottom-up approach, building from foundational components (denc, auth) to high-level functionality (monclient, rados).

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

### Running the Example

The project includes a simple client/server example demonstrating the messenger protocol:

```bash
# Terminal 1: Start server
cargo run --bin rados-client server 6789

# Terminal 2: Connect as client  
cargo run --bin rados-client client 127.0.0.1:6789

# Terminal 3: Send a single ping
cargo run --bin rados-client ping 127.0.0.1:6789
```

### Using the Library

```rust
use rados_rs::{
    messenger::Messenger,
    types::{EntityName, EntityType, EntityAddr},
};
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> rados_rs::Result<()> {
    // Create a messenger instance
    let local_name = EntityName::client(12345);
    let bind_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
    let mut messenger = Messenger::new(local_name, bind_addr);
    
    // Start the messenger
    messenger.start().await?;
    
    // Connect to a Ceph monitor
    let mon_addr = EntityAddr::new_with_random_nonce("10.0.0.1:6789".parse().unwrap());
    let mon_name = EntityName::mon(0);
    messenger.connect_to(mon_addr, Some(mon_name)).await?;
    
    // Send messages...
    
    Ok(())
}
```

## 🔧 Architecture

### Messenger Layer
- **Connection Management**: Handles TCP connections with automatic reconnection
- **Protocol Handling**: Implements Ceph messenger protocol v2 
- **Message Routing**: Routes messages between local handlers and remote peers
- **Feature Negotiation**: Negotiates supported features with peers

### Type System
- **Entity Types**: Strongly typed entities (MON, OSD, CLIENT, MDS, MGR)
- **Addresses**: Network addresses with nonces for unique identification
- **Features**: Capability negotiation and version compatibility
- **Messages**: Type-safe message construction and parsing

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