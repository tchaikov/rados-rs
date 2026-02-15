# GitHub Copilot Instructions for RADOS-RS

## Project Overview

This is a Rust native implementation of the RADOS (Reliable Autonomic Distributed Object Store) client library, providing modern async/await access to Ceph storage clusters. The project is a Cargo workspace with multiple crates implementing different aspects of the Ceph protocol.

### Project Goal
Implement a pure Rust librados client without C library dependencies, providing type-safe, async access to Ceph storage clusters.

## Key Guidelines

When working on this project, follow these essential principles:

1. **Minimal Changes**: Make the smallest possible changes to achieve the goal. Never start from scratch - always fix existing code.

2. **Test Before Commit**: ALWAYS run the complete test suite before committing:
   ```bash
   cargo fmt --all
   cargo clippy --workspace --all-targets -- -D warnings
   cargo test --workspace --all-targets
   ```

3. **Cross-Validation**: When implementing Ceph protocol features, always cross-validate with Ceph's C++ reference implementation and use ceph-dencoder for encoding validation.

4. **Type Safety**: Leverage Rust's type system for correctness. Use `thiserror` for custom errors, `anyhow` for application errors.

5. **Async/Await**: All I/O operations must use Tokio's async runtime.

6. **Code Style**: 
   - Follow standard Rust formatting (enforced by `cargo fmt`)
   - Address all Clippy warnings (CI runs with `-D warnings`)
   - Use meaningful variable names that reflect Ceph terminology

## Repository Structure

The project is organized as a Cargo workspace:

```
rados-rs/
├── crates/
│   ├── denc/          # Core encoding/decoding (Denc trait)
│   ├── denc-derive/   # Procedural macros for Denc
│   ├── dencoder/      # Binary for testing Ceph type encoding
│   ├── auth/          # CephX authentication
│   ├── msgr2/         # Messenger protocol v2 implementation
│   ├── monclient/     # Monitor client
│   ├── osdclient/     # OSD client
│   ├── crush/         # CRUSH algorithm implementation
│   ├── cephconfig/    # Ceph configuration parsing
│   └── rados/         # CLI tool
├── .github/
│   ├── copilot-instructions.md    # This file
│   ├── copilot-setup-steps.yml    # Dependency pre-installation
│   └── workflows/                 # CI/CD workflows
├── docker/            # Docker-based Ceph cluster for testing
├── docs/              # Additional documentation
└── CLAUDE.md          # Detailed development guide
```

## Architecture

### Core Crates
1. **`denc`** - Encoding/Decoding
   - Implements Ceph's DENC encoding/decoding protocol
   - Strongly typed entities (MON, OSD, CLIENT, MDS, MGR)
   - Network addresses with nonces
   - Feature negotiation and version compatibility
   - Complex types: OSDMap, PgPool, CRUSH map structures

2. **`denc-derive`** - Procedural macros for encoding/decoding
   - Derive macros for automatic encoding/decoding implementation

3. **`msgr2`** - Messenger Protocol v2
   - Complete implementation of Ceph messenger protocol v2.1
   - State machine-based protocol handling
   - Frame encoding/decoding with encryption and compression
   - Session management and connection negotiation
   - AES-128-GCM encryption and CRC32C checksums

4. **`auth`** - Authentication
   - CephX authentication protocol implementation
   - Keyring parsing and management
   - Service ticket handling

5. **`monclient`** - Monitor Client
   - Communication with Ceph monitors
   - Cluster map retrieval (MonMap, OSDMap, CRUSH)

6. **`osdclient`** - OSD Client
   - Direct communication with Object Storage Daemons
   - Object operations (read/write)

7. **`crush`** - CRUSH Algorithm
   - Object placement calculation
   - PG mapping logic

## Development Environment

### Required Before Each Commit
Before committing any changes, you MUST run these commands to ensure code quality:

1. **Format code**: `cargo fmt --all`
2. **Lint code**: `cargo clippy --workspace --all-targets -- -D warnings`
3. **Run tests**: `cargo test --workspace --all-targets`

All three must pass without errors before committing.

### Building
```bash
# Build entire workspace
cargo build --workspace

# Build specific crate
cargo build -p denc
```

### Testing
```bash
# Run all unit tests
cargo test --workspace --all-targets

# Run specific crate tests
cargo test -p denc
cargo test -p msgr2
cargo test -p auth

# Run integration tests (requires ceph-object-corpus to be cloned)
# These tests are marked with #[ignore] and require additional setup
cargo test -p denc --tests -- --ignored --nocapture
cargo test -p monclient --tests -- --ignored --nocapture
cargo test -p msgr2 --tests -- --ignored --nocapture
cargo test -p osdclient --tests -- --ignored --nocapture

# Run examples
cargo run --example test_osdmap_decode -p denc
```

### Linting
```bash
# Check formatting (use this before commit)
cargo fmt --all --check

# Auto-fix formatting
cargo fmt --all

# Run Clippy (required before commit)
cargo clippy --workspace --all-targets -- -D warnings
```

### Full CI Check Locally
To run the same checks as CI before pushing:
```bash
# Format check
cargo fmt --all --check

# Clippy check
cargo clippy --workspace --all-targets -- -D warnings

# Unit tests
cargo test --workspace --all-targets

# Corpus comparison tests (if ceph-object-corpus is available)
export CORPUS_ROOT=/tmp/ceph-object-corpus
export CORPUS_VERSION=19.2.0-404-g78ddc7f9027
cargo test -p dencoder --test corpus_comparison_test -- --ignored --nocapture
```

### CI Pipeline
The project uses GitHub Actions for CI with these workflows:

1. **Basic CI** (`.github/workflows/ci.yml`):
   - Format checking (`cargo fmt --all --check`)
   - Clippy linting (`cargo clippy --workspace --all-targets -- -D warnings`)
   - Unit tests (`cargo test --workspace --all-targets`)
   - Corpus comparison tests (multiple Ceph versions)

2. **Integration Tests** (`.github/workflows/test-with-ceph.yml`):
   - Starts a Docker-based Ceph cluster
   - Runs integration tests with real Ceph cluster
   - Tests with ceph-object-corpus

Dependencies are pre-installed via `.github/copilot-setup-steps.yml`

## Coding Standards and Conventions

### General Guidelines
1. **Never start from scratch** - Always fix existing code rather than rewriting
2. **Minimal changes** - Make the smallest possible changes to achieve the goal
3. **Type safety** - Leverage Rust's type system for correctness
4. **Async/await** - Use Tokio for all async operations
5. **Error handling** - Use `thiserror` for custom errors, `anyhow` for application errors
6. **Cross-validation** - Always cross-validate implementations with Ceph's C++ reference implementation
7. **Test before commit** - Ensure all tests pass before committing any changes

### Code Style
- Follow standard Rust formatting (enforced by `cargo fmt`)
- Clippy warnings must be addressed (CI runs with `-D warnings`)
- Prefer explicit types over type inference in public APIs
- Use meaningful variable names that reflect Ceph terminology

### Testing
- Unit tests should be colocated with the code they test
- Integration tests go in the `tests/` directory
- Use corpus files from Ceph for validation when available
- Always include roundtrip tests for encoding/decoding
- **Run all tests before committing**: Use `cargo test --workspace --all-targets` to ensure nothing breaks
- Cross-validate with Ceph's C++ implementation when implementing protocol features

## Ceph-Specific Context

### Encoding/Decoding (DENC)
- This project implements Ceph's DENC encoding protocol in Rust
- Reference implementation: `$HOME/dev/ceph/src/include/denc.h` (if Ceph source available)
- **Always cross-validate** with Ceph's C++ implementation when implementing encoding/decoding
- Corpus files contain binary-encoded Ceph types for validation
- Encoding may be affected by feature flags (marked with `WRITE_CLASS_ENCODER_FEATURES`)
- Use versioned encoding for forward compatibility (`VersionedEncode` trait)

### Key Concepts
1. **Features**: Capability negotiation between client and server
2. **Entity Types**: MON, OSD, CLIENT, MDS, MGR
3. **Addresses**: Network addresses with nonces for unique identification
4. **PG (Placement Groups)**: Fundamental unit of object distribution
5. **CRUSH**: Controlled Replication Under Scalable Hashing algorithm

### Validation Tools
When debugging encoding/decoding issues, you can use ceph-dencoder to analyze corpus files (if available):
```bash
# Example using ceph-dencoder from development build
cd $HOME/dev/ceph/build
bin/ceph-dencoder type <TYPE> import <FILE> decode dump_json
```

### Protocol References
- Official msgr2 protocol documentation: `$HOME/dev/ceph/doc/dev/msgr2.rst` (if Ceph source available)
- Protocol implementations in Ceph source (if available):
  - `$HOME/dev/ceph/src/crimson/net/ProtocolV2.{cc,h}`
  - `$HOME/dev/ceph/src/crimson/net/FrameAssemblerV2.{cc,h}`
  - `$HOME/dev/ceph/src/msg/async/ProtocolV2.{cc,h}`
  - `$HOME/dev/ceph/src/msg/async/frames_v2.{cc,h}`

### Testing Against Real Cluster
A local Ceph cluster may be available for testing:
- Config: `$HOME/dev/ceph/build/ceph.conf` or custom config path
- Start: `cd $HOME/dev/ceph/build; ../src/vstart.sh -d`
- Stop: `cd $HOME/dev/ceph/build; ../src/stop.sh`
- Official client verification (example):
  ```bash
  # Using development build with AddressSanitizer
  LD_PRELOAD=/usr/lib/libasan.so.8 $HOME/dev/ceph/build/bin/ceph \
    --conf "<path-to-ceph.conf>" \
    -s 2>/dev/null
  ```

## Implementation Status

### ✅ Completed
- Core entity types (EntityName, EntityAddr, FeatureSet)
- Messenger protocol v2 banner exchange and negotiation
- Frame protocol encoding/decoding
- Basic async connection handling
- CephX authentication structures (partial)

### 🚧 In Progress
- Complete CephX authentication handshake
- Message handler framework
- Connection state management

### 📋 Planned (Priority Order)
1. **Monitor Client** (CRITICAL FIRST STEP)
   - MonMap, OSD Map, CRUSH map retrieval
   - Object placement calculation (PG mapping)
2. **Complete CephX Authentication**
3. **OSD Client** (depends on Monitor client)
4. **Object Operations** (PUT, GET, DELETE)
5. **Pool Management**

## Common Patterns

### Encoding/Decoding
```rust
use denc::{Encode, Decode};

// Implement for new types
impl Encode for MyType {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), EncodeError> {
        // Encode fields in order
        self.field1.encode(buf)?;
        self.field2.encode(buf)?;
        Ok(())
    }
}

impl Decode for MyType {
    fn decode(buf: &mut impl Buf) -> Result<Self, DecodeError> {
        Ok(Self {
            field1: Decode::decode(buf)?,
            field2: Decode::decode(buf)?,
        })
    }
}
```

### Async Connection Handling
```rust
use msgr2::{SessionConnecting, ConnectionInfo};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let conn_info = ConnectionInfo {
        local_name: EntityName::client(12345),
        peer_addr: EntityAddr::parse("v2:10.0.0.1:6789/0")?,
        // ... other fields
    };
    
    // Establish connection
    let session = SessionConnecting::connect(conn_info).await?;
    Ok(())
}
```

### Versioned Encoding
```rust
impl VersionedEncode for MyType {
    fn encode_versioned(&self, buf: &mut impl BufMut) -> Result<(), EncodeError> {
        // ENCODE_START
        encode_version(buf, VERSION, COMPAT_VERSION)?;
        
        // Encode fields
        self.field1.encode(buf)?;
        if self.version >= 2 {
            self.field2.encode(buf)?;
        }
        
        // ENCODE_FINISH
        Ok(())
    }
}
```

## Important Notes

1. **No starting from scratch**: Always modify existing code rather than creating new files unless absolutely necessary
2. **Cross-validation with C++ implementation**: When implementing any Ceph protocol feature, always cross-validate behavior and output with Ceph's C++ reference implementation
3. **Validate against ceph-dencoder**: When implementing encoding, always validate against ceph-dencoder output when available
4. **Test before commit**: Always run `cargo test --workspace --all-targets` and ensure all tests pass before committing changes
5. **Feature flags**: Be aware that encoding may differ based on feature flags negotiated during connection
6. **Async patterns**: All I/O should use Tokio's async runtime
7. **Error context**: Provide meaningful error messages that help debug Ceph protocol issues

## Dependencies

Key dependencies and their purposes:
- `tokio`: Async runtime
- `bytes`: Efficient byte buffer manipulation
- `tracing`: Structured logging and diagnostics
- `thiserror`: Custom error types
- `anyhow`: Application error handling
- `crc32c`: Checksum calculation
- `serde`: Serialization/deserialization

## Related Documentation

- [Ceph Official Docs](https://docs.ceph.com/)
- [librados API](https://docs.ceph.com/en/latest/rados/api/librados/)
- [Ceph Messenger Protocol v2](https://docs.ceph.com/en/latest/dev/msgr2/)
- Project-specific docs in repository:
  - `CLAUDE.md`: Development context and guidelines
  - `STATUS.md`: Implementation status (Chinese)
  - `TODO.md`: Detailed task list
  - `DEVELOPMENT_NOTES.md`: Development tips
  - `ENCODING_METADATA.md`: Encoding system details
  - `MSGR2_ANALYSIS.md`: Messenger protocol analysis
