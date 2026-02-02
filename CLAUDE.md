# Rados-rs Development Guide

A Rust implementation of librados for Ceph cluster communication.

## Table of Contents

- [Project Overview](#project-overview)
- [Local Resources](#local-resources)
- [Development Cluster](#development-cluster)
- [Coding Standards](#coding-standards)
- [Verification Workflow](#verification-workflow)
- [Architecture Notes](#architecture-notes)

---

## Project Overview

### Goal
Implement Ceph type encoding/decoding and librados functionality in Rust.

### Key Technologies
- **Ceph Protocol**: msgr2 (messenger v2) for network communication
- **Encoding**: Denc trait for efficient serialization/deserialization
- **Authentication**: CephX authentication protocol

---

## Local Resources

### Ceph Source Code
- **Location**: `~/dev/ceph`
- **Build directory**: `~/dev/ceph/build`

### Protocol Implementation
Msgr2 protocol implemented in:
- `~/dev/ceph/src/crimson/net/ProtocolV2.{cc,h}`
- `~/dev/ceph/src/crimson/net/FrameAssemblerV2.{cc,h}`
- `~/dev/ceph/src/msg/async/ProtocolV2.{cc,h}`
- `~/dev/ceph/src/msg/async/frames_v2.{cc,h}`

### Documentation
- **Msgr2 protocol**: `~/dev/ceph/doc/dev/msgr2.rst`
- **Denc encoding**: `~/dev/ceph/src/include/denc.h`
- **OSDMap**: `~/dev/ceph/src/osd/OSDMap.{h,cc}`

### Test Data (Corpus)
- **Location**: `~/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/`
- Binary-encoded Ceph types for testing

---

## Development Cluster

### Starting the Cluster
```bash
cd ~/dev/ceph/build
../src/vstart.sh -d --without-dashboard
```

### Stopping the Cluster
```bash
cd ~/dev/ceph/build
../src/stop.sh
```

### Cluster Configuration
- **Config file**: `/home/kefu/dev/ceph/build/ceph.conf`
- **Usage in tests**: `export CEPH_CONF=/home/kefu/dev/ceph/build/ceph.conf`

### Verifying Behavior
Use official Ceph client to verify implementation behavior:
```bash
LD_PRELOAD=/usr/lib/libasan.so.8 \
  /home/kefu/dev/ceph/build/bin/ceph \
  --conf "/home/kefu/dev/ceph/build/ceph.conf" \
  -s 2>/dev/null
```

To capture TCP traffic for cross-checking, use tcpdump with the official client.

---

## Coding Standards

### Primary Principle: Use Denc Everywhere

**High-level code must use the Denc trait for ALL encoding/decoding.**
Manual primitive operations (`get_u*_le`, `put_u*_le`, `buf.remaining() < N`) should only appear inside Denc trait implementations.

### Denc Trait Usage

#### ✅ GOOD: High-Level Code
```rust
// In application code, services, protocol handlers
pub fn encode_message(&self) -> Result<Bytes> {
    let mut buf = BytesMut::new();

    // Use Denc::encode() for all fields
    self.version.encode(&mut buf, 0)?;      // Use ? operator
    self.session_id.encode(&mut buf, 0)?;   // No .map_err()
    self.payload.encode(&mut buf, 0)?;

    Ok(buf.freeze())
}

pub fn decode_message(data: &mut &[u8]) -> Result<Self> {
    // Use Denc::decode() for all fields
    let version = u64::decode(data, 0)?;        // Use ? operator
    let session_id = u64::decode(data, 0)?;     // No .map_err()
    let payload = Bytes::decode(data, 0)?;

    Ok(Self { version, session_id, payload })
}
```

#### ✅ GOOD: Denc Implementation (Low-Level)
```rust
// Inside impl Denc for CustomType
impl Denc for PgId {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        // Manual primitives are OK here - we ARE the Denc implementation
        buf.put_u64_le(self.pool);
        buf.put_u32_le(self.seed);
        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        // Manual primitives are OK here
        let pool = buf.get_u64_le();
        let seed = buf.get_u32_le();
        Ok(Self { pool, seed })
    }
}
```

---

## Bad Smells to Avoid

### ❌ Manual Primitive Encoding in High-Level Code
```rust
// BAD: Manual primitives outside Denc implementations
fn encode(&self, buf: &mut BytesMut) {
    buf.put_u64_le(self.version);        // BAD
    buf.put_u32_le(self.session_id);     // BAD
}

// GOOD: Use Denc
fn encode(&self, buf: &mut BytesMut) -> Result<()> {
    self.version.encode(buf, 0)?;
    self.session_id.encode(buf, 0)?;
    Ok(())
}
```

### ❌ Manual Buffer Length Checks
```rust
// BAD: Manual remaining() checks
if data.remaining() < 8 {
    return Err(Error::InsufficientData);
}
let value = data.get_u64_le();

// GOOD: Use Denc (handles bounds checking automatically)
let value = u64::decode(&mut data, 0)?;
```

### ❌ Manual Length-Prefixed Encoding
```rust
// BAD: Manual length + data
buf.put_u32_le(data.len() as u32);
buf.extend_from_slice(&data);

// GOOD: Use Bytes::encode()
let bytes = Bytes::from(data);
bytes.encode(&mut buf, 0)?;
```

### ❌ Duplicate Constants
```rust
// BAD: Defined in multiple places
const CEPH_AES_IV: &[u8; 16] = b"cephsageyudagreg";  // In file A
const CEPH_AES_IV: &[u8; 16] = b"cephsageyudagreg";  // In file B

// GOOD: Define once, import everywhere
use crate::protocol::CEPH_AES_IV;
```

### ❌ Manual Struct Encoding When Denc Could Be Used
```rust
// BAD: Manual field-by-field encoding
buf.put_u8(1);  // struct_v
buf.put_u64_le(magic);
payload.encode(&mut buf, 0)?;

// GOOD: Implement Denc for a wrapper struct
let envelope = CephXEncryptedEnvelope { payload };
envelope.encode(&mut buf, 0)?;
```

### ❌ Custom Error Handling for Each Field in Denc Implementations
```rust
// BAD: Wrapping every field's error with .map_err()
fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
    let field1 = u32::decode(buf, 0).map_err(|e| {
        RadosError::Protocol(format!("Failed to decode field1: {}", e))
    })?;
    let field2 = String::decode(buf, 0).map_err(|e| {
        RadosError::Protocol(format!("Failed to decode field2: {}", e))
    })?;
    Ok(Self { field1, field2 })
}

// GOOD: Just use ? operator and let Denc's error handling propagate
fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
    let field1 = u32::decode(buf, 0)?;
    let field2 = String::decode(buf, 0)?;
    Ok(Self { field1, field2 })
}
```

**Rationale**: The Denc trait already provides descriptive error messages. Adding custom error handling for each field creates noise without adding value. The stack trace will show which decode() call failed.

---

## Code Review Checklist

When reviewing code, investigate if you see:

1. ❌ Manual `if buf.remaining() < N` checks
2. ❌ Manual `get_u*_le()` or `put_u*_le()` calls outside `impl Denc`
3. ❌ Manual byte slicing and length prefix handling
4. ❌ Repeated encoding patterns across functions
5. ❌ Duplicate constant definitions
6. ❌ Custom `.map_err()` for every field in `decode()`

**Action**: Refactor to use Denc trait or consolidate into reusable structures.

### Exceptions (Where Manual Encoding is OK)

1. **Inside `impl Denc` blocks** - This is the implementation layer
2. **Helper functions in `crush` crate** - Avoids circular dependency
3. **Low-level AES encryption/decryption** - Requires raw bytes
4. **Test code** - May need manual encoding for specific test scenarios

---

## Verification Workflow

### Testing Encoding/Decoding

Use `ceph-dencoder` to analyze and verify encoded data:

```bash
cd ~/dev/ceph/build

# Decode and display a type from corpus
env ASAN_OPTIONS=detect_odr_violation=0,detect_leaks=0 \
    CEPH_LIB=$HOME/dev/ceph/build/lib \
    bin/ceph-dencoder type pg_pool_t \
    import ~/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/pg_pool_t/453c7bee75dca4766602cee267caa589 \
    decode dump_json
```

### Running Tests

```bash
# Unit tests
cargo test --package <crate>

# Integration tests (requires running cluster)
export CEPH_CONF=/home/kefu/dev/ceph/build/ceph.conf
cargo test --package monclient --tests -- --ignored --test-threads=1 --nocapture
```

### Development Tools

```bash
# Format code
cargo fmt

# Lint code
cargo clippy --all-targets --all-features

# Build
cargo build --package <crate>
```

---

## Architecture Notes

### Ceph Encoding System

#### Feature Flags
Encoding may be affected by feature flags at encode time. This is a compile-time property orthogonal to the `Sized` trait.

Types affected by features use `WRITE_CLASS_ENCODER_FEATURES` macro in C++.

#### Versioned Encoding
Many Ceph types use `ENCODE_START`/`DECODE_START` wrappers that include:
- Structure version number
- Compatibility version number

This allows forward/backward compatibility as types evolve.

#### Key Concepts
- **Corpus**: Binary-encoded test data from actual Ceph builds
- **Dencoder**: Tool to encode/decode Ceph types for verification
- **Features**: Capability flags that affect encoding format
- **OSDMap**: Core data structure containing cluster topology

### Message Hierarchy

```
Message (base)
├── PaxosServiceMessage (adds paxos fields)
│   ├── MAuth (authentication)
│   ├── MMonCommand (monitor commands)
│   └── MPoolOp (pool operations)
└── Regular Messages
    └── MAuthReply (auth responses)
```

**Key Rule**: PaxosServiceMessage types must encode paxos fields first (version, deprecated_session_mon, deprecated_session_mon_tid) before message-specific fields.

### Generic Patterns

#### CephXEncryptedEnvelope<T>
Generic wrapper for encrypted CephX protocol data:
```rust
pub struct CephXEncryptedEnvelope<T> {
    pub payload: T,
}
```

Automatically handles:
- struct_v (u8)
- AUTH_ENC_MAGIC (u64) verification
- Payload encoding/decoding

Use this instead of manually parsing struct_v and magic.

---

## Guidelines Summary

### DO:
✅ Use Denc trait for all encoding/decoding in high-level code
✅ Use `?` operator for error propagation
✅ Define constants once, import everywhere
✅ Implement Denc for reusable patterns
✅ Test against corpus data
✅ Run cargo fmt and cargo clippy

### DON'T:
❌ Use manual primitives (`get_u*_le`, `put_u*_le`) outside `impl Denc`
❌ Add manual buffer checks (`remaining() < N`)
❌ Wrap every field with `.map_err()`
❌ Duplicate constants across files
❌ Start from scratch - always fix existing code
❌ Tolerate test failures

---

## Quick Reference

| Task | Command |
|------|---------|
| Start cluster | `cd ~/dev/ceph/build && ../src/vstart.sh -d --without-dashboard` |
| Stop cluster | `cd ~/dev/ceph/build && ../src/stop.sh` |
| Decode corpus | `cd ~/dev/ceph/build && bin/ceph-dencoder type <TYPE> import <FILE> decode dump_json` |
| Run tests | `CEPH_CONF=~/dev/ceph/build/ceph.conf cargo test -p <PACKAGE> --tests -- --ignored` |
| Format code | `cargo fmt` |
| Lint code | `cargo clippy --all-targets --all-features` |
