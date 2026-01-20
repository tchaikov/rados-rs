# RADOS OSD Client

A Rust implementation of a RADOS OSD client for performing object operations against a Ceph cluster.

## Status: Foundation Complete ✅

This crate provides a complete architectural foundation for OSD operations. The core structure, types, message encoding, and API are implemented and ready for integration testing.

## Implemented Features

### Phase 1: Foundation ✅
- Complete crate structure with modular organization
- Core types:
  - `ObjectId` - Object identification (hobject_t equivalent)
  - `StripedPgId` - Striped placement group ID (spg_t equivalent)
  - `RequestId` - Request tracking
  - `OpCode` - Operation types enum
  - `OSDOp` - Operation structure with builders
  - Result types: `ReadResult`, `WriteResult`, `StatResult`
- Error handling with comprehensive error types
- Compiles cleanly with cargo fmt/clippy

### Phase 2: Message Protocol ✅
- MOSDOp message encoding (v8 format)
- MOSDOpReply message decoding
- Message type constants (CEPH_MSG_OSD_OP = 42, CEPH_MSG_OSD_OPREPLY = 43)
- Simplified but functional encoding that follows Ceph protocol structure

### Phase 3: Session Management ✅
- `OSDSession` - Per-OSD connection and request tracking
- Request ID generation with atomic counters
- Pending operation tracking with HashMap
- Background receive task framework (recv_task)
- Connection management infrastructure

### Phase 4: Client API ✅
- `OSDClient` - Main entry point for object operations
- Public API methods:
  - `read(pool, oid, offset, len)` - Read object data
  - `write(pool, oid, offset, data)` - Write object data
  - `write_full(pool, oid, data)` - Overwrite entire object
  - `stat(pool, oid)` - Get object statistics
  - `delete(pool, oid)` - Delete object
- Session management with connection pooling
- CRUSH placement integration (placeholder)
- Operation timeout handling with tracker

### Phase 5: Operation Builders ✅
- Type-safe operation construction
- `ReadOp`, `WriteOp`, `StatOp` builders
- Clean, discoverable API

## What Needs Completion

### 1. MonClient Integration 🔧

The MonClient needs to expose a method to get the current OSDMap:

```rust
// Add to crates/monclient/src/client.rs
impl MonClient {
    pub async fn get_osdmap(&self) -> Result<Arc<OSDMap>> {
        let state = self.state.read().await;
        // Return the latest decoded OSDMap from state
        // This requires storing decoded OSDMaps in MonClientState
    }
}
```

**Why needed**: CRUSH placement requires pool info and CRUSH map from OSDMap

### 2. OSD Connection 🔧

Complete the `OSDSession::connect()` method:

```rust
// In crates/osdclient/src/session.rs
pub async fn connect(&self, osd_addr: &str) -> Result<()> {
    // 1. Parse osd_addr (format: "v2:IP:PORT/NONCE")
    // 2. Create msgr2::Connection
    // 3. Perform banner exchange
    // 4. Perform HELLO handshake
    // 5. Perform AUTH handshake
    // 6. Store connection
}
```

**Reference**: See `monclient/src/connection.rs` for msgr2 connection patterns

### 3. Message Send/Receive Integration 🔧

Wire up actual message transmission:

```rust
// In session.submit_op()
conn.send_message(msgr2::message::Message::new(
    CEPH_MSG_OSD_OP,
    payload,
    // data section for write payloads
)).await?;

// In session.recv_task()
loop {
    let msg = conn.recv_message().await?;
    if msg.msg_type() == CEPH_MSG_OSD_OPREPLY {
        let reply = MOSDOpReply::decode(&msg.front)?;
        self.handle_reply(reply).await;
    }
}
```

### 4. Message Encoding Refinement 🔧

The current implementation uses simplified v8 encoding. For production:

- Add proper hobject_t encoding (currently simplified)
- Add object_locator_t encoding with all fields
- Add osd_reqid_t encoding
- Handle message features flags correctly
- Add trace/tracing support
- Test against actual Ceph corpus files

### 5. Integration Testing 📝

Create tests against local Ceph cluster:

```rust
// crates/osdclient/tests/integration.rs
#[tokio::test]
async fn test_write_read_cycle() {
    let mon_client = /* connect to local cluster */;
    let osd_client = OSDClient::new(config, mon_client).await?;

    let data = Bytes::from("Hello RADOS!");
    osd_client.write_full(pool_id, "test_obj", data.clone()).await?;

    let result = osd_client.read(pool_id, "test_obj", 0, data.len()).await?;
    assert_eq!(result.data, data);
}
```

### 6. Examples 📝

Create practical examples:

- `examples/simple_write.rs` - Write data to an object
- `examples/simple_read.rs` - Read data from an object
- `examples/object_lifecycle.rs` - Create, read, stat, delete cycle

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        User Application                      │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
                   ┌───────────────┐
                   │  OSDClient    │  Public API
                   └───────┬───────┘
                           │
            ┌──────────────┼──────────────┐
            │              │              │
            ▼              ▼              ▼
      ┌─────────┐    ┌──────────┐   ┌────────┐
      │ Session │    │  CRUSH   │   │ Tracker│
      │ Manager │    │Placement │   │        │
      └────┬────┘    └────┬─────┘   └────────┘
           │              │
           │    ┌─────────┴──────┐
           │    │                │
           ▼    ▼                ▼
    ┌──────────────┐      ┌─────────────┐
    │ OSDSession   │      │ MonClient   │
    │ (per OSD)    │      │ (OSDMap)    │
    └──────┬───────┘      └─────────────┘
           │
           ▼
    ┌──────────────┐
    │ msgr2        │
    │ Connection   │
    └──────────────┘
           │
           ▼
       [Ceph OSD]
```

## Usage Example (Once Complete)

```rust
use osdclient::{OSDClient, OSDClientConfig};
use monclient::{MonClient, MonClientConfig};
use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<()> {
    // Connect to monitors
    let mon_config = MonClientConfig {
        entity_name: "client.admin".to_string(),
        mon_addrs: vec!["v2:127.0.0.1:3300".to_string()],
        ..Default::default()
    };
    let mon_client = Arc::new(MonClient::new(mon_config).await?);
    mon_client.init().await?;

    // Subscribe to OSDMap
    mon_client.subscribe("osdmap", 0, 0).await?;

    // Create OSD client
    let osd_config = OSDClientConfig::default();
    let osd_client = OSDClient::new(osd_config, mon_client).await?;

    // Write data
    let data = Bytes::from("Hello RADOS!");
    osd_client.write_full(1, "my_object", data).await?;

    // Read data
    let result = osd_client.read(1, "my_object", 0, 100).await?;
    println!("Read {} bytes", result.data.len());

    // Stat object
    let stat = osd_client.stat(1, "my_object").await?;
    println!("Size: {}, mtime: {:?}", stat.size, stat.mtime);

    // Delete object
    osd_client.delete(1, "my_object").await?;

    Ok(())
}
```

## Testing Against Local Cluster

```bash
# Start local Ceph cluster
cd ~/dev/ceph/build
../src/vstart.sh -d

# Run integration tests
cd ~/dev/rados-rs
cargo test --package osdclient --test integration

# Run examples
cargo run --package osdclient --example simple_write

# Stop cluster
cd ~/dev/ceph/build
../src/stop.sh
```

## Message Format Reference

### MOSDOp (v8 encoding):
1. pgid (spg_t): pool (i64), seed (u32), shard (i8)
2. hash (u32)
3. osdmap_epoch (u32)
4. flags (u32)
5. reqid (osd_reqid_t)
6. trace (empty for now)
7. client_inc (u32)
8. mtime (u64)
9. object_locator_t
10. object name (string)
11. operations (vector of osd_op)
12. snapid (u64)
13. snap_seq (u64)
14. snaps (vector)
15. retry_attempt (i32)
16. features (u64)

### MOSDOpReply (simplified):
1. object (hobject_t)
2. pgid (spg_t)
3. flags (u32)
4. result (i32)
5. epoch (u32)
6. version (u64)
7. user_version (u64)
8. reqid
9. operations (vector of results with outdata)

## Files

- `src/lib.rs` - Public API exports
- `src/client.rs` - OSDClient implementation
- `src/session.rs` - OSDSession and request tracking
- `src/messages.rs` - MOSDOp/MOSDOpReply encoding/decoding
- `src/types.rs` - Core types and structures
- `src/error.rs` - Error types
- `src/tracker.rs` - Request timeout tracking
- `src/operation.rs` - Operation builders

## Next Steps

1. **Immediate**: Add `get_osdmap()` to MonClient (see section 1 above)
2. **Connection**: Implement OSDSession::connect() (see section 2 above)
3. **Integration**: Wire up message send/receive (see section 3 above)
4. **Testing**: Create integration tests against local cluster
5. **Refinement**: Enhance message encoding based on test results
6. **Production**: Add retry logic, backoff handling, connection pooling

## Acknowledgments

This implementation follows the architecture outlined in the implementation plan and learns from (but does not copy) the C++ Objecter implementation in the Ceph codebase. The design prioritizes idiomatic Rust patterns with async/await, strong typing, and zero-copy where possible.
