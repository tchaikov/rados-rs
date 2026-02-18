# RADOS-RS Development TODO

> **Last Updated**: 2026-02-19
>
> This document reflects the current implementation status based on a thorough
> code review. The project has 10 crates totaling ~41,000 lines of Rust code,
> with 311 unit tests passing and integration tests validated against a real
> Ceph cluster via Docker CI.
>
> **Recent Verification** (2026-02-18): Comprehensive verification completed.
> Many items previously listed as TODO are now confirmed complete, including:
> CRUSH CHOOSE_INDEP, device classes, PgPool encoding, rotating keys, and
> batch operations via OpBuilder.

---

## Current Status Overview

| Crate | Purpose | Completeness | Tests |
|-------|---------|-------------|-------|
| **denc** | Encoding/decoding (Denc trait) | ✅ ~98% | 50+ unit, corpus |
| **denc-derive** | `#[derive(ZeroCopyDencode)]` proc macro | ✅ 100% | via denc |
| **dencoder** | ceph-dencoder compatible CLI tool | ✅ 100% | corpus comparison |
| **auth** | CephX authentication | ✅ ~90% | 42 unit |
| **msgr2** | Messenger protocol v2.1 | ✅ ~98% | 32+ unit |
| **crush** | CRUSH algorithm & object placement | ✅ ~95% | 20 unit |
| **monclient** | Monitor client | ✅ ~85% | 66+ unit |
| **osdclient** | OSD client, IoCtx, OSDMap | ✅ ~90% | 74+ unit |
| **cephconfig** | ceph.conf parser | ✅ 100% | 22 unit |
| **rados** | CLI tool (put/get/stat/rm/ls) | ✅ functional | integration |

---

## ✅ Completed Components

### Encoding/Decoding (`denc`)
- [x] Core `Denc` trait with buffer-based encode/decode
- [x] `VersionedEncode` trait with ENCODE_START/DECODE_START pattern
- [x] `ZeroCopyDencode` derive macro for POD types
- [x] Primitives: u8–u64, i8–i64, f32, f64, bool
- [x] Collections: `Vec<T>`, `String`, `BTreeMap`, `HashMap`, `Option<T>`, `Bytes`
- [x] Core types: `EVersion`, `UTime`, `UuidD`, `EntityName`, `EntityType`, `FsId`
- [x] Type-safe IDs: `OsdId`, `PoolId`, `Epoch`, `GlobalId`
- [x] `EntityAddr` / `EntityAddrvec` with MSG_ADDR2 feature support
- [x] `HObject` with Ceph-compatible ordering semantics
- [x] `MonMap`, `MonInfo`, `MonFeature`, `MonCephRelease`
- [x] `PgNlsResponse`, `ListObjectImpl`
- [x] Feature flags system (JEWEL through TENTACLE incarnation masks)
- [x] Encoding metadata system (`mark_simple_encoding!`, `mark_versioned_encoding!`)
- [x] 100% corpus validation for `entity_addr_t`, `pg_merge_meta_t`, `pg_pool_t`

### Authentication (`auth`)
- [x] CephX challenge-response authentication (client & server)
- [x] Session key derivation (AES-128-CBC with XOR folding)
- [x] Service ticket encryption/decryption
- [x] Authorizer building (`CephXAuthorizeA` + encrypted `CephXAuthorizeB`)
- [x] HMAC-SHA256 signing & verification
- [x] Keyring file parsing (INI format)
- [x] `AuthProvider` trait with `MonitorAuthProvider` and `ServiceAuthProvider`
- [x] Connection secret extraction (SECURE mode)
- [x] Ticket validity & renewal tracking

### Messenger Protocol (`msgr2`)
- [x] Complete msgr2.1 state machine (12 states, client + server)
- [x] All 22 frame tags implemented
- [x] Banner exchange with feature negotiation
- [x] AES-128-GCM encryption (SECURE mode) with nonce management
- [x] Compression: Snappy, Zstd, LZ4, Zlib
- [x] Session management with cookies and sequence numbers
- [x] `SESSION_RECONNECT` support for connection resumption
- [x] `KEEPALIVE2` / `KEEPALIVE2_ACK` frame support
- [x] Message throttling (rate limits, queue depth, async token bucket)
- [x] Message revocation system (Queued/Sending/Sent/Revoked)
- [x] High-level async `FrameIO` and `Connection` APIs
- [x] Error classification (recoverable vs. fatal)

### CRUSH Algorithm (`crush`)
- [x] All 5 bucket selection algorithms (Straw2, Uniform, List, Tree, Straw)
- [x] CRUSH rule execution engine (TAKE, CHOOSE_FIRSTN, CHOOSE_LEAF_FIRSTN, EMIT)
- [x] Tunable parameters (SetChooseTries, SetChooseLeafTries, etc.)
- [x] Object→PG mapping via rjenkins hash (bit-for-bit compatible with Ceph)
- [x] PG→OSD mapping via CRUSH rules
- [x] CRUSH map binary decoding
- [x] hashpspool support, namespace/key override
- [x] Precomputed ln lookup tables (ported from Ceph C++)

### Monitor Client (`monclient`)
- [x] Msgr2 connection with CephX authentication
- [x] Monitor hunting/failover with multiple connections
- [x] MonMap parsing, grouping, and updates
- [x] Subscription system (osdmap, monmap, etc.) with renewal
- [x] `invoke()` API for arbitrary monitor commands
- [x] `get_version()` / `MMonGetVersion` for map epochs
- [x] DNS SRV monitor discovery (`_ceph-mon._tcp`)
- [x] Pool operations via `MPoolOp` messages
- [x] Event broadcasting via broadcast channel
- [x] Keepalive timeout detection

### OSD Client (`osdclient`)
- [x] `OSDClient` with per-OSD session management
- [x] `IoCtx` for pool-scoped operations
- [x] Object operations: read, write, write_full, stat, delete
- [x] Sparse read support
- [x] Object listing with pagination (PGLS)
- [x] `MOSDOp` / `MOSDOpReply` encoding/decoding (version 9)
- [x] OSDMap full decode with pool/OSD state tracking
- [x] OSDMap incremental decode
- [x] PgPool versioned decode (v1–v29)
- [x] Request tracking with operation timeouts
- [x] OSD backoff handling
- [x] Request throttling (ops + bytes limits)
- [x] Pool create/delete via monitor commands
- [x] PgMap types: `OsdStat`, `PgStat`, `PoolStat`, `ObjectStatSum`, `ObjectStatCollection`

### Infrastructure
- [x] `cephconfig`: Full ceph.conf parser with typed values (Size, Duration, Ratio, Count)
- [x] `dencoder`: ceph-dencoder compatible tool (import/decode/dump_json/encode/export)
- [x] `rados` CLI: put, get, stat, rm, ls commands
- [x] CI pipeline: fmt check, clippy, unit tests, corpus comparison tests
- [x] Docker-based Ceph integration tests (monclient, msgr2, osdclient)

---

## 🚧 In Progress / Gaps

### High Priority

- [x] **PgPool encoding completeness** — COMPLETED. Both `application_metadata` and
      `create_time` are fully implemented in encoding/decoding at lines 904, 907, 1340,
      1343, 1604, 1609 of `crates/osdclient/src/osdmap.rs`.

- [x] **CRUSH CHOOSE_INDEP** — COMPLETED. Fully implemented for erasure-coded pools
      in `crates/crush/src/mapper.rs` lines 186-196, 431-458, 866-908 with test coverage.

- [x] **CRUSH device classes** — COMPLETED. Device classes are decoded in
      `crates/crush/src/decode.rs` lines 181-190 and accessible via `get_device_class()`
      API in `crates/crush/src/types.rs` lines 242-245.

- [x] **Auth test coverage** — COMPLETED. 42 unit tests covering client handler,
      protocol encode/decode, keyring parsing, and server setup.

- [x] **OSD connection recovery** — COMPLETED. Session recovery on OSDMap changes
      is fully implemented. The system detects down OSDs via `is_down()`, checks for
      address changes via `session_address_stale()`, drains pending operations with
      `drain_session_ops()`, and closes stale sessions. Operations are automatically
      re-targeted and migrated to new OSDs. See `crates/osdclient/src/client.rs:1360-1520`.

- [x] **Proper message length calculation** — COMPLETED. Message now properly
      uses `header.get_data_off()` to calculate segment lengths.

### Medium Priority

- [x] **Extended attributes (xattrs)** — COMPLETED. All xattr opcodes (GetXattr,
      SetXattr, RemoveXattr, ListXattrs) are implemented with OSDOp builders and
      IoCtx API methods (get_xattr, set_xattr, remove_xattr, list_xattrs).

- [x] **Rotating key support** — COMPLETED. Ticket renewal is fully integrated in
      MonClient tick loop (`crates/monclient/src/client.rs` lines 960-1050). The system
      checks `need_key()`, builds renewal requests via `build_ticket_renewal_request()`,
      and sends MAuth messages to renew service tickets.

- [ ] **Server-side auth handler** — `CephXServerHandler` has basic implementation
      but lacks multi-service validation and integration testing.

- [x] **MonClient map-specific APIs** — COMPLETED. `get_osdmap()` and `get_monmap()`
      convenience methods added to MonClient. OSDMap is now cached in MonClientState
      and accessible directly without requiring OSDClient integration.

- [x] **IoCtx flush implementation** — NOT NEEDED. No flush() method exists and
      operations are synchronous by default. Removed from scope.

- [x] **OSDMap CRC validation** — COMPLETED. CRC32C validation using Ceph's
      SCTP implementation. See commit f9d0798.

### Lower Priority

- [x] **CRUSH MSR operations** — COMPLETED. `ChooseMsr`, `SetMsrDescents`, and
      `SetMsrCollisionTries` operations are now fully implemented in
      `crates/crush/src/mapper.rs` with the `crush_choose_msr()` function.

- [ ] **GSS authentication** — Marked unimplemented in msgr2 auth handling.

- [ ] **Watch/Notify** — Not implemented. Needed for RBD and CephFS cache coherency.

- [x] **Object locking** — COMPLETED. Advisory locking API (lock_exclusive,
      lock_shared, unlock) fully implemented with OpCode::Call for object class
      method invocation. Lock types, flags, and request structures in lock.rs.
      IoCtx API methods available for all lock operations.

- [ ] **Snapshots** — Snap context is encoded in MOSDOp but snapshot operations
      (snap create/remove/rollback) are not exposed.

- [x] **Object classes** — COMPLETED. RADOS class method calls via OpCode::Call
      are fully implemented. The call() builder method allows invoking any object
      class method with custom input data.

- [x] **Batch/compound operations** — COMPLETED. `OpBuilder` in
      `crates/osdclient/src/operation.rs` provides fluent API for building single or
      compound operations (e.g., `.stat().read(0, 4096).build()`), equivalent to
      C++ librados ObjectOperation builders.

- [ ] **Metrics & observability** — No metrics collection or distributed tracing
      integration beyond `tracing` crate log output.

- [x] **Connection pooling** — NOT NEEDED. Each OSD gets a single session with
      one persistent connection, matching Ceph C++ Objecter design. Operations
      multiplex over the single connection via mpsc channels. This is the correct
      architecture and does not need traditional connection pooling. Documented
      in `crates/osdclient/README.md`.

---

## 🐛 Known Issues & Technical Debt

- [x] Debug `eprintln!()` statements already converted to `tracing` macros.
- [ ] `msgr2/src/frames.rs:488` TODO for multi-segment msgr2.1 epilogue handling.
- [ ] `msgr2/src/protocol.rs:1642-1660` message loop `run()` not yet implemented;
      requires architectural refactoring.
- [ ] `crush/src/decode.rs:365` ignored test for binary format alignment issues.
- [ ] `denc/src/denc.rs:925` TODO about derive macro not working inside denc crate.

---

## 🔍 Comprehensive Design Review Findings (2026-02-19)

> **Scope**: Full review of 11 crates (~41,000 lines) cross-referenced with
> `~/dev/ceph/src/msg/async/ProtocolV2.{cc,h}` (3,114 lines) and related C++ sources.

### TIER 1: CRITICAL — Session Recovery & Correctness

**Status**: Current implementation may lose messages or send duplicates during reconnection.

---

#### Missing Sent Message Queue & ACK Tracking

**C++ Implementation** (`ProtocolV2.h` lines 93–105):
- Maintains `std::list<Message *> sent` — messages awaiting ACK
- Tracks `out_seq`, `in_seq`, `ack_left` for message ordering
- On reconnection: `requeue_sent()` re-adds unACKed messages to outgoing queue
- Server ACKs allow `discard_requeued_up_to()` to remove confirmed messages

**Rust Status**:
- ❌ No sent message queue
- ❌ No ACK tracking
- ❌ Assumes messages are lost on reconnection

**Impact**:
- Messages may be duplicated (sent twice) or silently dropped
- Message ordering not guaranteed across reconnections
- Bandwidth wasted retransmitting already-received messages

**Recommendation**:
```rust
// Add to osdclient/src/session.rs
pub struct OSDSession {
    // ... existing fields ...
    sent_messages: VecDeque<(u64, Message)>,  // (seq, msg) pairs awaiting ACK
    out_seq: AtomicU64,
    in_seq: AtomicU64,
    ack_left: AtomicU64,
}

impl OSDSession {
    async fn handle_message_ack(&self, seq: u64) {
        self.sent_messages.retain(|(msg_seq, _)| *msg_seq > seq);
        self.ack_left.fetch_sub(1, Ordering::SeqCst);
    }

    async fn requeue_sent_on_reconnect(&mut self) {
        for (_, msg) in self.sent_messages.drain(..) {
            self.send_tx.send(msg).await.ok();
        }
    }
}
```

**Files to modify**:
- `crates/osdclient/src/session.rs` — Add sent queue + ACK handling
- `crates/msgr2/src/frames.rs` — Ensure ACK frame is decoded
- `crates/msgr2/src/protocol.rs` — Piggyback ACKs on data messages

**Effort**: HIGH COMPLEXITY

---

#### Incomplete Session Identity Tracking

**C++ Implementation** (`ProtocolV2.h` lines 84–91):
```cpp
uint64_t client_cookie;      // Client's session identifier
uint64_t server_cookie;      // Server's session identifier
uint64_t global_seq;         // Snapshot of global sequence
uint64_t connect_seq;        // Connection attempt counter
uint64_t peer_global_seq;    // Peer's global sequence
uint64_t message_seq;        // Application message counter
bool reconnecting;           // True during reconnection
bool replacing;              // True if replacing existing connection
```

**Rust Status**:
- ✅ Has incarnation counter (analogous to connect_seq)
- ❌ Missing `client_cookie`, `server_cookie`
- ❌ Missing `global_seq` tracking
- ❌ Missing `reconnecting` / `replacing` flags

**Impact**:
- Server cannot distinguish reconnections from new connections
- Cannot detect if client is replacing a previous connection
- Message ordering not validated across reconnections

**Recommendation**:
```rust
// Enhance SessionInfo in osdclient/src/session.rs
pub struct SessionInfo {
    conn_state: ConnectionState,
    peer_addr: Option<denc::EntityAddr>,

    // NEW: Session identity
    client_cookie: u64,
    server_cookie: u64,
    peer_supported_features: u64,

    // NEW: Sequence tracking
    global_seq: u64,
    connect_seq: u64,
    message_seq: u64,

    // NEW: Connection state flags
    reconnecting: bool,
    replacing: bool,
}
```

**Files to modify**:
- `crates/osdclient/src/session.rs` — Enhanced SessionInfo
- `crates/msgr2/src/state_machine.rs` — Cookie exchange in session negotiation
- `crates/msgr2/src/frames.rs` — Encode cookies in ClientIdent/ServerIdent frames

**Effort**: MEDIUM COMPLEXITY

---

### TIER 2: HIGH PRIORITY — Robustness & Error Handling

#### State Cleanup on Fault Recovery

**C++ Implementation** (`ProtocolV2.h` line 154):
- `_fault()` method with comprehensive cleanup:
  - `reset_recv_state()` — Clear incoming buffers
  - `reset_security()` — Reset encryption state
  - `reset_throttle()` — Clear throttle counters
  - `reset_session()` — Full session reset
  - `discard_out_queue()` — Clear pending sends

**Rust Status**:
- ✅ Fault handling exists (`StateResult::Fault`)
- ⚠️ Incomplete cleanup: no explicit crypto/throttle/buffer reset

**Recommendation**:
```rust
// Add to msgr2/src/state_machine.rs
impl StateMachine {
    pub fn fault_reset(&mut self) {
        self.session_stream_handlers = None;      // Clear crypto
        self.session_compression_handlers = None; // Clear compression
        self.throttle.reset();                    // Clear throttle state
        self.recv_buffer.clear();                 // Clear buffers
    }
}
```

**Files to modify**:
- `crates/msgr2/src/state_machine.rs` — Add `fault_reset()`
- `crates/msgr2/src/protocol.rs` — Call `fault_reset()` on errors
- `crates/msgr2/src/crypto.rs` — Add `reset()` method

**Effort**: LOW COMPLEXITY

---

#### Type Duplication — EntityName (4 Definitions)

| Crate | Fields | Use Case |
|-------|--------|----------|
| `denc::types` | `entity_type: EntityType, num: u64` | General encoding |
| `auth::types` | `entity_type: u32, id: String` | CephX authentication |
| `osdclient::types` | `entity_type: u8, num: u64` | Zero-copy wire protocol |
| `monclient::types` | `entity_type: String, entity_id: String` | Human-readable |

**Risk**: Conversion errors, maintenance burden, confusion when types diverge.

**Recommendation**:
1. Keep `denc::types::EntityName` as canonical (with `EntityType` enum + `u64`)
2. Zero-copy wire variant in `osdclient` stays if needed for performance
3. Add `From` trait implementations for conversions
4. Replace `auth` and `monclient` duplicates with re-exports or newtypes

**Files to modify**:
- `crates/denc/src/types.rs` — Canonical definition
- `crates/auth/src/types.rs` — Replace with newtype or re-export
- `crates/osdclient/src/types.rs` — Add `From<denc::EntityName>` if keeping own variant
- `crates/monclient/src/types.rs` — Replace with newtype or re-export

**Effort**: MEDIUM COMPLEXITY

---

#### Missing Compression Statistics

**C++ Implementation** (`compression_onwire.h`):
- Tracks ratio: `get_ratio()`, `get_initial_size()`, `get_final_size()`
- Useful for monitoring compression effectiveness and debugging performance

**Rust Status**:
- ✅ Compression algorithms implemented (Snappy, Zstd, LZ4, Zlib)
- ❌ No statistics tracking

**Recommendation**:
```rust
// Add to msgr2/src/compression.rs
pub struct CompressionStats {
    pub algorithm: CompressionAlgorithm,
    pub initial_size: u64,
    pub compressed_size: u64,
    pub compression_count: u64,
    pub decompression_count: u64,
}

impl CompressionStats {
    pub fn ratio(&self) -> f64 {
        self.compressed_size as f64 / self.initial_size as f64
    }
}
```

**Files to modify**:
- `crates/msgr2/src/compression.rs` — Add `CompressionStats`
- `crates/msgr2/src/protocol.rs` — Update stats during compress/decompress
- `crates/msgr2/src/connection.rs` — Expose stats via API

**Effort**: LOW COMPLEXITY

---

### TIER 3: MEDIUM PRIORITY — Idiomatic Rust Improvements

#### Async Pattern: Reduce Arc<Mutex> Contention

**Current Pattern** (some locations):
```rust
let state = client.state.lock().await;
// Lock held across await points or long computations
```

**Better Pattern**: Clone minimal data, release lock immediately:
```rust
let needed_data = {
    let state = client.state.lock().await;
    state.some_field.clone()
};
// Use cloned data without holding lock
```

**Or use `RwLock` for read-heavy access** (already done in `monmap`/`osdmap` — good pattern):
```rust
self.state.read().await   // Multiple readers
self.state.write().await  // Exclusive writer
```

**Files to audit**:
- `crates/monclient/src/client.rs` — Already uses `RwLock` (GOOD)
- `crates/osdclient/src/client.rs` — Check session map access patterns
- `crates/msgr2/src/protocol.rs` — Check state machine lock duration

**Effort**: LOW–MEDIUM COMPLEXITY

---

#### Error Handling: Manual Buffer Length Checks

**Pattern found throughout codebase**:
```rust
if buf.remaining() < 8 {
    return Err(RadosError::Protocol(format!(
        "Insufficient bytes: need 8, have {}", buf.remaining()
    )));
}
let value = buf.get_u64_le();
```

**Issue**: Verbose boilerplate; inconsistent error messages.

**Better Pattern**: Let nested `decode()` handle bounds checking:
```rust
let value = u64::decode(&mut buf, 0)?;  // Already checks bounds internally
```

**Recommendation**: Remove manual checks where nested `Denc::decode()` calls exist.

**Files to audit**: All files with manual `buf.remaining()` checks, particularly
`crates/denc/src/*.rs` and `crates/osdclient/src/denc_types.rs`.

**Effort**: LOW COMPLEXITY

---

#### Iterator Patterns: Prefer Functional Style

**Recommendation**: Where appropriate, replace manual accumulation loops:
```rust
// Before
let mut result = Vec::new();
for item in items {
    if let Some(v) = process(item) { result.push(v); }
}

// After
let result: Vec<_> = items.iter().filter_map(process).collect();
```

**Files to audit**:
- `crates/crush/src/mapper.rs` — CRUSH selection loops
- `crates/osdclient/src/client.rs` — Session iteration
- `crates/monclient/src/client.rs` — Monitor selection

**Effort**: LOW COMPLEXITY

---

### TIER 4: LOW PRIORITY — Observability & Performance

#### Missing Connection Diagnostics

**C++ Feature**: `dump(Formatter *f)` method for live state inspection.

**Rust Status**: ❌ No diagnostic dump capability.

**Recommendation**:
```rust
// Add to msgr2/src/protocol.rs
pub struct ConnectionDiagnostics {
    pub state: StateKind,
    pub remote_addr: SocketAddr,
    pub remote_features: u64,
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub last_keepalive: Option<Instant>,
    pub reconnection_count: u32,
    pub current_incarnation: u32,
}

impl Connection {
    pub fn diagnostics(&self) -> ConnectionDiagnostics { /* ... */ }
}
```

**Files to modify**:
- `crates/msgr2/src/protocol.rs` — Add `ConnectionDiagnostics`
- `crates/osdclient/src/session.rs` — Expose session diagnostics
- `crates/rados/src/main.rs` — Add `--debug-session` flag

**Effort**: MEDIUM COMPLEXITY

---

#### Missing Priority-Based Message Queueing

**C++ Feature** (`ProtocolV2.h` lines 93–105):
- `std::map<int, std::list<out_queue_entry_t>, std::greater<int>> out_queue`
- Higher-priority messages sent first (e.g., heartbeats before bulk data)

**Rust Status**: FIFO queue via `mpsc` channel — heartbeats can be delayed.

**Recommendation**:
```rust
// Add to msgr2/src/protocol.rs
pub enum MessagePriority { High = 2, Normal = 1, Low = 0 }

struct PriorityQueue {
    high: VecDeque<Message>,
    normal: VecDeque<Message>,
    low: VecDeque<Message>,
}

impl PriorityQueue {
    fn pop(&mut self) -> Option<Message> {
        self.high.pop_front()
            .or_else(|| self.normal.pop_front())
            .or_else(|| self.low.pop_front())
    }
}
```

**Files to modify**:
- `crates/msgr2/src/protocol.rs` — Replace `mpsc` with `PriorityQueue`
- `crates/osdclient/src/session.rs` — Set message priorities
- `crates/monclient/src/client.rs` — Mark heartbeats as `High`

**Effort**: MEDIUM COMPLEXITY

---

### Implementation Phases

| Phase | Goal | Items |
|-------|------|-------|
| **Phase 1** (Critical) | Fix session recovery to match C++ | Sent message queue, session identity |
| **Phase 2** (Robustness) | Improve error safety | Fault reset, type consolidation, compression stats |
| **Phase 3** (Idiomatic) | Cleaner Rust patterns | Async/lock audit, iterator style, error handling |
| **Phase 4** (Observability) | Visibility into runtime state | Diagnostics, priority queue |

### Risk Assessment

| Risk | Area | Mitigation |
|------|------|-----------|
| **HIGH** | Session recovery changes | Feature flag, extensive fault injection tests |
| **MEDIUM** | EntityName consolidation | Deprecation warnings, migration guide, test all conversions |
| **LOW** | Async/error refactoring | Mostly refactoring; no protocol changes |

### C++ Reference Locations

| Topic | File |
|-------|------|
| Session identity & sequence tracking | `src/msg/async/ProtocolV2.h` lines 84–105 |
| Sent message queue & ACK | `src/msg/async/ProtocolV2.h` lines 93–105 |
| Fault recovery | `src/msg/async/ProtocolV2.h` line 154 |
| Compression stats | `src/msg/async/compression_onwire.h` |
| OSD session management | `src/osdc/Objecter.h` lines 650–750 |

---

## 🎯 Recommended Priority Order

### Phase 1: Robustness & Testing (High Priority)
1. ~~Add unit tests for CephX client handler and protocol encode/decode~~ ✅ DONE (42 tests)
2. ~~Implement proper message length calculation in msgr2~~ ✅ DONE (commit bd89f5f)
3. ~~Fix IoCtx `flush()` to wait for actual OSD acknowledgments~~ ✅ NOT NEEDED (ops are sync)
4. ~~Add OSD session reconnection using `SESSION_RECONNECT`~~ ✅ DONE (session recovery on OSDMap changes)
5. ~~Replace debug `eprintln!()` with `tracing` macros~~ ✅ DONE

### Phase 2: Erasure Coding & Device Classes (High Priority)
1. ~~Implement CRUSH `CHOOSE_INDEP` for erasure-coded pool support~~ ✅ DONE
2. ~~Add CRUSH device class support for tiered placement~~ ✅ DONE
3. ~~Complete PgPool encoding (v24+ fields) for full roundtrip fidelity~~ ✅ DONE

### Phase 3: Extended API Surface (Medium Priority)
1. ~~Expose xattr operations via IoCtx~~ ✅ DONE (get_xattr, set_xattr, remove_xattr, list_xattrs)
2. Implement Watch/Notify for cache coherency
3. ~~Add `ObjectOperation` builder for compound operations~~ ✅ DONE (OpBuilder)
4. ~~Add advisory object locking~~ ✅ DONE (lock_exclusive, lock_shared, unlock)

### Phase 4: Advanced Features (Lower Priority)
1. Snapshot operations
2. ~~RADOS class method calls~~ ✅ DONE (OpCode::Call with call() builder)
3. ~~Connection pooling with load balancing~~ ✅ NOT NEEDED (1:1 OSD-to-session architecture)
4. Metrics collection and distributed tracing (tracing logs exist, but no prometheus/opentelemetry)
5. ~~Rotating key renewal integration~~ ✅ DONE

---

**CI Configuration:**
- Unit tests: `cargo test --workspace --all-targets`
- Corpus tests: `cargo test -p dencoder --test corpus_comparison_test -- --ignored`
- Integration tests: See `.github/workflows/test-with-ceph.yml` (Docker-based)
- Linting: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings`