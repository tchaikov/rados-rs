# RADOS-RS Development TODO

> **Last Updated**: 2026-02-18
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

- [ ] **OSD connection recovery** — Session reconnection after OSD restart or
      network failure is not fully implemented. The `SESSION_RECONNECT` frame is
      supported at the msgr2 layer but the OSD client doesn't use it.
      See `crates/osdclient/src/client.rs:1316` TODO about rescanning sessions.

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

## 🎯 Recommended Priority Order

### Phase 1: Robustness & Testing (High Priority)
1. ~~Add unit tests for CephX client handler and protocol encode/decode~~ ✅ DONE (42 tests)
2. ~~Implement proper message length calculation in msgr2~~ ✅ DONE (commit bd89f5f)
3. ~~Fix IoCtx `flush()` to wait for actual OSD acknowledgments~~ ✅ NOT NEEDED (ops are sync)
4. Add OSD session reconnection using `SESSION_RECONNECT` (frame exists but not used by OSDClient)
5. ~~Replace debug `eprintln!()` with `tracing` macros~~ ✅ DONE

### Phase 2: Erasure Coding & Device Classes (High Priority)
1. ~~Implement CRUSH `CHOOSE_INDEP` for erasure-coded pool support~~ ✅ DONE
2. ~~Add CRUSH device class support for tiered placement~~ ✅ DONE
3. ~~Complete PgPool encoding (v24+ fields) for full roundtrip fidelity~~ ✅ DONE

### Phase 3: Extended API Surface (Medium Priority)
1. Expose xattr operations via IoCtx
2. Implement Watch/Notify for cache coherency
3. ~~Add `ObjectOperation` builder for compound operations~~ ✅ DONE (OpBuilder)
4. Add advisory object locking

### Phase 4: Advanced Features (Lower Priority)
1. Snapshot operations
2. RADOS class method calls
3. Connection pooling with load balancing
4. Metrics collection and distributed tracing (tracing logs exist, but no prometheus/opentelemetry)
5. ~~Rotating key renewal integration~~ ✅ DONE

---

**CI Configuration:**
- Unit tests: `cargo test --workspace --all-targets`
- Corpus tests: `cargo test -p dencoder --test corpus_comparison_test -- --ignored`
- Integration tests: See `.github/workflows/test-with-ceph.yml` (Docker-based)
- Linting: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings`