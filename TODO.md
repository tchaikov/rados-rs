# RADOS-RS Development TODO

> **Last Updated**: 2025-02-15
>
> This document reflects the current implementation status based on a thorough
> code review. The project has 10 crates totaling ~41,000 lines of Rust code,
> with 311 unit tests passing and integration tests validated against a real
> Ceph cluster via Docker CI.

---

## Current Status Overview

| Crate | Purpose | Completeness | Tests |
|-------|---------|-------------|-------|
| **denc** | Encoding/decoding (Denc trait) | ✅ ~95% | 50+ unit, corpus |
| **denc-derive** | `#[derive(ZeroCopyDencode)]` proc macro | ✅ 100% | via denc |
| **dencoder** | ceph-dencoder compatible CLI tool | ✅ 100% | corpus comparison |
| **auth** | CephX authentication | ✅ ~85% | 3 unit |
| **msgr2** | Messenger protocol v2.1 | ✅ ~98% | 32+ unit |
| **crush** | CRUSH algorithm & object placement | ✅ ~90% | 20 unit |
| **monclient** | Monitor client | ✅ ~80% | 66+ unit |
| **osdclient** | OSD client, IoCtx, OSDMap | ✅ ~85% | 74+ unit |
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

- [ ] **PgPool encoding completeness** — Decoding works for all versions (v1–v29)
      but roundtrip encoding produces shorter output (missing v24+ fields like
      `application_metadata`, `create_time`, and v28+ fields).
      See `crates/osdclient/src/osdmap.rs:449-457` TODOs for `pool_opts_t` serialization.

- [ ] **CRUSH CHOOSE_INDEP** — Not implemented; needed for erasure-coded pools.
      Currently warns and skips. See `crates/crush/src/mapper.rs`.

- [ ] **CRUSH device classes** — Skipped during CRUSH map decode. Required for
      device-class-based placement rules (e.g., SSD vs HDD tiers).
      See `crates/crush/src/decode.rs`.

- [ ] **Auth test coverage** — CephX client handler (`client.rs`, 1208 lines) has
      zero unit tests. Protocol encode/decode (`protocol.rs`) also untested.
      Only keyring parsing and basic server setup are tested (3 tests total).

- [x] **OSD connection recovery** — Session reconnection after OSD restart or
      network failure is now implemented with a two-level approach:
      - Short-term failures: msgr2 Connection automatically uses SESSION_RECONNECT
        (up to 3 retries) when send/recv fails
      - OSD map changes: OSDClient.rescan_pending_ops() migrates pending operations
        to new target OSDs when OSDMap updates arrive (e.g., after OSD restart)
      See `crates/osdclient/src/client.rs` rescan_pending_ops() and
      `crates/osdclient/src/session.rs` io_task() documentation.

- [ ] **Proper message length calculation** — `msgr2/src/message.rs:117` uses a
      hardcoded `min(remaining, 1024)` instead of proper front/middle/data
      segment length parsing.

### Medium Priority

- [ ] **Extended attributes (xattrs)** — `OpCode::GetXattr`/`SetXattr` are defined
      in `osdclient/src/types.rs` but the IoCtx API methods are not exposed.
      The `OpData::Xattr` variant has a TODO for fields.

- [ ] **Rotating key support** — Auth structures for rotating keys exist but the
      renewal logic is not integrated with `MonClient`.

- [ ] **Server-side auth handler** — `CephXServerHandler` has basic implementation
      but lacks multi-service validation and integration testing.

- [ ] **MonClient map-specific APIs** — No convenience methods like `get_osdmap()`
      from MonClient; users must use low-level subscribe + `get_version()`.

- [ ] **IoCtx flush implementation** — `flush()` method currently just clears
      the pending writes map without actually waiting for OSD acknowledgments.
      See `crates/osdclient/src/ioctx.rs:362-379`.

- [ ] **OSDMap CRC validation** — CRC is read but not validated.
      See `crates/osdclient/src/osdmap.rs:2331`.

### Lower Priority

- [ ] **CRUSH MSR operations** — `ChooseMsr`, `SetMsrDescents`, `SetMsrCollisionTries`
      are marked unsupported.

- [ ] **GSS authentication** — Marked unimplemented in msgr2 auth handling.

- [ ] **Watch/Notify** — Not implemented. Needed for RBD and CephFS cache coherency.

- [ ] **Object locking** — Advisory locking API (`lock_exclusive`, `lock_shared`)
      not implemented.

- [ ] **Snapshots** — Snap context is encoded in MOSDOp but snapshot operations
      (snap create/remove/rollback) are not exposed.

- [ ] **Object classes** — RADOS class method calls (`call()`) not implemented.

- [ ] **Batch/compound operations** — `ObjectOperation` builder (like C++ librados
      `ObjectWriteOperation`/`ObjectReadOperation`) not implemented.

- [ ] **Metrics & observability** — No metrics collection or distributed tracing
      integration beyond `tracing` crate log output.

- [ ] **Connection pooling** — Each OSD gets a single session. No multi-connection
      or load-balancing support.

---

## 🐛 Known Issues & Technical Debt

- [ ] Debug `eprintln!()` statements in `monclient/src/connection.rs` (lines 176,
      190, 193) should be converted to `tracing` macros.
- [ ] `msgr2/src/frames.rs:488` TODO for multi-segment msgr2.1 epilogue handling.
- [ ] `msgr2/src/protocol.rs:1642-1660` message loop `run()` not yet implemented;
      requires architectural refactoring.
- [ ] `crush/src/decode.rs:365` ignored test for binary format alignment issues.
- [ ] `denc/src/denc.rs:925` TODO about derive macro not working inside denc crate.

---

## 🎯 Recommended Priority Order

### Phase 1: Robustness & Testing (High Priority)
1. Add unit tests for CephX client handler and protocol encode/decode
2. Implement proper message length calculation in msgr2
3. Fix IoCtx `flush()` to wait for actual OSD acknowledgments
4. ~~Add OSD session reconnection using `SESSION_RECONNECT`~~ ✓ **Complete**
5. Replace debug `eprintln!()` with `tracing` macros

### Phase 2: Erasure Coding & Device Classes (High Priority)
1. Implement CRUSH `CHOOSE_INDEP` for erasure-coded pool support
2. Add CRUSH device class support for tiered placement
3. Complete PgPool encoding (v24+ fields) for full roundtrip fidelity

### Phase 3: Extended API Surface (Medium Priority)
1. Expose xattr operations via IoCtx
2. Implement Watch/Notify for cache coherency
3. Add `ObjectOperation` builder for compound operations
4. Add advisory object locking

### Phase 4: Advanced Features (Lower Priority)
1. Snapshot operations
2. RADOS class method calls
3. Connection pooling with load balancing
4. Metrics collection and distributed tracing
5. Rotating key renewal integration

---

**CI Configuration:**
- Unit tests: `cargo test --workspace --all-targets`
- Corpus tests: `cargo test -p dencoder --test corpus_comparison_test -- --ignored`
- Integration tests: See `.github/workflows/test-with-ceph.yml` (Docker-based)
- Linting: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings`