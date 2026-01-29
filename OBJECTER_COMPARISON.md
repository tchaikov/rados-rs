# Objecter Implementation Comparison: rados-rs vs C++ Ceph

## Executive Summary

The rados-rs implementation provides a solid foundation for basic CRUD operations but is missing several advanced features present in the C++ Objecter. This document details the gaps and provides recommendations for improvement.

---

## ✅ Features Currently Implemented in rados-rs

1. **Basic CRUD Operations**: read, write, write_full, stat, delete
2. **Object Listing**: with pagination support
3. **Session Management**: Per-OSD connections with request tracking
4. **Message Encoding/Decoding**: MOSDOp and MOSDOpReply (v9/v8)
5. **CRUSH Integration**: Object-to-OSD mapping with PG overrides
6. **Authentication**: CephX protocol via MonClient
7. **Encryption**: AES-GCM support in msgr2
8. **Timeout Handling**: Per-operation timeouts
9. **Request Correlation**: Transaction ID tracking with async channels

---

## ❌ Missing Critical Features

### 1. **Linger Operations (Watch/Notify)**

**C++ Implementation:**
- `LingerOp` structure for long-lived subscriptions
- Watch operations with automatic keep-alive pings
- Notify operations for pub/sub patterns
- Reconnection support with register_gen tracking
- Error handling via `CB_DoWatchError`
- Watch ping timer with `watch_valid_thru` tracking

**Status in rados-rs:** ❌ Not implemented

**Impact:** HIGH - Required for:
- RBD image header watches
- Cache tier eviction notifications
- Object change notifications
- Distributed locking

**Location in C++:**
- `Objecter.h`: lines 835-858 (registration)
- `Objecter.cc`: lines 560-770 (implementation)

---

### 2. **OSD Backoff Handling**

**C++ Implementation:**
- `MOSDBackoff` message handling
- Per-PG backoff regions tracked in `OSDSession::backoffs`
- Automatic request pausing for specific object ranges
- Backoff expiration and resume logic

**Status in rados-rs:** ❌ Not implemented

**Impact:** HIGH - Without backoff handling:
- Client floods OSDs during recovery
- Violates OSD backpressure requests
- Degrades cluster performance

**Location in C++:**
- `Objecter.h`: lines 369-385
- `Objecter.cc`: `handle_osd_backoff()`

---

### 3. **Intelligent Retry Logic**

**C++ Implementation:**
- **Attempt tracking**: Each op has `attempts` counter
- **Attempt validation**: Ignores stale responses via attempt matching
- **EAGAIN handling**: Replica read bounces with automatic retry
- **Redirect handling**: Clears TID and resubmits on redirect reply
- **Map-triggered rescan**: `_scan_requests()` checks all ops on map updates
- **Target recalculation**: `_calc_target()` detects PG mapping changes

**Status in rados-rs:** ⚠️ **Partially implemented** - Basic timeout retry exists, but missing:
- ❌ OSDMap-triggered request rescanning
- ❌ Redirect reply handling
- ❌ EAGAIN/replica read retry
- ❌ Attempt validation for stale responses
- ❌ Session-level retry on connection loss

**Impact:** HIGH - Operations may:
- Fail during cluster topology changes
- Not follow redirects properly
- Use stale OSD connections

**Location in C++:**
- `Objecter.cc`: lines 3303-3699 (`handle_osd_op_reply()`)
- `Objecter.cc`: lines 1210-1442 (`handle_osd_map()`)

---

### 4. **Request Throttling and Budget System**

**C++ Implementation:**
- `Throttle op_throttle_bytes`: Limits bytes in-flight
- `Throttle op_throttle_ops`: Limits operation count
- `calc_op_budget()`: Calculates per-op budget
- Budget acquisition in `_op_submit_with_budget()`
- Budget release via `put_op_budget_bytes()`
- `keep_balanced_budget` mode for strict per-op throttling

**Status in rados-rs:** ❌ Not implemented

**Impact:** MEDIUM - Without throttling:
- Client can overwhelm OSDs with requests
- Memory usage unbounded for pending operations
- No backpressure mechanism

**Location in C++:**
- `Objecter.h`: lines 2085-2088
- `Objecter.cc`: lines 2377-2409, 2687-2691

---

### 5. **msgr2 Connection Resumption**

**C++ Implementation:**
- **Session cookies**: Client/server cookie pairs for session identity
- **Sequence tracking**: `out_seq`, `in_seq`, `message_seq`
- **Reconnect frames**: `TAG_SESSION_RECONNECT` with state preservation
- **Sent message queue**: Unacknowledged messages kept for retransmission
- **Selective replay**: `discard_requeued_up_to()` removes acknowledged messages
- **Race handling**: Address-based tie-breaking for simultaneous reconnects
- **Connection reuse**: `reuse_connection()` transfers TCP socket to existing session

**Status in rados-rs:** ❌ Not implemented

**Current behavior:** Connection loss = session loss, must re-authenticate

**Impact:** HIGH - Without resumption:
- Every TCP disconnection requires full re-authentication
- All pending operations lost on network hiccup
- Higher latency and overhead
- Not resilient to transient network issues

**Location in C++:**
- `ProtocolV2.cc`: lines 1200-1500 (reconnect handling)
- `ProtocolV2.cc`: lines 800-900 (reuse_connection)

---

### 6. **Session Incarnation Tracking**

**C++ Implementation:**
- `OSDSession::incarnation`: Generation number per session
- Incremented when session is closed and reopened
- Allows detection of stale responses from old sessions
- Cache invalidation on incarnation change

**Status in rados-rs:** ❌ Not implemented

**Impact:** MEDIUM - May process stale responses after reconnection

**Location in C++:**
- `Objecter.h`: line 315

---

### 7. **Pool-Level Error Detection**

**C++ Implementation:**
- **Pool DNE detection**: `POOL_DNE` flag in target
- **Pool EIO detection**: `POOL_EIO` flag
- **check_latest_map mechanism**: Waits for fresh map before retrying
- **Map epoch validation**: Ensures operations use current topology

**Status in rados-rs:** ❌ Not implemented

**Impact:** MEDIUM - Operations may fail without proper error context

**Location in C++:**
- `Objecter.cc`: lines 1350-1400 (in `handle_osd_map()`)

---

### 8. **Replica Read Support**

**C++ Implementation:**
- `BALANCE_READS` flag: Distributes reads across replicas
- `LOCALIZE_READS` flag: Prefers local OSDs for reads
- EAGAIN handling: Bounces back from replica to primary if needed
- `last_took` tracking: Remembers which replica was used

**Status in rados-rs:** ❌ Not implemented

**Impact:** LOW-MEDIUM - All reads go to primary OSD
- Higher load on primary OSDs
- Missed locality optimization opportunities

**Location in C++:**
- `Objecter.cc`: lines 2140-2180

---

### 9. **Command Operations**

**C++ Implementation:**
- `CommandOp` structure for admin commands
- `osd_command()` API for sending commands to specific OSDs
- Command result parsing with output buffers

**Status in rados-rs:** ❌ Not implemented

**Impact:** LOW - Not needed for basic client operations

---

### 10. **Completion Serialization**

**C++ Implementation:**
- `num_locks` completion locks per session
- Per-object-range locking via `completion_locks[]`
- Ensures callbacks fire in order per object
- Prevents race conditions in layered clients (e.g., RBD)

**Status in rados-rs:** ❌ Not implemented

**Impact:** LOW-MEDIUM - Callbacks may fire out of order

**Location in C++:**
- `Objecter.h`: line 319
- `Objecter.cc`: lines 3625-3645

---

### 11. **Trace/Span Context**

**C++ Implementation:**
- ZipkinTracer integration
- OpenTelemetry span context
- `BlkinTraceInfo` in messages
- `JaegerSpanContext` encoding

**Status in rados-rs:** ⚠️ **Partially implemented**
- ✅ BlkinTraceInfo encoding (hardcoded zeros)
- ❌ Actual tracer integration
- ❌ Span context propagation

**Impact:** LOW - Only needed for distributed tracing

---

### 12. **Performance Metrics**

**C++ Implementation:**
- Extensive perf counters via `PerfCounters`
- Per-operation-type metrics
- Laggy operation tracking
- Session statistics

**Status in rados-rs:** ❌ Not implemented

**Impact:** LOW - Useful for monitoring but not critical

---

## 🔍 Connection Resumption Deep Dive

### How msgr2 Connection Resumption Works

#### **Session Identity:**
```
Client Cookie (64-bit): Generated by client on first connect
Server Cookie (64-bit): Assigned by server in SERVER_IDENT frame
                        Stored by client for reconnection
```

#### **Reconnection Flow:**

1. **TCP Disconnection Detected**
   - Client's TCP connection fails
   - If `server_cookie != 0` and `!policy.lossy`, attempt reconnection

2. **Client Sends RECONNECT Frame**
   ```
   TAG_SESSION_RECONNECT {
       client_cookie: 0x123 (original)
       server_cookie: 0xABC (from server)
       global_seq: Y (monotonic, prevents stale reconnects)
       connect_seq: 1 (incremented on each reconnect attempt)
       msg_seq: 5 (last message sequence received from server)
   }
   ```

3. **Server Validation**
   - Looks up existing session by `server_cookie`
   - Validates `client_cookie` matches
   - Checks `global_seq` is not stale
   - Checks `connect_seq` for race conditions

4. **Server Sends RECONNECT_OK**
   ```
   TAG_SESSION_RECONNECT_OK {
       msg_seq: 3 (last message sequence received from client)
   }
   ```

5. **Message Replay**
   - Client has sent messages 1-5
   - Server only acknowledged 1-3 (via msg_seq=3)
   - Client resends messages 4-5 with original sequence numbers
   - Server processes 4-5, session fully restored

#### **What's Preserved:**
- ✅ Session identity (cookies)
- ✅ Cryptographic state (encryption keys)
- ✅ Message sequence numbers
- ✅ Unacknowledged sent messages (for replay)

#### **What's NOT Preserved:**
- ❌ TCP socket (replaced)
- ❌ Already-acknowledged messages (discarded)

---

## 📋 Priority Recommendations

### **Priority 1 (Critical):**
1. **Implement msgr2 Connection Resumption**
   - Required for production resilience
   - Prevents authentication overhead on reconnect
   - Enables lossless message delivery

2. **Implement OSDMap-Triggered Request Rescanning**
   - Required for cluster topology changes
   - Prevents operations from using stale mappings
   - Handle PG splits, pool deletions, OSD failures

3. **Implement OSD Backoff Handling**
   - Required for cluster health
   - Respects OSD backpressure
   - Prevents client-caused performance degradation

### **Priority 2 (High Value):**
4. **Implement Request Throttling**
   - Prevents resource exhaustion
   - Provides backpressure to application
   - Improves stability under load

5. **Implement Linger Operations (Watch/Notify)**
   - Required for RBD and other advanced features
   - Enables event-driven patterns
   - Critical for cache tier support

6. **Implement Redirect Handling**
   - Handle OSD-initiated request redirects
   - Required for erasure-coded pools
   - Needed for special pool configurations

### **Priority 3 (Nice to Have):**
7. **Implement Replica Read Support**
   - Performance optimization
   - Reduces primary OSD load
   - Locality improvements

8. **Implement Session Incarnation Tracking**
   - Robustness improvement
   - Prevents stale response processing

9. **Implement Completion Serialization**
   - Required for layered clients (RBD)
   - Prevents callback races

---

## 📝 Implementation Notes

### For Connection Resumption:

**Files to modify:**
- `msgr2/src/protocol.rs`: Add session state preservation
- `msgr2/src/state_machine.rs`: Add RECONNECT frame handling
- `osdclient/src/session.rs`: Add message queue for replay

**Key data structures needed:**
```rust
struct SessionState {
    client_cookie: u64,
    server_cookie: u64,
    global_seq: u64,
    connect_seq: u64,
    out_seq: AtomicU64,
    in_seq: AtomicU64,
    sent_messages: VecDeque<Message>,  // For replay
}
```

### For OSDMap Rescanning:

**Files to modify:**
- `osdclient/src/client.rs`: Subscribe to OSDMap updates
- `osdclient/src/session.rs`: Add `rescan_requests()` method

**Algorithm:**
```rust
async fn handle_osdmap_update(&self, new_map: OsdMap) {
    for session in self.sessions.values() {
        for (tid, op) in session.pending_ops.iter() {
            let new_target = self.calc_target(&op.object, &new_map);
            if new_target.osd != session.osd {
                // Resend to new OSD
                self.resend_op(op, new_target).await;
            }
        }
    }
}
```

### For Backoff Handling:

**Files to modify:**
- `msgr2/src/message_types.rs`: Add MOSDBackoff message type
- `osdclient/src/session.rs`: Add backoff tracking per PG

**Data structure:**
```rust
struct Backoff {
    pgid: PgId,
    begin: ObjectId,
    end: ObjectId,
    expire: Instant,
}
```

---

## 🎯 Conclusion

The rados-rs implementation provides a solid foundation but needs significant enhancements for production readiness. The three most critical gaps are:

1. **msgr2 connection resumption** - for network resilience
2. **OSDMap-triggered rescanning** - for topology awareness
3. **OSD backoff handling** - for cluster health

These should be addressed before considering the client production-ready for demanding workloads.
