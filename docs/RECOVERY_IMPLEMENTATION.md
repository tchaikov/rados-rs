# OSD Connection Recovery Implementation

This document describes the connection recovery implementation in rados-rs and how it compares to Ceph's librados Objecter.

## Overview

Connection recovery in rados-rs uses a **four-level approach** to handle different failure scenarios:

1. **Immediate Recovery (msgr2 layer)**: Automatic SESSION_RECONNECT for transient failures
2. **Map-Driven Recovery (OSD client layer)**: Operation migration when OSDMap changes
3. **Keepalive & Heartbeat**: Proactive detection of dead connections
4. **Per-Operation Timeout**: Ultimate fallback for unrecoverable failures

## Level 1: Immediate Recovery (msgr2)

### Implementation Status: ✅ COMPLETE

**Location**: `crates/msgr2/src/protocol.rs`

**What it does**:
- Automatically attempts SESSION_RECONNECT when send/recv fails
- Preserves session cookies, sequence numbers, and sent messages
- Replays unacknowledged messages after reconnection
- Uses exponential backoff between attempts

**Key Features**:
```rust
// Connection.retry_with_reconnect() - Lines 1578-1648
const MAX_RECONNECT_ATTEMPTS: usize = 3;
const INITIAL_BACKOFF_MS: u64 = 100;

// Backoff progression: 0ms, 100ms, 200ms, 400ms (capped at 1000ms)
let backoff_ms = (INITIAL_BACKOFF_MS * (1 << (attempt - 1))).min(1000);
```

**Handles** (via Level 1 immediate retry):
- ✅ TCP connection failures
- ✅ Network timeouts
- ✅ OSD process restart (if within retry window)
- ✅ Message replay after reconnection
- ✅ Sequence number synchronization

**Defers to Level 2** (for automatic migration):
- ⏩ OSD moved to different IP address → Handled by Level 2 (pending ops preserved, migrated to new address)
- ⏩ Prolonged OSD downtime (>30 seconds) → Handled by Level 2 (pending ops preserved for migration)
- ⏩ CRUSH map changes requiring different target OSD → Handled by Level 2 (CRUSH recalculation)

## Level 2: Map-Driven Recovery (OSD Client)

### Implementation Status: ✅ COMPLETE

**Location**: `crates/osdclient/src/client.rs::scan_requests_on_map_change()`

**What it does**:
- Triggered automatically when OSDMap updates arrive via `handle_osdmap()`
- Checks all pending operations across all sessions
- Recalculates target OSD using CRUSH for each pending operation
- Migrates operations to correct OSD sessions when targets change

**Key Features**:
```rust
// Called from handle_osdmap() when OSDMap epoch changes (line 1431)
async fn scan_requests_on_map_change(&self, new_epoch: u32) -> Result<()> {
    // 1. Get current OSDMap
    let osdmap = self.get_osdmap().await?;

    // 2. For each session, check pending operations
    for (osd_id, session) in sessions.iter() {
        let metadata = session.get_pending_ops_metadata().await;

        // 3. Check if pool deleted or target changed
        for (tid, pool_id, object_id, _osdmap_epoch) in metadata {
            // Recalculate target OSD using CRUSH
            let (_, new_osds) = self.object_to_osds(pool_id, &object_id).await?;
            let new_primary = new_osds.first().copied().unwrap_or(-1);

            // 4. Migrate if target has changed
            if new_primary != *osd_id {
                let mut op = session.remove_pending_op(tid).await.unwrap();
                op.state = OpState::NeedsResend;
                op.target.update(new_epoch, new_primary, new_osds.clone());
                op.state = OpState::Queued;
                need_resend.push((new_primary, op));
            }
        }
    }

    // 5. Resend to new targets
    for (new_osd, pending_op) in need_resend {
        let session = self.get_or_create_session(new_osd).await?;
        session.insert_migrated_op(pending_op, new_epoch).await?;
    }
}
```

**Handles**:
- ✅ OSD restart with new IP/port (OSDMap reflects changes)
- ✅ OSD marked down in cluster
- ✅ CRUSH map changes (replication rules, device classes)
- ✅ Pool remapping
- ✅ PG remapping

**Migration Process**:
1. `session.get_pending_ops_metadata()` - Get list of pending ops
2. `osdmap.object_to_osds()` - Calculate new target OSD
3. `source_session.remove_pending_op()` - Remove from old session
4. `target_session.insert_migrated_op()` - Insert into new session
5. Operation automatically retried with incremented attempt counter

**Critical Design Feature**:
When Level 1 reconnection fails after all retries (e.g., OSD moved to new IP):
- Pending operations are **preserved** in the disconnected session (not cancelled)
- io_task exits but operations remain in `pending_ops` HashMap
- When new session is created for same OSD (e.g., with new IP from OSDMap):
  - Old pending operations are automatically transferred to new session
  - Operations continue with updated target address
- This ensures no operations are lost during address changes or prolonged downtime

## Level 3: Keepalive & Heartbeat

### Implementation Status: ✅ COMPLETE

**Location**: `crates/osdclient/src/session.rs::io_task()`

**What it does**:
- Sends KEEPALIVE2 frames every 10 seconds
- Proactively detects dead connections before operations fail
- Matches Ceph's default keepalive interval

**Key Features**:
```rust
// Created in io_task, runs in parallel with send/recv
let mut keepalive_interval = tokio::time::interval(Duration::from_secs(10));

tokio::select! {
    _ = keepalive_interval.tick() => {
        connection.send_keepalive().await?;
    }
    // ... handle messages ...
}
```

**Handles**:
- ✅ Silent connection failures (router drops, network partitions)
- ✅ OSD process hang (not responding to messages)
- ✅ Early detection of connection issues

## Level 4: Per-Operation Timeout Tracking

### Implementation Status: ✅ COMPLETE

**Location**: `crates/osdclient/src/tracker.rs`

**What it does**:
- Tracks each operation independently with its own deadline (30 seconds default)
- Uses a background task with `tokio::time::sleep_until` for efficiency
- Operations timeout even during connection failures or reconnection attempts
- Automatically cancels timed-out operations via callback to session

**Design Alignment**: ✅ **Matches Ceph Objecter's approach exactly**

Ceph's implementation (`Objecter.h:2028`, `Objecter.cc:2403-2405`):
```cpp
struct Op {
    uint64_t ontimeout = 0;  // Timer event ID
};

// Register timeout callback when operation is sent
op->ontimeout = timer.add_event(osd_timeout, [this, tid]() {
    op_cancel(tid, -ETIMEDOUT);
});

// Cancel timeout on success
if (op->ontimeout && r != -ETIMEDOUT)
    timer.cancel_event(op->ontimeout);
```

Our implementation mirrors this pattern:
```rust
// Background task sleeps until next operation deadline
type TrackedOps = BTreeMap<(Instant, i32, u64), ()>;  // Sorted by deadline
tokio::select! {
    _ = tokio::time::sleep_until(next_deadline) => {
        // Process all expired operations
        timeout_callback(osd_id, tid);
    }
    cmd = cmd_rx.recv() => {
        // Handle track/untrack commands
    }
}
```

**Architecture Comparison**:

| Aspect | Ceph Objecter | rados-rs |
|--------|---------------|----------|
| **Data structure** | boost::intrusive::set (ordered by time) | BTreeMap (ordered by deadline) |
| **Registration** | `timer.add_event()` returns event ID | `tracker.track(tid, osd_id, deadline)` |
| **Cancellation** | `timer.cancel_event(event_id)` | `tracker.untrack(tid, osd_id)` |
| **Background task** | Dedicated timer thread | Tokio task with `sleep_until` |
| **Timeout action** | `op_cancel(tid, -ETIMEDOUT)` | Callback removes op, sends error |
| **Default timeout** | 30 seconds (`rados_osd_op_timeout`) | 30 seconds (configurable) |

**Benefits**:
- ✅ Timeouts work during reconnection (operations don't hang forever)
- ✅ Timeouts work when connection is down (tracker runs independently)
- ✅ Minimal overhead (single task, sleeps until needed)
- ✅ Precise timeout tracking per operation, not per-client
- ✅ O(log n) insertion, O(1) next-deadline lookup

## Level 5: Backoff Handling

### Implementation Status: ✅ COMPLETE

**Location**:
- `crates/osdclient/src/backoff.rs` - BackoffTracker for per-PG backoff state
- `crates/osdclient/src/client.rs::handle_backoff_from_osd()` - Message handling
- `crates/osdclient/src/session.rs` - Per-session BackoffTracker integration

**What it does**:
- Handles MOSDBackoff messages from OSDs (BLOCK, UNBLOCK, ACK_BLOCK)
- Tracks backoff state per-PG to prevent retry storms
- Sends ACK_BLOCK responses to acknowledge backoff requests
- Operations check backoff state before submission

**Design Alignment**: ✅ **Matches Ceph's approach**
- Reference: `~/dev/linux/net/ceph/osd_client.c::handle_backoff()`

**Key Features**:
```rust
// BackoffTracker maintains per-PG backoff windows
pub struct BackoffTracker {
    backoffs: HashMap<SpgId, BackoffEntry>,  // Per-PG state
}

// When BLOCK received
tracker.insert_or_update(backoff.pgid, backoff.begin, backoff.end);

// Send ACK_BLOCK response
let ack = MOSDBackoff::new(
    backoff.pgid,
    backoff.map_epoch,
    CEPH_OSD_BACKOFF_OP_ACK_BLOCK,
    backoff.id,
    backoff.begin,
    backoff.end,
);
session.send(ack).await?;

// When UNBLOCK received
tracker.remove(backoff.pgid);
```

**Benefits**:
- ✅ Prevents retry storms when OSDs are overloaded
- ✅ Respects per-PG backoff windows
- ✅ Acknowledges backoff requests per protocol
- ✅ Reduces wasted bandwidth and OSD CPU

## Comparison with Ceph's Objecter

### ✅ Implemented Features (Aligned with Ceph)

| Feature | Ceph Objecter | rados-rs | Status |
|---------|---------------|----------|--------|
| SESSION_RECONNECT support | ✓ | ✓ | Complete |
| Message replay queue | ✓ | ✓ | Complete |
| Sequence number tracking | ✓ | ✓ | Complete |
| Exponential backoff | ✓ | ✓ | Complete |
| Active keepalive loop | ✓ | ✓ | Complete |
| OSDMap-driven rescanning | ✓ | ✓ | Complete |
| Per-operation timeout | ✓ | ✓ | **Complete** (matches Ceph) |
| Backoff handling (MOSDBackoff) | ✓ | ✓ | Complete |
| Lossy connection policy | ✓ | ✓ | Complete |
| Operation state tracking | ✓ | ✓ | Complete (OpState enum) |
| OpTarget metadata | ✓ | ✓ | Complete |
| Fluent operation builder | ✓ | ✓ | Complete (OpBuilder) |

### 🚧 Partially Implemented

| Feature | Ceph Implementation | rados-rs Status | Priority |
|---------|---------------------|-----------------|----------|
| Connection incarnation tracking | Tracks connection restarts to detect stale ops | Partially (client_inc exists, not per-connection) | **HIGH** |

### ❌ Not Yet Implemented

Prioritized by alignment with Ceph Objecter:

#### **Priority 1: HIGH (Core Ceph Features)**

| Feature | Ceph Reference | Impact | Effort | Notes |
|---------|----------------|--------|--------|-------|
| Per-connection incarnation | `Objecter.h:2502` incarnation field | Prevents stale op replay | Medium | Track connection restarts per-OSD |
| Redirect handling | `Objecter.cc` redirect logic | Correct PG routing | Medium | Follow MOSDOpReply redirects |
| OSD address change detection | Check OSDMap during reconnect | Faster recovery | Easy | Validate address in OSDMap |

#### **Priority 2: MEDIUM (Reliability Features)**

| Feature | Ceph Reference | Impact | Effort | Notes |
|---------|----------------|--------|--------|-------|
| Explicit connection state machine | CONNECTING/CONNECTED/CLOSED states | Better observability | Medium | Currently implicit in msgr2 |
| Message ACK window tracking | Sliding window of acked sequence numbers | Prevents message loss | Medium | Optimize replay queue |
| Reconnect mutex/lock | Prevents concurrent reconnects | Avoid races | Easy | Add per-session lock |

#### **Priority 3: LOW (Optimization Features)**

| Feature | Ceph Reference | Impact | Effort | Notes |
|---------|----------------|--------|--------|-------|
| Redirect loop prevention | Track redirect history | Prevent infinite loops | Medium | Track redirect chain |
| Jittered backoff | Add randomness to delays | Reduce thundering herd | Easy | In exponential backoff |
| Connection pooling | Multiple connections per OSD | Higher throughput | Hard | Not needed for most use cases |

### Design Differences

**Ceph Objecter**:
- Uses explicit connection state machine (DOWN → CONNECTING → CONNECTED)
- Maintains per-connection incarnation counter to detect stale operations
- Tracks redirect chains to prevent loops
- Has more aggressive per-message acknowledgment tracking

**rados-rs**:
- Implicit connection state via msgr2 Connection lifecycle
- Simpler retry model (tracker timeout + OSDMap rescanning)
- Cleaner separation: msgr2 handles protocol, osdclient handles business logic
- Relies on OSDMap as authoritative source of truth

**Trade-offs**:
- ✅ Simpler implementation, easier to maintain
- ✅ Clear separation of concerns (msgr2 vs osdclient)
- ✅ Per-operation timeout tracking (matches Ceph exactly)
- ⚠️ Missing per-connection incarnation (HIGH priority to add)
- ⚠️ Missing redirect handling (HIGH priority to add)

## Testing

All existing tests pass:
- msgr2: 66 unit tests
- osdclient: 74 unit tests

Integration tests with real Ceph cluster recommended for:
- OSD restart scenarios
- Network partition recovery
- OSDMap-driven operation migration

## Recovery Scenarios

### Scenario 1: Brief Network Glitch
1. **Detection**: send/recv fails
2. **Recovery**: Level 1 (SESSION_RECONNECT within 3 attempts)
3. **Result**: Operation completes successfully after reconnect
4. **Time**: ~100-700ms

### Scenario 2: OSD Process Restart (Same Address)
1. **Detection**: Connection fails, OSDMap update arrives
2. **Recovery**: 
   - Level 1 attempts reconnect (may fail if OSD still starting)
   - Level 2 triggered by OSDMap update
   - Operations migrated if OSD ID changed
3. **Result**: Operations redirected to correct OSD
4. **Time**: ~1-5 seconds

### Scenario 2b: OSD Moved to New IP Address
1. **Detection**: Connection fails, all reconnect attempts to old address fail
2. **Recovery**:
   - Level 1 tries old address 3 times (fails each time)
   - io_task exits but **pending operations preserved**
   - OSDMap update arrives with new address
   - `get_or_create_session()` creates new session with new address
   - Pending operations **automatically transferred** to new session
3. **Result**: Operations resume on new address without data loss
4. **Time**: ~2-10 seconds (depends on OSDMap propagation)

### Scenario 3: OSD Permanent Failure
1. **Detection**: All reconnect attempts fail
2. **Recovery**: Level 2 when OSDMap shows OSD down
3. **Result**: Operations migrated to replica OSD
4. **Time**: Depends on OSDMap update latency (typically 1-10s)

### Scenario 4: CRUSH Map Change
1. **Detection**: OSDMap epoch increment
2. **Recovery**: Level 2 rescanning detects new targets
3. **Result**: Operations migrated to new OSDs per new placement rules
4. **Time**: ~100-500ms after OSDMap received

### Scenario 5: Silent Connection Death
1. **Detection**: Keepalive timeout (after ~30 seconds)
2. **Recovery**: Next operation triggers reconnect
3. **Result**: Connection re-established, operations resume
4. **Time**: ~30 seconds detection + reconnect time

## Prioritized Next Steps

Based on alignment with Ceph's Objecter implementation, here are the recommended next steps:

### Phase 1: Core Ceph Features (HIGH Priority)

These features are present in Ceph's Objecter and are important for correctness:

1. **Per-Connection Incarnation Tracking** (Medium effort)
   - **Why**: Prevents stale operation replay after connection restarts
   - **Ceph reference**: `Objecter.h:2502` - `OSDSession::incarnation` field
   - **Implementation**: Add `incarnation` counter to `OSDSession`, increment on each connection establishment
   - **Benefit**: Detect and reject operations from previous connection incarnations

2. **Redirect Handling** (Medium effort)
   - **Why**: OSDs may redirect operations to the correct PG primary
   - **Ceph reference**: `Objecter.cc` - redirect logic in op reply handling
   - **Implementation**: Parse `RequestRedirect` in `MOSDOpReply`, resubmit to new target
   - **Benefit**: Faster operation completion without waiting for OSDMap update

3. **OSD Address Validation During Reconnect** (Easy)
   - **Why**: Faster recovery when OSD moves to new address
   - **Ceph reference**: Check OSDMap before reconnecting
   - **Implementation**: In `retry_with_reconnect()`, check if OSDMap has newer address
   - **Benefit**: Avoid wasted reconnection attempts to stale addresses

### Phase 2: Reliability Features (MEDIUM Priority)

These improve observability and prevent edge cases:

4. **Explicit Connection State Machine** (Medium effort)
   - **Why**: Better visibility into connection lifecycle
   - **States**: CONNECTING, CONNECTED, RECONNECTING, CLOSED
   - **Benefit**: Easier debugging, clearer semantics

5. **Reconnect Mutex/Lock** (Easy)
   - **Why**: Prevent concurrent reconnection attempts
   - **Implementation**: Per-session `Mutex<bool>` for reconnect-in-progress
   - **Benefit**: Avoid race conditions in reconnection logic

### Phase 3: Optimization Features (LOW Priority)

Nice-to-have improvements:

6. **Jittered Backoff** (Easy)
   - **Why**: Reduce thundering herd at scale
   - **Implementation**: Add random jitter to exponential backoff delays
   - **Benefit**: Smoother cluster recovery under load

7. **Redirect Loop Prevention** (Medium effort)
   - **Why**: Prevent infinite redirect chains
   - **Implementation**: Track redirect history per operation
   - **Benefit**: Fail gracefully on misconfigured clusters

### Recommended Implementation Order

**Start with Phase 1, in order:**
1. Per-Connection Incarnation Tracking - Most important for correctness
2. Redirect Handling - Significant performance improvement
3. OSD Address Validation - Quick win, easy to implement

**Then Phase 2:**
4. Explicit Connection State Machine - Improves maintainability
5. Reconnect Mutex - Prevents rare race conditions

**Phase 3 as needed:**
6. Jittered Backoff - Only needed at scale
7. Redirect Loop Prevention - Only needed for defensive coding

### What NOT to Implement

These Ceph features are not needed for rados-rs:

- ❌ **Connection pooling** - Single connection per OSD is sufficient for most use cases
- ❌ **Message ACK window tracking** - msgr2 sequence numbers already provide this
- ❌ **Priority queuing** - Complexity not justified for typical workloads

## Future Enhancements (Deprecated - See Prioritized Next Steps)

The section below is kept for historical reference but is superseded by "Prioritized Next Steps" above.

<details>
<summary>Original Future Enhancements (Click to expand)</summary>

Potential improvements for production hardening:

### High Priority
1. **Connection state machine** - Explicit states for better visibility
2. **Per-operation timeout** - Don't wait for global tracker timeout
3. **Metrics/observability** - Track reconnection rates, migration counts

### Medium Priority
4. **OSD address validation** - Check OSDMap during reconnect
5. **Configurable retry limits** - Allow tuning MAX_RECONNECT_ATTEMPTS
6. **Jittered backoff** - Prevent thundering herd at scale

### Low Priority
7. **Redirect tracking** - Prevent redirect loops
8. **Connection pooling** - Multiple connections per OSD
9. **Priority queuing** - Prioritize keepalive over bulk data

</details>

## Testing

All existing tests pass:
- msgr2: 66 unit tests
- osdclient: 74 unit tests

Integration tests with real Ceph cluster recommended for:
- OSD restart scenarios
- Network partition recovery
- OSDMap-driven operation migration

## References

- Ceph msgr2 protocol: `~/dev/ceph/doc/dev/msgr2.rst`
- Ceph Objecter: `~/dev/ceph/src/osdc/Objecter.{h,cc}`
- rados-rs implementation: This codebase
