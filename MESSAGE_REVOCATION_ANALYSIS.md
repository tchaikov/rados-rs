# Message Revocation Analysis: Ceph vs Our Implementation

## Summary

After analyzing the Ceph source code, I found that **Ceph does NOT implement message revocation in the way we did**. The "revocation" concept in our implementation is actually a misunderstanding of Ceph's architecture.

## What Ceph Actually Does

### 1. Message Queue Management

Ceph has three main message queues:

```cpp
// From ProtocolV2.cc
std::list<Message*> sent;                    // Messages sent but not ACKed
std::map<int, std::list<out_queue_entry_t>> out_queue;  // Messages waiting to send
```

**Key operations:**

#### `requeue_sent()` - NOT Revocation
```cpp
void ProtocolV2::requeue_sent() {
  // Move messages from 'sent' back to 'out_queue' for retransmission
  // This happens on connection failure, NOT message cancellation
  out_seq -= sent.size();
  while (!sent.empty()) {
    Message *m = sent.back();
    sent.pop_back();
    m->clear_payload();  // Clear to re-encode
    rq.emplace_front(out_queue_entry_t{false, m});
  }
}
```

**Purpose**: Retransmit messages after connection failure (reconnection support)

#### `discard_requeued_up_to(seq)` - NOT Revocation
```cpp
uint64_t ProtocolV2::discard_requeued_up_to(uint64_t out_seq, uint64_t seq) {
  // Discard messages that were ACKed by the server
  // This happens after reconnection when server tells us what it received
  while (!rq.empty()) {
    Message* const m = rq.front().m;
    if (m->get_seq() == 0 || m->get_seq() > seq) break;
    m->put();  // Release message
    rq.pop_front();
  }
}
```

**Purpose**: Clean up messages that were already received by server (after reconnection)

#### `discard_out_queue()` - Connection Teardown
```cpp
void ProtocolV2::discard_out_queue() {
  // Discard ALL pending messages when connection is closed/reset
  for (Message *msg : sent) {
    msg->put();
  }
  sent.clear();
  for (auto& [ prio, entries ] : out_queue) {
    for (auto& entry : entries) {
      entry.m->put();
    }
  }
  out_queue.clear();
}
```

**Purpose**: Clean up when connection is permanently closed (not reconnecting)

### 2. What About "revoke_rx_buffer"?

Found in `Connection.h`:
```cpp
void revoke_rx_buffer(ceph_tid_t tid) {
  // This is for RECEIVE buffers, not SEND messages
  // Used to cancel a pre-allocated receive buffer
}
```

This is for **receive-side buffer management**, not message cancellation.

## What About Linux Kernel's Implementation?

### Sparse Read (NOT Revocation)

The Linux kernel implementation has `sparse_read` which is about **efficient data reading**, not message cancellation:

```c
// From linux/include/linux/ceph/messenger.h
int (*sparse_read)(struct ceph_connection *con,
                   struct ceph_msg_data_cursor *cursor,
                   char **buf);
```

**Purpose**: Read sparse data (files with holes) efficiently by skipping zero regions.

### Frame Abortion (Partial Feature)

Found in `frames_v2.h`:
```c
#define FRAME_LATE_FLAG_ABORTED           (1<<0)
#define FRAME_LATE_STATUS_ABORTED         0x1
```

**Purpose**: Signal that a frame transmission was aborted mid-flight. However, searching the code shows this is **defined but not actually used** in the current implementation.

## Comparison: Ceph vs Our Implementation

| Feature | Ceph | Our Implementation | Verdict |
|---------|------|-------------------|---------|
| Cancel in-flight messages | ❌ No | ✅ Yes | **Over-engineered** |
| Requeue on reconnection | ✅ Yes | ✅ Yes | ✅ Correct |
| Discard ACKed messages | ✅ Yes | ✅ Yes | ✅ Correct |
| Discard on connection close | ✅ Yes | ❌ No | ⚠️ Missing |
| Frame abortion | 🟡 Defined but unused | ❌ No | 🟡 Not needed |
| Sparse read | ✅ Yes (kernel) | ❌ No | 🟡 Optional |

## What We Actually Need

### ✅ Already Implemented Correctly

1. **Message replay on reconnection** - We have this in `Connection::reconnect()`
2. **Discard ACKed messages** - We have this in `discard_acknowledged_messages()`
3. **Sent message queue** - We have this in `ConnectionState::session.sent_messages`

### ❌ Missing: Connection Teardown

We need to add `discard_out_queue()` equivalent:

```rust
impl Connection {
    /// Discard all pending messages when connection is permanently closed
    pub fn discard_pending_messages(&mut self) {
        self.state.clear_sent_messages();
        // Also need to clear any queued messages waiting to send
    }
}
```

### ❌ Our Revocation System is Unnecessary

The `RevocationManager` we implemented is **not used by Ceph**. Ceph's approach is simpler:

- **Before sending**: Messages are in `out_queue`, can be discarded by clearing the queue
- **After sending**: Messages are in `sent` queue, will be retransmitted on reconnection
- **After ACK**: Messages are removed from `sent` queue

**Ceph never cancels individual messages mid-flight.** It either:
1. Sends them successfully
2. Retransmits them on reconnection
3. Discards ALL messages on connection close

## Recommendations

### Option 1: Remove Revocation (Recommended)

**Rationale**: Ceph doesn't use it, adds complexity, not needed for compatibility.

**Changes**:
```rust
// Remove from Connection
pub struct Connection {
    state: ConnectionState,
    server_addr: SocketAddr,
    target_entity_addr: Option<denc::EntityAddr>,
    config: crate::ConnectionConfig,
    throttle: Option<crate::throttle::MessageThrottle>,
    // revocation_manager: Option<crate::revocation::RevocationManager>, // REMOVE
}
```

**Keep the code** in `revocation.rs` for potential future use, but don't integrate it into Connection.

### Option 2: Keep Revocation as Optional Feature

**Rationale**: Might be useful for advanced use cases (e.g., request cancellation in client libraries).

**Changes**:
- Keep the implementation but document it as "extension beyond Ceph"
- Don't use it in the core Connection layer
- Expose it as an optional API for applications

### Option 3: Implement Proper Connection Teardown

**What we actually need**:

```rust
impl Connection {
    /// Close connection and discard all pending messages
    /// This matches Ceph's discard_out_queue() behavior
    pub async fn close(&mut self) {
        // Discard all sent messages (won't be retransmitted)
        self.state.clear_sent_messages();

        // Close the TCP connection
        // (TCP stream will be dropped)
    }

    /// Mark connection as down (for reconnection)
    /// This matches Ceph's mark_down() behavior
    pub async fn mark_down(&mut self) {
        // Keep sent messages for potential replay
        // Just close the TCP connection
    }
}
```

## Throttling: Still Valid

Our throttle implementation **is correct and useful**. Ceph has throttling at various layers:

1. **Dispatch throttle** (`ms_dispatch_throttle_bytes`) - We implemented this ✅
2. **Connection throttle** - Per-connection rate limiting ✅
3. **OSD throttle** - Per-OSD limits ✅

Throttling is a real Ceph feature and our implementation is compatible.

## Conclusion

### What to Do

1. **Keep throttle** - It's correct and matches Ceph ✅
2. **Remove or isolate revocation** - It's not how Ceph works ⚠️
3. **Add connection teardown** - We're missing `discard_out_queue()` ❌
4. **Document the differences** - Be clear about what's Ceph-compatible vs extensions 📝

### Immediate Action

I recommend:
1. Remove `revocation_manager` from Connection struct
2. Keep `revocation.rs` as a standalone module (might be useful later)
3. Add proper `close()` and `mark_down()` methods
4. Update documentation to clarify what's Ceph-compatible

### Why We Made This Mistake

The term "revocation" appeared in our research, but it was actually:
- `revoke_rx_buffer` - for receive buffers, not send messages
- Frame abortion flags - defined but not used
- Message requeue/discard - for reconnection, not cancellation

We over-engineered a solution for a problem that doesn't exist in Ceph's design.
