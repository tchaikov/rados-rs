# Priority-Based Message Queueing

## Overview

The `msgr2` module implements a priority-based message queue system that ensures high-priority messages (such as heartbeats) are sent before lower-priority bulk data transfers. This matches the behavior of Ceph's C++ `ProtocolV2` implementation.

## Architecture

### MessagePriority Enum

The `MessagePriority` enum defines three priority levels:

- **High (2)**: For time-sensitive messages like heartbeats, pings, and keepalives
- **Normal (1)**: For standard operations (default priority)
- **Low (0)**: For bulk data transfers that can be delayed

### PriorityQueue Implementation

The `PriorityQueue` struct maintains three separate `VecDeque` queues, one for each priority level:

```rust
pub struct PriorityQueue {
    high: VecDeque<Message>,
    normal: VecDeque<Message>,
    low: VecDeque<Message>,
}
```

When messages are pushed to the queue, they are automatically routed to the appropriate sub-queue based on their priority. When popping messages, the implementation checks queues in priority order (high → normal → low), ensuring higher-priority messages are always sent first.

### FIFO Within Priority

Within each priority level, messages maintain FIFO (First-In-First-Out) order. This ensures:
- Predictable message ordering within the same priority
- No starvation of messages at the same priority level
- Proper message replay on reconnection

## Usage

### Setting Message Priority

Messages can be created with explicit priority using the builder pattern:

```rust
use msgr2::message::{Message, MessagePriority};
use bytes::Bytes;

// Create a high-priority message
let heartbeat = Message::ping(); // Automatically set to High priority

// Create a message with explicit priority
let bulk_msg = Message::new(msg_type, data)
    .with_priority(MessagePriority::Normal.to_u16());
```

### Priority Assignment Guidelines

| Message Type | Priority | Rationale |
|-------------|----------|-----------|
| PING / PING_ACK | High | Keep-alive messages must not be delayed |
| Heartbeats | High | Critical for connection health monitoring |
| Standard operations | Normal | Default priority for most operations |
| Bulk data transfers | Low | Can be delayed without affecting latency-sensitive operations |

### Automatic Priority Handling

The following message types automatically use high priority:

- `Message::ping()` - Ping requests
- `Message::ping_ack()` - Ping responses

OSD operations can specify priority explicitly in the operation request, which is passed through to the message header.

## Implementation Details

### Connection State

The `ConnectionState` struct uses `PriorityQueue` to store sent messages awaiting acknowledgment:

```rust
struct SessionState {
    sent_messages: PriorityQueue,
    // ... other fields
}
```

This ensures that:
1. Messages are replayed in priority order after reconnection
2. High-priority messages are not blocked by bulk data in the queue
3. Message acknowledgment works correctly with priority ordering

### Message Replay

When a connection is re-established after a failure, messages are replayed in priority order. This means:
- High-priority messages (e.g., heartbeats) that weren't acknowledged are sent first
- Normal and low-priority messages follow in their original order within each priority level
- The remote end receives messages in the correct priority order

## Benefits

1. **Improved Responsiveness**: Heartbeats and time-sensitive operations are not delayed by bulk data transfers
2. **Better Connection Health**: Keep-alive messages are processed promptly, preventing spurious connection failures
3. **Predictable Performance**: Applications can control message priority to meet their latency requirements
4. **Ceph Compatibility**: Matches the behavior of Ceph's C++ implementation

## Testing

The priority queue implementation includes comprehensive tests:

- **Unit tests** in `crates/msgr2/src/protocol.rs`: Test queue operations and ordering
- **Integration tests** in `crates/msgr2/tests/priority_queue_integration.rs`: Test realistic scenarios with mixed priorities
- **Message priority tests** in `crates/msgr2/src/message.rs`: Verify priority assignment and conversion

Run tests with:
```bash
cargo test -p msgr2 priority
```

## Performance Considerations

The priority queue implementation has minimal overhead:

- **Push operation**: O(1) - Messages are appended to the appropriate queue
- **Pop operation**: O(1) amortized - Checks three queues in priority order
- **Memory**: Linear in the number of queued messages, same as a single `VecDeque`
- **No locks**: The queue is single-threaded within the connection state

The implementation is designed for the common case where most messages have the same priority, making it efficient for typical workloads.

## Migration Notes

The priority queue system is a drop-in replacement for the previous FIFO queue implementation. Existing code continues to work without changes because:

- Messages without explicit priority are assigned Normal priority (value 1)
- The queue interface (`push_back`, `pop_front`, `clear`, etc.) remains the same
- Replay logic automatically uses the priority queue

Applications that want to take advantage of priority-based queueing can simply:
1. Use `.with_priority()` to set explicit priorities on messages
2. Use pre-defined high-priority messages like `Message::ping()`
3. Continue using default priorities for normal operations
