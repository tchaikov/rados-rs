# Ceph Throttling Integration Analysis

## How Ceph Implements Throttling

Based on analysis of the Ceph source code, here's how the official implementation integrates throttling:

### 1. Throttle Architecture

Ceph uses a multi-level throttling system:

#### **Dispatch Queue Throttle** (`DispatchQueue::dispatch_throttler`)
- **Location**: `src/msg/DispatchQueue.h`
- **Purpose**: Limits total size of messages waiting to be dispatched
- **Configuration**: `ms_dispatch_throttle_bytes` (default: 100MB)
- **Scope**: Per-messenger instance (shared across all connections)

```cpp
class DispatchQueue {
  Throttle dispatch_throttler;  // Throttles message dispatch queue

  DispatchQueue(CephContext *cct, Messenger *msgr, string &name)
    : dispatch_throttler(cct,
                        std::string("msgr_dispatch_throttler-") + name,
                        cct->_conf->ms_dispatch_throttle_bytes)
  {}
};
```

#### **Policy Throttle** (`Connection::policy.throttler_bytes`)
- **Location**: `src/msg/async/AsyncConnection.h`
- **Purpose**: Per-connection or per-policy throttling
- **Configuration**: Set by connection policy (varies by entity type)
- **Scope**: Per-connection or per-policy group

```cpp
struct Policy {
  Throttle *throttler_bytes;  // Optional per-connection throttle
  Throttle *throttler_messages;
};
```

### 2. Throttle Points in ProtocolV2

#### **Receiving Messages** (in `ProtocolV2::handle_message()`)

```cpp
// 1. Check policy throttle first (if configured)
if (connection->policy.throttler_bytes) {
  if (!connection->policy.throttler_bytes->get_or_fail(cur_msg_size)) {
    // Throttled! Register timer event to retry later
    connection->register_time_events.insert(
      connection->center->create_time_event(
        cct->_conf->ms_client_throttle_retry_time_interval,
        connection->wakeup_handler));
    return nullptr;  // Stop processing, will retry later
  }
}

// 2. Check dispatch queue throttle
if (!connection->dispatch_queue->dispatch_throttler.get_or_fail(cur_msg_size)) {
  // Dispatch queue full, wait for space
  ldout(cct, 1) << "dispatch queue throttle full, waiting" << dendl;
  return nullptr;
}

// 3. Message accepted, set throttle size for later release
message->set_dispatch_throttle_size(cur_msg_size);
```

#### **Releasing Throttle** (in `ProtocolV2::throttle_release()`)

```cpp
void ProtocolV2::throttle_release() {
  // Release policy throttle
  if (connection->policy.throttler_bytes) {
    connection->policy.throttler_bytes->put(cur_msg_size);
  }

  // Release dispatch queue throttle
  if (state > THROTTLE_DISPATCH_QUEUE && state <= THROTTLE_DONE) {
    connection->dispatch_queue->dispatch_throttle_release(cur_msg_size);
  }
}
```

### 3. Configuration Options

From `src/common/options/global.yaml.in`:

```yaml
- name: ms_dispatch_throttle_bytes
  type: size
  level: advanced
  desc: Throttles total size of messages waiting to be dispatched
  default: 100_M  # 100 megabytes
```

### 4. MonClient and OSD Client Integration

#### **MonClient**
- Uses the messenger's dispatch queue throttle
- No special MonClient-specific throttling
- Inherits throttle behavior from the underlying messenger

#### **OSD Client**
- Uses policy-based throttling via `Connection::policy`
- Different policies for different connection types:
  - Client connections
  - OSD-to-OSD connections
  - Monitor connections

### 5. Key Differences from Our Implementation

| Aspect | Ceph Implementation | Our Implementation |
|--------|-------------------|-------------------|
| **Scope** | Global (dispatch queue) + per-connection (policy) | Per-connection |
| **Granularity** | Bytes only (message size) | Bytes + message count + queue depth |
| **Blocking** | Event-driven retry with timers | Async wait with tokio::sleep |
| **Release** | Explicit release after dispatch | Automatic with ACK tracking |
| **Configuration** | Global config + policy | Per-connection config |

### 6. Integration Recommendations for rados-rs

Based on Ceph's design, here's how we should integrate throttling:

#### **Option 1: Dispatch Queue Throttle (Recommended)**

Add a global dispatch throttle at the messenger level:

```rust
// In a future Messenger struct
pub struct Messenger {
    dispatch_throttle: MessageThrottle,
    connections: HashMap<ConnectionId, Connection>,
}

impl Messenger {
    pub fn new(config: MessengerConfig) -> Self {
        let throttle_config = ThrottleConfig::with_byte_rate(
            config.dispatch_throttle_bytes
        );

        Self {
            dispatch_throttle: MessageThrottle::new(throttle_config),
            connections: HashMap::new(),
        }
    }
}
```

#### **Option 2: Per-Connection Policy Throttle**

Add throttle to ConnectionConfig:

```rust
pub struct ConnectionConfig {
    // ... existing fields ...

    /// Optional throttle for this connection
    /// If None, no throttling is applied
    pub throttle_config: Option<ThrottleConfig>,
}

impl Connection {
    pub async fn recv_message(&mut self) -> Result<Message> {
        let frame = self.state.recv_frame().await?;

        // Check throttle before processing
        if let Some(throttle) = &self.throttle {
            let msg_size = frame.total_size();
            throttle.wait_for_send(msg_size).await;
        }

        // Process message...
    }
}
```

#### **Option 3: Hybrid Approach (Most Flexible)**

Combine both global and per-connection throttling:

```rust
pub struct Connection {
    // Per-connection throttle (optional)
    local_throttle: Option<MessageThrottle>,

    // Reference to global dispatch throttle (optional)
    dispatch_throttle: Option<Arc<MessageThrottle>>,
}

impl Connection {
    pub async fn recv_message(&mut self) -> Result<Message> {
        let msg_size = self.estimate_message_size();

        // Check local throttle first
        if let Some(throttle) = &self.local_throttle {
            throttle.wait_for_send(msg_size).await;
        }

        // Then check global dispatch throttle
        if let Some(throttle) = &self.dispatch_throttle {
            throttle.wait_for_send(msg_size).await;
        }

        // Receive and process message...
        let msg = self.recv_message_inner().await?;

        // Record send for throttle tracking
        if let Some(throttle) = &self.local_throttle {
            throttle.record_send(msg.total_size()).await;
        }
        if let Some(throttle) = &self.dispatch_throttle {
            throttle.record_send(msg.total_size()).await;
        }

        Ok(msg)
    }
}
```

### 7. Configuration Mapping

Map Ceph's configuration to our implementation:

```rust
impl ConnectionConfig {
    /// Create config matching Ceph's default throttle settings
    pub fn with_ceph_defaults() -> Self {
        Self {
            // Match Ceph's ms_dispatch_throttle_bytes = 100MB
            throttle_config: Some(ThrottleConfig::with_byte_rate(100 * 1024 * 1024)),
            ..Default::default()
        }
    }
}
```

### 8. MonClient and OSDClient Integration

For MonClient and OSDClient, we should:

1. **MonClient**: Use a shared dispatch throttle across all monitor connections
2. **OSDClient**: Use per-OSD throttles with configurable limits

```rust
// In monclient
pub struct MonClient {
    // Shared throttle for all monitor connections
    dispatch_throttle: Arc<MessageThrottle>,
    connections: Vec<Connection>,
}

// In osdclient
pub struct OSDClient {
    // Per-OSD throttles
    osd_throttles: HashMap<OsdId, MessageThrottle>,
}
```

### 9. Summary

**Ceph's throttling is primarily receiver-side:**
- Throttles incoming messages before dispatch
- Uses two levels: global dispatch queue + per-connection policy
- Releases throttle after message is dispatched to handler
- Configuration: `ms_dispatch_throttle_bytes` (100MB default)

**Our implementation is more flexible:**
- Can throttle both sending and receiving
- Supports message count, byte rate, and queue depth limits
- Uses async/await instead of event timers
- Ready to integrate at connection or messenger level

**Recommended integration:**
- Add dispatch throttle at messenger/client level (like Ceph)
- Keep per-connection throttle as optional feature
- Use our existing implementation with Ceph-compatible defaults
