# Client-Side Throttle Options in Ceph

## Summary

Yes, Ceph provides throttle options similar to `ms_dispatch_throttle_bytes` for client-OSD connections, but they are **server-side** (OSD-side) settings, not client-side.

## Relevant Options for Client Implementation

### 1. **Global Dispatch Throttle** (Receiver-Side)

```yaml
- name: ms_dispatch_throttle_bytes
  type: size
  level: advanced
  desc: Throttles total size of messages waiting to be dispatched
  default: 100_M  # 100 megabytes
```

**Scope**: This applies to ALL connections on the receiver side (client, OSD, monitor, etc.)
**Purpose**: Limits memory used by messages that have been received but not yet dispatched to handlers
**Location**: Applied at the messenger/dispatch queue level

### 2. **OSD Server-Side Client Throttles** (NOT Client-Side)

These are **OSD-side** settings that throttle incoming client requests:

```yaml
- name: osd_client_message_size_cap
  type: size
  level: advanced
  desc: maximum memory to devote to in-flight client requests
  default: 500_M  # 500 megabytes

- name: osd_client_message_cap
  type: uint
  level: advanced
  desc: maximum number of in-flight client requests
  default: 256
```

**Important**: These are **server-side** (OSD) settings, NOT client-side. The OSD uses these to throttle incoming client requests.

### 3. **Client Retry Interval** (Client-Side)

```yaml
- name: ms_client_throttle_retry_time_interval
  type: uint
  level: dev
  desc: In microseconds, the time interval between retries when throttle get_or_fail fails
  default: 5000  # 5 milliseconds
```

**Scope**: Client-side setting
**Purpose**: How long to wait before retrying when a send is throttled

## Key Insight: No Client-Side Send Throttle in Ceph

**Ceph does NOT have explicit client-side send throttle configuration options.**

The throttling in Ceph is primarily:
1. **Receiver-side**: `ms_dispatch_throttle_bytes` (applies to all receivers)
2. **Server-side policy**: OSD/Monitor sets throttles for incoming connections via `Policy::throttler_bytes` and `Policy::throttler_messages`

## Why This Matters for Our Implementation

Our implementation is **more flexible** than Ceph's:

| Feature | Ceph | Our Implementation |
|---------|------|-------------------|
| **Client-side send throttle** | ❌ No explicit option | ✅ Full support via `ThrottleConfig` |
| **Receiver-side throttle** | ✅ `ms_dispatch_throttle_bytes` | ✅ Can be added |
| **Per-connection throttle** | ✅ Via Policy (server-side) | ✅ Via `ConnectionConfig` |
| **Message count limit** | ✅ Via Policy | ✅ `max_messages_per_sec` |
| **Byte rate limit** | ✅ Via Policy | ✅ `max_bytes_per_sec` |
| **Queue depth limit** | ❌ No explicit option | ✅ `max_dispatch_queue_depth` |

## Recommended Configuration for rados-rs

Since we're implementing a **client library**, we should:

### Option 1: Match Ceph's Receiver-Side Throttle (Recommended)

```rust
impl ConnectionConfig {
    /// Create config with Ceph-compatible dispatch throttle
    pub fn with_ceph_defaults() -> Self {
        Self {
            // Match Ceph's ms_dispatch_throttle_bytes = 100MB
            // This throttles received messages before dispatch
            throttle_config: Some(ThrottleConfig::with_byte_rate(100 * 1024 * 1024)),
            ..Default::default()
        }
    }
}
```

### Option 2: Add Client-Side Send Throttle (Optional)

```rust
impl ConnectionConfig {
    /// Create config with client-side send throttle
    /// This is MORE than what Ceph provides
    pub fn with_send_throttle(max_bytes_per_sec: u64) -> Self {
        Self {
            throttle_config: Some(ThrottleConfig::with_byte_rate(max_bytes_per_sec)),
            ..Default::default()
        }
    }
}
```

### Option 3: No Throttle (Default)

```rust
impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            // No throttle by default (like Ceph client)
            throttle_config: None,
            ..Default::default()
        }
    }
}
```

## Integration Recommendation

For a **client library** like rados-rs:

1. **Default**: No throttle (matches Ceph client behavior)
2. **Optional**: Allow users to configure throttle via `ConnectionConfig`
3. **Future**: Add receiver-side throttle when implementing MonClient/OSDClient

```rust
// Example usage
let config = ConnectionConfig {
    // Optional: Add throttle if user wants it
    throttle_config: Some(ThrottleConfig::with_limits(
        100,              // 100 messages/sec
        10 * 1024 * 1024, // 10 MB/sec
        50,               // 50 in-flight messages
    )),
    ..Default::default()
};

let mut conn = Connection::connect(addr, config).await?;
```

## Conclusion

**For client-side implementation:**
- ✅ Our throttle implementation is ready to use
- ✅ More flexible than Ceph's client (which has no explicit send throttle)
- ✅ Can be configured per-connection
- ✅ Default to no throttle (matches Ceph client behavior)
- ✅ Users can opt-in to throttling if needed

**We don't need to worry about OSD-side throttle options** - those are server-side concerns and not relevant for our client library implementation.
