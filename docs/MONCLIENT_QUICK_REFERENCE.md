# MonClient Quick Reference

## Key Concepts

### Monitor Client Responsibilities
1. **Discovery**: Find and connect to available monitors
2. **Authentication**: Authenticate using CephX
3. **Subscriptions**: Subscribe to cluster maps (osdmap, monmap, mgrmap, etc.)
4. **Commands**: Execute admin commands on the monitor cluster
5. **Version Queries**: Query current versions of cluster maps

### Connection States

```
┌─────────┐    init()     ┌──────────┐   auth success   ┌─────────────┐
│  Idle   │──────────────>│ Hunting  │─────────────────>│  Connected  │
└─────────┘               └──────────┘                  └─────────────┘
                               │ ^                            │
                               │ │ reconnect                  │
                               │ │                            │
                               └─┘<───────────────────────────┘
                                      connection lost
```

## Core APIs

### Initialization

```rust
// Create client
let config = MonClientConfig {
    entity_name: EntityName::client("admin"),
    mon_addrs: vec!["v2:127.0.0.1:3300".to_string()],
    keyring: Keyring::from_file("/etc/ceph/ceph.client.admin.keyring")?,
    connect_timeout: Duration::from_secs(30),
    command_timeout: Duration::from_secs(60),
    hunt_interval: Duration::from_secs(3),
};

let client = MonClient::new(config).await?;
client.init().await?;
```

### Subscriptions

```rust
// Subscribe to a map (persistent)
client.subscribe("osdmap", 0, 0).await?;

// Subscribe for one update only
client.subscribe("osdmap", 0, CEPH_SUBSCRIBE_ONETIME).await?;

// Unsubscribe
client.unsubscribe("osdmap").await?;

// Wait for specific version
client.wait_for_map("osdmap", 100).await?;
```

### Commands

```rust
// Send command to monitor cluster
let result = client.send_command(
    vec!["osd".to_string(), "tree".to_string()],
    Bytes::new(),
).await?;

println!("Return code: {}", result.retval);
println!("Output: {}", result.outs);
println!("Data: {:?}", result.outbl);

// Send to specific monitor
let result = client.send_command_to_mon(
    0,  // monitor rank
    vec!["mon".to_string(), "stat".to_string()],
    Bytes::new(),
).await?;
```

### Version Queries

```rust
// Get current and oldest version
let (newest, oldest) = client.get_version("osdmap").await?;
println!("Current: {}, Oldest: {}", newest, oldest);
```

### MonMap Access

```rust
// Get monitor count
let count = client.get_mon_count();

// Get monitor addresses
let addrs = client.get_mon_addrs(0)?;

// Get FSID
let fsid = client.get_fsid();

// Get current monmap epoch
let epoch = client.get_monmap_epoch();
```

## Message Types Reference

### Monitor Messages

| Message Type | Value | Direction | Purpose |
|-------------|-------|-----------|---------|
| `CEPH_MSG_MON_MAP` | 0x0042 | Mon → Client | MonMap update |
| `CEPH_MSG_MON_SUBSCRIBE` | 0x000f | Client → Mon | Subscribe to maps |
| `CEPH_MSG_MON_SUBSCRIBE_ACK` | 0x0010 | Mon → Client | Subscription acknowledged |
| `CEPH_MSG_MON_GET_VERSION` | 0x0052 | Client → Mon | Query map version |
| `CEPH_MSG_MON_GET_VERSION_REPLY` | 0x0053 | Mon → Client | Version response |
| `CEPH_MSG_MON_COMMAND` | 0x0050 | Client → Mon | Execute command |
| `CEPH_MSG_MON_COMMAND_ACK` | 0x0051 | Mon → Client | Command result |

### Subscription Flags

| Flag | Value | Description |
|------|-------|-------------|
| `CEPH_SUBSCRIBE_ONETIME` | 0x01 | Unsubscribe after one update |

## Common Subscription Targets

| Target | Description |
|--------|-------------|
| `monmap` | Monitor map |
| `osdmap` | OSD map |
| `mgrmap` | Manager map |
| `mdsmap` | MDS map |
| `fsmap` | Filesystem map |
| `config` | Configuration updates |

## Error Handling

```rust
use monclient::MonClientError;

match client.send_command(cmd, inbl).await {
    Ok(result) => {
        if result.retval == 0 {
            println!("Success: {}", result.outs);
        } else {
            eprintln!("Command failed: {}", result.outs);
        }
    }
    Err(MonClientError::NotConnected) => {
        eprintln!("Not connected to monitor");
        // Retry or reconnect
    }
    Err(MonClientError::Timeout) => {
        eprintln!("Command timed out");
        // Retry with longer timeout
    }
    Err(MonClientError::AuthenticationFailed(msg)) => {
        eprintln!("Auth failed: {}", msg);
        // Check keyring
    }
    Err(e) => {
        eprintln!("Error: {}", e);
    }
}
```

## State Management

### MonSub State Machine

```
┌─────────┐  want()   ┌─────────┐  send()   ┌──────────┐
│  Empty  │──────────>│ sub_new │──────────>│ sub_sent │
└─────────┘           └─────────┘           └──────────┘
                           │                      │
                           │                      │ got() with ONETIME
                           │                      v
                           │                  ┌─────────┐
                           └─────────────────>│ Removed │
                                 unwant()     └─────────┘
```

### Connection Hunting

```rust
// MonClient tries monitors in weighted random order
// First successful authentication wins
// Failed attempts are tracked to avoid retry storms

Hunting Process:
1. Select untried monitors (weighted random)
2. Connect in parallel (up to N concurrent)
3. First successful auth → active connection
4. Close other pending connections
5. Send pending subscriptions
6. Start message loop
```

## Configuration Options

```rust
pub struct MonClientConfig {
    // Required
    pub entity_name: EntityName,
    pub mon_addrs: Vec<String>,
    pub keyring: Keyring,

    // Optional (with defaults)
    pub connect_timeout: Duration,      // Default: 30s
    pub command_timeout: Duration,      // Default: 60s
    pub hunt_interval: Duration,        // Default: 3s
    pub max_hunt_attempts: usize,       // Default: 10
    pub subscription_renewal: Duration, // Default: 300s
}
```

## Debugging

### Enable Tracing

```rust
use tracing_subscriber;

tracing_subscriber::fmt()
    .with_max_level(tracing::Level::DEBUG)
    .with_target(true)
    .init();

// Now MonClient will emit detailed logs
```

### Common Issues

1. **Connection Fails**
   - Check monitor addresses are correct
   - Verify network connectivity
   - Check firewall rules

2. **Authentication Fails**
   - Verify keyring path and permissions
   - Check entity name matches keyring
   - Ensure monitor has the key

3. **Subscriptions Not Working**
   - Verify connection is established
   - Check subscription was sent (enable debug logs)
   - Ensure monitor supports the map type

4. **Commands Timeout**
   - Increase command_timeout
   - Check monitor is responsive
   - Verify command syntax

## Performance Considerations

### Connection Pooling
- MonClient maintains one active connection
- Future: support multiple connections for load balancing

### Message Batching
- Subscriptions are batched when possible
- Commands are sent immediately

### Reconnection Strategy
- Exponential backoff on repeated failures
- Weighted random selection of monitors
- Avoid thundering herd on monitor restart

## Integration with RADOS Client

```rust
// Typical usage in RADOS client
pub struct RadosClient {
    mon_client: Arc<MonClient>,
    osd_client: OsdClient,
    // ...
}

impl RadosClient {
    pub async fn new(config: RadosConfig) -> Result<Self> {
        // Create monitor client
        let mon_client = Arc::new(MonClient::new(config.mon_config).await?);
        mon_client.init().await?;

        // Subscribe to required maps
        mon_client.subscribe("osdmap", 0, 0).await?;
        mon_client.subscribe("mgrmap", 0, 0).await?;

        // Wait for initial maps
        let (osdmap_ver, _) = mon_client.get_version("osdmap").await?;
        mon_client.wait_for_map("osdmap", osdmap_ver).await?;

        // Create OSD client with osdmap
        let osd_client = OsdClient::new(/* ... */);

        Ok(Self {
            mon_client,
            osd_client,
        })
    }
}
```

## Testing

### Unit Tests
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_subscription_state() {
        let mut sub = MonSub::new();
        assert!(!sub.have_new());

        sub.want("osdmap", 0, 0);
        assert!(sub.have_new());

        sub.renewed();
        assert!(!sub.have_new());
    }
}
```

### Integration Tests
```rust
#[tokio::test]
async fn test_connect_to_monitor() {
    let config = test_config();
    let client = MonClient::new(config).await.unwrap();
    client.init().await.unwrap();

    assert!(client.is_connected());
    assert!(client.is_authenticated());
}
```

## References

- C++ Implementation: `~/dev/ceph/src/mon/MonClient.{h,cc}`
- MonSub: `~/dev/ceph/src/mon/MonSub.{h,cc}`
- Protocol: `~/dev/ceph/doc/dev/msgr2.rst`
- Message Types: `~/dev/ceph/src/include/msgr.h`
