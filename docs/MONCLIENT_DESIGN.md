# MonClient Design for rados-rs

## Overview

The MonClient is responsible for managing connections to Ceph monitor daemons and handling:
- Monitor discovery and connection management
- Authentication with monitors
- Subscription to cluster maps (monmap, osdmap, mgrmap, etc.)
- Sending commands to monitors
- Version queries for cluster maps

## Architecture

### Core Components

```
┌─────────────────────────────────────────────────────────────┐
│                         MonClient                            │
├─────────────────────────────────────────────────────────────┤
│  - Connection Management (hunting, active connection)        │
│  - Authentication State                                      │
│  - Subscription Management (MonSub)                          │
│  - Command Tracking                                          │
│  - Version Request Tracking                                  │
└─────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ MonConnection│    │    MonSub    │    │   MonMap     │
│              │    │              │    │              │
│ - msgr2 conn │    │ - sub_new    │    │ - monitors   │
│ - auth state │    │ - sub_sent   │    │ - fsid       │
│ - global_id  │    │ - timers     │    │ - epoch      │
└──────────────┘    └──────────────┘    └──────────────┘
```

## Module Structure

```
crates/monclient/
├── Cargo.toml
├── src/
│   ├── lib.rs              # Public API
│   ├── client.rs           # MonClient main implementation
│   ├── connection.rs       # MonConnection wrapper
│   ├── subscription.rs     # MonSub implementation
│   ├── monmap.rs          # MonMap structure
│   ├── messages.rs        # Monitor message types
│   ├── commands.rs        # Command tracking and execution
│   ├── error.rs           # Error types
│   └── types.rs           # Common types
└── tests/
    └── integration_test.rs
```

## Key Data Structures

### 1. MonClient

```rust
pub struct MonClient {
    // Configuration
    config: MonClientConfig,
    entity_name: EntityName,

    // State
    state: Arc<Mutex<MonClientState>>,

    // Async runtime
    runtime_handle: tokio::runtime::Handle,
}

struct MonClientState {
    // Monitor map
    monmap: MonMap,

    // Connection management
    active_con: Option<MonConnection>,
    pending_cons: HashMap<EntityAddr, MonConnection>,
    tried: HashSet<usize>,  // monitor ranks we've tried

    // Authentication
    authenticated: bool,
    global_id: u64,
    auth_handler: Option<Box<dyn AuthClientHandler>>,

    // Subscriptions
    subscriptions: MonSub,

    // Command tracking
    commands: HashMap<u64, MonCommand>,
    last_command_tid: u64,

    // Version requests
    version_requests: HashMap<u64, VersionRequest>,
    last_version_req_id: u64,

    // State flags
    hunting: bool,
    want_monmap: bool,
    initialized: bool,
    stopping: bool,
}
```

### 2. MonConnection

```rust
pub struct MonConnection {
    // Underlying msgr2 connection
    connection: msgr2::Connection,

    // Authentication state
    auth_state: AuthState,
    global_id: u64,

    // Monitor info
    mon_rank: usize,
    mon_addr: EntityAddr,
}

enum AuthState {
    None,
    Authenticating,
    Authenticated,
}
```

### 3. MonSub (Subscription Manager)

```rust
pub struct MonSub {
    // Pending subscriptions to send
    sub_new: HashMap<String, SubscribeItem>,

    // Sent subscriptions waiting for ack
    sub_sent: HashMap<String, SubscribeItem>,

    // Renewal timing
    renew_sent: Option<Instant>,
    renew_after: Option<Instant>,
}

pub struct SubscribeItem {
    pub start: u64,      // version to start from
    pub flags: u8,       // CEPH_SUBSCRIBE_ONETIME, etc.
}

// Subscription flags
pub const CEPH_SUBSCRIBE_ONETIME: u8 = 1;
```

### 4. MonMap

```rust
pub struct MonMap {
    pub fsid: Uuid,
    pub epoch: u32,
    pub created: SystemTime,
    pub modified: SystemTime,
    pub monitors: Vec<MonInfo>,
}

pub struct MonInfo {
    pub name: String,
    pub rank: usize,
    pub addrs: EntityAddrVec,
    pub priority: i32,
    pub weight: u16,
}
```

### 5. Message Types

```rust
// Message type constants (add to msgr2/src/message.rs)
pub const CEPH_MSG_MON_SUBSCRIBE: u16 = 0x000f;
pub const CEPH_MSG_MON_SUBSCRIBE_ACK: u16 = 0x0010;
pub const CEPH_MSG_MON_GET_VERSION: u16 = 0x0052;
pub const CEPH_MSG_MON_GET_VERSION_REPLY: u16 = 0x0053;

// Monitor messages
pub struct MMonSubscribe {
    pub what: HashMap<String, SubscribeItem>,
}

pub struct MMonSubscribeAck {
    pub interval: u32,
    pub fsid: Uuid,
}

pub struct MMonGetVersion {
    pub tid: u64,
    pub what: String,  // "osdmap", "mgrmap", etc.
}

pub struct MMonGetVersionReply {
    pub tid: u64,
    pub version: u64,
    pub oldest_version: u64,
}

pub struct MMonMap {
    pub monmap_bl: Bytes,  // encoded MonMap
}
```

## Core Functionality

### 1. Initialization and Connection

```rust
impl MonClient {
    /// Create a new MonClient
    pub async fn new(config: MonClientConfig) -> Result<Self> {
        // Build initial monmap from config
        // Initialize state
        // Return client (not yet connected)
    }

    /// Initialize and connect to monitors
    pub async fn init(&self) -> Result<()> {
        // Build initial monmap
        // Start hunting for monitors
        // Authenticate
        // Wait for initial monmap
    }

    /// Start hunting for an available monitor
    async fn start_hunting(&self) -> Result<()> {
        // Try connecting to multiple monitors in parallel
        // First successful auth wins
        // Others are closed
    }

    /// Establish connection to a specific monitor
    async fn connect_to_mon(&self, rank: usize) -> Result<MonConnection> {
        // Create msgr2 connection
        // Perform authentication
        // Send initial subscriptions
    }
}
```

### 2. Subscription Management

```rust
impl MonClient {
    /// Subscribe to a cluster map
    pub fn subscribe(&self, what: &str, start: u64, flags: u8) -> Result<()> {
        // Add to sub_new
        // Trigger send if connected
    }

    /// Unsubscribe from a cluster map
    pub fn unsubscribe(&self, what: &str) -> Result<()> {
        // Remove from subscriptions
    }

    /// Wait for a specific map version
    pub async fn wait_for_map(&self, what: &str, version: u64) -> Result<()> {
        // Subscribe if needed
        // Wait for version via channel
    }

    /// Internal: send subscription message
    async fn send_subscriptions(&self) -> Result<()> {
        // Build MMonSubscribe from sub_new
        // Send message
        // Move sub_new to sub_sent
    }

    /// Handle subscription ack
    fn handle_subscribe_ack(&self, msg: MMonSubscribeAck) -> Result<()> {
        // Update renewal timer
        // Notify waiters
    }
}
```

### 3. Command Execution

```rust
impl MonClient {
    /// Send a command to the monitor cluster
    pub async fn send_command(
        &self,
        cmd: Vec<String>,
        inbl: Bytes,
    ) -> Result<CommandResult> {
        // Allocate command ID
        // Create command tracker
        // Send MMonCommand
        // Wait for MMonCommandAck
    }

    /// Send a command to a specific monitor
    pub async fn send_command_to_mon(
        &self,
        mon_rank: usize,
        cmd: Vec<String>,
        inbl: Bytes,
    ) -> Result<CommandResult> {
        // Similar but target specific monitor
    }
}

pub struct CommandResult {
    pub retval: i32,
    pub outs: String,
    pub outbl: Bytes,
}

struct MonCommand {
    tid: u64,
    cmd: Vec<String>,
    inbl: Bytes,
    result_tx: oneshot::Sender<CommandResult>,
    timeout: Option<tokio::time::Instant>,
}
```

### 4. Version Queries

```rust
impl MonClient {
    /// Get the latest version of a cluster map
    pub async fn get_version(&self, what: &str) -> Result<(u64, u64)> {
        // Allocate request ID
        // Send MMonGetVersion
        // Wait for MMonGetVersionReply
        // Return (newest, oldest)
    }
}

struct VersionRequest {
    req_id: u64,
    what: String,
    result_tx: oneshot::Sender<(u64, u64)>,
}
```

### 5. Message Handling

```rust
impl MonClient {
    /// Main message dispatch loop
    async fn message_loop(&self) -> Result<()> {
        loop {
            let msg = self.receive_message().await?;

            match msg.msg_type() {
                CEPH_MSG_MON_MAP => self.handle_monmap(msg)?,
                CEPH_MSG_MON_SUBSCRIBE_ACK => self.handle_subscribe_ack(msg)?,
                CEPH_MSG_MON_GET_VERSION_REPLY => self.handle_version_reply(msg)?,
                CEPH_MSG_MON_COMMAND_ACK => self.handle_command_ack(msg)?,
                CEPH_MSG_AUTH_REPLY => self.handle_auth_reply(msg)?,
                _ => {
                    // Unknown message, log and ignore
                }
            }
        }
    }

    /// Handle MonMap message
    fn handle_monmap(&self, msg: Message) -> Result<()> {
        // Decode MonMap from message
        // Update internal monmap
        // Notify waiters
        // Check if we need to reconnect (e.g., v2 address available)
    }
}
```

## Configuration

```rust
pub struct MonClientConfig {
    /// Entity name (e.g., "client.admin")
    pub entity_name: EntityName,

    /// Initial monitor addresses
    pub mon_addrs: Vec<String>,

    /// Keyring for authentication
    pub keyring: Keyring,

    /// Connection timeout
    pub connect_timeout: Duration,

    /// Command timeout
    pub command_timeout: Duration,

    /// Hunt interval (time between connection attempts)
    pub hunt_interval: Duration,
}
```

## Error Handling

```rust
#[derive(Debug, thiserror::Error)]
pub enum MonClientError {
    #[error("Not connected to any monitor")]
    NotConnected,

    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("Command failed: {0}")]
    CommandFailed(String),

    #[error("Timeout waiting for response")]
    Timeout,

    #[error("Monitor unavailable")]
    MonitorUnavailable,

    #[error("Invalid monmap: {0}")]
    InvalidMonMap(String),

    #[error("Message error: {0}")]
    MessageError(#[from] msgr2::Error),

    #[error("Encoding error: {0}")]
    EncodingError(#[from] denc::Error),
}
```

## Usage Example

```rust
use monclient::{MonClient, MonClientConfig};

#[tokio::main]
async fn main() -> Result<()> {
    // Create configuration
    let config = MonClientConfig {
        entity_name: EntityName::client("admin"),
        mon_addrs: vec![
            "v2:127.0.0.1:3300".to_string(),
            "v2:127.0.0.1:3301".to_string(),
        ],
        keyring: Keyring::from_file("/etc/ceph/ceph.client.admin.keyring")?,
        connect_timeout: Duration::from_secs(30),
        command_timeout: Duration::from_secs(60),
        hunt_interval: Duration::from_secs(3),
    };

    // Create and initialize client
    let client = MonClient::new(config).await?;
    client.init().await?;

    // Subscribe to osdmap
    client.subscribe("osdmap", 0, 0).await?;

    // Get current osdmap version
    let (newest, oldest) = client.get_version("osdmap").await?;
    println!("OSDMap version: {} (oldest: {})", newest, oldest);

    // Send a command
    let result = client.send_command(
        vec!["osd".to_string(), "tree".to_string()],
        Bytes::new(),
    ).await?;

    println!("Command output: {}", result.outs);

    // Wait for specific osdmap version
    client.wait_for_map("osdmap", newest + 1).await?;

    Ok(())
}
```

## Implementation Phases

### Phase 1: Basic Structure
- [ ] Create monclient crate
- [ ] Define core data structures
- [ ] Implement MonMap encoding/decoding
- [ ] Basic MonClient skeleton

### Phase 2: Connection Management
- [ ] MonConnection wrapper around msgr2::Connection
- [ ] Monitor hunting logic
- [ ] Connection state management
- [ ] Reconnection on failure

### Phase 3: Subscription System
- [ ] MonSub implementation
- [ ] MMonSubscribe message encoding
- [ ] MMonSubscribeAck handling
- [ ] Subscription renewal logic

### Phase 4: Commands and Queries
- [ ] Command tracking
- [ ] MMonCommand/MMonCommandAck messages
- [ ] Version query implementation
- [ ] Timeout handling

### Phase 5: Integration
- [ ] Integration tests with local cluster
- [ ] Error handling refinement
- [ ] Documentation
- [ ] Examples

## Key Differences from C++ Implementation

1. **Async/Await**: Uses Tokio instead of boost::asio
2. **Type Safety**: Rust's type system prevents many runtime errors
3. **No Dispatcher Pattern**: Direct async message handling
4. **Simplified Locking**: Arc<Mutex<>> instead of complex lock hierarchies
5. **Channel-based Notifications**: oneshot channels for command/query results
6. **No Raw Pointers**: All memory safety guaranteed by Rust

## Dependencies

```toml
[dependencies]
tokio = { workspace = true }
bytes = { workspace = true }
async-trait = { workspace = true }
tracing = { workspace = true }
thiserror = { workspace = true }
anyhow = { workspace = true }
uuid = { version = "1.0", features = ["v4"] }

# Internal dependencies
msgr2 = { path = "../msgr2" }
auth = { path = "../auth" }
denc = { path = "../denc" }
```

## Testing Strategy

1. **Unit Tests**: Test individual components (MonSub, MonMap parsing, etc.)
2. **Integration Tests**: Test against local Ceph cluster
3. **Mock Tests**: Test connection logic with mock monitors
4. **Stress Tests**: Test reconnection, command queuing, etc.

## Future Enhancements

1. **Connection Pooling**: Maintain connections to multiple monitors
2. **Load Balancing**: Distribute read operations across monitors
3. **Metrics**: Expose connection stats, command latency, etc.
4. **Config Updates**: Handle dynamic config updates from monitors
5. **Log Forwarding**: Implement LogClient for forwarding logs to monitors
