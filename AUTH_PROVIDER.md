# Async Authentication Provider System

This document describes the elegant, idiomatic Rust authentication system for Ceph connections.

## Overview

The authentication system provides a clean separation between:
- **Authentication logic** (CephX protocol, tickets, authorizers) - in `auth` crate
- **Transport layer** (msgr2 protocol, frames, TCP) - in `msgr2` crate
- **Interface** (async `AuthProvider` trait) - bridges the two

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│                    Application Layer                       │
│                                                              │
│  let mut mon_auth = MonitorAuthProvider::new(...)?;        │
│  let osd_auth = ServiceAuthProvider::from_authenticated(   │
│                     mon_auth.handler().clone());            │
└────────────────────────────────────────────────────────────┘
                            ↓
┌────────────────────────────────────────────────────────────┐
│                  AuthProvider Trait                        │
│                                                              │
│  async fn build_auth_payload(&mut self, ...) -> Result<Bytes>  │
│  async fn handle_auth_response(&mut self, ...) -> Result<(...)>│
│  async fn has_valid_ticket(&self, ...) -> bool            │
└────────────────────────────────────────────────────────────┘
        ↓                                    ↓
┌──────────────────┐              ┌──────────────────────┐
│MonitorAuthProvider│              │ServiceAuthProvider │
│  (Full CephX)     │              │  (Authorizers)       │
└──────────────────┘              └──────────────────────┘
        │                                    │
        ├─ CephXClientHandler                ├─ CephXClientHandler
        ├─ Full challenge-response           ├─ Uses tickets from monitor
        └─ Obtains service tickets           └─ Builds authorizers
```

## Usage

### Connecting to a Monitor

For monitor connections, use `MonitorAuthProvider` which performs full CephX authentication:

```rust
use auth::MonitorAuthProvider;
use msgr2::{ConnectionConfig, AuthMethod};

// Create monitor authentication provider
let mut mon_auth = MonitorAuthProvider::new("client.admin".to_string())?;

// Set credentials from base64-encoded key
mon_auth.set_secret_key_from_base64("AQCj2YBAAAAAnj1...")?;

// Or load from keyring file
// mon_auth.set_secret_key_from_keyring("/etc/ceph/ceph.client.admin.keyring")?;

// Create connection config with the auth provider
let config = ConnectionConfig::with_auth_provider(Box::new(mon_auth.clone()));

// Connect to monitor
let mut conn = msgr2::connect("127.0.0.1:3300", config).await?;
```

### Connecting to OSDs/Services

For service connections, use `ServiceAuthProvider` with credentials obtained from monitor authentication:

```rust
use auth::{MonitorAuthProvider, ServiceAuthProvider};
use msgr2::ConnectionConfig;

// 1. First authenticate with monitor
let mut mon_auth = MonitorAuthProvider::new("client.admin".to_string())?;
mon_auth.set_secret_key_from_base64("AQCj2YBAAAAAnj1...")?;

let config = ConnectionConfig::with_auth_provider(Box::new(mon_auth.clone()));
let mut mon_conn = msgr2::connect("127.0.0.1:3300", config).await?;

// At this point, mon_auth.handler() contains service tickets

// 2. Create service provider from authenticated monitor session
let osd_auth = ServiceAuthProvider::from_authenticated_handler(
    mon_auth.handler().clone()
);

// 3. Connect to OSD using authorizers
let osd_config = ConnectionConfig::with_auth_provider(Box::new(osd_auth));
let mut osd_conn = msgr2::connect("127.0.0.1:6800", osd_config).await?;
```

### Complete Example: Monitor + Multiple OSDs

```rust
use auth::{MonitorAuthProvider, ServiceAuthProvider};
use msgr2::ConnectionConfig;
use std::collections::HashMap;

async fn connect_cluster() -> Result<(), Box<dyn std::error::Error>> {
    // Step 1: Authenticate with monitor
    let mut mon_auth = MonitorAuthProvider::new("client.admin".to_string())?;
    mon_auth.set_secret_key_from_keyring("/etc/ceph/ceph.client.admin.keyring")?;

    let mon_config = ConnectionConfig::with_auth_provider(Box::new(mon_auth.clone()));
    let mon_conn = msgr2::connect("127.0.0.1:3300", mon_config).await?;

    println!("✓ Connected to monitor");

    // Step 2: Create shared service provider for all OSDs
    // The handler contains service tickets valid for all services
    let service_auth = ServiceAuthProvider::from_authenticated_handler(
        mon_auth.handler().clone()
    );

    // Step 3: Connect to multiple OSDs using the same service provider
    let mut osd_connections = HashMap::new();

    for (osd_id, addr) in &[
        (0, "127.0.0.1:6800"),
        (1, "127.0.0.1:6801"),
        (2, "127.0.0.1:6802"),
    ] {
        let osd_config = ConnectionConfig::with_auth_provider(
            Box::new(service_auth.clone())
        );
        let osd_conn = msgr2::connect(addr, osd_config).await?;
        osd_connections.insert(osd_id, osd_conn);
        println!("✓ Connected to OSD.{}", osd_id);
    }

    Ok(())
}
```

### No Authentication (Development/Testing)

For clusters with authentication disabled:

```rust
use msgr2::{ConnectionConfig, AuthMethod};

let config = ConnectionConfig::with_no_auth();
let conn = msgr2::connect("127.0.0.1:3300", config).await?;
```

## AuthProvider Trait

The `AuthProvider` trait provides a clean async interface for authentication:

```rust
#[async_trait::async_trait]
pub trait AuthProvider: Send + Sync + Debug {
    /// Build authentication payload for initial request
    async fn build_auth_payload(&mut self, global_id: u64, service_id: u32)
        -> Result<Bytes>;

    /// Handle server's authentication response
    async fn handle_auth_response(
        &mut self,
        payload: Bytes,
        global_id: u64,
        con_mode: u32,
    ) -> Result<(Option<Bytes>, Option<Bytes>)>;

    /// Check if valid ticket exists for service
    async fn has_valid_ticket(&self, service_id: u32) -> bool;

    /// Clone this provider
    fn clone_box(&self) -> Box<dyn AuthProvider>;
}
```

### Implementation Details

#### MonitorAuthProvider

- Performs full CephX challenge-response authentication
- Obtains service tickets from monitor
- Used only for monitor connections
- Returns `(session_key, connection_secret)` from `handle_auth_response()`

#### ServiceAuthProvider

- Uses tickets obtained from monitor authentication
- Builds authorizers containing encrypted nonces
- Used for OSD, MDS, MGR connections
- Always returns `(None, None)` from `handle_auth_response()` (no AUTH_DONE)

## Ticket Management

Service tickets are automatically managed by the `CephXSession`:

```rust
pub struct CephXSession {
    pub entity_name: EntityName,
    pub global_id: GlobalId,
    pub session_key: CryptoKey,
    pub ticket_handlers: HashMap<u32, TicketHandler>,
    // ...
}
```

Each `TicketHandler` stores:
- Service-specific session key
- Ticket blob (encrypted service ticket)
- Expiration times
- Secret ID for ticket renewal

Ticket validity is checked automatically:
```rust
if service_auth.has_valid_ticket(auth::entity_type::OSD).await {
    // Safe to connect
}
```

## Entity Types

Service IDs are defined in `auth::entity_type`:

```rust
pub mod entity_type {
    pub const MON: u32 = 0x01;    // Monitor
    pub const MDS: u32 = 0x02;    // Metadata Server
    pub const OSD: u32 = 0x04;    // Object Storage Daemon
    pub const CLIENT: u32 = 0x08; // Client
    pub const MGR: u32 = 0x10;    // Manager
    pub const AUTH: u32 = 0x20;   // Auth
}
```

## Best Practices

1. **Reuse ServiceAuthProvider**: Create one `ServiceAuthProvider` from monitor auth and reuse it for all service connections.

2. **Clone Efficiently**: Both providers implement `Clone` via shared handler cloning (cheap operation).

3. **Error Handling**: Check `has_valid_ticket()` before connecting to services to fail fast if tickets are missing.

4. **Keyring Security**: Store keyring files with restricted permissions (0600).

5. **Connection Pooling**: Share the same `ServiceAuthProvider` instance across connection pools.

## Protocol Flow

### Monitor Authentication (Full CephX)

```
Client                          Monitor
  │                               │
  ├─── AUTH_REQUEST ─────────────>│  (CephXAuthenticate)
  │                               │
  │<──── AUTH_REPLY_MORE ─────────┤  (CephXServerChallenge)
  │                               │
  ├─── AUTH_REQUEST_MORE ────────>│  (CephXRequestChallengeAndTickets)
  │                               │
  │<──── AUTH_DONE ───────────────┤  (session_key + service tickets)
  │                               │
```

### Service Authentication (Authorizer)

```
Client                          OSD
  │                               │
  ├─── AUTH_REQUEST ─────────────>│  (CephXAuthorizeA + encrypted CephXAuthorizeB)
  │                               │
  │<──── Connected ───────────────┤  (server validates authorizer)
  │                               │
```

The authorizer contains:
- **CephXAuthorizeA**: Global ID, service ID, ticket blob (plaintext)
- **CephXAuthorizeB**: Nonce, challenge response (encrypted with ticket's session key)

## Troubleshooting

**"Authentication failed: no valid ticket"**
- Ensure monitor authentication completed successfully before creating ServiceAuthProvider
- Check that service tickets were obtained during monitor auth

**"Invalid signature"**
- Verify the secret key matches the keyring
- Ensure entity name matches (e.g., "client.admin" not "admin")

**"Ticket expired"**
- Service tickets have limited validity
- Re-authenticate with monitor to obtain fresh tickets

## Implementation Reference

The authentication system closely follows the Linux kernel implementation:
- `~/dev/linux/net/ceph/auth_x.c` - Core CephX logic
- `~/dev/linux/net/ceph/auth_x.h` - Protocol structures
- `~/dev/ceph/src/auth/cephx/` - C++ reference implementation