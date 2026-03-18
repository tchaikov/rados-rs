# msgr2 - Ceph Messenger v2 Protocol Implementation

This crate implements the Ceph messenger v2 protocol in Rust.

## State machine overview

The implementation has two closely related state-machine layers:

- `protocol.rs` drives the high-level async handshake as a sequence of pure phases.
- `state_machine.rs` contains the lower-level protocol states that own the wire-level transitions and side effects.

The diagrams below mirror the current implementation closely enough to use as a navigation aid when reading the code.

### Handshake phases driven by `protocol.rs`

```mermaid
stateDiagram-v2
    [*] --> Hello
    Hello --> Auth
    Auth --> AuthCoordinator
    AuthCoordinator --> AuthSign
    AuthSign --> Compression: compression negotiated
    AuthSign --> Session: no compression
    Compression --> Session
    Session --> Ready
    Ready --> [*]

    note right of Hello
        Client: send HELLO, receive HELLO
        Server: receive HELLO, send HELLO
    end note

    note right of Auth
        AUTH_REQUEST <-> AUTH_REPLY_MORE / AUTH_DONE
        CephX may require multiple rounds
    end note

    note right of AuthCoordinator
        Computes HMAC signatures and
        installs encryption state
    end note

    note right of AuthSign
        Client sends AUTH_SIGNATURE first
        Server sends AUTH_SIGNATURE first
    end note

    note right of Session
        CLIENT_IDENT or SESSION_RECONNECT
        completes the handshake
    end note
```

### Lower-level protocol states in `state_machine.rs`

```mermaid
stateDiagram-v2
    [*] --> BannerConnecting
    [*] --> BannerAccepting

    BannerConnecting --> HelloConnecting
    HelloConnecting --> AuthConnecting
    AuthConnecting --> AuthConnectingSign
    AuthConnectingSign --> CompressionConnecting: compression needed
    AuthConnectingSign --> SessionConnecting: no compression
    CompressionConnecting --> SessionConnecting
    SessionConnecting --> Ready
    SessionConnecting --> SessionConnecting: WAIT / RETRY / RESET / RECONNECT retry

    BannerAccepting --> HelloAccepting
    HelloAccepting --> AuthAccepting
    AuthAccepting --> AuthAccepting: AUTH_REPLY_MORE loop
    AuthAccepting --> AuthAcceptingSign: AUTH_DONE
    AuthAcceptingSign --> CompressionAccepting: compression needed
    AuthAcceptingSign --> SessionAccepting: no compression
    CompressionAccepting --> SessionAccepting
    SessionAccepting --> Ready
```

### Session sub-state machine

This is the part that decides whether a connection becomes a fresh session, a resumed session, or must retry/reset first.

```mermaid
stateDiagram-v2
    [*] --> DecideOutbound

    DecideOutbound --> SendClientIdent: server_cookie == 0
    DecideOutbound --> SendReconnect: server_cookie != 0

    SendClientIdent --> WaitServerSessionReply
    SendReconnect --> WaitServerSessionReply

    WaitServerSessionReply --> Established: SERVER_IDENT
    WaitServerSessionReply --> Reconnected: SESSION_RECONNECT_OK
    WaitServerSessionReply --> WaitServerSessionReply: WAIT
    WaitServerSessionReply --> SendReconnect: SESSION_RETRY / bump connect_seq
    WaitServerSessionReply --> SendReconnect: SESSION_RETRY_GLOBAL / update global_seq
    WaitServerSessionReply --> SendClientIdent: SESSION_RESET / clear server_cookie
    WaitServerSessionReply --> Failed: IDENT_MISSING_FEATURES
    WaitServerSessionReply --> Failed: required feature mismatch

    Established --> [*]
    Reconnected --> [*]
    Failed --> [*]
```

### Server-side accept path for the session phase

```mermaid
stateDiagram-v2
    [*] --> WaitClientSessionFrame

    WaitClientSessionFrame --> ValidateClientIdent: CLIENT_IDENT
    WaitClientSessionFrame --> SendReconnectOk: SESSION_RECONNECT

    ValidateClientIdent --> RejectClient: missing required features
    ValidateClientIdent --> SendServerIdent: supported

    SendServerIdent --> Ready
    SendReconnectOk --> Ready
    RejectClient --> [*]
    Ready --> [*]
```

### Runtime behavior after `Ready`

Once the handshake finishes, the connection switches to framed message exchange:

- incoming keepalives are answered with `KEEPALIVE2_ACK`,
- message ordering is tracked with `in_seq` / `out_seq`,
- sent messages are retained for replay across reconnects,
- outbound messages are scheduled through a three-lane priority queue: `high`, `normal`, then `low`.

```mermaid
flowchart TD
    A[Ready connection] --> B{Incoming frame}
    B -->|KEEPALIVE2| C[Send KEEPALIVE2_ACK]
    B -->|ACK / control| D[Update connection state]
    B -->|message| E[Advance in_seq and dispatch]

    F[Outbound message] --> G{Priority}
    G -->|High| H[high queue]
    G -->|Normal| I[normal queue]
    G -->|Low| J[low queue]
    H --> K[Pop next outbound]
    I --> K
    J --> K
    K --> L[Encode/compress/encrypt/send]
```

## Testing

### Unit Tests

Run the standard unit tests with:

```bash
cargo test --lib
```

### Integration Tests

The integration tests in `tests/connection_tests.rs` require a running Ceph cluster. **These tests will fail if the prerequisites are not met.**

#### Prerequisites

1. A running Ceph cluster (local or remote)
2. The `CEPH_CONF` environment variable set to the path of the ceph.conf file

#### Running Integration Tests

```bash
# Set the ceph config path
export CEPH_CONF=/path/to/ceph.conf

# Run the integration tests
cargo test --test connection_tests -- --nocapture
```

#### Configuration

The integration tests will load configuration from the ceph.conf file specified by `CEPH_CONF`. The configuration file should include:
- Monitor addresses in the `mon host` option
- Keyring path in the `keyring` option (or it will use the default path)

## CI/CD

**Important**: The integration tests require a running Ceph cluster and will fail in CI if `CEPH_CONF` is not set. Make sure your CI environment either:
1. Sets up a Ceph cluster and provides the `CEPH_CONF` environment variable, or
2. Explicitly excludes the `connection_tests` from the test run

To exclude integration tests in CI:
```bash
cargo test --workspace --lib --bins
```
