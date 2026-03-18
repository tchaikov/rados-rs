# osdclient

A native Rust implementation of the Ceph OSD client. Communicates directly with
Ceph OSDs over the msgr2 protocol, performing CRUSH-based object placement from a
live OSDMap received via `monclient`.

## Features

### Object I/O

| Method | Description |
|--------|-------------|
| `IoCtx::write_full(oid, data)` | Overwrite entire object |
| `IoCtx::write(oid, offset, data)` | Partial write at offset |
| `IoCtx::read(oid, offset, len)` | Read bytes (len=0 means read to end) |
| `IoCtx::sparse_read(oid, offset, len)` | Read with extent map (efficient for sparse objects) |
| `IoCtx::stat(oid)` | Size and mtime without fetching data |
| `IoCtx::create(oid, exclusive)` | Create object |
| `IoCtx::remove(oid)` | Delete object |

### Tokio I/O Adaptors

`RadosObject` implements `AsyncRead + AsyncWrite + AsyncSeek` over a single RADOS
object, making it usable anywhere a `tokio::io` stream is expected:

```rust
use osdclient::{IoCtx, RadosObject};
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncSeekExt};
use std::io::SeekFrom;

let mut obj = RadosObject::new(ioctx, "my-object".to_string());

obj.write_all(b"hello world").await?;
obj.seek(SeekFrom::Start(0)).await?;

let mut buf = Vec::new();
obj.read_to_end(&mut buf).await?;
assert_eq!(buf, b"hello world");
```

Performance notes:
- No read-ahead or write coalescing — wrap with `tokio::io::BufReader`/`BufWriter` for small I/O.
- `SeekFrom::End` issues a stat RPC; prefer `SeekFrom::Start`/`SeekFrom::Current` in hot paths.

### Object Listing

`list_objects_stream` returns a lazily-paginated `futures::Stream` of object names,
driving PGNLS requests across all PGs in bitwise-sorted order:

```rust
use futures::StreamExt;
use osdclient::list_objects_stream;

let stream = list_objects_stream(ioctx, 100);
stream.for_each(|result| async move {
    match result {
        Ok(name) => println!("{name}"),
        Err(e)   => eprintln!("error: {e}"),
    }
}).await;
```

`IoCtx::ls()` collects all names into a `Vec<String>` (convenience wrapper around
the stream).

### Extended Attributes

```rust
ioctx.set_xattr("oid", "key", Bytes::from("value")).await?;
let val = ioctx.get_xattr("oid", "key").await?;
let keys = ioctx.list_xattrs("oid").await?;
ioctx.remove_xattr("oid", "key").await?;
```

### Object Locking

Advisory exclusive and shared locks matching `rados_lock_exclusive` /
`rados_lock_shared` in librados:

```rust
ioctx.lock_exclusive("oid", "mylock", "cookie", "desc", None).await?;
// ... critical section ...
ioctx.unlock("oid", "mylock", "cookie").await?;
```

### Pool Snapshots

```rust
// List all pool-level snapshots
let snaps = ioctx.pool_snap_list().await?;

// Resolve name → snap_id
let snap_id = ioctx.pool_snap_lookup("weekly-backup").await?;

// Roll back an object to a snapshot
ioctx.snap_rollback("oid", snap_id).await?;
```

### Pool Management

```rust
// Via OSDClient (uses MPoolOp messages)
client.create_pool("my-pool", None).await?;
client.delete_pool("my-pool", true).await?;
let pools = client.list_pools().await?;
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      User Application                       │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
          ┌──────────────────────────┐
          │  IoCtx (per-pool facade) │
          └──────────────┬───────────┘
                         │
                         ▼
                 ┌───────────────┐
                 │  OSDClient    │  routing, throttle, OSDMap watch
                 └───────┬───────┘
                         │
          ┌──────────────┼──────────────┐
          │              │              │
          ▼              ▼              ▼
    ┌─────────┐    ┌──────────┐   ┌──────────┐
    │ Session │    │  CRUSH   │   │ Throttle │
    │ Manager │    │Placement │   │ /Tracker │
    └────┬────┘    └──────────┘   └──────────┘
         │
         ▼
  ┌──────────────┐      ┌─────────────┐
  │ OSDSession   │      │  MonClient  │
  │ (per OSD)    │◄─────│  (OSDMap)   │
  └──────┬───────┘      └─────────────┘
         │
         ▼
  ┌──────────────┐
  │  msgr2       │
  │  Connection  │
  └──────────────┘
         │
         ▼
     [Ceph OSD]
```

## Connection Management

One `OSDSession` per OSD, matching the Ceph C++ Objecter design since 2010. Operations
multiplex over the single per-OSD connection via mpsc channels. Automatic reconnection
and CRUSH remap on OSDMap epoch changes.

## Running Tests

Integration tests require a running Ceph cluster:

```bash
# Start a local vstart cluster
cd /path/to/ceph/build
../src/vstart.sh -d --without-dashboard

# Run integration tests
CEPH_LIB=/path/to/ceph/build/lib \
ASAN_OPTIONS=detect_odr_violation=0,detect_leaks=0 \
CEPH_CONF=/path/to/ceph/build/ceph.conf \
cargo test -p osdclient --tests -- --ignored --nocapture

# Unit tests (no cluster required)
cargo test -p osdclient --lib
```

Available test suites:

| File | Coverage |
|------|----------|
| `tests/integration_test.rs` | Full CRUD, sparse read, xattrs, locks, snapshots |
| `tests/object_io_test.rs` | `RadosObject` AsyncRead/Write/Seek, `list_objects_stream` |
| `tests/pool_operations.rs` | Pool create/delete/list |
| `tests/rados_operations.rs` | High-level rados-style operations |
| `tests/object_placement_test.rs` | CRUSH placement correctness |
| `tests/osdmap_test.rs` | OSDMap decode and pool lookup |

## Source Layout

| File | Responsibility |
|------|---------------|
| `src/client.rs` | `OSDClient`: routing, CRUSH placement, pool/session management |
| `src/ioctx.rs` | `IoCtx`: per-pool facade over `OSDClient` |
| `src/object_io.rs` | `RadosObject`: `AsyncRead + AsyncWrite + AsyncSeek` adaptor |
| `src/list_stream.rs` | `list_objects_stream`: paginated PGNLS as `futures::Stream` |
| `src/session.rs` | `OSDSession`: per-OSD msgr2 connection and op tracking |
| `src/messages.rs` | `MOSDOp` / `MOSDOpReply` encode/decode |
| `src/osdmap.rs` | OSDMap decode and pool/PG queries |
| `src/operation.rs` | `OpBuilder`: fluent builder for compound OSD operations |
| `src/types.rs` | Core types: `OSDOp`, `OpCode`, result types |
| `src/error.rs` | `OSDClientError` |
| `src/throttle.rs` | In-flight ops/bytes throttle |
| `src/tracker.rs` | Per-op timeout tracking |
| `src/backoff.rs` | Per-PG backoff (OSD BACKOFF messages) |
| `src/lock.rs` | Advisory lock request types |
| `src/snapshot.rs` | `SnapId` newtype |
| `src/pg_nls_response.rs` | `pg_nls_response_t` decode |
| `src/pgmap_types.rs` | PGMap / OSD stat types |
| `src/denc_types.rs` | Wire types for OSD ops (hobject_t, osd_reqid_t, …) |
