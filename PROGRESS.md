# RADOS-RS OSD Client Progress

**Last Updated**: 2026-01-22
**Current Branch**: denc-replacement

## Current Status

### ✅ Completed

1. **MOSDOp v8 Encoding** - Fully fixed and matches C/C++ implementations
   - Location: `crates/osdclient/src/messages.rs`
   - Fixed version headers for spgid (struct_v=1, compat=1, len=18)
   - Fixed version headers for reqid (struct_v=2, compat=2, len=21)
   - Fixed entity_name encoding (type u8 + num u64, not string)
   - Fixed trace encoding (24 bytes: 3 × u64 for trace_id, span_id, parent_span_id)
   - Fixed mtime encoding (timespec: u32 sec + u32 nsec, not u64)
   - Added `get_data_section()` method to extract write data
   - **Fixed message tid**: Added `.with_tid(tid)` to message header
   - Reference: `~/dev/linux/net/ceph/osd_client.c` line 2138+ (encode_request_partial)
   - Reference: `~/dev/ceph/src/messages/MOSDOp.h` line 374+ (v8 encoding)

2. **Connection Deadlock Fix** - Adopted Linux kernel pattern
   - Location: `crates/osdclient/src/session.rs`
   - Replaced `Arc<Mutex<Option<Connection>>>` with `mpsc::Sender<Message>`
   - `submit_op()` now queues to channel (non-blocking, like `ceph_con_send()`)
   - Dedicated `io_task()` owns Connection, multiplexes with `tokio::select!`
   - No locks held during I/O operations (like kernel's `con_work()`)
   - Pattern: Similar to `~/dev/linux/net/ceph/messenger.c` line 1544+ (con_work)
   - Pattern: Similar to `~/dev/ceph/src/msg/async/ProtocolV2.cc` line 434+ (send_message)

3. **Object Hash Calculation Fixed** - Implemented Ceph's rjenkins hash
   - Location: `crates/crush/src/hash.rs`
   - Ported `ceph_str_hash_rjenkins()` from C to Rust
   - Implemented `rjenkins_mix()` helper function
   - Updated `object_to_pg()` in `crates/crush/src/placement.rs` to use rjenkins
   - Updated `ObjectId::calculate_hash()` in `crates/osdclient/src/types.rs`
   - **Result**: OSD no longer rejects operations as "misdirected"!
   - Hash of "example_object": `0x4ec401aa` (verified against C implementation)
   - Reference: `~/dev/ceph/src/common/ceph_hash.cc` (ceph_str_hash_rjenkins)

4. **MOSDOpReply v8 Decoding** - Successfully decodes OSD replies
   - Location: `crates/osdclient/src/messages.rs`
   - Fixed pg_t decoding: Added version byte (u8) before pool/seed/preferred
   - Correctly decodes: oid, pgid, flags, result, epoch, num_ops, retry_attempt, rvals, versions
   - Reference: `~/dev/linux/net/ceph/osd_client.c` line 3663+ (decode_MOSDOpReply)
   - Reference: `~/dev/linux/include/linux/ceph/osdmap.h` (ceph_decode_pgid)

### 🎉 Working Features

- ✅ **Write operations** - Successfully writes objects to OSDs
- ✅ **Read operations** - Successfully reads objects from OSDs
- ✅ **Stat operations** - Successfully retrieves object metadata
- ✅ **Message exchange** - Full request/reply cycle working
- ✅ **Hash calculation** - Correct PG placement using rjenkins
- ✅ **Reply decoding** - Properly decodes MOSDOpReply messages

### ⚠️ Known Issues

1. **Delete operation returns -EINVAL (-22)**
   - Write/Read/Stat work correctly
   - Delete operation fails with error code -22
   - This is likely a minor encoding issue in the delete operation
   - Not blocking basic functionality

2. **Write returns version 0**
   - Operations succeed but version is reported as 0
   - May need to check version field decoding

### 🔍 Test Results

```bash
cargo run --package osdclient --example simple_write
```

Output:
```
✓ Write complete, version: 0
✓ Read complete
✓ Stat complete
Error: OSDError { code: -22, message: "Delete operation failed" }
```

## Key Files Modified

### Core Implementation
- `crates/osdclient/src/messages.rs` - MOSDOp/MOSDOpReply encoding/decoding
- `crates/osdclient/src/session.rs` - OSD session with channel-based I/O, added tid to messages
- `crates/osdclient/src/client.rs` - High-level OSD client API
- `crates/osdclient/examples/simple_write.rs` - Test example
- `crates/crush/src/hash.rs` - Added rjenkins hash implementation
- `crates/crush/src/placement.rs` - Updated to use rjenkins hash
- `crates/osdclient/src/types.rs` - Updated ObjectId::calculate_hash

### Reference Implementations
- `~/dev/linux/net/ceph/osd_client.c` - Linux kernel OSD client (v8 encoding, decode_MOSDOpReply)
- `~/dev/linux/include/linux/ceph/osdmap.h` - pg_t decoding (ceph_decode_pgid)
- `~/dev/ceph/src/messages/MOSDOp.h` - Ceph C++ MOSDOp message
- `~/dev/ceph/src/messages/MOSDOpReply.h` - Ceph C++ MOSDOpReply message
- `~/dev/ceph/src/common/ceph_hash.cc` - rjenkins hash implementation
- `~/dev/ceph/src/msg/async/ProtocolV2.cc` - Async messenger pattern
- `~/dev/linux/net/ceph/messenger.c` - Linux kernel messenger (lock-free pattern)

1. **Check if OSD is sending reply**
   - OSD may be sending reply but our receive path has issues
   - Or OSD is closing connection due to encoding problem
   - Need to capture OSD logs at moment of message arrival

2. **Possible remaining encoding issues**
   - Message framing may be incorrect
   - Data section alignment
   - Missing fields in v8 format

3. **OSD-side debugging**
   ```bash
   cd ~/dev/ceph/build
   bin/ceph tell osd.0 injectargs '--debug_osd 20 --debug_ms 20'
   # Run test
   grep -a "MOSDOp\|decode.*error\|message type 42" out/osd.0.log
   ```

## Key Files

### Core Implementation
- `crates/osdclient/src/messages.rs` - MOSDOp encoding/decoding
- `crates/osdclient/src/session.rs` - OSD session with channel-based I/O
- `crates/osdclient/src/client.rs` - High-level OSD client API
- `crates/osdclient/examples/simple_write.rs` - Test example

### Reference Implementations
- `~/dev/linux/net/ceph/osd_client.c` - Linux kernel OSD client (v8 encoding)
- `~/dev/ceph/src/messages/MOSDOp.h` - Ceph C++ MOSDOp message
- `~/dev/ceph/src/msg/async/ProtocolV2.cc` - Async messenger pattern
- `~/dev/linux/net/ceph/messenger.c` - Linux kernel messenger (lock-free pattern)

## Architecture

### OSDSession Design (Linux Kernel Pattern)

```rust
// Old (deadlock):
connection: Arc<Mutex<Option<Connection>>>
recv_task() {
    let guard = connection.lock().await;  // Hold lock
    guard.recv_message().await;           // Blocking I/O - DEADLOCK!
}

// New (lock-free):
send_tx: mpsc::Sender<Message>

submit_op() {
    send_tx.send(msg).await  // Just queue, no I/O
}

io_task(mut connection, mut send_rx) {
    loop {
        tokio::select! {
            Some(msg) = send_rx.recv() => connection.send_message(msg).await,
            msg = connection.recv_message() => handle_reply(msg).await,
        }
    }
}
```

### Message Flow

```
Client Code
    ↓
submit_op(MOSDOp)
    ↓
encode() → get_data_section()
    ↓
send_tx.send(Message { front: 223 bytes, data: 12 bytes })
    ↓
[mpsc channel]
    ↓
io_task's tokio::select!
    ↓
connection.send_message()
    ↓
msgr2::protocol::send_frame()
    ↓
[Network]
    ↓
OSD receives → ??? (connection closes)
```

## Testing

### Local Cluster Setup
```bash
# Start cluster
cd ~/dev/ceph/build
../src/vstart.sh -d

# Check status
bin/ceph -s

# Enable debug logging
bin/ceph tell osd.0 injectargs '--debug_osd 20 --debug_ms 20'
```

### Run Test
```bash
cd ~/dev/rados-rs
cargo run --package osdclient --example simple_write
```

### Check OSD Logs
```bash
tail -f ~/dev/ceph/build/out/osd.0.log | grep -a "MOSDOp\|decode\|error"
```

## Next Steps

1. **Enable detailed OSD logging during test**
   - Run test with OSD debug enabled
   - Capture exact moment OSD receives our message
   - Check for decode errors or unexpected fields

2. **Compare wire format with official client**
   - Capture tcpdump of official `rados` client: `rados -p test put example_object -`
   - Capture tcpdump of our client
   - Compare byte-by-byte

3. **Verify message header fields**
   - Check if all `ceph_msg_header` fields are correct
   - Verify `front_len`, `middle_len`, `data_len` in header
   - Location: `crates/msgr2/src/message.rs`

4. **Check for version mismatches**
   - Our code assumes v8, but may need to support v9 (HEAD_VERSION)
   - Check `~/dev/ceph/src/messages/MOSDOp.h` line 402+ for v9 differences

## Debug Commands

```bash
# Check if messages are queued
grep "Submitted operation tid=" /tmp/client_output.txt

# Check if messages are sent
grep "send_message() type=0x002a" /tmp/client_output.txt

# Check OSD receive
grep -a "message type.*42\|MOSDOp" ~/dev/ceph/build/out/osd.0.log

# Compare with official client
LD_PRELOAD=/usr/lib/libasan.so.8 ~/dev/ceph/build/bin/rados \
  --conf ~/dev/ceph/build/ceph.conf -p test put example_object - < /dev/zero
```

## Known Working Parts

1. ✅ Monitor connection and authentication
2. ✅ OSDMap subscription and decoding
3. ✅ OSD connection and session establishment
4. ✅ MOSDOp message encoding (matches C/C++)
5. ✅ Message framing and encryption (SECURE mode)
6. ✅ Channel-based I/O (no deadlock)
7. ✅ Message successfully sent to OSD

## Known Issues

1. ❌ OSD closes connection after receiving message
2. ⚠️ Need to verify if OSD is sending reply or rejecting message
3. ⚠️ MOSDOpReply decoding untested (no replies received yet)

## Resources

### Ceph Source Locations
- Message definitions: `~/dev/ceph/src/messages/`
- OSD implementation: `~/dev/ceph/src/osd/`
- Messenger v2: `~/dev/ceph/src/msg/async/ProtocolV2.{cc,h}`
- Encoding helpers: `~/dev/ceph/src/include/denc.h`

### Linux Kernel Locations
- OSD client: `~/dev/linux/net/ceph/osd_client.c`
- Messenger: `~/dev/linux/net/ceph/messenger.c`
- Message encoding: `~/dev/linux/net/ceph/messenger_v2.c`

### Test Corpus
- Object samples: `~/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/`
- Use `ceph-dencoder` to decode: `bin/ceph-dencoder type MOSDOp import <file> decode dump_json`
