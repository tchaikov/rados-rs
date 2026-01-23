# Bug Investigation Report - Object Operations Testing

## Date: January 24, 2026

## Summary
Investigated two reported issues with the rados-rs implementation:
1. **Object name truncation** - "test_cli_object" becomes "test_cli_obj"
2. **Delete operation not working** - Delete succeeds but object remains

## Test Environment
- **Cluster:** Local Ceph vstart cluster
- **Monitor:** v2:192.168.1.37:40830
- **Pool:** testpool
- **Client:** target/debug/rados (rados-rs implementation)

## Test Results

### Issue 1: Object Name Truncation
**Status:** ✅ **NOT REPRODUCIBLE** - Working correctly

**Test:**
```bash
echo "Hello RADOS test!" | target/debug/rados -p testpool \
  --mon-host "v2:192.168.1.37:40830" \
  --keyring /home/kefu/dev/ceph/build/keyring \
  --name client.admin put test_our_client_name -
```

**Result:**
- Object created successfully with full name: `test_our_client_name`
- Verified with official client: `bin/rados -p testpool ls | grep test_our`
- Output: `test_our_client_name` (complete, not truncated)

**Conclusion:** Object names are being encoded and transmitted correctly. The truncation issue may have been fixed in previous commits or was related to a different scenario.

### Issue 2: Delete Operation Not Working
**Status:** ✅ **NOT REPRODUCIBLE** - Working correctly

**Test Sequence:**
```bash
# Create object
echo "Test delete bug" | target/debug/rados -p testpool \
  --mon-host "v2:192.168.1.37:40830" \
  --keyring /home/kefu/dev/ceph/build/keyring \
  --name client.admin put test_delete_check -

# Verify creation
bin/rados -p testpool stat test_delete_check
# Output: testpool/test_delete_check mtime 0.000000, size 16

# Delete with our client
target/debug/rados -p testpool \
  --mon-host "v2:192.168.1.37:40830" \
  --keyring /home/kefu/dev/ceph/build/keyring \
  --name client.admin rm test_delete_check

# Verify deletion
bin/rados -p testpool stat test_delete_check
# Output: error stat-ing testpool/test_delete_check: (2) No such file or directory
```

**Result:**
- Delete operation completed successfully
- Object no longer exists in cluster
- Verified with official Ceph client

**Conclusion:** Delete operations are working correctly. The OpCode for Delete (0x1105) is correctly calculated as WR(0x1000) | DATA(0x0100) | 5.

## Code Analysis

### Delete Operation Implementation

**OpCode Definition** (`crates/osdclient/src/types.rs:174`):
```rust
Delete = osd_op!(WR, DATA, 5),
```

This expands to:
```rust
Delete = CEPH_OSD_OP_MODE_WR | CEPH_OSD_OP_TYPE_DATA | 5
       = 0x1000 | 0x0100 | 5
       = 0x1105 (4357)
```

**Delete Operation Creation** (`crates/osdclient/src/types.rs:278-285`):
```rust
pub fn delete() -> Self {
    Self {
        op: OpCode::Delete,
        flags: 0,
        extent: None,
        indata: Bytes::new(),
    }
}
```

**Delete Request** (`crates/osdclient/src/client.rs:565-627`):
- Creates ObjectId with correct pool and oid
- Calculates hash for CRUSH placement
- Creates delete operation: `vec![OSDOp::delete()]`
- Builds MOSDOp message with correct flags
- Submits to OSD session
- Waits for result and checks return code

### Object Name Encoding

**Object Name Encoding** (`crates/osdclient/src/messages.rs:156-158`):
```rust
// 10. object name (object_t)
buf.put_u32_le(self.object.oid.len() as u32);
buf.put_slice(self.object.oid.as_bytes());
```

- Length is encoded as u32 little-endian
- Full object name bytes are written
- No truncation in encoding logic

## Unified Message Framework Integration

Both operations now use the unified CephMessage framework:

**Encoding** (`crates/osdclient/src/session.rs:216-227`):
```rust
let ceph_msg = CephMessage::from_payload(&op, 0, CrcFlags::ALL)?;
let mut msg = msgr2::message::Message::new(CEPH_MSG_OSD_OP, ceph_msg.front)
    .with_version(8)
    .with_tid(tid);
msg.data = ceph_msg.data;
```

**Benefits:**
- Automatic CRC calculation and verification
- Type-safe encoding through CephMessagePayload trait
- Consistent message structure (header + footer + payload)

## Possible Explanations for Original Reports

### Object Name Truncation
1. **Fixed in previous commits** - The unified message framework may have fixed encoding issues
2. **Different test scenario** - Original issue may have involved special characters or edge cases
3. **Caching issue** - Cluster may have cached old object names

### Delete Operation
1. **Timing issue** - Original test may not have waited for operation to complete
2. **Different object** - May have been testing with wrong object name
3. **Cluster state** - Cluster may have been in inconsistent state

## Recommendations

### 1. Add Comprehensive Integration Tests
Create automated tests that verify:
- Object creation with various name lengths and characters
- Delete operations with verification
- Read/write/stat operations
- Error handling

### 2. Add Logging for Debugging
Consider adding optional debug logging for:
- Object name encoding (length + actual bytes)
- Operation codes being sent
- OSD responses and return codes

### 3. Monitor Warnings
Address the warning: "Received reply without retry_attempt for tid X"
- This indicates the OSD is not sending retry_attempt field
- May need to handle older OSD versions

## Conclusion

✅ **Both reported issues are NOT REPRODUCIBLE** with the current implementation.

The unified CephMessage framework successfully handles:
- Complete object name encoding/decoding
- Delete operations with proper OpCode
- CRC verification for data integrity
- Type-safe message construction

**Status:** Both operations working correctly as of commit 995739e.

## Test Commands for Verification

```bash
# Set environment
MON_HOST="v2:192.168.1.37:40830"
KEYRING="/home/kefu/dev/ceph/build/keyring"

# Test write
echo "test data" | target/debug/rados -p testpool \
  --mon-host "$MON_HOST" --keyring "$KEYRING" \
  --name client.admin put test_object -

# Verify with official client
cd ~/dev/ceph/build && bin/rados -p testpool ls | grep test_object

# Test delete
target/debug/rados -p testpool \
  --mon-host "$MON_HOST" --keyring "$KEYRING" \
  --name client.admin rm test_object

# Verify deletion
cd ~/dev/ceph/build && bin/rados -p testpool stat test_object
# Should output: error stat-ing testpool/test_object: (2) No such file or directory
```

---

**Report Generated:** January 24, 2026
**Tested By:** Claude Sonnet 4.5
**Framework Version:** Unified CephMessage (commit 995739e)
