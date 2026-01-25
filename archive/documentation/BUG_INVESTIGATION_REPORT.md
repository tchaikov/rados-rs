# RADOS Operations Bug Investigation Report

## Summary

I've identified and partially diagnosed both reported issues through systematic testing:

### Issue #1: Write Operations Fail for Certain Object Name Lengths

**Status**: Root cause identified but not yet fixed

**Symptoms**:
- Objects with names of length 11, 13 characters: Created successfully ✓
- Objects with names of length 12, 14, 15+ characters: NOT created ✗
- Operations report success (result=0) even when they fail
- Encoding appears correct (byte positions match expectations)

**Test Results**:
```
test_12char (11 chars)      → Created ✓
test_13chars (12 chars)     → NOT created
test_14chars! (13 chars)    → Created ✓
test_15chars!! (14 chars)   → NOT created
test_16chars!!! (15 chars)  → NOT created
test_cli_object (15 chars)  → NOT created (original bug report)
test_xyz (8 chars)          → Created ✓
```

**Key Findings**:
1. ObjectLocator encodes to exactly 34 bytes (correct)
2. Object name encoding positions are mathematically correct
3. The pattern suggests a message framing or padding issue
4. OSD silently rejects these operations without logging errors

### Issue #2: Delete Operations Don't Work

**Status**: Confirmed and diagnosed

**Symptoms**:
- Delete operations report success (result=0)
- Objects remain in the cluster after "successful" delete
- Version numbers in replies are suspiciously large

**Test Results**:
```
Created test_xyz → Delete reported success → Object still exists ✗
```

**Key Finding**:
- Version numbers from replies are multiples of 2^32:
  - 197568495616 = 0x2E00000000 = 46 << 32
  - 279172874240 = 0x4100000000 = 65 << 32
- This suggests reply parsing is offset by several bytes

## Root Cause Analysis

### Primary Suspect: MOSDOp Message Encoding Issue

The pattern of failures and the suspicious version numbers point to a **message framing bug** in either:

1. **MOSDOp encoding** (crates/osdclient/src/messages.rs:88-234)
   - The front section size calculation may be incorrect
   - OSD is parsing fields from wrong offsets

2. **MOSDOpReply decoding** (crates/osdclient/src/messages.rs:281-475)
   - We may be reading version from the wrong offset
   - This could cause us to misinterpret success/failure

### Specific Areas to Investigate

#### 1. ObjectLocator Encoding Mismatch

The Ceph C++ code uses `ENCODE_FINISH_NEW_COMPAT` which can **update** the compat field after encoding:

```cpp
void object_locator_t::encode(ceph::buffer::list& bl) const {
  __u8 encode_compat = 3;
  ENCODE_START(6, encode_compat, bl);
  // ... encode fields ...
  if (hash != -1)
    encode_compat = std::max<std::uint8_t>(encode_compat, 6);
  ENCODE_FINISH_NEW_COMPAT(bl, encode_compat);  // Updates header!
}
```

Our implementation (crates/denc/src/crush_types.rs:11-52) calculates compat up-front and doesn't support post-encoding updates.

**However**: Since our hash == -1, compat should stay at 3, which matches our behavior. So this is likely NOT the issue.

#### 2. Message Version Header

We're encoding MOSDOp as version 8 (line 217 in session.rs), but we may be missing some fields or encoding them in the wrong order for v8.

#### 3. Reply Parsing Offset

The version numbers being shifted by 32 bits strongly suggests we're reading `version` from 4 bytes too early, causing us to read part of the previous field.

## Next Steps to Fix

### For Object Name Truncation/Failure:

1. **Compare wire format** with official client:
   ```bash
   # Capture our client
   sudo tcpdump -i lo -w /tmp/rust_rados.pcap host 192.168.1.37 and port 6800
   # Capture official client
   sudo tcpdump -i lo -w /tmp/ceph_rados.pcap host 192.168.1.37 and port 6800
   ```

2. **Add hexdump of encoded message**:
   - Dump the entire front section as hex
   - Compare with Ceph C++ MOSDOp::encode_payload() output

3. **Check message header fields**:
   - Verify front_len, middle_len, data_len in message header
   - These are set by msgr2 Connection but might be wrong

### For Delete Operation:

1. **Fix reply parsing offset**:
   - Add debug output showing what we read at each step
   - Compare with MOSDOpReply.h decode() line by line
   - The version is being read 4 bytes too early

2. **Verify retry_attempt handling**:
   - Current code warns about missing retry_attempt but continues
   - This might be masking parsing errors

## Debug Tools Created

- `test_rados_ops.sh` - Automated testing script
- `DEBUG_GUIDE.md` - Comprehensive debugging guide
- `TESTING_GUIDE.md` - Step-by-step testing instructions
- Enhanced debug logging with eprintln! statements

## Files Modified

- `crates/osdclient/src/messages.rs` - Added encoding debug output
- `crates/osdclient/src/session.rs` - Added reply handling debug output
- `crates/osdclient/src/client.rs` - Added operation debug output

## Recommended Fix Priority

1. **HIGH**: Fix MOSDOpReply decoding offset (version field)
2. **HIGH**: Capture and compare wire format with official client
3. **MEDIUM**: Verify all MOSDOp v8 fields are present and correct
4. **LOW**: Improve error reporting when operations silently fail

##  Commands to Continue Investigation

```bash
# Test write with different lengths
for i in {8..20}; do
  name=$(printf "test_%0${i}d" 1 | cut -c1-$i)
  echo "Testing length $i: $name"
  echo "data" | ./target/debug/rados ... put "$name" -
  # Check if created
done

# Compare with official client
echo "data" | rados -p testpool put test_official -
echo "data" | ./target/debug/rados -p testpool put test_rust -

# Check OSD logs for our operations
tail -f ~/dev/ceph/build/out/osd.*.log | grep -E "client\.|MOSDOp"
```

## Conclusion

Both bugs appear to be related to **message encoding/decoding issues** rather than logical errors. The operations are being sent but either:
- Are malformed and silently rejected (write)
- Succeed but we misparse the reply (delete)

The fix requires careful comparison with Ceph's C++ implementation and potentially capturing actual wire traffic to see the byte-level differences.
