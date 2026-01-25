# RADOS Operations Issues - Debug Guide

## Issues Identified

### Issue #1: Object Name Truncation
**Symptom**: When writing "test_cli_object" (15 chars), the object appears as "test_cli_obj" (12 chars) - exactly 3 characters are lost.

### Issue #2: Delete Operation Not Working
**Symptom**: Delete operation completes without errors, but the object still exists in the cluster.

## Debug Logging Added

I've added comprehensive debug logging to help diagnose these issues:

1. **ObjectLocator Encoding**: Shows the size and content of the encoded ObjectLocator
2. **Object Name Encoding**: Shows the object name and its length before/after encoding
3. **Operation Details**: Shows the operation codes and data sizes

## Running Tests

### Step 1: Start the Ceph Cluster
```bash
cd ~/dev/ceph/build
../src/vstart.sh -d
# Wait for cluster to be ready
bin/ceph -s
```

### Step 2: Run the Test Script
```bash
cd ~/dev/rados-rs
chmod +x test_rados_ops.sh
./test_rados_ops.sh
```

### Step 3: Analyze Debug Output

Look for lines like:
```
Encoding ObjectLocator: pool=2, key='', namespace='', hash=-1
ObjectLocator encoded, size: XX bytes
Encoding object name 'test_cli_object' with length 15
Object name encoding complete, buffer position after object name: YYY
```

## Manual Testing

### Test Write Operation:
```bash
echo "Hello RADOS!" | RUST_LOG=debug ./target/release/rados \
  --pool testpool --mon-host v2:127.0.0.1:3300 \
  --keyring ~/dev/ceph/build/keyring --name client.admin \
  --debug put test_cli_object - 2>&1 | grep -E "(Encoding|Object)"
```

### Verify with Official Client:
```bash
cd ~/dev/ceph/build
bin/rados -p testpool ls | grep test_cli
bin/rados -p testpool stat test_cli_object
```

### Test Delete Operation:
```bash
RUST_LOG=debug ./target/release/rados \
  --pool testpool --mon-host v2:127.0.0.1:3300 \
  --keyring ~/dev/ceph/build/keyring --name client.admin \
  --debug rm test_cli_object 2>&1 | grep -E "(Encoding|Object|opcode)"
```

### Verify Deletion:
```bash
cd ~/dev/ceph/build
bin/rados -p testpool stat test_cli_object
# Should return "error stat-ing testpool/test_cli_object: (2) No such file or directory"
```

## What to Look For

### For Object Name Truncation:

1. **Check the encoded length**: The debug output shows:
   ```
   Encoding object name 'test_cli_object' with length 15
   ```
   This should be 15 for "test_cli_object".

2. **Check ObjectLocator size**: Should be around 30-35 bytes for empty key/namespace:
   ```
   ObjectLocator encoded, size: XX bytes
   ```
   If this is wrong, it could cause the decoder to read from the wrong offset.

3. **Compare with tcpdump**: You can capture the actual traffic to see what bytes are sent:
   ```bash
   sudo tcpdump -i lo -w /tmp/rados.pcap port 6800-6810 &
   # Run the write operation
   # Then analyze with wireshark
   ```

### For Delete Not Working:

1. **Check operation code**: Should see:
   ```
   Op[0]: opcode=0x2005 (Delete), indata_len=0
   ```
   The opcode 0x2005 = CEPH_OSD_OP_DELETE

2. **Check flags**: Should include WRITE flag:
   ```
   flags=0x0021 (ACK|WRITE)
   ```

## Potential Root Causes

### Object Name Truncation - Possible Causes:

1. **ObjectLocator encoding bug**: The versioned encoding of ObjectLocator might be adding wrong size, causing the OSD decoder to skip bytes and start reading the object name from the wrong offset.

2. **String encoding bug**: The string encoding in denc might have an off-by-one error.

3. **Message header issue**: The front section size might be calculated incorrectly.

### Delete Not Working - Possible Causes:

1. **Missing flags**: Delete might require ONDISK flag in addition to ACK and WRITE.

2. **Wrong operation encoding**: The delete operation structure might not be encoded correctly.

3. **Retry attempt field**: The retry_attempt field might need to be 0 instead of -1 for first attempts.

## Next Steps

After running the tests, share:

1. The full debug output showing the encoding details
2. The output from `rados ls` showing what objects were actually created
3. Any error messages from the OSD logs (check ~/dev/ceph/build/out/*.log)

This will help identify the exact location of the bugs.
