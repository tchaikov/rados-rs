# Quick Start Guide - Testing RADOS Operations

## Prerequisites Check

```bash
# Make sure you're in the rados-rs directory
cd ~/dev/rados-rs
```

## Step 1: Start the Ceph Cluster

```bash
cd ~/dev/ceph/build
../src/vstart.sh -d

# Wait a moment for it to start, then verify
sleep 5
bin/ceph -s

# Should show something like:
#   cluster:
#     id:     ...
#     health: HEALTH_OK (or HEALTH_WARN initially)
#   services:
#     mon: 3 daemons
#     mgr: x(active)
#     osd: 3 osds: 3 up, 3 in
```

## Step 2: Verify testpool Exists

```bash
cd ~/dev/ceph/build
bin/ceph osd pool ls

# Should show:
#   .mgr
#   testpool

# If testpool doesn't exist, create it:
bin/ceph osd pool create testpool 32
```

## Step 3: Run the Tests

```bash
cd ~/dev/rados-rs

# Run the automated test script
./test_rados_ops.sh 2>&1 | tee test_output.log

# This will test multiple object names and show:
# - What names we try to write
# - What names actually get created (using official rados ls)
# - Whether delete works
```

## Step 4: Analyze Results

Look for these patterns in the output:

### For Object Name Truncation:
```
Test Object: 'test_cli_object' (15 chars)
...
Found: 'test_cli_obj' (12 chars)
⚠️  NAME MISMATCH! Expected 'test_cli_object' but got 'test_cli_obj'
Missing chars: 3
```

### For Delete Not Working:
```
5. Deleting with Rust rados CLI...
...
6. Verifying deletion...
⚠️  OBJECT STILL EXISTS AFTER DELETE!
```

## Step 5: Examine Debug Logs

The test script captures debug output. Look for encoding details:

```bash
# Extract just the encoding information
grep -E "(Encoding|ObjectLocator|opcode)" test_output.log
```

You should see lines like:
```
Encoding ObjectLocator: pool=2, key='', namespace='', hash=-1
ObjectLocator encoded, size: 33 bytes
Encoding object name 'test_cli_object' with length 15
Object name encoding complete, buffer position after object name: 189
Op[0]: opcode=0x2001 (Write), indata_len=11
```

## Manual Testing (Alternative)

If the automated script has issues, test manually:

```bash
cd ~/dev/rados-rs

# Test 1: Write with a 15-character name
echo "HelloWorld" | RUST_LOG=debug ./target/release/rados \
  --pool testpool \
  --mon-host v2:127.0.0.1:3300 \
  --keyring ~/dev/ceph/build/keyring \
  --name client.admin \
  --debug put test_cli_object - 2>&1 | tee write.log

# Test 2: Check what was actually created
cd ~/dev/ceph/build
bin/rados -p testpool ls | grep test_cli

# Test 3: Stat the object
bin/rados -p testpool stat test_cli_object

# Test 4: Delete with our CLI
cd ~/dev/rados-rs
RUST_LOG=debug ./target/release/rados \
  --pool testpool \
  --mon-host v2:127.0.0.1:3300 \
  --keyring ~/dev/ceph/build/keyring \
  --name client.admin \
  --debug rm test_cli_object 2>&1 | tee delete.log

# Test 5: Verify deletion
sleep 1
cd ~/dev/ceph/build
bin/rados -p testpool stat test_cli_object
# Should return: "error stat-ing testpool/test_cli_object: (2) No such file or directory"
```

## Troubleshooting

### Cluster Won't Start
```bash
cd ~/dev/ceph/build
# Stop any existing processes
../src/stop.sh
# Clean up and restart
rm -rf out dev
MON=3 OSD=3 MDS=0 ../src/vstart.sh -n -d
```

### Connection Refused
```bash
# Check if monitors are listening
netstat -tlnp | grep 330

# Check monitor logs
tail -50 ~/dev/ceph/build/out/mon.a.log
```

### No testpool
```bash
cd ~/dev/ceph/build
bin/ceph osd pool create testpool 32
bin/ceph osd pool application enable testpool rados
```

## What to Report

After running the tests, please share:

1. The output from `./test_rados_ops.sh` (or the test_output.log file)
2. The encoding details from the debug logs:
   - ObjectLocator size
   - Object name length
   - Operation opcodes
3. What objects actually appear when you run `rados ls`
4. Any errors from OSD logs: `tail -100 ~/dev/ceph/build/out/osd.*.log`

This information will help identify whether the bug is in:
- Our encoding logic (wrong bytes being sent)
- Message framing (incorrect section boundaries)
- OSD decoding (mismatch in protocol interpretation)
