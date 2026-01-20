# OSDMap Decoding Investigation - Findings

## Issue Summary
OSDMap decoding fails when connecting to a real Ceph cluster (Reef 19.2.x). The issue occurs in the client-section decoding after successfully decoding pools.

## Root Cause
The encoding format for `osd_weight` and possibly `osd_addrs_client` in OSDMap client version 10 differs from our current implementation.

## Detailed Analysis

### What Works ✓
- PgPool decoding works correctly
- Pools are decoded successfully (2 pools)
- Pool names decoded successfully
- max_osd, flags, and initial fields decode correctly

### Where It Fails ✗
After successfully decoding:
- `osd_state` as Vec<u32> with 3 elements: [1, 65536, 1]
- max_osd = 1 (expected)
- client_v = 10

The next field `osd_weight` fails because:

**Bytes at failure point:** `[02, 01, 00, 00, 00, 01, 01, 01, 1c, 00, 00, 00...]`

### Current Understanding

1. **osd_state encoding** (client_v >= 5):
   - Uses Vec<u32> with length prefix ✓ Confirmed working
   - Length = 3 elements (not matching max_osd=1)
   - This is surprising but may be correct

2. **osd_weight encoding** (UNKNOWN):
   - NOT using standard Vec<u32> with length prefix
   - Bytes `02 01 00 00` would decode as length=258 (wrong)
   - **Hypothesis 1**: osd_weight not present in this version - TRIED, fails later
   - **Hypothesis 2**: osd_weight uses osd_state.len() without prefix - TRIED, values look wrong (258, 16843008, 28)
   - **Hypothesis 3**: osd_weight uses different encoding (compact, u16, or other)

3. **osd_addrs_client encoding** (client_v >= 8):
   - Expected: Vec<EntityAddrvec>
   - Bytes suggest versioned encoding but length values are invalid (16M+)
   - May also use a different encoding pattern

## Bytes Analysis

After osd_state (16 bytes consumed), remaining bytes:
```
[02, 01, 00, 00, 00, 01, 01, 01, 1c, 00, 00, 00, 02, 00, 00, 00, 13, b9, 41, c2...]
```

Possible interpretations:
1. If these are osd_weight values (3 x u32): 258, 16843008, 28 - doesn't look right
2. If `02 01 00 00 00` is ENCODE_START: v=2, compat=1, len=16777216 - too large
3. If mixed encoding: unclear pattern

## Next Steps Required

### Immediate Actions Needed
1. **Consult Ceph C++ source code** for OSDMap::encode_client() in version that produces client_v=10
   - Check exact encoding for osd_state (why length != max_osd?)
   - Check if/how osd_weight is encoded in different versions
   - Check osd_addrs_client encoding format

2. **Create test corpus** from actual Ceph cluster:
   - Extract raw OSDMap bytes from working C++ client
   - Use ceph-dencoder to dump structure
   - Compare with our decoding

3. **Version-specific research**:
   - Determine what client_v=5, 8, and 10 changed
   - Check if osd_weight became optional or changed encoding
   - Verify Vec<EntityAddrvec> encoding format

### Code Changes Needed
Once the correct encoding is understood:
1. Update osd_weight decode logic for client_v >= 5/8/10
2. Update osd_addrs_client decode logic for client_v >= 8
3. Handle case where array lengths != max_osd
4. Add comprehensive tests with real cluster data

## Environment
- Ceph version: Reef 19.2.x (docker image ceph/daemon-base:latest-reef)
- OSDMap client_v: 10
- Cluster: 1 MON, 1 MGR, 1 OSD
- Pools: 2 (device_health_metrics, test-pool)

## Files Modified
- `crates/denc/src/osdmap.rs` - Added debug logging, attempted fixes
- `crates/monclient/examples/test_osdmap_short.rs` - Test harness

## References Needed
- Ceph source: `src/osd/OSDMap.h`, `src/osd/OSDMap.cc`
- Particularly: `OSDMap::encode_client()` and `OSDMap::decode()`
- Version history for client encoding changes
