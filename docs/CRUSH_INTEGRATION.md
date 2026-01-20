# CRUSH Integration in OSDMap

## Overview

The OSDMap now automatically parses and integrates the CRUSH (Controlled Replication Under Scalable Hashing) map during decoding. This enables object placement calculation and other CRUSH-based operations directly from the OSDMap.

## Changes Made

### 1. CRUSH Dependency
Added the `crush` crate as a dependency to the `denc` crate:
```toml
[dependencies]
crush = { path = "../crush" }
```

### 2. OSDMap Field Update
Changed the `crush` field in `OSDMap` from raw bytes to a parsed `CrushMap`:
```rust
// Before
pub crush: Bytes,

// After
#[serde(skip)]
pub crush: Option<CrushMap>,
```

### 3. Automatic CRUSH Parsing
During OSDMap decoding, the CRUSH map is automatically parsed from the encoded bytes:
```rust
// Decode CRUSH map (as bytes, then parse it)
let crush_bytes = Bytes::decode(&mut client_bytes, features)?;

// Parse the CRUSH map from the bytes
if !crush_bytes.is_empty() {
    let mut crush_buf = crush_bytes.clone();
    match CrushMap::decode(&mut crush_buf) {
        Ok(crush_map) => {
            map.crush = Some(crush_map);
        }
        Err(e) => {
            // Log the error but continue - CRUSH map parsing is optional
            eprintln!("Warning: Failed to parse CRUSH map: {:?}", e);
            map.crush = None;
        }
    }
}
```

### 4. Helper Methods
Added convenience methods to `OSDMap` for accessing CRUSH-related data:

```rust
impl OSDMap {
    /// Get the CRUSH map, if available
    pub fn get_crush_map(&self) -> Option<&CrushMap>

    /// Get a pool by ID
    pub fn get_pool(&self, pool_id: i64) -> Option<&PgPool>

    /// Get a pool name by ID
    pub fn get_pool_name(&self, pool_id: i64) -> Option<&String>

    /// Get the CRUSH rule ID for a pool
    pub fn get_pool_crush_rule(&self, pool_id: i64) -> Option<u8>

    /// Calculate PG to OSD mapping using CRUSH (placeholder)
    pub fn pg_to_osds(&self, pg: &PgId) -> Result<Vec<i32>, RadosError>
}
```

## Usage Examples

### Basic CRUSH Access
```rust
use denc::{OSDMap, VersionedEncode};

let osdmap = OSDMap::decode_versioned(&mut bytes, features)?;

// Access the CRUSH map
if let Some(crush_map) = osdmap.get_crush_map() {
    println!("CRUSH map has {} buckets", crush_map.max_buckets);
    println!("CRUSH map has {} devices", crush_map.max_devices);
    println!("CRUSH map has {} rules", crush_map.max_rules);
}
```

### Pool and CRUSH Rule Access
```rust
// Get pool information
for (pool_id, _pool) in &osdmap.pools {
    if let Some(pool_name) = osdmap.get_pool_name(*pool_id) {
        println!("Pool {}: {}", pool_id, pool_name);
    }

    // Get the CRUSH rule for this pool
    if let Some(rule_id) = osdmap.get_pool_crush_rule(*pool_id) {
        if let Some(crush_map) = osdmap.get_crush_map() {
            if let Ok(rule) = crush_map.get_rule(rule_id as u32) {
                println!("  Uses CRUSH rule {}", rule.rule_id);
                println!("  Rule has {} steps", rule.steps.len());
            }
        }
    }
}
```

### Object Placement
```rust
use denc::PgId;

let pg = PgId { pool: 1, seed: 123 };

// Map PG to OSDs using CRUSH
match osdmap.pg_to_osds(&pg) {
    Ok(osds) => println!("PG maps to OSDs: {:?}", osds),
    Err(e) => eprintln!("Error: {:?}", e),
}

// Complete object placement: object name -> PG -> OSDs
match osdmap.object_to_osds(pool_id, "my_object") {
    Ok(osds) => println!("Object will be stored on OSDs: {:?}", osds),
    Err(e) => eprintln!("Error: {:?}", e),
}

// Just get the PG for an object
match osdmap.object_to_pg(pool_id, "my_object") {
    Ok(pg) => println!("Object maps to PG: {}", pg),
    Err(e) => eprintln!("Error: {:?}", e),
}
```

## Testing

### Integration Tests
The integration is tested in multiple test files:
- `tests/osdmap_crush_integration_test.rs` - Verifies CRUSH map parsing and helper methods
- `tests/object_placement_test.rs` - Tests complete object placement pipeline
- Tests validate:
  - CRUSH map parsing from corpus files
  - All helper methods
  - CRUSH rule access for pools
  - PG to OSD mapping
  - Object to PG mapping
  - Complete object to OSD pipeline
  - Deterministic mapping (same object always maps to same PG)

### Running Tests
```bash
# Run all OSDMap tests
cargo test --package denc --test osdmap_test
cargo test --package denc --test osdmap_crush_integration_test
cargo test --package denc --test object_placement_test

# Run with output to see details
cargo test --package denc --test object_placement_test -- --nocapture

# Run all denc tests
cargo test --package denc
```

## Implementation Status

### ✅ Completed Features

1. **CRUSH Map Integration**
   - Automatic parsing during OSDMap decode
   - Error handling for malformed CRUSH data
   - Access via `get_crush_map()`

2. **Pool Access Methods**
   - `get_pool()` - Get pool by ID
   - `get_pool_name()` - Get pool name
   - `get_pool_crush_rule()` - Get CRUSH rule for pool

3. **PG to OSD Mapping**
   - Full implementation using CRUSH mapper
   - Handles OSD weights
   - Supports all CRUSH rule operations
   - Returns ordered list of replica OSDs

4. **Object to PG Mapping**
   - Hashes object name to PG
   - Respects pool pg_num
   - Deterministic mapping

5. **Complete Object Placement**
   - End-to-end: object name -> PG -> OSDs
   - Single method call: `object_to_osds()`

## Future Enhancements

### Potential Improvements

1. **Advanced CRUSH Features**
   - Support for pg_upmap overrides
   - Support for pg_upmap_items
   - Respect primary_temp settings
2. **CRUSH Weight Updates**
   - Support for dynamic CRUSH weight updates
   - Rebalancing calculations

3. **Advanced Object Locators**
   - Support for namespace in object hashing
   - Support for key overrides
   - Custom hash functions

4. **Performance Optimizations**
   - Cache CRUSH mapping results
   - Batch object placement queries

## Compatibility

- All existing OSDMap decoding tests pass
- Backward compatible with code that doesn't need CRUSH data
- CRUSH parsing failures are non-fatal (map continues to work)
- No changes to the encoded format (CRUSH is still stored as bytes in the wire format)

## References

- CRUSH crate: `crates/crush/`
- CRUSH algorithm paper: https://ceph.com/wp-content/uploads/2016/08/weil-crush-sc06.pdf
- Ceph source: `~/dev/ceph/src/crush/`
- OSDMap implementation: `~/dev/ceph/src/osd/OSDMap.h`
