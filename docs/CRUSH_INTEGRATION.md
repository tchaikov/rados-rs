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

### Object Placement (Placeholder)
```rust
use denc::PgId;

let pg = PgId { pool: 1, seed: 123 };

// This is a placeholder - full implementation coming soon
match osdmap.pg_to_osds(&pg) {
    Ok(osds) => println!("PG maps to OSDs: {:?}", osds),
    Err(e) => eprintln!("Error: {:?}", e),
}
```

## Testing

### Integration Tests
The integration is tested in `tests/osdmap_crush_integration_test.rs`:
- Verifies CRUSH map parsing from corpus files
- Tests all helper methods
- Validates CRUSH rule access for pools

### Running Tests
```bash
# Run all OSDMap tests
cargo test --package denc --test osdmap_test
cargo test --package denc --test osdmap_crush_integration_test

# Run with output to see details
cargo test --package denc --test osdmap_crush_integration_test -- --nocapture
```

## Future Work

### 1. Complete PG to OSD Mapping
The `pg_to_osds()` method is currently a placeholder. Full implementation will:
- Use the CRUSH mapper from the `crush` crate
- Calculate the actual OSD set for a given PG
- Handle CRUSH tunables and bucket weights
- Support pg_upmap and other overrides

### 2. Object Locator
Implement object name to PG mapping:
```rust
pub fn object_to_pg(&self, pool_id: i64, object_name: &str) -> Result<PgId, RadosError>
```

### 3. Full Object Placement
Complete end-to-end object placement:
```rust
pub fn object_to_osds(&self, pool_id: i64, object_name: &str) -> Result<Vec<i32>, RadosError>
```

### 4. CRUSH Weight Updates
Support for dynamic CRUSH weight updates and rebalancing calculations.

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
