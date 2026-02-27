# Simplification Plan: Remove Pre-Nautilus (v14) Support

## Goal

Simplify the rados-rs codebase by removing support for Ceph versions older than Nautilus (v14, released March 2019). This will eliminate conditional decoding logic for legacy formats while maintaining full compatibility with all actively supported Ceph releases.

## Rationale

1. **Nautilus (v14) is 5+ years old** - Released March 2019, well past EOL
2. **All modern Ceph deployments use Nautilus or later** - Pacific (v16), Quincy (v17), Reef (v18), Squid (v19)
3. **Significant code complexity reduction** - Remove ~200-300 lines of conditional logic
4. **Clearer implementation** - Decode functions become straightforward field reads
5. **Better performance** - Fewer branches in hot paths
6. **Easier maintenance** - Less legacy code to understand and maintain

## Scope

### Types to Simplify (7 types)

1. **ObjectLocator** - Remove v2 and v5 conditionals
2. **HObject** - Remove v1, v2, v4 conditionals
3. **MonInfo** - Remove v2, v4, v5, v6 conditionals
4. **MonMap** - Remove v1-v5 legacy format support
5. **AuthTicket** - Remove v2 conditional
6. **CephXAuthenticate** - Remove v2 conditional
7. **ObjectStatSum** - Already v14 minimum, document clearly

### Types to Keep As-Is

- **All Level 0-1 types** - Already simple, no version conditionals
- **Types with post-Nautilus conditionals** - Keep v16-20 logic for ObjectStatSum, v7-9 for MonMap
- **Feature-dependent encoding** - Keep MSG_ADDR2, OSDENC feature handling

## Implementation Plan

### Phase 1: Add Minimum Version Constants

**Goal**: Define minimum supported versions for each type

**Files to modify**:
- `crates/crush/src/placement.rs`
- `crates/denc/src/hobject.rs`
- `crates/denc/src/monmap.rs`
- `crates/auth/src/types.rs`
- `crates/auth/src/protocol.rs`

**Changes**:
```rust
// Add constants at the top of each impl block
impl VersionedEncode for ObjectLocator {
    /// Minimum supported version (Nautilus v14+)
    const MIN_SUPPORTED_VERSION: u8 = 5;

    fn encoding_version(&self, _features: u64) -> u8 {
        6
    }

    fn compat_version(&self, _features: u64) -> u8 {
        if self.hash != -1 { 6 } else { 3 }
    }

    fn decode_content<B: Buf>(
        buf: &mut B,
        features: u64,
        struct_v: u8,
        _compat_version: u8,
    ) -> Result<Self, RadosError> {
        // Add version check
        if struct_v < Self::MIN_SUPPORTED_VERSION {
            return Err(RadosError::Protocol(format!(
                "ObjectLocator version {} not supported (requires Ceph Nautilus v14+, minimum version {})",
                struct_v, Self::MIN_SUPPORTED_VERSION
            )));
        }

        // Simplified decoding (no conditionals)
        // ...
    }
}
```

**Estimated effort**: 1-2 hours

---

### Phase 2: Simplify ObjectLocator

**File**: `crates/crush/src/placement.rs`

**Current code** (lines 226-276):
- Handles v1 (old pool format)
- Handles v2-4 (no namespace)
- Handles v5 (namespace added)
- Handles v6 (hash added)

**Changes**:

1. Add minimum version constant: `MIN_SUPPORTED_VERSION = 5`
2. Add version check at start of `decode_content`
3. Remove `if struct_v < 2` branch (old pool format)
4. Remove `if struct_v >= 5` conditional for namespace
5. Keep `if struct_v >= 6` conditional for hash (v6 is post-Nautilus)

**Before**:
```rust
let pool_id = if struct_v < 2 {
    // Old format: int32_t pool, int16_t preferred
    let op = i32::decode(buf, features)?;
    let _pref = i16::decode(buf, features)?;
    op as u64
} else {
    // New format: int64_t pool, int32_t preferred
    let pool = i64::decode(buf, features)?;
    let _preferred = i32::decode(buf, features)?;
    pool as u64
};

let namespace = if struct_v >= 5 {
    String::decode(buf, features)?
} else {
    String::new()
};

let hash = if struct_v >= 6 {
    i64::decode(buf, features)?
} else {
    -1i64
};
```

**After**:
```rust
// Version check
if struct_v < 5 {
    return Err(RadosError::Protocol(format!(
        "ObjectLocator version {} not supported (requires Nautilus v14+)",
        struct_v
    )));
}

// Always use new format (v5+)
let pool = i64::decode(buf, features)?;
let _preferred = i32::decode(buf, features)?;
let pool_id = pool as u64;

// Always decode namespace (v5+)
let namespace = String::decode(buf, features)?;

// Hash added in v6 (post-Nautilus, keep conditional)
let hash = if struct_v >= 6 {
    i64::decode(buf, features)?
} else {
    -1i64
};
```

**Lines removed**: ~15 lines
**Estimated effort**: 30 minutes

---

### Phase 3: Simplify HObject

**File**: `crates/denc/src/hobject.rs`

**Current code** (lines 196-248):
- Handles v0 (no key field)
- Handles v1 (key added)
- Handles v2 (max added)
- Handles v3 (no nspace/pool)
- Handles v4 (nspace/pool added)

**Changes**:

1. Add minimum version constant: `MIN_SUPPORTED_VERSION = 4`
2. Add version check at start of `decode_content`
3. Remove all conditionals for v1, v2, v4
4. Always decode all fields

**Before**:
```rust
let key = if version >= 1 {
    <String as Denc>::decode(buf, features)?
} else {
    String::new()
};

let oid = <String as Denc>::decode(buf, features)?;
let snapid = <u64 as Denc>::decode(buf, features)?;
let hash = <u32 as Denc>::decode(buf, features)?;

let max = if version >= 2 {
    <bool as Denc>::decode(buf, features)?
} else {
    false
};

let (nspace, pool) = if version >= 4 {
    let ns = <String as Denc>::decode(buf, features)?;
    let mut p = <i64 as Denc>::decode(buf, features)?;

    // Compatibility fix for hammer
    if p == -1 && snapid == 0 && hash == 0 && !max && oid.is_empty() {
        p = i64::MIN;
    }

    (ns, p as u64)
} else {
    (String::new(), u64::MAX)
};
```

**After**:
```rust
// Version check
if version < 4 {
    return Err(RadosError::Protocol(format!(
        "HObject version {} not supported (requires Nautilus v14+)",
        version
    )));
}

// Always decode all fields (v4+)
let key = String::decode(buf, features)?;
let oid = String::decode(buf, features)?;
let snapid = u64::decode(buf, features)?;
let hash = u32::decode(buf, features)?;
let max = bool::decode(buf, features)?;
let nspace = String::decode(buf, features)?;
let mut pool = i64::decode(buf, features)?;

// Keep Hammer compatibility fix
if pool == -1 && snapid == 0 && hash == 0 && !max && oid.is_empty() {
    pool = i64::MIN;
}

let pool = pool as u64;
```

**Lines removed**: ~20 lines
**Estimated effort**: 30 minutes

---

### Phase 4: Simplify MonInfo

**File**: `crates/denc/src/monmap.rs`

**Current code** (lines 278-353):
- Handles v1 (no priority)
- Handles v2 (priority added)
- Handles v3 (no weight)
- Handles v4 (weight added)
- Handles v5 (crush_loc added)
- Handles v6 (time_added added)

**Changes**:

1. Add minimum version constant: `MIN_SUPPORTED_VERSION = 6`
2. Add version check at start of `decode_content`
3. Remove all conditionals for v2, v4, v5, v6
4. Always decode all fields
5. Update `encoded_size_content` to remove conditionals

**Before**:
```rust
let priority = if version >= 2 {
    <u16 as Denc>::decode(buf, features)?
} else {
    0
};

let weight = if version >= 4 {
    <u16 as Denc>::decode(buf, features)?
} else {
    0
};

let crush_loc = if version >= 5 {
    <BTreeMap<String, String> as Denc>::decode(buf, features)?
} else {
    BTreeMap::new()
};

let time_added = if version >= 6 {
    <UTime as Denc>::decode(buf, features)?
} else {
    UTime::default()
};
```

**After**:
```rust
// Version check
if version < 6 {
    return Err(RadosError::Protocol(format!(
        "MonInfo version {} not supported (requires Nautilus v14+)",
        version
    )));
}

// Always decode all fields (v6+)
let name = String::decode(buf, features)?;
let public_addrs = EntityAddrvec::decode(buf, features)?;
let priority = u16::decode(buf, features)?;
let weight = u16::decode(buf, features)?;
let crush_loc = BTreeMap::<String, String>::decode(buf, features)?;
let time_added = UTime::decode(buf, features)?;
```

**Also update `encoded_size_content`**:
```rust
fn encoded_size_content(&self, features: u64, _version: u8) -> Option<usize> {
    // Always encode all fields (v6+)
    Some(
        self.name.encoded_size(features)?
            + self.public_addrs.encoded_size(features)?
            + 2  // priority (u16)
            + 2  // weight (u16)
            + self.crush_loc.encoded_size(features)?
            + self.time_added.encoded_size(features)?
    )
}
```

**Lines removed**: ~30 lines
**Estimated effort**: 45 minutes

---

### Phase 5: Simplify MonMap

**File**: `crates/denc/src/monmap.rs`

**Current code** (lines 492-650):
- Handles v1 (legacy entity_inst_t format - already rejected)
- Handles v2-v5 (legacy mon_addr map format)
- Handles v6+ (modern format with mon_info and ranks)
- Complex conversion logic from legacy to modern format

**Changes**:

1. Update minimum version constant: `MIN_SUPPORTED_VERSION = 6`
2. Simplify version check to reject v < 6
3. Remove all legacy format handling (v2-v5)
4. Remove `legacy_mon_addr` variable and conversion logic
5. Always decode modern format

**Before** (simplified view):
```rust
if version == 1 {
    return Err(RadosError::Protocol(
        "MonMap decoding version 1 not supported".to_string(),
    ));
}

let mut legacy_mon_addr: BTreeMap<String, EntityAddr> = BTreeMap::new();

if version < 6 {
    // v2-v5: decode map<string, entity_addr_t>
    legacy_mon_addr = <BTreeMap<String, EntityAddr> as Denc>::decode(buf, features)?;
}

// Decode mon_info
let mon_info = if version < 5 {
    // Build mon_info from legacy_mon_addr
    let mut info_map = BTreeMap::new();
    for (name, addr) in legacy_mon_addr.iter() {
        let mut addrvec = EntityAddrvec::new();
        addrvec.addrs.push(addr.clone());
        info_map.insert(
            name.clone(),
            MonInfo {
                name: name.clone(),
                public_addrs: addrvec,
                priority: 0,
                weight: 0,
                crush_loc: BTreeMap::new(),
                time_added: UTime::default(),
            },
        );
    }
    info_map
} else {
    <BTreeMap<String, MonInfo> as Denc>::decode(buf, features)?
};

let ranks = if version >= 6 {
    <Vec<String> as Denc>::decode(buf, features)?
} else {
    // Build ranks from legacy_mon_addr keys
    legacy_mon_addr.keys().cloned().collect()
};
```

**After**:
```rust
// Version check - only support v6+
if version < 6 {
    return Err(RadosError::Protocol(format!(
        "MonMap version {} not supported (requires Nautilus v14+, which uses v6+)",
        version
    )));
}

// Always decode modern format (v6+)
let mon_info = BTreeMap::<String, MonInfo>::decode(buf, features)?;
let ranks = Vec::<String>::decode(buf, features)?;

// Continue with v7+ fields (keep these conditionals)
let min_mon_release = if version >= 7 {
    MonCephRelease::decode(buf, features)?
} else {
    MonCephRelease::Unknown
};

// ... rest of v7-v9 fields
```

**Lines removed**: ~60 lines (largest simplification)
**Estimated effort**: 1 hour

---

### Phase 6: Simplify AuthTicket

**File**: `crates/auth/src/types.rs`

**Current code** (lines 369-408):
- Handles v1 (no old_auid)
- Handles v2 (old_auid added)

**Changes**:

1. Add minimum version check in decode
2. Remove conditional for old_auid
3. Always decode old_auid field

**Before**:
```rust
fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
    let struct_v = u8::decode(buf, features)?;
    let name = EntityName::decode(buf, features)?;
    let global_id = u64::decode(buf, features)?;

    let old_auid = if struct_v >= 2 {
        u64::decode(buf, features)?
    } else {
        u64::MAX
    };

    let created = SystemTime::decode(buf, features)?;
    let expires = SystemTime::decode(buf, features)?;
    let caps = AuthCapsInfo::decode(buf, features)?;
    let flags = u32::decode(buf, features)?;

    Ok(Self { ... })
}
```

**After**:
```rust
fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
    let struct_v = u8::decode(buf, features)?;

    // Version check
    if struct_v < 2 {
        return Err(RadosError::Protocol(format!(
            "AuthTicket version {} not supported (requires Nautilus v14+)",
            struct_v
        )));
    }

    let name = EntityName::decode(buf, features)?;
    let global_id = u64::decode(buf, features)?;
    let old_auid = u64::decode(buf, features)?;
    let created = SystemTime::decode(buf, features)?;
    let expires = SystemTime::decode(buf, features)?;
    let caps = AuthCapsInfo::decode(buf, features)?;
    let flags = u32::decode(buf, features)?;

    Ok(Self { ... })
}
```

**Lines removed**: ~5 lines
**Estimated effort**: 15 minutes

---

### Phase 7: Simplify CephXAuthenticate

**File**: `crates/auth/src/protocol.rs`

**Current code** (lines 330-359):
- Handles v1 (no other_keys)
- Handles v2 (other_keys added)

**Changes**:

1. Add minimum version check in decode
2. Remove conditional for other_keys
3. Always decode other_keys field

**Before**:
```rust
fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
    let struct_v = u8::decode(buf, features)?;
    let client_challenge = u64::decode(buf, features)?;
    let key = u64::decode(buf, features)?;
    let old_ticket = CephXTicketBlob::decode(buf, features)?;

    let other_keys = if struct_v >= 2 {
        u32::decode(buf, 0)?
    } else {
        0
    };

    Ok(Self { ... })
}
```

**After**:
```rust
fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
    let struct_v = u8::decode(buf, features)?;

    // Version check
    if struct_v < 2 {
        return Err(RadosError::Protocol(format!(
            "CephXAuthenticate version {} not supported (requires Luminous v12+)",
            struct_v
        )));
    }

    let client_challenge = u64::decode(buf, features)?;
    let key = u64::decode(buf, features)?;
    let old_ticket = CephXTicketBlob::decode(buf, features)?;
    let other_keys = u32::decode(buf, 0)?;

    Ok(Self { ... })
}
```

**Lines removed**: ~5 lines
**Estimated effort**: 15 minutes

---

### Phase 8: Update Documentation

**Files to update**:
1. `COMPATIBILITY.md` - Add "Simplification" section
2. `README.md` - Document minimum Ceph version requirement
3. `CLAUDE.md` - Update development guide
4. Type-level documentation comments

**Changes**:

1. **COMPATIBILITY.md**: Add section at the top:
```markdown
## Minimum Supported Version

**rados-rs requires Ceph Nautilus (v14) or later.**

Released in March 2019, Nautilus is the minimum version for full compatibility.
Support for older versions (Luminous v12, Mimic v13) has been removed to
simplify the codebase and reduce maintenance burden.

### Rationale

- Nautilus is 5+ years old and well past EOL
- All modern Ceph deployments use Nautilus or later
- Removing pre-Nautilus support eliminated ~200-300 lines of conditional logic
- Simpler code is easier to maintain and has better performance
```

2. **README.md**: Add requirements section:
```markdown
## Requirements

- **Ceph**: Nautilus (v14) or later
  - Tested with: Nautilus (v14), Octopus (v15), Pacific (v16), Quincy (v17), Reef (v18), Squid (v19)
- **Rust**: 1.70 or later
```

3. **CLAUDE.md**: Update "Known Limitations" section:
```markdown
### Minimum Ceph Version

rados-rs requires **Ceph Nautilus (v14) or later**. Older versions are not supported:

- ❌ Luminous (v12) - Not supported
- ❌ Mimic (v13) - Not supported
- ✅ Nautilus (v14) - Minimum supported version
- ✅ Octopus (v15) and later - Fully supported

When decoding versioned types, versions older than the minimum will return
a `RadosError::Protocol` error with a clear message.
```

4. **Type documentation**: Add doc comments to each simplified type:
```rust
/// ObjectLocator - Object placement information
///
/// ## Version Support
/// - **Encoding**: v6 (current)
/// - **Decoding**: v5+ (Nautilus v14+)
/// - **Minimum Ceph**: Nautilus (v14)
///
/// Versions < 5 are not supported and will return an error.
```

**Estimated effort**: 1 hour

---

### Phase 9: Add Integration Tests

**Goal**: Verify that version checks work correctly

**File**: Create `crates/denc/tests/version_checks.rs`

**Test cases**:

1. Test that decoding with minimum version succeeds
2. Test that decoding with version < minimum fails with clear error
3. Test that error messages mention "Nautilus v14+"

**Example test**:
```rust
#[test]
fn test_object_locator_rejects_old_versions() {
    use bytes::BytesMut;
    use crush::ObjectLocator;
    use denc::{Denc, VersionedEncode};

    // Manually construct a v4 ObjectLocator (pre-namespace)
    let mut buf = BytesMut::new();
    buf.put_u8(4); // version
    buf.put_u8(3); // compat_version
    buf.put_u32_le(20); // length placeholder

    // Encode v4 format (no namespace field)
    buf.put_i64_le(1); // pool
    buf.put_i32_le(-1); // preferred
    buf.put_u32_le(0); // key length

    // Try to decode - should fail
    let result = ObjectLocator::decode(&mut buf.as_ref(), 0);
    assert!(result.is_err());

    let err = result.unwrap_err();
    assert!(err.to_string().contains("Nautilus"));
    assert!(err.to_string().contains("version 4"));
}

#[test]
fn test_object_locator_accepts_minimum_version() {
    use bytes::BytesMut;
    use crush::ObjectLocator;
    use denc::{Denc, VersionedEncode};

    // Construct a v5 ObjectLocator (with namespace, no hash)
    let mut buf = BytesMut::new();
    buf.put_u8(5); // version
    buf.put_u8(3); // compat_version
    buf.put_u32_le(24); // length

    // Encode v5 format
    buf.put_i64_le(1); // pool
    buf.put_i32_le(-1); // preferred
    buf.put_u32_le(0); // key length (empty string)
    buf.put_u32_le(0); // namespace length (empty string)

    // Should decode successfully
    let result = ObjectLocator::decode(&mut buf.as_ref(), 0);
    assert!(result.is_ok());

    let locator = result.unwrap();
    assert_eq!(locator.pool_id, 1);
    assert_eq!(locator.namespace, "");
    assert_eq!(locator.hash, -1);
}
```

**Similar tests for**:
- HObject (v4 minimum)
- MonInfo (v6 minimum)
- MonMap (v6 minimum)
- AuthTicket (v2 minimum)
- CephXAuthenticate (v2 minimum)

**Estimated effort**: 2 hours

---

### Phase 10: Update Corpus Tests

**Goal**: Ensure corpus tests still pass after simplification

**Files to check**:
- `crates/denc/tests/corpus_test.rs`
- Any type-specific corpus tests

**Changes**:

1. Verify all corpus files use versions >= minimum
2. If any corpus files use old versions, either:
   - Skip those specific test cases with a comment
   - Remove them entirely if they're pre-Nautilus

**Example**:
```rust
#[test]
#[ignore]
fn test_corpus_object_locator() {
    // Iterate through corpus files
    for entry in corpus_files("object_locator_t") {
        let data = std::fs::read(&entry.path).unwrap();

        // Check version before attempting decode
        let version = data[0];
        if version < 5 {
            eprintln!("Skipping {} (version {} < 5, pre-Nautilus)",
                     entry.path.display(), version);
            continue;
        }

        // Decode and verify
        let result = ObjectLocator::decode(&mut &data[..], 0);
        assert!(result.is_ok(), "Failed to decode {}", entry.path.display());
    }
}
```

**Estimated effort**: 1 hour

---

## Summary

### Total Changes

| Phase | Type | Lines Removed | Effort |
|-------|------|---------------|--------|
| 1 | Constants | +30 | 1-2 hours |
| 2 | ObjectLocator | -15 | 30 min |
| 3 | HObject | -20 | 30 min |
| 4 | MonInfo | -30 | 45 min |
| 5 | MonMap | -60 | 1 hour |
| 6 | AuthTicket | -5 | 15 min |
| 7 | CephXAuthenticate | -5 | 15 min |
| 8 | Documentation | +50 | 1 hour |
| 9 | Tests | +150 | 2 hours |
| 10 | Corpus | ~0 | 1 hour |
| **Total** | | **-135 net** | **8-9 hours** |

### Benefits

1. **Code Simplification**: Remove ~200-300 lines of conditional logic
2. **Performance**: Fewer branches in decode hot paths
3. **Maintainability**: Less legacy code to understand
4. **Clarity**: Decode functions become straightforward field reads
5. **Safety**: Explicit version checks with clear error messages

### Risks & Mitigation

**Risk**: Breaking existing deployments using pre-Nautilus Ceph
- **Mitigation**: Document minimum version clearly in README and error messages
- **Impact**: Low - Nautilus is 5+ years old, unlikely anyone is using older versions

**Risk**: Corpus tests may fail if they include pre-Nautilus data
- **Mitigation**: Skip or remove pre-Nautilus corpus test cases
- **Impact**: Low - corpus data is primarily for validation, not functionality

**Risk**: Integration tests against old clusters may fail
- **Mitigation**: Update CI to only test against Nautilus+ clusters
- **Impact**: Low - we don't currently test against pre-Nautilus clusters

### Testing Strategy

1. **Unit tests**: Verify version checks reject old versions
2. **Integration tests**: Test against Nautilus, Pacific, Quincy, Reef, Squid clusters
3. **Corpus tests**: Verify all supported corpus files decode correctly
4. **Regression tests**: Run full test suite to ensure no breakage

### Rollout Plan

1. **Phase 1-7**: Implement simplifications (4-5 hours)
2. **Phase 8**: Update documentation (1 hour)
3. **Phase 9-10**: Add tests and verify (3 hours)
4. **Review**: Code review and testing (1-2 hours)
5. **Commit**: Single commit with clear message explaining changes

### Commit Message Template

```
Simplify implementation by requiring Ceph Nautilus (v14) minimum

Remove support for Ceph versions older than Nautilus (v14, March 2019)
to simplify the codebase and reduce maintenance burden.

Changes:
- ObjectLocator: Remove v2 and v5 conditional decoding
- HObject: Remove v1, v2, v4 conditional decoding
- MonInfo: Remove v2, v4, v5, v6 conditional decoding
- MonMap: Remove v1-v5 legacy format support
- AuthTicket: Remove v2 conditional decoding
- CephXAuthenticate: Remove v2 conditional decoding

All types now reject versions below their minimum with clear error
messages mentioning "Nautilus v14+" requirement.

Benefits:
- Removed ~200-300 lines of conditional logic
- Simpler decode functions (straightforward field reads)
- Better performance (fewer branches)
- Easier maintenance (less legacy code)

Testing:
- Added version check tests for all simplified types
- Verified corpus tests still pass
- Updated documentation to reflect minimum version

Nautilus (v14) is 5+ years old and well past EOL. All modern Ceph
deployments use Nautilus or later (Pacific v16, Quincy v17, Reef v18,
Squid v19).
```

## Next Steps

1. Review this plan with stakeholders
2. Get approval to proceed
3. Create a feature branch: `simplify-pre-nautilus-removal`
4. Implement phases 1-10
5. Submit PR for review
6. Merge after approval

## Questions for Review

1. Is Nautilus (v14) an acceptable minimum version?
2. Should we keep any pre-Nautilus support for specific types?
3. Are there any deployment scenarios we're not considering?
4. Should this be a major version bump (breaking change)?
