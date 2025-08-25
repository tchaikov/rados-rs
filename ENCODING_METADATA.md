# Encoding Metadata System

## Overview

We've implemented a compile-time trait system to declaratively mark types with their encoding properties. This matches Ceph's C++ encoding patterns and allows tools to inspect encoding behavior at compile time.

## Compile-Time Properties

Every Ceph encodable type has two inherent properties:

1. **USES_VERSIONING**: Does this type use `ENCODE_START`/`DECODE_START` wrapping?
   - **C++ equivalent**: Types using `ENCODE_START(version, compat, bl)` macros
   - **Rust**: Types implementing `VersionedEncode` trait

2. **FEATURE_DEPENDENT**: Does the encoding format change based on feature flags?
   - **C++ equivalent**: Types marked with `WRITE_CLASS_ENCODER_FEATURES`
   - **Rust**: Types with `const FEATURE_DEPENDENT: bool = true`

## Type Categories

### Simple Types
- No versioning metadata
- Encoding doesn't depend on features
- **Examples**: `PgId`, `EVersion`, `UTime`, `UuidD`, `OsdInfo`
- **C++ pattern**: Direct field encoding without `ENCODE_START`

```rust
mark_simple_encoding!(PgId);
mark_simple_encoding!(UTime);
```

### Versioned Types
- Uses `ENCODE_START`/`DECODE_START` wrapping
- Encoding format is fixed (doesn't depend on features)
- **Examples**: `PoolSnapInfo`, `PgMergeMeta`
- **C++ pattern**: `ENCODE_START(2, 2, bl)` with fixed version

```rust
mark_versioned_encoding!(PoolSnapInfo);
mark_versioned_encoding!(PgMergeMeta);
```

### Feature-Dependent Types
- Uses versioned encoding AND encoding changes based on features
- **Examples**: `PgPool`, `EntityAddr`, `OsdXInfo`
- **C++ pattern**: `WRITE_CLASS_ENCODER_FEATURES` annotation
- Version/format depends on feature flags

```rust
mark_feature_dependent_encoding!(PgPool);
mark_feature_dependent_encoding!(EntityAddr);
```

## Usage Examples

### Marking a New Type

```rust
// In osdmap.rs after implementing the type

// Simple type
mark_simple_encoding!(MySimpleType);

// Versioned type (fixed version)
impl VersionedEncode for MyVersionedType {
    fn encoding_version(&self, _features: u64) -> u8 { 2 }
    fn compat_version(&self, _features: u64) -> u8 { 1 }
    // ... encode_content/decode_content ...
}
mark_versioned_encoding!(MyVersionedType);

// Feature-dependent type
impl VersionedEncode for MyFeatureType {
    const FEATURE_DEPENDENT: bool = true;  // Mark as feature-dependent

    fn encoding_version(&self, features: u64) -> u8 {
        if (features & SOME_FEATURE) != 0 {
            3  // New version with feature
        } else {
            2  // Old version without feature
        }
    }
    // ...
}
mark_feature_dependent_encoding!(MyFeatureType);
```

### Querying Metadata at Runtime

```rust
use denc::encoding_metadata::HasEncodingMetadata;

let meta = PgPool::encoding_metadata();
println!("Uses versioning: {}", meta.uses_versioning);
println!("Feature dependent: {}", meta.feature_dependent);
```

## Type Registry

All types are registered with encoding metadata:

```rust
// Level 1: Simple types
mark_simple_encoding!(PgId);
mark_simple_encoding!(EVersion);
mark_simple_encoding!(UTime);
mark_simple_encoding!(UuidD);
mark_simple_encoding!(OsdInfo);

// Level 2: Versioned (not feature-dependent)
mark_versioned_encoding!(PoolSnapInfo);
mark_versioned_encoding!(PgMergeMeta);

// Level 2/3: Feature-dependent
mark_feature_dependent_encoding!(EntityAddr);    // MSG_ADDR2 feature
mark_feature_dependent_encoding!(OsdXInfo);      // SERVER_OCTOPUS feature
mark_feature_dependent_encoding!(PgPool);        // Multiple features
```

## Cross-Validation with C++ dencoder

Use the `cross_validate_dencoder.sh` script to verify Rust implementation matches C++ behavior:

```bash
# Run full cross-validation
./cross_validate_dencoder.sh

# Compare specific type JSON output
diff -u /tmp/dencoder_cross_validation/cpp_pool_snap_info_t_*.json \
        /tmp/dencoder_cross_validation/rust_pool_snap_info_t_*.json
```

The script tests:
1. **Decode correctness**: Both Rust and C++ decode successfully
2. **JSON structure**: Field values match (field order may differ)
3. **Round-trip encoding**: Decode → Encode → Decode produces same result
4. **Interoperability**: C++ can decode Rust-encoded data

## Expected Differences in JSON Output

### Field Order
- **Rust**: JSON fields in struct definition order
- **C++**: JSON fields in alphabetical or custom order
- ✅ **Acceptable**: Field order doesn't affect correctness

### Timestamp Format
- **Rust**: `{"sec": 1727592736, "nsec": 628101458}`
- **C++**: `"2024-09-29T14:52:16.628101+0800"`
- ✅ **Acceptable**: Both represent the same timestamp

### What Must Match
- ✅ Field names
- ✅ Numeric values (exact match)
- ✅ String values (exact match)
- ✅ Nested structure (same hierarchy)
- ✅ Array lengths and element values

## Testing Strategy

1. **Test by dependency level**: Level 1 → Level 2 → Level 3
2. **Verify zero remaining bytes**: All corpus files must decode completely
3. **Cross-validate with C++**: Compare JSON outputs for all corpus files
4. **Test feature variants**: For feature-dependent types, test with different feature flags
5. **Round-trip validation**: Encode → Decode must be identity operation

## Implementation Checklist

When adding a new encodable type:

- [ ] Implement `Denc` or `VersionedEncode` trait
- [ ] Mark with appropriate encoding metadata macro
- [ ] Add to `dencoder.rs` type registry
- [ ] Update `list_types()` in dencoder
- [ ] Add to `test_dencoder.sh` at appropriate level
- [ ] Test with all available corpus files
- [ ] Cross-validate JSON output with C++ dencoder
- [ ] Verify round-trip encoding works
- [ ] Document any feature dependencies

## Benefits of This System

1. **Compile-time safety**: Encoding properties are known at compile time
2. **Self-documenting**: Macros clearly mark encoding behavior
3. **Tool support**: Dencoder can query and display encoding properties
4. **Testing guidance**: Clear which types need feature testing
5. **C++ compatibility**: Directly maps to C++ encoding patterns
6. **Progressive validation**: Test simple types before complex ones
