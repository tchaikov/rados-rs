# Denc Performance Optimization Proposal

## Problem Statement

Currently, `Denc::encode()` returns `Result<Bytes, RadosError>`, which causes performance issues:

1. **Memory Fragmentation**: Each field of a composite type is encoded to its own `Bytes` object
2. **Repeated Allocations**: Multiple `BytesMut` allocations for each field
3. **Memcpy Overhead**: Each field's bytes are copied via `extend_from_slice()` into the parent buffer

Example of current inefficiency:
```rust
// Current approach - each field allocates and copies
impl Denc for MyStruct {
    fn encode(&self, features: u64) -> Result<Bytes, RadosError> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&self.field1.encode(features)?); // Alloc + copy
        buf.extend_from_slice(&self.field2.encode(features)?); // Alloc + copy
        buf.extend_from_slice(&self.field3.encode(features)?); // Alloc + copy
        Ok(buf.freeze())
    }
}
```

## Proposed Solution

### Design Goals

1. **Zero-allocation path**: When total size is known, allocate once and encode directly
2. **Incremental path**: When size is unknown, grow buffer efficiently with size hints
3. **Unified iteration**: Share field iteration logic across size calculation, encoding, and decoding
4. **Backward compatibility**: Keep existing `Denc` trait for gradual migration

### Architecture

#### 1. New Trait: `DencMut` (Buffer-based encoding)

```rust
/// Efficient encoding trait that writes directly to a mutable buffer
pub trait DencMut: Sized {
    /// Does this type use versioned encoding?
    const USES_VERSIONING: bool = false;

    /// Does encoding format depend on feature flags?
    const FEATURE_DEPENDENT: bool = false;

    /// Encode directly into a mutable buffer
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError>;

    /// Decode from a buffer
    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError>;

    /// Calculate encoded size (returns None if size depends on runtime data)
    fn encoded_size(&self, features: u64) -> Option<usize>;
}
```

#### 2. Marker Trait: `FixedSize`

```rust
/// Marker trait for types with compile-time known size
pub trait FixedSize: DencMut {
    /// The fixed size in bytes
    const SIZE: usize;
}

// Automatically implement for primitive types
impl FixedSize for u8 { const SIZE: usize = 1; }
impl FixedSize for u16 { const SIZE: usize = 2; }
impl FixedSize for u32 { const SIZE: usize = 4; }
impl FixedSize for u64 { const SIZE: usize = 8; }
// etc.
```

#### 3. Procedural Macro: `#[derive(DencMut)]`

The macro generates three functions from field iteration:

```rust
#[derive(DencMut)]
struct MyStruct {
    field1: u32,        // FixedSize
    field2: u64,        // FixedSize
    field3: Vec<u8>,    // Variable size
}

// Generated code:
impl DencMut for MyStruct {
    fn encoded_size(&self, features: u64) -> Option<usize> {
        let mut size = 0;
        size += 4;  // field1: u32
        size += 8;  // field2: u64

        // field3 is Vec<u8>, size depends on runtime
        let field3_size = self.field3.encoded_size(features)?;
        size += field3_size;

        Some(size)
    }

    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        // Preallocate if possible
        if let Some(total_size) = self.encoded_size(features) {
            if buf.remaining_mut() < total_size {
                return Err(RadosError::InsufficientSpace {
                    required: total_size,
                    available: buf.remaining_mut()
                });
            }
        }

        // Encode fields directly into buffer
        self.field1.encode(buf, features)?;
        self.field2.encode(buf, features)?;
        self.field3.encode(buf, features)?;

        Ok(())
    }

    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        Ok(Self {
            field1: u32::decode(buf, features)?,
            field2: u64::decode(buf, features)?,
            field3: Vec::<u8>::decode(buf, features)?,
        })
    }
}
```

#### 4. Optimization Strategies

**Case 1: All fields are FixedSize**
```rust
#[derive(DencMut)]
struct FixedStruct {
    a: u32,
    b: u64,
    c: u16,
}

// Generated: FixedSize is automatically implemented
impl FixedSize for FixedStruct {
    const SIZE: usize = 4 + 8 + 2; // = 14
}

// encoded_size() returns Some(14) - no runtime calculation needed
```

**Case 2: Mix of fixed and variable size**
```rust
#[derive(DencMut)]
struct MixedStruct {
    header: u32,        // FixedSize
    data: Vec<u8>,      // Variable
    footer: u64,        // FixedSize
}

// encoded_size() calculates: 4 + data.len() + 4 (vec length) + 8
// Still efficient - only need to check vec length, no encoding
```

**Case 3: Nested structs**
```rust
#[derive(DencMut)]
struct Outer {
    inner1: FixedStruct,    // FixedSize
    inner2: MixedStruct,    // Variable
}

// Recursively calculates size by calling inner.encoded_size()
```

### Migration Path

1. **Phase 1**: Implement `DencMut` trait and derive macro
2. **Phase 2**: Implement `DencMut` for primitive types
3. **Phase 3**: Add bridge implementations:
   ```rust
   // Bridge: DencMut -> old Denc (for backward compatibility)
   impl<T: DencMut> Denc for T {
       fn encode(&self, features: u64) -> Result<Bytes, RadosError> {
           let size = self.encoded_size(features).unwrap_or(1024);
           let mut buf = BytesMut::with_capacity(size);
           DencMut::encode(self, &mut buf, features)?;
           Ok(buf.freeze())
       }

       fn decode(bytes: &mut Bytes) -> Result<Self, RadosError> {
           DencMut::decode(bytes, 0)
       }
   }
   ```
4. **Phase 4**: Migrate existing types to use `#[derive(DencMut)]`
5. **Phase 5**: Deprecate old `Denc` trait (optional, for long-term)

### Integration with Existing Code

The new system integrates well with existing `zerocopy` module:

```rust
// ZeroCopyDencode types automatically get DencMut
impl<T: ZeroCopyDencode> DencMut for T {
    fn encode<B: BufMut>(&self, buf: &mut B, _features: u64) -> Result<(), RadosError> {
        zerocopy::Encode::encode(self, buf).map_err(|e| RadosError::Denc(e.to_string()))
    }

    fn decode<B: Buf>(buf: &mut B, _features: u64) -> Result<Self, RadosError> {
        zerocopy::Decode::decode(buf).map_err(|e| RadosError::Denc(e.to_string()))
    }

    fn encoded_size(&self, _features: u64) -> Option<usize> {
        Some(Encode::encoded_size(self))
    }
}

// If T: ZeroCopyDencode + FixedSize, it's automatically FixedSize
```

## Performance Benefits

### Before (Current)
```rust
// Encoding a struct with 10 fields:
// - 10 BytesMut allocations
// - 10 encode() calls returning Bytes
// - 10 extend_from_slice() memcpy operations
// - 1 final BytesMut allocation for parent
// Total: 11 allocations, 10 memcpy operations
```

### After (Optimized)
```rust
// Case 1: All FixedSize fields
// - 1 BytesMut allocation with exact size
// - Direct writes to buffer
// Total: 1 allocation, 0 memcpy operations

// Case 2: Mixed fixed/variable
// - 1 BytesMut allocation with calculated size
// - Direct writes to buffer
// Total: 1 allocation, 0 memcpy operations (if size calculation is accurate)
```

### Benchmark Expectations

For a typical composite type with 10 fields:
- **Memory allocations**: 11 → 1 (90% reduction)
- **Memcpy operations**: 10 → 0 (100% reduction)
- **Encoding time**: Expected 50-70% reduction
- **Memory fragmentation**: Significantly reduced

## Implementation Checklist

- [ ] Define `DencMut` trait
- [ ] Define `FixedSize` marker trait
- [ ] Implement `DencMut` for primitive types (u8, u16, u32, u64, i32, i64, bool)
- [ ] Implement `DencMut` for `Vec<T>`, `Bytes`, `String`
- [ ] Create `#[derive(DencMut)]` procedural macro
  - [ ] Handle field iteration
  - [ ] Generate `encoded_size()` with optimization for FixedSize fields
  - [ ] Generate `encode()` with preallocation
  - [ ] Generate `decode()`
  - [ ] Auto-implement `FixedSize` when all fields are FixedSize
- [ ] Add bridge implementations for backward compatibility
- [ ] Integrate with `zerocopy` module
- [ ] Write comprehensive tests
- [ ] Write benchmarks comparing old vs new approach
- [ ] Migrate existing types (PgPool, EntityAddr, etc.)
- [ ] Update documentation

## Open Questions

1. **Versioning**: How should `DencMut` handle versioned encoding (ENCODE_START/DECODE_START)?
   - Option A: Separate trait `VersionedDencMut`
   - Option B: Add version parameters to `encode`/`decode`

2. **Feature-dependent encoding**: Should we have separate methods or use the features parameter?
   - Current proposal: Use features parameter in all methods

3. **Error handling**: Should we use `RadosError` or create a new error type?
   - Current proposal: Keep `RadosError` for consistency

4. **Naming**: Is `DencMut` the best name?
   - Alternatives: `DencBuf`, `DencDirect`, `DencEfficient`
   - Current choice: `DencMut` (emphasizes mutable buffer)

## Example Usage

```rust
use denc::{DencMut, FixedSize};

#[derive(DencMut)]
struct OsdMapHeader {
    epoch: u32,           // FixedSize
    fsid: [u8; 16],       // FixedSize
    created: u64,         // FixedSize
    modified: u64,        // FixedSize
}

// Automatically implements FixedSize with SIZE = 36

fn encode_osdmap_header(header: &OsdMapHeader) -> Result<Bytes, RadosError> {
    // Single allocation with exact size
    let mut buf = BytesMut::with_capacity(OsdMapHeader::SIZE);
    header.encode(&mut buf, 0)?;
    Ok(buf.freeze())
}
```

## Conclusion

This design provides:
1. **Significant performance improvements** through reduced allocations and memcpy
2. **Backward compatibility** via bridge implementations
3. **Gradual migration path** - can adopt incrementally
4. **Type safety** - compile-time size checking where possible
5. **Ergonomic API** - derive macro handles complexity

The key insight is that by moving from `encode() -> Bytes` to `encode(&mut BufMut)`, we enable the caller to control allocation strategy, which is essential for performance in serialization-heavy workloads.
