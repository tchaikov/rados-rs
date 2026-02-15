---
applyTo: "crates/denc/**/*.rs"
---

## DENC Implementation Guidelines

When working with DENC (encoding/decoding) types, follow these guidelines:

### Implementing Denc Trait

The `Denc` trait is the core abstraction for encoding and decoding Ceph types:

```rust
pub trait Denc: Sized {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError>;
    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError>;
}
```

### When to Use ZeroCopyDencode

Use `#[derive(ZeroCopyDencode)]` for POD (Plain Old Data) types where:
- ALL fields are primitives or other ZeroCopyDencode types
- No variable-length fields (no `String`, `Vec`, `Bytes`)
- No version headers in wire format
- Fields encode/decode in declaration order

```rust
#[derive(Debug, Clone, PartialEq, Eq, denc::ZeroCopyDencode)]
#[repr(C, packed)]
pub struct EntityName {
    pub entity_type: u8,
    pub num: u64,
}
```

### Manual Denc Implementation

For types with variable-length fields or version headers:

```rust
impl Denc for MyComplexType {
    fn encode<B: BufMut>(&self, buf: &mut B, features: u64) -> Result<(), RadosError> {
        // Encode version header if needed
        buf.put_u8(STRUCT_VERSION);
        
        // Encode fields using their Denc implementations
        self.field1.encode(buf, features)?;
        self.field2.encode(buf, features)?;
        
        Ok(())
    }
    
    fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
        // Decode version header
        let version = buf.get_u8();
        
        // Decode fields
        let field1 = Type1::decode(buf, features)?;
        let field2 = Type2::decode(buf, features)?;
        
        Ok(Self { field1, field2 })
    }
}
```

### Testing Encoding/Decoding

ALWAYS include roundtrip tests:

```rust
#[test]
fn test_roundtrip() {
    let original = MyType { /* ... */ };
    let mut buf = BytesMut::new();
    
    // Encode
    original.encode(&mut buf, 0).unwrap();
    
    // Decode
    let mut read_buf = buf.freeze();
    let decoded = MyType::decode(&mut read_buf, 0).unwrap();
    
    // Verify
    assert_eq!(original, decoded);
    assert_eq!(read_buf.remaining(), 0, "should consume all bytes");
}
```

### Cross-Validation with Ceph

When implementing encoding for Ceph types:

1. **Reference Implementation**: Check `$HOME/dev/ceph/src/include/denc.h` or the specific type's `.h` file
2. **Use ceph-dencoder**: Validate against corpus files
   ```bash
   cd $HOME/dev/ceph/build
   bin/ceph-dencoder type OSDMap import /path/to/corpus/file decode dump_json
   ```
3. **Feature Flags**: Be aware that encoding may differ based on negotiated features

### Common Patterns

- **Versioned Encoding**: Use `ENCODE_START` / `ENCODE_FINISH` pattern for forward compatibility
- **Optional Fields**: Encode presence flag before optional data
- **Collections**: Encode length prefix before elements
- **Bytes**: Use `Bytes` type which handles length prefixing automatically

### Error Handling

- Use `?` operator to propagate Denc errors
- Don't add custom `.map_err()` unless providing crucial context
- Let Denc's error handling propagate naturally

### Field Naming

Prefer simple, clear names:
- ✅ `sec`, `nsec` (simple)
- ❌ `tv_sec`, `tv_nsec` (unnecessarily verbose)

Use custom `Serialize` implementations for JSON compatibility when needed.
