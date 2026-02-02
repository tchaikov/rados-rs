## Ceph encoding

- 使用env ASAN_OPTIONS=detect_odr_violation=0,detect_leaks=0 CEPH_LIB=$HOME/dev/ceph/build/lib ~/dev/ceph/build/bin/ceph-dencoder type pg_pool_t import ~/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/pg_pool_t/453c7bee75dca4766602cee267caa589 decode dump_json可以以 json 格式 dump pg_pool_t 的内容
- ~/dev/ceph/src/include/denc.h 是 ceph 编码解码的实现
- ~/dev/ceph/ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects 下保存着很多 ceph 类型 encode 得到的二进制文件，我们把它们叫做 corpus
- 这个项目的目标是用 rust 实现 librados。
- 当本项目无法正常解码的时候，我们可以用 "cd ~/dev/ceph/build; bin/ceph-dencoder" 来分析 corpus 的内容，提供参考。
- OSDMap 的定义在 ~/dev/ceph/src/osd/OSDMap.h 和 ~/dev/ceph/src/osd/OSDMap.cc 里
- 我们在编码时当时采用的 features 可能会影响编码的结果，这是除了 Sized 的另外一种和 Denc 相关的编译期属性，我们也许也可以用 trait 表示，因为有的类型的编码会受到 features 影响，而有的不会。请注意，那些会被 features 影响的类型会使用 WRITE_CLASS_ENCODER_FEATURES 来对这种行为进行标注。

## local resources

- ceph's source code is located at ~/dev/ceph
- the official document for the messenger v2 protocol is located at ~/dev/ceph/doc/dev/msgr2.rst
- the msgr2 protocol is implemented in following places
  - ~/dev/ceph/src/crimson/net/ProtocolV2.cc
  - ~/dev/ceph/src/crimson/net/ProtocolV2.h
  - ~/dev/ceph/src/crimson/net/FrameAssemblerV2.cc
  - ~/dev/ceph/src/crimson/net/FrameAssemblerV2.h
  - ~/dev/ceph/src/msg/async/ProtocolV2.cc
  - ~/dev/ceph/src/msg/async/ProtocolV2.h
  - ~/dev/ceph/src/msg/async/frames_v2.cc
  - ~/dev/ceph/src/msg/async/frames_v2.h
  
## cluster for testing

- there is a cluster running locally. it's setting is located at `/home/kefu/dev/ceph/build/ceph.conf`, use `cd ~/dev/ceph/build; ../src/vstart.sh -d --without-dashboard` to start it, and use `cd ~/dev/ceph/build; ../src/stop.sh` to stop it
- please use `LD_PRELOAD=/usr/lib/libasan.so.8 /home/kefu/dev/ceph/build/bin/ceph --conf "/home/kefu/dev/ceph/build/ceph.conf" -s 2>/dev/null` to run the offical client to verify the behavior, or when we need to dump the TCP traffic to cross check with our own implementation`

## guidelines

- do not start from scratch.
- always fix existing code
- test encoding using dencoder in this project, and verify it using ceph-dencoder with the corpus fro ceph project

## Denc Coding Principles

### Always Use Denc Trait

- **NEVER** manually encode/decode primitive types with `get_u*_le()`/`put_u*_le()`
- **ALWAYS** use the Denc trait: `u32::decode(&mut buf, 0)?` and `value.encode(&mut buf, 0)?`
- **ALWAYS** use `Bytes::decode()` for length-prefixed data instead of manual length extraction

### Error Handling

- **Keep it simple**: Use `?` operator directly instead of `.map_err()` for each field
- If underlying types have proper Denc support, error handling should be minimal
- Only add custom error messages when providing additional context is truly helpful
- Let the Denc trait errors propagate naturally

### Constants and Shared Definitions

- **Define once, use everywhere**: Magic numbers, IVs, and constants should be defined in a single location
- Example: `CEPH_AES_IV` and `AUTH_ENC_MAGIC` are defined in `protocol.rs` and reused
- **NEVER** duplicate constant definitions across files

### Generic Wrappers for Common Patterns

- Create reusable generic types for common encoding patterns
- Example: `CephXEncryptedEnvelope<T>` wraps any `T: Denc` with encryption envelope
- Promotes code reuse and maintains consistency

### Function Signatures

- Use `Bytes` instead of `&[u8]` for encoded data when working with Denc
- Use `&mut Bytes` or `impl Buf` for decoding to leverage Denc trait directly
- Avoid converting between types unnecessarily

## Code Smells (Bad Patterns to Avoid)

### ❌ Manual Buffer Length Checking in High-Level Functions

```rust
// BAD: Manual length checking
if buf.remaining() < 4 {
    return Err(...);
}
let value = buf.get_u32_le();

// GOOD: Let Denc handle it
let value = u32::decode(&mut buf, 0)?;
```

### ❌ Manual Primitive Decoding

```rust
// BAD: Manual byte manipulation
let mut chunk_val = 0u64;
for (i, &byte) in chunk.iter().enumerate() {
    chunk_val |= (byte as u64) << (i * 8);
}

// GOOD: Use Denc
let chunk_val = u64::decode(&mut buf, 0)?;
```

### ❌ Manual Length Prefix Encoding

```rust
// BAD: Manual length + data
buf.put_u32_le(data.len() as u32);
buf.extend_from_slice(&data);

// GOOD: Use Bytes::encode()
let bytes = Bytes::from(data);
bytes.encode(&mut buf, 0)?;
```

### ❌ Duplicate Constants

```rust
// BAD: Defined in multiple places
const CEPH_AES_IV: &[u8; 16] = b"cephsageyudagreg";  // In file A
const CEPH_AES_IV: &[u8; 16] = b"cephsageyudagreg";  // In file B

// GOOD: Define once, import everywhere
use crate::protocol::CEPH_AES_IV;
```

### ❌ Manual Struct Encoding When Denc Could Be Used

```rust
// BAD: Manual field-by-field encoding
buf.put_u8(1);  // struct_v
buf.put_u64_le(magic);
payload.encode(&mut buf, 0)?;

// GOOD: Implement Denc for a wrapper struct
let envelope = CephXEncryptedEnvelope { payload };
envelope.encode(&mut buf, 0)?;
```

### ❌ Custom Error Handling for Each Field in Denc Implementations

```rust
// BAD: Wrapping every field's error with .map_err()
fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
    let field1 = u32::decode(buf, 0).map_err(|e| {
        RadosError::Protocol(format!("Failed to decode field1: {}", e))
    })?;
    let field2 = String::decode(buf, 0).map_err(|e| {
        RadosError::Protocol(format!("Failed to decode field2: {}", e))
    })?;
    Ok(Self { field1, field2 })
}

// GOOD: Just use ? operator and let Denc's error handling propagate
fn decode<B: Buf>(buf: &mut B, features: u64) -> Result<Self, RadosError> {
    let field1 = u32::decode(buf, 0)?;
    let field2 = String::decode(buf, 0)?;
    Ok(Self { field1, field2 })
}
```

The Denc trait already provides descriptive error messages. Adding custom error
handling for each field creates noise and doesn't add value. The stack trace
will show which decode() call failed.

### When to Investigate for Denc Usage

If you see any of these patterns, consider refactoring to use Denc:

1. Manual `if buf.remaining() < N` checks
2. Manual `get_u*_le()` or `put_u*_le()` calls
3. Manual byte slicing and length prefix handling
4. Repeated encoding patterns across functions
5. Duplicate constant definitions
6. Custom error handling for every primitive field

### Exceptions

- Helper functions in `crush` crate use manual decoding to avoid circular dependency
- Low-level AES encryption/decryption may need raw bytes
- Test code may use manual encoding for specific test scenarios
