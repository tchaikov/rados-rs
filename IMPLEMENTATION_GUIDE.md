# Implementation Guide for Repository Rewrite

This document provides detailed, practical guidance for implementing the repository rewrite according to the commit sequence plan.

## Prerequisites

### Required Tools
- Rust 1.75.0 or later
- Git 2.30 or later
- Cargo
- Text editor with Rust support

### Reference Materials
- Original repository code (for copying implementation)
- Ceph source code (for reference)
- Ceph msgr2 protocol documentation (`ceph/doc/dev/msgr2.rst`)

## General Workflow for Each Commit

### 1. Setup
```bash
# Create a clean working directory
git checkout commit-rewrite
git pull origin commit-rewrite

# Ensure clean state
git status  # Should show nothing
```

### 2. Implementation
```bash
# Create necessary directories
mkdir -p crates/denc/src
mkdir -p crates/denc/tests

# Copy/create source files
# Edit files to implement just the features for this commit
```

### 3. Testing
```bash
# Run tests
cargo test -p <crate-name>

# Verify compilation
cargo build --all

# Check for warnings
cargo clippy --all -- -D warnings

# Format code
cargo fmt --all
```

### 4. Commit
```bash
# Add files
git add .

# Commit with descriptive message
git commit -m "Phase X.Y: Descriptive commit title

- Bullet point describing what was added
- Another bullet point for significant changes
- Tests: N tests added

Dependencies: Commit X, Y
Tests Passing: N/N"

# Verify commit
git log -1 --stat
```

### 5. Validation
```bash
# Ensure commit compiles
git show HEAD | grep "^diff"  # Review changes
cargo clean
cargo build --all

# Run all tests
cargo test --all
```

## Phase 1: Foundation - denc and denc-derive

### Commit 1: Project structure and denc trait definitions

**Goal**: Establish the workspace and core traits.

**Files to create**:
```
Cargo.toml                          # Workspace definition
crates/denc/Cargo.toml              # Denc crate manifest
crates/denc/src/lib.rs              # Main library file
crates/denc/src/traits.rs           # Denc and VersionedEncode traits
crates/denc/src/error.rs            # RadosError type
crates/denc/tests/trait_tests.rs   # Trait validation tests
```

**Cargo.toml** (workspace root for Commit 1):
```toml
[workspace]
members = [
    "crates/denc",
]
resolver = "2"

[workspace.package]
version = "0.1.0"
edition = "2021"
rust-version = "1.75"
authors = ["Your Name <email@example.com>"]
license = "MIT"

[workspace.dependencies]
bytes = "1.5"
thiserror = "1.0"
```

**Note**: Additional crates will be added to the `members` array in later commits:
- Commit 5: Add `"crates/denc-derive"`
- Commit 19: Add `"crates/auth"`
- Commit 23: Add `"crates/msgr2"`
- Commit 39: Add `"crates/crush"`
- Commit 45: Add `"crates/monclient"`
- Later: Add `"crates/rados"` for high-level API

**crates/denc/Cargo.toml**:
```toml
[package]
name = "denc"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
authors.workspace = true
license.workspace = true

[dependencies]
bytes = { workspace = true }
thiserror = { workspace = true }
```

**Key trait definition** (in `crates/denc/src/traits.rs`):
```rust
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;

/// Core encoding/decoding trait
pub trait Denc: Sized {
    /// Encode the value into a buffer
    fn encode<B: BufMut>(&self, buf: &mut B) -> io::Result<()>;
    
    /// Decode a value from a buffer
    fn decode<B: Buf>(buf: &mut B) -> io::Result<Self>;
    
    /// Get the encoded size (if known)
    fn encoded_size(&self) -> Option<usize> {
        None
    }
}

/// Marker trait for fixed-size types
pub trait FixedSize: Denc {
    const SIZE: usize;
}

/// Trait for versioned encoding
pub trait VersionedEncode: Denc {
    const VERSION: u8;
    const COMPAT_VERSION: u8;
}
```

**Commit Message**:
```
Phase 1.1: Project structure and denc trait definitions

- Create Cargo workspace with denc crate
- Define Denc trait for encoding/decoding
- Define FixedSize marker trait
- Define VersionedEncode trait
- Add RadosError type hierarchy
- Add trait validation tests

Tests: 2 tests added
Dependencies: None
Tests Passing: 2/2
```

### Commit 2: Primitive type encoding

**Goal**: Implement Denc for all primitive types.

**Files to modify/create**:
```
crates/denc/src/lib.rs              # Add mod primitives
crates/denc/src/primitives.rs       # Implement Denc for primitives
crates/denc/tests/primitive_tests.rs # Primitive encoding tests
```

**Example implementation** (in `crates/denc/src/primitives.rs`):
```rust
use crate::traits::{Denc, FixedSize};
use bytes::{Buf, BufMut};
use std::io;

// Implement for u8
impl Denc for u8 {
    fn encode<B: BufMut>(&self, buf: &mut B) -> io::Result<()> {
        buf.put_u8(*self);
        Ok(())
    }
    
    fn decode<B: Buf>(buf: &mut B) -> io::Result<Self> {
        if buf.remaining() < 1 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "buffer too short"));
        }
        Ok(buf.get_u8())
    }
    
    fn encoded_size(&self) -> Option<usize> {
        Some(Self::SIZE)
    }
}

impl FixedSize for u8 {
    const SIZE: usize = 1;
}

// Similar implementations for u16, u32, u64, i8, i16, i32, i64, bool, f32, f64
```

**Test example**:
```rust
#[test]
fn test_u32_encode_decode() {
    let mut buf = BytesMut::new();
    let value = 0x12345678u32;
    value.encode(&mut buf).unwrap();
    
    let mut buf = buf.freeze();
    let decoded = u32::decode(&mut buf).unwrap();
    assert_eq!(value, decoded);
}
```

**Commit Message**:
```
Phase 1.1: Primitive type encoding

- Implement Denc for integer types (u8-u64, i8-i64)
- Implement Denc for bool, f32, f64
- Add FixedSize implementations for all primitives
- Add comprehensive primitive encoding tests

Tests: 11 tests added
Dependencies: Commit 1
Tests Passing: 13/13 (2 from previous + 11 new)
```

### Commit 3: Basic collection types

**Files to modify/create**:
```
crates/denc/src/lib.rs              # Add mod collections
crates/denc/src/collections.rs      # String, Vec, Option implementations
crates/denc/tests/collection_tests.rs # Collection tests
```

**Example for Vec<T>**:
```rust
impl<T: Denc> Denc for Vec<T> {
    fn encode<B: BufMut>(&self, buf: &mut B) -> io::Result<()> {
        // Encode length as u32
        (self.len() as u32).encode(buf)?;
        
        // Encode each element
        for item in self {
            item.encode(buf)?;
        }
        Ok(())
    }
    
    fn decode<B: Buf>(buf: &mut B) -> io::Result<Self> {
        let len = u32::decode(buf)? as usize;
        let mut vec = Vec::with_capacity(len.min(1024)); // Cap allocation
        
        for _ in 0..len {
            vec.push(T::decode(buf)?);
        }
        Ok(vec)
    }
}
```

**Commit Message**:
```
Phase 1.1: Basic collection types

- Implement Denc for String (UTF-8 encoded)
- Implement Denc for Vec<T>
- Implement Denc for Option<T>
- Add tests for basic collections
- Add tests for nested collections (Vec<Vec<u32>>, etc.)

Tests: 20 tests added
Dependencies: Commit 2
Tests Passing: 33/33
```

## Commit Message Best Practices

### Format
```
Phase X.Y: Short descriptive title (50 chars max)

Detailed description of what this commit adds:
- Use bullet points for clarity
- Each point describes one significant change
- Keep it concise but informative

Implementation notes (optional):
- Algorithms used
- Design decisions
- Performance considerations

Tests: N tests added (M unit, K integration)
Dependencies: Commit A, Commit B
Tests Passing: X/X
```

### Examples of Good Commit Messages

**Good**:
```
Phase 1.2: Entity naming types

- Implement entity_name_t with type and ID
- Implement entity_addr_t with IP and port
- Implement entity_inst_t combining name and address
- Add entity type enumeration (MON, OSD, CLIENT, MDS, MGR)
- Add parsing from string format

Tests: 8 tests added
Dependencies: Commit 6 (utime_t)
Tests Passing: 65/65
```

**Bad**:
```
Add entity stuff

Added some types for entities.
```

## Testing Guidelines

### Unit Test Structure
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    
    #[test]
    fn test_basic_encode_decode() {
        // Arrange
        let original = MyType::new(/* ... */);
        
        // Act
        let mut buf = BytesMut::new();
        original.encode(&mut buf).unwrap();
        let mut buf = buf.freeze();
        let decoded = MyType::decode(&mut buf).unwrap();
        
        // Assert
        assert_eq!(original, decoded);
    }
    
    #[test]
    fn test_edge_cases() {
        // Test empty, maximum, minimum values
    }
    
    #[test]
    fn test_error_handling() {
        // Test buffer too short, invalid data, etc.
    }
}
```

### Integration Test Structure
```rust
// tests/integration_test.rs
use denc::Denc;
use bytes::BytesMut;

#[test]
fn test_complex_type_round_trip() {
    // Test realistic use case with multiple types
}
```

## Handling Common Issues

### Issue: Compilation Errors
```bash
# Get detailed error information
cargo build 2>&1 | tee build.log

# Check specific crate
cargo check -p denc

# Clear build cache if needed
cargo clean
```

### Issue: Test Failures
```bash
# Run specific test with output
cargo test test_name -- --nocapture

# Run tests in single thread for debugging
cargo test -- --test-threads=1

# Show full backtrace
RUST_BACKTRACE=full cargo test
```

### Issue: Circular Dependencies
- Review dependency graph
- Ensure lower-level crates don't depend on higher-level ones
- Move shared code to a common dependency

### Issue: Binary Encoding Mismatch
- Use hexdump to compare byte-by-byte
- Reference Ceph source code for encoding format
- Add debug logging to encoding functions

## Code Quality Checklist

Before committing, verify:

- [ ] `cargo build --all` - Builds successfully
- [ ] `cargo test --all` - All tests pass
- [ ] `cargo clippy --all` - No clippy warnings
- [ ] `cargo fmt --all` - Code is formatted
- [ ] `cargo doc --all` - Documentation builds
- [ ] Public APIs have doc comments
- [ ] Tests cover main use cases
- [ ] Error handling is comprehensive
- [ ] No `unwrap()` in production code (use `?` or proper error handling)

## Progress Tracking

Use a checklist to track progress:

```markdown
## Phase 1.1: Core Infrastructure
- [x] Commit 1: Traits and types
- [x] Commit 2: Primitive encoding
- [x] Commit 3: Basic collections
- [ ] Commit 4: Complex collections
- [ ] Commit 5: Derive macros

## Phase 1.2: Ceph-Specific Types
- [ ] Commit 6: Time and UUID
- [ ] Commit 7: Entity naming
...
```

## Time Management

Estimated time per commit:
- Simple commits (constants, basic types): 15-30 minutes
- Medium commits (complex types, algorithms): 1-2 hours
- Complex commits (derive macros, state machines): 3-5 hours
- Integration commits (corpus tests, end-to-end): 2-4 hours

## Getting Help

If stuck:
1. Review original implementation
2. Check Ceph source code
3. Consult Rust documentation
4. Add detailed comments for later review

## Final Validation

After all commits:
```bash
# Clean build
cargo clean
cargo build --all --release

# Full test suite
cargo test --all

# Linting
cargo clippy --all -- -D warnings

# Documentation
cargo doc --all --no-deps

# Check for outdated dependencies
cargo outdated

# Security audit
cargo audit
```

## Delivery

When complete:
1. Push all commits to branch
2. Create pull request targeting `commit-rewrite`
3. Add comprehensive PR description
4. Request review from maintainers
5. Address review feedback
6. Celebrate! 🎉
