# Dependency Graph and Build Order

This document provides a detailed view of the dependencies between crates and commits in the repository rewrite.

## Crate Dependency Graph

```
┌─────────────────────────────────────────────────────────────────┐
│                         Workspace Root                           │
└─────────────────────────────────────────────────────────────────┘
                                │
                ┌───────────────┼───────────────┐
                │               │               │
                ▼               ▼               ▼
        ┌───────────┐   ┌──────────────┐   ┌─────────┐
        │   denc    │   │ denc-derive  │   │  auth   │
        └───────────┘   └──────────────┘   └─────────┘
                │               │               │
                └───────────────┼───────────────┤
                                │               │
                                ▼               ▼
                        ┌───────────────────────────┐
                        │        msgr2              │
                        └───────────────────────────┘
                                │
                ┌───────────────┼───────────────┐
                │               │               │
                ▼               ▼               ▼
        ┌───────────┐   ┌───────────┐   ┌──────────┐
        │   crush   │   │ monclient │   │  rados   │
        └───────────┘   └───────────┘   └──────────┘
                                │
                                ▼
                        ┌───────────────┐
                        │   Examples    │
                        └───────────────┘
```

## Build Order

The crates must be built in the following order:

1. **denc** - Foundation encoding/decoding
2. **denc-derive** - Procedural macros (depends on denc)
3. **auth** - Authentication (depends on denc)
4. **msgr2** - Messaging protocol (depends on denc, auth)
5. **crush** - CRUSH algorithm (depends on denc)
6. **monclient** - Monitor client (depends on msgr2, crush, denc)
7. **rados** - High-level API (depends on monclient)

## Commit Dependencies by Phase

### Phase 1: denc Foundation

```
Commit 1 (Traits & Types)
    │
    ▼
Commit 2 (Primitives)
    │
    ▼
Commit 3 (Basic Collections)
    │
    ▼
Commit 4 (Complex Collections)
    │
    ▼
Commit 5 (Derive Macros)
    │
    ▼
Commits 6-10 (Ceph Types) ──┬──▶ Commit 11 (Object IDs)
                            │       │
                            │       ▼
                            │   Commit 12 (Pool/PG Types)
                            │       │
                            │       ▼
                            │   Commits 13-14 (CRUSH Types)
                            │       │
                            │       ▼
                            └──▶ Commits 15-17 (OSD/Map Types)
                                    │
                                    ▼
                                Commit 18 (Corpus Tests)
```

### Phase 2: auth Crate

```
Phase 1 Complete
    │
    ▼
Commit 19 (Auth Constants)
    │
    ▼
Commit 20 (Session Keys)
    │
    ▼
Commit 21 (Ticket Handling)
    │
    ▼
Commit 22 (Auth Helpers)
```

### Phase 3: msgr2 Crate

```
Phase 1 & 2 Complete
    │
    ▼
Commit 23 (Frame Types)
    │
    ▼
Commit 24 (Control Frames) ──┐
    │                         │
    ▼                         │
Commit 25 (Message Frames)    │
    │                         │
    ▼                         │
Commit 26 (Compression)       │
    │                         │
    ▼                         │
Commit 27 (Encryption) ◀──────┘
    │
    ▼
Commit 28 (Connection States)
    │
    ▼
Commit 29-33 (State Machine)
    │
    ▼
Commit 34-37 (Message Routing)
    │
    ▼
Commit 38 (Integration Tests)
```

### Phase 4: crush Crate

```
Phase 1 Complete (Commits 13-14, 17)
    │
    ▼
Commit 39 (CRUSH Map)
    │
    ▼
Commit 40 (Choose Functions)
    │
    ▼
Commit 41 (PG Mapping)
    │
    ▼
Commit 42 (Straw2)
    │
    ▼
Commit 43 (Map Updates)
    │
    ▼
Commit 44 (Test Suite)
```

### Phase 5: monclient Crate

```
Phases 1, 3, 4 Complete
    │
    ▼
Commit 45 (MonClient Structure)
    │
    ▼
Commit 46 (Map Subscription)
    │
    ▼
Commit 47 (Command Handling)
    │
    ▼
Commit 48 (Monitor Failover)
    │
    ▼
Commit 49 (End-to-end Integration)
```

## External Dependencies

### Rust Crates

#### denc crate
- `bytes` - Efficient byte buffer handling
- `thiserror` - Error handling

#### denc-derive crate
- `syn` - Parsing Rust code
- `quote` - Generating Rust code
- `proc-macro2` - Procedural macro support

#### auth crate
- `denc` (internal)
- `aes-gcm` - AES encryption
- `sha2` - SHA-256 hashing
- `hmac` - HMAC authentication

#### msgr2 crate
- `denc` (internal)
- `auth` (internal)
- `tokio` - Async runtime
- `tokio-util` - Codec utilities
- `bytes` - Byte buffer handling
- `flate2` - Zlib compression
- `lz4` - LZ4 compression
- `snap` - Snappy compression

#### crush crate
- `denc` (internal)
- `rand` - Random number generation

#### monclient crate
- `msgr2` (internal)
- `crush` (internal)
- `denc` (internal)
- `tokio` - Async runtime

## Critical Path Analysis

### Longest Dependency Chain

```
Commit 1 (Traits)
  → Commit 2 (Primitives)
  → Commit 3 (Collections)
  → Commit 4 (Complex Collections)
  → Commit 5 (Derive Macros)
  → Commit 6 (Time/UUID)
  → Commit 11 (Object IDs)
  → Commit 12 (Pool/PG)
  → Commit 17 (OSDMap)
  → Commit 39 (CRUSH Map)
  → Commit 40 (Choose Functions)
  → Commit 41 (PG Mapping)
  → Commit 42 (Straw2)
  → Commit 43 (Map Updates)
  → Commit 46 (Map Subscription)
  → Commit 49 (Integration)
```

This is the critical path with **16 commits** in sequence.

### Parallelizable Work

Some commits can be worked on in parallel if multiple developers are involved:

**After Commit 5 (Derive Macros):**
- Commits 6-10 (Ceph types) - Parallel
- Commits 13-14 (CRUSH types) - Parallel (independent)
- Commits 15-16 (OSD/Mon types) - Parallel (independent)

**After Phase 1:**
- Commits 19-22 (auth crate) - Parallel to Phase 3.1
- Commits 23-27 (msgr2 foundation) - Parallel to auth

**After Commit 27:**
- Commits 39-44 (crush crate) - Parallel to commits 28-38 (msgr2 state machine)

## Dependency Verification

Each commit should verify its dependencies are met:

```bash
# Example dependency check for Commit 20
cargo tree -p auth --depth 1 | grep denc  # Should show denc dependency
cargo test -p denc                        # All denc tests must pass
```

## Breaking Changes

If any dependency needs to change its public API, the following commits are affected:

### denc API changes affect:
- All subsequent commits (2-49)
- Critical: Update all derived implementations

### auth API changes affect:
- Commits 24, 27, 30 (msgr2 auth integration)
- Commit 45 (monclient auth)

### msgr2 API changes affect:
- Commit 45 (monclient connection)
- Commit 49 (integration tests)

### crush API changes affect:
- Commit 46 (monclient map subscription)
- Commit 49 (integration tests)

## Version Compatibility

### Minimum Rust Version
- **1.75.0** - Required for latest proc-macro features
- Async/await support (stable since 1.39)
- GATs (Generic Associated Types) - stable since 1.65

### Ceph Compatibility
- Target: Ceph 16.x (Pacific) and later
- Messenger protocol v2 (introduced in Nautilus 14.x)
- CephX authentication (all versions)

## Cargo Workspace Structure

```toml
[workspace]
members = [
    "crates/denc",
    "crates/denc-derive",
    "crates/auth",
    "crates/msgr2",
    "crates/crush",
    "crates/monclient",
    "crates/rados",
]

[workspace.package]
version = "0.1.0"
edition = "2021"
rust-version = "1.75"
license = "MIT"
```

## Testing Dependencies

### Unit Tests
- No external dependencies
- Use standard `#[test]` attribute

### Integration Tests
- May require `test-log` for better logging
- May require `serial_test` for sequential execution

### Corpus Tests (Commit 18)
- Requires binary test data files
- Files should be in `tests/corpus/` directory
- Generated from actual Ceph cluster

### Performance Tests
- `criterion` - Benchmarking framework
- Added in later commits after functionality is stable

## Documentation Dependencies

### API Documentation
- Generated with `cargo doc`
- Requires all dependencies to be documented

### Examples
- Located in `examples/` directory
- Depend on public APIs from all crates
- Should compile with `cargo build --examples`

## CI/CD Dependencies

### GitHub Actions Workflow
- Rust stable toolchain
- Rust nightly (for miri tests, optional)
- `cargo clippy` - Linting
- `cargo fmt` - Formatting check
- `cargo test` - All tests
- `cargo audit` - Security vulnerabilities

### Required for CI (Commit 49)
```yaml
dependencies:
  - actions/checkout@v4
  - dtolnay/rust-toolchain@stable
  - Swatinem/rust-cache@v2
```

## Dependency Lock Strategy

### Cargo.lock
- **Committed to repository** for reproducible builds
- Updated with `cargo update` when needed
- Pinned versions for security-sensitive dependencies

### Version Constraints
```toml
[dependencies]
# Exact version for critical dependencies
aes-gcm = "=0.10.3"

# Compatible version for stable APIs
tokio = "1.35"

# Flexible for dev dependencies
criterion = "0.5"
```

## Risk Mitigation for Dependencies

### Security
- Run `cargo audit` after each commit
- Monitor security advisories for external crates
- Use `cargo deny` to check licenses

### Supply Chain
- Verify checksums of dependencies
- Use only well-maintained crates with active development
- Prefer crates from established authors/organizations

### Stability
- Avoid pre-1.0 dependencies when possible
- Pin versions for critical functionality
- Test with both minimum and latest dependency versions
