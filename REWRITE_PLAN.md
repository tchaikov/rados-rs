# RADOS-RS Repository Rewrite Plan

This document outlines the detailed commit sequence for restructuring the rados-rs repository to support external review and long-term maintainability.

## Objectives

1. **Modular commits**: Each commit should be self-contained, compilable, and focused on a single feature or logical unit
2. **Bottom-up approach**: Build from foundational components to high-level functionality
3. **Testable increments**: Each commit should include relevant unit tests
4. **Reviewable history**: Clear, logical progression that reviewers can follow

## Branch Strategy

- **Rewrite Branch**: `copilot/plan-commit-sequence-rewrite`
- All rewrite work will be done on this dedicated branch
- Each phase will be committed sequentially with clear commit messages

## Commit Sequence Plan

### Phase 1: Foundation - denc and denc-derive crates

The encoding/decoding layer is the foundation of the entire stack. All other crates depend on it.

#### 1.1 Core Infrastructure (3-5 commits)
1. **Denc traits and core types**
   - `Denc` trait definition
   - `DencMut` trait for mutable encoding
   - `VersionedEncode` trait for versioned types
   - Basic error types
   - **Tests**: Trait implementation validation

2. **Primitive type encoding**
   - Integer types (u8, u16, u32, u64, i8, i16, i32, i64)
   - Boolean encoding
   - Floating point types
   - **Tests**: Roundtrip tests for all primitives

3. **Collection type encoding**
   - String encoding (with length prefix)
   - Vec<T> encoding
   - Option<T> encoding
   - **Tests**: Roundtrip tests for collections

4. **Complex collection types**
   - HashMap/BTreeMap encoding
   - Tuple encoding
   - Array encoding
   - **Tests**: Nested collection roundtrip tests

5. **denc-derive procedural macros**
   - `#[derive(Denc)]` basic implementation
   - Support for simple structs
   - Support for enums
   - **Tests**: Macro expansion tests

#### 1.2 Ceph-Specific Types (6-8 commits)
6. **Corpus validation infrastructure** (CRITICAL - DO THIS EARLY)
   - Add `corpus_comparison_test.rs` from main branch
   - Add GitHub CI workflow for corpus testing (already exists in `.github/workflows/ci.yml`)
   - Set up dencoder binary for corpus validation
   - Initial test with any simple type that has corpus (e.g., `pg_t`)
   - **Why early**: This establishes the validation gatekeeper before implementing complex types
   - **Tests**: Verify CI can clone corpus and run ceph-dencoder

7. **Entity types**
   - `EntityName` (client, mon, osd, mds, mgr)
   - Entity type parsing and encoding
   - **Tests**: Entity name parsing and roundtrip

8. **Entity addressing**
   - `EntityAddr` structure
   - `EntityAddrType` enum
   - Socket address handling
   - **Corpus validation**: Add `entity_addr_t` to corpus_comparison_test.rs
   - **Tests**: Address parsing and encoding with MSG_ADDR2 feature flag

9. **Time types**
   - `UTime` (Ceph's utime_t)
   - Time encoding/decoding
   - **Corpus validation**: Add `utime_t` to corpus_comparison_test.rs
   - **Tests**: Time roundtrip tests

10. **Feature negotiation**
   - `FeatureSet` implementation
   - Feature flag constants
   - Feature compatibility checks
   - **Tests**: Feature set operations

11. **Placement Group types**
    - `PgId` (pg_t) encoding
    - `Eversion` (eversion_t)
    - **Corpus validation**: Add `pg_t` and `eversion_t` to corpus_comparison_test.rs
    - **Tests**: PG ID calculation and encoding

12. **PG merge metadata**
    - `PgMergeMeta` structure
    - Version-aware encoding
    - **Corpus validation**: Add `pg_merge_meta_t` to corpus_comparison_test.rs
    - **Add existing tests**: `pg_merge_meta_test.rs`, `pg_merge_meta_validation.rs`
    - **Tests**: Roundtrip with corpus validation

13. **Pool configuration**
    - `PgPool` (pg_pool_t) with versioned encoding (v1-v29)
    - Hit set parameters
    - Pool options
    - **Corpus validation**: Add `pg_pool_t` to corpus_comparison_test.rs (marked as exception for computed fields)
    - **Tests**: Roundtrip tests across different versions

14. **MonMap types**
    - `MonInfo` structure
    - `MonMap` structure
    - Election strategy types
    - **Corpus validation**: Add `mon_info_t` and `MonMap` to corpus_comparison_test.rs (marked as exceptions for field name differences)
    - **Tests**: MonMap encoding/decoding

#### 1.3 OSD and CRUSH Types (4-6 commits)
15. **OSD info types**
    - `OsdInfo` structure
    - `OsdXinfo` structure
    - **Corpus validation**: Add `osd_info_t` and `osd_xinfo_t` to corpus_comparison_test.rs
    - **Tests**: OSD info roundtrip

16. **OSDMap structure**
    - Basic OSDMap fields
    - Pool management in OSDMap
    - **Corpus validation**: Add `OSDMap` to corpus_comparison_test.rs if corpus available
    - **Add existing tests**: `osdmap_test.rs`
    - **Tests**: OSDMap decoding from corpus

17. **CRUSH map types**
    - CRUSH bucket types
    - CRUSH rule structure
    - **Tests**: CRUSH structure validation

18. **CRUSH map integration**
    - Full CrushWrapper implementation
    - CRUSH map decoding
    - **Corpus validation**: Add `CrushWrapper` to corpus_comparison_test.rs if corpus available
    - **Add existing tests**: `osdmap_crush_integration_test.rs`, `object_placement_test.rs`
    - **Tests**: CRUSH map corpus validation

### Phase 2: Authentication - auth crate

Authentication must be complete before implementing the messenger protocol.

#### 2.1 Core Authentication (3-4 commits)
19. **Keyring management**
    - Keyring file parsing
    - Key storage and retrieval
    - **Tests**: Keyring parsing from file

20. **CephX types**
    - `CephXSessionKey`
    - `CephXServiceTicket`
    - `CephXTicketBlob`
    - **Corpus validation**: Add auth types to corpus_comparison_test.rs if corpus available
    - **Tests**: Type encoding/decoding

21. **CephX protocol messages**
    - `CephXRequest` types
    - `CephXReply` types
    - Message encoding/decoding
    - **Tests**: Message roundtrip tests

22. **CephX authentication handler**
    - Client-side authentication flow
    - Challenge-response implementation
    - Session key derivation
    - **Tests**: Authentication handshake simulation

### Phase 3: Messaging - msgr2 crate

The messenger protocol enables communication with Ceph components.

#### 3.1 Frame Protocol (4-5 commits)
23. **Frame types**
    - Frame header structure
    - Tag definitions
    - Segment structure
    - **Tests**: Frame header parsing

24. **Frame encoding**
    - Frame serialization
    - CRC calculation
    - **Tests**: Frame encoding roundtrip

25. **Frame decoding**
    - Frame deserialization
    - Frame assembly from segments
    - **Tests**: Multi-segment frame assembly

26. **Frame encryption**
    - AES-128-GCM integration
    - Encrypted frame handling
    - **Tests**: Encrypted frame roundtrip

27. **Frame compression**
    - Compression support (when available)
    - Compressed frame handling
    - **Tests**: Compression roundtrip

#### 3.2 Protocol State Machine (5-7 commits)
28. **Connection state types**
    - State enum definition
    - State transition rules
    - **Tests**: State machine validation

29. **Banner exchange**
    - Banner frame handling
    - Protocol version negotiation
    - **Tests**: Banner exchange simulation

30. **Hello exchange**
    - Hello frame structure
    - Entity type exchange
    - **Tests**: Hello frame encoding/decoding

31. **Authentication negotiation**
    - Auth method selection
    - Auth frame handling
    - Integration with auth crate
    - **Tests**: Auth negotiation flow

32. **Auth signature**
    - Signature frame handling
    - Session establishment
    - **Tests**: Signature verification

33. **Session establishment**
    - Session ready state
    - Connection configuration
    - **Tests**: Full connection handshake

34. **Keepalive handling**
    - Keepalive frame sending
    - Keepalive ACK handling
    - Connection health monitoring
    - **Tests**: Keepalive timeout scenarios

#### 3.3 Message Handling (3-4 commits)
35. **Message structure**
    - Message header
    - Message payload
    - Message encoding
    - **Tests**: Message serialization

36. **Message routing**
    - Message dispatcher
    - Handler registration
    - **Tests**: Message routing logic

37. **Message compression and encryption**
    - Per-message security options
    - **Add existing tests**: `ceph_config.rs`, `connection_tests.rs`
    - **Tests**: Secure message handling

38. **Connection pooling**
    - Multiple concurrent connections
    - Connection lifecycle management
    - **Tests**: Connection pool operations

### Phase 4: CRUSH Algorithm - crush crate

CRUSH is required for object placement calculations.

#### 4.1 CRUSH Implementation (5-6 commits)
39. **Hash functions**
    - CRUSH hash algorithms
    - **Tests**: Hash function validation

40. **Bucket algorithms**
    - Uniform bucket
    - List bucket
    - Tree bucket
    - Straw2 bucket
    - **Tests**: Bucket selection algorithms

41. **CRUSH choose algorithm**
    - Choose_firstn implementation
    - Choose_indep implementation
    - **Tests**: Object distribution validation

42. **CRUSH rule evaluation**
    - Rule parsing
    - Rule execution
    - **Tests**: Rule application tests

43. **Object placement**
    - Object → PG mapping
    - PG → OSD mapping via CRUSH
    - **Tests**: End-to-end placement tests

44. **Integration with OSDMap**
    - OSDMap + CRUSH integration
    - Pool-specific placement rules
    - **Tests**: Placement with real OSDMap data

### Phase 5: Monitor Client - monclient crate

The monitor client is the final piece enabling cluster interaction.

#### 5.1 Monitor Communication (4-5 commits)
45. **Monitor discovery**
    - MonMap retrieval
    - Monitor connection logic
    - **Tests**: Monitor discovery simulation

46. **Monitor messages**
    - Monitor command structure
    - Response parsing
    - **Tests**: Command encoding/decoding

47. **OSDMap retrieval**
    - OSDMap request/response
    - OSDMap version tracking
    - **Tests**: OSDMap fetch simulation

48. **Map update handling**
    - Automatic map updates
    - Version comparison
    - **Tests**: Map update detection

49. **Monitor client integration**
    - Complete monitor client API
    - Connection management
    - **Tests**: Full monitor client workflow

## Validation Strategy

### Quality Gates - Preserve All Existing Tests
**CRITICAL**: All existing tests must be preserved when the rewrite is finished. They serve as quality gates ensuring correctness.

**Existing test infrastructure to preserve:**
- **Unit tests**: All `#[test]` functions in `src/` files
- **Integration tests**:
  - `crates/denc/tests/corpus_comparison_test.rs` - **GATEKEEPER for all dencoders**
  - `crates/denc/tests/object_placement_test.rs`
  - `crates/denc/tests/osdmap_crush_integration_test.rs`
  - `crates/denc/tests/osdmap_test.rs`
  - `crates/denc/tests/pg_merge_meta_test.rs`
  - `crates/denc/tests/pg_merge_meta_validation.rs`
  - `crates/msgr2/tests/ceph_config.rs`
  - `crates/msgr2/tests/connection_tests.rs`
- **GitHub workflows**:
  - `.github/workflows/ci.yml` (format, clippy, unit tests, corpus-test)
  - `.github/workflows/test-with-ceph.yml` (integration tests with real Ceph cluster)

**Test addition strategy:**
- Add tests **as early as possible** once their dependencies are ready
- Each commit should include relevant tests for the functionality it adds
- Don't wait until the end - tests guide implementation correctness

### Per-Commit Validation
- Each commit must compile: `cargo build --workspace`
- Each commit must pass linting: `cargo clippy --workspace -- -D warnings`
- Each commit must pass all tests: `cargo test --workspace --all-targets`
- Each commit must be formatted: `cargo fmt --all --check`

### Corpus Validation Requirement (CRITICAL GATEKEEPER)
- **corpus_comparison_test.rs** is the gatekeeper for all dencoder implementations
- **MANDATORY**: Every new type's dencoder MUST be verified with corpus_comparison_test.rs
- **CONDITION**: Only applies to types that have corpus files in `ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects/`
- **PROCESS**:
  1. Implement the dencoder for the type
  2. Add the type to the test list in `corpus_comparison_test.rs`
  3. Run locally: `cd crates/denc && cargo test --test corpus_comparison_test -- --ignored --nocapture`
  4. Ensure the type passes corpus validation before committing
  5. CI will automatically run corpus validation on every push
- **CI Enforcement**: The `corpus-test` job in GitHub Actions will fail if any type doesn't match the official Ceph corpus

### Integration Validation
- Integration tests should be added as soon as their dependencies are ready
- Example: `pg_merge_meta_test.rs` should be added when `PgMergeMeta` type is complete
- Example: `connection_tests.rs` should be added when msgr2 connection protocol is ready
- End-to-end tests validate the complete workflow

### CI Integration
- All commits must pass all CI checks (format, clippy, unit tests, corpus tests)
- CI runs on every push to the rewrite branch
- **Four CI jobs must pass**:
  1. `fmt`: Format checking
  2. `clippy`: Linting
  3. `test`: Unit and integration tests
  4. `corpus-test`: Corpus validation (gatekeeper)
- Integration test with real Ceph (optional, may be run separately)
- No commit should be merged if any CI check fails

## Dependencies and Risks

### Critical Dependencies
1. **denc → all other crates**: Everything depends on encoding/decoding
2. **auth → msgr2**: Authentication must be complete before protocol implementation
3. **crush → monclient**: Object placement needs CRUSH
4. **msgr2 → monclient**: Communication protocol needed for monitor interaction

### Known Risks
1. **Corpus availability**: Some validation requires ceph-object-corpus
2. **Feature flag complexity**: MSG_ADDR2 and other feature flags affect encoding
3. **Version compatibility**: Supporting multiple encoding versions
4. **CRUSH algorithm complexity**: Subtle bugs in placement calculations

## Post-Rewrite Verification

After the rewrite is complete, verify that:

1. **All existing tests are preserved and passing**:
   - Run `cargo test --workspace --all-targets` - all tests must pass
   - Verify all integration tests from `crates/*/tests/` are present
   - Confirm corpus_comparison_test.rs validates all types with available corpus

2. **All CI workflows pass**:
   - Format check: `cargo fmt --all --check`
   - Clippy: `cargo clippy --workspace --all-targets -- -D warnings`
   - Unit tests: `cargo test --workspace --all-targets`
   - Corpus test: `cd crates/denc && cargo test --test corpus_comparison_test -- --ignored`
   - Integration test (optional): Connection tests with real Ceph cluster

3. **Documentation is current**:
   - Update main branch with rewritten history
   - Archive old implementation for reference
   - Update documentation to reflect new structure
   - Create migration guide for any external users

4. **Run full integration tests against live Ceph cluster** (if available)

## Summary of Existing Tests to Preserve

**Integration Tests** (must all be present at rewrite completion):
- `crates/denc/tests/corpus_comparison_test.rs` - **Gatekeeper for all dencoders**
- `crates/denc/tests/object_placement_test.rs` - Object placement logic validation
- `crates/denc/tests/osdmap_crush_integration_test.rs` - OSDMap + CRUSH integration
- `crates/denc/tests/osdmap_test.rs` - OSDMap parsing and validation
- `crates/denc/tests/pg_merge_meta_test.rs` - PG merge metadata roundtrip
- `crates/denc/tests/pg_merge_meta_validation.rs` - PG merge metadata corpus validation
- `crates/msgr2/tests/ceph_config.rs` - Ceph configuration parsing
- `crates/msgr2/tests/connection_tests.rs` - Connection protocol tests

**Unit Tests** (all `#[test]` functions in src/ files must be preserved)

**GitHub Workflows**:
- `.github/workflows/ci.yml` - Format, clippy, tests, corpus validation
- `.github/workflows/test-with-ceph.yml` - Integration with real Ceph cluster

## Notes

- This plan is a living document and may be adjusted as implementation progresses
- Each phase should be reviewed before moving to the next
- Maintain backward compatibility where possible
- Document any breaking changes clearly in commit messages
- **ALL existing tests must pass** when the rewrite is complete
- Tests should be added **as early as possible** once dependencies are ready, not at the end
