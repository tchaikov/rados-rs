# Pragmatic Repository Rewrite Execution Plan

## Overview
While REWRITE_PLAN.md outlines an ideal 49-commit sequence, manually crafting each commit would require hundreds of hours. This execution plan takes a more pragmatic approach by creating 12-15 well-structured commits that group related functionality while maintaining reviewability.

## Commit Sequence

### Phase 1: Foundation (5 commits)

**Commit 1: Core denc infrastructure and primitive types**
- Workspace setup with Cargo.toml
- denc crate with Denc trait, error types
- Primitive type implementations (integers, bool, floats)
- Basic tests for primitives
- Justification: These are tightly coupled - traits need primitives for meaningful tests

**Commit 2: Collection types and denc-derive**
- String, Vec<T>, Option<T> implementations
- HashMap/BTreeMap encoding
- denc-derive procedural macros for basic structs
- Tests for collections and derived types
- Justification: Collections and derives are used together immediately

**Commit 3: Ceph entity types and addressing**
- EntityName, EntityAddr types
- FeatureSet implementation
- Time types (UTime)
- Tests and entity_addr corpus validation
- Justification: Core Ceph identity types used everywhere

**Commit 4: PG types and pool configuration**
- PgId, Eversion, PgMergeMeta
- PgPool with versioned encoding
- Corpus validation tests
- Justification: Fundamental data structures for object placement

**Commit 5: MonMap, OSDMap, and CRUSH types**
- MonInfo, MonMap structures
- OsdInfo, OsdXinfo, OSDMap
- CrushWrapper and CRUSH map types  
- Integration tests (osdmap_test, osdmap_crush_integration_test, object_placement_test)
- Justification: These work together for cluster topology

### Phase 2: CRUSH Algorithm (1 commit)

**Commit 6: CRUSH implementation**
- crush crate with hash functions
- Bucket algorithms (uniform, list, tree, straw2)
- CRUSH choose algorithm (firstn, indep)
- Object placement calculations
- Tests for CRUSH operations
- Justification: CRUSH is a cohesive algorithm best reviewed as a unit

### Phase 3: Authentication (2 commits)

**Commit 7: CephX types and keyring**
- auth crate setup
- Keyring parsing and management
- CephX session keys, tickets, blobs
- Type encoding/decoding
- Tests for keyring and types
- Justification: Auth data structures belong together

**Commit 8: CephX protocol and authentication handler**
- CephX request/reply messages
- Client-side authentication flow
- Challenge-response implementation
- Session key derivation
- Authentication tests including corpus validation
- Justification: Protocol implementation is cohesive

### Phase 4: Messenger Protocol (4 commits)

**Commit 9: Frame protocol fundamentals**
- msgr2 crate setup
- Frame types, headers, tags
- Frame encoding/decoding
- CRC calculation
- Basic frame tests
- Justification: Frame protocol is the foundation for all messaging

**Commit 10: Frame security (encryption and compression)**
- AES-128-GCM encryption integration  
- Encrypted frame handling
- Compression support
- Security tests
- Justification: Security features are tightly coupled to frames

**Commit 11: Protocol state machine and handshake**
- Connection state types and transitions
- Banner exchange, Hello exchange
- Authentication negotiation
- Session establishment
- Keepalive handling
- State machine tests
- Justification: The handshake sequence is a cohesive flow

**Commit 12: Message handling and connection management**
- Message structure and encoding
- Message routing and dispatcher
- Connection pooling
- Lifecycle management
- Integration tests (ceph_config, connection_tests)
- Justification: Message layer completes the protocol stack

### Phase 5: Monitor Client (1 commit)

**Commit 13: Monitor client implementation**
- monclient crate complete
- Monitor discovery and connection
- Monitor messages and commands
- OSDMap retrieval and updates
- MonMap handling
- Complete integration
- Justification: Monitor client ties everything together

### Final Steps (1-2 commits)

**Commit 14: Documentation and examples**
- Update README.md
- Add/verify examples
- Update CLAUDE.md if needed
- CI workflow validation
- Justification: Documentation should reflect the final state

**Commit 15: Final validation and cleanup (if needed)**
- Any remaining test fixes
- CI adjustments
- Final corpus validation
- Justification: Ensure everything works together

## Validation per Commit

Each commit must:
1. Compile: `cargo build --workspace`
2. Pass tests: `cargo test --workspace --all-targets`
3. Pass clippy: `cargo clippy --workspace --all-targets -- -D warnings`
4. Pass format: `cargo fmt --all --check`
5. For commits with corpus validation: corpus tests must pass

## Benefits of This Approach

1. **Reviewable**: 13-15 commits is much more manageable than 49
2. **Logical grouping**: Related functionality stays together
3. **Testable**: Each commit represents a meaningful milestone
4. **Compilable**: All intermediate states build successfully
5. **Pragmatic**: Achievable in reasonable time while maintaining quality

## Trade-offs

- Less granular history than the 49-commit ideal plan
- Some commits are larger and touch more files
- Still maintains bottom-up approach and clear progression
- Preserves all tests and validation requirements
