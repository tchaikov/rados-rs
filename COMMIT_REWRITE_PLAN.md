# Repository Rewrite - Commit Sequence Plan

## Overview

This document outlines the detailed plan for rewriting and restructuring the commit history in `tchaikov/rados-rs` to support external review and long-term maintainability. The goal is to create a clean, logical commit sequence that builds the repository from foundational components to high-level functionality.

## Objectives

1. **Reviewability**: Each commit should represent a logical, self-contained unit of work
2. **Compilability**: Every commit must compile successfully
3. **Testability**: Each commit should include tests for the implemented functionality
4. **Bottom-up Construction**: Build from foundational components to high-level features
5. **Modularity**: Separate concerns into distinct crates (denc, auth, msgr2, crush, monclient)

## Target Branch

**Base Branch**: `commit-rewrite`

This plan creates a PR that targets the `commit-rewrite` branch, which contains only the initial repository setup (README.md and .gitignore).

## Commit Sequence Structure

The rewrite will follow a phased approach across 5 major phases, with approximately 40-50 commits total:

### Phase 1: Foundation - denc and denc-derive crates (~18 commits)

**Purpose**: Establish the encoding/decoding infrastructure that all other components depend on.

#### Phase 1.1: Core Infrastructure (5 commits)

1. **Commit 1: Project structure and denc trait definitions**
   - Create Cargo workspace
   - Add `denc` crate with core traits (`Denc`, `VersionedEncode`)
   - Add error types (`RadosError`)
   - Add basic trait validation tests
   - Dependencies: None
   - Tests: 2 trait validation tests

2. **Commit 2: Primitive type encoding**
   - Implement `Denc` for integer types (u8, u16, u32, u64, i8, i16, i32, i64)
   - Implement `Denc` for bool, f32, f64
   - Add `FixedSize` marker trait
   - Dependencies: Commit 1
   - Tests: 11 primitive encoding tests

3. **Commit 3: Basic collection types**
   - Implement `Denc` for `String`
   - Implement `Denc` for `Vec<T>`
   - Implement `Denc` for `Option<T>`
   - Dependencies: Commit 2
   - Tests: 20 collection tests (including nested collections)

4. **Commit 4: Complex collection types**
   - Implement `Denc` for `HashMap<K, V>` and `BTreeMap<K, V>`
   - Implement `Denc` for tuples (2-12 elements)
   - Implement `Denc` for arrays `[T; N]` (N=1-32)
   - Dependencies: Commit 3
   - Tests: 15 complex collection tests

5. **Commit 5: denc-derive procedural macros**
   - Create `denc-derive` crate
   - Implement `#[derive(Denc)]` for structs
   - Implement `#[derive(Denc)]` for enums
   - Add attribute support (`#[denc(skip)]`, `#[denc(version)]`)
   - Dependencies: Commits 1-4
   - Tests: 12 derive macro tests

#### Phase 1.2: Ceph-Specific Foundation Types (9 commits)

6. **Commit 6: Time and UUID types**
   - Implement `utime_t` (microsecond timestamp)
   - Implement `ceph_uuid_t`
   - Dependencies: Phase 1.1
   - Tests: 6 time/UUID tests

7. **Commit 7: Entity naming types**
   - Implement `entity_name_t`
   - Implement `entity_addr_t`
   - Implement `entity_inst_t`
   - Dependencies: Commit 6
   - Tests: 8 entity naming tests

8. **Commit 8: Authentication types**
   - Implement `AuthAuthorizer`
   - Implement `CephXTicketBlob`
   - Implement `CephXServiceTicket`
   - Dependencies: Commit 7
   - Tests: 5 auth type tests

9. **Commit 9: Feature set types**
   - Implement `FeatureSet`
   - Implement feature flags enumeration
   - Dependencies: Commit 7
   - Tests: 4 feature set tests

10. **Commit 10: Connection types**
    - Implement `ConnectionInfo`
    - Implement `PeerInfo`
    - Dependencies: Commits 8, 9
    - Tests: 6 connection type tests

11. **Commit 11: Object ID types**
    - Implement `object_t`
    - Implement `hobject_t`
    - Implement `ghobject_t`
    - Dependencies: Commit 6
    - Tests: 9 object ID tests

12. **Commit 12: Pool and PG types**
    - Implement `pg_t` (placement group)
    - Implement `pool_t`
    - Implement `pg_pool_t` (pool configuration)
    - Dependencies: Commit 11
    - Tests: 7 PG/pool tests

13. **Commit 13: CRUSH bucket types**
    - Implement `crush_bucket`
    - Implement bucket type variants (uniform, list, tree, straw, straw2)
    - Dependencies: Commit 12
    - Tests: 8 bucket tests

14. **Commit 14: CRUSH rule types**
    - Implement `crush_rule`
    - Implement `crush_rule_step`
    - Dependencies: Commit 13
    - Tests: 6 rule tests

#### Phase 1.3: OSD and Map Types (4 commits)

15. **Commit 15: OSD info types**
    - Implement `osd_info_t`
    - Implement `osd_xinfo_t`
    - Dependencies: Commit 7
    - Tests: 5 OSD info tests

16. **Commit 16: MonMap**
    - Implement `MonMap` structure
    - Implement monitor endpoint list
    - Dependencies: Commit 7
    - Tests: 4 MonMap tests

17. **Commit 17: OSDMap foundation**
    - Implement `OSDMap` structure
    - Implement epoch and pool tracking
    - Dependencies: Commits 12, 15
    - Tests: 6 OSDMap tests

18. **Commit 18: Corpus comparison tests**
    - Add binary corpus files from Ceph
    - Add corpus comparison test framework
    - Add tests for all implemented types
    - Dependencies: All Phase 1 commits
    - Tests: 20+ corpus validation tests

### Phase 2: Authentication - auth crate (~4 commits)

**Purpose**: Implement CephX authentication protocol.

19. **Commit 19: Auth protocol constants**
    - Implement `CEPH_AUTH_*` constants
    - Implement auth mode enumerations
    - Dependencies: Phase 1
    - Tests: 2 constant validation tests

20. **Commit 20: CephX session key handling**
    - Implement `CephXSessionAuthInfo`
    - Implement session key derivation
    - Dependencies: Commit 19
    - Tests: 5 session key tests

21. **Commit 21: CephX ticket handling**
    - Implement ticket request/response
    - Implement ticket encryption/decryption
    - Dependencies: Commit 20
    - Tests: 8 ticket handling tests

22. **Commit 22: Auth helper functions**
    - Implement auth utility functions
    - Implement key rotation helpers
    - Dependencies: Commit 21
    - Tests: 6 helper function tests

### Phase 3: Messaging - msgr2 crate (~16 commits)

**Purpose**: Implement Ceph messenger protocol v2.

#### Phase 3.1: Protocol Foundation (5 commits)

23. **Commit 23: Frame types and tags**
    - Implement frame tag enumerations
    - Implement frame header structures
    - Dependencies: Phase 1
    - Tests: 4 frame type tests

24. **Commit 24: Control frames**
    - Implement `HelloFrame`
    - Implement `AuthRequestFrame`
    - Implement `AuthDoneFrame`
    - Dependencies: Commit 23, Phase 2
    - Tests: 8 control frame tests

25. **Commit 25: Message frames**
    - Implement `MessageFrame`
    - Implement message header and payload
    - Dependencies: Commit 24
    - Tests: 6 message frame tests

26. **Commit 26: Compression support**
    - Implement compression negotiation
    - Implement zlib/lz4/snappy compression
    - Dependencies: Commit 25
    - Tests: 9 compression tests

27. **Commit 27: Encryption support**
    - Implement AES-GCM encryption
    - Implement encryption negotiation
    - Dependencies: Commit 26, Phase 2
    - Tests: 7 encryption tests

#### Phase 3.2: Connection State Machine (6 commits)

28. **Commit 28: Connection states**
    - Implement connection state enumeration
    - Implement state transition logic
    - Dependencies: Commit 27
    - Tests: 5 state machine tests

29. **Commit 29: Banner exchange**
    - Implement banner send/receive
    - Implement protocol version negotiation
    - Dependencies: Commit 28
    - Tests: 6 banner tests

30. **Commit 30: Authentication flow**
    - Implement auth request/response handling
    - Implement CephX flow integration
    - Dependencies: Commit 29, Phase 2
    - Tests: 10 auth flow tests

31. **Commit 31: Session establishment**
    - Implement session reconnect logic
    - Implement session reset handling
    - Dependencies: Commit 30
    - Tests: 8 session tests

32. **Commit 32: Keepalive handling**
    - Implement keepalive frame send/receive
    - Implement timeout detection
    - Dependencies: Commit 31
    - Tests: 5 keepalive tests

33. **Commit 33: Connection error handling**
    - Implement error recovery
    - Implement connection retry logic
    - Dependencies: Commit 32
    - Tests: 7 error handling tests

#### Phase 3.3: Message Routing (5 commits)

34. **Commit 34: Message dispatcher**
    - Implement message routing framework
    - Implement handler registration
    - Dependencies: Commit 25
    - Tests: 6 dispatcher tests

35. **Commit 35: Async connection handler**
    - Implement tokio-based connection management
    - Implement async read/write loops
    - Dependencies: Commits 33, 34
    - Tests: 8 async handler tests

36. **Commit 36: Message queue**
    - Implement outbound message queue
    - Implement priority handling
    - Dependencies: Commit 35
    - Tests: 5 queue tests

37. **Commit 37: Messenger API**
    - Implement `Messenger` public API
    - Implement connection pooling
    - Dependencies: Commit 36
    - Tests: 9 messenger API tests

38. **Commit 38: Integration tests**
    - Add client/server integration tests
    - Add multi-connection tests
    - Dependencies: Commit 37
    - Tests: 8 integration tests

### Phase 4: CRUSH Algorithm - crush crate (~6 commits)

**Purpose**: Implement CRUSH placement algorithm.

39. **Commit 39: CRUSH map structure**
    - Implement `CrushMap` loading
    - Implement bucket hierarchy
    - Dependencies: Phase 1 (commits 13-14)
    - Tests: 5 map structure tests

40. **Commit 40: Weight and choose functions**
    - Implement bucket weight calculation
    - Implement choose_firstn/chooseleaf algorithms
    - Dependencies: Commit 39
    - Tests: 12 choose algorithm tests

41. **Commit 41: PG to OSD mapping**
    - Implement PG hash functions
    - Implement CRUSH rule evaluation
    - Dependencies: Commit 40
    - Tests: 10 mapping tests

42. **Commit 42: Straw2 bucket algorithm**
    - Implement straw2 selection
    - Implement straw2 weight updates
    - Dependencies: Commit 41
    - Tests: 8 straw2 tests

43. **Commit 43: Map update handling**
    - Implement incremental map updates
    - Implement epoch tracking
    - Dependencies: Commit 42
    - Tests: 6 update tests

44. **Commit 44: CRUSH test suite**
    - Add comprehensive CRUSH tests
    - Add placement verification tests
    - Dependencies: Commits 39-43
    - Tests: 15 comprehensive tests

### Phase 5: Monitor Client - monclient crate (~5 commits)

**Purpose**: Implement monitor client for cluster communication.

45. **Commit 45: MonClient structure**
    - Implement `MonClient` API
    - Implement monitor connection handling
    - Dependencies: Phase 3, Commit 16
    - Tests: 5 MonClient tests

46. **Commit 46: Map subscription**
    - Implement OSDMap subscription
    - Implement MonMap updates
    - Dependencies: Commit 45, Phase 4
    - Tests: 7 subscription tests

47. **Commit 47: Command handling**
    - Implement monitor command API
    - Implement command response parsing
    - Dependencies: Commit 46
    - Tests: 8 command tests

48. **Commit 48: Monitor failover**
    - Implement monitor failover logic
    - Implement connection retry
    - Dependencies: Commit 47
    - Tests: 6 failover tests

49. **Commit 49: End-to-end integration**
    - Add complete integration test suite
    - Add example applications
    - Add CI/CD workflow
    - Dependencies: All previous commits
    - Tests: 10+ integration tests

## Dependencies Matrix

### Critical Dependencies

- **auth crate** depends on **denc crate** (types encoding)
- **msgr2 crate** depends on **denc crate** (message encoding) and **auth crate** (authentication)
- **crush crate** depends on **denc crate** (map encoding)
- **monclient crate** depends on **msgr2 crate** (communication), **crush crate** (CRUSH mapping), and **denc crate** (types)

### Inter-Phase Dependencies

```
Phase 1 (denc) → Phase 2 (auth)
                    ↓
Phase 1 (denc) → Phase 3 (msgr2)
                    ↓
Phase 1 (denc) → Phase 4 (crush)
                    ↓
All Phases → Phase 5 (monclient)
```

## Risk Assessment

### High-Risk Areas

1. **Corpus Compatibility** (Commit 18)
   - Risk: Binary encoding may not match Ceph's implementation
   - Mitigation: Comprehensive corpus testing with real Ceph data
   - Contingency: May need to adjust encoding logic based on corpus results

2. **Async Connection Handling** (Commit 35)
   - Risk: Race conditions or deadlocks in async code
   - Mitigation: Extensive concurrency testing
   - Contingency: May need to revise connection state machine

3. **CRUSH Algorithm Accuracy** (Commits 40-41)
   - Risk: Incorrect placement calculations
   - Mitigation: Verify against Ceph reference implementation
   - Contingency: Add debug logging and comparison tools

4. **Authentication Security** (Commits 20-21, 27, 30)
   - Risk: Security vulnerabilities in crypto implementation
   - Mitigation: Use well-tested crypto libraries, security review
   - Contingency: May need external security audit

### Medium-Risk Areas

1. **Derive Macro Complexity** (Commit 5)
   - Risk: Edge cases in generated code
   - Mitigation: Comprehensive test coverage
   
2. **Message Routing** (Commits 34-37)
   - Risk: Performance bottlenecks
   - Mitigation: Benchmark critical paths

3. **Monitor Failover** (Commit 48)
   - Risk: Network partition handling
   - Mitigation: Integration testing with simulated failures

### Low-Risk Areas

- Basic type encoding (Commits 2-4)
- Constant definitions (Commits 6, 19, 23)
- Test infrastructure (Commits 18, 38, 44, 49)

## Testing Strategy

### Unit Tests
- Each commit includes unit tests for new functionality
- Minimum 80% code coverage for each commit
- Tests must pass before moving to next commit

### Integration Tests
- Added at phase boundaries (Commits 18, 38, 49)
- Test interaction between crates
- Test real-world scenarios

### Corpus Tests
- Compare binary encoding with Ceph's implementation
- Validate backward compatibility
- Test all major type categories

### Performance Tests
- Benchmark critical paths (CRUSH, message routing)
- Profile memory usage
- Test under load

## Validation Checklist

For each commit:
- [ ] Code compiles without warnings
- [ ] All unit tests pass
- [ ] Code follows Rust best practices
- [ ] Public APIs are documented
- [ ] No security vulnerabilities (cargo audit)
- [ ] Git commit message is clear and descriptive

For each phase:
- [ ] Integration tests pass
- [ ] Documentation is updated
- [ ] Examples are working
- [ ] Dependencies are correctly specified

## Timeline Estimate

- **Phase 1** (Foundation): 8-12 hours
- **Phase 2** (Authentication): 3-4 hours
- **Phase 3** (Messaging): 10-14 hours
- **Phase 4** (CRUSH): 6-8 hours
- **Phase 5** (MonClient): 4-6 hours
- **Total**: 31-44 hours of continuous work

## Execution Approach

1. **Manual Commit Crafting**: Each commit will be manually created by copying and editing source files incrementally
2. **Incremental Testing**: Run tests after each commit to ensure compilability and correctness
3. **No History Preservation**: Complete history rewrite, not preserving current working history
4. **Branch Replacement**: After completion, the maintainer will handle removing the main branch and renaming

## Success Criteria

1. All 49 commits are created and pushed
2. Each commit compiles successfully
3. All tests pass (100% test success rate)
4. Code review confirms quality
5. Documentation is complete
6. CI/CD pipeline is green

## Notes

- This is a complete rewrite of the repository history
- Current working history will NOT be preserved
- The `commit-rewrite` branch will serve as the new main branch after validation
- Each commit must be self-contained and independently buildable
- Tests are mandatory for each commit
