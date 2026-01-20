# Repository Rewrite - Commit Sequence Plan

## Overview

This document outlines the detailed plan for **creating a completely new commit history from scratch** in `tchaikov/rados-rs` to support external review and long-term maintainability. The goal is to build a clean, logical commit sequence from the ground up that represents how the repository should have been developed, building from foundational components to high-level functionality.

**Important**: This is NOT a git history rewrite (like git rebase/filter-branch). Instead, we are manually recreating the entire repository by copying code from the original implementation into a new branch with a logical commit sequence.

## Objectives

1. **Reviewability**: Each commit should represent a logical, self-contained unit of work
2. **Compilability**: Every commit must compile successfully
3. **Testability**: Each commit should include tests for the implemented functionality
4. **Bottom-up Construction**: Build from foundational components to high-level features
5. **Modularity**: Separate concerns into distinct crates (denc, auth, msgr2, crush, monclient)
6. **Test Preservation**: All unit tests, integration tests, and GitHub workflows must be preserved and pass in the rewrite

## Gating Criteria

**Critical Requirements** (must be met for successful rewrite):

1. **Unit Tests**: All unit tests must be added alongside the corresponding implementation (same commit)
2. **Test Passage**: All tests (unit and integration) must pass at every commit
3. **Corpus Validation**: The `dencoder` tool must be added early (Commit 2) and used in CI to verify all types with corpus files under `ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects`
4. **GitHub Workflows**: All existing GitHub workflows must be preserved and pass
5. **Integration Tests**: Integration tests must be added once their dependencies are in place
6. **No Test Removal**: No existing tests may be removed or modified to pass; all original functionality must work

## Target Branch

**Base Branch**: `commit-rewrite`

This plan creates a PR that targets the `commit-rewrite` branch, which starts as a clean slate with only the initial repository setup (README.md and .gitignore). The new commit history will be built from scratch on top of this base, NOT by rewriting the existing main branch history.

**Approach**: Manual commit creation by copying source files from the original implementation into the new branch, one logical feature at a time.

## Commit Sequence Structure

The rewrite will follow a phased approach across 5 major phases, with **51 commits total**:

- **Phase 1**: Foundation (denc, denc-derive) - 20 commits
- **Phase 2**: Authentication (auth) - 4 commits
- **Phase 3**: Messaging (msgr2) - 16 commits
- **Phase 4**: CRUSH algorithm (crush) - 6 commits
- **Phase 5**: Monitor client (monclient) - 5 commits

### Phase 1: Foundation - denc and denc-derive crates (~20 commits)

**Purpose**: Establish the encoding/decoding infrastructure that all other components depend on.

#### Phase 1.1: Core Infrastructure (7 commits)

1. **Commit 1: Project structure and denc trait definitions**
   - Create Cargo workspace
   - Add `denc` crate with core traits (`Denc`, `VersionedEncode`)
   - Add error types (`RadosError`)
   - Add basic trait validation tests
   - Dependencies: None
   - Tests: 2 trait validation tests (added with implementation)

2. **Commit 2: Dencoder tool and corpus infrastructure**
   - Add `dencoder` binary crate for encoding/decoding validation
   - Add corpus file reading infrastructure
   - Add corpus directory structure (`ceph-object-corpus/archive/19.2.0-404-g78ddc7f9027/objects`)
   - Add initial CI workflow for corpus validation
   - Dependencies: Commit 1
   - Tests: Dencoder CLI tests (added with implementation)
   - **Critical**: This tool will be used throughout to validate all type implementations against Ceph corpus files

3. **Commit 3: Primitive type encoding**
   - Implement `Denc` for integer types (u8, u16, u32, u64, i8, i16, i32, i64)
   - Implement `Denc` for bool, f32, f64
   - Add `FixedSize` marker trait
   - Dependencies: Commit 2
   - Tests: 11 primitive encoding tests (added with implementation)
   - **Corpus Validation**: Run dencoder against corpus files for primitive types in CI

4. **Commit 4: Basic collection types**
   - Implement `Denc` for `String`
   - Implement `Denc` for `Vec<T>`
   - Implement `Denc` for `Option<T>`
   - Dependencies: Commit 3
   - Tests: 20 collection tests (added with implementation, including nested collections)
   - **Corpus Validation**: Run dencoder against corpus files in CI

5. **Commit 5: Complex collection types**
   - Implement `Denc` for `HashMap<K, V>` and `BTreeMap<K, V>`
   - Implement `Denc` for tuples (2-12 elements)
   - Implement `Denc` for arrays `[T; N]` (N=1-32)
   - Dependencies: Commit 4
   - Tests: 15 complex collection tests (added with implementation)
   - **Corpus Validation**: Run dencoder against corpus files in CI

6. **Commit 6: denc-derive procedural macros**
   - Create `denc-derive` crate
   - Implement `#[derive(Denc)]` for structs
   - Implement `#[derive(Denc)]` for enums
   - Add attribute support (`#[denc(skip)]`, `#[denc(version)]`)
   - Dependencies: Commits 1-5
   - Tests: 12 derive macro tests (added with implementation)

7. **Commit 7: GitHub Actions CI workflow enhancement**
   - Add complete CI workflow file (`.github/workflows/ci.yml`)
   - Add cargo build, test, clippy, fmt checks
   - Add dencoder corpus validation job
   - Add test result reporting
   - Dependencies: Commits 1-6
   - **Critical**: CI must run dencoder against all corpus files as types are added
   - Tests: CI workflow validation

#### Phase 1.2: Ceph-Specific Foundation Types (9 commits)

8. **Commit 8: Time and UUID types**
   - Implement `utime_t` (microsecond timestamp)
   - Implement `ceph_uuid_t`
   - Dependencies: Phase 1.1
   - Tests: 6 time/UUID tests (added with implementation)
   - **Corpus Validation**: Run dencoder against corpus files in CI

9. **Commit 9: Entity naming types**
   - Implement `entity_name_t`
   - Implement `entity_addr_t`
   - Implement `entity_inst_t`
   - Dependencies: Commit 8
   - Tests: 8 entity naming tests (added with implementation)
   - **Corpus Validation**: Run dencoder against corpus files in CI

10. **Commit 10: Authentication types**
    - Implement `AuthAuthorizer`
    - Implement `CephXTicketBlob`
    - Implement `CephXServiceTicket`
    - Dependencies: Commit 9
    - Tests: 5 auth type tests (added with implementation)
    - **Corpus Validation**: Run dencoder against corpus files in CI

11. **Commit 11: Feature set types**
    - Implement `FeatureSet`
    - Implement feature flags enumeration
    - Dependencies: Commit 9
    - Tests: 4 feature set tests (added with implementation)
    - **Corpus Validation**: Run dencoder against corpus files in CI

12. **Commit 12: Connection types**
    - Implement `ConnectionInfo`
    - Implement `PeerInfo`
    - Dependencies: Commits 10, 11
    - Tests: 6 connection type tests (added with implementation)
    - **Corpus Validation**: Run dencoder against corpus files in CI

13. **Commit 13: Object ID types**
    - Implement `object_t`
    - Implement `hobject_t`
    - Implement `ghobject_t`
    - Dependencies: Commit 8
    - Tests: 9 object ID tests (added with implementation)
    - **Corpus Validation**: Run dencoder against corpus files in CI

14. **Commit 14: Pool and PG types**
    - Implement `pg_t` (placement group)
    - Implement `pool_t`
    - Implement `pg_pool_t` (pool configuration)
    - Dependencies: Commit 13
    - Tests: 7 PG/pool tests (added with implementation)
    - **Corpus Validation**: Run dencoder against corpus files in CI

15. **Commit 15: CRUSH bucket types**
    - Implement `crush_bucket`
    - Implement bucket type variants (uniform, list, tree, straw, straw2)
    - Dependencies: Commit 14
    - Tests: 8 bucket tests (added with implementation)
    - **Corpus Validation**: Run dencoder against corpus files in CI

16. **Commit 16: CRUSH rule types**
    - Implement `crush_rule`
    - Implement `crush_rule_step`
    - Dependencies: Commit 15
    - Tests: 6 rule tests (added with implementation)
    - **Corpus Validation**: Run dencoder against corpus files in CI

#### Phase 1.3: OSD and Map Types (4 commits)

17. **Commit 17: OSD info types**
    - Implement `osd_info_t`
    - Implement `osd_xinfo_t`
    - Dependencies: Commit 9
    - Tests: 5 OSD info tests (added with implementation)
    - **Corpus Validation**: Run dencoder against corpus files in CI

18. **Commit 18: MonMap**
    - Implement `MonMap` structure
    - Implement monitor endpoint list
    - Dependencies: Commit 9
    - Tests: 4 MonMap tests (added with implementation)
    - **Corpus Validation**: Run dencoder against corpus files in CI

19. **Commit 19: OSDMap foundation**
    - Implement `OSDMap` structure
    - Implement epoch and pool tracking
    - Dependencies: Commits 14, 17
    - Tests: 6 OSDMap tests (added with implementation)
    - **Corpus Validation**: Run dencoder against corpus files in CI

20. **Commit 20: Comprehensive corpus validation**
    - Add integration test suite validating all Phase 1 types
    - Verify all corpus files decode correctly
    - Add corpus regression tests
    - Dependencies: All Phase 1 commits
    - Tests: 20+ comprehensive corpus validation tests
    - **Critical**: All types with corpus files must pass dencoder validation

### Phase 2: Authentication - auth crate (~4 commits)

**Purpose**: Implement CephX authentication protocol.

21. **Commit 21: Auth protocol constants**
    - Implement `CEPH_AUTH_*` constants
    - Implement auth mode enumerations
    - Dependencies: Phase 1
    - Tests: 2 constant validation tests (added with implementation)

22. **Commit 22: CephX session key handling**
    - Implement `CephXSessionAuthInfo`
    - Implement session key derivation
    - Dependencies: Commit 21
    - Tests: 5 session key tests (added with implementation)

23. **Commit 23: CephX ticket handling**
    - Implement ticket request/response
    - Implement ticket encryption/decryption
    - Dependencies: Commit 22
    - Tests: 8 ticket handling tests (added with implementation)

24. **Commit 24: Auth helper functions and integration tests**
    - Implement auth utility functions
    - Implement key rotation helpers
    - Add auth crate integration tests
    - Dependencies: Commit 23
    - Tests: 6 helper function tests + integration tests (added with implementation)

### Phase 3: Messaging - msgr2 crate (~16 commits)

**Purpose**: Implement Ceph messenger protocol v2.

#### Phase 3.1: Protocol Foundation (5 commits)

25. **Commit 25: Frame types and tags**
    - Implement frame tag enumerations
    - Implement frame header structures
    - Dependencies: Phase 1
    - Tests: 4 frame type tests (added with implementation)

26. **Commit 26: Control frames**
    - Implement `HelloFrame`
    - Implement `AuthRequestFrame`
    - Implement `AuthDoneFrame`
    - Dependencies: Commit 25, Phase 2
    - Tests: 8 control frame tests (added with implementation)

27. **Commit 27: Message frames**
    - Implement `MessageFrame`
    - Implement message header and payload
    - Dependencies: Commit 26
    - Tests: 6 message frame tests (added with implementation)

28. **Commit 28: Compression support**
    - Implement compression negotiation
    - Implement zlib/lz4/snappy compression
    - Dependencies: Commit 27
    - Tests: 9 compression tests (added with implementation)

29. **Commit 29: Encryption support**
    - Implement AES-GCM encryption
    - Implement encryption negotiation
    - Dependencies: Commit 28, Phase 2
    - Tests: 7 encryption tests (added with implementation)

#### Phase 3.2: Connection State Machine (6 commits)

30. **Commit 30: Connection states**
    - Implement connection state enumeration
    - Implement state transition logic
    - Dependencies: Commit 29
    - Tests: 5 state machine tests (added with implementation)

31. **Commit 31: Banner exchange**
    - Implement banner send/receive
    - Implement protocol version negotiation
    - Dependencies: Commit 30
    - Tests: 6 banner tests (added with implementation)

32. **Commit 32: Authentication flow**
    - Implement auth request/response handling
    - Implement CephX flow integration
    - Dependencies: Commit 31, Phase 2
    - Tests: 10 auth flow tests (added with implementation)

33. **Commit 33: Session establishment**
    - Implement session reconnect logic
    - Implement session reset handling
    - Dependencies: Commit 32
    - Tests: 8 session tests (added with implementation)

34. **Commit 34: Keepalive handling**
    - Implement keepalive frame send/receive
    - Implement timeout detection
    - Dependencies: Commit 33
    - Tests: 5 keepalive tests (added with implementation)

35. **Commit 35: Connection error handling**
    - Implement error recovery
    - Implement connection retry logic
    - Dependencies: Commit 34
    - Tests: 7 error handling tests (added with implementation)

#### Phase 3.3: Message Routing (5 commits)

36. **Commit 36: Message dispatcher**
    - Implement message routing framework
    - Implement handler registration
    - Dependencies: Commit 27
    - Tests: 6 dispatcher tests (added with implementation)

37. **Commit 37: Async connection handler**
    - Implement tokio-based connection management
    - Implement async read/write loops
    - Dependencies: Commits 35, 36
    - Tests: 8 async handler tests (added with implementation)

38. **Commit 38: Message queue**
    - Implement outbound message queue
    - Implement priority handling
    - Dependencies: Commit 37
    - Tests: 5 queue tests (added with implementation)

39. **Commit 39: Messenger API**
    - Implement `Messenger` public API
    - Implement connection pooling
    - Dependencies: Commit 38
    - Tests: 9 messenger API tests (added with implementation)

40. **Commit 40: msgr2 integration tests**
    - Add client/server integration tests
    - Add multi-connection tests
    - Add end-to-end message flow tests
    - Dependencies: Commit 39
    - Tests: 8+ integration tests (added with implementation)
    - **Critical**: All integration tests must pass

### Phase 4: CRUSH Algorithm - crush crate (~6 commits)

**Purpose**: Implement CRUSH placement algorithm.

41. **Commit 41: CRUSH map structure**
    - Implement `CrushMap` loading
    - Implement bucket hierarchy
    - Dependencies: Phase 1 (commits 15-16)
    - Tests: 5 map structure tests (added with implementation)

42. **Commit 42: Weight and choose functions**
    - Implement bucket weight calculation
    - Implement choose_firstn/chooseleaf algorithms
    - Dependencies: Commit 41
    - Tests: 12 choose algorithm tests (added with implementation)

43. **Commit 43: PG to OSD mapping**
    - Implement PG hash functions
    - Implement CRUSH rule evaluation
    - Dependencies: Commit 42
    - Tests: 10 mapping tests (added with implementation)

44. **Commit 44: Straw2 bucket algorithm**
    - Implement straw2 selection
    - Implement straw2 weight updates
    - Dependencies: Commit 43
    - Tests: 8 straw2 tests (added with implementation)

45. **Commit 45: Map update handling**
    - Implement incremental map updates
    - Implement epoch tracking
    - Dependencies: Commit 44
    - Tests: 6 update tests (added with implementation)

46. **Commit 46: CRUSH integration tests**
    - Add comprehensive CRUSH tests
    - Add placement verification tests
    - Add CRUSH algorithm regression tests
    - Dependencies: Commits 41-45
    - Tests: 15+ comprehensive integration tests (added with implementation)
    - **Critical**: Verify CRUSH algorithm accuracy against reference implementation

### Phase 5: Monitor Client - monclient crate (~5 commits)

**Purpose**: Implement monitor client for cluster communication.

47. **Commit 47: MonClient structure**
    - Implement `MonClient` API
    - Implement monitor connection handling
    - Dependencies: Phase 3, Commit 18
    - Tests: 5 MonClient tests (added with implementation)

48. **Commit 48: Map subscription**
    - Implement OSDMap subscription
    - Implement MonMap updates
    - Dependencies: Commit 47, Phase 4
    - Tests: 7 subscription tests (added with implementation)

49. **Commit 49: Command handling**
    - Implement monitor command API
    - Implement command response parsing
    - Dependencies: Commit 48
    - Tests: 8 command tests (added with implementation)

50. **Commit 50: Monitor failover**
    - Implement monitor failover logic
    - Implement connection retry
    - Dependencies: Commit 49
    - Tests: 6 failover tests (added with implementation)

51. **Commit 51: End-to-end integration and workflows**
    - Add complete integration test suite
    - Add example applications (client, server, utilities)
    - Verify all existing integration tests pass
    - Preserve all GitHub workflows from original repository
    - Dependencies: All previous commits
    - Tests: 10+ integration tests (added with implementation)
    - **Critical**: All unit tests, integration tests, and GitHub workflows must pass

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

1. **Manual Commit Creation from Scratch**: Each commit will be manually created by copying source files from the original repository and organizing them into logical, incremental commits
2. **Incremental Testing**: Run tests after each commit to ensure compilability and correctness
3. **Fresh History**: Create completely new commit history from scratch; NOT a git rebase/rewrite of existing history
4. **Copy, Don't Modify**: Copy code from the original implementation in the main branch, organizing it into the new logical structure
5. **Branch Strategy**: After completion and validation, the `commit-rewrite` branch will become the new default branch

**Key Distinction**: This is NOT `git rebase`, `git filter-branch`, or any git history manipulation. This is building a new repository structure from the ground up by selectively copying files into a new commit sequence.

## Success Criteria

1. All 51 commits are created and pushed
2. Each commit compiles successfully
3. All tests pass (100% test success rate)
4. Code review confirms quality
5. Documentation is complete
6. CI/CD pipeline is green
7. All functionality from original repository is preserved

## Notes

- This creates a completely NEW commit history from scratch
- Original main branch history will NOT be modified or preserved in the new branch
- Code is COPIED from the original implementation into the new logical structure
- The `commit-rewrite` branch will serve as the new default branch after validation
- Each commit must be self-contained and independently buildable
- Tests are mandatory for each commit
- This is manual reconstruction, NOT automated git history manipulation
