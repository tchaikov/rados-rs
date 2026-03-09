# Crush Crate Module Documentation Guide

Reference for Phase 5 doc work on the 8 undocumented modules in `crates/crush/src/`.

## Module Dependencies & Documentation Order

```
types.rs ←─────────────────────────┐
   ↑                               │
   │                            lib.rs (crate exports)
   │                               ↑
   ├─→ decode.rs ←─→ {bucket, mapper, placement}
   │                               ↑
   └─→ bucket.rs ←─ mapper.rs ←─ placement.rs
   └─→ hash.rs ←───────────────────┤
   └─→ error.rs ←──────────────────┤
   └─→ crush_ln_table.rs ← mapper.rs
```

**Suggested doc order**: types → placement → mapper → {bucket, hash, decode, error} → crush_ln_table

## Tier 1: Critical Path (DO FIRST)

### types.rs (221 lines)

**Public API**:
- `pub struct CrushMap` - Main data structure
- `pub struct CrushBucket` - Bucket definition
- `pub struct CrushRule` - Placement rule
- `pub enum BucketAlgorithm` - Algorithm variants: Straw2, Uniform, List, Tree, Straw
- `pub enum RuleType` - Rule type: Object, PG
- `pub struct RuleOp` - Rule operation step
- `pub struct CrushRuleStep` - Decoded rule step

**Doc template**:
```rust
//! CRUSH data structures and types
//!
//! This module defines the core data structures for CRUSH maps, including:
//!
//! - [`CrushMap`]: The complete CRUSH map with buckets, rules, and device classes
//! - [`CrushBucket`]: A hierarchical bucket containing items (devices or other buckets)
//! - [`CrushRule`]: A placement rule for determining OSD selection strategy
//! - [`BucketAlgorithm`]: The algorithm for bucket selection (Straw2, Uniform, List, Tree, Straw)
//!
//! # CRUSH Algorithm Overview
//!
//! CRUSH (Controlled Replication Under Scalable Hashing) maps objects to OSDs using:
//! 1. Object hash + pool → PG (placement group)
//! 2. PG hash + CRUSH rule → OSDs
//!
//! The map defines a hierarchical tree of buckets (failure domains) and selection algorithms.
//! Rules control which buckets/devices are selected, allowing data placement policies.
//!
//! Reference: Ceph source code `src/crush/crush_types.h`
```

**Key sections to explain**:
- CrushMap format & decoding
- Bucket hierarchy and types
- Algorithm selection (Straw2 advantages, etc.)
- Rule execution flow
- Device classes (e.g., "ssd", "hdd") for tiered storage

---

### placement.rs (619 lines)

**Public API** (re-exported from lib.rs):
- `pub fn pg_to_osds(...)` - Map PG to OSDs using CRUSH rule
- `pub fn object_to_osds(...)` - Direct object-to-OSDs mapping
- `pub fn object_to_pg(...)` - Object to PG mapping
- `pub struct ObjectLocator` - Object placement hint
- `pub struct PgId` - Placement group identifier

**Doc template**:
```rust
//! Object placement API for CRUSH
//!
//! This module provides the primary public interface for mapping Ceph objects to OSDs.
//!
//! # Three-Layer API
//!
//! Object placement in Ceph follows a 3-layer hierarchy:
//!
//! 1. **Object Hash → PG**: [`object_to_pg()`] hashes object name with pool rules
//!    to determine a placement group (PG). This is pool-specific and deterministic.
//!
//! 2. **PG → OSDs**: [`pg_to_osds()`] uses the CRUSH rule to select OSDs from the
//!    bucket hierarchy. This respects failure domain rules and replication count.
//!
//! 3. **Combined**: [`object_to_osds()`] performs both steps with a pool's rules.
//!
//! # Object Locator
//!
//! [`ObjectLocator`] provides hints for placement:
//! - `hash` (i64 >= 0): Explicit hash position, bypasses name-based hashing
//! - `key` (String): Alternative to hash for placement
//! - `namespace` (String): Pool namespace for multi-tenancy
//! - `pool_id` (u64): Target pool identifier
//!
//! # Example
//!
//! ```
//! use crush::{CrushMap, ObjectLocator, PgId};
//!
//! # let crush_map = CrushMap::default();
//! let locator = ObjectLocator::new(1); // pool 1
//! let pg = crush::object_to_pg(&crush_map, "my-object", &locator)?;
//! let osds = crush::pg_to_osds(&crush_map, pg, 0, &[], 3, false)?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! Reference: Ceph source `src/osd/osd_types.h` (ObjectLocator), `src/crush/mapper.c`
```

**Key sections**:
- 3-layer mapping explanation
- ObjectLocator semantics (hash vs key behavior)
- PG format and meaning
- Replication factor impact
- Example with real pool/object names

---

## Tier 2: Core Algorithms (DO AFTER TIER 1)

### mapper.rs (1,146 lines)

**Function**: Executes CRUSH rules to select OSDs from hierarchy

**Key functions**:
- `pub fn crush_do_rule(...)` - Main entry point
- `fn crush_do_rule_step(...)` - Recursive rule step execution
- `fn get_item_by_id(...)` - Resolve item by ID

**Doc approach**:
- Brief intro: "CRUSH rule execution engine"
- Reference C++ source: `src/crush/mapper.c`
- Document crush_do_rule() parameters (rule, x, num_rep, weights, result_max)
- Explain step types: TAKE, EMIT, CHOOSE, CHOOSE_LEAF, JUMP, SET_CHOOSE_LOCAL_TRIES, etc.
- Note: Don't document every function (too large), focus on crush_do_rule()

**Doc template**:
```rust
//! CRUSH rule execution engine
//!
//! This module implements the core CRUSH algorithm that selects OSDs from a hierarchical
//! bucket tree according to placement rules.
//!
//! # Rule Execution
//!
//! A CRUSH rule is a sequence of operations (steps) that:
//! 1. [`TAKE`](RuleOp::Take) a starting item (usually the root bucket)
//! 2. Recursively [`CHOOSE`](RuleOp::Choose) or [`CHOOSE_LEAF`](RuleOp::ChooseLeaf) items
//! 3. [`EMIT`](RuleOp::Emit) selected items to the result set
//!
//! The [`crush_do_rule()`] function executes a complete rule given:
//! - A CRUSH map with buckets and device classes
//! - A rule ID and input hash `x`
//! - Replication parameters (num_rep, weights, result_max)
//!
//! Reference: Ceph source `src/crush/mapper.c` (crush_do_rule)
```

### bucket.rs (307 lines)

**Functions**: Bucket selection algorithms
- `pub fn bucket_choose(...)` - Dispatch to algorithm
- `fn bucket_straw2_choose(...)` - Straw2 algorithm
- `fn bucket_straw_choose(...)` - Straw algorithm
- `fn bucket_tree_choose(...)` - Tree algorithm
- `fn bucket_uniform_choose(...)` - Uniform algorithm
- `fn bucket_list_choose(...)` - List algorithm

**Doc template**:
```rust
//! Bucket selection algorithms for CRUSH
//!
//! This module implements the five bucket selection algorithms used by CRUSH to
//! choose items within a bucket:
//!
//! - **Straw2**: Fast, cache-efficient evolution of Straw (recommended for modern clusters)
//! - **Straw**: Original probabilistic selection with item weights
//! - **Tree**: Balanced binary tree selection
//! - **Uniform**: Simple round-robin (legacy)
//! - **List**: Linear list with weight-based selection (legacy)
//!
//! Each algorithm balances items based on their weight values.
//!
//! Reference: Ceph source `src/crush/mapper.c` (bucket selection functions)
```

### hash.rs (293 lines)

**Functions**: Hash implementations
- `pub fn crush_hash32_2(...)` - 2-value hash
- `pub fn crush_hash32_3(...)` - 3-value hash
- `pub fn crush_hash32_4(...)` - 4-value hash

**Doc template**:
```rust
//! Hash functions for CRUSH
//!
//! This module implements the cryptographic hashing primitives used by CRUSH to
//! pseudorandomly select items and distribute load.
//!
//! The three functions support hashing different numbers of inputs:
//! - [`crush_hash32_2()`]: Hash 2 values (object hash + item ID)
//! - [`crush_hash32_3()`]: Hash 3 values (adds secondary input)
//! - [`crush_hash32_4()`]: Hash 4 values (adds tertiary input)
//!
//! Reference: Ceph source `src/crush/hash.c`
```

---

## Tier 3: Supporting Modules (DO AFTER TIER 2)

### decode.rs (504 lines)

**Function**: CRUSH map binary format decoding

**Doc**: "Binary decoding for CRUSH map format. Contains low-level structures for parsing
Ceph's native CRUSH map binary encoding. Most users should use [`CrushMap::decode()`](crate::types::CrushMap::decode)
instead of this module directly."

### error.rs (39 lines)

**Function**: Error types

**Doc**: "Error types for CRUSH operations. Re-exports [`CrushError`] enum for CRUSH rule execution and decoding failures."

---

## Tier 4: Deferrable Data

### crush_ln_table.rs (629 lines)

**Function**: Lookup tables for logarithm approximation

**Doc**: **DEFER** with single-line doc:

```rust
//! Pre-computed lookup tables for the crush_ln() logarithm approximation function
```

**Reason**: Data file, no algorithmic content. 629 lines of pure data (LL_TBL, RH_LH_TBL arrays).
Not worth documenting in detail. Can add to future "implementation notes" document if needed.

---

## Writing Style Guide

### Module-level doc comment structure

```rust
//! Brief one-liner about what this module does
//!
//! # Longer Description
//!
//! More detailed explanation of purpose, algorithms, or concepts.
//! 
//! # Examples
//!
//! Show real usage examples when helpful.
//!
//! # References
//!
//! Point to Ceph C++ source files (~/dev/ceph/src/...) for deep dives.
```

### Key conventions used in other crates (follow these)

From **denc**:
```rust
//! Object identifiers and hierarchy
//!
//! Encodes RADOS object names and pools into the HObject format used internally
//! by Ceph OSDs for distinguishing objects in different pools and namespaces.
```

From **msgr2**:
```rust
//! Message frame encoding and negotiation
//!
//! Frames are the fundamental unit of msgr2 protocol exchange, carrying both
//! control information and encrypted message payloads.
```

From **osdclient**:
```rust
//! OSD session and connection management
//!
//! Maintains per-OSD connections with automatic reconnection, handles
//! message pipelining, and reply matching using sequence numbers.
```

### Reference C++ sources

Include comments like:
- `Reference: ~/dev/ceph/src/crush/mapper.c` for implementation details
- `Matches C++ type CrushMap from ~/dev/ceph/src/crush/crush_types.h`
- `See also: Ceph source src/osd/osd_types.h for ObjectLocator`

---

## Checklist for Each Module

- [ ] Module-level doc comment (3-5 sentences)
- [ ] Link to Ceph C++ reference
- [ ] List public types/functions
- [ ] 1 paragraph explaining purpose
- [ ] Example if helpful (placement.rs, mapper.rs)
- [ ] Cross-references to related modules
- [ ] Run `cargo doc --open` and verify rendering

## Estimated Time per Module

| Module | Lines | Est. Time |
|--------|-------|-----------|
| types.rs | 221 | 30 min |
| placement.rs | 619 | 45 min |
| mapper.rs | 1,146 | 60 min |
| bucket.rs | 307 | 30 min |
| hash.rs | 293 | 20 min |
| decode.rs | 504 | 30 min |
| error.rs | 39 | 5 min |
| crush_ln_table.rs | 629 | 2 min |
| **TOTAL** | **3,817** | **3.5 hours** |

**Realistic with review/testing**: 4-5 hours for quality work.

