# Phase 3: Hot Path Optimization Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Reduce allocations and improve throughput by 10-20%

**Architecture:** Optimize hot paths identified in Phase 2 profiling by eliminating unnecessary allocations, pre-allocating buffers, and reducing string conversions.

**Tech Stack:** Rust, Arc for shared ownership, BytesMut capacity hints, Denc trait

---

## Overview

Phase 3 focuses on hot path optimizations based on Phase 2 baseline measurements:
- Current: MOSDOp encoding ~360ns, full message ~983ns
- Target: 10-20% improvement through allocation reduction
- Approach: Use Arc for shared data, pre-allocate buffers, reduce cloning

**Key Principle:** "Measure first, optimize second" - we have baselines from Phase 2

---

## Task 1: Eliminate Message Cloning in OSDClient Session

**Files:**
- Modify: `crates/osdclient/src/session.rs`
- Modify: `crates/osdclient/src/client.rs`
- Test: `crates/osdclient/src/session.rs` (existing tests)

**Context:** Currently, MOSDOp messages are cloned when retrying operations. This creates unnecessary allocations. We should use Arc<MOSDOp> for shared ownership.

**Step 1: Identify current cloning locations**

Read `crates/osdclient/src/session.rs` and search for:
- `.clone()` calls on MOSDOp or operation data
- Places where messages are stored and later retried
- Look around line 626 (mentioned in master plan)

**Step 2: Change message storage to use Arc**

In `crates/osdclient/src/session.rs`, find the struct that stores pending operations.

Current pattern (example):
```rust
struct PendingOp {
    message: MOSDOp,  // Cloned on retry
    attempts: u32,
}
```

Change to:
```rust
struct PendingOp {
    message: Arc<MOSDOp>,  // Shared, no clone needed
    attempts: u32,
}
```

**Step 3: Update operation submission**

Where operations are created and stored, wrap in Arc:
```rust
let message = MOSDOp::new(...);
let message = Arc::new(message);
pending_ops.insert(tid, PendingOp { message, attempts: 0 });
```

**Step 4: Update retry logic**

Where operations are retried, use Arc::clone (cheap reference count increment):
```rust
// OLD: let retry_msg = pending.message.clone();  // Expensive deep clone
// NEW:
let retry_msg = Arc::clone(&pending.message);  // Cheap refcount increment
```

**Step 5: Run tests**

```bash
cargo test -p osdclient --lib
```

Expected: All tests pass

**Step 6: Run benchmarks to verify improvement**

```bash
cargo bench -p osdclient --bench operations
```

Expected: 5-10% improvement in operation submission benchmarks

**Step 7: Commit**

```bash
git add crates/osdclient/src/session.rs crates/osdclient/src/client.rs
git commit -m "perf: use Arc for shared MOSDOp messages

Replace message cloning with Arc for shared ownership:
- Wrap MOSDOp in Arc when storing pending operations
- Use Arc::clone for cheap refcount increment on retry
- Eliminates expensive deep clones in retry path

Benchmark improvement: ~X% faster operation submission

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 2: Pre-allocate Buffers in Denc Encoding

**Files:**
- Modify: `crates/denc/src/denc.rs`
- Modify: `crates/denc/src/types.rs`
- Test: `crates/denc/tests/encoding_tests.rs`

**Context:** Currently, BytesMut::new() creates buffers with default capacity, causing reallocations during encoding. We should use encoded_size() hints to pre-allocate.

**Step 1: Audit current buffer allocation patterns**

Read `crates/denc/src/denc.rs` and search for:
- `BytesMut::new()` calls
- `BytesMut::with_capacity()` calls
- Places where we could use `encoded_size()` hint

**Step 2: Add helper function for pre-allocated encoding**

In `crates/denc/src/denc.rs`, add:
```rust
/// Encode a value with pre-allocated buffer based on encoded_size hint
pub fn encode_with_capacity<T: Denc>(value: &T, features: u64) -> Result<Bytes, RadosError> {
    let size = value.encoded_size(features)?;
    let mut buf = BytesMut::with_capacity(size);
    value.encode(&mut buf, features)?;
    Ok(buf.freeze())
}
```

**Step 3: Update high-traffic encoding sites**

Find places in `crates/osdclient/src/messages.rs` and `crates/msgr2/src/protocol.rs` where messages are encoded:

OLD:
```rust
let mut buf = BytesMut::new();
message.encode(&mut buf, features)?;
```

NEW:
```rust
let size = message.encoded_size(features)?;
let mut buf = BytesMut::with_capacity(size);
message.encode(&mut buf, features)?;
```

**Step 4: Run tests**

```bash
cargo test -p denc --lib
cargo test -p osdclient --lib
cargo test -p msgr2 --lib
```

Expected: All tests pass

**Step 5: Run benchmarks to verify improvement**

```bash
cargo bench -p denc --bench message_encoding
cargo bench -p osdclient --bench operations
```

Expected: 3-5% improvement in encoding benchmarks

**Step 6: Commit**

```bash
git add crates/denc/src/denc.rs crates/denc/src/types.rs crates/osdclient/src/messages.rs crates/msgr2/src/protocol.rs
git commit -m "perf: pre-allocate buffers using encoded_size hints

Add encode_with_capacity helper and use throughout:
- Pre-allocate BytesMut with encoded_size() hint
- Eliminates reallocation during encoding
- Applied to message encoding hot paths

Benchmark improvement: ~X% faster encoding

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 3: Reduce String Conversions in Error Paths

**Files:**
- Modify: `crates/osdclient/src/session.rs`
- Modify: `crates/monclient/src/client.rs`
- Test: Existing tests

**Context:** Error messages use format!() which allocates strings even when errors aren't returned. Use lazy formatting or references where possible.

**Step 1: Identify hot path error formatting**

Search for `format!()` calls in:
- `crates/osdclient/src/session.rs`
- `crates/monclient/src/client.rs`

Look for patterns like:
```rust
.map_err(|e| Error::Protocol(format!("Failed to {}: {}", operation, e)))?
```

**Step 2: Use lazy error formatting**

For errors that are rarely hit, keep format!(). For hot paths, use references:

OLD (allocates string every time):
```rust
if condition {
    return Err(Error::Protocol(format!("Invalid state: {}", state)));
}
```

NEW (only allocates if error occurs):
```rust
if condition {
    return Err(Error::Protocol(format!("Invalid state: {}", state)));
}
// Keep format!() - it only runs when error occurs
```

For truly hot paths where we check errors frequently:
```rust
// Use static strings where possible
if condition {
    return Err(Error::Protocol("Invalid state".into()));
}
```

**Step 3: Reduce unnecessary .clone() on strings**

Search for `.clone()` on String/&str in hot paths:

OLD:
```rust
let name = entity.name.clone();
process(&name);
```

NEW:
```rust
process(&entity.name);  // Use reference
```

**Step 4: Run tests**

```bash
cargo test -p osdclient --lib
cargo test -p monclient --lib
```

Expected: All tests pass

**Step 5: Run benchmarks**

```bash
cargo bench -p osdclient --bench operations
```

Expected: 2-3% improvement (small but measurable)

**Step 6: Commit**

```bash
git add crates/osdclient/src/session.rs crates/monclient/src/client.rs
git commit -m "perf: reduce string allocations in hot paths

Optimize string handling:
- Use references instead of .clone() where possible
- Keep format!() for actual errors (lazy evaluation)
- Use static strings for common error messages

Benchmark improvement: ~X% faster

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 4: Optimize Frame Assembly Buffer Allocation

**Files:**
- Modify: `crates/msgr2/src/protocol.rs`
- Modify: `crates/msgr2/src/frames.rs`
- Test: `crates/msgr2/tests/integration_tests.rs`

**Context:** Frame assembly creates multiple BytesMut buffers. Pre-allocate based on frame size.

**Step 1: Audit frame assembly**

Read `crates/msgr2/src/frames.rs` and `crates/msgr2/src/protocol.rs`:
- Find Frame::to_wire() implementation
- Find frame assembly code
- Look for BytesMut allocations

**Step 2: Pre-allocate frame buffers**

In Frame::to_wire():

OLD:
```rust
pub fn to_wire(&self) -> Result<Bytes> {
    let mut buf = BytesMut::new();
    // encode preamble
    // encode segments
    Ok(buf.freeze())
}
```

NEW:
```rust
pub fn to_wire(&self) -> Result<Bytes> {
    // Calculate total size: preamble + all segments
    let total_size = PREAMBLE_SIZE + self.segments.iter().map(|s| s.len()).sum::<usize>();
    let mut buf = BytesMut::with_capacity(total_size);
    // encode preamble
    // encode segments
    Ok(buf.freeze())
}
```

**Step 3: Run tests**

```bash
cargo test -p msgr2 --lib
CEPH_CONF=/home/kefu/dev/ceph/build/ceph.conf cargo test -p msgr2 --tests -- --ignored
```

Expected: All tests pass

**Step 4: Run benchmarks**

```bash
cargo bench -p msgr2 --bench connection
```

Expected: 5-8% improvement in frame encoding

**Step 5: Commit**

```bash
git add crates/msgr2/src/protocol.rs crates/msgr2/src/frames.rs
git commit -m "perf: pre-allocate frame assembly buffers

Calculate total frame size and pre-allocate:
- Sum preamble + segment sizes
- Allocate BytesMut with exact capacity
- Eliminates reallocations during frame assembly

Benchmark improvement: ~X% faster frame encoding

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 5: Run Full Benchmark Suite and Document Improvements

**Files:**
- Modify: `docs/PROFILING.md`

**Context:** After all optimizations, measure actual improvements against Phase 2 baselines.

**Step 1: Run all benchmarks**

```bash
cargo bench --workspace 2>&1 | tee phase3_results.txt
```

**Step 2: Compare with Phase 2 baselines**

Extract metrics and compare:
- CRUSH mapping (should be unchanged - not optimized)
- Message encoding (expect 5-10% improvement)
- Connection frames (expect 5-8% improvement)
- OSD operations (expect 10-15% improvement)

**Step 3: Update PROFILING.md**

Add Phase 3 results section:
```markdown
### Phase 3 Optimizations (as of 2026-03-05)

**Improvements over Phase 2 baseline:**
- Message encoding: X% faster (was Y ns, now Z ns)
- Connection frames: X% faster (was Y ns, now Z ns)
- OSD operations: X% faster (was Y ns, now Z ns)

**Optimizations applied:**
- Arc for shared message ownership
- Pre-allocated buffers with encoded_size hints
- Reduced string allocations
- Optimized frame assembly
```

**Step 4: Commit**

```bash
git add docs/PROFILING.md
git commit -m "perf: document Phase 3 optimization results

Record performance improvements:
- Overall throughput: +X%
- Message encoding: +Y%
- Frame assembly: +Z%

All optimizations maintain correctness (tests pass).

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Verification Checklist

After completing all tasks, verify:

- [ ] All unit tests pass: `cargo test --workspace --lib`
- [ ] All integration tests pass: `CEPH_CONF=/home/kefu/dev/ceph/build/ceph.conf cargo test --tests -- --ignored`
- [ ] Benchmarks show 10-20% improvement in targeted areas
- [ ] No performance regressions in other areas
- [ ] Memory usage hasn't increased significantly
- [ ] All changes committed with descriptive messages

---

## Success Metrics

**Phase 3 Complete When:**
- ✅ 10-20% throughput improvement in hot paths
- ✅ Reduced allocation overhead visible in flamegraphs
- ✅ All tests passing
- ✅ Benchmarks documented

**Expected Outcomes:**
- Faster operation submission (Arc eliminates clones)
- Faster encoding (pre-allocated buffers)
- Reduced GC pressure (fewer allocations)
- Maintained correctness (all tests pass)

---

## Notes

- Focus on hot paths identified in Phase 2 profiling
- Measure before and after each optimization
- Don't optimize cold paths (diminishing returns)
- Keep changes focused and testable
- Document actual improvements (not estimates)
