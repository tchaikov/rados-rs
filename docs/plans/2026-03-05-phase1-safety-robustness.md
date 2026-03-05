# Phase 1: Safety & Robustness Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Eliminate panic points and improve error handling across the rados-rs codebase

**Architecture:** Replace `.unwrap()` and `.expect()` calls with proper error handling using `?` operator, `.unwrap_or_default()`, or contextual error messages. Focus on production code paths first (non-test, non-example code).

**Tech Stack:** Rust, thiserror, anyhow, tracing

---

## Overview

This plan addresses **718 `.unwrap()` calls** across 66 files in the rados-rs codebase. The analysis shows:

**Unwrap Categories:**
1. **Test code** (~60%): 430+ unwraps in test functions - LOW PRIORITY
2. **Production code** (~40%): 288 unwraps in library code - HIGH PRIORITY

**Production Code Breakdown by File:**
- `osdclient/src/pgmap_types.rs`: 20 unwraps (all in tests)
- `denc/src/denc.rs`: 30 unwraps (mostly in tests)
- `auth/src/protocol.rs`: 20 unwraps (all in tests)
- `cephconfig/src/types.rs`: 39 unwraps (all in tests)
- `monclient/src/client.rs`: 2 unwraps in production code
- `osdclient/src/types.rs`: 8 `.expect()` in production code
- `osdclient/src/throttle.rs`: 2 `.expect()` in production code
- `msgr2/src/state_machine.rs`: 4 `.expect()` in production code
- `msgr2/src/frames.rs`: 2 `.expect()` in production code

**Strategy:**
1. Focus on production code first (non-test files)
2. Leave test code unwraps as-is (acceptable in tests)
3. Replace dangerous unwraps with proper error handling
4. Add context to error messages for debugging

---

## Task 1: Fix Lock Poisoning Errors in MonClient

**Files:**
- Modify: `crates/monclient/src/client.rs:909`

**Context:** The MonClient uses `.expect()` for lock acquisition with message "tasks lock should not be contended during init". This can panic if the lock is poisoned.

**Step 1: Locate the problematic code**

```bash
grep -n "tasks lock should not be contended" crates/monclient/src/client.rs
```

Expected: Line 909

**Step 2: Read the surrounding context**

Read `crates/monclient/src/client.rs` lines 900-920 to understand the lock usage pattern.

**Step 3: Replace expect with proper error handling**

Current code:
```rust
.expect("tasks lock should not be contended during init")
```

Replace with:
```rust
.map_err(|e| MonClientError::Other(format!("Failed to acquire tasks lock during init: {}", e)))?
```

**Step 4: Verify the change compiles**

Run: `cargo check -p monclient`
Expected: No errors

**Step 5: Run tests**

Run: `cargo test -p monclient --lib`
Expected: All tests pass

**Step 6: Commit**

```bash
git add crates/monclient/src/client.rs
git commit -m "fix: replace expect with proper error handling in MonClient lock

Replace .expect() with proper error propagation for tasks lock acquisition.
This prevents panics if the lock is poisoned and provides better error context.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 2: Fix Encoding Expects in OSDClient Types

**Files:**
- Modify: `crates/osdclient/src/types.rs:multiple locations`

**Context:** The types.rs file has 8 `.expect()` calls for encoding operations that should never fail, but could in theory if buffer operations fail.

**Step 1: Find all expect calls in types.rs**

```bash
grep -n "\.expect(" crates/osdclient/src/types.rs
```

Expected: 8 locations with encoding-related expects

**Step 2: Read the file to understand context**

Read `crates/osdclient/src/types.rs` to see the encoding patterns.

**Step 3: Replace expects with unwrap_or_else for better error messages**

For each `.expect("Failed to encode X")` pattern, replace with:

```rust
// Before:
.expect("Failed to encode HObject cursor")

// After:
.unwrap_or_else(|e| {
    tracing::error!("Failed to encode HObject cursor: {}", e);
    Bytes::new()  // Return empty bytes on error
})
```

Or if the function returns Result, use `?` operator:

```rust
// Before:
Denc::encode(&request, &mut buf, 0).expect("Failed to encode lock request");

// After:
Denc::encode(&request, &mut buf, 0)?;
```

**Step 4: Update function signatures if needed**

If changing from `.expect()` to `?`, ensure the function returns `Result<T, E>`.

**Step 5: Verify compilation**

Run: `cargo check -p osdclient`
Expected: No errors

**Step 6: Run tests**

Run: `cargo test -p osdclient --lib`
Expected: All tests pass

**Step 7: Commit**

```bash
git add crates/osdclient/src/types.rs
git commit -m "fix: replace expect with proper error handling in OSDClient types

Replace .expect() calls in encoding operations with proper error handling.
Use ? operator where functions return Result, or unwrap_or_else with
logging for infallible contexts.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 3: Fix Semaphore Expects in Throttle

**Files:**
- Modify: `crates/osdclient/src/throttle.rs:multiple locations`

**Context:** The throttle module uses `.expect("Semaphore should not be closed")` which can panic if the semaphore is unexpectedly closed.

**Step 1: Find expect calls in throttle.rs**

```bash
grep -n "Semaphore should not be closed" crates/osdclient/src/throttle.rs
```

Expected: 2 locations

**Step 2: Read the throttle implementation**

Read `crates/osdclient/src/throttle.rs` to understand the semaphore usage.

**Step 3: Replace expects with proper error handling**

```rust
// Before:
.expect("Semaphore should not be closed")

// After:
.map_err(|_| OSDClientError::Other("Throttle semaphore closed unexpectedly".into()))?
```

**Step 4: Verify the error type exists**

Check that `OSDClientError::Other` variant exists, or add it if needed.

**Step 5: Verify compilation**

Run: `cargo check -p osdclient`
Expected: No errors

**Step 6: Run tests**

Run: `cargo test -p osdclient --lib`
Expected: All tests pass

**Step 7: Commit**

```bash
git add crates/osdclient/src/throttle.rs
git commit -m "fix: replace expect with error handling in throttle semaphore

Replace .expect() with proper error propagation for semaphore operations.
This prevents panics if the semaphore is closed and provides better error context.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 4: Fix State Machine Downcast Expects in Msgr2

**Files:**
- Modify: `crates/msgr2/src/state_machine.rs:multiple locations`

**Context:** The state machine uses `.expect()` for type downcasting with messages like "State kind is AUTH_CONNECTING_SIGN but downcast failed". These should return proper errors.

**Step 1: Find downcast expects**

```bash
grep -n "downcast failed" crates/msgr2/src/state_machine.rs
```

Expected: 4 locations

**Step 2: Read the state machine code**

Read `crates/msgr2/src/state_machine.rs` to understand the downcast pattern.

**Step 3: Replace expects with proper error handling**

```rust
// Before:
.expect("State kind is AUTH_CONNECTING_SIGN but downcast failed")

// After:
.ok_or_else(|| {
    Msgr2Error::Protocol(format!(
        "State machine error: expected AUTH_CONNECTING_SIGN state but downcast failed"
    ))
})?
```

**Step 4: Verify error type exists**

Check that `Msgr2Error::Protocol` variant exists.

**Step 5: Verify compilation**

Run: `cargo check -p msgr2`
Expected: No errors

**Step 6: Run tests**

Run: `cargo test -p msgr2 --lib`
Expected: All tests pass

**Step 7: Commit**

```bash
git add crates/msgr2/src/state_machine.rs
git commit -m "fix: replace expect with error handling in state machine downcasts

Replace .expect() with proper error propagation for state downcasting.
This prevents panics on unexpected state transitions and provides better
error context for debugging protocol issues.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 5: Fix Frame Assembly Expects in Msgr2

**Files:**
- Modify: `crates/msgr2/src/frames.rs:multiple locations`

**Context:** Frame assembly uses `.expect("Failed to assemble frame")` and `.expect("Failed to encode field")`.

**Step 1: Find frame expects**

```bash
grep -n "\.expect(" crates/msgr2/src/frames.rs
```

Expected: 2 locations

**Step 2: Read the frames code**

Read `crates/msgr2/src/frames.rs` to understand frame assembly.

**Step 3: Replace expects with ? operator**

```rust
// Before:
.expect("Failed to assemble frame")

// After:
?
```

Ensure the function returns `Result<T, Msgr2Error>`.

**Step 4: Verify compilation**

Run: `cargo check -p msgr2`
Expected: No errors

**Step 5: Run tests**

Run: `cargo test -p msgr2 --lib`
Expected: All tests pass

**Step 6: Commit**

```bash
git add crates/msgr2/src/frames.rs
git commit -m "fix: replace expect with error propagation in frame assembly

Replace .expect() with ? operator for frame assembly and encoding.
This allows errors to propagate properly instead of panicking.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 6: Add Error Context with Tracing

**Files:**
- Modify: `crates/osdclient/src/client.rs:213`
- Modify: `crates/monclient/src/client.rs:multiple locations`

**Context:** Some error messages lack context about what operation was being performed.

**Step 1: Identify generic error messages**

```bash
grep -n "OSDMap not available" crates/osdclient/src/client.rs
grep -n "Channel closed" crates/monclient/src/client.rs
```

**Step 2: Add context to error messages**

```rust
// Before:
Err(OSDClientError::Connection("OSDMap not available".to_string()))

// After:
Err(OSDClientError::Connection(format!(
    "OSDMap not available for pool {} operation", pool_id
)))
```

**Step 3: Add tracing spans for operations**

```rust
#[tracing::instrument(skip(self), fields(pool_id))]
pub async fn get_pool_info(&self, pool_id: u64) -> Result<PoolInfo> {
    // ... implementation
}
```

**Step 4: Verify compilation**

Run: `cargo check -p osdclient -p monclient`
Expected: No errors

**Step 5: Run tests**

Run: `cargo test -p osdclient -p monclient --lib`
Expected: All tests pass

**Step 6: Commit**

```bash
git add crates/osdclient/src/client.rs crates/monclient/src/client.rs
git commit -m "feat: add error context and tracing spans

Add contextual information to error messages including operation details.
Add tracing spans to key operations for better observability.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 7: Audit and Document Remaining Unwraps

**Files:**
- Create: `docs/UNWRAP_AUDIT.md`

**Context:** Document all remaining unwraps in production code with justification.

**Step 1: Find all remaining unwraps in production code**

```bash
grep -r "\.unwrap()" --include="*.rs" crates/ | \
  grep -v "tests/" | \
  grep -v "examples/" | \
  grep -v "benches/" > /tmp/remaining_unwraps.txt
```

**Step 2: Create audit document**

```markdown
# Unwrap Audit

This document tracks all `.unwrap()` and `.expect()` calls in production code.

## Acceptable Unwraps

These unwraps are acceptable because they are in contexts where failure is impossible:

### Test Code
- All unwraps in `#[cfg(test)]` modules are acceptable
- All unwraps in `tests/` directories are acceptable
- All unwraps in `examples/` are acceptable

### Infallible Operations
- `Mutex::lock()` on newly created mutexes (no contention possible)
- Array indexing with compile-time bounds checking
- Parsing of hardcoded valid strings

## Production Code Unwraps (Remaining)

### High Priority (Must Fix)
- None remaining after Phase 1

### Medium Priority (Should Fix)
- Document any remaining unwraps here with justification

### Low Priority (Nice to Fix)
- Test-only code paths
```

**Step 3: Commit the audit document**

```bash
git add docs/UNWRAP_AUDIT.md
git commit -m "docs: add unwrap audit document

Document all remaining unwraps in production code with justification.
This provides a reference for future code reviews.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 8: Run Full Test Suite

**Files:**
- None (verification only)

**Step 1: Run all unit tests**

```bash
cargo test --workspace --lib
```

Expected: All tests pass

**Step 2: Run clippy**

```bash
cargo clippy --all-targets --all-features
```

Expected: No new warnings

**Step 3: Run format check**

```bash
cargo fmt --check
```

Expected: All files formatted correctly

**Step 4: Run integration tests (if cluster available)**

```bash
CEPH_CONF=/home/kefu/dev/ceph/build/ceph.conf cargo test -p monclient --tests -- --ignored
CEPH_CONF=/home/kefu/dev/ceph/build/ceph.conf cargo test -p osdclient --tests -- --ignored
```

Expected: All integration tests pass

---

## Task 9: Update CLAUDE.md with Error Handling Guidelines

**Files:**
- Modify: `CLAUDE.md` (add new section)

**Step 1: Add error handling section to CLAUDE.md**

Add after the "Coding Standards" section:

```markdown
### Error Handling Guidelines

**DO:**
✅ Use `?` operator for error propagation in functions returning Result
✅ Use `.unwrap_or_default()` for recoverable cases with sensible defaults
✅ Use `.ok_or_else()` to convert Option to Result with context
✅ Add tracing spans to operations for debugging
✅ Include operation context in error messages (pool_id, object_id, etc.)
✅ Use `.expect()` ONLY in tests or truly impossible cases with detailed explanations

**DON'T:**
❌ Use `.unwrap()` in production code paths
❌ Use `.expect()` without detailed justification
❌ Discard errors with `|_|` in map_err
❌ Use generic error messages like "failed" or "error"
❌ Panic on recoverable errors

**Examples:**

```rust
// ✅ GOOD: Proper error propagation
pub async fn get_pool(&self, pool_id: u64) -> Result<Pool> {
    let osdmap = self.get_osdmap().await?;
    let pool = osdmap.pools.get(&pool_id)
        .ok_or_else(|| OSDClientError::PoolNotFound(pool_id))?;
    Ok(pool.clone())
}

// ✅ GOOD: Recoverable with default
let timeout = config.timeout.unwrap_or(Duration::from_secs(30));

// ✅ GOOD: Tracing span with context
#[tracing::instrument(skip(self), fields(pool_id, object_id))]
pub async fn read_object(&self, pool_id: u64, object_id: &str) -> Result<Bytes> {
    // ... implementation
}

// ❌ BAD: Unwrap in production code
let pool = osdmap.pools.get(&pool_id).unwrap();

// ❌ BAD: Generic error message
.map_err(|_| Error::Other("failed".into()))?

// ❌ BAD: Discarding error context
.map_err(|_| Error::Other("operation failed".into()))?
```
```

**Step 2: Commit the documentation update**

```bash
git add CLAUDE.md
git commit -m "docs: add error handling guidelines to CLAUDE.md

Add comprehensive error handling guidelines with examples of good and bad
patterns. This helps maintain consistency in error handling across the codebase.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Verification Checklist

After completing all tasks, verify:

- [ ] Zero `.unwrap()` in production code paths (non-test, non-example)
- [ ] All `.expect()` calls have detailed justifications or are replaced
- [ ] Error messages include operation context
- [ ] Tracing spans added to key operations
- [ ] All unit tests pass: `cargo test --workspace --lib`
- [ ] No clippy warnings: `cargo clippy --all-targets --all-features`
- [ ] Code formatted: `cargo fmt`
- [ ] Integration tests pass (if cluster available)
- [ ] Documentation updated with error handling guidelines
- [ ] Unwrap audit document created

---

## Success Metrics

**Phase 1 Complete When:**
- ✅ Zero `.unwrap()` in production code paths
- ✅ All errors have meaningful context
- ✅ No clippy warnings
- ✅ All tests pass
- ✅ Documentation updated

**Expected Impact:**
- **Safety**: Eliminates panic points in production code
- **Debuggability**: Better error messages with context
- **Maintainability**: Clear guidelines for future development

---

## Notes

- Test code unwraps are acceptable and don't need to be changed
- Focus on production code paths first
- Each task should take 15-30 minutes
- Commit after each task for easy rollback
- Run tests after each change to catch regressions early
