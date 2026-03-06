# Phase 5: Function Decomposition Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Improve maintainability and testability by decomposing complex functions

**Architecture:** Break down large functions (>100 lines) into smaller, focused functions with single responsibilities. Extract reusable logic into helper functions.

**Tech Stack:** Rust refactoring, maintaining existing tests

---

## Overview

Phase 5 focuses on improving code maintainability by decomposing complex functions:
- Current: Several functions exceed 100-200 lines
- Target: No function exceeds 100 lines
- Approach: Extract logical sections into helper functions with clear names

**Key Principle:** "Each function should do one thing and do it well"

---

## Task 1: Refactor osdclient::list() Function

**Files:**
- Modify: `crates/osdclient/src/client.rs`
- Test: Existing tests should continue to pass

**Context:** The `list()` function is ~240 lines and handles cursor parsing, PG querying, and result building. Split into focused helper functions.

**Step 1: Read and analyze the list() function**

Read `crates/osdclient/src/client.rs` and find the `list()` function. Understand its structure:
- Cursor parsing logic
- PG object querying
- Result building and pagination

**Step 2: Extract cursor parsing logic**

Create helper function:
```rust
/// Parse list cursor from string format
fn parse_list_cursor(cursor: Option<&str>) -> Result<(Option<StripedPgId>, Option<String>)> {
    // Extract cursor parsing logic
    // Returns (starting_pgid, starting_object)
}
```

**Step 3: Extract PG querying logic**

Create helper function:
```rust
/// Query objects from a specific PG
async fn query_pg_objects(
    &self,
    pgid: StripedPgId,
    start_object: Option<&str>,
    max_entries: u32,
) -> Result<Vec<ObjectEntry>> {
    // Extract PG querying logic
}
```

**Step 4: Extract result building logic**

Create helper function:
```rust
/// Build list result with pagination cursor
fn build_list_result(
    objects: Vec<ObjectEntry>,
    max_entries: u32,
    current_pgid: StripedPgId,
    last_object: Option<String>,
) -> ListResult {
    // Extract result building logic
}
```

**Step 5: Refactor list() to use helpers**

Simplify the main `list()` function to orchestrate the helpers:
```rust
pub async fn list(&self, pool: i64, cursor: Option<&str>, max_entries: u32) -> Result<ListResult> {
    let (start_pgid, start_object) = parse_list_cursor(cursor)?;

    // Get PGs for pool
    let pgs = self.get_pgs_for_pool(pool).await?;

    // Query objects
    let objects = self.query_pg_objects(start_pgid, start_object.as_deref(), max_entries).await?;

    // Build result
    Ok(build_list_result(objects, max_entries, current_pgid, last_object))
}
```

**Step 6: Run tests**

```bash
cargo test -p osdclient --lib
```

Expected: All tests pass

**Step 7: Run integration tests**

```bash
CEPH_CONF=/home/kefu/dev/ceph/build/ceph.conf cargo test -p osdclient --tests -- --ignored
```

Expected: All tests pass

**Step 8: Commit**

```bash
git add crates/osdclient/src/client.rs
git commit -m "refactor: decompose list() function into helpers

Extract logical sections into focused functions:
- parse_list_cursor(): Parse cursor string
- query_pg_objects(): Query objects from PG
- build_list_result(): Build paginated result

Improves readability and testability.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 2: Refactor monclient::start_hunting() Function

**Files:**
- Modify: `crates/monclient/src/client.rs`
- Test: Existing tests should continue to pass

**Context:** The `start_hunting()` function is ~130 lines and handles monitor selection, weighted shuffling, and parallel connection attempts.

**Step 1: Read and analyze start_hunting()**

Read `crates/monclient/src/client.rs` and find `start_hunting()`. Understand:
- Monitor priority selection
- Weighted random shuffling
- Parallel connection attempts

**Step 2: Extract monitor selection logic**

Create helper function:
```rust
/// Select monitors by priority (prefer same subnet, then random)
fn select_monitors_by_priority(
    monmap: &MonMap,
    client_addr: Option<&EntityAddr>,
) -> Vec<(usize, &MonInfo)> {
    // Extract priority selection logic
}
```

**Step 3: Extract weighted shuffle logic**

Create helper function:
```rust
/// Weighted random shuffle (higher priority = more likely to be first)
fn weighted_shuffle<T>(items: Vec<(usize, T)>) -> Vec<T> {
    // Extract weighted shuffle logic
}
```

**Step 4: Extract parallel connection logic**

Create helper function:
```rust
/// Attempt to connect to monitors in parallel
async fn connect_parallel(
    &self,
    monitors: Vec<usize>,
) -> Result<Connection> {
    // Extract parallel connection logic
}
```

**Step 5: Refactor start_hunting() to use helpers**

Simplify the main function:
```rust
async fn start_hunting(&self) -> Result<Connection> {
    let monmap = self.monmap_state.read().await;
    let client_addr = self.get_client_addr();

    let prioritized = select_monitors_by_priority(&monmap, client_addr.as_ref());
    let shuffled = weighted_shuffle(prioritized);

    self.connect_parallel(shuffled).await
}
```

**Step 6: Run tests**

```bash
cargo test -p monclient --lib
```

Expected: All tests pass

**Step 7: Run integration tests**

```bash
CEPH_CONF=/home/kefu/dev/ceph/build/ceph.conf cargo test -p monclient --tests -- --ignored
```

Expected: All tests pass

**Step 8: Commit**

```bash
git add crates/monclient/src/client.rs
git commit -m "refactor: decompose start_hunting() into helpers

Extract logical sections:
- select_monitors_by_priority(): Priority-based selection
- weighted_shuffle(): Weighted random ordering
- connect_parallel(): Parallel connection attempts

Improves readability and testability.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 3: Refactor osdclient::handle_osdmap() Function

**Files:**
- Modify: `crates/osdclient/src/client.rs`
- Test: Existing tests should continue to pass

**Context:** The `handle_osdmap()` function is ~170 lines and handles state updates, session management, and subscription handling.

**Step 1: Read and analyze handle_osdmap()**

Read `crates/osdclient/src/client.rs` and find `handle_osdmap()`. Understand:
- OSDMap state update
- Session creation/removal based on new map
- Subscription management

**Step 2: Extract state update logic**

Create helper function:
```rust
/// Update OSDMap state and return affected sessions
async fn update_osdmap_state(
    &self,
    new_map: Arc<MOSDMap>,
) -> Result<Vec<i32>> {
    // Extract state update logic
    // Returns list of OSD IDs that need session updates
}
```

**Step 3: Extract session management logic**

Create helper function:
```rust
/// Update sessions based on new OSDMap
async fn update_sessions_for_osdmap(
    &self,
    new_map: Arc<MOSDMap>,
    affected_osds: Vec<i32>,
) -> Result<()> {
    // Extract session update logic
}
```

**Step 4: Extract subscription handling logic**

Create helper function:
```rust
/// Handle OSDMap subscription updates
async fn handle_osdmap_subscription(
    &self,
    new_map: &MOSDMap,
) -> Result<()> {
    // Extract subscription logic
}
```

**Step 5: Refactor handle_osdmap() to use helpers**

Simplify the main function:
```rust
async fn handle_osdmap(&self, new_map: MOSDMap) -> Result<()> {
    let new_map = Arc::new(new_map);

    let affected_osds = self.update_osdmap_state(new_map.clone()).await?;
    self.update_sessions_for_osdmap(new_map.clone(), affected_osds).await?;
    self.handle_osdmap_subscription(&new_map).await?;

    Ok(())
}
```

**Step 6: Run tests**

```bash
cargo test -p osdclient --lib
```

Expected: All tests pass

**Step 7: Run integration tests**

```bash
CEPH_CONF=/home/kefu/dev/ceph/build/ceph.conf cargo test -p osdclient --tests -- --ignored
```

Expected: All tests pass

**Step 8: Commit**

```bash
git add crates/osdclient/src/client.rs
git commit -m "refactor: decompose handle_osdmap() into helpers

Extract logical sections:
- update_osdmap_state(): Update state and identify changes
- update_sessions_for_osdmap(): Manage session lifecycle
- handle_osdmap_subscription(): Handle subscriptions

Improves readability and testability.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 4: Identify and Refactor Other Long Functions

**Files:**
- Various files in `crates/osdclient/src/` and `crates/monclient/src/`
- Test: Existing tests should continue to pass

**Context:** Find other functions exceeding 100 lines and refactor them.

**Step 1: Find long functions**

```bash
# Find functions longer than 100 lines
cd crates/osdclient/src
for file in *.rs; do
    echo "=== $file ==="
    grep -n "^[[:space:]]*\(pub \)\?async fn\|^[[:space:]]*\(pub \)\?fn" "$file" | head -20
done
```

**Step 2: Prioritize by complexity**

Focus on functions that:
- Are called frequently (hot paths)
- Have complex logic (multiple responsibilities)
- Would benefit from testing individual pieces

**Step 3: Refactor each function**

For each long function:
1. Identify logical sections
2. Extract into helper functions
3. Update main function to use helpers
4. Run tests to verify

**Step 4: Run all tests**

```bash
cargo test --workspace --lib
```

Expected: All tests pass

**Step 5: Commit**

```bash
git add -A
git commit -m "refactor: decompose remaining long functions

Break down complex functions into focused helpers:
- [List specific functions refactored]

All functions now under 100 lines.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 5: Document Phase 5 Results

**Files:**
- Create: `docs/REFACTORING.md`

**Context:** Document the refactoring work and its benefits.

**Step 1: Create REFACTORING.md**

```markdown
# Code Refactoring Guide

## Phase 5: Function Decomposition (2026-03-06)

### Goals
- No function exceeds 100 lines
- Each function has single responsibility
- Improved testability and readability

### Functions Refactored

#### osdclient::list() (was 240 lines)
- `parse_list_cursor()`: Parse pagination cursor
- `query_pg_objects()`: Query objects from PG
- `build_list_result()`: Build paginated result

#### monclient::start_hunting() (was 130 lines)
- `select_monitors_by_priority()`: Priority-based selection
- `weighted_shuffle()`: Weighted random ordering
- `connect_parallel()`: Parallel connection attempts

#### osdclient::handle_osdmap() (was 170 lines)
- `update_osdmap_state()`: Update state and identify changes
- `update_sessions_for_osdmap()`: Manage session lifecycle
- `handle_osdmap_subscription()`: Handle subscriptions

### Benefits
- **Testability**: Helper functions can be unit tested independently
- **Readability**: Main functions show high-level flow
- **Maintainability**: Changes isolated to specific helpers
- **Reusability**: Helpers can be used in multiple places

### Guidelines
- Functions should be under 100 lines
- Extract logical sections with clear names
- Maintain existing test coverage
- Document complex helper functions
```

**Step 2: Commit**

```bash
git add docs/REFACTORING.md
git commit -m "docs: document Phase 5 function decomposition

Record refactoring work:
- Functions refactored and their helpers
- Benefits for testability and maintainability
- Guidelines for future refactoring

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Verification Checklist

After completing all tasks, verify:

- [ ] All functions under 100 lines
- [ ] All unit tests pass: `cargo test --workspace --lib`
- [ ] All integration tests pass: `CEPH_CONF=/home/kefu/dev/ceph/build/ceph.conf cargo test --tests -- --ignored`
- [ ] No functionality changes (pure refactoring)
- [ ] Code is more readable and maintainable
- [ ] All changes committed with descriptive messages

---

## Success Metrics

**Phase 5 Complete When:**
- ✅ All functions under 100 lines
- ✅ Complex functions decomposed into helpers
- ✅ All tests passing
- ✅ Refactoring documented

**Expected Outcomes:**
- Improved code readability
- Better testability (helpers can be tested independently)
- Easier maintenance (changes isolated to specific functions)
- No functionality changes (pure refactoring)

---

## Notes

- This is pure refactoring - no functionality changes
- Maintain existing test coverage
- Focus on readability and maintainability
- Extract helpers with clear, descriptive names
- Keep changes focused and testable
