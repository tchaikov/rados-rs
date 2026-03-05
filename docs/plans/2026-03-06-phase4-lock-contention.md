# Phase 4: Lock Contention Reduction Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Improve scalability under concurrent load by 20-30%

**Architecture:** Replace coarse-grained locks with fine-grained or lock-free data structures. Use DashMap for concurrent access, split monolithic state locks, and release locks before async operations.

**Tech Stack:** Rust, DashMap (lock-free concurrent HashMap), parking_lot (faster RwLock), Arc for shared ownership

---

## Overview

Phase 4 focuses on reducing lock contention to improve concurrent throughput:
- Current: RwLock on pending_ops serializes all operation access
- Target: 20-30% improvement in concurrent throughput
- Approach: DashMap for lock-free access, split state locks, release before await

**Key Principle:** "Locks should be as fine-grained as possible and held for minimal time"

---

## Task 1: Replace pending_ops RwLock with DashMap

**Files:**
- Modify: `crates/osdclient/src/session.rs`
- Modify: `Cargo.toml` (add dashmap dependency)
- Test: `crates/osdclient/src/session.rs` (existing tests)

**Context:** Currently, `pending_ops: Arc<RwLock<HashMap<u64, PendingOp>>>` serializes all access. DashMap provides lock-free concurrent access with internal sharding.

**Step 1: Add dashmap dependency**

In `crates/osdclient/Cargo.toml`, add:
```toml
dashmap = "5.5"
```

**Step 2: Change pending_ops type**

In `crates/osdclient/src/session.rs`, find the OSDSession struct:

OLD:
```rust
pub struct OSDSession {
    pending_ops: Arc<RwLock<HashMap<u64, PendingOp>>>,
    // ...
}
```

NEW:
```rust
use dashmap::DashMap;

pub struct OSDSession {
    pending_ops: Arc<DashMap<u64, PendingOp>>,
    // ...
}
```

**Step 3: Update initialization**

Find where `pending_ops` is initialized:

OLD:
```rust
pending_ops: Arc::new(RwLock::new(HashMap::new())),
```

NEW:
```rust
pending_ops: Arc::new(DashMap::new()),
```

**Step 4: Update insert operations**

Find all `.write().await` followed by `.insert()`:

OLD:
```rust
let mut pending = self.pending_ops.write().await;
pending.insert(tid, PendingOp { ... });
```

NEW:
```rust
self.pending_ops.insert(tid, PendingOp { ... });
```

**Step 5: Update remove operations**

Find all `.write().await` followed by `.remove()`:

OLD:
```rust
let mut pending = pending_ops.write().await;
pending.remove(&tid)
```

NEW:
```rust
pending_ops.remove(&tid).map(|(_, v)| v)
```

**Step 6: Update read operations**

Find all `.read().await` followed by `.get()`:

OLD:
```rust
let pending = self.pending_ops.read().await;
if let Some(op) = pending.get(&tid) {
    // use op
}
```

NEW:
```rust
if let Some(op) = self.pending_ops.get(&tid) {
    // use op (op is a Ref guard)
}
```

**Step 7: Update iteration**

Find all `.read().await` followed by iteration:

OLD:
```rust
let pending = self.pending_ops.read().await;
for (tid, op) in pending.iter() {
    // ...
}
```

NEW:
```rust
for entry in self.pending_ops.iter() {
    let (tid, op) = entry.pair();
    // ...
}
```

**Step 8: Run tests**

```bash
cargo test -p osdclient --lib
```

Expected: All tests pass

**Step 9: Run benchmarks to verify improvement**

```bash
cargo bench -p osdclient --bench operations
```

Expected: Similar or slightly better performance (main benefit is concurrent access)

**Step 10: Commit**

```bash
git add crates/osdclient/Cargo.toml crates/osdclient/src/session.rs
git commit -m "perf: replace pending_ops RwLock with DashMap

Use DashMap for lock-free concurrent access:
- Eliminates RwLock serialization point
- Internal sharding for concurrent operations
- No .write().await or .read().await needed

Enables better concurrent throughput scaling.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 2: Split MonClient State Lock

**Files:**
- Modify: `crates/monclient/src/client.rs`
- Test: `crates/monclient/src/client.rs` (existing tests)

**Context:** Currently, a single `state: Arc<RwLock<MonClientState>>` protects monmap, subscriptions, and commands. Split into separate locks to reduce contention.

**Step 1: Identify current state structure**

Read `crates/monclient/src/client.rs` around line 132 to find:
```rust
struct MonClientState {
    monmap: MonMap,
    subscriptions: HashMap<String, Subscription>,
    pending_commands: HashMap<u64, CommandState>,
    // ...
}
```

**Step 2: Split into separate locks**

Replace single state lock with individual locks:

OLD:
```rust
pub struct MonClient {
    state: Arc<RwLock<MonClientState>>,
    // ...
}
```

NEW:
```rust
pub struct MonClient {
    monmap: Arc<RwLock<MonMap>>,
    subscriptions: Arc<RwLock<HashMap<String, Subscription>>>,
    pending_commands: Arc<RwLock<HashMap<u64, CommandState>>>,
    // ... other fields
}
```

**Step 3: Update initialization**

OLD:
```rust
state: Arc::new(RwLock::new(MonClientState {
    monmap,
    subscriptions: HashMap::new(),
    pending_commands: HashMap::new(),
})),
```

NEW:
```rust
monmap: Arc::new(RwLock::new(monmap)),
subscriptions: Arc::new(RwLock::new(HashMap::new())),
pending_commands: Arc::new(RwLock::new(HashMap::new())),
```

**Step 4: Update monmap access**

Find all `state.read().await.monmap` or `state.write().await.monmap`:

OLD:
```rust
let state = self.state.read().await;
let mon_addr = state.monmap.get_monitor(rank);
```

NEW:
```rust
let monmap = self.monmap.read().await;
let mon_addr = monmap.get_monitor(rank);
```

**Step 5: Update subscriptions access**

Find all `state.write().await.subscriptions`:

OLD:
```rust
let mut state = self.state.write().await;
state.subscriptions.insert(key, sub);
```

NEW:
```rust
let mut subscriptions = self.subscriptions.write().await;
subscriptions.insert(key, sub);
```

**Step 6: Update commands access**

Find all `state.write().await.pending_commands`:

OLD:
```rust
let mut state = self.state.write().await;
state.pending_commands.insert(tid, cmd);
```

NEW:
```rust
let mut commands = self.pending_commands.write().await;
commands.insert(tid, cmd);
```

**Step 7: Run tests**

```bash
cargo test -p monclient --lib
```

Expected: All tests pass

**Step 8: Run integration tests**

```bash
CEPH_CONF=/home/kefu/dev/ceph/build/ceph.conf cargo test -p monclient --tests -- --ignored
```

Expected: All tests pass

**Step 9: Commit**

```bash
git add crates/monclient/src/client.rs
git commit -m "perf: split MonClient state into separate locks

Replace single state lock with fine-grained locks:
- monmap: Arc<RwLock<MonMap>>
- subscriptions: Arc<RwLock<HashMap<...>>>
- pending_commands: Arc<RwLock<HashMap<...>>>

Reduces lock contention by allowing concurrent access
to different state components.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 3: Release Locks Before Async Operations

**Files:**
- Modify: `crates/osdclient/src/client.rs`
- Test: `crates/osdclient/src/client.rs` (existing tests)

**Context:** Holding locks across `.await` points blocks other tasks. Clone Arc-wrapped data before awaiting.

**Step 1: Identify lock-held-across-await patterns**

Search `crates/osdclient/src/client.rs` for patterns like:
```rust
let state = self.state.read().await;
// ... use state ...
some_async_call().await;  // BAD: lock held across await
```

**Step 2: Fix pattern in handle_osdmap (around line 276-300)**

OLD:
```rust
let state = self.state.read().await;
let sessions = state.sessions.clone();
drop(state);  // Explicit drop, but could be clearer

for session in sessions.values() {
    session.update_osdmap(new_map.clone()).await;
}
```

NEW:
```rust
// Clone Arc before releasing lock
let sessions = {
    let state = self.state.read().await;
    state.sessions.clone()
};  // Lock released here

// Now await without holding lock
for session in sessions.values() {
    session.update_osdmap(new_map.clone()).await;
}
```

**Step 3: Search for other instances**

Use grep to find potential issues:
```bash
cd crates/osdclient
grep -n "\.read().await" src/client.rs | grep -A 5 "\.await"
grep -n "\.write().await" src/client.rs | grep -A 5 "\.await"
```

Fix any instances where locks are held across await points.

**Step 4: Run tests**

```bash
cargo test -p osdclient --lib
```

Expected: All tests pass

**Step 5: Run integration tests**

```bash
CEPH_CONF=/home/kefu/dev/ceph/build/ceph.conf cargo test -p osdclient --tests -- --ignored
```

Expected: All tests pass

**Step 6: Commit**

```bash
git add crates/osdclient/src/client.rs
git commit -m "perf: release locks before async operations

Use scoped blocks to release locks before .await:
- Clone Arc-wrapped data inside lock scope
- Release lock before awaiting
- Prevents blocking other tasks

Improves concurrent throughput.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 4: Add parking_lot for Faster RwLock

**Files:**
- Modify: `Cargo.toml` (workspace dependencies)
- Modify: `crates/osdclient/src/client.rs`
- Modify: `crates/monclient/src/client.rs`
- Test: Existing tests

**Context:** `parking_lot::RwLock` is 2-3x faster than `std::sync::RwLock` in contended scenarios and has a simpler API (no poisoning).

**Step 1: Add parking_lot to workspace dependencies**

In root `Cargo.toml`, add to `[workspace.dependencies]`:
```toml
parking_lot = "0.12"
```

**Step 2: Add parking_lot to osdclient**

In `crates/osdclient/Cargo.toml`:
```toml
parking_lot = { workspace = true }
```

**Step 3: Replace std::sync::RwLock imports**

In `crates/osdclient/src/client.rs`:

OLD:
```rust
use std::sync::{Arc, RwLock};
```

NEW:
```rust
use std::sync::Arc;
use parking_lot::RwLock;
```

**Step 4: Remove .await from lock operations**

parking_lot locks are synchronous, not async:

OLD:
```rust
let state = self.state.read().await;
```

NEW:
```rust
let state = self.state.read();
```

OLD:
```rust
let mut state = self.state.write().await;
```

NEW:
```rust
let mut state = self.state.write();
```

**Step 5: Repeat for monclient**

Apply same changes to `crates/monclient/src/client.rs`:
- Add parking_lot dependency
- Replace imports
- Remove .await from lock operations

**Step 6: Run tests**

```bash
cargo test -p osdclient --lib
cargo test -p monclient --lib
```

Expected: All tests pass

**Step 7: Run benchmarks**

```bash
cargo bench -p osdclient --bench operations
```

Expected: 2-5% improvement in concurrent scenarios

**Step 8: Commit**

```bash
git add Cargo.toml crates/osdclient/Cargo.toml crates/osdclient/src/client.rs crates/monclient/Cargo.toml crates/monclient/src/client.rs
git commit -m "perf: use parking_lot for faster RwLock

Replace std::sync::RwLock with parking_lot::RwLock:
- 2-3x faster in contended scenarios
- Simpler API (no poisoning, synchronous)
- Smaller memory footprint

Benchmark improvement: ~X% in concurrent access.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 5: Add Concurrent Benchmark

**Files:**
- Create: `crates/osdclient/benches/concurrent.rs`
- Modify: `crates/osdclient/Cargo.toml`

**Context:** Need benchmarks to measure concurrent throughput improvements.

**Step 1: Create concurrent benchmark file**

Create `crates/osdclient/benches/concurrent.rs`:

```rust
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use osdclient::messages::MOSDOp;
use osdclient::types::{OSDOp, ObjectId, RequestId, StripedPgId};
use std::sync::Arc;
use tokio::runtime::Runtime;

fn bench_concurrent_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_ops");

    let rt = Runtime::new().unwrap();

    for num_threads in [1, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("submit", num_threads),
            &num_threads,
            |b, &num_threads| {
                b.iter(|| {
                    rt.block_on(async {
                        let handles: Vec<_> = (0..num_threads)
                            .map(|i| {
                                tokio::spawn(async move {
                                    for _ in 0..100 {
                                        let object = ObjectId::new(1, &format!("obj_{}", i));
                                        let pgid = StripedPgId::from_pg(1, 0x12345678);
                                        let ops = vec![OSDOp::read(0, 4096)];
                                        let reqid = RequestId::new("client.0", 1, i as u64);
                                        let mosdop = MOSDOp::new(1, 1, 0, object, pgid, ops, reqid, 0);
                                        black_box(Arc::new(mosdop));
                                    }
                                })
                            })
                            .collect();

                        for handle in handles {
                            handle.await.unwrap();
                        }
                    });
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_concurrent_operations);
criterion_main!(benches);
```

**Step 2: Add benchmark to Cargo.toml**

In `crates/osdclient/Cargo.toml`, add:
```toml
[[bench]]
name = "concurrent"
harness = false
```

**Step 3: Run benchmark**

```bash
cargo bench -p osdclient --bench concurrent
```

Expected: Shows scaling with number of threads

**Step 4: Commit**

```bash
git add crates/osdclient/benches/concurrent.rs crates/osdclient/Cargo.toml
git commit -m "perf: add concurrent operations benchmark

Add benchmark to measure concurrent throughput:
- Tests 1, 2, 4, 8 threads
- Measures operation submission scaling
- Baseline for lock contention improvements

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 6: Document Phase 4 Results

**Files:**
- Modify: `docs/PROFILING.md`

**Context:** Document lock contention improvements and concurrent throughput gains.

**Step 1: Run all benchmarks**

```bash
cargo bench --workspace 2>&1 | tee phase4_results.txt
```

**Step 2: Compare concurrent benchmark results**

Compare before/after for concurrent benchmark:
- 1 thread: Should be similar (no contention)
- 2 threads: Expect 10-15% improvement
- 4 threads: Expect 20-25% improvement
- 8 threads: Expect 25-30% improvement

**Step 3: Update PROFILING.md**

Add Phase 4 results section:

```markdown
### Phase 4 Lock Contention Reduction (as of 2026-03-06)

**Improvements over Phase 3:**
- Concurrent throughput (2 threads): +X%
- Concurrent throughput (4 threads): +Y%
- Concurrent throughput (8 threads): +Z%

**Optimizations applied:**
1. DashMap for pending_ops (Task 1)
   - Lock-free concurrent access
   - Internal sharding eliminates serialization

2. Split MonClient state locks (Task 2)
   - Separate locks for monmap, subscriptions, commands
   - Allows concurrent access to different components

3. Release locks before async operations (Task 3)
   - Clone Arc before .await
   - Prevents blocking other tasks

4. parking_lot RwLock (Task 4)
   - 2-3x faster than std::sync::RwLock
   - Simpler API, smaller memory footprint

**Commits:**
- [hash] perf: replace pending_ops RwLock with DashMap
- [hash] perf: split MonClient state into separate locks
- [hash] perf: release locks before async operations
- [hash] perf: use parking_lot for faster RwLock
```

**Step 4: Commit**

```bash
git add docs/PROFILING.md
git commit -m "perf: document Phase 4 lock contention results

Record concurrent throughput improvements:
- 2 threads: +X%
- 4 threads: +Y%
- 8 threads: +Z%

All optimizations maintain correctness (tests pass).

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Verification Checklist

After completing all tasks, verify:

- [ ] All unit tests pass: `cargo test --workspace --lib`
- [ ] All integration tests pass: `CEPH_CONF=/home/kefu/dev/ceph/build/ceph.conf cargo test --tests -- --ignored`
- [ ] Concurrent benchmarks show 20-30% improvement at 4+ threads
- [ ] Single-threaded performance not regressed
- [ ] No deadlocks or race conditions
- [ ] All changes committed with descriptive messages

---

## Success Metrics

**Phase 4 Complete When:**
- ✅ 20-30% concurrent throughput improvement (4+ threads)
- ✅ DashMap eliminates pending_ops serialization
- ✅ Split locks reduce MonClient contention
- ✅ All tests passing

**Expected Outcomes:**
- Better scaling with concurrent operations
- Reduced lock hold times
- No blocking across await points
- Faster lock operations (parking_lot)

---

## Notes

- Focus on concurrent access patterns
- Measure with concurrent benchmarks
- Don't optimize single-threaded paths
- Keep changes focused and testable
- Document actual improvements (not estimates)
