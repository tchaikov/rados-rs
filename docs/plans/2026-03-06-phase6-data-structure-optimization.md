# Phase 6: Data Structure Optimization Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Optimize collection usage and add caching for 30-40% reduction in routing overhead

**Architecture:** Add LRU cache for CRUSH calculations, simplify backoff tracker data structures, audit collection choices for optimal performance.

**Tech Stack:** Rust, lru crate for caching, optimized data structures

---

## Overview

Phase 6 focuses on data structure optimization and caching:
- Current: CRUSH calculations repeated for every operation
- Target: 30-40% reduction in operation routing overhead
- Approach: LRU cache for PG→OSD mappings, simplified backoff tracking

**Key Principle:** "Cache expensive calculations, simplify data structures"

---

## Task 1: Add CRUSH Calculation Cache

**Files:**
- Modify: `crates/osdclient/src/osdmap.rs`
- Modify: `crates/osdclient/Cargo.toml`
- Test: Existing tests should continue to pass

**Context:** CRUSH calculations are expensive and repeated for every operation. Add LRU cache for PG→OSD mappings.

**Step 1: Add lru dependency**

In `crates/osdclient/Cargo.toml`, add:
```toml
lru = "0.12"
```

**Step 2: Add cache to OSDMap**

In `crates/osdclient/src/osdmap.rs`, add cache field:
```rust
use lru::LruCache;
use std::num::NonZeroUsize;

pub struct OSDMap {
    // ... existing fields ...

    /// LRU cache for PG → OSD mappings
    /// Key: (pool_id, pg_num), Value: Vec<i32> (OSD list)
    #[serde(skip)]
    crush_cache: std::sync::Mutex<LruCache<(i64, u32), Vec<i32>>>,
}
```

**Step 3: Initialize cache**

In OSDMap initialization:
```rust
impl OSDMap {
    pub fn new(...) -> Self {
        Self {
            // ... existing fields ...
            crush_cache: std::sync::Mutex::new(LruCache::new(NonZeroUsize::new(1000).unwrap())),
        }
    }
}
```

**Step 4: Add cache lookup in pg_to_osds**

Modify the `pg_to_osds` method:
```rust
pub fn pg_to_osds(&self, pool: i64, pg_num: u32) -> Result<Vec<i32>> {
    let cache_key = (pool, pg_num);

    // Check cache first
    {
        let mut cache = self.crush_cache.lock().unwrap();
        if let Some(osds) = cache.get(&cache_key) {
            return Ok(osds.clone());
        }
    }

    // Cache miss - calculate
    let osds = self.calculate_pg_to_osds(pool, pg_num)?;

    // Store in cache
    {
        let mut cache = self.crush_cache.lock().unwrap();
        cache.put(cache_key, osds.clone());
    }

    Ok(osds)
}

fn calculate_pg_to_osds(&self, pool: i64, pg_num: u32) -> Result<Vec<i32>> {
    // Move existing CRUSH calculation logic here
}
```

**Step 5: Invalidate cache on OSDMap updates**

When OSDMap is updated, clear the cache:
```rust
pub fn apply_incremental(&mut self, inc: &OSDMapIncremental) -> Result<()> {
    // ... existing update logic ...

    // Clear cache after updates
    self.crush_cache.lock().unwrap().clear();

    Ok(())
}
```

**Step 6: Run tests**

```bash
cargo test -p osdclient --lib
```

Expected: All tests pass

**Step 7: Run benchmarks**

```bash
cargo bench -p osdclient --bench operations
```

Expected: 30-40% improvement in operation routing

**Step 8: Commit**

```bash
git add crates/osdclient/Cargo.toml crates/osdclient/src/osdmap.rs
git commit -m "perf: add LRU cache for CRUSH calculations

Add 1000-entry LRU cache for PG → OSD mappings:
- Cache key: (pool_id, pg_num)
- Cache value: Vec<i32> (OSD list)
- Invalidate on OSDMap updates

Expected: 30-40% reduction in routing overhead.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 2: Simplify Backoff Tracker Data Structure

**Files:**
- Modify: `crates/osdclient/src/backoff.rs`
- Test: Existing tests should continue to pass

**Context:** Current backoff tracker uses nested HashMap<PgId, BTreeMap<HObject, BackoffEntry>>. Simplify to single BTreeMap with composite key.

**Step 1: Read and analyze current structure**

Read `crates/osdclient/src/backoff.rs` to understand:
- Current data structure
- How entries are inserted
- How entries are queried
- How entries are removed

**Step 2: Design new structure**

Create composite key type:
```rust
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct BackoffKey {
    pgid: StripedPgId,
    begin: denc::HObject,
}

pub struct BackoffTracker {
    // Old: HashMap<StripedPgId, BTreeMap<HObject, BackoffEntry>>
    // New: Single BTreeMap with composite key
    entries: BTreeMap<BackoffKey, BackoffEntry>,
}
```

**Step 3: Update insert logic**

```rust
pub fn register(&mut self, entry: BackoffEntry) {
    let key = BackoffKey {
        pgid: entry.pgid,
        begin: entry.begin.clone(),
    };
    self.entries.insert(key, entry);
}
```

**Step 4: Update query logic**

```rust
pub fn check(&self, pgid: StripedPgId, hobj: &denc::HObject) -> bool {
    // Use BTreeMap range query for efficient lookup
    let start_key = BackoffKey {
        pgid,
        begin: hobj.clone(),
    };

    self.entries.range(..=start_key)
        .rev()
        .take_while(|(k, _)| k.pgid == pgid)
        .any(|(_, entry)| entry.contains(hobj))
}
```

**Step 5: Update remove logic**

```rust
pub fn remove_by_id(&mut self, pgid: StripedPgId, id: u64) {
    self.entries.retain(|k, v| !(k.pgid == pgid && v.id == id));
}
```

**Step 6: Run tests**

```bash
cargo test -p osdclient --lib
```

Expected: All tests pass

**Step 7: Run benchmarks**

Create benchmark for backoff checks:
```rust
// In benches/backoff.rs
fn bench_backoff_check(c: &mut Criterion) {
    let mut tracker = BackoffTracker::new();
    // Add entries
    // Benchmark check operations
}
```

Expected: 15-20% faster backoff checks

**Step 8: Commit**

```bash
git add crates/osdclient/src/backoff.rs
git commit -m "perf: simplify backoff tracker data structure

Replace nested HashMap/BTreeMap with single BTreeMap:
- Composite key: (pgid, begin_hobj)
- Simpler insert/remove logic
- Efficient range queries

Expected: 15-20% faster backoff checks.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 3: Audit and Optimize Collection Choices

**Files:**
- Various files in `crates/osdclient/src/` and `crates/monclient/src/`
- Test: Existing tests should continue to pass

**Context:** Review collection usage throughout the codebase and optimize where appropriate.

**Step 1: Identify collection usage patterns**

Search for:
- HashMap vs BTreeMap usage
- Vec usage for small collections
- Unnecessary allocations

```bash
grep -r "HashMap\|BTreeMap\|Vec" crates/osdclient/src/*.rs | head -50
```

**Step 2: Apply optimization rules**

1. **Small collections (< 10 items)**: Use Vec instead of HashMap
2. **Ordered iteration needed**: Use BTreeMap
3. **Fast lookup needed**: Use HashMap
4. **Frequent iteration**: Consider Vec with linear search

**Step 3: Optimize identified cases**

For each case:
- Assess collection size and access patterns
- Choose optimal data structure
- Update code
- Run tests

**Step 4: Run all tests**

```bash
cargo test --workspace --lib
```

Expected: All tests pass

**Step 5: Commit**

```bash
git add -A
git commit -m "perf: optimize collection choices

Audit and optimize data structure usage:
- Use Vec for small collections (< 10 items)
- Use HashMap for fast lookup
- Use BTreeMap for ordered iteration

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 4: Add Cache Hit Rate Metrics

**Files:**
- Modify: `crates/osdclient/src/osdmap.rs`
- Create: `crates/osdclient/benches/crush_cache.rs`

**Context:** Add metrics to measure cache effectiveness.

**Step 1: Add cache statistics**

```rust
pub struct CacheStats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
}

impl OSDMap {
    pub fn cache_stats(&self) -> (u64, u64) {
        (
            self.cache_stats.hits.load(Ordering::Relaxed),
            self.cache_stats.misses.load(Ordering::Relaxed),
        )
    }

    pub fn cache_hit_rate(&self) -> f64 {
        let (hits, misses) = self.cache_stats();
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}
```

**Step 2: Update cache lookup to track stats**

```rust
pub fn pg_to_osds(&self, pool: i64, pg_num: u32) -> Result<Vec<i32>> {
    let cache_key = (pool, pg_num);

    {
        let mut cache = self.crush_cache.lock().unwrap();
        if let Some(osds) = cache.get(&cache_key) {
            self.cache_stats.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(osds.clone());
        }
    }

    self.cache_stats.misses.fetch_add(1, Ordering::Relaxed);
    // ... rest of logic
}
```

**Step 3: Create cache benchmark**

```rust
// benches/crush_cache.rs
fn bench_crush_cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("crush_cache");

    // Benchmark with cache hits
    // Benchmark with cache misses
    // Measure hit rate

    group.finish();
}
```

**Step 4: Run benchmarks**

```bash
cargo bench -p osdclient --bench crush_cache
```

Expected: Cache hit rate > 80%

**Step 5: Commit**

```bash
git add crates/osdclient/src/osdmap.rs crates/osdclient/benches/crush_cache.rs
git commit -m "perf: add cache hit rate metrics

Add statistics tracking for CRUSH cache:
- Track hits and misses
- Calculate hit rate
- Benchmark cache effectiveness

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Task 5: Document Phase 6 Results

**Files:**
- Modify: `docs/PROFILING.md`

**Context:** Document data structure optimizations and their impact.

**Step 1: Run all benchmarks**

```bash
cargo bench --workspace 2>&1 | tee phase6_results.txt
```

**Step 2: Update PROFILING.md**

Add Phase 6 results section:

```markdown
### Phase 6 Data Structure Optimization (as of 2026-03-06)

**Improvements over Phase 5:**
- Operation routing: X% faster (CRUSH cache)
- Backoff checks: Y% faster (simplified data structure)
- Cache hit rate: Z% (target: >80%)

**Optimizations applied:**
1. CRUSH calculation cache (Task 1)
   - 1000-entry LRU cache for PG → OSD mappings
   - Invalidated on OSDMap updates
   - Expected: 30-40% reduction in routing overhead

2. Simplified backoff tracker (Task 2)
   - Single BTreeMap with composite key
   - Replaced nested HashMap/BTreeMap
   - Expected: 15-20% faster backoff checks

3. Optimized collection choices (Task 3)
   - Vec for small collections
   - HashMap for fast lookup
   - BTreeMap for ordered iteration

4. Cache metrics (Task 4)
   - Track hits/misses
   - Measure hit rate
   - Verify cache effectiveness

**Commits:**
- [hash] perf: add LRU cache for CRUSH calculations
- [hash] perf: simplify backoff tracker data structure
- [hash] perf: optimize collection choices
- [hash] perf: add cache hit rate metrics
```

**Step 3: Commit**

```bash
git add docs/PROFILING.md
git commit -m "perf: document Phase 6 optimization results

Record data structure improvements:
- CRUSH cache: +X% routing performance
- Backoff tracker: +Y% check performance
- Cache hit rate: Z%

All optimizations maintain correctness.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Verification Checklist

After completing all tasks, verify:

- [ ] All unit tests pass: `cargo test --workspace --lib`
- [ ] All integration tests pass: `CEPH_CONF=/home/kefu/dev/ceph/build/ceph.conf cargo test --tests -- --ignored`
- [ ] CRUSH cache hit rate > 80%
- [ ] 30-40% improvement in operation routing
- [ ] 15-20% improvement in backoff checks
- [ ] All changes committed with descriptive messages

---

## Success Metrics

**Phase 6 Complete When:**
- ✅ 30-40% reduction in operation routing overhead
- ✅ CRUSH cache hit rate > 80%
- ✅ Simplified backoff tracker
- ✅ All tests passing

**Expected Outcomes:**
- Faster operation routing (CRUSH cache)
- Faster backoff checks (simplified structure)
- Optimal collection usage throughout
- Maintained correctness (all tests pass)

---

## Notes

- Focus on hot paths (operation routing, backoff checks)
- Measure cache effectiveness with metrics
- Don't over-optimize cold paths
- Keep changes focused and testable
- Document actual improvements (not estimates)
