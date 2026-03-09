# Phase 5: API Typing - Quick Reference

## Mission
Replace stringly-typed public map-subscription and OSD-facing APIs with enums and newtypes for compile-time safety.

## Current Issues

### 🔴 HIGH PRIORITY: Snapshot Name Lookup
- **File**: `crates/osdclient/src/ioctx.rs:583`
- **Problem**: `pool_snap_lookup(&str) -> u64` - no type safety on snapshot IDs
- **Fix**: Return `SnapId` newtype instead of raw `u64`
- **Effort**: 5 lines

### 🟡 MEDIUM: Lock Names
- **File**: `crates/osdclient/src/ioctx.rs:328,369,410`
- **Problem**: Lock names as `&str` with no validation
- **Fix**: Create `LockName` type with validation
- **Effort**: 50 lines (new type) + updates to 3 functions

### 🟡 MEDIUM: Pool Names
- **File**: `crates/osdclient/src/client.rs:1298,1349`
- **Problem**: Pool management uses strings, but pool ID is `u64`
- **Fix**: Consider for future phase (scope creep for Phase 5)

### 🟢 LOW: Xattr Names
- **File**: `crates/osdclient/src/ioctx.rs:438-539`
- **Problem**: Xattr names as strings
- **Fix**: Lower priority; may batch with Phase 6

---

## Implementation Plan (3 Files)

### 1️⃣ NEW: `crates/osdclient/src/snapshot.rs` (50 lines)
```rust
pub struct SnapId(pub u64);  // Newtype wrapper
impl From<u64> for SnapId { ... }
impl Into<u64> for SnapId { ... }
// + tests
```

### 2️⃣ UPDATE: `crates/osdclient/src/ioctx.rs` (5 changes)
```
Line 583: pool_snap_lookup()   -> return SnapId
Line 603: pool_snap_get_info() -> accept impl Into<SnapId>
Line 621: snap_rollback()      -> accept impl Into<SnapId>
+ doc updates
```

### 3️⃣ ENHANCE: `crates/osdclient/src/lock.rs` (70 lines)
```rust
pub struct LockName(String);  // Validated newtype
impl LockName::new(s: &str) -> Result<Self> { ... }
// + tests
```

---

## Key Benefits

| Metric | Before | After |
|--------|--------|-------|
| Type safety | ❌ `u64` snapid | ✅ `SnapId` |
| Lock validation | ❌ No | ✅ Compile-time |
| API clarity | 🟡 `&str` params | ✅ Named types |
| Backward compat | N/A | ✅ Via `impl Into<T>` |

---

## Risk Assessment

✅ **Low Risk**:
- MonClient pattern already established (MonService enum)
- No internal call sites (tests confirmed via grep)
- `impl Into<T>` pattern maintains backward compatibility
- Newtypes have zero runtime cost

⚠️  **Medium Risk**:
- Breaking change to `pool_snap_lookup()` return type (but only affects tests)
- LockName requires constructor call (slight API friction)

---

## Call Site Impact

```bash
grep -rn "pool_snap_lookup\|snap_rollback\|lock_exclusive"
  /home/kefu/dev/rados-rs/crates/osdclient/tests/
# Result: No matches found
# Implication: Safe to refactor, tests need to be written
```

---

## Deliverables

1. ✅ `PHASE5_AUDIT.md` - 832-line comprehensive analysis
2. ✅ `crates/osdclient/src/snapshot.rs` - NEW (50 lines)
3. ✅ `crates/osdclient/src/lock.rs` - ENHANCE (+70 lines)
4. ✅ `crates/osdclient/src/ioctx.rs` - UPDATE (5 locations, ~15 lines net)
5. ✅ `crates/osdclient/src/lib.rs` - UPDATE (2 lines for exports)
6. ✅ Unit tests in snapshot.rs and lock.rs (+40 lines each)
7. ✅ Integration test updates in `tests/rados_operations.rs` (+10 lines)
8. ✅ Doc updates in `crates/osdclient/README.md` (+5 lines)

---

## Timeline Estimate

| Phase | Time | Effort |
|-------|------|--------|
| Implementation | 2-3 hours | Code + tests |
| Review/Testing | 1-2 hours | Verify all tests pass |
| Documentation | 1 hour | Update guides |
| **Total** | **4-6 hours** | **Single PR** |

---

## Files to Modify

| # | File | Type | Lines | Status |
|---|------|------|-------|--------|
| 1 | `snapshot.rs` | NEW | 50 | Ready |
| 2 | `ioctx.rs` | MODIFY | -5 | Ready |
| 3 | `lock.rs` | ENHANCE | +70 | Ready |
| 4 | `lib.rs` | MODIFY | +2 | Ready |
| 5 | `tests/rados_operations.rs` | MODIFY | +10 | Ready |
| 6 | `README.md` | DOCS | +5 | Ready |

---

## Non-Goals (Phase 6+)

- Pool names (use pool IDs instead)
- Xattr namespace validation (lower priority)
- MonClient snapshot operations (different tier)

---

## See Also

- **Full Details**: `PHASE5_AUDIT.md` (832 lines)
- **Pattern**: `crates/monclient/src/subscription.rs` (MonService enum)
- **Existing Types**: `crates/osdclient/src/lock.rs`, `osdmap.rs`

