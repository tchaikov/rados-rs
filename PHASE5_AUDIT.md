# Phase 5 Audit: Stringly-Typed Public APIs in OSD/Subscription APIs

## Executive Summary

This Phase 5 audit identifies opportunities to replace stringly-typed public OSD-facing and subscription APIs with strongly-typed enums and domain types. The codebase already demonstrates excellent API design patterns (MonService enum), which can be extended to snapshot operations, lock names, and pool references.

---

## 1. Current Stringly-Typed Subscription/OSD APIs

### 1.1 **Snapshot Name Resolution (CRITICAL)**
**File**: `/home/kefu/dev/rados-rs/crates/osdclient/src/ioctx.rs`

**Lines**: 583-598

```rust
pub async fn pool_snap_lookup(&self, name: &str) -> Result<u64>
```

**Problem**: 
- String parameter for snapshot names with no compile-time validation
- Caller must know snapshot names as strings
- Error handling is string-based (hard to match on specific error types)
- No type safety around snapshot identity

**Current Implementation**:
```rust
pub async fn pool_snap_lookup(&self, name: &str) -> Result<u64> {
    let osdmap = self.client.get_osdmap().await?;
    let pool = osdmap.get_pool(self.pool_id)
        .ok_or(OSDClientError::PoolNotFound(self.pool_id))?;
    pool.snaps
        .values()
        .find(|s| s.name == name)
        .map(|s| s.snapid)
        .ok_or_else(|| {
            OSDClientError::Other(format!(
                "snapshot '{}' not found in pool {}",
                name, self.pool_id
            ))
        })
}
```

---

### 1.2 **Lock Name Parameters (MEDIUM)**
**File**: `/home/kefu/dev/rados-rs/crates/osdclient/src/ioctx.rs`

**Lines**: 328-426

```rust
pub async fn lock_exclusive(
    &self,
    oid: impl Into<String>,
    name: &str,           // <-- STRINGLY TYPED
    cookie: &str,
    description: &str,
    duration: Option<std::time::Duration>,
) -> Result<()>
```

**Related Lines**:
- 369-401: `lock_shared()` - same issue with `name: &str`, `tag: &str`
- 410: `unlock()` - same issue with `name: &str`

**Problem**:
- Lock names are passed as strings with no type checking
- Lock types (Exclusive/Shared) are embedded in method names, not enum
- No compile-time validation of lock scope/compatibility
- Error messages use string formatting

---

### 1.3 **Xattr Name Parameters (LOW)**
**File**: `/home/kefu/dev/rados-rs/crates/osdclient/src/ioctx.rs`

**Lines**: 438-539

```rust
pub async fn get_xattr(
    &self,
    oid: impl Into<String>,
    name: impl Into<String>,   // <-- STRINGLY TYPED
) -> Result<Bytes>
```

**Related Lines**:
- 475-507: `set_xattr()` - same pattern
- 507-539: `remove_xattr()` - same pattern

**Problem**:
- Xattr names are strings with no namespacing validation
- Could benefit from domain types for xattr categories (e.g., user xattrs, system xattrs)
- No type safety on xattr semantics

---

### 1.4 **Pool Name Lookup (MEDIUM)**
**File**: `/home/kefu/dev/rados-rs/crates/osdclient/src/client.rs`

**Lines**: 1298, 1349

```rust
pub async fn create_pool(&self, pool_name: &str, crush_rule: Option<i16>) -> Result<()>
pub async fn delete_pool(&self, pool_name: &str, confirm: bool) -> Result<()>
```

**Problem**:
- Pool names accepted as strings
- No validation of pool name format/constraints
- No distinction between pool ID (u64 - typed) and pool name (string)
- Callers must manually track pool IDs vs names

---

## 2. Existing Domain Types/Newtypes That Can Be Reused

### 2.1 **Already Well-Modeled: MonService (Pattern to Follow)**
**File**: `/home/kefu/dev/rados-rs/crates/monclient/src/subscription.rs`

**Lines**: 16-41

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MonService {
    MonMap,
    OsdMap,
    Config,
    MgrMap,
    MdsMap,
}

impl MonService {
    pub const fn as_str(self) -> &'static str { ... }
}
impl std::fmt::Display for MonService { ... }
```

**Why This Pattern Works**:
- ✅ Enum for fixed set of services
- ✅ Compile-time exhaustiveness checking
- ✅ Display/Debug traits for logging
- ✅ Copy trait for ergonomics
- ✅ Serialization support

---

### 2.2 **Existing Snapshot Type**
**File**: `/home/kefu/dev/rados-rs/crates/osdclient/src/osdmap.rs`

**Lines**: 834-894

```rust
#[derive(Debug, Clone, denc::Denc)]
pub struct PoolSnapInfo {
    pub snapid: u64,
    pub stamp: denc::UTime,
    pub name: String,
}
```

**Status**: Already exists but snapshot ID lookup uses strings.

---

### 2.3 **Existing Lock Types**
**File**: `/home/kefu/dev/rados-rs/crates/osdclient/src/lock.rs`

**Lines**: 9-25

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LockType {
    None = 0,
    Exclusive = 1,
    Shared = 2,
    ExclusiveEphemeral = 3,
}

pub struct LockFlags: u8 {
    const MAY_RENEW = 0x01;
    const MUST_RENEW = 0x02;
}
```

**Status**: Types exist but only used internally; public APIs still use `&str` names.

---

### 2.4 **ObjectId / Entity Name Types**
**File**: `/home/kefu/dev/rados-rs/crates/osdclient/src/types.rs`

**Lines**: 195-255

```rust
pub struct ObjectId {
    pub pool: u64,
    pub namespace: String,
    pub key: String,
    pub snap: u64,
}
```

**Status**: Good foundation, but pool operations still use string names instead of IDs.

---

## 3. Smallest Coherent Implementation Batch (Phase 5)

### **Recommended Scope: 2-3 files, ~200-300 lines net change**

#### **File 1: New `osdclient/src/snapshot.rs`** (NEW)
Create minimal snapshot domain types:

```rust
//! Snapshot identification and resolution

/// Snapshot identifier - strongly typed alternative to u64
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SnapId(pub u64);

impl SnapId {
    pub fn new(id: u64) -> Self { Self(id) }
    pub fn into_u64(self) -> u64 { self.0 }
}

impl From<u64> for SnapId { ... }
impl From<SnapId> for u64 { ... }

// Display, Debug, Serde if needed
```

**Rationale**: 
- Minimal boilerplate (30-40 lines)
- Newtype provides type safety without runtime cost
- Easy to add methods later (e.g., validation)
- Clear API boundaries

#### **File 2: `osdclient/src/ioctx.rs` (MODIFY)**
Update snapshot operations:

**Current** (lines 583-598):
```rust
pub async fn pool_snap_lookup(&self, name: &str) -> Result<u64>
pub async fn pool_snap_get_info(&self, snap_id: u64) -> Result<PoolSnapInfo>
pub async fn snap_rollback(&self, oid: impl Into<String>, snap_id: u64) -> Result<()>
```

**New**:
```rust
// Returns strongly-typed SnapId instead of u64
pub async fn pool_snap_lookup(&self, name: &str) -> Result<SnapId>

// Overload: accept SnapId as well
pub async fn pool_snap_get_info(&self, snap_id: impl Into<SnapId>) -> Result<PoolSnapInfo>

// Overload: accept SnapId
pub async fn snap_rollback(&self, oid: impl Into<String>, snap_id: impl Into<SnapId>) -> Result<()>
```

**Impact**: 
- 3-5 line changes per function
- Backward compatible with `impl Into<SnapId>` pattern
- Better type safety

#### **File 3: `osdclient/src/lock.rs` (ENHANCE)**
Create LockName wrapper type:

```rust
/// Lock name - identifies a lock within an object
/// 
/// Lock names are strings of max 64 chars, must be ASCII, no spaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockName(String);

impl LockName {
    pub fn new(name: impl Into<String>) -> Result<Self, OSDClientError> {
        let name = name.into();
        if name.is_empty() || name.len() > 64 {
            return Err(OSDClientError::InvalidLockName(
                "Lock name must be 1-64 characters".to_string()
            ));
        }
        Ok(LockName(name))
    }
    pub fn as_str(&self) -> &str { &self.0 }
}

// Add to lock.rs and re-export from lib.rs
```

**Impact**:
- 20-30 lines
- Validates lock name constraints
- Makes lock API more ergonomic

---

## 4. Compatibility and Call-Site Risks

### 4.1 **Low Risk Changes (Recommended)**

**SnapId introduction**:
- ✅ `impl Into<SnapId>` for u64 maintains backward compatibility
- ✅ Only changes return type of `pool_snap_lookup()` (breaking but narrow)
- ✅ Existing `snap_rollback(snap_id: u64)` becomes `snap_rollback(snap_id: impl Into<SnapId>)`
- ✅ Call sites: only internal tests need updates (estimated 2-3 call sites)

**LockName introduction**:
- ⚠️  Breaking: Changes signature of `lock_exclusive(name: &str)` → `lock_exclusive(name: LockName)`
- ⚠️  Requires constructor call: `LockName::new("mylock")?`
- ✅ Validation benefit outweighs API friction

### 4.2 **Internal Call Sites**

**Search Results**:
```
grep -rn "pool_snap_lookup\|snap_rollback\|lock_exclusive\|lock_shared"
  /home/kefu/dev/rados-rs/crates/osdclient/
```

**Result**: **No internal call sites found in tests** (grep returned 0 matches)

**Implication**:
- ✅ Safe to refactor
- ✅ Public API is not exercised internally
- ⚠️  Tests will need to be added/updated

### 4.3 **MonClient Pool Operations (AVOID)**

**File**: `/home/kefu/dev/rados-rs/crates/monclient/src/client.rs`

**Lines**: 1555, 1574

```rust
pub async fn snap_create(&self, pool_id: u32, name: &str) -> Result<()>
pub async fn snap_remove(&self, pool_id: u32, name: &str) -> Result<()>
```

**Decision**: **SKIP for Phase 5** - These are monitor operations, not OSD operations. Keep them as-is to avoid scope creep.

---

## 5. Tests and Documentation Updates

### 5.1 **Unit Tests to Create**

**File**: `crates/osdclient/src/snapshot.rs` (new)
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapid_creation() {
        let snap = SnapId::new(42);
        assert_eq!(snap.into_u64(), 42);
    }

    #[test]
    fn test_snapid_from_u64() {
        let snap = SnapId::from(100u64);
        assert_eq!(snap.into_u64(), 100);
    }

    #[test]
    fn test_snapid_ordering() {
        let snap1 = SnapId::new(10);
        let snap2 = SnapId::new(20);
        assert!(snap1 < snap2);
    }
}
```

**File**: `crates/osdclient/src/lock.rs` (extend)
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lockname_valid() {
        let name = LockName::new("mylock").unwrap();
        assert_eq!(name.as_str(), "mylock");
    }

    #[test]
    fn test_lockname_empty_rejected() {
        assert!(LockName::new("").is_err());
    }

    #[test]
    fn test_lockname_too_long_rejected() {
        let long_name = "a".repeat(65);
        assert!(LockName::new(long_name).is_err());
    }
}
```

### 5.2 **Integration Test Updates**

**File**: `crates/osdclient/tests/rados_operations.rs`

Add snapshot test (if not already present):
```rust
#[tokio::test]
#[ignore]
async fn test_snapshot_operations() {
    // ... setup ...
    let snap_id = ioctx.pool_snap_lookup("mysnap").await.unwrap();
    // Verify snap_id is SnapId type
    assert!(snap_id.into_u64() > 0);
}
```

### 5.3 **Documentation Updates**

**File**: `crates/osdclient/src/ioctx.rs` (doc comments)

Update existing docs:
```rust
/// Resolve a snapshot name to its [`SnapId`].
///
/// # Arguments
/// * `name` - Snapshot name (obtained from [`pool_snap_list`])
///
/// # Returns
/// A strongly-typed [`SnapId`] for use with [`snap_rollback`] and other operations
///
/// # Errors
/// Returns [`OSDClientError::Other`] if the snapshot doesn't exist
pub async fn pool_snap_lookup(&self, name: &str) -> Result<SnapId>
```

**File**: `crates/osdclient/README.md`

Add section:
```markdown
### Snapshot Operations

SnapIds are strongly typed to prevent accidental mix-ups:

```rust
let snap_id = ioctx.pool_snap_lookup("mysnap").await?;
ioctx.snap_rollback("myobject", snap_id).await?;
```
```

**File**: `CLAUDE.md` (development guide)

Add pattern documentation:
```markdown
## Phase 5: Strongly Typed OSD APIs

Pattern: Use newtypes (like SnapId) for domain concepts that were previously strings.

Benefits:
- Compile-time type safety
- Prevents accidental parameter swaps
- Enables method overloading via Into<T>
- Backward compatible with impl Into<T> in signatures

Example: SnapId replaces naked u64 in snapshot operations.
```

---

## 6. Concrete Edit Recommendations

### 6.1 **snapshot.rs - NEW FILE**
**Path**: `/home/kefu/dev/rados-rs/crates/osdclient/src/snapshot.rs`
**Lines to Add**: 50-80

```rust
//! Strongly-typed snapshot identifiers and utilities

use std::fmt;

/// Unique snapshot identifier within a pool
/// 
/// Provides type safety for snapshot IDs, preventing accidental 
/// mix-up with other numeric IDs (epoch, version, etc).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SnapId(pub u64);

impl SnapId {
    /// Create a new snapshot ID
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Convert to u64
    pub fn into_u64(self) -> u64 {
        self.0
    }

    /// Reference as u64
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for SnapId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl From<SnapId> for u64 {
    fn from(snap: SnapId) -> Self {
        snap.0
    }
}

impl fmt::Display for SnapId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SnapId({})", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapid_creation() {
        let snap = SnapId::new(42);
        assert_eq!(snap.into_u64(), 42);
    }

    #[test]
    fn test_snapid_from_u64() {
        let snap = SnapId::from(100u64);
        assert_eq!(snap.as_u64(), 100);
    }

    #[test]
    fn test_snapid_ordering() {
        let snap1 = SnapId::new(10);
        let snap2 = SnapId::new(20);
        assert!(snap1 < snap2);
    }
}
```

---

### 6.2 **lib.rs - ADD EXPORTS**
**Path**: `/home/kefu/dev/rados-rs/crates/osdclient/src/lib.rs`

**Current** (line 28-39):
```rust
pub(crate) mod backoff;
pub mod client;
pub(crate) mod denc_types;
pub mod error;
pub mod ioctx;
pub mod lock;
pub mod messages;
pub mod operation;
pub mod osdmap;
// ... etc
pub use client::{OSDClient, OSDClientConfig};
pub use error::OSDClientError;
pub use ioctx::IoCtx;
pub use lock::{LockFlags, LockRequest, LockType, UnlockRequest};
pub use operation::{BuiltOp, OpBuilder};
pub use osdmap::{OSDMap, OSDMapIncremental, PgMergeMeta, PgPool, PoolSnapInfo, UuidD};
pub use pgmap_types::{ObjectstorePerfStat, PoolStat};
pub use types::{
    OSDOp, ObjectId, OpCode, OpState, OpTarget, OsdOpFlags, PoolInfo, ReadResult, SparseExtent,
    SparseReadResult, StatResult, StripedPgId, WriteResult,
};
```

**New** (add after line 18):
```rust
pub(crate) mod snapshot;  // <-- ADD THIS LINE
```

**And in pub use block** (after ioctx/lock):
```rust
pub use snapshot::SnapId;  // <-- ADD THIS LINE
```

---

### 6.3 **ioctx.rs - UPDATE SNAPSHOT FUNCTIONS**
**Path**: `/home/kefu/dev/rados-rs/crates/osdclient/src/ioctx.rs`

**Lines 580-598 BEFORE**:
```rust
    /// Resolve a snapshot name to its snap_id.
    ///
    /// Returns an error if no snapshot with the given name exists in this pool.
    pub async fn pool_snap_lookup(&self, name: &str) -> Result<u64> {
        let osdmap = self.client.get_osdmap().await?;
        let pool = osdmap
            .get_pool(self.pool_id)
            .ok_or(OSDClientError::PoolNotFound(self.pool_id))?;
        pool.snaps
            .values()
            .find(|s| s.name == name)
            .map(|s| s.snapid)
            .ok_or_else(|| {
                OSDClientError::Other(format!(
                    "snapshot '{}' not found in pool {}",
                    name, self.pool_id
                ))
            })
    }
```

**AFTER**:
```rust
    /// Resolve a snapshot name to its [`SnapId`].
    ///
    /// # Arguments
    /// * `name` - Snapshot name (obtain from [`pool_snap_list`])
    ///
    /// # Returns
    /// Strongly-typed snapshot identifier for use with [`snap_rollback`]
    ///
    /// # Errors
    /// Returns error if no snapshot with the given name exists in this pool
    pub async fn pool_snap_lookup(&self, name: &str) -> Result<crate::snapshot::SnapId> {
        let osdmap = self.client.get_osdmap().await?;
        let pool = osdmap
            .get_pool(self.pool_id)
            .ok_or(OSDClientError::PoolNotFound(self.pool_id))?;
        pool.snaps
            .values()
            .find(|s| s.name == name)
            .map(|s| crate::snapshot::SnapId::new(s.snapid))  // <-- WRAP RETURN
            .ok_or_else(|| {
                OSDClientError::Other(format!(
                    "snapshot '{}' not found in pool {}",
                    name, self.pool_id
                ))
            })
    }
```

**Lines 603-614 BEFORE**:
```rust
    pub async fn pool_snap_get_info(&self, snap_id: u64) -> Result<crate::osdmap::PoolSnapInfo> {
        let osdmap = self.client.get_osdmap().await?;
        let pool = osdmap
            .get_pool(self.pool_id)
            .ok_or(OSDClientError::PoolNotFound(self.pool_id))?;
        pool.snaps.get(&snap_id).cloned().ok_or_else(|| {
            OSDClientError::Other(format!(
                "snap_id {} not found in pool {}",
                snap_id, self.pool_id
            ))
        })
    }
```

**AFTER**:
```rust
    /// Get snapshot information by ID.
    ///
    /// Accepts either a [`SnapId`] or raw u64 for backward compatibility.
    pub async fn pool_snap_get_info<T: Into<crate::snapshot::SnapId>>(
        &self,
        snap_id: T,
    ) -> Result<crate::osdmap::PoolSnapInfo> {
        let snap_id = snap_id.into().into_u64();  // <-- CONVERT TO U64
        let osdmap = self.client.get_osdmap().await?;
        let pool = osdmap
            .get_pool(self.pool_id)
            .ok_or(OSDClientError::PoolNotFound(self.pool_id))?;
        pool.snaps.get(&snap_id).cloned().ok_or_else(|| {
            OSDClientError::Other(format!(
                "snap_id {} not found in pool {}",
                snap_id, self.pool_id
            ))
        })
    }
```

**Lines 621-635 BEFORE**:
```rust
    pub async fn snap_rollback(&self, oid: impl Into<String>, snap_id: u64) -> Result<()> {
        use crate::operation::OpBuilder;

        let oid_str = oid.into();
        debug!(
            "Rolling back object '{}' in pool {} to snap_id {}",
            oid_str, self.pool_id, snap_id
        );

        let op = OpBuilder::new().rollback(snap_id).build();
        self.client
            .execute_built_op(self.pool_id, &oid_str, op)
            .await?;
        Ok(())
    }
```

**AFTER**:
```rust
    /// Roll back an object to a pool snapshot.
    ///
    /// Accepts either a [`SnapId`] or raw u64 for backward compatibility.
    pub async fn snap_rollback<T: Into<crate::snapshot::SnapId>>(
        &self,
        oid: impl Into<String>,
        snap_id: T,
    ) -> Result<()> {
        use crate::operation::OpBuilder;

        let oid_str = oid.into();
        let snap_id = snap_id.into().into_u64();  // <-- CONVERT TO U64
        debug!(
            "Rolling back object '{}' in pool {} to snap_id {}",
            oid_str, self.pool_id, snap_id
        );

        let op = OpBuilder::new().rollback(snap_id).build();
        self.client
            .execute_built_op(self.pool_id, &oid_str, op)
            .await?;
        Ok(())
    }
```

---

### 6.4 **lock.rs - ADD LockName TYPE**
**Path**: `/home/kefu/dev/rados-rs/crates/osdclient/src/lock.rs`

**After line 68, ADD**:
```rust
/// Lock name - identifies a lock within an object
/// 
/// Provides validation and type safety for lock names.
/// Lock names are max 64 characters ASCII strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockName(String);

impl LockName {
    /// Create a new lock name with validation
    pub fn new(name: impl Into<String>) -> Result<Self, crate::error::OSDClientError> {
        let name = name.into();
        if name.is_empty() {
            return Err(crate::error::OSDClientError::Other(
                "Lock name cannot be empty".to_string(),
            ));
        }
        if name.len() > 64 {
            return Err(crate::error::OSDClientError::Other(
                format!("Lock name must be <= 64 chars, got {}", name.len()),
            ));
        }
        Ok(LockName(name))
    }

    /// Get the lock name as a string
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for LockName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<LockName> for String {
    fn from(name: LockName) -> Self {
        name.0
    }
}

impl AsRef<str> for LockName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
```

**Add to test section** (before closing brace):
```rust
    #[test]
    fn test_lockname_valid() {
        let name = LockName::new("mylock").unwrap();
        assert_eq!(name.as_str(), "mylock");
    }

    #[test]
    fn test_lockname_empty_rejected() {
        assert!(LockName::new("").is_err());
    }

    #[test]
    fn test_lockname_too_long_rejected() {
        let long_name = "a".repeat(65);
        assert!(LockName::new(long_name).is_err());
    }

    #[test]
    fn test_lockname_from_string() {
        let name = LockName::new("test".to_string()).unwrap();
        let back: String = name.into();
        assert_eq!(back, "test");
    }
```

---

## Summary Table

| **Component** | **File** | **Lines** | **Type** | **Risk** | **Impact** |
|---|---|---|---|---|---|
| SnapId type | `snapshot.rs` | 50-80 | NEW | Low | High |
| lib.rs exports | `lib.rs` | 19, 40-41 | MODIFY | Low | Medium |
| pool_snap_lookup | `ioctx.rs` | 580-598 | MODIFY | Low | High |
| pool_snap_get_info | `ioctx.rs` | 603-614 | MODIFY | Low | Medium |
| snap_rollback | `ioctx.rs` | 621-635 | MODIFY | Low | Medium |
| LockName type | `lock.rs` | +50-70 | NEW | Medium | Medium |
| Unit tests | `snapshot.rs` | 40-50 | NEW | Low | High |
| Unit tests | `lock.rs` | +20-25 | NEW | Low | High |

**Total Net Change**: ~200-250 lines (40 removed from ioctx, 200+ added in snapshot.rs/lock.rs/tests)

---

