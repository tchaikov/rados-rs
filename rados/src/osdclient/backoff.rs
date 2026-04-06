//! Per-PG backoff tracking for OSD operations
//!
//! This module implements backoff tracking as used by Ceph's Objecter.
//! When an OSD is overloaded or undergoing recovery, it sends BLOCK messages
//! to request clients pause operations on specific object ranges. When the
//! condition clears, it sends UNBLOCK messages.
//!
//! # Design
//!
//! The backoff tracker uses a single BTreeMap with composite key (pgid, begin_hobject).
//! This provides efficient range queries while maintaining simpler code than nested maps.
//!
//! # Reference
//!
//! - ~/dev/ceph/src/osdc/Objecter.h OSDSession::backoffs, backoffs_by_id
//! - ~/dev/ceph/src/osdc/Objecter.cc handle_osd_backoff(), _send_op()

use crate::osdclient::types::StripedPgId;
#[cfg(test)]
use std::collections::HashSet;
use std::collections::{BTreeMap, HashMap};
use tracing::{debug, warn};

/// Composite key for backoff entries: (pgid, begin_hobject)
///
/// Ordered first by pgid, then by begin hobject. This allows efficient
/// range queries for both PG-level and object-level lookups.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct BackoffKey {
    pgid: StripedPgId,
    begin: crate::HObject,
}

/// Create a minimal HObject suitable for use as the lower bound in BTreeMap range queries.
fn min_hobject() -> crate::HObject {
    crate::HObject {
        key: String::new(),
        oid: String::new(),
        snapid: 0,
        hash: 0,
        max: false,
        nspace: String::new(),
        pool: 0,
    }
}

/// A single backoff entry tracking a blocked object range
#[derive(Debug, Clone)]
pub struct BackoffEntry {
    /// Placement group ID
    pub pgid: StripedPgId,
    /// Unique backoff ID from the OSD
    pub id: u64,
    /// Start of blocked range (inclusive)
    pub begin: crate::HObject,
    /// End of blocked range (exclusive)
    pub end: crate::HObject,
}

/// Per-PG backoff tracker
///
/// Tracks active backoffs for a single OSD session, allowing efficient:
/// - Range checking when sending operations
/// - Registration of new backoffs (BLOCK)
/// - Removal of backoffs by ID (UNBLOCK)
///
/// Uses a single BTreeMap with composite key for simpler code and faster lookups.
#[derive(Debug, Default)]
pub struct BackoffTracker {
    /// Primary index: (pgid, begin_hobject) -> BackoffEntry
    /// BTreeMap enables efficient range queries for `is_blocked`.
    entries: BTreeMap<BackoffKey, BackoffEntry>,
    /// Secondary index: backoff_id -> BackoffKey for O(1) removal.
    by_id: HashMap<u64, BackoffKey>,
}

impl BackoffTracker {
    /// Create a new empty backoff tracker
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new backoff (called on BLOCK message)
    ///
    /// Returns true if this is a new backoff, false if it already existed
    pub fn register(&mut self, entry: BackoffEntry) -> bool {
        let key = BackoffKey {
            pgid: entry.pgid,
            begin: entry.begin.clone(),
        };

        let is_new = !self.by_id.contains_key(&entry.id);

        if !is_new {
            debug!("Backoff id={} already registered, updating", entry.id);
            // Remove old key from secondary index if the BackoffKey changed
            if let Some(old_key) = self.by_id.get(&entry.id).filter(|k| **k != key) {
                self.entries.remove(old_key);
            }
        }

        self.by_id.insert(entry.id, key.clone());
        self.entries.insert(key, entry);
        is_new
    }

    /// Remove a backoff by ID (called on UNBLOCK message)
    ///
    /// Returns the removed entry if found, along with any mismatched range info
    pub fn remove_by_id(
        &mut self,
        id: u64,
        expected_begin: &crate::HObject,
        expected_end: &crate::HObject,
    ) -> Option<BackoffEntry> {
        // O(1) lookup via secondary index
        let key = self.by_id.remove(&id)?;

        // O(log n) removal from BTreeMap using full key
        let entry = self.entries.remove(&key)?;

        // Warn if range doesn't match (but still remove, matching Ceph behavior)
        if entry.begin != *expected_begin || entry.end != *expected_end {
            warn!(
                "Backoff id={} unblock range [{:?}, {:?}) doesn't match registered [{:?}, {:?})",
                id, expected_begin, expected_end, entry.begin, entry.end
            );
        }

        Some(entry)
    }

    /// Check if an object is blocked by any backoff in the given PG
    ///
    /// Uses efficient range lookup matching Ceph's _send_op() logic:
    /// 1. Find the backoff with begin <= hobj using lower_bound
    /// 2. Check if hobj falls within [begin, end)
    pub fn is_blocked(&self, pgid: &StripedPgId, hobj: &crate::HObject) -> Option<&BackoffEntry> {
        // Create range bounds for this PG
        let start_key = BackoffKey {
            pgid: *pgid,
            begin: min_hobject(),
        };

        let end_key = BackoffKey {
            pgid: *pgid,
            begin: hobj.clone(),
        };

        // Find all entries in this PG with begin <= hobj
        let mut iter = self.entries.range(start_key..=end_key);

        // Get the last entry with begin <= hobj
        if let Some((_, entry)) = iter.next_back() {
            // Check if hobj is in [begin, end)
            if hobj >= &entry.begin && hobj < &entry.end {
                debug!(
                    "Object {:?} blocked by backoff id={} range [{:?}, {:?})",
                    hobj, entry.id, entry.begin, entry.end
                );
                return Some(entry);
            }
        }

        None
    }

    /// Check if there are any active backoffs
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[cfg(test)]
    pub fn get_by_id(&self, id: u64) -> Option<&BackoffEntry> {
        let key = self.by_id.get(&id)?;
        self.entries.get(key)
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub fn num_pgs(&self) -> usize {
        self.entries
            .keys()
            .map(|key| key.pgid)
            .collect::<HashSet<_>>()
            .len()
    }

    #[cfg(test)]
    pub fn clear(&mut self) {
        self.entries.clear();
        self.by_id.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_hobj(name: &str) -> crate::HObject {
        crate::HObject {
            key: String::new(),
            oid: name.to_string(),
            snapid: u64::MAX - 1, // SNAP_HEAD
            hash: 0,
            max: false,
            nspace: String::new(),
            pool: 1,
        }
    }

    fn make_pgid() -> StripedPgId {
        StripedPgId::new(1, 0, -1)
    }

    #[test]
    fn test_register_and_lookup() {
        let mut tracker = BackoffTracker::new();
        let pgid = make_pgid();

        let entry = BackoffEntry {
            pgid,
            id: 1,
            begin: make_hobj("aaa"),
            end: make_hobj("zzz"),
        };

        assert!(tracker.register(entry));
        assert_eq!(tracker.len(), 1);

        // Object in range should be blocked
        assert!(tracker.is_blocked(&pgid, &make_hobj("bbb")).is_some());
        assert!(tracker.is_blocked(&pgid, &make_hobj("mmm")).is_some());

        // Object at begin should be blocked (inclusive)
        assert!(tracker.is_blocked(&pgid, &make_hobj("aaa")).is_some());

        // Object at end should NOT be blocked (exclusive)
        assert!(tracker.is_blocked(&pgid, &make_hobj("zzz")).is_none());

        // Object before range should not be blocked
        assert!(tracker.is_blocked(&pgid, &make_hobj("000")).is_none());
    }

    #[test]
    fn test_remove_by_id() {
        let mut tracker = BackoffTracker::new();
        let pgid = make_pgid();

        let entry = BackoffEntry {
            pgid,
            id: 42,
            begin: make_hobj("aaa"),
            end: make_hobj("zzz"),
        };

        tracker.register(entry.clone());
        assert_eq!(tracker.len(), 1);

        // Remove by ID
        let removed = tracker.remove_by_id(42, &entry.begin, &entry.end);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().id, 42);
        assert_eq!(tracker.len(), 0);

        // Object should no longer be blocked
        assert!(tracker.is_blocked(&pgid, &make_hobj("bbb")).is_none());
    }

    #[test]
    fn test_multiple_backoffs_same_pg() {
        let mut tracker = BackoffTracker::new();
        let pgid = make_pgid();

        // Register two non-overlapping backoffs
        tracker.register(BackoffEntry {
            pgid,
            id: 1,
            begin: make_hobj("aaa"),
            end: make_hobj("ccc"),
        });

        tracker.register(BackoffEntry {
            pgid,
            id: 2,
            begin: make_hobj("mmm"),
            end: make_hobj("ppp"),
        });

        assert_eq!(tracker.len(), 2);
        assert_eq!(tracker.num_pgs(), 1);

        // Check blocking
        assert!(tracker.is_blocked(&pgid, &make_hobj("bbb")).is_some());
        assert!(tracker.is_blocked(&pgid, &make_hobj("nnn")).is_some());
        assert!(tracker.is_blocked(&pgid, &make_hobj("ddd")).is_none());
        assert!(tracker.is_blocked(&pgid, &make_hobj("qqq")).is_none());
    }

    #[test]
    fn test_multiple_pgs() {
        let mut tracker = BackoffTracker::new();

        let pgid1 = StripedPgId::new(1, 0, -1);
        let pgid2 = StripedPgId::new(1, 1, -1);

        tracker.register(BackoffEntry {
            pgid: pgid1,
            id: 1,
            begin: make_hobj("aaa"),
            end: make_hobj("zzz"),
        });

        tracker.register(BackoffEntry {
            pgid: pgid2,
            id: 2,
            begin: make_hobj("aaa"),
            end: make_hobj("zzz"),
        });

        assert_eq!(tracker.len(), 2);
        assert_eq!(tracker.num_pgs(), 2);

        // Each PG has its own backoff
        assert!(tracker.is_blocked(&pgid1, &make_hobj("bbb")).is_some());
        assert!(tracker.is_blocked(&pgid2, &make_hobj("bbb")).is_some());

        // Remove one
        tracker.remove_by_id(1, &make_hobj("aaa"), &make_hobj("zzz"));
        assert!(tracker.is_blocked(&pgid1, &make_hobj("bbb")).is_none());
        assert!(tracker.is_blocked(&pgid2, &make_hobj("bbb")).is_some());
    }

    #[test]
    fn test_get_by_id() {
        let mut tracker = BackoffTracker::new();
        let pgid = make_pgid();

        tracker.register(BackoffEntry {
            pgid,
            id: 123,
            begin: make_hobj("aaa"),
            end: make_hobj("zzz"),
        });

        let entry = tracker.get_by_id(123);
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().id, 123);

        assert!(tracker.get_by_id(999).is_none());
    }

    #[test]
    fn test_clear() {
        let mut tracker = BackoffTracker::new();
        let pgid = make_pgid();

        tracker.register(BackoffEntry {
            pgid,
            id: 1,
            begin: make_hobj("aaa"),
            end: make_hobj("zzz"),
        });

        assert!(!tracker.is_empty());
        tracker.clear();
        assert!(tracker.is_empty());
        assert_eq!(tracker.len(), 0);
        assert_eq!(tracker.num_pgs(), 0);
    }

    #[test]
    fn test_duplicate_registration() {
        let mut tracker = BackoffTracker::new();
        let pgid = make_pgid();

        let entry = BackoffEntry {
            pgid,
            id: 1,
            begin: make_hobj("aaa"),
            end: make_hobj("zzz"),
        };

        assert!(tracker.register(entry.clone()));
        assert!(!tracker.register(entry)); // Duplicate returns false
        assert_eq!(tracker.len(), 1);
    }
}
