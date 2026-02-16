//! Per-PG backoff tracking for OSD operations
//!
//! This module implements backoff tracking as used by Ceph's Objecter.
//! When an OSD is overloaded or undergoing recovery, it sends BLOCK messages
//! to request clients pause operations on specific object ranges. When the
//! condition clears, it sends UNBLOCK messages.
//!
//! # Design
//!
//! The backoff tracker maintains two data structures:
//! - `backoffs`: Map from (pgid, begin_hobject) -> BackoffEntry for range lookups
//! - `backoffs_by_id`: Map from backoff_id -> (pgid, begin_hobject) for O(1) removal
//!
//! This matches Ceph's Objecter::OSDSession backoff tracking.
//!
//! # Reference
//!
//! - ~/dev/ceph/src/osdc/Objecter.h OSDSession::backoffs, backoffs_by_id
//! - ~/dev/ceph/src/osdc/Objecter.cc handle_osd_backoff(), _send_op()

use crate::types::StripedPgId;
use std::collections::{BTreeMap, HashMap};
use tracing::{debug, warn};

/// A single backoff entry tracking a blocked object range
#[derive(Debug, Clone)]
pub struct BackoffEntry {
    /// Placement group ID
    pub pgid: StripedPgId,
    /// Unique backoff ID from the OSD
    pub id: u64,
    /// Start of blocked range (inclusive)
    pub begin: denc::HObject,
    /// End of blocked range (exclusive)
    pub end: denc::HObject,
}

/// Per-PG backoff tracker
///
/// Tracks active backoffs for a single OSD session, allowing efficient:
/// - Range checking when sending operations
/// - Registration of new backoffs (BLOCK)
/// - Removal of backoffs by ID (UNBLOCK)
#[derive(Debug, Default)]
pub struct BackoffTracker {
    /// Map: pgid -> (begin_hobject -> BackoffEntry)
    /// Using BTreeMap for begin_hobject to enable efficient range queries
    backoffs: HashMap<StripedPgId, BTreeMap<denc::HObject, BackoffEntry>>,

    /// Map: backoff_id -> (pgid, begin_hobject) for O(1) lookup during UNBLOCK
    backoffs_by_id: HashMap<u64, (StripedPgId, denc::HObject)>,
}

impl BackoffTracker {
    /// Create a new empty backoff tracker
    pub fn new() -> Self {
        Self {
            backoffs: HashMap::new(),
            backoffs_by_id: HashMap::new(),
        }
    }

    /// Register a new backoff (called on BLOCK message)
    ///
    /// Returns true if this is a new backoff, false if it already existed
    pub fn register(&mut self, entry: BackoffEntry) -> bool {
        let pgid = entry.pgid;
        let id = entry.id;
        let begin = entry.begin.clone();

        // Check if already registered
        if self.backoffs_by_id.contains_key(&id) {
            debug!("Backoff id={} already registered, updating", id);
            // Update existing entry
            if let Some(pg_backoffs) = self.backoffs.get_mut(&pgid) {
                pg_backoffs.insert(begin.clone(), entry);
            }
            return false;
        }

        // Register in both maps
        self.backoffs_by_id.insert(id, (pgid, begin.clone()));

        let pg_backoffs = self.backoffs.entry(pgid).or_default();
        pg_backoffs.insert(begin, entry);

        true
    }

    /// Remove a backoff by ID (called on UNBLOCK message)
    ///
    /// Returns the removed entry if found, along with any mismatched range info
    pub fn remove_by_id(
        &mut self,
        id: u64,
        expected_begin: &denc::HObject,
        expected_end: &denc::HObject,
    ) -> Option<BackoffEntry> {
        // Look up by ID for O(1) removal
        let (pgid, begin) = self.backoffs_by_id.remove(&id)?;

        // Get the PG's backoff map
        let pg_backoffs = self.backoffs.get_mut(&pgid)?;

        // Remove the entry
        let entry = pg_backoffs.remove(&begin)?;

        // Warn if range doesn't match (but still remove, matching Ceph behavior)
        if entry.begin != *expected_begin || entry.end != *expected_end {
            warn!(
                "Backoff id={} unblock range [{:?}, {:?}) doesn't match registered [{:?}, {:?})",
                id, expected_begin, expected_end, entry.begin, entry.end
            );
        }

        // Clean up empty PG entry
        if pg_backoffs.is_empty() {
            self.backoffs.remove(&pgid);
        }

        Some(entry)
    }

    /// Check if an object is blocked by any backoff in the given PG
    ///
    /// Uses efficient range lookup matching Ceph's _send_op() logic:
    /// 1. Find the backoff with begin <= hobj using lower_bound
    /// 2. Check if hobj falls within [begin, end)
    pub fn is_blocked(&self, pgid: &StripedPgId, hobj: &denc::HObject) -> Option<&BackoffEntry> {
        let pg_backoffs = self.backoffs.get(pgid)?;

        // Use range query to find potential blocking backoff
        // We want the largest begin <= hobj
        let mut iter = pg_backoffs.range(..=hobj.clone());

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

    /// Get all backoffs for a specific PG
    ///
    /// Used when resending operations after UNBLOCK
    pub fn get_pg_backoffs(
        &self,
        pgid: &StripedPgId,
    ) -> Option<&BTreeMap<denc::HObject, BackoffEntry>> {
        self.backoffs.get(pgid)
    }

    /// Get a backoff by ID
    pub fn get_by_id(&self, id: u64) -> Option<&BackoffEntry> {
        let (pgid, begin) = self.backoffs_by_id.get(&id)?;
        self.backoffs.get(pgid)?.get(begin)
    }

    /// Check if there are any active backoffs
    pub fn is_empty(&self) -> bool {
        self.backoffs_by_id.is_empty()
    }

    /// Get the total number of active backoffs
    pub fn len(&self) -> usize {
        self.backoffs_by_id.len()
    }

    /// Get the number of PGs with active backoffs
    pub fn num_pgs(&self) -> usize {
        self.backoffs.len()
    }

    /// Clear all backoffs (used on session reset)
    pub fn clear(&mut self) {
        self.backoffs.clear();
        self.backoffs_by_id.clear();
    }

    /// Iterate over all backoffs in a PG that overlap with a given range
    ///
    /// Used to find operations to resend after UNBLOCK
    pub fn iter_overlapping<'a>(
        &'a self,
        pgid: &StripedPgId,
        begin: &'a denc::HObject,
        end: &'a denc::HObject,
    ) -> impl Iterator<Item = &'a BackoffEntry> {
        self.backoffs
            .get(pgid)
            .into_iter()
            .flat_map(move |pg_backoffs| {
                // Get all entries that might overlap with [begin, end)
                pg_backoffs
                    .range(..end.clone())
                    .filter_map(move |(_, entry)| {
                        // Check for overlap: entry overlaps if entry.end > begin
                        if entry.end > *begin {
                            Some(entry)
                        } else {
                            None
                        }
                    })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_hobj(name: &str) -> denc::HObject {
        denc::HObject {
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
