//! Monitor subscription management
//!
//! Handles subscribing to cluster maps (osdmap, monmap, etc.) and tracking
//! subscription state.

use std::collections::HashMap;
use std::time::Instant;

/// Subscription flag: unsubscribe after receiving one update
pub const CEPH_SUBSCRIBE_ONETIME: u8 = 1;

/// Subscription renewal is scheduled at this fraction of the ack interval
const SUBSCRIPTION_RENEWAL_RATIO: f64 = 0.5;

/// Monitor-managed subscription and version-query services.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MonService {
    MonMap,
    OsdMap,
    Config,
    MgrMap,
    MdsMap,
}

impl MonService {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MonMap => "monmap",
            Self::OsdMap => "osdmap",
            Self::Config => "config",
            Self::MgrMap => "mgrmap",
            Self::MdsMap => "mdsmap",
        }
    }
}

impl std::fmt::Display for MonService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Subscription renewal mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RenewalMode {
    /// Modern monitors advertising MON_STATEFUL_SUB keep subscription state without renew acks.
    StatefulOrUnknown,
    /// Legacy monitors send subscribe ACKs with a renewal interval.
    LegacyAcked,
}

/// Subscription manager
///
/// Tracks which maps we want to subscribe to and manages the subscription lifecycle.
#[derive(Debug)]
pub struct MonSub {
    /// Pending subscriptions to send
    sub_new: HashMap<MonService, SubscribeItem>,

    /// Sent subscriptions waiting for ack
    sub_sent: HashMap<MonService, SubscribeItem>,

    /// When we last sent a renewal
    renew_sent: Option<Instant>,

    /// When we should renew subscriptions
    renew_after: Option<Instant>,

    /// Whether renewals are needed for this session.
    renewal_mode: RenewalMode,
}

impl MonSub {
    pub fn new() -> Self {
        Self {
            sub_new: HashMap::new(),
            sub_sent: HashMap::new(),
            renew_sent: None,
            renew_after: None,
            renewal_mode: RenewalMode::StatefulOrUnknown,
        }
    }

    /// Check if there are new subscriptions to send
    pub fn have_new(&self) -> bool {
        !self.sub_new.is_empty()
    }

    /// Check if subscriptions need renewal
    pub fn need_renew(&self) -> bool {
        self.renewal_mode == RenewalMode::LegacyAcked
            && self
                .renew_after
                .is_some_and(|renew_after| Instant::now() > renew_after)
    }

    /// Get the new subscriptions to send
    pub fn get_subs(&self) -> &HashMap<MonService, SubscribeItem> {
        &self.sub_new
    }

    /// Mark subscriptions as sent
    pub fn renewed(&mut self) {
        self.renew_after = None;
        if self.renew_sent.is_none() {
            self.renew_sent = Some(Instant::now());
        }

        // Move sub_new to sub_sent
        for (what, item) in self.sub_new.drain() {
            self.sub_sent.insert(what, item);
        }
    }

    /// Handle subscription ack from monitor
    pub fn acked(&mut self, interval_secs: u32) {
        self.renewal_mode = RenewalMode::LegacyAcked;
        if let Some(renew_sent) = self.renew_sent {
            // Schedule renewal for half the interval
            let interval = std::time::Duration::from_secs(
                (interval_secs as f64 * SUBSCRIPTION_RENEWAL_RATIO) as u64,
            );
            self.renew_after = Some(renew_sent + interval);
        }
        self.renew_sent = None;
    }

    /// Mark that we received a map update
    pub fn got(&mut self, what: MonService, version: u64) {
        // Check sub_new first
        if let Some(item) = self.sub_new.get_mut(&what) {
            if item.start <= version {
                if item.flags & CEPH_SUBSCRIBE_ONETIME != 0 {
                    self.sub_new.remove(&what);
                } else {
                    item.start = version + 1;
                }
            }
        }
        // Then check sub_sent
        else if let Some(item) = self.sub_sent.get_mut(&what)
            && item.start <= version
        {
            if item.flags & CEPH_SUBSCRIBE_ONETIME != 0 {
                self.sub_sent.remove(&what);
            } else {
                item.start = version + 1;
            }
        }
    }

    /// Reload sent subscriptions back to new (for reconnection)
    pub fn reload(&mut self) -> bool {
        for (&what, item) in &self.sub_sent {
            self.sub_new.entry(what).or_insert(*item);
        }
        self.have_new()
    }

    /// Add a new subscription
    ///
    /// Returns true if this is a new or changed subscription
    pub fn want(&mut self, what: MonService, start: u64, flags: u8) -> bool {
        let new_item = SubscribeItem { start, flags };

        // Check if already in sub_new with same params
        if let Some(item) = self.sub_new.get(&what)
            && item.start == start
            && item.flags == flags
        {
            return false;
        }
        // Check if already in sub_sent with same params
        else if let Some(item) = self.sub_sent.get(&what)
            && item.start == start
            && item.flags == flags
        {
            return false;
        }

        self.sub_new.insert(what, new_item);
        true
    }

    /// Increment subscription start version
    ///
    /// Only updates if the new start is greater than current
    pub fn inc_want(&mut self, what: MonService, start: u64, flags: u8) -> bool {
        // Check sub_new first
        if let Some(item) = self.sub_new.get_mut(&what) {
            if item.start >= start {
                return false;
            }
            item.start = start;
            item.flags = flags;
            return true;
        }

        // Check sub_sent
        if let Some(item) = self.sub_sent.get(&what)
            && item.start >= start
        {
            return false;
        }

        // Add to sub_new
        self.sub_new.insert(what, SubscribeItem { start, flags });
        true
    }

    /// Remove a subscription
    pub fn unwant(&mut self, what: MonService) {
        self.sub_new.remove(&what);
        self.sub_sent.remove(&what);
    }

    /// Clear all subscriptions
    pub fn clear(&mut self) {
        self.sub_new.clear();
        self.sub_sent.clear();
        self.renew_sent = None;
        self.renew_after = None;
        self.renewal_mode = RenewalMode::StatefulOrUnknown;
    }
}

impl Default for MonSub {
    fn default() -> Self {
        Self::new()
    }
}

/// A subscription item
#[derive(Debug, Clone, Copy, denc::Denc)]
pub struct SubscribeItem {
    /// Version to start from
    pub start: u64,
    /// Subscription flags (CEPH_SUBSCRIBE_ONETIME, etc.)
    pub flags: u8,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_lifecycle() {
        let mut sub = MonSub::new();

        // Initially no subscriptions
        assert!(!sub.have_new());

        // Add a subscription
        assert!(sub.want(MonService::OsdMap, 0, 0));
        assert!(sub.have_new());

        // Adding same subscription returns false
        assert!(!sub.want(MonService::OsdMap, 0, 0));

        // Mark as sent
        sub.renewed();
        assert!(!sub.have_new());

        // Got an update
        sub.got(MonService::OsdMap, 5);

        // Reload after disconnect
        assert!(sub.reload());
        assert!(sub.have_new());
    }

    #[test]
    fn test_onetime_subscription() {
        let mut sub = MonSub::new();

        // Subscribe with ONETIME flag
        sub.want(MonService::OsdMap, 0, CEPH_SUBSCRIBE_ONETIME);
        sub.renewed();

        // After receiving update, subscription is removed
        sub.got(MonService::OsdMap, 1);
        assert!(!sub.reload());
    }

    #[test]
    fn test_inc_want() {
        let mut sub = MonSub::new();

        // Initial subscription
        assert!(sub.inc_want(MonService::OsdMap, 10, 0));

        // Lower version doesn't update
        assert!(!sub.inc_want(MonService::OsdMap, 5, 0));

        // Higher version updates
        assert!(sub.inc_want(MonService::OsdMap, 15, 0));
    }

    #[test]
    fn test_got_updates_start_epoch() {
        let mut sub = MonSub::new();

        // Subscribe to osdmap starting from epoch 0
        sub.want(MonService::OsdMap, 0, 0);
        sub.renewed();

        // Receive epoch 5
        sub.got(MonService::OsdMap, 5);

        // After got(), the subscription should be updated to start from epoch 6
        // This is verified by checking that reload brings it back with updated epoch
        assert!(sub.reload());

        // The subscription should now want epoch 6 onwards (5 + 1)
        let subs = sub.get_subs();
        assert_eq!(subs.get(&MonService::OsdMap).unwrap().start, 6);
    }

    #[test]
    fn test_subscription_renewal_needed() {
        let mut sub = MonSub::new();

        // Initially, no renewal needed
        assert!(!sub.need_renew());

        // Subscribe and mark as sent
        sub.want(MonService::OsdMap, 0, 0);
        sub.renewed();

        // Still no renewal needed (no ack received yet)
        assert!(!sub.need_renew());

        // Receive ack with 60 second interval
        sub.acked(60);

        // Now we have a renew_after timestamp, but it's in the future
        // So need_renew() should return false initially
        assert!(!sub.need_renew());

        // The renew_after is set to renew_sent + (interval / 2)
        // Since we just acked, it should be ~30 seconds in the future
        // We can't easily test the time-based renewal in a unit test
        // without mocking time or sleeping
    }

    #[test]
    fn test_stateful_subscriptions_do_not_trigger_renewal_from_stale_deadline() {
        let mut sub = MonSub::new();
        sub.want(MonService::OsdMap, 0, 0);
        sub.renewed();

        sub.renew_after = Some(Instant::now() - std::time::Duration::from_secs(1));
        assert!(
            !sub.need_renew(),
            "modern/stateful subscriptions should ignore legacy renewal deadlines until an ack arrives"
        );
    }

    #[test]
    fn test_legacy_ack_enables_renewal_checks() {
        let mut sub = MonSub::new();
        sub.want(MonService::OsdMap, 0, 0);
        sub.renewed();
        sub.acked(60);

        sub.renew_after = Some(Instant::now() - std::time::Duration::from_secs(1));
        assert!(
            sub.need_renew(),
            "legacy subscribe acks should re-enable renewal checks"
        );
    }

    #[test]
    fn test_clear_resets_legacy_renewal_mode() {
        let mut sub = MonSub::new();
        sub.want(MonService::OsdMap, 0, 0);
        sub.renewed();
        sub.acked(60);
        sub.clear();

        sub.renew_after = Some(Instant::now() - std::time::Duration::from_secs(1));
        assert!(
            !sub.need_renew(),
            "clearing subscriptions should drop legacy renewal state"
        );
    }

    #[test]
    fn test_got_increments_epoch() {
        let mut sub = MonSub::new();

        // Subscribe starting from epoch 10
        sub.want(MonService::OsdMap, 10, 0);
        sub.renewed();

        // Simulate receiving epochs 10, 11, 12
        sub.got(MonService::OsdMap, 10);
        sub.reload();
        let subs = sub.get_subs();
        assert_eq!(subs.get(&MonService::OsdMap).unwrap().start, 11);

        sub.renewed();
        sub.got(MonService::OsdMap, 11);
        sub.reload();
        let subs = sub.get_subs();
        assert_eq!(subs.get(&MonService::OsdMap).unwrap().start, 12);

        sub.renewed();
        sub.got(MonService::OsdMap, 12);
        sub.reload();
        let subs = sub.get_subs();
        assert_eq!(subs.get(&MonService::OsdMap).unwrap().start, 13);
    }
}
