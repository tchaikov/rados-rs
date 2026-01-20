//! Monitor subscription management
//!
//! Handles subscribing to cluster maps (osdmap, monmap, etc.) and tracking
//! subscription state.

use std::collections::HashMap;
use std::time::Instant;

/// Subscription flag: unsubscribe after receiving one update
pub const CEPH_SUBSCRIBE_ONETIME: u8 = 1;

/// Subscription manager
///
/// Tracks which maps we want to subscribe to and manages the subscription lifecycle.
#[derive(Debug)]
pub struct MonSub {
    /// Pending subscriptions to send
    sub_new: HashMap<String, SubscribeItem>,

    /// Sent subscriptions waiting for ack
    sub_sent: HashMap<String, SubscribeItem>,

    /// When we last sent a renewal
    renew_sent: Option<Instant>,

    /// When we should renew subscriptions
    renew_after: Option<Instant>,
}

impl MonSub {
    pub fn new() -> Self {
        Self {
            sub_new: HashMap::new(),
            sub_sent: HashMap::new(),
            renew_sent: None,
            renew_after: None,
        }
    }

    /// Check if there are new subscriptions to send
    pub fn have_new(&self) -> bool {
        !self.sub_new.is_empty()
    }

    /// Check if subscriptions need renewal
    pub fn need_renew(&self) -> bool {
        if let Some(renew_after) = self.renew_after {
            Instant::now() > renew_after
        } else {
            false
        }
    }

    /// Get the new subscriptions to send
    pub fn get_subs(&self) -> &HashMap<String, SubscribeItem> {
        &self.sub_new
    }

    /// Mark subscriptions as sent
    pub fn renewed(&mut self) {
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
        if let Some(renew_sent) = self.renew_sent {
            // Schedule renewal for half the interval
            let interval = std::time::Duration::from_secs(interval_secs as u64 / 2);
            self.renew_after = Some(renew_sent + interval);
            self.renew_sent = None;
        }
    }

    /// Mark that we received a map update
    pub fn got(&mut self, what: &str, version: u64) {
        // Check sub_new first
        if let Some(item) = self.sub_new.get_mut(what) {
            if item.start <= version {
                if item.flags & CEPH_SUBSCRIBE_ONETIME != 0 {
                    self.sub_new.remove(what);
                } else {
                    item.start = version + 1;
                }
            }
        }
        // Then check sub_sent
        else if let Some(item) = self.sub_sent.get_mut(what) {
            if item.start <= version {
                if item.flags & CEPH_SUBSCRIBE_ONETIME != 0 {
                    self.sub_sent.remove(what);
                } else {
                    item.start = version + 1;
                }
            }
        }
    }

    /// Reload sent subscriptions back to new (for reconnection)
    pub fn reload(&mut self) -> bool {
        for (what, item) in &self.sub_sent {
            if !self.sub_new.contains_key(what) {
                self.sub_new.insert(what.clone(), *item);
            }
        }
        self.have_new()
    }

    /// Add a new subscription
    ///
    /// Returns true if this is a new or changed subscription
    pub fn want(&mut self, what: impl Into<String>, start: u64, flags: u8) -> bool {
        let what = what.into();
        let new_item = SubscribeItem { start, flags };

        // Check if already in sub_new with same params
        if let Some(item) = self.sub_new.get(&what) {
            if item.start == start && item.flags == flags {
                return false;
            }
        }
        // Check if already in sub_sent with same params
        else if let Some(item) = self.sub_sent.get(&what) {
            if item.start == start && item.flags == flags {
                return false;
            }
        }

        self.sub_new.insert(what, new_item);
        true
    }

    /// Increment subscription start version
    ///
    /// Only updates if the new start is greater than current
    pub fn inc_want(&mut self, what: impl Into<String>, start: u64, flags: u8) -> bool {
        let what = what.into();

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
        if let Some(item) = self.sub_sent.get(&what) {
            if item.start >= start {
                return false;
            }
        }

        // Add to sub_new
        self.sub_new.insert(what, SubscribeItem { start, flags });
        true
    }

    /// Remove a subscription
    pub fn unwant(&mut self, what: &str) {
        self.sub_new.remove(what);
        self.sub_sent.remove(what);
    }

    /// Clear all subscriptions
    pub fn clear(&mut self) {
        self.sub_new.clear();
        self.sub_sent.clear();
        self.renew_sent = None;
        self.renew_after = None;
    }
}

impl Default for MonSub {
    fn default() -> Self {
        Self::new()
    }
}

/// A subscription item
#[derive(Debug, Clone, Copy)]
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
        assert!(sub.want("osdmap", 0, 0));
        assert!(sub.have_new());

        // Adding same subscription returns false
        assert!(!sub.want("osdmap", 0, 0));

        // Mark as sent
        sub.renewed();
        assert!(!sub.have_new());

        // Got an update
        sub.got("osdmap", 5);

        // Reload after disconnect
        assert!(sub.reload());
        assert!(sub.have_new());
    }

    #[test]
    fn test_onetime_subscription() {
        let mut sub = MonSub::new();

        // Subscribe with ONETIME flag
        sub.want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME);
        sub.renewed();

        // After receiving update, subscription is removed
        sub.got("osdmap", 1);
        assert!(!sub.reload());
    }

    #[test]
    fn test_inc_want() {
        let mut sub = MonSub::new();

        // Initial subscription
        assert!(sub.inc_want("osdmap", 10, 0));

        // Lower version doesn't update
        assert!(!sub.inc_want("osdmap", 5, 0));

        // Higher version updates
        assert!(sub.inc_want("osdmap", 15, 0));
    }
}
