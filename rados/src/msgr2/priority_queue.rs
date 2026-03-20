//! Priority-based three-lane queue for outbound messages.

use std::collections::VecDeque;

use crate::msgr2::message::MessagePriority;

/// Types that can be dispatched into a priority queue.
pub trait Prioritized {
    fn priority(&self) -> MessagePriority;
}

/// Priority-based queue with three lanes (high → normal → low).
///
/// Higher-priority items are dequeued first. Within a priority level,
/// ordering is FIFO. This matches Ceph's ProtocolV2 outgoing queue behaviour.
#[derive(Debug, Clone, Default)]
pub struct PriorityQueue<T> {
    high: VecDeque<T>,
    normal: VecDeque<T>,
    low: VecDeque<T>,
}

impl<T: Prioritized> PriorityQueue<T> {
    pub fn new() -> Self {
        Self {
            high: VecDeque::new(),
            normal: VecDeque::new(),
            low: VecDeque::new(),
        }
    }

    /// Push an item into the appropriate priority sub-queue.
    pub fn push_back(&mut self, item: T) {
        match item.priority() {
            MessagePriority::High => self.high.push_back(item),
            MessagePriority::Normal => self.normal.push_back(item),
            MessagePriority::Low => self.low.push_back(item),
        }
    }

    /// Pop the highest-priority item available.
    pub fn pop_front(&mut self) -> Option<T> {
        self.high
            .pop_front()
            .or_else(|| self.normal.pop_front())
            .or_else(|| self.low.pop_front())
    }

    /// Total number of queued items across all priority levels.
    pub fn len(&self) -> usize {
        self.high.len() + self.normal.len() + self.low.len()
    }

    /// Returns `true` if all priority queues are empty.
    pub fn is_empty(&self) -> bool {
        self.high.is_empty() && self.normal.is_empty() && self.low.is_empty()
    }

    /// Clear all queues.
    pub fn clear(&mut self) {
        self.high.clear();
        self.normal.clear();
        self.low.clear();
    }
}

impl<T> PriorityQueue<T> {
    /// Peek at the next item (highest-priority queue's front).
    pub fn front(&self) -> Option<&T> {
        self.high
            .front()
            .or_else(|| self.normal.front())
            .or_else(|| self.low.front())
    }

    /// Iterate over all items in priority order (high → normal → low).
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.high
            .iter()
            .chain(self.normal.iter())
            .chain(self.low.iter())
    }
}
