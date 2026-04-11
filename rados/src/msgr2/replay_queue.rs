//! Bounded replay queue of unacknowledged sent messages.
//!
//! When a msgr2 connection reconnects (after a transient TCP failure) it
//! can resume its session via SESSION_RECONNECT and replay any messages
//! the server hadn't acknowledged yet. The queue here tracks those
//! messages between `send_message` and the matching `Ack` frame from the
//! peer, optionally bounded by `ConnectionConfig::max_replay_queue_len`
//! so a stalled peer can't make the queue grow without limit.
//!
//! This type previously lived in `rados/src/msgr2/split.rs` because it
//! was shared between `ConnectionState::SessionState` (the unsplit path)
//! and `SharedState` (the split halves path). The split-halves machinery
//! has been deleted; `ReplayQueue` now stands on its own, used only by
//! `ConnectionState`.

use std::collections::VecDeque;

use crate::msgr2::error::{Msgr2Error as Error, Result};
use crate::msgr2::message::Message;

/// Bounded queue of sent messages kept for potential replay on reconnect.
#[derive(Debug, Clone, Default)]
pub struct ReplayQueue {
    messages: VecDeque<Message>,
    max: Option<usize>,
}

impl ReplayQueue {
    pub fn new(max: Option<usize>) -> Self {
        Self {
            messages: VecDeque::new(),
            max,
        }
    }

    /// Return an error if the queue is already at its configured limit.
    ///
    /// Used internally by [`record`](Self::record) and kept private because
    /// external callers should go through `record` rather than manually
    /// splitting limit-check from insertion — the split path that used to
    /// need that decomposition is gone.
    fn check_limit(&self) -> Result<()> {
        if let Some(limit) = self.max
            && self.messages.len() >= limit
        {
            return Err(Error::Protocol(format!(
                "replay queue limit ({limit}) reached; acknowledge or raise \
                 ConnectionConfig::max_replay_queue_len before sending more messages"
            )));
        }
        Ok(())
    }

    /// Check the configured bound, then record the message for replay.
    pub fn record(&mut self, message: Message) -> Result<()> {
        self.check_limit()?;
        self.messages.push_back(message);
        Ok(())
    }

    /// Drop all messages whose sequence number is ≤ `ack_seq`.
    ///
    /// Called in response to an inbound `Ack` frame or the ack_seq piggy-
    /// backed on every Message header: both carry the peer's most recent
    /// acknowledged sequence, and everything at or below it can be
    /// discarded from the replay buffer.
    pub fn discard_acked(&mut self, ack_seq: u64) {
        while self
            .messages
            .front()
            .is_some_and(|m| m.header.get_seq() <= ack_seq)
        {
            self.messages.pop_front();
        }
    }

    pub fn clear(&mut self) {
        self.messages.clear();
    }

    pub fn len(&self) -> usize {
        self.messages.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Message> {
        self.messages.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn record_and_discard_acked() {
        let mut queue = ReplayQueue::new(None);
        for seq in 1..=5u64 {
            queue
                .record(Message::new(0, Bytes::new()).with_seq(seq))
                .expect("unbounded queue should always accept");
        }
        assert_eq!(queue.len(), 5);

        queue.discard_acked(3);
        assert_eq!(queue.len(), 2);
        // Front should now be seq=4. Use get_seq() to avoid an unaligned
        // reference to the packed MsgHeader.
        assert_eq!(queue.iter().next().unwrap().header.get_seq(), 4);

        queue.discard_acked(10);
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn record_respects_bounded_limit() {
        let mut queue = ReplayQueue::new(Some(2));
        queue
            .record(Message::new(0, Bytes::new()).with_seq(1))
            .unwrap();
        queue
            .record(Message::new(0, Bytes::new()).with_seq(2))
            .unwrap();

        // Third record must fail because the bound is 2.
        let result = queue.record(Message::new(0, Bytes::new()).with_seq(3));
        assert!(result.is_err(), "bounded queue should reject overflow");
        assert_eq!(queue.len(), 2);

        // Ack clears room; the next record succeeds again.
        queue.discard_acked(1);
        assert_eq!(queue.len(), 1);
        queue
            .record(Message::new(0, Bytes::new()).with_seq(3))
            .expect("queue has room after ack");
        assert_eq!(queue.len(), 2);
    }

    #[test]
    fn clear_empties_the_queue() {
        let mut queue = ReplayQueue::new(None);
        queue
            .record(Message::new(0, Bytes::new()).with_seq(1))
            .unwrap();
        queue
            .record(Message::new(0, Bytes::new()).with_seq(2))
            .unwrap();
        assert_eq!(queue.len(), 2);

        queue.clear();
        assert_eq!(queue.len(), 0);
        assert!(queue.iter().next().is_none());
    }
}
