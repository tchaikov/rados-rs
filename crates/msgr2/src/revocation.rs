//! Message revocation for msgr2 connections
//!
//! This module implements the ability to cancel in-flight messages before
//! transmission completes. This is useful for:
//! - Canceling requests that are no longer needed
//! - Implementing timeouts for operations
//! - Cleaning up on connection errors
//!
//! Messages can be revoked at different stages:
//! - **Queued**: Message is waiting to be sent (easy to revoke)
//! - **Sending**: Message is currently being transmitted (may be partially sent)
//! - **Sent**: Message has been fully transmitted (cannot be revoked)

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};

/// Unique identifier for a message that can be revoked
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MessageId(u64);

impl MessageId {
    /// Create a new message ID
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Get the raw ID value
    pub fn value(&self) -> u64 {
        self.0
    }
}

/// Status of a message in the revocation system
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageStatus {
    /// Message is queued and waiting to be sent
    Queued,
    /// Message is currently being transmitted
    Sending,
    /// Message has been fully transmitted
    Sent,
    /// Message was revoked before transmission completed
    Revoked,
}

/// Result of a revocation attempt
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevocationResult {
    /// Message was successfully revoked (was queued or sending)
    Revoked,
    /// Message was already sent (too late to revoke)
    AlreadySent,
    /// Message ID not found (already completed or never existed)
    NotFound,
}

/// Handle for a revocable message
///
/// This handle can be used to check the status of a message or revoke it.
/// When dropped, the handle does NOT automatically revoke the message.
#[derive(Debug)]
pub struct MessageHandle {
    id: MessageId,
    revocation_tx: Option<oneshot::Sender<()>>,
    manager: Arc<Mutex<RevocationManagerState>>,
}

impl MessageHandle {
    /// Get the message ID
    pub fn id(&self) -> MessageId {
        self.id
    }

    /// Check if the message has been revoked
    pub async fn is_revoked(&self) -> bool {
        let state = self.manager.lock().await;
        state
            .messages
            .get(&self.id)
            .map(|info| info.status == MessageStatus::Revoked)
            .unwrap_or(false)
    }

    /// Get the current status of the message
    pub async fn status(&self) -> Option<MessageStatus> {
        let state = self.manager.lock().await;
        state.messages.get(&self.id).map(|info| info.status)
    }

    /// Attempt to revoke the message
    ///
    /// Returns:
    /// - `Revoked`: Message was successfully revoked
    /// - `AlreadySent`: Message was already fully transmitted
    /// - `NotFound`: Message ID not found
    pub async fn revoke(mut self) -> RevocationResult {
        let mut state = self.manager.lock().await;

        if let Some(info) = state.messages.get_mut(&self.id) {
            match info.status {
                MessageStatus::Queued | MessageStatus::Sending => {
                    // Mark as revoked
                    info.status = MessageStatus::Revoked;

                    // Signal the sender to stop
                    if let Some(tx) = self.revocation_tx.take() {
                        let _ = tx.send(());
                    }

                    tracing::debug!("Message {:?} revoked", self.id);
                    RevocationResult::Revoked
                }
                MessageStatus::Sent => {
                    tracing::debug!("Message {:?} already sent, cannot revoke", self.id);
                    RevocationResult::AlreadySent
                }
                MessageStatus::Revoked => {
                    tracing::debug!("Message {:?} already revoked", self.id);
                    RevocationResult::Revoked
                }
            }
        } else {
            tracing::debug!("Message {:?} not found", self.id);
            RevocationResult::NotFound
        }
    }

    /// Wait for the message to complete (either sent or revoked)
    ///
    /// Returns the final status of the message.
    pub async fn wait_completion(&self) -> MessageStatus {
        loop {
            if let Some(status) = self.status().await {
                match status {
                    MessageStatus::Sent | MessageStatus::Revoked => return status,
                    _ => {
                        // Still in progress, wait a bit
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    }
                }
            } else {
                // Message not found, assume it completed
                return MessageStatus::Sent;
            }
        }
    }
}

/// Information about a tracked message
#[derive(Debug)]
struct MessageInfo {
    status: MessageStatus,
}

/// Internal state for the revocation manager
#[derive(Debug)]
struct RevocationManagerState {
    /// Next message ID to assign
    next_id: u64,
    /// Map of message ID to message info
    messages: HashMap<MessageId, MessageInfo>,
}

impl RevocationManagerState {
    fn new() -> Self {
        Self {
            next_id: 1,
            messages: HashMap::new(),
        }
    }

    fn allocate_id(&mut self) -> MessageId {
        let id = MessageId(self.next_id);
        self.next_id += 1;
        id
    }
}

/// Manager for message revocation
///
/// This tracks all in-flight messages and allows them to be revoked
/// before transmission completes.
///
/// # Example
///
/// ```no_run
/// use msgr2::revocation::RevocationManager;
///
/// # async fn example() -> Result<(), &'static str> {
/// let manager = RevocationManager::new();
///
/// // Register a message for sending
/// let (handle, revocation_rx) = manager.register_message().await;
///
/// // Simulate sending - check for revocation
/// let send_result: Result<(), &str> = tokio::select! {
///     _ = revocation_rx => {
///         // Message was revoked
///         Err("revoked")
///     }
///     _ = async {
///         // Simulate message send
///         tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
///         Ok::<(), &str>(())
///     } => {
///         // Message sent successfully
///         manager.mark_sent(handle.id()).await;
///         Ok(())
///     }
/// };
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct RevocationManager {
    state: Arc<Mutex<RevocationManagerState>>,
}

impl RevocationManager {
    /// Create a new revocation manager
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(RevocationManagerState::new())),
        }
    }

    /// Register a new message for sending
    ///
    /// Returns:
    /// - A handle that can be used to revoke the message
    /// - A receiver that will be signaled if the message is revoked
    pub async fn register_message(&self) -> (MessageHandle, oneshot::Receiver<()>) {
        let mut state = self.state.lock().await;

        let id = state.allocate_id();
        let (revocation_tx, revocation_rx) = oneshot::channel();

        state.messages.insert(
            id,
            MessageInfo {
                status: MessageStatus::Queued,
            },
        );

        tracing::trace!("Registered message {:?} for revocation tracking", id);

        let handle = MessageHandle {
            id,
            revocation_tx: Some(revocation_tx),
            manager: self.state.clone(),
        };

        (handle, revocation_rx)
    }

    /// Mark a message as currently being sent
    pub async fn mark_sending(&self, id: MessageId) {
        let mut state = self.state.lock().await;
        if let Some(info) = state.messages.get_mut(&id) {
            if info.status == MessageStatus::Queued {
                info.status = MessageStatus::Sending;
                tracing::trace!("Message {:?} marked as sending", id);
            }
        }
    }

    /// Mark a message as fully sent
    pub async fn mark_sent(&self, id: MessageId) {
        let mut state = self.state.lock().await;
        if let Some(info) = state.messages.get_mut(&id) {
            if info.status != MessageStatus::Revoked {
                info.status = MessageStatus::Sent;
                tracing::trace!("Message {:?} marked as sent", id);
            }
        }
    }

    /// Remove a message from tracking (cleanup after completion)
    pub async fn remove_message(&self, id: MessageId) {
        let mut state = self.state.lock().await;
        state.messages.remove(&id);
        tracing::trace!("Message {:?} removed from tracking", id);
    }

    /// Get the status of a message
    pub async fn get_status(&self, id: MessageId) -> Option<MessageStatus> {
        let state = self.state.lock().await;
        state.messages.get(&id).map(|info| info.status)
    }

    /// Get the number of tracked messages
    pub async fn tracked_count(&self) -> usize {
        let state = self.state.lock().await;
        state.messages.len()
    }

    /// Clear all tracked messages (useful for cleanup on connection close)
    pub async fn clear(&self) {
        let mut state = self.state.lock().await;
        state.messages.clear();
        tracing::debug!("Cleared all tracked messages");
    }

    /// Get statistics about tracked messages
    pub async fn stats(&self) -> RevocationStats {
        let state = self.state.lock().await;

        let mut queued = 0;
        let mut sending = 0;
        let mut sent = 0;
        let mut revoked = 0;

        for info in state.messages.values() {
            match info.status {
                MessageStatus::Queued => queued += 1,
                MessageStatus::Sending => sending += 1,
                MessageStatus::Sent => sent += 1,
                MessageStatus::Revoked => revoked += 1,
            }
        }

        RevocationStats {
            total: state.messages.len(),
            queued,
            sending,
            sent,
            revoked,
        }
    }
}

impl Default for RevocationManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about message revocation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RevocationStats {
    /// Total number of tracked messages
    pub total: usize,
    /// Number of queued messages
    pub queued: usize,
    /// Number of messages currently being sent
    pub sending: usize,
    /// Number of sent messages
    pub sent: usize,
    /// Number of revoked messages
    pub revoked: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_register_and_revoke() {
        let manager = RevocationManager::new();

        let (handle, mut revocation_rx) = manager.register_message().await;
        let id = handle.id();

        // Should be queued initially
        assert_eq!(manager.get_status(id).await, Some(MessageStatus::Queued));

        // Revoke the message
        let result = handle.revoke().await;
        assert_eq!(result, RevocationResult::Revoked);

        // Should be revoked now
        assert_eq!(manager.get_status(id).await, Some(MessageStatus::Revoked));

        // Revocation signal should be received
        assert!(revocation_rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn test_revoke_after_sent() {
        let manager = RevocationManager::new();

        let (handle, _revocation_rx) = manager.register_message().await;
        let id = handle.id();

        // Mark as sent
        manager.mark_sent(id).await;

        // Try to revoke
        let result = handle.revoke().await;
        assert_eq!(result, RevocationResult::AlreadySent);

        // Should still be sent
        assert_eq!(manager.get_status(id).await, Some(MessageStatus::Sent));
    }

    #[tokio::test]
    async fn test_mark_sending() {
        let manager = RevocationManager::new();

        let (handle, _revocation_rx) = manager.register_message().await;
        let id = handle.id();

        // Mark as sending
        manager.mark_sending(id).await;
        assert_eq!(manager.get_status(id).await, Some(MessageStatus::Sending));

        // Can still revoke while sending
        let result = handle.revoke().await;
        assert_eq!(result, RevocationResult::Revoked);
    }

    #[tokio::test]
    async fn test_multiple_messages() {
        let manager = RevocationManager::new();

        let (handle1, _rx1) = manager.register_message().await;
        let (handle2, _rx2) = manager.register_message().await;
        let (handle3, _rx3) = manager.register_message().await;

        let id1 = handle1.id();
        let id2 = handle2.id();
        let id3 = handle3.id();

        // All should be different
        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);

        // Revoke first
        handle1.revoke().await;

        // Mark second as sent
        manager.mark_sent(id2).await;

        // Check statuses
        assert_eq!(manager.get_status(id1).await, Some(MessageStatus::Revoked));
        assert_eq!(manager.get_status(id2).await, Some(MessageStatus::Sent));
        assert_eq!(manager.get_status(id3).await, Some(MessageStatus::Queued));
    }

    #[tokio::test]
    async fn test_remove_message() {
        let manager = RevocationManager::new();

        let (handle, _rx) = manager.register_message().await;
        let id = handle.id();

        assert_eq!(manager.tracked_count().await, 1);

        manager.remove_message(id).await;

        assert_eq!(manager.tracked_count().await, 0);
        assert_eq!(manager.get_status(id).await, None);
    }

    #[tokio::test]
    async fn test_clear() {
        let manager = RevocationManager::new();

        let (_h1, _rx1) = manager.register_message().await;
        let (_h2, _rx2) = manager.register_message().await;
        let (_h3, _rx3) = manager.register_message().await;

        assert_eq!(manager.tracked_count().await, 3);

        manager.clear().await;

        assert_eq!(manager.tracked_count().await, 0);
    }

    #[tokio::test]
    async fn test_stats() {
        let manager = RevocationManager::new();

        let (_h1, _rx1) = manager.register_message().await;
        let (h2, _rx2) = manager.register_message().await;
        let (h3, _rx3) = manager.register_message().await;
        let (h4, _rx4) = manager.register_message().await;

        // Mark different statuses
        manager.mark_sending(h2.id()).await;
        manager.mark_sent(h3.id()).await;
        h4.revoke().await;

        let stats = manager.stats().await;
        assert_eq!(stats.total, 4);
        assert_eq!(stats.queued, 1); // h1
        assert_eq!(stats.sending, 1); // h2
        assert_eq!(stats.sent, 1); // h3
        assert_eq!(stats.revoked, 1); // h4
    }

    #[tokio::test]
    async fn test_wait_completion() {
        let manager = RevocationManager::new();

        let (handle, _rx) = manager.register_message().await;
        let id = handle.id();

        // Spawn a task to mark as sent after a delay
        let manager_clone = manager.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;
            manager_clone.mark_sent(id).await;
        });

        // Wait for completion
        let status = handle.wait_completion().await;
        assert_eq!(status, MessageStatus::Sent);
    }

    #[tokio::test]
    async fn test_revocation_signal() {
        let manager = RevocationManager::new();

        let (handle, revocation_rx) = manager.register_message().await;

        // Spawn a task to revoke after a delay
        tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;
            handle.revoke().await;
        });

        // Wait for revocation signal
        let result = tokio::time::timeout(Duration::from_millis(200), revocation_rx).await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_handle_is_revoked() {
        let manager = RevocationManager::new();

        let (handle, _rx) = manager.register_message().await;

        assert!(!handle.is_revoked().await);

        let id = handle.id();
        manager.mark_sent(id).await;

        assert!(!handle.is_revoked().await);
    }

    #[tokio::test]
    async fn test_revoke_not_found() {
        let manager = RevocationManager::new();

        let (handle, _rx) = manager.register_message().await;
        let id = handle.id();

        // Remove the message
        manager.remove_message(id).await;

        // Try to revoke
        let result = handle.revoke().await;
        assert_eq!(result, RevocationResult::NotFound);
    }
}
