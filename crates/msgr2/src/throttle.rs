//! Message throttling for msgr2 connections
//!
//! This module implements client-side message throttling to control:
//! - Message rate (messages per second)
//! - Byte throughput (bytes per second)
//! - Dispatch queue depth (number of in-flight messages)
//!
//! Throttling provides backpressure control to prevent overwhelming the server
//! and ensures fair resource usage across multiple connections.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Throttle configuration for message sending
#[derive(Debug, Clone)]
pub struct ThrottleConfig {
    /// Maximum number of messages per second (0 = unlimited)
    pub max_messages_per_sec: u64,

    /// Maximum bytes per second (0 = unlimited)
    pub max_bytes_per_sec: u64,

    /// Maximum number of in-flight messages (0 = unlimited)
    pub max_dispatch_queue_depth: usize,

    /// Time window for rate limiting (default: 1 second)
    pub rate_window: Duration,
}

impl Default for ThrottleConfig {
    fn default() -> Self {
        Self {
            max_messages_per_sec: 0,     // Unlimited by default
            max_bytes_per_sec: 0,        // Unlimited by default
            max_dispatch_queue_depth: 0, // Unlimited by default
            rate_window: Duration::from_secs(1),
        }
    }
}

impl ThrottleConfig {
    /// Create a throttle config with no limits (pass-through)
    pub fn unlimited() -> Self {
        Self::default()
    }

    /// Create a throttle config with message rate limit
    pub fn with_message_rate(max_messages_per_sec: u64) -> Self {
        Self {
            max_messages_per_sec,
            ..Default::default()
        }
    }

    /// Create a throttle config with byte rate limit
    pub fn with_byte_rate(max_bytes_per_sec: u64) -> Self {
        Self {
            max_bytes_per_sec,
            ..Default::default()
        }
    }

    /// Create a throttle config with dispatch queue depth limit
    pub fn with_queue_depth(max_dispatch_queue_depth: usize) -> Self {
        Self {
            max_dispatch_queue_depth,
            ..Default::default()
        }
    }

    /// Create a throttle config with all limits
    pub fn with_limits(
        max_messages_per_sec: u64,
        max_bytes_per_sec: u64,
        max_dispatch_queue_depth: usize,
    ) -> Self {
        Self {
            max_messages_per_sec,
            max_bytes_per_sec,
            max_dispatch_queue_depth,
            rate_window: Duration::from_secs(1),
        }
    }
}

/// Message send record for rate tracking
#[derive(Debug, Clone)]
struct SendRecord {
    timestamp: Instant,
    byte_count: usize,
}

/// Message throttle state
#[derive(Debug)]
struct ThrottleState {
    /// Configuration
    config: ThrottleConfig,

    /// Recent send history (within rate_window)
    send_history: VecDeque<SendRecord>,

    /// Current number of in-flight messages
    in_flight_count: usize,

    /// Total messages sent
    total_messages: u64,

    /// Total bytes sent
    total_bytes: u64,
}

impl ThrottleState {
    fn new(config: ThrottleConfig) -> Self {
        Self {
            config,
            send_history: VecDeque::new(),
            in_flight_count: 0,
            total_messages: 0,
            total_bytes: 0,
        }
    }

    /// Clean up old send records outside the rate window
    fn cleanup_old_records(&mut self, now: Instant) {
        let cutoff = now - self.config.rate_window;
        while let Some(record) = self.send_history.front() {
            if record.timestamp < cutoff {
                self.send_history.pop_front();
            } else {
                break;
            }
        }
    }

    /// Check if we can send a message of the given size
    fn can_send(&mut self, byte_count: usize) -> bool {
        let now = Instant::now();
        self.cleanup_old_records(now);

        // Check dispatch queue depth
        if self.config.max_dispatch_queue_depth > 0
            && self.in_flight_count >= self.config.max_dispatch_queue_depth
        {
            tracing::trace!(
                "Throttle: queue depth limit reached ({}/{})",
                self.in_flight_count,
                self.config.max_dispatch_queue_depth
            );
            return false;
        }

        // Check message rate
        if self.config.max_messages_per_sec > 0 {
            let recent_messages = self.send_history.len() as u64;
            if recent_messages >= self.config.max_messages_per_sec {
                tracing::trace!(
                    "Throttle: message rate limit reached ({}/{})",
                    recent_messages,
                    self.config.max_messages_per_sec
                );
                return false;
            }
        }

        // Check byte rate
        if self.config.max_bytes_per_sec > 0 {
            let recent_bytes: usize = self.send_history.iter().map(|r| r.byte_count).sum();
            if recent_bytes + byte_count > self.config.max_bytes_per_sec as usize {
                tracing::trace!(
                    "Throttle: byte rate limit reached ({} + {} > {})",
                    recent_bytes,
                    byte_count,
                    self.config.max_bytes_per_sec
                );
                return false;
            }
        }

        true
    }

    /// Record a message send
    fn record_send(&mut self, byte_count: usize) {
        self.send_history.push_back(SendRecord {
            timestamp: Instant::now(),
            byte_count,
        });
        self.in_flight_count += 1;
        self.total_messages += 1;
        self.total_bytes += byte_count as u64;

        tracing::trace!(
            "Throttle: recorded send {} bytes, in_flight={}, total_messages={}, total_bytes={}",
            byte_count,
            self.in_flight_count,
            self.total_messages,
            self.total_bytes
        );
    }

    /// Record a message acknowledgment (reduces in-flight count)
    fn record_ack(&mut self) {
        if self.in_flight_count > 0 {
            self.in_flight_count -= 1;
            tracing::trace!("Throttle: recorded ack, in_flight={}", self.in_flight_count);
        }
    }

    /// Get current statistics
    fn stats(&self) -> ThrottleStats {
        let recent_messages = self.send_history.len();
        let recent_bytes: usize = self.send_history.iter().map(|r| r.byte_count).sum();

        ThrottleStats {
            in_flight_count: self.in_flight_count,
            recent_messages,
            recent_bytes,
            total_messages: self.total_messages,
            total_bytes: self.total_bytes,
        }
    }
}

/// Throttle statistics
#[derive(Debug, Clone)]
pub struct ThrottleStats {
    /// Current number of in-flight messages
    pub in_flight_count: usize,

    /// Number of messages sent in the current rate window
    pub recent_messages: usize,

    /// Number of bytes sent in the current rate window
    pub recent_bytes: usize,

    /// Total messages sent since connection start
    pub total_messages: u64,

    /// Total bytes sent since connection start
    pub total_bytes: u64,
}

/// Message throttle for controlling send rate
///
/// This provides backpressure control by limiting:
/// - Message rate (messages per second)
/// - Byte throughput (bytes per second)
/// - Dispatch queue depth (number of in-flight messages)
///
/// # Example
///
/// ```no_run
/// use msgr2::throttle::{MessageThrottle, ThrottleConfig};
///
/// # async fn example() {
/// // Create throttle with 100 messages/sec and 1MB/sec limits
/// let config = ThrottleConfig::with_limits(100, 1024 * 1024, 50);
/// let throttle = MessageThrottle::new(config);
///
/// // Wait for permission to send
/// throttle.wait_for_send(1024).await;
///
/// // Record the send
/// throttle.record_send(1024).await;
///
/// // Later, when ACK is received
/// throttle.record_ack().await;
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct MessageThrottle {
    state: Arc<Mutex<ThrottleState>>,
}

impl MessageThrottle {
    /// Create a new message throttle with the given configuration
    pub fn new(config: ThrottleConfig) -> Self {
        Self {
            state: Arc::new(Mutex::new(ThrottleState::new(config))),
        }
    }

    /// Create an unlimited throttle (no limits)
    pub fn unlimited() -> Self {
        Self::new(ThrottleConfig::unlimited())
    }

    /// Wait until we can send a message of the given size
    ///
    /// This will block until throttle limits allow the send.
    /// Returns immediately if no limits are configured.
    pub async fn wait_for_send(&self, byte_count: usize) {
        loop {
            {
                let mut state = self.state.lock().await;
                if state.can_send(byte_count) {
                    return;
                }
            }

            // Wait a bit before checking again
            // Use a small delay to avoid busy-waiting
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Try to acquire permission to send a message of the given size
    ///
    /// Returns true if the send is allowed, false if throttled.
    /// This is a non-blocking version of wait_for_send().
    pub async fn try_send(&self, byte_count: usize) -> bool {
        let mut state = self.state.lock().await;
        state.can_send(byte_count)
    }

    /// Record a message send
    ///
    /// This should be called after successfully sending a message.
    /// It updates the throttle state to track the send.
    pub async fn record_send(&self, byte_count: usize) {
        let mut state = self.state.lock().await;
        state.record_send(byte_count);
    }

    /// Record a message acknowledgment
    ///
    /// This should be called when an ACK is received for a sent message.
    /// It reduces the in-flight message count.
    pub async fn record_ack(&self) {
        let mut state = self.state.lock().await;
        state.record_ack();
    }

    /// Get current throttle statistics
    pub async fn stats(&self) -> ThrottleStats {
        let state = self.state.lock().await;
        state.stats()
    }

    /// Reset throttle state (clear history and counters)
    pub async fn reset(&self) {
        let mut state = self.state.lock().await;
        state.send_history.clear();
        state.in_flight_count = 0;
        state.total_messages = 0;
        state.total_bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_unlimited_throttle() {
        let throttle = MessageThrottle::unlimited();

        // Should allow any number of sends
        for _ in 0..1000 {
            assert!(throttle.try_send(1024).await);
            throttle.record_send(1024).await;
        }

        let stats = throttle.stats().await;
        assert_eq!(stats.total_messages, 1000);
        assert_eq!(stats.total_bytes, 1024 * 1000);
    }

    #[tokio::test]
    async fn test_message_rate_limit() {
        let config = ThrottleConfig::with_message_rate(10);
        let throttle = MessageThrottle::new(config);

        // Should allow first 10 messages
        for i in 0..10 {
            assert!(
                throttle.try_send(100).await,
                "Message {} should be allowed",
                i
            );
            throttle.record_send(100).await;
        }

        // 11th message should be throttled
        assert!(
            !throttle.try_send(100).await,
            "Message 11 should be throttled"
        );

        let stats = throttle.stats().await;
        assert_eq!(stats.total_messages, 10);
        assert_eq!(stats.recent_messages, 10);
    }

    #[tokio::test]
    async fn test_byte_rate_limit() {
        let config = ThrottleConfig::with_byte_rate(1000);
        let throttle = MessageThrottle::new(config);

        // Should allow messages up to 1000 bytes
        assert!(throttle.try_send(500).await);
        throttle.record_send(500).await;

        assert!(throttle.try_send(400).await);
        throttle.record_send(400).await;

        // Next message would exceed limit
        assert!(!throttle.try_send(200).await);

        let stats = throttle.stats().await;
        assert_eq!(stats.total_bytes, 900);
        assert_eq!(stats.recent_bytes, 900);
    }

    #[tokio::test]
    async fn test_queue_depth_limit() {
        let config = ThrottleConfig::with_queue_depth(5);
        let throttle = MessageThrottle::new(config);

        // Should allow first 5 messages
        for i in 0..5 {
            assert!(
                throttle.try_send(100).await,
                "Message {} should be allowed",
                i
            );
            throttle.record_send(100).await;
        }

        // 6th message should be throttled (queue full)
        assert!(
            !throttle.try_send(100).await,
            "Message 6 should be throttled"
        );

        // ACK one message
        throttle.record_ack().await;

        // Now should allow another message
        assert!(
            throttle.try_send(100).await,
            "Message should be allowed after ACK"
        );
        throttle.record_send(100).await;

        let stats = throttle.stats().await;
        assert_eq!(stats.in_flight_count, 5);
    }

    #[tokio::test]
    async fn test_rate_window_expiry() {
        let mut config = ThrottleConfig::with_message_rate(5);
        config.rate_window = Duration::from_millis(100);
        let throttle = MessageThrottle::new(config);

        // Send 5 messages
        for _ in 0..5 {
            assert!(throttle.try_send(100).await);
            throttle.record_send(100).await;
        }

        // Should be throttled
        assert!(!throttle.try_send(100).await);

        // Wait for rate window to expire
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Should allow new messages
        assert!(throttle.try_send(100).await);
    }

    #[tokio::test]
    async fn test_combined_limits() {
        let config = ThrottleConfig::with_limits(10, 1000, 5);
        let throttle = MessageThrottle::new(config);

        // Send 5 messages of 150 bytes each (750 bytes total)
        for _ in 0..5 {
            assert!(throttle.try_send(150).await);
            throttle.record_send(150).await;
        }

        // Queue depth limit reached (5 in-flight)
        assert!(!throttle.try_send(150).await);

        // ACK one message
        throttle.record_ack().await;

        // Now limited by byte rate (750 + 150 = 900 < 1000, so should work)
        assert!(throttle.try_send(150).await);
        throttle.record_send(150).await;

        // Now at byte limit (900 bytes)
        assert!(!throttle.try_send(150).await);

        let stats = throttle.stats().await;
        assert_eq!(stats.in_flight_count, 5);
        assert_eq!(stats.total_messages, 6);
        assert_eq!(stats.total_bytes, 900);
    }

    #[tokio::test]
    async fn test_wait_for_send() {
        let mut config = ThrottleConfig::with_message_rate(2);
        config.rate_window = Duration::from_millis(100);
        let throttle = MessageThrottle::new(config);

        // Send 2 messages
        throttle.wait_for_send(100).await;
        throttle.record_send(100).await;

        throttle.wait_for_send(100).await;
        throttle.record_send(100).await;

        // This should wait until rate window expires
        let start = Instant::now();
        throttle.wait_for_send(100).await;
        let elapsed = start.elapsed();

        // Should have waited at least 100ms for rate window to expire
        assert!(elapsed >= Duration::from_millis(90)); // Allow some slack
    }

    #[tokio::test]
    async fn test_reset() {
        let config = ThrottleConfig::with_limits(10, 1000, 5);
        let throttle = MessageThrottle::new(config);

        // Send some messages
        for _ in 0..5 {
            throttle.record_send(100).await;
        }

        let stats = throttle.stats().await;
        assert_eq!(stats.total_messages, 5);
        assert_eq!(stats.in_flight_count, 5);

        // Reset
        throttle.reset().await;

        let stats = throttle.stats().await;
        assert_eq!(stats.total_messages, 0);
        assert_eq!(stats.in_flight_count, 0);
        assert_eq!(stats.total_bytes, 0);
    }
}
