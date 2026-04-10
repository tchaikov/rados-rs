//! Request tracker for timeout handling
//!
//! Provides per-operation timeout tracking independent of connection status.
//! Uses a background task that sleeps until the next operation deadline,
//! minimizing overhead while ensuring operations timeout even during
//! connection failures or reconnection attempts.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{debug, warn};

/// Configuration for request tracking
#[derive(Debug, Clone)]
pub struct TrackerConfig {
    /// Default operation timeout
    pub operation_timeout: Duration,
}

impl Default for TrackerConfig {
    fn default() -> Self {
        Self {
            operation_timeout: Duration::from_secs(30),
        }
    }
}

/// Command to the timeout manager task
enum TimeoutCommand {
    /// Track a new operation with its deadline
    Track {
        tid: u64,
        osd_id: i32,
        deadline: Instant,
    },
    /// Stop tracking an operation (completed successfully)
    Untrack { tid: u64, osd_id: i32 },
    /// Shutdown the tracker
    Shutdown,
}

/// Timeout callback for notifying sessions of timed-out operations
pub type TimeoutCallback = Arc<dyn Fn(i32, u64) + Send + Sync>;

/// Request tracker for handling per-operation timeouts.
///
/// Uses a background task with `tokio::time::sleep_until` to efficiently
/// track operation deadlines. Operations are stored in a BTreeMap ordered
/// by deadline, allowing O(log n) insertion and O(1) next-deadline lookup.
///
/// The command channel is **bounded**, sized to the client's
/// `max_inflight_ops`. Each in-flight op produces at most one Track + one
/// Untrack, so the throttle already caps the channel depth; bounding it
/// makes that relationship explicit and gives proper backpressure instead
/// of the unbounded-growth risk the previous `mpsc::unbounded_channel()`
/// carried when the tracker task fell behind.
pub struct Tracker {
    config: TrackerConfig,
    /// Channel for sending commands to the timeout manager task.
    cmd_tx: mpsc::Sender<TimeoutCommand>,
}

impl Tracker {
    /// Create a new tracker with a timeout callback.
    ///
    /// `max_inflight_ops` comes from the enclosing `OSDClientConfig`; the
    /// command channel buffer is sized to `2 * max_inflight_ops` so a burst
    /// of simultaneous Track + Untrack commands (one per op going in flight
    /// and one per op completing at the same time) never blocks — under
    /// normal operation the throttle caps the concurrent op count at
    /// `max_inflight_ops`, so the buffer cannot overflow unless the
    /// timeout manager task is itself blocked.
    ///
    /// The callback is invoked when an operation times out, passing
    /// `(osd_id, tid)`. This allows the OSDClient to cancel the operation
    /// in the appropriate session.
    pub fn new(
        config: TrackerConfig,
        max_inflight_ops: usize,
        timeout_callback: TimeoutCallback,
    ) -> Self {
        // 2x headroom over the throttle cap, with a floor so a
        // mis-configured max_inflight_ops = 0 still yields a usable channel
        // for the single Shutdown command.
        let buffer = max_inflight_ops.saturating_mul(2).max(8);
        let (cmd_tx, cmd_rx) = mpsc::channel(buffer);

        // Spawn background timeout manager task
        tokio::spawn(Self::timeout_manager_task(cmd_rx, timeout_callback));

        Self { config, cmd_tx }
    }

    /// Get the configured operation timeout duration
    pub fn operation_timeout(&self) -> Duration {
        self.config.operation_timeout
    }

    /// Track a new operation with a deadline.
    ///
    /// The operation will be cancelled via the timeout callback if it
    /// doesn't complete before the deadline. If the tracker task has
    /// already exited (shutdown in progress), the send is silently
    /// dropped — callers at that point don't need timeout tracking.
    pub async fn track(&self, tid: u64, osd_id: i32, deadline: Instant) {
        let _ = self
            .cmd_tx
            .send(TimeoutCommand::Track {
                tid,
                osd_id,
                deadline,
            })
            .await;
    }

    /// Stop tracking an operation (called when the operation completes).
    pub async fn untrack(&self, tid: u64, osd_id: i32) {
        let _ = self
            .cmd_tx
            .send(TimeoutCommand::Untrack { tid, osd_id })
            .await;
    }

    /// Shutdown the tracker (called from `OSDClient::shutdown`).
    pub async fn shutdown(&self) {
        let _ = self.cmd_tx.send(TimeoutCommand::Shutdown).await;
    }

    /// Background task that manages operation timeouts
    ///
    /// Uses a BTreeMap to efficiently track operations ordered by deadline,
    /// with a HashMap as secondary index for O(1) deadline lookup during untrack.
    /// Sleeps until the next deadline, then processes all expired operations.
    ///
    /// Both data structures are plain local variables (not `Arc<RwLock<>>`)
    /// because `tokio::select!` branches are mutually exclusive within this
    /// single task -- there is no concurrent access.
    async fn timeout_manager_task(
        mut cmd_rx: mpsc::Receiver<TimeoutCommand>,
        timeout_callback: TimeoutCallback,
    ) {
        // Primary index: Map (deadline, osd_id, tid) -> ()
        // BTreeMap keeps entries sorted by key, so first entry is next to timeout
        let mut tracked_ops: BTreeMap<(Instant, i32, u64), ()> = BTreeMap::new();

        // Secondary index: Map (osd_id, tid) -> deadline
        // Allows O(1) deadline lookup for untrack operations
        let mut deadline_index: HashMap<(i32, u64), Instant> = HashMap::new();

        loop {
            // Get the next deadline, if any
            let next_deadline = tracked_ops.keys().next().map(|(deadline, _, _)| *deadline);

            // Wait for either: next timeout, or a command
            tokio::select! {
                // Sleep until next deadline
                _ = async {
                    if let Some(deadline) = next_deadline {
                        tokio::time::sleep_until(deadline).await;
                    } else {
                        // No deadlines, wait forever
                        std::future::pending::<()>().await;
                    }
                } => {
                    // Process expired operations
                    let now = Instant::now();

                    // Collect all expired operations
                    let expired: Vec<(Instant, i32, u64)> = tracked_ops
                        .range(..(now, i32::MAX, u64::MAX))
                        .map(|(k, _)| *k)
                        .collect();

                    // Remove and timeout each expired operation
                    for (i, &(deadline, osd_id, tid)) in expired.iter().enumerate() {
                        tracked_ops.remove(&(deadline, osd_id, tid));
                        deadline_index.remove(&(osd_id, tid));
                        warn!(
                            "Operation timeout: OSD {} tid={} (deadline exceeded by {:?})",
                            osd_id,
                            tid,
                            now.duration_since(deadline)
                        );
                        timeout_callback(osd_id, tid);

                        // Yield every 50 timeouts to avoid blocking other tasks
                        if i > 0 && i % 50 == 0 {
                            tokio::task::yield_now().await;
                        }
                    }
                }

                // Handle commands
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(TimeoutCommand::Track { tid, osd_id, deadline }) => {
                            tracked_ops.insert((deadline, osd_id, tid), ());
                            deadline_index.insert((osd_id, tid), deadline);
                            debug!("Tracking operation: OSD {} tid={} deadline={:?}", osd_id, tid, deadline);
                        }
                        Some(TimeoutCommand::Untrack { tid, osd_id }) => {
                            // O(1) deadline lookup from secondary index
                            if let Some(deadline) = deadline_index.remove(&(osd_id, tid)) {
                                // O(log n) removal from BTreeMap using full key
                                tracked_ops.remove(&(deadline, osd_id, tid));
                                debug!("Untracked operation: OSD {} tid={}", osd_id, tid);
                            } else {
                                debug!("Untrack called for non-existent operation: OSD {} tid={}", osd_id, tid);
                            }
                        }
                        Some(TimeoutCommand::Shutdown) => {
                            debug!("Tracker shutting down");
                            break;
                        }
                        None => {
                            debug!("Tracker command channel closed");
                            break;
                        }
                    }
                }
            }
        }

        debug!("Timeout manager task exited");
    }
}
