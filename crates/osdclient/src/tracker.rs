//! Request tracker for timeout handling
//!
//! Provides per-operation timeout tracking independent of connection status.
//! Uses a background task that sleeps until the next operation deadline,
//! minimizing overhead while ensuring operations timeout even during
//! connection failures or reconnection attempts.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
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

/// Request tracker for handling per-operation timeouts
///
/// Uses a background task with `tokio::time::sleep_until` to efficiently
/// track operation deadlines. Operations are stored in a BTreeMap ordered
/// by deadline, allowing O(log n) insertion and O(1) next-deadline lookup.
pub struct Tracker {
    config: TrackerConfig,
    /// Channel for sending commands to the timeout manager task
    cmd_tx: mpsc::UnboundedSender<TimeoutCommand>,
}

impl Tracker {
    /// Create a new tracker with a timeout callback
    ///
    /// The callback is invoked when an operation times out, passing (osd_id, tid).
    /// This allows the OSDClient to cancel the operation in the appropriate session.
    pub fn new(config: TrackerConfig, timeout_callback: TimeoutCallback) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        // Spawn background timeout manager task
        tokio::spawn(Self::timeout_manager_task(cmd_rx, timeout_callback));

        Self { config, cmd_tx }
    }

    /// Get the configured operation timeout duration
    pub fn operation_timeout(&self) -> Duration {
        self.config.operation_timeout
    }

    /// Track a new operation with a deadline
    ///
    /// The operation will be cancelled via the timeout callback if it doesn't
    /// complete before the deadline.
    pub fn track(&self, tid: u64, osd_id: i32, deadline: Instant) {
        let _ = self.cmd_tx.send(TimeoutCommand::Track {
            tid,
            osd_id,
            deadline,
        });
    }

    /// Stop tracking an operation (called when operation completes)
    pub fn untrack(&self, tid: u64, osd_id: i32) {
        let _ = self.cmd_tx.send(TimeoutCommand::Untrack { tid, osd_id });
    }

    /// Shutdown the tracker (called when OSDClient is dropped)
    pub fn shutdown(&self) {
        let _ = self.cmd_tx.send(TimeoutCommand::Shutdown);
    }

    /// Background task that manages operation timeouts
    ///
    /// Uses a BTreeMap to efficiently track operations ordered by deadline,
    /// with a HashMap as secondary index for O(1) deadline lookup during untrack.
    /// Sleeps until the next deadline, then processes all expired operations.
    async fn timeout_manager_task(
        mut cmd_rx: mpsc::UnboundedReceiver<TimeoutCommand>,
        timeout_callback: TimeoutCallback,
    ) {
        // Primary index: Map (deadline, osd_id, tid) -> ()
        // BTreeMap keeps entries sorted by key, so first entry is next to timeout
        type TrackedOps = BTreeMap<(Instant, i32, u64), ()>;
        let tracked_ops: Arc<RwLock<TrackedOps>> = Arc::new(RwLock::new(BTreeMap::new()));

        // Secondary index: Map (osd_id, tid) -> deadline
        // Allows O(1) deadline lookup for untrack operations
        type DeadlineIndex = HashMap<(i32, u64), Instant>;
        let deadline_index: Arc<RwLock<DeadlineIndex>> = Arc::new(RwLock::new(HashMap::new()));

        loop {
            // Get the next deadline, if any
            let next_deadline = {
                let ops = tracked_ops.read().await;
                ops.keys().next().map(|(deadline, _, _)| *deadline)
            };

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
                    let mut ops = tracked_ops.write().await;
                    let mut index = deadline_index.write().await;

                    // Collect all expired operations
                    let expired: Vec<(Instant, i32, u64)> = ops
                        .range(..(now, i32::MAX, u64::MAX))
                        .map(|(k, _)| *k)
                        .collect();

                    // Remove and timeout each expired operation
                    for (i, (deadline, osd_id, tid)) in expired.iter().enumerate() {
                        ops.remove(&(*deadline, *osd_id, *tid));
                        index.remove(&(*osd_id, *tid));
                        warn!(
                            "Operation timeout: OSD {} tid={} (deadline exceeded by {:?})",
                            osd_id,
                            tid,
                            now.duration_since(*deadline)
                        );
                        timeout_callback(*osd_id, *tid);

                        // Yield every 50 timeouts to avoid blocking other tasks
                        if i > 0 && i % 50 == 0 {
                            // Drop locks before yielding
                            drop(ops);
                            drop(index);
                            tokio::task::yield_now().await;
                            // Re-acquire locks
                            ops = tracked_ops.write().await;
                            index = deadline_index.write().await;
                        }
                    }
                }

                // Handle commands
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(TimeoutCommand::Track { tid, osd_id, deadline }) => {
                            let mut ops = tracked_ops.write().await;
                            let mut index = deadline_index.write().await;
                            ops.insert((deadline, osd_id, tid), ());
                            index.insert((osd_id, tid), deadline);
                            debug!("Tracking operation: OSD {} tid={} deadline={:?}", osd_id, tid, deadline);
                        }
                        Some(TimeoutCommand::Untrack { tid, osd_id }) => {
                            let mut ops = tracked_ops.write().await;
                            let mut index = deadline_index.write().await;

                            // O(1) deadline lookup from secondary index
                            if let Some(deadline) = index.remove(&(osd_id, tid)) {
                                // O(log n) removal from BTreeMap using full key
                                ops.remove(&(deadline, osd_id, tid));
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
