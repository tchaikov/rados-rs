//! OSD session management
//!
//! This module handles per-OSD connection and request tracking.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{oneshot, RwLock};
use tracing::{debug, info, warn};

use crate::error::{OSDClientError, Result};
use crate::messages::{MOSDOp, MOSDOpReply};
use crate::types::{OpResult, RequestId};

/// Per-OSD connection and request tracking
pub struct OSDSession {
    pub osd_id: i32,
    connection: Arc<RwLock<Option<msgr2::protocol::Connection>>>,
    pending_ops: Arc<RwLock<HashMap<u64, PendingOp>>>,
    next_tid: AtomicU64,
    entity_name: String,
    client_inc: u32,
}

/// Tracking information for a pending operation
pub struct PendingOp {
    pub tid: u64,
    pub reqid: RequestId,
    pub result_tx: oneshot::Sender<Result<OpResult>>,
    pub submitted_at: Instant,
}

impl OSDSession {
    /// Create a new OSD session
    pub fn new(osd_id: i32, entity_name: String, client_inc: u32) -> Self {
        Self {
            osd_id,
            connection: Arc::new(RwLock::new(None)),
            pending_ops: Arc::new(RwLock::new(HashMap::new())),
            next_tid: AtomicU64::new(1),
            entity_name,
            client_inc,
        }
    }

    /// Get the next transaction ID
    pub fn next_tid(&self) -> u64 {
        self.next_tid.fetch_add(1, Ordering::SeqCst)
    }

    /// Connect to the OSD
    ///
    /// This establishes a msgr2 connection to the OSD at the given address.
    /// Note: This is a simplified implementation that needs proper authentication.
    pub async fn connect(&self, osd_addr: &str) -> Result<()> {
        info!("Connecting to OSD {} at {}", self.osd_id, osd_addr);

        // TODO: Implement proper msgr2 connection with authentication
        // For now, we'll create a placeholder
        // In a real implementation, this would:
        // 1. Parse the osd_addr
        // 2. Create a msgr2::Connection
        // 3. Perform banner exchange
        // 4. Perform HELLO handshake
        // 5. Perform AUTH handshake
        // 6. Store the connection

        warn!("OSD connection not fully implemented yet");
        Err(OSDClientError::Connection(
            "OSD connection not yet implemented".into(),
        ))
    }

    /// Submit an operation to the OSD
    ///
    /// This sends a MOSDOp message and returns a receiver for the reply.
    pub async fn submit_op(&self, op: MOSDOp) -> Result<oneshot::Receiver<Result<OpResult>>> {
        let tid = op.reqid.tid;

        // Create channel for result
        let (tx, rx) = oneshot::channel();

        // Track the operation
        {
            let mut pending = self.pending_ops.write().await;
            pending.insert(
                tid,
                PendingOp {
                    tid,
                    reqid: op.reqid.clone(),
                    result_tx: tx,
                    submitted_at: Instant::now(),
                },
            );
        }

        // Send the message
        let connection_guard = self.connection.read().await;
        if let Some(_conn) = connection_guard.as_ref() {
            // Encode the operation
            let _payload = op.encode()?;

            // TODO: Send via msgr2 connection
            // In a real implementation:
            // conn.send_message(CEPH_MSG_OSD_OP, payload, data).await?;

            drop(connection_guard);
            debug!("Submitted operation tid={} to OSD {}", tid, self.osd_id);
            Ok(rx)
        } else {
            // Remove from pending since we can't send
            let mut pending = self.pending_ops.write().await;
            pending.remove(&tid);

            Err(OSDClientError::Connection("Not connected to OSD".into()))
        }
    }

    /// Background task for receiving replies
    ///
    /// This runs in a loop receiving messages from the OSD connection
    /// and matching them to pending operations.
    pub async fn recv_task(self: Arc<Self>) {
        info!("Starting recv_task for OSD {}", self.osd_id);

        // TODO: Implement proper message receive loop
        // This is currently a placeholder that checks connection and exits
        let has_connection = {
            let guard = self.connection.read().await;
            guard.is_some()
        };

        if !has_connection {
            debug!("No connection, recv_task for OSD {} not started", self.osd_id);
            return;
        }

        // When fully implemented, this will:
        // loop {
        //     let msg = match conn.recv_message().await {
        //         Ok(m) => m,
        //         Err(e) => break,
        //     };
        //     if msg.header.msg_type == CEPH_MSG_OSD_OPREPLY {
        //         match MOSDOpReply::decode(&msg.payload) {
        //             Ok(reply) => self.handle_reply(reply).await,
        //             Err(e) => error!("Failed to decode: {}", e),
        //         }
        //     }
        // }

        info!("recv_task for OSD {} stopped (not implemented)", self.osd_id);
    }

    /// Handle an incoming MOSDOpReply
    async fn handle_reply(&self, reply: MOSDOpReply) {
        let tid = reply.reqid.tid;

        // Find the pending operation
        let pending_op = {
            let mut pending = self.pending_ops.write().await;
            pending.remove(&tid)
        };

        if let Some(op) = pending_op {
            let elapsed = op.submitted_at.elapsed();
            debug!("Received reply for tid={} after {:?}", tid, elapsed);

            // Convert reply to result
            let result = if reply.result == 0 {
                Ok(reply.to_op_result())
            } else {
                Err(OSDClientError::OSDError {
                    code: reply.result,
                    message: format!("OSD operation failed"),
                })
            };

            // Send result (ignore if receiver dropped)
            let _ = op.result_tx.send(result);
        } else {
            warn!("Received reply for unknown tid={}", tid);
        }
    }

    /// Check if we're connected
    pub async fn is_connected(&self) -> bool {
        let guard = self.connection.read().await;
        guard.is_some()
    }

    /// Get number of pending operations
    pub async fn pending_count(&self) -> usize {
        let guard = self.pending_ops.read().await;
        guard.len()
    }
}
