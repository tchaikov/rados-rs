//! OSD session management
//!
//! This module handles per-OSD connection and request tracking.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{oneshot, Mutex, RwLock};
use tracing::{debug, error, info, warn};

use crate::error::{OSDClientError, Result};
use crate::messages::{MOSDOp, MOSDOpReply};
use crate::types::{OpResult, RequestId};

/// Per-OSD connection and request tracking
pub struct OSDSession {
    pub osd_id: i32,
    connection: Arc<Mutex<Option<msgr2::protocol::Connection>>>,
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
            connection: Arc::new(Mutex::new(None)),
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
    pub async fn connect(&self, addr: std::net::SocketAddr) -> Result<()> {
        info!("Connecting to OSD {} at {}", self.osd_id, addr);

        // Create connection config with default features
        let config = msgr2::ConnectionConfig::default();

        // Connect using msgr2 (banner exchange only)
        let mut connection = msgr2::protocol::Connection::connect(addr, config)
            .await
            .map_err(|e| OSDClientError::Connection(format!("Failed to connect: {}", e)))?;

        info!(
            "Banner exchange complete, establishing session with OSD {}",
            self.osd_id
        );

        // Complete the full handshake (HELLO, AUTH, SESSION)
        connection.establish_session().await.map_err(|e| {
            OSDClientError::Connection(format!("Failed to establish session: {}", e))
        })?;

        info!("✓ Session established with OSD {}", self.osd_id);

        // Store the connection
        {
            let mut conn_guard = self.connection.lock().await;
            *conn_guard = Some(connection);
        }

        info!("✓ Connection stored for OSD {}", self.osd_id);

        Ok(())
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

        // Encode the operation
        let payload = op.encode()?;

        // Send the message
        let mut connection_guard = self.connection.lock().await;
        if let Some(conn) = connection_guard.as_mut() {
            // Build message with payload in front section
            let msg = msgr2::message::Message::new(crate::messages::CEPH_MSG_OSD_OP, payload);

            // Send via msgr2 connection
            conn.send_message(msg).await.map_err(|e| {
                OSDClientError::Connection(format!("Failed to send message: {}", e))
            })?;

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

        loop {
            // Receive message with connection locked
            let msg = {
                let mut guard = self.connection.lock().await;
                match guard.as_mut() {
                    Some(conn) => match conn.recv_message().await {
                        Ok(msg) => msg,
                        Err(e) => {
                            error!("Failed to receive message from OSD {}: {}", self.osd_id, e);
                            break;
                        }
                    },
                    None => {
                        debug!("No connection, recv_task for OSD {} exiting", self.osd_id);
                        return;
                    }
                }
            };

            // Handle message based on type
            if msg.msg_type() == crate::messages::CEPH_MSG_OSD_OPREPLY {
                match MOSDOpReply::decode(&msg.front) {
                    Ok(reply) => {
                        self.handle_reply(reply).await;
                    }
                    Err(e) => {
                        error!(
                            "Failed to decode MOSDOpReply from OSD {}: {}",
                            self.osd_id, e
                        );
                    }
                }
            } else {
                debug!(
                    "Received unexpected message type {} from OSD {}",
                    msg.msg_type(),
                    self.osd_id
                );
            }
        }

        info!("recv_task for OSD {} stopped", self.osd_id);
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
                    message: "OSD operation failed".to_string(),
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
        let guard = self.connection.lock().await;
        guard.is_some()
    }

    /// Get number of pending operations
    pub async fn pending_count(&self) -> usize {
        let guard = self.pending_ops.read().await;
        guard.len()
    }
}
